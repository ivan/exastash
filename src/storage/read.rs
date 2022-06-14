//! Functions to read content from storage

use anyhow::{Result, Error, anyhow, bail, ensure};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use tracing::{info, debug, error};
use futures::{stream::{self, Stream, BoxStream, TryStreamExt}};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream;
use tokio_util::codec::FramedRead;
use reqwest::StatusCode;
use aes::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
use aes::cipher::generic_array::GenericArray;
use futures_async_stream::try_stream;
use std::sync::Arc;
use parking_lot::Mutex;
use crate::blake3::Blake3HashingStream;
use crate::db;
use crate::db::inode;
use crate::db::storage::{get_storage_views, StorageView, fofs, inline, gdrive, internetarchive};
use crate::db::storage::gdrive::file::{GdriveFile, GdriveOwner};
use crate::db::google_auth::{GoogleAccessToken, GoogleServiceAccount};
use crate::util;
use crate::policy;
use crate::gdrive::{request_gdrive_file, get_crc32c_in_response};
use crate::crypto::{GcmDecoder, gcm_create_key};

type Aes128Ctr = ctr::Ctr64BE<aes::Aes128>;

/// Return a Vec of access tokens potentially suitable for read and delete operations
/// on a file.
///
/// If `owner_id` is `None`, this can return more than one token, and all tokens may
/// need to be tried.
pub(crate) async fn get_access_tokens(owner_id: Option<i32>, domain_id: i16) -> Result<Vec<String>> {
    let pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;

    let all_owners = GdriveOwner::find_by_domain_ids(&mut transaction, &[domain_id]).await?;
    let all_owner_ids: Vec<_> = all_owners.iter().map(|owner| owner.id).collect();
    let owner_ids = match owner_id {
        // Old files in our database have no recorded owner, so we may need to try all owners.
        None => all_owner_ids.clone(),
        Some(id) => vec![id],
    };

    let mut tokens = vec![];

    // Regardless of file owner, service accounts are presumed to have been granted
    // read access to all or most files on the domain.
    //
    // Always try a random service account first, because we have more service
    // accounts than regular accounts, thus making us less likely to run into daily
    // per-account transfer limits.
    for service_account in GoogleServiceAccount::find_by_owner_ids(&mut transaction, &all_owner_ids, Some(1)).await? {
        let auth = yup_oauth2::ServiceAccountAuthenticator::builder(service_account.key).build().await?;
        let scopes = &["https://www.googleapis.com/auth/drive"];
        let token = auth.token(scopes).await?;
        tokens.push(token.as_str().to_string());
    }

    for token in GoogleAccessToken::find_by_owner_ids(&mut transaction, &owner_ids).await? {
        tokens.push(token.access_token);
    }

    transaction.commit().await?; // close read-only transaction

    Ok(tokens)
}

/// Pinned boxed dyn Stream of bytes::Bytes
pub type ReadStream = BoxStream<'static, Result<Bytes, Error>>;

/// Takes a `Stream` of a gdrive response body and return a `Stream` that yields
/// an Err if the crc32c or body length is correct.
fn stream_add_validation(
    gdrive_file: &gdrive::file::GdriveFile,
    stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin + Send + 'static,
) -> ReadStream {
    let expected_crc = gdrive_file.crc32c;
    let expected_size = gdrive_file.size as u64;
    let mut crc = 0;
    let mut size = 0;
    Box::pin(
        #[try_stream]
        async move {
            #[for_await]
            for item in stream {
                let bytes = item?;
                size += bytes.len() as u64;
                crc = crc32c::crc32c_append(crc, bytes.as_ref());
                yield bytes;
            }
            if size != expected_size {
                bail!("expected response body with {} bytes but got {} bytes", expected_size, size);
            }
            if crc != expected_crc {
                bail!("expected response body with crc32c of {} but got data with crc32c of {}", expected_crc, crc);
            }
        }
    )
}

async fn touch_last_probed(file_ids: &[&str]) -> Result<()> {
    let pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;
    db::disable_synchronous_commit(&mut transaction).await?;
    GdriveFile::touch_last_probed(&mut transaction, file_ids).await?;
    transaction.commit().await?;
    Ok(())
}

/// Returns a Stream of Bytes for a `GdriveFile`, first validating the
/// response code and `x-goog-hash`.
pub async fn stream_gdrive_file(gdrive_file: &gdrive::file::GdriveFile, domain_id: i16) -> Result<impl Stream<Item = Result<Bytes, Error>>> {
    let access_tokens = get_access_tokens(gdrive_file.owner_id, domain_id).await?;
    if access_tokens.is_empty() {
        bail!("no access tokens were available for owners associated file_id={:?} (domain_id={})", gdrive_file.id, domain_id);
    }
    let tries = 3;
    let access_tokens_tries = access_tokens.iter().cycle().take(access_tokens.len() * tries);

    let mut out = Err(anyhow!("Google did not respond with an OK response after trying all access tokens"));
    for access_token in access_tokens_tries {
        debug!("trying access token {}", access_token);
        let response = request_gdrive_file(&gdrive_file.id, access_token).await?;
        let headers = response.headers();
        debug!(file_id = gdrive_file.id.as_str(), "Google responded to request with headers {:#?}", headers);
        match response.status() {
            StatusCode::OK => {
                let content_length = response.content_length().ok_or_else(|| {
                    anyhow!("Google responded without a Content-Length")
                })?;
                if content_length != gdrive_file.size as u64 {
                    bail!("Google responded with Content-Length {}, expected {}", content_length, gdrive_file.size);
                }
                let goog_crc32c = get_crc32c_in_response(&response)?;
                if goog_crc32c != gdrive_file.crc32c {
                    bail!("Google sent crc32c={} but we expected crc32c={}", goog_crc32c, gdrive_file.crc32c);
                }
                out = Ok(stream_add_validation(gdrive_file, response.bytes_stream()));
                break;
            },
            // BAD_REQUEST, FORBIDDEN, INTERNAL_SERVER_ERROR, SERVICE_UNAVAILABLE have been observed as transient errors from Google Drive
            // UNAUTHORIZED, NOT_FOUND probably indicate that the wrong access token was used
            StatusCode::BAD_REQUEST |
            StatusCode::UNAUTHORIZED |
            StatusCode::FORBIDDEN |
            StatusCode::NOT_FOUND |
            StatusCode::INTERNAL_SERVER_ERROR |
            StatusCode::SERVICE_UNAVAILABLE => {
                debug!("Google responded with HTTP status code {} for file_id={:?}, \
                        trying another access token if available", response.status(), gdrive_file.id);
                continue;
            }
            _ => bail!("Google responded with HTTP status code {} for file_id={:?}", response.status(), gdrive_file.id),
        };
    }
    let gdrive_file_id = gdrive_file.id.clone();
    // Go faster by not .await'ing touch_last_probed
    tokio::spawn(async move {
        if let Err(err) = touch_last_probed(&[&gdrive_file_id]).await {
            error!(?err, "touch_last_probed failed");
        }
    });
    out
}

fn stream_gdrive_ctr_chunks(file: &inode::File, storage: &gdrive::Storage) -> ReadStream {
    let file = file.clone();
    let storage = storage.clone();

    Box::pin(
        #[try_stream]
        async move {
            let mut ctr_stream_bytes = 0;
            let pool = db::pgpool().await;
            let mut transaction = pool.begin().await?;
            let gdrive_ids: Vec<&str> = storage.gdrive_ids.iter().map(String::as_str).collect();
            let gdrive_files = GdriveFile::find_by_ids_in_order(&mut transaction, &gdrive_ids).await?;
            transaction.commit().await?; // close read-only transaction

            let mut total_bytes_read: i64 = 0;

            for gdrive_file in gdrive_files {
                info!(id = &*gdrive_file.id, size = gdrive_file.size, "streaming gdrive file");
                let encrypted_stream = stream_gdrive_file(&gdrive_file, storage.google_domain).await?;
                let key = GenericArray::from_slice(&storage.cipher_key);
                let nonce = GenericArray::from_slice(&[0; 16]);
                let mut cipher = Aes128Ctr::new(key, nonce);
                cipher.seek(ctr_stream_bytes);
                ctr_stream_bytes += gdrive_file.size as u64;

                #[for_await]
                for frame in encrypted_stream {
                    let encrypted = frame?;
                    let mut decrypted = encrypted.to_vec();
                    cipher.apply_keystream(&mut decrypted);
                    let mut bytes: Bytes = decrypted.into();
                    // We need to truncate the NULL padding that was suffixed to the chunk before encryption.
                    // keep_bytes will usually be too large, but there is no harm.
                    let mut keep_bytes = file.size - total_bytes_read;
                    if keep_bytes < 0 {
                        keep_bytes = 0;
                    }
                    total_bytes_read += bytes.len() as i64;
                    bytes.truncate(keep_bytes as usize);
                    yield bytes;
                }
            }
        }
    )
}

pub(crate) fn get_aes_gcm_length(content_length: u64, block_size: usize) -> u64 {
    // We want division to round up here, so fix it up by incrementing when needed
    let mut number_of_tags = content_length / block_size as u64;
    if content_length % block_size as u64 != 0 {
        number_of_tags += 1;
    }
    let length_of_tags = 16 * number_of_tags;
    content_length + length_of_tags
}

fn stream_gdrive_gcm_chunks(file: &inode::File, storage: &gdrive::Storage) -> ReadStream {
    let file = file.clone();
    let storage = storage.clone();

    Box::pin(
        #[try_stream]
        async move {
            let pool = db::pgpool().await;
            let mut transaction = pool.begin().await?;
            let gdrive_ids: Vec<&str> = storage.gdrive_ids.iter().map(String::as_str).collect();
            let gdrive_files = GdriveFile::find_by_ids_in_order(&mut transaction, &gdrive_ids).await?;
            transaction.commit().await?; // close read-only transaction

            let whole_block_size = 65536;
            // Block size for all of our AES-128-GCM files
            let block_size = whole_block_size - 16;
            let aes_gcm_length = get_aes_gcm_length(file.size as u64, block_size);

            let mut gcm_stream_bytes = 0;
            for gdrive_file in gdrive_files {
                info!(id = &*gdrive_file.id, size = gdrive_file.size, "streaming gdrive file");
                let encrypted_stream = stream_gdrive_file(&gdrive_file, storage.google_domain).await?;
                let encrypted_read = encrypted_stream
                    .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                    .into_async_read()
                    .compat();

                // We need to truncate the random padding off the gdrive file itself, to avoid
                // AES-GCM decryption failure.
                //
                // aes_gcm_length tells us when to stop for the size of the entire stream,
                // but we actually need to truncate an individual gdrive file.
                let last_gcm_stream_bytes = gcm_stream_bytes;
                gcm_stream_bytes += gdrive_file.size as u64;
                // keep_bytes will be too large except for the last gdrive file in the sequence, but
                // there is no harm.
                let keep_bytes = aes_gcm_length - last_gcm_stream_bytes;
                let truncated_read = encrypted_read.take(keep_bytes);

                let key = gcm_create_key(storage.cipher_key).unwrap();
                let first_block_number = last_gcm_stream_bytes / whole_block_size as u64;
                let decoder = GcmDecoder::new(block_size, key, first_block_number);
                let frame_reader = FramedRead::new(truncated_read, decoder);
                #[for_await]
                for frame in frame_reader {
                    yield frame?;
                }
            }
        }
    )
}

fn stream_gdrive_files(file: &inode::File, storage: &gdrive::Storage) -> ReadStream {
    match storage.cipher {
        gdrive::Cipher::Aes128Gcm => stream_gdrive_gcm_chunks(file, storage),
        // We no longer create AES-128-CTR files, but we still need to read them
        gdrive::Cipher::Aes128Ctr => stream_gdrive_ctr_chunks(file, storage),
    }
}

pub(crate) async fn request_remote_fofs_file(file: &inode::File, storage: &fofs::StorageView) -> Result<reqwest::Response> {
    // We need `policy` to go out of scope because trait `std::marker::Send`
    // is not implemented for `*mut libquickjs_sys::JSRuntime`
    let base_url = {
        let policy = policy::get_policy()?;
        policy.fofs_base_url(&storage.pile_hostname)?
    };
    let url = format!("{}/fofs/{}/{}/{}", base_url, storage.pile_id, storage.cell_id, file.id);
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .send().await?;
    Ok(response)
}

async fn stream_fofs_file(file: &inode::File, storage: &fofs::StorageView) -> Result<ReadStream> {
    let my_hostname = util::get_hostname();
    if storage.pile_hostname != my_hostname {
        let response = request_remote_fofs_file(file, storage).await?;

        let content_length = response.content_length().ok_or_else(|| {
            anyhow!("remote fofs host {} responded without a Content-Length", storage.pile_hostname)
        })?;
        if content_length != file.size as u64 {
            bail!("file should be {} bytes but remote fofs host {} responded with Content-Length: {}",
            file.size, storage.pile_hostname, content_length);
        }
        let stream = response.bytes_stream();

        Ok(Box::pin(
            #[try_stream]
            async move {
                #[for_await]
                for item in stream {
                    let bytes = item?;
                    yield bytes;
                }
            }
        ))
    } else {
        let fname = format!("{}/{}/{}/{}", storage.pile_path, storage.pile_id, storage.cell_id, file.id);
        let fofs_file_size = tokio::fs::metadata(&fname).await?.len();
        if fofs_file_size != file.size as u64 {
            bail!("file in fofs {:?} had unexpected size={} instead of size={}", fname, fofs_file_size, file.size)
        }
        let file = tokio::fs::File::open(fname).await?;
        let stream = ReaderStream::new(file);

        Ok(Box::pin(
            #[try_stream]
            async move {
                #[for_await]
                for item in stream {
                    let bytes = item?;
                    yield bytes;
                }
            }
        ))
    }
}

/// Return the content of a storage as a pinned boxed Stream on which caller can call `.into_async_read()`
async fn read_storage_without_checks(file: &inode::File, storage: &StorageView) -> Result<ReadStream> {
    Ok(match storage {
        StorageView::Inline(inline::Storage { content_zstd, .. }) => {
            info!(id = file.id, "reading file from inline storage");
            let content = zstd::stream::decode_all(content_zstd.as_slice())?;
            ensure!(
                content.len() as i64 == file.size,
                "length of inline storage for file id={} is {} but file size is {}", file.id, content.len(), file.size
            );

            let mut bytes = BytesMut::new();
            bytes.put(&content[..]);
            Box::pin(stream::iter::<_>(vec![Ok(bytes.copy_to_bytes(bytes.remaining()))]))
        }
        StorageView::Fofs(fofs_storage) => {
            info!(id = file.id, pile_id = fofs_storage.pile_id, "reading file from fofs storage");
            stream_fofs_file(file, fofs_storage).await?
        }
        StorageView::Gdrive(gdrive_storage) => {
            info!(id = file.id, google_domain = gdrive_storage.google_domain, "reading file from gdrive storage");
            stream_gdrive_files(file, gdrive_storage)
        }
        StorageView::InternetArchive(internetarchive::Storage { .. }) => {
            unimplemented!()
        }
    })
}

/// Return the content of a storage as a pinned boxed Stream on which caller can call `.into_async_read()`,
/// while also verifying the size and the b3sum of the file (if it has a known b3sum).
pub async fn read_storage(file: &inode::File, storage: &StorageView, b3sum: Arc<Mutex<blake3::Hasher>>) -> Result<ReadStream> {
    let underlying_stream = read_storage_without_checks(file, storage).await?;
    let hashing_stream = Blake3HashingStream::new(underlying_stream, b3sum.clone());
    let file = file.clone();
    Ok(Box::pin(
        #[try_stream]
        async move {
            let mut bytes_read: i64 = 0;

            #[for_await]
            for frame in hashing_stream {
                let frame = frame?;
                bytes_read += frame.len() as i64;
                yield frame;
            }

            if bytes_read != file.size {
                bail!("file with id={} should have had {} bytes but read {}", file.id, file.size, bytes_read);
            }

            let computed_hash = blake3::Hasher::finalize(&b3sum.lock().clone());
            if let Some(db_hash) = file.b3sum {
                ensure!(
                    computed_hash.as_bytes() == &db_hash,
                    "computed b3sum for content is {:?} but file has b3sum={:?}",
                    hex::encode(computed_hash.as_bytes()), hex::encode(db_hash)
                );
            }
        }
    ))
}

/// Sort a slice of StorageView by priority, best first
fn sort_storage_views_by_priority(storages: &mut [StorageView]) {
    storages.sort_by_cached_key(|storage| {
        match storage {
            // Prefer inline because it already has the file content
            StorageView::Inline(inline::Storage { .. }) => 0,
            // Prefer fofs over gdrive to reduce unnecessary API calls to Google.
            // Prefer localhost fofs over other fofs.
            StorageView::Fofs(fofs::StorageView { pile_hostname, .. }) => {
                if pile_hostname == &util::get_hostname() { 1 } else { 2 }
            },
            // Prefer gdrive over internetarchive because internetarchive is very slow now
            StorageView::Gdrive { .. } => 3,
            StorageView::InternetArchive(internetarchive::Storage { .. }) => 4,
        }
    });
}

/// Return the content of a file as a pinned boxed Stream on which caller can call `.into_async_read()`
/// If the file is missing a b3sum but was otherwise read without error, add the b3sum to the database.
pub async fn read(file_id: i64) -> Result<(ReadStream, inode::File)> {
    let pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;

    let mut files = inode::File::find_by_ids(&mut transaction, &[file_id]).await?;
    transaction.commit().await?; // close read-only transaction
    ensure!(files.len() == 1, "no such file with id={}", file_id);
    let file = files.pop().unwrap();
    let file_size = file.size;

    if file_size == 0 {
        let bytes = Bytes::new();
        return Ok((Box::pin(stream::iter::<_>(vec![Ok(bytes)])), file));
    }

    let mut storages = get_storage_views(&[file_id]).await?;
    sort_storage_views_by_priority(&mut storages);
    let b3sum = Arc::new(Mutex::new(blake3::Hasher::new()));
    let underlying_stream = match storages.get(0) {
        Some(storage) => read_storage(&file, storage, b3sum.clone()).await?,
        None => bail!("file with id={} has no storage", file_id)
    };

    let file_b3sum = file.b3sum;
    // We only need to wrap the stream with this stream if file.b3sum is unset
    let stream = if file_b3sum.is_none() {
        Box::pin(
            #[try_stream]
            async move {
                #[for_await]
                for frame in underlying_stream {
                    yield frame?;
                }

                let mut transaction = pool.begin().await?;
                let computed_hash = blake3::Hasher::finalize(&b3sum.lock().clone());
                info!("fixing unset b3sum on file id={} to {:?}", file_id, hex::encode(computed_hash.as_bytes()));
                db::disable_synchronous_commit(&mut transaction).await?;
                inode::File::set_b3sum(&mut transaction, file_id, computed_hash.as_bytes()).await?;
                transaction.commit().await?;
            }
        )
    } else {
        underlying_stream
    };

    Ok((stream, file))
}

/// Helper function for copying a ReadStream to an AsyncWrite
pub async fn write_stream_to_sink<S>(stream: ReadStream, sink: &mut S) -> Result<()>
where
    S: tokio::io::AsyncWrite + Unpin
{
    let mut read = stream
        .map_err(|e: Error| futures::io::Error::new(futures::io::ErrorKind::Other, e))
        .into_async_read()
        .compat();
    tokio::io::copy(&mut read, sink).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ensure_send<T: Send>(_: T) {}

    /// Ensure the future returned by `read` is Send, to avoid breaking callers
    /// e.g. tubekit that require Send.
    #[test]
    fn test_read_is_send() {
        let fut = read(0);
        ensure_send(fut);
    }
}
