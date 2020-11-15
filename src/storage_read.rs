//! Functions to read content from storage

use std::pin::Pin;
use anyhow::{Result, Error, anyhow, bail, ensure};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use tracing::{info, debug};
use futures::{stream::{self, Stream, TryStreamExt}};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio::io::AsyncReadExt;
use tokio_util::codec::FramedRead;
use reqwest::StatusCode;
use aes_ctr::Aes128Ctr;
use futures_async_stream::try_stream;
use aes_ctr::cipher::generic_array::GenericArray;
use aes_ctr::cipher::{NewStreamCipher, SyncStreamCipher, SyncStreamCipherSeek};
use std::sync::Arc;
use parking_lot::Mutex;
use crate::blake3::Blake3HashingStream;
use crate::db;
use crate::db::inode;
use crate::db::storage::{get_storages, Storage, inline, gdrive, internetarchive};
use crate::db::storage::gdrive::file::{GdriveFile, GdriveOwner};
use crate::db::google_auth::{GsuiteAccessToken, GsuiteServiceAccount};
use crate::gdrive::{request_gdrive_file, get_crc32c_in_response};
use crate::crypto::{GcmDecoder, gcm_create_key};


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
    for service_account in GsuiteServiceAccount::find_by_owner_ids(&mut transaction, &all_owner_ids, Some(1)).await? {
        let auth = yup_oauth2::ServiceAccountAuthenticator::builder(service_account.key).build().await?;
        let scopes = &["https://www.googleapis.com/auth/drive"];
        let token = auth.token(scopes).await?;
        tokens.push(token.as_str().to_string());
    }

    for token in GsuiteAccessToken::find_by_owner_ids(&mut transaction, &owner_ids).await? {
        tokens.push(token.access_token);
    }

    Ok(tokens)
}

/// Takes a `Stream` of a gdrive response body and return a `Stream` that yields
/// an Err if the crc32c or body length is correct.
fn stream_add_validation(
    gdrive_file: &gdrive::file::GdriveFile,
    stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin + 'static,
) -> Pin<Box<dyn Stream<Item = Result<Bytes, Error>>>> {
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
                bail!("expected response body with {} bytes but got {}", expected_size, size);
            }
            if crc != expected_crc {
                bail!("expected response body to crc32c to {} but got {}", expected_crc, crc);
            }
        }
    )
}

/// Returns a Stream of Bytes for a `GdriveFile`, first validating the
/// response code and `x-goog-hash`.
pub async fn stream_gdrive_file(gdrive_file: &gdrive::file::GdriveFile, domain_id: i16) -> Result<impl Stream<Item = Result<Bytes, Error>>> {
    let access_tokens = get_access_tokens(gdrive_file.owner_id, domain_id).await?;
    if access_tokens.is_empty() {
        bail!("no access tokens were available for owners associated file_id={:?} (domain_id={})", gdrive_file.id, domain_id);
    }
    let mut out = Err(anyhow!("Google did not respond with an OK response after trying all access tokens"));
    for access_token in &access_tokens {
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
            StatusCode::UNAUTHORIZED => {
                debug!("Google responded with HTTP status code {} for file_id={:?}, \
                        trying another access token if available", response.status(), gdrive_file.id);
                continue;
            }
            _ => bail!("Google responded with HTTP status code {} for file_id={:?}", response.status(), gdrive_file.id),
        };
    }
    out
}

fn stream_gdrive_ctr_chunks(file: &inode::File, storage: &gdrive::Storage) -> Pin<Box<dyn Stream<Item = Result<Bytes, Error>>>> {
    let _file = file.clone();
    let storage = storage.clone();

    Box::pin(
        #[try_stream]
        async move {
            let mut ctr_stream_bytes = 0;
            let pool = db::pgpool().await;
            let mut transaction = pool.begin().await?;
            let gdrive_ids: Vec<&str> = storage.gdrive_ids.iter().map(String::as_str).collect();
            let gdrive_files = GdriveFile::find_by_ids_in_order(&mut transaction, &gdrive_ids).await?;
            drop(transaction);
            for gdrive_file in gdrive_files {
                info!(id = &*gdrive_file.id, size = gdrive_file.size, "streaming gdrive file");
                let encrypted_stream = stream_gdrive_file(&gdrive_file, storage.gsuite_domain).await?;
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
                    let bytes = decrypted.into();
                    yield bytes;
                }
                // TODO: on EOF, make sure we got the expected number of bytes
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

/// Pinned boxed dyn Stream of bytes::Bytes
pub type ReadStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>>>>;

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
            drop(transaction);

            let whole_block_size = 65536;
            // Block size for all of our AES-128-GCM files
            let block_size = whole_block_size - 16;
            let aes_gcm_length = get_aes_gcm_length(file.size as u64, block_size);

            let mut gcm_stream_bytes = 0;
            for gdrive_file in gdrive_files {
                info!(id = &*gdrive_file.id, size = gdrive_file.size, "streaming gdrive file");
                let encrypted_stream = stream_gdrive_file(&gdrive_file, storage.gsuite_domain).await?;
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
                // This will be too large except for the last gdrive file in the sequence, but
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
                // TODO: on EOF, make sure we got the expected number of bytes
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

/// Return the content of a storage as a pinned boxed Stream on which caller can call `.into_async_read()`
async fn read_storage_(file: &inode::File, storage: &Storage) -> Result<ReadStream> {
    info!(id = file.id, "reading file");
    Ok(match storage {
        Storage::Inline(inline::Storage { content_zstd, .. }) => {
            let content = zstd::stream::decode_all(content_zstd.as_slice())?;
            ensure!(
                content.len() as i64 == file.size,
                "length of inline storage for file id={} is {} but file size is {}", file.id, content.len(), file.size
            );

            let mut bytes = BytesMut::new();
            bytes.put(&content[..]);
            Box::pin(stream::iter::<_>(vec![Ok(bytes.to_bytes())]))
        }
        Storage::Gdrive(gdrive_storage) => {
            stream_gdrive_files(&file, gdrive_storage)
        }
        Storage::InternetArchive(internetarchive::Storage { .. }) => {
            unimplemented!()
        }
    })
}

/// Return the content of a storage as a pinned boxed Stream on which caller can call `.into_async_read()`,
/// while also verifying the b3sum of the file.
pub async fn read_storage(file: &inode::File, storage: &Storage) -> Result<ReadStream> {
    let underlying_stream = read_storage_(file, storage).await?;
    let file_b3sum = file.b3sum;
    let b3sum = Arc::new(Mutex::new(blake3::Hasher::new()));
    let hashing_stream = Blake3HashingStream::new(underlying_stream, b3sum);
    let b3sum = hashing_stream.b3sum().clone();
    Ok(
        Box::pin(
            #[try_stream]
            async move {
                #[for_await]
                for frame in hashing_stream {
                    yield frame?;
                }
                if let Some(db_hash) = file_b3sum {
                    let computed_hash = blake3::Hasher::finalize(&b3sum.lock().clone());
                    ensure!(
                        computed_hash.as_bytes() == &db_hash,
                        "computed b3sum for content is {:?} but file has b3sum={:?}",
                        hex::encode(computed_hash.as_bytes()), hex::encode(db_hash)
                    );
                }
            }
        )
    )
}

/// Return the content of a file as a pinned boxed Stream on which caller can call `.into_async_read()`
pub async fn read(file_id: i64) -> Result<(ReadStream, inode::File)> {
    let pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;

    let mut files = inode::File::find_by_ids(&mut transaction, &[file_id]).await?;
    ensure!(files.len() == 1, "no such file with id={}", file_id);
    let file = files.pop().unwrap();

    if file.size == 0 {
        let bytes = Bytes::new();
        return Ok((Box::pin(stream::iter::<_>(vec![Ok(bytes)])), file));
    }

    let storages = get_storages(&mut transaction, &[file_id]).await?;
    drop(transaction);
    let stream = match storages.get(0) {
        Some(storage) => read_storage(&file, &storage).await?,
        None          => bail!("file with id={} has no storage", file_id)
    };
    Ok((stream, file))
}
