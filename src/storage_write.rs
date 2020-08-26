//! Functions to write content to storage

use rand::Rng;
use std::sync::Arc;
use std::pin::Pin;
use std::cmp::min;
use std::os::unix::fs::PermissionsExt;
use chrono::Utc;
use anyhow::bail;
use futures::{
    ready,
    stream::{self, Stream, StreamExt, TryStreamExt},
    task::{Context, Poll},
    future::FutureExt,
};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use tokio::fs;
use tokio_util::codec::Encoder;
use crate::db::inode;
use crate::db::storage;
use crate::db::storage::gdrive::{self, file::GdriveFile};
use crate::storage_read::{get_access_tokens, get_aes_gcm_length};
use crate::gdrive::create_gdrive_file;
use crate::crypto::{GcmEncoder, gcm_create_key};
use crate::conceal_size::conceal_size;
use sqlx::{Postgres, Transaction};
use pin_project::pin_project;
use parking_lot::Mutex;
use md5::{Md5, Digest};

#[pin_project]
struct StreamWithHashing<S> {
    #[pin]
    stream: S,
    // We use Arc<Mutex<...>> here because reqwest::Body::wrap_stream wants to take
    // ownership of a Stream, but we still need to read out the crc32c and md5
    // after reqwest is done with the stream.
    crc32c: Arc<Mutex<u32>>,
    md5: Arc<Mutex<Md5>>,
}

impl<S> StreamWithHashing<S> {
    fn new(stream: S) -> StreamWithHashing<S> {
        StreamWithHashing {
            stream,
            crc32c: Arc::new(Mutex::new(0)),
            md5: Arc::new(Mutex::new(Md5::new())),
        }
    }

    /// Returns an `Arc` which can be derefenced to get the crc32c of the data streamed so far
    #[inline]
    fn crc32c(&self) -> Arc<Mutex<u32>> {
        self.crc32c.clone()
    }

    /// Returns an `Arc` which can be derefenced to get the md5 of the data streamed so far
    #[inline]
    fn md5(&self) -> Arc<Mutex<Md5>> {
        self.md5.clone()
    }
}

impl<S, O, E> Stream for StreamWithHashing<S>
where
    O: AsRef<[u8]>,
    E: std::error::Error,
    S: Stream<Item = Result<O, E>>,
{
    type Item = Result<O, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let crc32c = self.crc32c();
        let md5 = self.md5();
        if let Some(res) = ready!(self.project().stream.poll_next(cx)) {
            if let Ok(bytes) = &res {
                let mut crc32c_m = crc32c.lock();
                *crc32c_m = crc32c::crc32c_append(*crc32c_m, bytes.as_ref());
                md5.lock().update(bytes);
            }
            Poll::Ready(Some(res))
        } else {
            Poll::Ready(None)
        }
    }
}

/// Uploads a file to Google Drive and returns a `GdriveFile`.  You must commit
/// it to the database yourself.
///
/// `file_stream` is a `Stream` containing the content to upload
/// `size` is the length of the `Stream` and the resulting Google Drive file
/// `owner_id` is the gdrive_owner for the file
/// `domain_id` is the gsuite_domain for the file
/// `parent` is the Google Drive folder in which to create a file
/// `filename` is the name of the file to create in Google Drive
pub async fn create_gdrive_file_on_domain<S>(file_stream_fn: impl Fn(u64) -> S, size: u64, domain_id: i16, owner_id: i32, parent: &str, filename: &str) -> Result<GdriveFile>
where
    S: Stream<Item=Result<Bytes, std::io::Error>> + Send + Sync + 'static
{
    let mut crc32c = None;
    let mut md5 = None;

    let hashing_stream_fn = |offset| {
        // TODO: support non-0 offset (rehash the part of the file already uploaded?)
        assert_eq!(offset, 0);
        let stream = StreamWithHashing::new(file_stream_fn(offset));
        crc32c = Some(stream.crc32c());
        md5 = Some(stream.md5());
        stream
    };

    let access_token_fn = async || -> Result<String> {
        let mut access_tokens = get_access_tokens(Some(owner_id), domain_id).await?;
        if access_tokens.is_empty() {
            bail!("no access tokens were available for domain_id={} owner_id={}", domain_id, owner_id);
        }
        let access_token = access_tokens.pop().unwrap();
        Ok(access_token)
    };

    let response = create_gdrive_file(hashing_stream_fn, access_token_fn, size, parent, filename).await?;
    
    // Assume they were set at least once by the closure
    let md5 = md5.unwrap();
    let crc32c = crc32c.unwrap();

    let md5 = md5.lock().clone().finalize();
    if response.md5 != md5.as_slice() {
        bail!("expected Google to create file with md5={:?}, got {:?}", md5, response.md5);
    }

    let crc32c_m = crc32c.lock();
    Ok(GdriveFile {
        id: response.id,
        owner_id: Some(owner_id),
        md5: response.md5,
        crc32c: *crc32c_m,
        size: size as i64,
        last_probed: None,
    })
}

// Match terastash's filenames
#[inline]
fn new_chunk_filename() -> String {
    let now = Utc::now();
    let secs = now.timestamp();
    let nanos = now.timestamp_subsec_nanos();
    let random = rand::thread_rng().gen::<[u8; 16]>();
    format!("{secs}-{nanos}-{}", hex::encode(random))
}

#[inline]
fn new_cipher_key() -> [u8; 16] {
    rand::thread_rng().gen::<[u8; 16]>()
}

struct RandomPadding {
    bytes_left: u64,
}

impl RandomPadding {
    fn new(bytes: u64) -> Self {
        Self { bytes_left: bytes }
    }
}

impl Iterator for RandomPadding {
    type Item = Bytes;

    fn next(&mut self) -> Option<Bytes> {
        if self.bytes_left == 0 {
            return None
        }
        let count = min(65536, self.bytes_left);
        self.bytes_left -= count;
        let mut rng = rand::thread_rng();
        let bytes: Vec<u8> = (0..count).map(|_| { rng.gen::<u8>() }).collect();
        Some(Bytes::from(bytes))
    }
}

/// Write the content of a file to a G Suite domain.
/// Caller must commit the transaction themselves.
pub async fn write_to_gdrive<S>(
    transaction: &mut Transaction<'_, Postgres>,
    file_stream_fn: impl Fn(u64) -> S,
    file: &inode::File,
    domain_id: i16
) -> Result<()>
where
    S: Stream<Item=Result<Vec<u8>, std::io::Error>> + Send + Sync + 'static
{
    let mut placements = gdrive::GdriveFilePlacement::find_by_domain(transaction, domain_id, Some(1)).await?;
    if placements.is_empty() {
        bail!("database has no gdrive_file_placement for domain={}", domain_id);
    }
    let placement = placements.pop().unwrap();
    let parent_name = &placement.parent;
    let parent = gdrive::GdriveParent::find_by_name(transaction, parent_name).await?.unwrap();

    let whole_block_size = 65536;
    let block_size = whole_block_size - 16;
    let encrypted_size = get_aes_gcm_length(file.size as u64, block_size);
    let gdrive_file_size = conceal_size(encrypted_size);
    let padding_size = gdrive_file_size - encrypted_size;

    let cipher_key = new_cipher_key();
    let encrypted_stream_fn = |offset| {
        // TODO: support non-0 offset
        assert_eq!(offset, 0);
        let unencrypted = file_stream_fn(offset);

        let key = gcm_create_key(cipher_key).unwrap();
        let mut encoder = GcmEncoder::new(block_size, key, 0);

        unencrypted.map_ok(move |bytes| -> Bytes {
            let mut out = BytesMut::new();
            encoder.encode(bytes.into(), &mut out).unwrap();
            out.into()
        }).chain(
            stream::iter(RandomPadding::new(padding_size))
            .map(Ok)
        )
    };

    let filename = new_chunk_filename();
    let gdrive_file =
        create_gdrive_file_on_domain(encrypted_stream_fn, gdrive_file_size, domain_id, placement.owner, &parent.parent, &filename).await?
        .create(transaction).await?;
    // terastash uploaded large files as multi-chunk files; exastash currently uploads all files as one chunk
    let gdrive_ids = vec![gdrive_file.id.clone()];

    gdrive::Storage {
        file_id: file.id,
        gsuite_domain: domain_id,
        cipher: gdrive::Cipher::Aes128Gcm,
        cipher_key,
        gdrive_ids,
    }.create(transaction).await?;

    Ok(())
}

/// Write a file to storage and return the new file id
pub async fn write(mut transaction: Transaction<'_, Postgres>, path: String, store_inline: bool, store_gdrive: &[i16]) -> Result<i64> {
    let attr = fs::metadata(&path).await?;
    let mtime = attr.modified()?.into();
    let birth = inode::Birth::here_and_now();
    let size = attr.len();
    let permissions = attr.permissions();
    let executable = permissions.mode() & 0o111 != 0;
    let file = inode::NewFile { mtime, birth, size: size as i64, executable }.create(&mut transaction).await?;
    if size > 0 && !store_inline && store_gdrive.is_empty() {
        bail!("a file with size > 0 needs storage, please specify a --store- option");
    }
    if store_inline {
        let content = fs::read(path.clone()).await?;
        let compression_level = 22;
        let content_zstd = zstd::stream::encode_all(content.as_slice(), compression_level)?;
        storage::inline::Storage { file_id: file.id, content_zstd }.create(&mut transaction).await?;
    }
    if !store_gdrive.is_empty() {
        let file_stream_fn = |offset| {
            // TODO: support non-0 offset if we implement upload retries
            assert_eq!(offset, 0);
            fs::read(path.clone()).into_stream()
        };
        for domain in store_gdrive {
            write_to_gdrive(&mut transaction, file_stream_fn, &file, *domain).await?;
        }
    }
    transaction.commit().await?;
    Ok(file.id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_padding() {
        let out: Vec<Bytes> = RandomPadding::new(0).collect();
        assert_eq!(out.len(), 0);

        let out: Vec<Bytes> = RandomPadding::new(1).collect();
        assert_eq!(out.len(), 1);

        let out: Vec<Bytes> = RandomPadding::new(65536).collect();
        assert_eq!(out.len(), 1);

        // Try to ensure data is actually random
        let out2: Vec<Bytes> = RandomPadding::new(65536).collect();
        assert_ne!(out2, out);

        let out: Vec<Bytes> = RandomPadding::new(65536 + 1).collect();
        assert_eq!(out.len(), 2);

        let out: Vec<Bytes> = RandomPadding::new(65536 * 2).collect();
        assert_eq!(out.len(), 2);
    }
}
