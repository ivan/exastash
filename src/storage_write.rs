//! Functions to write content to storage

use rand::Rng;
use std::sync::Arc;
use std::pin::Pin;
use std::cmp::min;
use std::convert::{TryFrom, TryInto};
use std::fs::Metadata;
use std::path::PathBuf;
use chrono::{DateTime, Utc};
use anyhow::{anyhow, bail};
use futures::{
    ready,
    stream::{self, Stream, StreamExt, TryStreamExt},
    task::{Context, Poll},
};
use tracing::info;
use anyhow::{ensure, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::fs;
use tokio_util::codec::{Encoder, FramedRead};
use crate::crypto::{FixedReadSizeDecoder, GcmEncoder, gcm_create_key};
use crate::conceal_size::conceal_size;
use crate::db;
use crate::db::inode;
use crate::db::storage::{inline, gdrive::{self, file::GdriveFile}};
use crate::blake3::{Blake3HashingStream, b3sum_bytes};
use crate::storage_read::{get_access_tokens, get_aes_gcm_length};
use crate::gdrive::{create_gdrive_file, GdriveUploadError};
use crate::util::{self, elide};
use custom_debug_derive::Debug as CustomDebug;
use pin_project::pin_project;
use parking_lot::Mutex;
use md5::{Md5, Digest};

#[pin_project]
struct GdriveHashingStream<S> {
    #[pin]
    stream: S,
    // We use Arc<Mutex<...>> here because reqwest::Body::wrap_stream wants to take
    // ownership of a Stream, but we still need to read out the crc32c and md5
    // after reqwest is done with the stream.
    crc32c: Arc<Mutex<u32>>,
    md5: Arc<Mutex<Md5>>,
}

impl<S> GdriveHashingStream<S> {
    fn new(stream: S, crc32c: Arc<Mutex<u32>>, md5: Arc<Mutex<Md5>>) -> GdriveHashingStream<S> {
        GdriveHashingStream { stream, crc32c, md5 }
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

impl<S, O, E> Stream for GdriveHashingStream<S>
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

/// A provider of a stream that starts at some byte offset
#[async_trait]
pub trait StreamAtOffset: Send + Sync {
    /// Get a `Stream` of `std::io::Result<Bytes>` starting at byte offset `offset`.
    async fn stream(&mut self, offset: usize) -> Result<Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>>>;
}

pub(crate) struct GdriveFileProducer<SAO: StreamAtOffset> {
    efs: SAO,
    crc32c: Arc<Mutex<u32>>,
    md5: Arc<Mutex<Md5>>,
}

impl<SAO: StreamAtOffset> GdriveFileProducer<SAO> {
    fn new(efs: SAO) -> Self {
        GdriveFileProducer {
            efs,
            crc32c: Arc::new(Mutex::new(0)),
            md5: Arc::new(Mutex::new(Md5::new())),
        }
    }

    #[inline]
    pub(crate) fn hashes(&self) -> (Arc<Mutex<u32>>, Arc<Mutex<Md5>>) {
        (self.crc32c.clone(), self.md5.clone())
    }
}

#[async_trait]
impl<SAO: StreamAtOffset> StreamAtOffset for GdriveFileProducer<SAO> {
    async fn stream(&mut self, offset: usize) -> Result<Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>>> {
        // TODO: support non-0 offset
        assert_eq!(offset, 0);

        let encrypted_stream = self.efs.stream(offset).await?;
        let stream = GdriveHashingStream::new(encrypted_stream, self.crc32c.clone(), self.md5.clone());
        Ok(Box::pin(stream))
    }
}

/// Uploads a file to Google Drive and returns a `GdriveFile`.  You must commit
/// it to the database yourself.
///
/// `producer` is a `StreamAtOffset` where `.stream(offset)` returns a `Stream`
///  containing the content to upload.
/// `size` is the length of the `Stream` and the resulting Google Drive file.
/// `owner_id` is the gdrive_owner for the file.
/// `domain_id` is the google_domain for the file.
/// `parent` is the Google Drive folder in which to create a file.
/// `filename` is the name of the file to create in Google Drive.
pub async fn create_gdrive_file_on_domain<SAO: StreamAtOffset>(
    producer: SAO,
    size: u64,
    domain_id: i16,
    owner_id: i32,
    parent: &str,
    filename: &str
) -> Result<GdriveFile> {
    let access_token_fn = async || -> Result<String> {
        let mut access_tokens = get_access_tokens(Some(owner_id), domain_id).await?;
        if access_tokens.is_empty() {
            bail!("no access tokens were available for domain_id={} owner_id={}", domain_id, owner_id);
        }
        let access_token = access_tokens.pop().unwrap();
        Ok(access_token)
    };

    let gfs = GdriveFileProducer::new(producer);
    let (crc32c, md5) = gfs.hashes();
    let response = create_gdrive_file(gfs, access_token_fn, size, parent, filename).await?;

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

/// Produces a stream of AES-128-GCM encrypted and authenticated file contents,
/// suitable for storing in untrusted storage.
#[derive(CustomDebug)]
pub struct EncryptedFileProducer {
    lfp: LocalFileProducer,
    block_size: usize,
    #[debug(with = "elide")]
    cipher_key: [u8; 16],
    padding_size: u64,
}

impl EncryptedFileProducer {
    fn new(lfp: LocalFileProducer, block_size: usize, cipher_key: [u8; 16], padding_size: u64) -> Self {
        EncryptedFileProducer { lfp, block_size, cipher_key, padding_size }
    }
}

#[async_trait]
impl StreamAtOffset for EncryptedFileProducer {
    async fn stream(&mut self, offset: usize) -> Result<Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>>> {
        // TODO: support non-0 offset
        assert_eq!(offset, 0);

        let read_size = self.block_size;
        self.lfp.set_read_size(read_size);
        let unencrypted = self.lfp.stream(offset).await?;

        let key = gcm_create_key(self.cipher_key).unwrap();
        let mut encoder = GcmEncoder::new(self.block_size, key, 0);

        let block_size = self.block_size;
        let padding_size = self.padding_size;
        let stream = unencrypted.map_ok(move |bytes| -> Bytes {
            assert!(bytes.len() <= block_size, "single read from file must be shorter or same length as block size {}, was {}", block_size, bytes.len());
            let mut out = BytesMut::new();
            encoder.encode(bytes, &mut out).unwrap();
            out.into()
        }).chain(
            stream::iter(RandomPadding::new(padding_size))
            .map(Ok)
        );
        Ok(Box::pin(stream))
    }
}

async fn replace_gdrive_file_placement(old_placement: &gdrive::GdriveFilePlacement) -> Result<()> {
    let pool = db::pgpool().await;

    // Mark current parent as full
    let mut transaction = pool.begin().await?;
    gdrive::GdriveParent::set_full(&mut transaction, &old_placement.parent, true).await?;
    transaction.commit().await?;

    let mut transaction = pool.begin().await?;

    // Select the current placement and lock the row
    let found_placement = old_placement.find_self_and_lock(&mut transaction).await?;
    if found_placement.is_none() {
        info!("the gdrive_file_placement we wanted to replace is missing, maybe it was replaced by another process?");
        return Ok(());
    }
    // TODO: if someone else just locked it, ignore and return

    // Find a non-full parent
    let new_parent = gdrive::GdriveParent::find_first_non_full(&mut transaction).await?
        .ok_or_else(|| {
            anyhow!("cannot replace placement {:?} because there are no non-full gdrive_parents", old_placement)
        })?;

    // Remove the original placement
    old_placement.remove(&mut transaction).await?;

    // Add the new placement
    let new_placement = gdrive::GdriveFilePlacement {
        domain: old_placement.domain,
        owner: old_placement.owner,
        parent: new_parent.name
    };
    new_placement.create(&mut transaction).await?;

    info!("about to replace {:?} with {:?}", old_placement, new_placement);
    transaction.commit().await?;
    info!("successfully replaced gdrive_file_placement");

    Ok(())
}

/// Write the content of a file to a google domain.
/// Returns a `(GdriveFile, gdrive::Storage)` on which caller must `.create()` to commit.
/// If the gdrive parent into which we are uploading is full, replaces the parent in gdrive_file_placement,
/// then returns the original error.
pub async fn write_to_gdrive(
    lfp: LocalFileProducer,
    file: &inode::File,
    domain_id: i16
) -> Result<(GdriveFile, gdrive::Storage)> {
    let pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;

    let mut placements = gdrive::GdriveFilePlacement::find_by_domain(&mut transaction, domain_id, Some(1)).await?;
    if placements.is_empty() {
        bail!("database has no gdrive_file_placement for domain={}", domain_id);
    }
    let placement = placements.pop().unwrap();
    let parent_name = &placement.parent;
    let parent = gdrive::GdriveParent::find_by_name(&mut transaction, parent_name).await?.unwrap();
    // Don't hold the transaction during the upload.
    drop(transaction);

    let whole_block_size = 65536;
    let block_size = whole_block_size - 16;
    let encrypted_size = get_aes_gcm_length(file.size as u64, block_size);
    let gdrive_file_size = conceal_size(encrypted_size);
    let padding_size = gdrive_file_size - encrypted_size;

    let cipher_key = new_cipher_key();
    let efp = EncryptedFileProducer::new(lfp, block_size, cipher_key, padding_size);

    let filename = new_chunk_filename();
    // While terastash uploaded large files as multi-chunk files,
    // exastash currently uploads all files as one chunk.
    let result = create_gdrive_file_on_domain(efp, gdrive_file_size, domain_id, placement.owner, &parent.parent, &filename).await;

    // If Google indicates the parent is full, replace the parent for the caller,
    // because they may want to try again.
    if let Err(err) = &result {
        let err = err.downcast_ref::<GdriveUploadError>();
        if let Some(GdriveUploadError::ParentIsFull(_)) = err {
            info!("Google Drive indicates that parent in placement {:?} is full", placement);
            replace_gdrive_file_placement(&placement).await?;
        }
    }

    let gdrive_file = result?;

    let storage = gdrive::Storage {
        file_id: file.id,
        google_domain: domain_id,
        cipher: gdrive::Cipher::Aes128Gcm,
        cipher_key,
        gdrive_ids: vec![gdrive_file.id.clone()],
    };

    Ok((gdrive_file, storage))
}

/// Like `zstd::stream::encode_all`, but first check that the compressed data
/// decodes to the input data.
pub fn paranoid_zstd_encode_all(bytes: &[u8], level: i32) -> Result<Vec<u8>> {
    let content_zstd = zstd::stream::encode_all(bytes, level)?;
    let content = zstd::stream::decode_all(content_zstd.as_slice())?;
    if content != bytes {
        bail!("zstd-compressed data failed to round-trip back to input data");
    }
    Ok(content_zstd)
}

/// Descriptor indicating which storages should be used for a new file
#[derive(Debug, PartialEq, Eq)]
pub struct DesiredStorage {
    /// Whether to store inline in the database
    pub inline: bool,
    /// A list of google_domain ids in which to store the file
    pub gdrive: Vec<i16>,
}

/// Local file metadata that can be stored in exastash
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RelevantFileMetadata {
    /// Size of the local file in bytes
    pub size: i64,
    /// The mtime of the local file, precision only up to microseconds
    pub mtime: DateTime<Utc>,
    /// Whether the local file is executable
    pub executable: bool,
}

impl TryFrom<&Metadata> for RelevantFileMetadata {
    type Error = anyhow::Error;

    fn try_from(attr: &Metadata) -> Result<RelevantFileMetadata> {
        use std::os::unix::fs::PermissionsExt;

        // Remove the nanoseconds so that a RelevantFileMetadata's mtime
        // can be compared directly with a timestamptz from PostgreSQL.
        let mtime = util::without_nanos(attr.modified()?.into());
        let size = attr.len() as i64;
        let permissions = attr.permissions();
        let executable = permissions.mode() & 0o100 != 0;
        Ok(RelevantFileMetadata { size, mtime, executable })
    }
}

impl TryFrom<Metadata> for RelevantFileMetadata {
    type Error = anyhow::Error;

    fn try_from(attr: Metadata) -> Result<RelevantFileMetadata> {
        (&attr).try_into()
    }
}

/// Provide a Stream for a local file and compute a b3sum of the complete file contents
#[derive(Debug, Clone)]
pub struct LocalFileProducer {
    path: PathBuf,
    read_size: usize,
    b3sum: Arc<Mutex<blake3::Hasher>>,
}

impl LocalFileProducer {
    /// Create a `LocalFileProducer` that can stream a local file
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        LocalFileProducer {
            path: path.into(),
            read_size: 0,
            b3sum: Arc::new(Mutex::new(blake3::Hasher::new())),
        }
    }

    /// You must call this before .stream(...) to set the max length of the Bytes yielded by the stream
    pub fn set_read_size(&mut self, read_size: usize) {
        self.read_size = read_size;
    }

    /// Returns an `Arc` which can be derefenced to get the b3sum of the data streamed so far
    #[inline]
    fn b3sum(&self) -> Arc<Mutex<blake3::Hasher>> {
        self.b3sum.clone()
    }
}

#[async_trait]
impl StreamAtOffset for LocalFileProducer {
    async fn stream(&mut self, offset: usize) -> Result<Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>>> {
        let decoder = FixedReadSizeDecoder::new(self.read_size);
        // TODO: support non-0 offset if we implement upload retries
        assert_eq!(offset, 0);
        let async_read = fs::File::open(self.path.clone()).await?;
        let file_stream = FramedRead::new(async_read, decoder);
        let hashing_stream = Blake3HashingStream::new(file_stream, self.b3sum());
        Ok(Box::pin(hashing_stream))
    }
}

/// Write a file to storage and return the new file id
pub async fn write(path: String, metadata: &RelevantFileMetadata, desired_storage: &DesiredStorage) -> Result<i64> {
    let pool = db::pgpool().await;

    // We don't want to hold a transaction open as we upload a file, so we get a new id for a
    // file here but don't create it until later.
    let mut transaction = pool.begin().await?;
    let next_file_id = inode::File::next_id(&mut transaction).await?;
    drop(transaction);

    let birth = inode::Birth::here_and_now();
    let mut file = inode::File {
        id: next_file_id,
        mtime: metadata.mtime,
        birth,
        size: metadata.size,
        executable: metadata.executable,
        b3sum: None,
    };

    if metadata.size > 0 && !desired_storage.inline && desired_storage.gdrive.is_empty() {
        bail!("a file with size > 0 needs storage, but no storage was specified");
    }

    let mut inline_storages_to_commit: Vec<inline::Storage> = vec![];
    let mut gdrive_files_to_commit: Vec<GdriveFile> = vec![];
    let mut gdrive_storages_to_commit: Vec<gdrive::Storage> = vec![];

    let mut hash = None;

    if desired_storage.inline {
        let content = fs::read(path.clone()).await?;
        hash = Some(b3sum_bytes(&content));
        ensure!(
            content.len() as i64 == metadata.size,
            "read {} bytes from file but file size was read as {}", content.len(), file.size
        );
        let compression_level = 22;
        let content_zstd = paranoid_zstd_encode_all(content.as_slice(), compression_level)?;

        let storage = inline::Storage { file_id: file.id, content_zstd };
        inline_storages_to_commit.push(storage);
    }

    if !desired_storage.gdrive.is_empty() {
        for domain in &desired_storage.gdrive {
            let lfp = LocalFileProducer::new(path.clone());
            let b3sum = lfp.b3sum();
            let (gdrive_file, storage) = write_to_gdrive(lfp, &file, *domain).await?;
            let hash_this_upload = blake3::Hasher::finalize(&b3sum.lock().clone());
            if let Some(h) = hash {
                if hash_this_upload != h {
                    bail!("blake3 hash of local file changed during upload into \
                           multiple storages, was={:?} now={:?}", h, hash_this_upload);
                }
            }
            hash = Some(hash_this_upload);
            gdrive_files_to_commit.push(gdrive_file);
            gdrive_storages_to_commit.push(storage);
        }
    }
    file.b3sum = Some(*hash.unwrap().as_bytes());

    let mut transaction = pool.begin().await?;
    file.create(&mut transaction).await?;
    for storage in inline_storages_to_commit {
        storage.create(&mut transaction).await?;
    }
    for gdrive_file in gdrive_files_to_commit {
        gdrive_file.create(&mut transaction).await?;
    }
    for storage in gdrive_storages_to_commit {
        storage.create(&mut transaction).await?;
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
