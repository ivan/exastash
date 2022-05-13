//! Functions to write content to storage

use num::ToPrimitive;
use rand::Rng;
use std::{collections::{HashMap, HashSet}, path::{PathBuf, Path}, sync::Arc};
use std::pin::Pin;
use std::cmp::min;
use std::fs::Metadata;
use std::sync::atomic::Ordering;
use chrono::{DateTime, Utc};
use anyhow::{anyhow, bail};
use futures::{ready, stream::{self, Stream, StreamExt, TryStreamExt}, task::{Context, Poll}};
use tracing::{info, warn};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use tokio::{fs, io::{AsyncRead, AsyncReadExt}};
use tokio_util::codec::{Encoder, FramedRead};
use crate::util::FixedReadSizeDecoder;
use crate::crypto::{GcmEncoder, gcm_create_key};
use crate::conceal_size::conceal_size;
use crate::db;
use crate::db::inode;
use crate::db::storage::{inline, gdrive::{self, file::GdriveFile}, fofs};
use crate::blake3::{Blake3HashingReader, b3sum_bytes};
use crate::storage_read::{get_access_tokens, get_aes_gcm_length};
use crate::gdrive::{create_gdrive_file, GdriveUploadError};
use crate::util;
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
    fn new(stream: S) -> GdriveHashingStream<S> {
        let crc32c = Arc::new(Mutex::new(0));
        let md5 = Arc::new(Mutex::new(Md5::new()));
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

/// Uploads a file to Google Drive and returns a `GdriveFile`.  You must commit
/// it to the database yourself.
///
/// `stream` is a `Stream` containing the file content to upload.
/// `size` is the length of the `Stream` and the resulting Google Drive file.
/// `owner_id` is the gdrive_owner for the file.
/// `domain_id` is the google_domain for the file.
/// `parent` is the Google Drive folder in which to create a file.
/// `filename` is the name of the file to create in Google Drive.
pub async fn create_gdrive_file_on_domain<S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>(
    stream: S,
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

    let gfs = GdriveHashingStream::new(stream);
    let crc32c = gfs.crc32c();
    let md5 = gfs.md5();
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

/// Takes an unencrypted AsyncRead and returns an AES-128-GCM encrypted stream,
/// suitable for storing in untrusted storage.
async fn encrypt_reader<A: AsyncRead + Send + Sync + 'static>(
    reader: A,
    block_size: usize,
    cipher_key: [u8; 16],
    padding_size: u64
) -> Result<Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>>> {
    // Re-chunk the stream to make sure each chunk is appropriately-sized for the GcmEncoder
    let rechunked = {
        let decoder = FixedReadSizeDecoder::new(block_size);
        FramedRead::new(reader, decoder)
    };

    let mut encoder = {
        let key = gcm_create_key(cipher_key).unwrap();
        GcmEncoder::new(block_size, key, 0)
    };

    let stream = rechunked.map_ok(move |bytes| -> Bytes {
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

async fn replace_gdrive_file_placement(old_placement: &gdrive::GdriveFilePlacement) -> Result<()> {
    let pool = db::pgpool().await;

    // Mark current parent as full
    let mut transaction = pool.begin().await?;
    info!("setting full = {} on gdrive_parent name = {:?}", true, &old_placement.parent);
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
pub async fn write_to_gdrive<A: AsyncRead + Send + Sync + 'static>(
    reader: A,
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
    transaction.commit().await?; // close read-only transaction

    let whole_block_size = 65536;
    let block_size = whole_block_size - 16;
    let encrypted_size = get_aes_gcm_length(file.size as u64, block_size);
    let gdrive_file_size = conceal_size(encrypted_size);
    let padding_size = gdrive_file_size - encrypted_size;
    let cipher_key = new_cipher_key();
    let efp = encrypt_reader(reader, block_size, cipher_key, padding_size).await?;

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

/// Like `zstd::stream::encode_all`, but async, and also ensuring that the
/// compressed data decodes to the input data.
pub async fn paranoid_zstd_encode_all(bytes: Vec<u8>, level: i32) -> Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        let content_zstd = zstd::stream::encode_all(bytes.as_slice(), level)?;
        let content = zstd::stream::decode_all(content_zstd.as_slice())?;
        if content != bytes {
            bail!("zstd-compressed data failed to round-trip back to input data");
        }
        Ok(content_zstd)
    }).await?
}

/// Descriptor indicating which storages should be used for a new file
#[derive(Debug, PartialEq, Eq)]
pub struct DesiredStorages {
    /// A list of fofs pile ids in which to store the file
    pub fofs: Vec<i32>,
    /// Whether to store inline in the database
    pub inline: bool,
    /// A list of google_domain ids in which to store the file
    pub gdrive: Vec<i16>,
}

impl DesiredStorages {
    /// How many storages we want to store to
    pub fn len(&self) -> usize {
        let mut total = 0;
        if self.inline {
            total += 1;
        }
        total += self.fofs.len();
        total += self.gdrive.len();
        total
    }

    /// Whether we lack any storages to store to
    pub fn is_empty(&self) -> bool {
        if self.inline || !self.fofs.is_empty() || !self.gdrive.is_empty() {
            return false;
        }
        true
    }
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


async fn make_readonly(path: impl AsRef<Path>) -> Result<()> {
    let mut permissions = tokio::fs::metadata(&path).await?.permissions();
    permissions.set_readonly(true);
    tokio::fs::set_permissions(path, permissions).await?;
    Ok(())
}

/// Add storages for a file and commit them to the database.
/// If a particular storage for a file already exists, it will be skipped.
/// If a b3sum is calculated and the file does not already have one in the database, fix it.
pub async fn add_storages<A: AsyncRead + Send + Sync + Unpin + 'static>(
    mut producer: impl FnMut() -> Result<A>,
    file: &inode::File,
    desired: &DesiredStorages,
) -> Result<()> {
    let mut last_hash = None;
    let pool = db::pgpool().await;

    if !desired.fofs.is_empty() {     
        let pile_ids = &desired.fofs;
        let mut transaction = pool.begin().await?;
        let piles: HashMap<i32, fofs::Pile> = fofs::Pile::find_by_ids(&mut transaction, pile_ids).await?
            .into_iter()
            .map(|pile| (pile.id, pile))
            .collect();
        for pile_id in pile_ids {
            if !piles.contains_key(pile_id) {
                bail!("while adding fofs storage, a fofs pile with id={} was not found", pile_id);
            }
        }
        let already_in_piles: HashSet<i32> = {
            let storages = fofs::StorageView::find_by_file_ids(&mut transaction, &[file.id]).await?;
            transaction.commit().await?; // close read-only transaction
            storages.iter().map(|storage| storage.pile_id).collect()
        };

        for pile in piles.values() {
            if already_in_piles.contains(&pile.id) {
                info!(file_id = file.id, file_size = file.size, pile = pile.id, "not storing file in fofs pile (already in this pile)");
                continue;
            }
            info!(file_id = file.id, file_size = file.size, pile = pile.id, "storing file in fofs pile");

            let my_hostname = util::get_hostname();

            let mut transaction = pool.begin().await?;
            let cells = fofs::Cell::find_by_pile_ids_and_fullness(&mut transaction, &[pile.id], false).await?;
            // We don't need more than one cell, so take the first
            let cell = match cells.into_iter().next() {
                Some(cell) => cell,
                None => fofs::NewCell { pile_id: pile.id }.create(&mut transaction).await?
            };
            transaction.commit().await?;

            if pile.hostname == my_hostname {
                let cell_dir = format!("{}/{}/{}", pile.path, pile.id, cell.id);
                std::fs::create_dir_all(&cell_dir)?;

                let fname = format!("{}/{}", cell_dir, file.id);

                // Rarely, we might have a fofs file that was never recorded in the database.
                // Remove it before overwriting, because it might be read-only.
                let result = tokio::fs::remove_file(&fname).await;
                if result.is_ok() {
                    warn!("removed existing fofs file {:?}", fname);
                }

                let mut local_file = tokio::fs::File::create(&fname).await?;
                let mut reader = producer()?;
                tokio::io::copy(&mut reader, &mut local_file).await?;
                make_readonly(&fname).await?;

                let mut set_cell_full = false;
                let random: f32 = rand::thread_rng().gen_range(0.0..1.0);
                let mut files_in_cell = -1;
                if random < pile.fullness_check_ratio.to_f32().expect("failed to convert fullness_check_ratio to f32") {
                    files_in_cell = std::fs::read_dir(cell_dir)?.count() as i32;
                    if files_in_cell >= pile.files_per_cell {
                        set_cell_full = true;
                    }
                }

                let mut transaction = pool.begin().await?;
                fofs::Storage { file_id: file.id, cell_id: cell.id }.create(&mut transaction).await?;
                if set_cell_full {
                    info!(cell_id = cell.id, files_per_cell = pile.files_per_cell, files_in_cell = files_in_cell, "marking fofs cell as full");
                    fofs::Cell::set_full(&mut transaction, cell.id, true).await?;
                }
                transaction.commit().await?;
            } else {
                unimplemented!("uploading to another machine");
            }

            // TODO: if file is already available in some other storage, instead of POSTing the file over,
            // call add-storages on that machine instead, so that we don't waste our own bandwidth
            // transferring to that machine
        }
    }

    // We don't check if it already exists first because maybe_create is a no-op in that case
    if desired.inline {
        info!(file_id = file.id, file_size = file.size, "storing file inline");

        let mut reader = producer()?;
        let mut content = vec![];
        reader.read_to_end(&mut content).await?;
        last_hash = Some(b3sum_bytes(&content));
        if content.len() as i64 != file.size {
            bail!("while adding inline storage, read {} bytes from file but file has size={}", content.len(), file.size);
        }
        if let Some(file_hash) = file.b3sum {
            if last_hash.unwrap() != file_hash {
                bail!("while adding inline storage, content had b3sum={:?} but file has b3sum={:?}", last_hash.unwrap(), file_hash);
            }
        }
        let compression_level = 19; // levels > 19 use a lot more memory to decompress
        let content_zstd = paranoid_zstd_encode_all(content, compression_level).await?;

        let mut transaction = pool.begin().await?;
        inline::Storage { file_id: file.id, content_zstd }.maybe_create(&mut transaction).await?;
        transaction.commit().await?;
    }

    if !desired.gdrive.is_empty() {
        let already_on_domains: HashSet<i16> = {
            let mut transaction = pool.begin().await?;
            let storages = gdrive::Storage::find_by_file_ids(&mut transaction, &[file.id]).await?;
            transaction.commit().await?; // close read-only transaction
            storages.iter().map(|storage| storage.google_domain).collect()
        };

        for domain in &desired.gdrive {
            if already_on_domains.contains(domain) {
                info!(file_id = file.id, file_size = file.size, domain = domain, "not storing file in gdrive (already in this domain)");
                continue;
            }
            info!(file_id = file.id, file_size = file.size, domain = domain, "storing file in gdrive domain");

            let reader = producer()?;
            let counting_reader = util::ByteCountingReader::new(reader);
            let length_arc = counting_reader.length();
            let hashing_reader = Blake3HashingReader::new(counting_reader);
            let b3sum = hashing_reader.b3sum();

            let (gdrive_file, storage) = write_to_gdrive(hashing_reader, file, *domain).await?;
            let read_length = length_arc.load(Ordering::SeqCst);
            if read_length != file.size as u64 {
                bail!("while adding gdrive storage, read {} bytes from file but file has size={}", read_length, file.size);
            }
            let hash_this_upload = blake3::Hasher::finalize(&b3sum.lock().clone());
            if let Some(file_hash) = file.b3sum {
                if hash_this_upload != file_hash {
                    bail!("while adding gdrive storage, content had b3sum={:?} but file has b3sum={:?}", hash_this_upload, file_hash);
                }
            }
            if let Some(h) = last_hash {
                if hash_this_upload != h {
                    bail!("blake3 hash of local file changed during upload into \
                           multiple storages, was={:?} now={:?}", h, hash_this_upload);
                }
            }
            last_hash = Some(hash_this_upload);

            let mut transaction = pool.begin().await?;
            gdrive_file.create(&mut transaction).await?;
            storage.create(&mut transaction).await?;
            transaction.commit().await?;
        }
    }

    if let (None, Some(h)) = (file.b3sum, last_hash) {
        let mut transaction = pool.begin().await?;
        inode::File::set_b3sum(&mut transaction, file.id, h.as_bytes()).await?;
        transaction.commit().await?;
    }

    Ok(())
}

/// Return `count` number of open `tokio::fs::File`s for a path.
pub async fn readers_for_file(path: PathBuf, count: usize) -> Result<Vec<tokio::fs::File>> {
    let mut readers = Vec::with_capacity(count);
    for _ in 0..count {
        let reader = fs::File::open(path.clone()).await?;
        readers.push(reader);
    }
    Ok(readers)
}

/// Create a new stash file based on a local file, write storage, return the new file id
pub async fn create_stash_file_from_local_file(path: String, metadata: &RelevantFileMetadata, desired: &DesiredStorages) -> Result<i64> {
    if metadata.size > 0 && desired.len() == 0 {
        bail!("a file with size > 0 needs storage, but no storage was specified");
    }

    let pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;
    let birth = inode::Birth::here_and_now();
    let file = inode::NewFile {
        mtime: metadata.mtime,
        birth,
        size: metadata.size,
        executable: metadata.executable,
        b3sum: None,
    }.create(&mut transaction).await?;
    transaction.commit().await?;

    let mut readers = readers_for_file(path.into(), desired.len()).await?;
    let producer = move || {
        readers.pop().ok_or_else(|| anyhow!("no readers left"))
    };
    add_storages(producer, &file, desired).await?;

    Ok(file.id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[expect(clippy::needless_collect)]
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
