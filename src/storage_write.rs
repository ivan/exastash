//! Functions to write content to storage

use std::sync::Arc;
use std::pin::Pin;
use anyhow::bail;
use futures::{ready, stream::Stream, task::{Context, Poll}};
use anyhow::Result;
use crate::db::storage::gdrive::file::GdriveFile;
use crate::storage_read::get_access_tokens;
use crate::gdrive::create_gdrive_file;
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
    S: Stream<Item=Result<Vec<u8>, std::io::Error>> + Send + Sync + 'static
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
        Ok(access_token.into())
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
