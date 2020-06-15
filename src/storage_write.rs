//! Functions to write content to storage

use std::sync::Arc;
use std::pin::Pin;
use std::path::PathBuf;
use anyhow::{anyhow, bail};
use futures::{ready, stream::Stream, task::{Context, Poll}};
use anyhow::Result;
use tokio::fs;
use serde::Deserialize;
use serde_json::json;
use crate::db::storage::gdrive::file::GdriveFile;
use crate::storage_read::get_access_tokens;
use futures::future::FutureExt;
use pin_project::pin_project;
use parking_lot::Mutex;
use serde_hex::{SerHex, Strict};
use md5::{Md5, Digest};

#[pin_project]
struct StreamWithHashing<S> {
    #[pin]
    stream: S,
    // We use Arc<Mutex<...> here because reqwest::Body::wrap_stream wants to take
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
    fn crc32c(&self) -> Arc<Mutex<u32>> {
        self.crc32c.clone()
    }

    /// Returns an `Arc` which can be derefenced to get the md5 of the data streamed so far
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
        let crc32c = self.crc32c.clone();
        let md5 = self.md5.clone();
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

#[derive(Debug, Deserialize)]
struct GoogleCreateResponse {
    kind: String,
    id: String,
    name: String,
    parents: Vec<String>,
    size: String,
    #[serde(rename = "md5Checksum")]
    #[serde(with = "SerHex::<Strict>")]
    md5: [u8; 16],
}

/// Uploads a file to Google Drive and returns a `GdriveFile`.  You must commit
/// it to the database yourself.
///
/// `path` is a `Path` to some local file contents to upload
/// `owner_id` is the gdrive_owner for the file
/// `domain_id` is the gsuite_domain for the file
/// `parent` is the Google Drive folder in which to create a file
/// `filename` is the name of the file to create in Google Drive
pub async fn create_gdrive_file(path: PathBuf, domain_id: i16, owner_id: i32, parent: &str, filename: &str) -> Result<GdriveFile> {
    let attr = fs::metadata(&path).await?;
    let size = attr.len();
    let metadata = json!({
        "name": filename,
        "parents": [parent],
        "mimeType": "application/octet-stream",
    });

    let client = reqwest::Client::new();

    let mut access_tokens = get_access_tokens(Some(owner_id), domain_id).await?;
    if access_tokens.is_empty() {
        bail!("no access tokens were available for domain_id={} owner_id={}", domain_id, owner_id);
    }
    let access_token = access_tokens.pop().unwrap();

    // https://developers.google.com/drive/api/v3/manage-uploads#resumable
    // https://developers.google.com/drive/api/v3/reference/files/create
    // Note: use fields=* to get all fields in response
    let initial_url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true&fields=kind,id,name,parents,size,md5Checksum";
    let initial_response = client
        .post(initial_url)
        .json(&metadata)
        .header("Authorization", format!("Bearer {}", access_token))
        .header("X-Upload-Content-Type", "application/octet-stream")
        .header("X-Upload-Content-Length", size)
        .send()
        .await?;

    let upload_url = initial_response.headers().get("Location")
        .ok_or_else(|| anyhow!("did not get Location header in response to initial upload request"))?
        .to_str()?;
    // let stream = stream_calculate_crc32c(&mut crc32c, fs::File::open(path).await?);
    let file_stream = fs::read(path).into_stream();
    let hashing_stream = StreamWithHashing::new(file_stream);
    let crc32c = hashing_stream.crc32c();
    let md5 = hashing_stream.md5();
    let body = reqwest::Body::wrap_stream(hashing_stream);
    let upload_response = client
        .put(upload_url)
        .body(body)
        .send()
        .await?;
    // TODO: retry/resume partial uploads

    let response: GoogleCreateResponse = upload_response.json().await?;
    if response.kind != "drive#file" {
        bail!("expected Google to create object with kind=drive#file, got {:?}", response.kind);
    }
    if response.size != size.to_string() {
        bail!("expected Google to create file with size={}, got {}", size, response.size);
    }
    if response.parents != vec![parent] {
        bail!("expected Google to create file with parents={:?}, got {:?}", vec![parent], response.parents);
    }
    if response.name != filename {
        bail!("expected Google to create file with name={:?}, got {:?}", filename, response.name);
    }
    let md5 = md5.lock().clone().finalize();
    if response.md5 != md5.as_slice() {
        bail!("expected Google to create file with md5={:?}, got {:?}", md5, response.md5);
    }
    let id = response.id;

    let crc32c_m = crc32c.lock();
    Ok(GdriveFile {
        id: id.to_string(),
        owner_id: Some(owner_id),
        md5: response.md5,
        crc32c: *crc32c_m,
        size: size as i64,
        last_probed: None,
    })
}
