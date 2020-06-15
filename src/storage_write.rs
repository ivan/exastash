//! Functions to write content to from storage

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

#[pin_project]
struct StreamWithHashing<T: Stream<Item = Result<Vec<u8>, std::io::Error>>> {
    #[pin]
    stream: T,
    // We use Arc<Mutex<...> here because reqwest::Body::wrap_stream wants to take
    // ownership of a Stream, but we still need to read out the crc32c and md5
    // after reqwest is done with them.
    crc32c: Arc<Mutex<u32>>,
}

impl<T: Stream<Item = Result<Vec<u8>, std::io::Error>>> StreamWithHashing<T> {
    fn new(stream: T) -> StreamWithHashing<T> {
        StreamWithHashing { stream, crc32c: Arc::new(Mutex::new(0)) }
    }

    /// Get an `Arc` which can be derefenced to get the crc32c of the data streamed so far
    fn crc32c(&self) -> Arc<Mutex<u32>> {
        self.crc32c.clone()
    }
}

impl<S> Stream for StreamWithHashing<S>
where
    S: Stream<Item = Result<Vec<u8>, std::io::Error>>,
{
    type Item = Result<Vec<u8>, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let crc32c = self.crc32c.clone();
        if let Some(res) = ready!(self.project().stream.poll_next(cx)) {
            if let Ok(bytes) = &res {
                let mut crc32c_m = crc32c.lock();
                *crc32c_m = crc32c::crc32c_append(*crc32c_m, bytes);
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
    md5_checksum: String,
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
    let body = reqwest::Body::wrap_stream(hashing_stream);
    let upload_response = client
        .put(upload_url)
        .body(body)
        .send()
        .await?;
    // TODO: retry/resume partial uploads

    let response: GoogleCreateResponse = upload_response.json().await?;
    dbg!(&response);
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
    // TODO check md5
    let id = response.id;
    // TODO
    let md5 = [0; 16];

    let crc32c = crc32c.lock();
    Ok(GdriveFile {
        id: id.to_string(),
        owner_id: Some(owner_id),
        md5,
        crc32c: *crc32c,
        size: size as i64,
        last_probed: None,
    })
}
