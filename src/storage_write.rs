//! Functions to write content to from storage

use anyhow::{anyhow, bail};
use std::path::Path;
use anyhow::Result;
use tokio::fs;
use serde::Deserialize;
use serde_json::json;
use crate::db::storage::gdrive::file::GdriveFile;
use crate::storage_read::get_access_tokens;

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
pub async fn create_gdrive_file(path: &Path, domain_id: i16, owner_id: i32, parent: &str, filename: &str) -> Result<GdriveFile> {
    let attr = fs::metadata(path).await?;
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
    let upload_response = client
        .put(upload_url)
        .body(fs::read(path).await?)
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
    let crc32c = 0;

    Ok(GdriveFile {
        id: id.to_string(),
        owner_id: Some(owner_id),
        md5,
        crc32c,
        size: size as i64,
        last_probed: None,
    })
}
