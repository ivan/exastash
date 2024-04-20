//! Functions to read from and write to Google Drive, without anything exastash-specific

use anyhow::{anyhow, bail, ensure, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use data_encoding::BASE64;
use serde::Deserialize;
use serde_hex::{SerHex, Strict};
use serde_json::{json, Value};
use std::io::Cursor;
use std::ops::AsyncFn;
use byteorder::{BigEndian, ReadBytesExt};
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use futures::stream::Stream;
use bytes::Bytes;
pub use yup_oauth2::AccessToken;
use crate::db::storage::gdrive::file::GdriveFile;
use crate::lazy_regex;
use crate::storage::read::get_access_tokens;
use crate::db;

pub(crate) fn get_header_value<'a>(response: &'a reqwest::Response, header: &str) -> Result<&'a str> {
    let headers = response.headers();
    let value = headers
        .get(header)
        .ok_or_else(|| anyhow!("response was missing {header}; headers were {:#?}", headers))?
        .to_str()
        .map_err(|_| anyhow!("{header} value contained characters that are not visible ASCII; headers were {:#?}", headers))?;
    Ok(value)
}

/// Returns the crc32c value in the x-goog-hash header in a `reqwest::Response`.
pub(crate) fn get_crc32c_in_response(response: &reqwest::Response) -> Result<u32> {
    let value = get_header_value(response, "x-goog-hash")?;
    let prefix = "crc32c=";
    let encoded_len = 8;
    if value.len() != prefix.len() + encoded_len {
        bail!("x-goog-hash value {:?} was not {} bytes", value, prefix.len() + encoded_len);
    }
    ensure!(value.starts_with(prefix), "x-goog-hash value {:?} did not start with {:?}", value, prefix);
    let b64 = &value[prefix.len()..];
    let mut out = [0u8; 6];
    let wrote_bytes = BASE64
        .decode_mut(b64.as_bytes(), &mut out)
        .map_err(|_| anyhow!("failed to decode base64 in header: {}", value))?;
    ensure!(wrote_bytes == 4, "x-goog-hash value {} decoded to {} bytes, expected 4", value, wrote_bytes);
    let mut rdr = Cursor::new(out);
    let crc32c = rdr.read_u32::<BigEndian>().unwrap();
    Ok(crc32c)
}

/// Returns a `reqwest::Response` that can be used to retrieve a particular Google Drive file.
pub(crate) async fn request_gdrive_file(file_id: &str, access_token: &str) -> Result<reqwest::Response> {
    static FILE_ID_RE: &Lazy<Regex> = lazy_regex!(r#"\A[-_0-9A-Za-z]{28,160}\z"#);
    if FILE_ID_RE.captures(file_id).is_none() {
        bail!("invalid gdrive file_id: {:?}", file_id);
    }
    let url = format!("https://www.googleapis.com/drive/v3/files/{file_id}?alt=media");
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {access_token}"))
        .send().await?;
    Ok(response)
}

/// Delete a shared drive
pub async fn delete_shared_drive(drive_id: &str, access_token: &str) -> Result<()> {
    static DRIVE_ID_RE: &Lazy<Regex> = lazy_regex!(r#"\A[-_0-9A-Za-z]{19}\z"#);
    if DRIVE_ID_RE.captures(drive_id).is_none() {
        bail!("invalid gdrive drive_id: {:?}", drive_id);
    }
    let url = format!("https://www.googleapis.com/drive/v3/drives/{drive_id}");
    let client = reqwest::Client::new();
    let response = client
        .delete(&url)
        .header("Authorization", format!("Bearer {access_token}"))
        .send().await?;
    let status = response.status();
    if !(status == 200 || status == 204) {
        bail!("expected status 200 or 204 in response to drive delete request, got {status}");
    }
    Ok(())
}

/// List shared drives
/// Note that Google's backend is broken and may not return all of your shared drives.
pub async fn list_shared_drives(access_token: &str) -> Result<Value> {
    let url = "https://www.googleapis.com/drive/v3/drives?pageSize=100";
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .header("Authorization", format!("Bearer {access_token}"))
        .send().await?;
    let status = response.status();
    if status != 200 {
        bail!("expected status 200 in response to drive list request, got {status}");
    }
    Ok(response.json().await?)
}

/// Get info about a shared drive
pub async fn get_shared_drive(drive_id: &str, access_token: &str) -> Result<Value> {
    let url = format!("https://www.googleapis.com/drive/v3/drives/{drive_id}");
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .header("Authorization", format!("Bearer {access_token}"))
        .send().await?;
    let status = response.status();
    if status != 200 {
        bail!("expected status 200 in response to get drive request, got {status}");
    }
    Ok(response.json().await?)
}

/// List permissions on a file or shared drive
pub async fn list_permissions(file_or_drive_id: &str, access_token: &str) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(2);
    let mut next_page_token: Option<String> = None;
    loop {
        let base_url = format!("https://www.googleapis.com/drive/v3/files/{file_or_drive_id}/permissions?supportsTeamDrives=true");
        let url = match next_page_token {
            Some(ref token) => format!("{base_url}&pageToken={token}"),
            None => base_url,
        };
        let client = reqwest::Client::new();
        let response = client
            .get(url)
            .header("Authorization", format!("Bearer {access_token}"))
            .send().await?;
        let status = response.status();
        if status != 200 {
            bail!("expected status 200 in response to permissions list request, got {status}");
        }
        let value: Value = response.json().await?;
        if let Some(token) = value.get("nextPageToken") {
            next_page_token = token.as_str().map(String::from);
        } else {
            next_page_token = None;
        }
        values.push(value);
        if next_page_token.is_none() {
            break;
        }
    }
    Ok(values)
}

#[derive(Debug, Deserialize)]
pub(crate) struct GdriveUploadResponse {
    pub(crate) kind: String,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) parents: Vec<String>,
    pub(crate) size: String,
    #[serde(rename = "md5Checksum")]
    #[serde(with = "SerHex::<Strict>")]
    pub(crate) md5: [u8; 16],
}

/// Reasons why the upload to Google Drive failed.
#[allow(missing_docs)]
#[derive(Debug, Eq, thiserror::Error, PartialEq)]
pub enum GdriveUploadError {
    #[error("expected status 200 in response to initial upload request, got {0} with body {}", .1.to_string())]
    InitialUploadRequestNotOk(StatusCode, Value),

    #[error("did not get Location header in response to initial upload request: {0:#?}")]
    InitialUploadRequestMissingLocationHeader(HeaderMap),

    #[error("parent is full: {0}")]
    ParentIsFull(String),

    #[error("expected status 200 in response to upload request, got {0} with body {}", .1.to_string())]
    UploadRequestNotOk(StatusCode, Value),

    #[error("expected JSON in response for initial upload request, got {}", .0)]
    InitialUploadRequestUnparseable(String),

    #[error("expected JSON in response for upload request, got {}", .0)]
    UploadRequestUnparseable(String),

    #[error("expected Google to create object with kind=drive#file, got {0:?}")]
    CreatedFileHasWrongKind(String),

    #[error("expected Google to create file with size={0:?}, got {1:?}")]
    CreatedFileHasWrongSize(String, String),

    #[error("expected Google to create file with parents={0:?}, got {1:?}")]
    CreatedFileHasWrongParents(Vec<String>, Vec<String>),

    #[error("expected Google to create file with name={0:?}, got {1:?}")]
    CreatedFileHasWrongName(String, String),
}

/// Reasons why the deletion on Google Drive failed.
#[allow(missing_docs)]
#[derive(Debug, Eq, thiserror::Error, PartialEq)]
pub enum GdriveDeleteError {
    #[error("expected empty response for delete request, got status={0:?}, body={1:?}")]
    DeleteRequestNotOk(StatusCode, String),
}

/// Return `true` if the given JSON response indicates that the shared drive
/// file limit has been exceeded.
///
/// ```ignore
/// {
///   "error": {
///     "errors": [
///       {
///         "domain": "global",
///         "reason": "teamDriveFileLimitExceeded",
///         "message": "The file limit for this shared drive has been exceeded."
///       }
///     ],
///     "code": 403,
///     "message": "The file limit for this shared drive has been exceeded."
///   }
/// }
/// ```
fn is_shared_drive_full_response(json: &Value) -> bool {
    let matching_reason = Value::String("teamDriveFileLimitExceeded".into());

    if json.is_object() {
        let error = &json["error"];
        if error.is_object() {
            let errors = &error["errors"];
            if let Value::Array(arr) = errors {
                for e in arr {
                    if let Value::Object(props) = e {
                        if props["reason"] == matching_reason {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

pub(crate) async fn create_gdrive_file<S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>(
    stream: S,
    // TODO: Change `AsyncFn` to `async Fn()` once rust-analyzer supports it
    access_token_fn: impl AsyncFn() -> Result<String>,
    size: u64,
    parent: &str,
    filename: &str,
) -> Result<GdriveUploadResponse> {
    let client = reqwest::Client::new();

    // https://developers.google.com/drive/api/v3/reference/files/create
    let metadata = json!({
        "name": filename,
        "parents": [parent],
        "mimeType": "application/octet-stream",
    });
    // https://developers.google.com/drive/api/v3/manage-uploads#resumable
    // Note: use fields=* to get all fields in response
    let initial_url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&supportsAllDrives=true&fields=kind,id,name,parents,size,md5Checksum";
    let initial_response = client
        .post(initial_url)
        .json(&metadata)
        .header("Authorization", format!("Bearer {}", access_token_fn().await?))
        .header("X-Upload-Content-Type", "application/octet-stream")
        .header("X-Upload-Content-Length", size)
        .send().await?;

    let status = initial_response.status();
    if status != 200 {
        let body = initial_response.text().await?;
        let Ok(json) = serde_json::from_str(&body) else {
            bail!(GdriveUploadError::InitialUploadRequestUnparseable(body));
        };
        bail!(GdriveUploadError::InitialUploadRequestNotOk(status, json));
    }
    let headers = initial_response.headers();
    let upload_url = headers.get("Location")
        .ok_or_else(|| anyhow!(GdriveUploadError::InitialUploadRequestMissingLocationHeader(headers.clone())))?
        .to_str()?;
    let body = reqwest::Body::wrap_stream(stream);
    let upload_response = client
        .put(upload_url)
        .body(body)
        .send().await?;
    // TODO: retry/resume partial uploads

    let status = upload_response.status();
    if status != 200 {
        let body = upload_response.text().await?;
        let Ok(json) = serde_json::from_str(&body) else {
            bail!(GdriveUploadError::UploadRequestUnparseable(body));
        };
        if is_shared_drive_full_response(&json) {
            let message = json["error"]["message"].to_string();
            bail!(GdriveUploadError::ParentIsFull(message));
        }
        bail!(GdriveUploadError::UploadRequestNotOk(status, json));
    }
    let response: GdriveUploadResponse = upload_response.json().await?;

    if response.kind != "drive#file" {
        bail!(GdriveUploadError::CreatedFileHasWrongKind(response.kind));
    }
    if response.size != size.to_string() {
        bail!(GdriveUploadError::CreatedFileHasWrongSize(size.to_string(), response.size));
    }
    if response.parents != vec![parent] {
        bail!(GdriveUploadError::CreatedFileHasWrongParents(vec![parent.into()], response.parents));
    }
    if response.name != filename {
        bail!(GdriveUploadError::CreatedFileHasWrongName(filename.into(), response.name));
    }

    Ok(response)
}

pub(crate) async fn delete_gdrive_file(file_id: &str) -> Result<()> {
    let pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;
    let mut gdrive_files = GdriveFile::find_by_ids_in_order(&mut transaction, &[file_id]).await?;
    let gdrive_file = gdrive_files.pop().unwrap();
    transaction.commit().await?; // close read-only transaction

    // Hack
    let domain_id = 1;
    let access_tokens = get_access_tokens(gdrive_file.owner_id, domain_id).await?;
    if access_tokens.is_empty() {
        bail!("no access tokens were available for owners associated file_id={:?} (domain_id={})", gdrive_file.id, domain_id);
    }
    let tries = 1; // We had 3 before, not sure if that was useful
    let access_tokens_tries = access_tokens.iter().cycle().take(access_tokens.len() * tries);

    let mut out = Err(anyhow!("Google did not respond with an OK response after trying all access tokens"));
    for (access_token, _service_account) in access_tokens_tries {
        let client = reqwest::Client::new();

        let url = format!("https://www.googleapis.com/drive/v3/files/{file_id}?supportsAllDrives=true");
        let response = client
            .delete(url)
            .header("Authorization", format!("Bearer {}", access_token))
            .send().await?;
    
        let status = response.status();
        if status == 403 || status == 404 {
            // Wrong access token, try another
            continue;
        }
        if status == 200 || status == 204 {
            out = Ok(());
            break;
        }
        let body = response.text().await?;
        bail!(GdriveDeleteError::DeleteRequestNotOk(status, body));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_invalid_file_id() {
        let result = request_gdrive_file("/invalid/", "").await;
        assert_eq!(result.expect_err("expected an error").to_string(), "invalid gdrive file_id: \"/invalid/\"");
    }
}
