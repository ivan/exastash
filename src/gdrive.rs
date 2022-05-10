//! Functions to read from and write to Google Drive, without anything exastash-specific

use anyhow::{anyhow, bail, ensure, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use data_encoding::BASE64;
use serde::Deserialize;
use serde_hex::{SerHex, Strict};
use serde_json::{json, Value};
use std::io::Cursor;
use std::future::Future;
use byteorder::{BigEndian, ReadBytesExt};
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use futures::stream::Stream;
use bytes::Bytes;
pub use yup_oauth2::AccessToken;
use crate::lazy_regex;

pub fn get_header_value<'a>(response: &'a reqwest::Response, header: &str) -> Result<&'a str> {
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

    #[error("expected Google to create object with kind=drive#file, got {0:?}")]
    CreatedFileHasWrongKind(String),

    #[error("expected Google to create file with size={0:?}, got {1:?}")]
    CreatedFileHasWrongSize(String, String),

    #[error("expected Google to create file with parents={0:?}, got {1:?}")]
    CreatedFileHasWrongParents(Vec<String>, Vec<String>),

    #[error("expected Google to create file with name={0:?}, got {1:?}")]
    CreatedFileHasWrongName(String, String),
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

pub(crate) async fn create_gdrive_file<S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static, A>(
    stream: S,
    access_token_fn: impl Fn() -> A,
    size: u64,
    parent: &str,
    filename: &str
) -> Result<GdriveUploadResponse>
where
    A: Future<Output=Result<String>>
{
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
        let json = serde_json::from_str(&body)?;
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
        let json = serde_json::from_str(&body)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_invalid_file_id() {
        let result = request_gdrive_file("/invalid/", "").await;
        assert_eq!(result.expect_err("expected an error").to_string(), "invalid gdrive file_id: \"/invalid/\"");
    }
}
