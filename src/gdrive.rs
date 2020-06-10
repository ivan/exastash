//! Functions to read from Google Drive, without anything exastash-specific

use anyhow::{anyhow, bail, ensure, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use data_encoding::BASE64;
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt};
pub use yup_oauth2::AccessToken;
use crate::lazy_regex;

pub fn get_header_value<'a>(response: &'a reqwest::Response, header: &str) -> Result<&'a str> {
    let headers = response.headers();
    let value = headers
        .get(header)
        .ok_or_else(|| anyhow!("response was missing {}; headers were {:#?}", header, headers))?
        .to_str()
        .map_err(|_| anyhow!("{} value contained characters that are not visible ASCII; headers were {:#?}", header, headers))?;
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
    let url = format!("https://www.googleapis.com/drive/v3/files/{}?alt=media", file_id);
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_invalid_file_id() {
        let result = request_gdrive_file("/invalid/", "").await;
        assert_eq!(result.err().expect("expected an error").to_string(), "invalid gdrive file_id: \"/invalid/\"");
    }
}
