use std::path::Path;
use anyhow::{anyhow, bail, Result};
use tokio::fs::DirEntry;
use futures::stream::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use rand::seq::SliceRandom;
use directories::ProjectDirs;
use data_encoding::BASE64;
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt};
pub use yup_oauth2::AccessToken;
use crate::lazy_regex;

/// Returns a Bearer token for a particular service account, where json_path is a
/// path to a service account credential file exported from Google in JSON format.
async fn get_token_for_service_account<P: AsRef<Path>>(json_path: P) -> Result<AccessToken> {
    let creds = yup_oauth2::read_service_account_key(json_path).await?;
    let sa = yup_oauth2::ServiceAccountAuthenticator::builder(creds).build().await?;
    let scopes = &["https://www.googleapis.com/auth/drive"];
    Ok(sa.token(scopes).await?)
}

/// Returns a Vec of all service account files for a particular domain.
async fn get_service_account_files(domain: i16) -> Result<Vec<DirEntry>> {
    let exastash = ProjectDirs::from("", "", "exastash")
        .ok_or_else(|| anyhow!("could not get home directory"))?;
    let dir = exastash.config_dir().join("service-accounts").join(domain.to_string());
    let stream = tokio::fs::read_dir(dir).await?;
    Ok(stream.map(|r| r.unwrap()).collect::<Vec<DirEntry>>().await)
}

/// Returns a Bearer token for a random service account for a particular domain.
async fn get_token_for_random_service_account(domain: i16) -> Result<AccessToken> {
    let files = get_service_account_files(domain).await?;
    let mut rng = rand::thread_rng();
    let file = files.choose(&mut rng).expect("no service accounts");
    get_token_for_service_account(file.path()).await
}

/// Returns the crc32c value in the x-goog-hash header in a `reqwest::Response`.
pub(crate) fn get_crc32c_in_response(response: &reqwest::Response) -> Result<u32> {
    let headers = response.headers();
    let value = headers
        .get("x-goog-hash")
        .ok_or_else(|| anyhow!("response was missing x-goog-hash; headers were {:#?}", headers))?
        .to_str()
        .map_err(|_| anyhow!("x-goog-hash value contained characters that are not visible ASCII; headers were {:#?}", headers))?;
    if value.len() != 7 + 8 { // "crc32c=" + 8 base64 bytes including trailing "=="
        bail!("x-goog-hash value {:?} was not {} bytes", value, 7 + 8);
    }
    if !value.starts_with("crc32c=") {
        bail!("x-goog-hash value {:?} did not start with {:?}", value, "crc32c=");
    }
    let b64 = &value[7..];
    let mut out = [0u8; 6];
    let written_bytes = BASE64
        .decode_mut(b64.as_bytes(), &mut out)
        .map_err(|_| anyhow!("failed to decode base64 in header: {}", value))?;
    if written_bytes != 4 {
        bail!("x-goog-hash value {} decoded to {} bytes, expected 4", value, written_bytes);
    }
    let mut rdr = Cursor::new(out);
    let crc32c = rdr.read_u32::<BigEndian>().unwrap();
    Ok(crc32c)
}

/// Returns a `reqwest::Response` that can be used to retrieve a particular Google Drive file.
///
/// This takes AsRef<str> instead of AccessToken because AccessToken has private fields
/// and we can't construct a fake one in tests.
async fn request_gdrive_file_with_access_token<T: AsRef<str>>(file_id: &str, access_token: T) -> Result<reqwest::Response> {
    static FILE_ID_RE: &Lazy<Regex> = lazy_regex!(r#"\A[-_0-9A-Za-z]{28,160}\z"#);
    if let None = FILE_ID_RE.captures(file_id) {
        bail!("invalid gdrive file_id: {:?}", file_id);
    }
    let url = format!("https://www.googleapis.com/drive/v3/files/{}?alt=media", file_id);
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", access_token.as_ref()))
        .send()
        .await?;
    Ok(response)
}

/// Returns a Stream of Bytes containing the content of a particular Google Drive file.
pub(crate) async fn request_gdrive_file_on_domain(file_id: &str, domain: i16) -> Result<reqwest::Response> {
    let access_token = get_token_for_random_service_account(domain).await?;
    request_gdrive_file_with_access_token(file_id, access_token).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_invalid_file_id() {
        let result = request_gdrive_file_with_access_token("/invalid/", "").await;
        assert_eq!(result.err().expect("expected an error").to_string(), "invalid gdrive file_id: \"/invalid/\"");
    }
}
