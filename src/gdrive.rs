use std::path::Path;
use anyhow::{anyhow, bail, Result};
use reqwest::StatusCode;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use futures::stream::{StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use regex::Regex;
use std::fmt::Display;
use tokio::fs::DirEntry;
use rand::seq::SliceRandom;
use directories::ProjectDirs;
pub use yup_oauth2::AccessToken;
use crate::lazy_regex;

async fn get_token_for_service_account<P: AsRef<Path>>(json_path: P) -> Result<AccessToken> {
    let creds = yup_oauth2::read_service_account_key(json_path).await?;
    let sa = yup_oauth2::ServiceAccountAuthenticator::builder(creds).build().await?;
    let scopes = &["https://www.googleapis.com/auth/drive"];
    Ok(sa.token(scopes).await?)
}

async fn get_service_account_files() -> Result<Vec<DirEntry>> {
    let exastash = ProjectDirs::from("", "", "exastash")
        .ok_or_else(|| anyhow!("Could not get home directory"))?;
    let dir = exastash.config_dir().join("service-accounts");
    let stream = tokio::fs::read_dir(dir).await?;
    Ok(stream.map(|r| r.unwrap()).collect::<Vec<DirEntry>>().await)
}

async fn get_token_for_random_service_account() -> Result<AccessToken> {
    let files = get_service_account_files().await?;
    let mut rng = rand::thread_rng();
    let file = files.choose(&mut rng).expect("no service accounts");
    get_token_for_service_account(file.path()).await
}

// Take AsRef<str> instead of AccessToken because AccessToken has private fields
// and we can't construct a fake one in tests
async fn stream_gdrive_file<T: AsRef<str> + Display>(access_token: T, file_id: &str) -> Result<impl tokio::io::AsyncRead> {
    static FILE_ID_RE: &Lazy<Regex> = lazy_regex!(r#"\A[-_0-9A-Za-z]{28,160}\z"#);
    if let None = FILE_ID_RE.captures(file_id) {
        bail!("Invalid gdrive file_id: {:?}", file_id);
    }
    let url = format!("https://www.googleapis.com/drive/v3/files/{}?alt=media", file_id);
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", access_token))
        .send()
        .await?;
    Ok(match response.status() {
        StatusCode::OK => {
            response
                .bytes_stream()
                .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read()
                .compat()
        },
        _ => {
            bail!("{} responded with HTTP status code {}", url, response.status());
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_invalid_file_id() {
        let result = stream_gdrive_file("", "/invalid/").await;
        assert_eq!(result.err().expect("expected an error").to_string(), "Invalid gdrive file_id: \"/invalid/\"");
    }
}
