use std::path::Path;
use anyhow::{anyhow, bail, Result};
use tracing::info;
use reqwest::StatusCode;
use tokio::fs::DirEntry;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use futures::stream::{StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use regex::Regex;
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

async fn get_service_account_files(domain: &i16) -> Result<Vec<DirEntry>> {
    let exastash = ProjectDirs::from("", "", "exastash")
        .ok_or_else(|| anyhow!("Could not get home directory"))?;
    let dir = exastash.config_dir().join("service-accounts").join(domain.to_string());
    let stream = tokio::fs::read_dir(dir).await?;
    Ok(stream.map(|r| r.unwrap()).collect::<Vec<DirEntry>>().await)
}

async fn get_token_for_random_service_account(domain: &i16) -> Result<AccessToken> {
    let files = get_service_account_files(domain).await?;
    let mut rng = rand::thread_rng();
    let file = files.choose(&mut rng).expect("no service accounts");
    get_token_for_service_account(file.path()).await
}

// Take AsRef<str> instead of AccessToken because AccessToken has private fields
// and we can't construct a fake one in tests
async fn stream_gdrive_file_with_access_token<T: AsRef<str>>(file_id: &str, access_token: T) -> Result<impl tokio::io::AsyncRead> {
    info!(id = file_id, "streaming gdrive file");
    static FILE_ID_RE: &Lazy<Regex> = lazy_regex!(r#"\A[-_0-9A-Za-z]{28,160}\z"#);
    if let None = FILE_ID_RE.captures(file_id) {
        bail!("Invalid gdrive file_id: {:?}", file_id);
    }
    let url = format!("https://www.googleapis.com/drive/v3/files/{}?alt=media", file_id);
    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", access_token.as_ref()))
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

pub(crate) async fn stream_gdrive_file_on_domain(file_id: &str, domain: &i16) -> Result<impl tokio::io::AsyncRead> {
    let access_token = get_token_for_random_service_account(domain).await?;
    stream_gdrive_file_with_access_token(file_id, access_token).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_invalid_file_id() {
        let result = stream_gdrive_file_with_access_token("/invalid/", "").await;
        assert_eq!(result.err().expect("expected an error").to_string(), "Invalid gdrive file_id: \"/invalid/\"");
    }
}
