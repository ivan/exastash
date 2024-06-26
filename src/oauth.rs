//! Functions for managing OAuth 2.0 access tokens

use std::collections::HashMap;
use anyhow::{anyhow, bail, Result};
use tracing::{info, debug};
use yup_oauth2::{ApplicationSecret, RefreshFlow, InstalledFlowAuthenticator, InstalledFlowReturnMethod};
use sqlx::{Transaction, Postgres};
use sqlx::postgres::PgPool;
use chrono::{Utc, Duration};
use hyper_rustls::HttpsConnectorBuilder;
use crate::db::google_auth::{GoogleApplicationSecret, GoogleAccessToken};
use crate::db::storage::gdrive::file::GdriveOwner;


/// Create an access token for an owner.
///
/// This is a three-step process:
/// 1) human must take the URL printed to the terminal and visit it with the
///    Google account corresponding to the owner
/// 2) human must take the code from Google and paste it into the terminal
/// 3) new google_access_token is inserted into the database
/// 
/// Does not commit the transaction, you must do so yourself.
pub async fn create_access_token(transaction: &mut Transaction<'_, Postgres>, owner_id: i32) -> Result<()> {
    let owners = GdriveOwner::find_by_owner_ids(transaction, &[owner_id]).await?;
    if owners.is_empty() {
        bail!("owner id {} not in database", owner_id);
    }
    let owner = &owners[0];
    let secrets = GoogleApplicationSecret::find_by_domain_ids(transaction, &[owner.domain]).await?;
    if secrets.is_empty() {
        bail!("application secret not in database for domain {}", owner.domain);
    }
    let secret = secrets[0].secret["installed"].clone();
    let app_secret: ApplicationSecret = serde_json::from_value(secret)?;
    let auth = InstalledFlowAuthenticator::builder(app_secret, InstalledFlowReturnMethod::HTTPRedirect)
        .build().await
        .unwrap();
    let scopes = &["https://www.googleapis.com/auth/drive"];
    let token = auth.token(scopes).await?;
    let info = token.info();
    GoogleAccessToken {
        owner_id,
        access_token: info.access_token.clone(),
        refresh_token: info.refresh_token.clone().unwrap(),
        expires_at: info.expires_at.unwrap(),
    }.create(transaction).await?;

    Ok(())
}

/// Refresh and update in database all google_access_tokens that expire within 55 minutes
pub async fn refresh_access_tokens(client: &mut PgPool) -> Result<()> {
    // We assume that we get access tokens that are valid for 60 minutes
    let expiry_within_minutes = 55;
    info!("refreshing access tokens that expire within {} minutes", expiry_within_minutes);

    let mut transaction = client.begin().await?;

    // Map of domain_id -> ApplicationSecret
    let mut secrets_map = HashMap::new();
    let secrets = GoogleApplicationSecret::find_all(&mut transaction).await?;
    for secret in secrets {
        let installed = secret.secret["installed"].clone();
        let app_secret: ApplicationSecret = serde_json::from_value(installed)?;
        secrets_map.insert(secret.domain_id, app_secret);
    }

    // Map of owner_id -> GdriveOwner
    let mut owners_map = HashMap::new();
    let owners = GdriveOwner::find_all(&mut transaction).await?;
    for owner in owners {
        owners_map.insert(owner.id, owner);
    }

    let https = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_only()
        .enable_http1()
        .build();
    let hyper_client = hyper::Client::builder().build::<_, hyper::Body>(https);

    let expires_at = Utc::now() + Duration::try_minutes(expiry_within_minutes).unwrap();
    let tokens = GoogleAccessToken::find_by_expires_at(&mut transaction, expires_at).await?;
    for token in &tokens {
        debug!(?token, "refreshing token");
        let owner = owners_map.get(&token.owner_id).ok_or_else(|| anyhow!("cannot find owner in owners map: {}", token.owner_id))?;
        let secret = secrets_map.get(&owner.domain).ok_or_else(|| anyhow!("cannot find domain in secrets map: {}", owner.domain))?;

        let new_info = RefreshFlow::refresh_token(&hyper_client, secret, &token.refresh_token).await?;
        let new_token = GoogleAccessToken {
            owner_id: token.owner_id,
            access_token: new_info.access_token,
            refresh_token: new_info.refresh_token.ok_or_else(|| anyhow!("no refresh_token after refresh"))?,
            expires_at: new_info.expires_at.ok_or_else(|| anyhow!("no expires_at after refresh"))?,
        };

        token.delete(&mut transaction).await?;
        new_token.create(&mut transaction).await?;
    }
    transaction.commit().await?;
    info!("refreshed {} access tokens", tokens.len());

    Ok(())
}
