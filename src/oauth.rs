//! Functions for managing OAuth 2.0 access tokens

use std::collections::HashMap;
use anyhow::{anyhow, Result};
use tracing::{info, debug};
use yup_oauth2::{ApplicationSecret, RefreshFlow};
use tokio_postgres::Client;
use chrono::{Utc, Duration};
use crate::db;
use crate::db::google_auth::{GsuiteApplicationSecret, GsuiteAccessToken};
use crate::db::storage::gdrive::file::GdriveOwner;


/// Refresh and update in database all gsuite_access_tokens that expire within 55 minutes
pub async fn refresh_access_tokens(client: &mut Client) -> Result<()> {
    // We assume that we get access tokens that are valid for 60 minutes
    let expiry_within_minutes = 55;
    info!("refreshing access tokens that expire within {} minutes", expiry_within_minutes);

    let mut transaction = db::start_transaction(client).await?;

    // Map of domain_id -> ApplicationSecret
    let mut secrets_map = HashMap::new();
    let secrets = GsuiteApplicationSecret::find_all(&mut transaction).await?;
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

    let expires_at = Utc::now() + Duration::minutes(expiry_within_minutes);
    let tokens = GsuiteAccessToken::find_by_expires_at(&mut transaction, expires_at).await?;
    for token in &tokens {
        debug!("refreshing {:?}", token);
        let owner = owners_map.get(&token.owner_id).ok_or_else(|| anyhow!("cannot find owner in owners map: {}", token.owner_id))?;
        let secret = secrets_map.get(&owner.domain).ok_or_else(|| anyhow!("cannot find domain in secrets map: {}", owner.domain))?;

        let https = hyper_rustls::HttpsConnector::new();
        let hyper_client = hyper::Client::builder().build::<_, hyper::Body>(https);

        let new_info = RefreshFlow::refresh_token(&hyper_client, secret, &token.refresh_token).await?;
        let new_token = GsuiteAccessToken {
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
