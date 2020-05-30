//! CRUD operations for Google OAuth 2.0 and service account entities in PostgreSQL

use anyhow::Result;
use chrono::{DateTime, Utc};
use yup_oauth2::ServiceAccountKey;
use tokio_postgres::Transaction;

/// A gsuite_application_secret entity
#[derive(Debug, Clone)]
pub struct GsuiteApplicationSecret {
    /// The gsuite_domain this secret is for
    pub domain_id: i16,
    /// The secret itself, a JSON object with an "installed" key
    pub secret: serde_json::Value
}

impl GsuiteApplicationSecret {
    /// Create a gsuite_application_secret in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_>) -> Result<()> {
        transaction.execute(
            "INSERT INTO gsuite_application_secrets (domain_id, secret)
             VALUES ($1::smallint, $2::jsonb)",
            &[&self.domain_id, &self.secret]
        ).await?;
        Ok(())
    }
}

/// A gsuite_access_token entity
#[derive(Debug, Clone)]
pub struct GsuiteAccessToken {
    /// The gdrive_owner this access token is for
    pub owner_id: i32,
    /// The OAuth 2.0 access token
    pub access_token: String,
    /// The OAuth 2.0 refresh token
    pub refresh_token: String,
    /// The time at which the access token expires
    pub expires_at: DateTime<Utc>,
}

impl GsuiteAccessToken {
    /// Create a gsuite_access_token in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_>) -> Result<()> {
        transaction.execute(
            "INSERT INTO gsuite_access_tokens (owner_id, access_token, refresh_token, expires_at)
             VALUES ($1::int, $2::text, $3::text, $4::timestamptz)",
            &[&self.owner_id, &self.access_token, &self.refresh_token, &self.expires_at]
        ).await?;
        Ok(())
    }
}

/// A gsuite_service_Account entity
#[derive(Debug, Clone)]
pub struct GsuiteServiceAccount {
    /// The gdrive_owner this service account is for
    pub owner_id: i32,
    /// The key for this service account
    pub key: ServiceAccountKey,
}

impl GsuiteServiceAccount {
    /// Create a gsuite_service_account in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_>) -> Result<()> {
        let k = &self.key;
        transaction.execute(
            "INSERT INTO gsuite_service_accounts (owner_id, client_email, client_id, project_id, private_key_id, private_key, auth_uri, token_uri, auth_provider_x509_cert_url, client_x509_cert_url)
             VALUES ($1::int, $2::text, $3::text, $4::text, $5::text, $6::text, $7::text, $8::text, $9::text, $10::text)",
            &[&self.owner_id, &k.client_email, &k.client_id, &k.project_id, &k.private_key_id, &k.private_key, &k.auth_uri, &k.token_uri, &k.auth_provider_x509_cert_url, &k.client_x509_cert_url]
        ).await?;
        Ok(())
    }
}
