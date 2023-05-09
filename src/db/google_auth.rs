//! CRUD operations for Google OAuth 2.0 and service account entities in PostgreSQL

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use yup_oauth2::ServiceAccountKey;
use sqlx::{Postgres, Transaction};
use custom_debug_derive::Debug as CustomDebug;
use crate::util::elide;

/// A google_application_secret entity
#[derive(Clone, CustomDebug, sqlx::FromRow)]
pub struct GoogleApplicationSecret {
    /// The google_domain this secret is for
    pub domain_id: i16,
    /// The secret itself, a JSON object with an "installed" key
    #[debug(with = "elide")]
    pub secret: serde_json::Value
}

impl GoogleApplicationSecret {
    /// Create a google_application_secret in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!(r#"
            INSERT INTO stash.google_application_secrets (domain_id, secret)
            VALUES ($1, $2)"#,
            &self.domain_id, &self.secret
        ).execute(transaction).await?;
        Ok(())
    }

    /// Return a `Vec<GoogleApplicationSecret>` of all google_application_secrets.
    pub async fn find_all(transaction: &mut Transaction<'_, Postgres>) -> Result<Vec<GoogleApplicationSecret>> {
        Ok(sqlx::query_as!(GoogleApplicationSecret, r#"
            SELECT domain_id, secret
            FROM stash.google_application_secrets"#
        ).fetch_all(transaction).await?)
    }

    /// Return a `Vec<GoogleApplicationSecret>` for the corresponding list of `domain_ids`.
    /// There is no error on missing domains.
    pub async fn find_by_domain_ids(transaction: &mut Transaction<'_, Postgres>, domain_ids: &[i16]) -> Result<Vec<GoogleApplicationSecret>> {
        Ok(sqlx::query_as!(GoogleApplicationSecret, r#"
            SELECT domain_id, secret
            FROM stash.google_application_secrets
            WHERE domain_id = ANY($1)"#, domain_ids
        ).fetch_all(transaction).await?)
    }
}

/// A google_access_token entity
#[derive(Clone, CustomDebug, PartialEq, Eq, sqlx::FromRow)]
pub struct GoogleAccessToken {
    /// The gdrive_owner this access token is for
    pub owner_id: i32,
    /// The OAuth 2.0 access token
    #[debug(with = "elide")]
    pub access_token: String,
    /// The OAuth 2.0 refresh token
    #[debug(with = "elide")]
    pub refresh_token: String,
    /// The time at which the access token expires
    pub expires_at: DateTime<Utc>,
}

impl GoogleAccessToken {
    /// Create a google_access_token in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!(r#"
            INSERT INTO stash.google_access_tokens (owner_id, access_token, refresh_token, expires_at)
            VALUES ($1, $2, $3, $4)"#,
            &self.owner_id, &self.access_token, &self.refresh_token, &self.expires_at
        ).execute(transaction).await?;
        Ok(())
    }

    /// Delete this access token from the database, by its owner id.
    /// There is no error if the owner does not exist.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn delete(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!(r#"
            DELETE FROM stash.google_access_tokens WHERE owner_id = $1"#, &self.owner_id
        ).execute(transaction).await?;
        Ok(())
    }

    /// Return a `Vec<GoogleAccessToken>` of tokens that expire before `expires_at`.
    pub async fn find_by_expires_at(transaction: &mut Transaction<'_, Postgres>, expires_at: DateTime<Utc>) -> Result<Vec<GoogleAccessToken>> {
        let tokens = sqlx::query_as!(GoogleAccessToken, r#"
            SELECT owner_id, access_token, refresh_token, expires_at
            FROM stash.google_access_tokens
            WHERE expires_at < $1"#, expires_at
        ).fetch_all(transaction).await?;
        Ok(tokens)
    }

    /// Return a `Vec<GoogleAccessToken>` for the corresponding list of `owner_ids`.
    /// There is no error on missing owners.
    pub async fn find_by_owner_ids(transaction: &mut Transaction<'_, Postgres>, owner_ids: &[i32]) -> Result<Vec<GoogleAccessToken>> {
        let tokens = sqlx::query_as!(GoogleAccessToken, r#"
            SELECT owner_id, access_token, refresh_token, expires_at
            FROM stash.google_access_tokens
            WHERE owner_id = ANY($1)"#, owner_ids
        ).fetch_all(transaction).await?;
        Ok(tokens)
    }
}

/// A google_service_account entity
#[derive(Clone, CustomDebug, PartialEq, Eq)]
pub struct GoogleServiceAccount {
    /// The gdrive_owner this service account is for
    pub owner_id: i32,
    /// The key for this service account
    #[debug(with = "elide")]
    pub key: ServiceAccountKey,
}

impl From<GoogleServiceAccountRow> for GoogleServiceAccount {
    fn from(row: GoogleServiceAccountRow) -> Self {
        GoogleServiceAccount {
            owner_id:                        row.owner_id,
            key: ServiceAccountKey {
                client_email:                row.client_email,
                client_id:                   Some(row.client_id),
                project_id:                  Some(row.project_id),
                private_key_id:              Some(row.private_key_id),
                private_key:                 row.private_key,
                auth_uri:                    Some(row.auth_uri),
                token_uri:                   row.token_uri,
                auth_provider_x509_cert_url: Some(row.auth_provider_x509_cert_url),
                client_x509_cert_url:        Some(row.client_x509_cert_url),
                key_type:                    Some("service_account".into())
            }
        }
    }
}

#[derive(Debug)]
struct GoogleServiceAccountRow {
    /// The gdrive_owner this service account is for
    owner_id: i32,
    client_email: String,
    client_id: String,
    project_id: String,
    private_key_id: String,
    private_key: String,
    auth_uri: String,
    token_uri: String,
    auth_provider_x509_cert_url: String,
    client_x509_cert_url: String,
}

impl GoogleServiceAccount {
    /// Create a google_service_account in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        let k = &self.key;
        sqlx::query!(r#"
            INSERT INTO stash.google_service_accounts (
                owner_id, client_email, client_id, project_id, private_key_id, private_key,
                auth_uri, token_uri, auth_provider_x509_cert_url, client_x509_cert_url
            )
            VALUES ($1, $2::text, $3, $4, $5, $6, $7, $8, $9, $10)"#,
            &self.owner_id,
            &k.client_email,
            &k.client_id.clone().ok_or_else(|| anyhow!("client_id must not be None"))?,
            &k.project_id.clone().ok_or_else(|| anyhow!("project_id must not be None"))?,
            &k.private_key_id.clone().ok_or_else(|| anyhow!("private_key_id must not be None"))?,
            &k.private_key,
            &k.auth_uri.clone().ok_or_else(|| anyhow!("auth_uri must not be None"))?,
            &k.token_uri,
            &k.auth_provider_x509_cert_url.clone().ok_or_else(|| anyhow!("auth_provider_x509_cert_url must not be None"))?,
            &k.client_x509_cert_url.clone().ok_or_else(|| anyhow!("client_x509_cert_url must not be None"))?,
        ).execute(transaction).await?;
        Ok(())
    }

    /// Return a `Vec<GoogleServiceAccount>` for the corresponding list of `owner_ids`.
    /// There is no error on missing owners.
    /// Always returns rows in a random order.
    /// If limit is not `None`, returns max `N` rows.
    pub async fn find_by_owner_ids(transaction: &mut Transaction<'_, Postgres>, owner_ids: &[i32], limit: Option<i64>) -> Result<Vec<GoogleServiceAccount>> {
        let accounts = sqlx::query_as!(GoogleServiceAccountRow, r#"
            SELECT owner_id, client_email, client_id, project_id, private_key_id, private_key,
                   auth_uri, token_uri, auth_provider_x509_cert_url, client_x509_cert_url
            FROM stash.google_service_accounts
            WHERE owner_id = ANY($1)
            ORDER BY random()
            LIMIT $2"#, owner_ids, limit
        )
            .fetch(transaction)
            .map(|result| result.map(|row| row.into()))
            .try_collect().await?;
        Ok(accounts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use crate::db::tests::new_primary_pool;
    use crate::db::storage::gdrive::tests::create_dummy_domain;
    use crate::db::storage::gdrive::file::tests::create_dummy_owner;
    use crate::util::now_no_nanos;

    mod google_application_secret {
        use super::*;

        #[test]
        fn test_debug_elision() {
            let secret = GoogleApplicationSecret { domain_id: 1, secret: serde_json::Value::String("".into()) };
            assert_eq!(format!("{secret:?}"), "GoogleApplicationSecret { domain_id: 1, secret: ... }");
        }

        /// If there is no google_application_secret for a domain, find_by_domain_ids returns an empty Vec
        #[tokio::test]
        async fn test_no_google_application_secret() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert!(GoogleApplicationSecret::find_by_domain_ids(&mut transaction, &[domain.id]).await?.is_empty());

            Ok(())
        }

        /// If we create a google_application_secret, find_by_domain_ids finds it
        #[tokio::test]
        async fn test_create() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            GoogleApplicationSecret { domain_id: domain.id, secret: serde_json::json!({}) }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let secrets = GoogleApplicationSecret::find_by_domain_ids(&mut transaction, &[domain.id]).await?;
            assert_eq!(secrets.len(), 1);
            assert_eq!(secrets[0].domain_id, domain.id);

            Ok(())
        }
    }

    mod google_access_tokens {
        use super::*;

        #[test]
        fn test_debug_elision() {
            let token = GoogleAccessToken { owner_id: 1, access_token: "".into(), refresh_token: "".into(), expires_at: Utc::now() };
            assert!(format!("{token:?}").contains("access_token: ..."));
            assert!(format!("{token:?}").contains("refresh_token: ..."));
        }

        /// If there is no google_access_token for an owner, `find_by_owner_ids` and `find_by_expires_at` return an empty Vec
        #[expect(clippy::needless_collect)]
        #[tokio::test]
        async fn test_no_google_access_tokens() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert!(GoogleAccessToken::find_by_owner_ids(&mut transaction, &[owner.id]).await?.is_empty());
            let out = GoogleAccessToken::find_by_expires_at(&mut transaction, Utc::now()).await?;
            let tokens: Vec<_> = out
                .iter()
                .filter(|token| token.owner_id == owner.id)
                .collect();
            assert!(tokens.is_empty());

            Ok(())
        }

        /// If we create a google_access_token, `find_by_owner_ids` and `find_by_expires_at` find it.
        /// If we delete it, it is no longer found.
        #[tokio::test]
        async fn test_create_delete() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let now = now_no_nanos();
            let token = GoogleAccessToken { owner_id: owner.id, access_token: "A".into(), refresh_token: "R".into(), expires_at: now };
            token.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_eq!(GoogleAccessToken::find_by_owner_ids(&mut transaction, &[owner.id]).await?, vec![token.clone()]);
            assert_eq!(GoogleAccessToken::find_by_expires_at(&mut transaction, now + Duration::hours(1)).await?, vec![token.clone()]);

            token.delete(&mut transaction).await?;
            assert_eq!(GoogleAccessToken::find_by_owner_ids(&mut transaction, &[owner.id]).await?, vec![]);
            assert_eq!(GoogleAccessToken::find_by_expires_at(&mut transaction, now + Duration::hours(1)).await?, vec![]);

            Ok(())
        }
    }

    mod google_service_account {
        use super::*;

        fn dummy_service_account_key() -> ServiceAccountKey {
            ServiceAccountKey {
                key_type: Some("service_account".into()),
                project_id: Some("some-project-id".into()),
                private_key_id: Some("hex".into()),
                private_key: "".into(),
                client_email: "fake@example.com".into(),
                client_id: Some("123456789".into()),
                auth_uri: Some("https://accounts.google.com/o/oauth2/auth".into()),
                token_uri: "".into(),
                auth_provider_x509_cert_url: Some("https://www.googleapis.com/oauth2/v1/certs".into()),
                client_x509_cert_url: Some("https://www.googleapis.com/robot/v1/metadata/x509/...".into()),
            }
        }

        /// If there is no google_service_account for an owner, find_by_owner_ids returns an empty Vec
        #[tokio::test]
        async fn test_no_google_access_tokens() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert!(GoogleServiceAccount::find_by_owner_ids(&mut transaction, &[owner.id], None).await?.is_empty());
            assert!(GoogleServiceAccount::find_by_owner_ids(&mut transaction, &[owner.id], Some(1)).await?.is_empty());

            Ok(())
        }

        /// If we create a google_service_account, find_by_owner_ids finds it
        #[tokio::test]
        async fn test_create() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let account = GoogleServiceAccount { owner_id: owner.id, key: dummy_service_account_key() };
            account.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            for limit in [None, Some(1)] {
                let accounts = GoogleServiceAccount::find_by_owner_ids(&mut transaction, &[owner.id], limit).await?;
                assert_eq!(accounts, vec![account.clone()]);
            }

            Ok(())
        }

        #[test]
        fn test_debug_elision() {
            let account = GoogleServiceAccount { owner_id: 1, key: dummy_service_account_key() };
            assert_eq!(format!("{account:?}"), "GoogleServiceAccount { owner_id: 1, key: ... }");
        }
    }
}
