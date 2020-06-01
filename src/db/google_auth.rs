//! CRUD operations for Google OAuth 2.0 and service account entities in PostgreSQL

use std::fmt;
use anyhow::Result;
use chrono::{DateTime, Utc};
use yup_oauth2::ServiceAccountKey;
use tokio_postgres::Transaction;
use custom_debug_derive::CustomDebug;

#[inline]
fn elide<T: fmt::Debug>(_: &T, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "...")
}

/// A gsuite_application_secret entity
#[derive(Clone, CustomDebug)]
pub struct GsuiteApplicationSecret {
    /// The gsuite_domain this secret is for
    pub domain_id: i16,
    /// The secret itself, a JSON object with an "installed" key
    #[debug(with = "elide")]
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


    /// Return a `Vec<GsuiteApplicationSecret>` for the corresponding list of `domain_ids`.
    /// There is no error on missing domains.
    pub async fn find_by_domain_ids(transaction: &mut Transaction<'_>, domain_ids: &[i16]) -> Result<Vec<GsuiteApplicationSecret>> {
        let rows = transaction.query(
            "SELECT domain_id, secret
             FROM gsuite_application_secrets
             WHERE domain_id = ANY($1::smallint[])",
            &[&domain_ids]
        ).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(
                GsuiteApplicationSecret {
                    domain_id: row.get(0),
                    secret: row.get(1),
                }
            );
        }
        Ok(out)
    }
}

/// A gsuite_access_token entity
#[derive(Clone, CustomDebug)]
pub struct GsuiteAccessToken {
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

    /// Return a `Vec<GsuiteAccessToken>` for the corresponding list of `owner_ids`.
    /// There is no error on missing owners.
    pub async fn find_by_owner_ids(transaction: &mut Transaction<'_>, owner_ids: &[i32]) -> Result<Vec<GsuiteAccessToken>> {
        let rows = transaction.query(
            "SELECT owner_id, access_token, refresh_token, expires_at
             FROM gsuite_access_tokens
             WHERE owner_id = ANY($1::int[])",
            &[&owner_ids]
        ).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(
                GsuiteAccessToken {
                    owner_id: row.get(0),
                    access_token: row.get(1),
                    refresh_token: row.get(2),
                    expires_at: row.get(3),
                }
            );
        }
        Ok(out)
    }
}

/// A gsuite_service_Account entity
#[derive(Clone, CustomDebug)]
pub struct GsuiteServiceAccount {
    /// The gdrive_owner this service account is for
    pub owner_id: i32,
    /// The key for this service account
    #[debug(with = "elide")]
    pub key: ServiceAccountKey,
}

impl GsuiteServiceAccount {
    /// Create a gsuite_service_account in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_>) -> Result<()> {
        let k = &self.key;
        transaction.execute(
            "INSERT INTO gsuite_service_accounts (
                owner_id, client_email, client_id, project_id, private_key_id, private_key,
                auth_uri, token_uri, auth_provider_x509_cert_url, client_x509_cert_url)
             VALUES ($1::int, $2::text, $3::text, $4::text, $5::text, $6::text, $7::text, $8::text, $9::text, $10::text)",
            &[
                &self.owner_id, &k.client_email, &k.client_id, &k.project_id, &k.private_key_id, &k.private_key,
                &k.auth_uri, &k.token_uri, &k.auth_provider_x509_cert_url, &k.client_x509_cert_url
            ]
        ).await?;
        Ok(())
    }

    /// Return a `Vec<GsuiteServiceAccount>` for the corresponding list of `owner_ids`.
    /// There is no error on missing owners.
    /// If limit is not `None`, returns max `N` random rows.
    pub async fn find_by_owner_ids(transaction: &mut Transaction<'_>, owner_ids: &[i32], limit: Option<i32>) -> Result<Vec<GsuiteServiceAccount>> {
        let limit_sql = match limit {
            None => "".into(),
            Some(num) => format!("ORDER BY random() LIMIT {}", num)
        };
        let sql = format!("SELECT owner_id, client_email, client_id, project_id, private_key_id, private_key,
                                  auth_uri, token_uri, auth_provider_x509_cert_url, client_x509_cert_url
                           FROM gsuite_service_accounts
                           WHERE owner_id = ANY($1::int[])
                           {}", limit_sql);
        let rows = transaction.query(sql.as_str(), &[&owner_ids]).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(
                GsuiteServiceAccount {
                    owner_id: row.get(0),
                    key: ServiceAccountKey {
                        client_email: row.get(1),
                        client_id: row.get(2),
                        project_id: row.get(3),
                        private_key_id: row.get(4),
                        private_key: row.get(5),
                        auth_uri: row.get(6),
                        token_uri: row.get(7),
                        auth_provider_x509_cert_url: row.get(8),
                        client_x509_cert_url: row.get(9),
                        key_type: Some("service_account".into())
                    }
                }
            );
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_service_account_key() -> ServiceAccountKey {
        ServiceAccountKey {
            key_type: None,
            project_id: None,
            private_key_id: None,
            private_key: "".into(),
            client_email: "".into(),
            client_id: None,
            auth_uri: None,
            token_uri: "".into(),
            auth_provider_x509_cert_url: None,
            client_x509_cert_url: None,
        }
    }

    #[test]
    fn test_debug_elision() {
        let secret = GsuiteApplicationSecret { domain_id: 1, secret: serde_json::Value::String("".into()) };
        assert_eq!(format!("{:?}", secret), "GsuiteApplicationSecret { domain_id: 1, secret: ... }");

        let token = GsuiteAccessToken { owner_id: 1, access_token: "".into(), refresh_token: "".into(), expires_at: Utc::now() };
        assert!(format!("{:?}", token).contains("access_token: ..."));
        assert!(format!("{:?}", token).contains("refresh_token: ..."));

        let account = GsuiteServiceAccount { owner_id: 1, key: dummy_service_account_key() };
        assert_eq!(format!("{:?}", account), "GsuiteServiceAccount { owner_id: 1, key: ... }");
    }
}
