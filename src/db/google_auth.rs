//! CRUD operations for Google OAuth 2.0 and service account entities in PostgreSQL

use std::fmt;
use anyhow::Result;
use chrono::{DateTime, Utc};
use yup_oauth2::ServiceAccountKey;
use tokio_postgres::{Transaction, Row};
use custom_debug_derive::Debug as CustomDebug;

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

fn from_gsuite_application_secrets(rows: Vec<Row>) -> Result<Vec<GsuiteApplicationSecret>> {
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

impl GsuiteApplicationSecret {
    /// Create a gsuite_application_secret in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_>) -> Result<Self> {
        transaction.execute(
            "INSERT INTO gsuite_application_secrets (domain_id, secret)
             VALUES ($1::smallint, $2::jsonb)",
            &[&self.domain_id, &self.secret]
        ).await?;
        Ok(self)
    }

    /// Return a `Vec<GsuiteApplicationSecret>` of all gsuite_application_secrets.
    pub async fn find_all(transaction: &mut Transaction<'_>) -> Result<Vec<GsuiteApplicationSecret>> {
        let rows = transaction.query("SELECT domain_id, secret FROM gsuite_application_secrets", &[]).await?;
        from_gsuite_application_secrets(rows)
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
        from_gsuite_application_secrets(rows)
    }
}

/// A gsuite_access_token entity
#[derive(Clone, CustomDebug, PartialEq, Eq)]
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

fn from_gsuite_access_tokens(rows: Vec<Row>) -> Result<Vec<GsuiteAccessToken>> {
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

impl GsuiteAccessToken {
    /// Create a gsuite_access_token in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_>) -> Result<Self> {
        transaction.execute(
            "INSERT INTO gsuite_access_tokens (owner_id, access_token, refresh_token, expires_at)
             VALUES ($1::int, $2::text, $3::text, $4::timestamptz)",
            &[&self.owner_id, &self.access_token, &self.refresh_token, &self.expires_at]
        ).await?;
        Ok(self)
    }

    /// Delete this access token from the database, by its owner id.
    /// There is no error if the owner does not exist.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn delete(&self, transaction: &mut Transaction<'_>) -> Result<()> {
        transaction.execute("DELETE FROM gsuite_access_tokens WHERE owner_id = $1::int", &[&self.owner_id]).await?;
        Ok(())
    }

    /// Return a `Vec<GsuiteAccessToken>` of tokens that expire before `expires_at`.
    pub async fn find_by_expires_at(transaction: &mut Transaction<'_>, expires_at: DateTime<Utc>) -> Result<Vec<GsuiteAccessToken>> {
        let rows = transaction.query(
            "SELECT owner_id, access_token, refresh_token, expires_at
             FROM gsuite_access_tokens
             WHERE expires_at < $1::timestamptz",
            &[&expires_at]
        ).await?;
        from_gsuite_access_tokens(rows)
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
        from_gsuite_access_tokens(rows)
    }
}

/// A gsuite_service_Account entity
#[derive(Clone, CustomDebug, PartialEq, Eq)]
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
    pub async fn create(self, transaction: &mut Transaction<'_>) -> Result<Self> {
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
        Ok(self)
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
    use chrono::Duration;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;    
    use crate::db::storage::gdrive::tests::create_dummy_domain;
    use crate::db::storage::gdrive::file::tests::create_dummy_owner;
    use crate::util::now_no_nanos;

    mod gsuite_application_secret {
        use super::*;

        #[test]
        fn test_debug_elision() {
            let secret = GsuiteApplicationSecret { domain_id: 1, secret: serde_json::Value::String("".into()) };
            assert_eq!(format!("{:?}", secret), "GsuiteApplicationSecret { domain_id: 1, secret: ... }");
        }    

        /// If there is no gsuite_application_secret for a domain, find_by_domain_ids returns an empty Vec
        #[tokio::test]
        async fn test_no_gsuite_application_secret() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            assert!(GsuiteApplicationSecret::find_by_domain_ids(&mut transaction, &[domain.id]).await?.is_empty());

            Ok(())
        }

        /// If we create a gsuite_application_secret, find_by_domain_ids finds it
        #[tokio::test]
        async fn test_create() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            GsuiteApplicationSecret { domain_id: domain.id, secret: serde_json::json!({}) }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            let secrets = GsuiteApplicationSecret::find_by_domain_ids(&mut transaction, &[domain.id]).await?;
            assert_eq!(secrets.len(), 1);
            assert_eq!(secrets[0].domain_id, domain.id);

            Ok(())
        }
    }

    mod gsuite_access_tokens {
        use super::*;

        #[test]
        fn test_debug_elision() {
            let token = GsuiteAccessToken { owner_id: 1, access_token: "".into(), refresh_token: "".into(), expires_at: Utc::now() };
            assert!(format!("{:?}", token).contains("access_token: ..."));
            assert!(format!("{:?}", token).contains("refresh_token: ..."));
        }

        /// If there is no gsuite_access_token for an owner, `find_by_owner_ids` and `find_by_expires_at` return an empty Vec
        #[tokio::test]
        async fn test_no_gsuite_access_tokens() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            assert!(GsuiteAccessToken::find_by_owner_ids(&mut transaction, &[owner.id]).await?.is_empty());
            let out = GsuiteAccessToken::find_by_expires_at(&mut transaction, Utc::now()).await?;
            let tokens: Vec<_> = out
                .iter()
                .filter(|token| token.owner_id == owner.id)
                .collect();
            assert!(tokens.is_empty());

            Ok(())
        }

        /// If we create a gsuite_access_token, `find_by_owner_ids` and `find_by_expires_at` find it.
        /// If we delete it, it is no longer found.
        #[tokio::test]
        async fn test_create_delete() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let now = now_no_nanos();
            let token = GsuiteAccessToken { owner_id: owner.id, access_token: "A".into(), refresh_token: "R".into(), expires_at: now }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            assert_eq!(GsuiteAccessToken::find_by_owner_ids(&mut transaction, &[owner.id]).await?, vec![token.clone()]);
            assert_eq!(GsuiteAccessToken::find_by_expires_at(&mut transaction, now + Duration::hours(1)).await?, vec![token.clone()]);

            token.delete(&mut transaction).await?;
            assert_eq!(GsuiteAccessToken::find_by_owner_ids(&mut transaction, &[owner.id]).await?, vec![]);
            assert_eq!(GsuiteAccessToken::find_by_expires_at(&mut transaction, now + Duration::hours(1)).await?, vec![]);            

            Ok(())
        }
    }

    mod gsuite_service_account {
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

        /// If there is no gsuite_service_account for an owner, find_by_owner_ids returns an empty Vec
        #[tokio::test]
        async fn test_no_gsuite_access_tokens() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            assert!(GsuiteServiceAccount::find_by_owner_ids(&mut transaction, &[owner.id], None).await?.is_empty());
            assert!(GsuiteServiceAccount::find_by_owner_ids(&mut transaction, &[owner.id], Some(1)).await?.is_empty());

            Ok(())
        }

        /// If we create a gsuite_service_account, find_by_owner_ids finds it
        #[tokio::test]
        async fn test_create() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let account = GsuiteServiceAccount { owner_id: owner.id, key: dummy_service_account_key() }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            for limit in &[None, Some(1_i32)] {
                let accounts = GsuiteServiceAccount::find_by_owner_ids(&mut transaction, &[owner.id], *limit).await?;
                assert_eq!(accounts, vec![account.clone()]);
            }

            Ok(())
        }

        #[test]
        fn test_debug_elision() {
            let account = GsuiteServiceAccount { owner_id: 1, key: dummy_service_account_key() };
            assert_eq!(format!("{:?}", account), "GsuiteServiceAccount { owner_id: 1, key: ... }");
        }    
    }
}
