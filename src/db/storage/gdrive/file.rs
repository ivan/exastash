//! CRUD operations for Google Drive files

use std::collections::HashMap;
use std::fmt::Debug;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction, Row};
use sqlx::postgres::PgRow;
use serde::Serialize;
use serde_hex::{SerHex, Strict};
use futures_async_stream::for_await;
use uuid::Uuid;

/// An owner of Google Drive files
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct GdriveOwner {
    /// ID for this owner
    pub id: i32,
    /// The google domain this owner is associated with
    pub domain: i16,
    /// Email or other identifying string
    pub owner: String,
}

impl GdriveOwner {
    /// Return a `Vec<GdriveOwner>` for all gdrive_owners.
    pub async fn find_all(transaction: &mut Transaction<'_, Postgres>) -> Result<Vec<GdriveOwner>> {
        Ok(sqlx::query_as::<_, GdriveOwner>("SELECT id, domain, owner FROM gdrive_owners")
            .fetch_all(transaction).await?)
    }

    /// Return a `Vec<GdriveOwner>` for the corresponding list of `owner_ids`.
    /// There is no error on missing owners.
    pub async fn find_by_owner_ids(transaction: &mut Transaction<'_, Postgres>, owner_ids: &[i32]) -> Result<Vec<GdriveOwner>> {
        Ok(sqlx::query_as::<_, GdriveOwner>("SELECT id, domain, owner FROM gdrive_owners WHERE id = ANY($1)")
            .bind(owner_ids)
            .fetch_all(transaction).await?)
    }

    /// Return a `Vec<GdriveOwner>` for the corresponding list of `domain_ids`.
    /// There is no error on missing domains.
    pub async fn find_by_domain_ids(transaction: &mut Transaction<'_, Postgres>, domain_ids: &[i16]) -> Result<Vec<GdriveOwner>> {
        Ok(sqlx::query_as::<_, GdriveOwner>("SELECT id, domain, owner FROM gdrive_owners WHERE domain = ANY($1)")
            .bind(domain_ids)
            .fetch_all(transaction).await?)
    }
}

/// A new owner of Google Drive files
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NewGdriveOwner {
    /// The google domain this owner is associated with
    pub domain: i16,
    /// Email or other identifying string
    pub owner: String,
}

impl NewGdriveOwner {
    /// Create a gdrive_owner in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<GdriveOwner> {
        let row = sqlx::query("INSERT INTO gdrive_owners (domain, owner) VALUES ($1::smallint, $2::text) RETURNING id")
            .bind(&self.domain)
            .bind(&self.owner)
            .fetch_one(transaction)
            .await?;
        let id: i32 = row.get(0);
        Ok(GdriveOwner {
            id,
            domain: self.domain,
            owner: self.owner,
        })
    }
}

/// A file in Google Drive, as Google understands it
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GdriveFile {
    /// Google Drive's file_id
    pub id: String,
    /// The email address of the owner, or other identifier like "teamdrive"
    pub owner_id: Option<i32>,
    /// The MD5 hash of the content of this chunk file
    #[serde(with = "SerHex::<Strict>")]
    pub md5: [u8; 16], // TODO: maybe [u32; 4]
    /// The CRC32C of the content of this chunk file
    pub crc32c: u32,
    /// The size of this file in bytes
    pub size: i64,
    /// The time the file was last confirmed to still exist and have correct metadata
    pub last_probed: Option<DateTime<Utc>>,
}

impl<'c> sqlx::FromRow<'c, PgRow> for GdriveFile {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(GdriveFile {
            id: row.get("id"),
            owner_id: row.get("owner"),
            md5: *row.get::<Uuid, _>("md5").as_bytes(),
            crc32c: row.get::<i32, _>("crc32c") as u32,
            size: row.get("size"),
            last_probed: row.get("last_probed"),
        })
    }
}

impl GdriveFile {
    /// Create a gdrive_file in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query("INSERT INTO gdrive_files (id, owner, md5, crc32c, size, last_probed)
                     VALUES ($1::text, $2::int, $3::uuid, $4::int, $5::bigint, $6::timestamptz)")
            .bind(&self.id)
            .bind(&self.owner_id)
            .bind(Uuid::from_bytes(self.md5))
            .bind(self.crc32c as i32)
            .bind(&self.size)
            .bind(&self.last_probed)
            .execute(transaction).await?;
        Ok(())
    }

    /// Remove gdrive files in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[&str]) -> Result<()> {
        sqlx::query("DELETE FROM gdrive_files WHERE id = ANY($1::text[])")
            .bind(ids)
            .execute(transaction).await?;
        Ok(())
    }

    /// Return gdrive files with matching ids, in the same order as the ids.
    pub async fn find_by_ids_in_order(transaction: &mut Transaction<'_, Postgres>, ids: &[&str]) -> Result<Vec<GdriveFile>> {
        let query = "SELECT id, owner, md5, crc32c, size, last_probed FROM gdrive_files WHERE id = ANY($1)";
        let cursor = sqlx::query_as::<_, GdriveFile>(query)
            .bind(ids)
            .fetch(transaction);
        let mut out = Vec::with_capacity(cursor.size_hint().1.unwrap_or(ids.len()));
        let mut map: HashMap<String, GdriveFile> = HashMap::new();
        #[for_await]
        for file in cursor {
            let file = file?;
            map.insert(file.id.to_string(), file);
        }
        for id in ids {
            let file = map.remove(&id.to_string()).ok_or_else(|| anyhow!("duplicate or nonexistent id given: {:?}", id))?;
            out.push(file);
        }
        Ok(out)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::tests::{new_primary_pool, new_secondary_pool};
    use crate::db::inode::tests::create_dummy_file;
    use crate::db::storage::gdrive::tests::create_dummy_domain;
    use crate::db::storage::gdrive::{Storage, Cipher};
    use atomic_counter::{AtomicCounter, RelaxedCounter};
    use once_cell::sync::Lazy;
    use crate::util;
    use serial_test::serial;

    static OWNER_COUNTER: Lazy<RelaxedCounter> = Lazy::new(|| {
        RelaxedCounter::new(1)
    });


    pub(crate) async fn create_dummy_owner(transaction: &mut Transaction<'_, Postgres>, domain: i16) -> Result<GdriveOwner> {
        let owner = format!("me-{}@example.com", OWNER_COUNTER.inc());
        Ok(NewGdriveOwner { domain, owner }.create(transaction).await?)
    }

    mod api {
        use super::*;

        // Can create gdrive files
        #[tokio::test]
        async fn test_create_gdrive_file() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let file1 = GdriveFile { id: "A".repeat(28),  owner_id: Some(owner.id), md5: [0; 16], crc32c: 0,   size: 1,    last_probed: None };
            file1.create(&mut transaction).await?;
            let file2 = GdriveFile { id: "A".repeat(160), owner_id: None,           md5: [0; 16], crc32c: 100, size: 1000, last_probed: Some(util::now_no_nanos()) };
            file2.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let files = GdriveFile::find_by_ids_in_order(&mut transaction, &[&file1.id, &file2.id]).await?;
            assert_eq!(files, vec![file1.clone(), file2.clone()]);

            // Files are returned in the same order as ids
            let files = GdriveFile::find_by_ids_in_order(&mut transaction, &[&file2.id, &file1.id]).await?;
            assert_eq!(files, vec![file2.clone(), file1.clone()]);

            // Duplicate id is not OK
            let result = GdriveFile::find_by_ids_in_order(&mut transaction, &[&file1.id, &file2.id, &file1.id]).await;
            assert_eq!(result.err().expect("expected an error").to_string(), format!("duplicate or nonexistent id given: {:?}", file1.id));

            // Nonexistent id is not OK
            let result = GdriveFile::find_by_ids_in_order(&mut transaction, &[&file1.id, &file2.id, "nonexistent"]).await;
            assert_eq!(result.err().expect("expected an error").to_string(), "duplicate or nonexistent id given: \"nonexistent\"");

            Ok(())
        }

        // Can remove gdrive files not referenced by storage_gdrive
        #[tokio::test]
        async fn test_remove_gdrive_files() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let file = GdriveFile { id: "Q".repeat(28), owner_id: Some(owner.id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            file.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            GdriveFile::remove_by_ids(&mut transaction, &[&file.id]).await?;
            transaction.commit().await?;

            Ok(())
        }

        // Cannot remove gdrive files that are referenced by storage_gdrive
        #[tokio::test]
        async fn test_cannot_remove_gdrive_files_still_referenced() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let file = GdriveFile { id: "M".repeat(28), owner_id: Some(owner.id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            file.create(&mut transaction).await?;
            // create_storage expects the domain to already be committed
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            Storage { file_id: dummy.id, google_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file.id.clone()] }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let result = GdriveFile::remove_by_ids(&mut transaction, &[&file.id]).await;
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                format!("error returned from database: gdrive_files={} is still referenced by storage_gdrive={}", file.id, dummy.id)
            );

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::tests::assert_cannot_truncate;

        /// Cannot UPDATE any row in gdrive_files table
        #[tokio::test]
        async fn test_cannot_update() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let file = GdriveFile { id: "B".repeat(28), owner_id: Some(owner.id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            file.create(&mut transaction).await?;
            transaction.commit().await?;

            let new_id = format!("'{}'", "C".repeat(28));
            for (column, value) in &[
                ("id", new_id.as_str()),
                ("md5", "'0000-0000-0000-0000-0000-0000-0000-0001'::uuid"),
                ("crc32c", "1"),
                ("size", "2")
            ] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE gdrive_files SET {column} = {value} WHERE id = $1");
                let result = sqlx::query(&query).bind(&file.id).execute(&mut transaction).await;
                assert_eq!(result.err().expect("expected an error").to_string(), "error returned from database: cannot change id, md5, crc32c, or size");
            }

            Ok(())
        }

        /// Cannot TRUNCATE gdrive_files table
        #[tokio::test]
        #[serial]
        async fn test_cannot_truncate() -> Result<()> {
            let pool = new_secondary_pool().await;

            let mut transaction = pool.begin().await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            GdriveFile { id: "D".repeat(28), owner_id: Some(owner.id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_cannot_truncate(&mut transaction, "gdrive_files").await;

            Ok(())
        }
    }
}
