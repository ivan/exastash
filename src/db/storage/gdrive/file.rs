//! CRUD operations for Google Drive files

use std::collections::HashMap;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use postgres::Row;
use tokio_postgres::Transaction;
use serde::Serialize;
use serde_hex::{SerHex, Strict};
use crate::postgres::{SixteenBytes, UnsignedInt4};

/// An owner of Google Drive files
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GdriveOwner {
    /// ID for this owner
    pub id: i32,
    /// The G Suite domain this owner is associated with
    pub domain: i16,
    /// Email or other identifying string
    pub owner: String,
}

fn from_gdrive_owners(rows: Vec<Row>) -> Result<Vec<GdriveOwner>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(GdriveOwner {
            id: row.get(0),
            domain: row.get(1),
            owner: row.get(2),
        })
    }
    Ok(out)
}

impl GdriveOwner {
    /// Return a `Vec<GdriveOwner>` for all gdrive_owners.
    pub async fn find_all(transaction: &mut Transaction<'_>) -> Result<Vec<GdriveOwner>> {
        let rows = transaction.query("SELECT id, domain, owner FROM gdrive_owners", &[]).await?;
        from_gdrive_owners(rows)
    }

    /// Return a `Vec<GdriveOwner>` for the corresponding list of `owner_ids`.
    /// There is no error on missing owners.
    pub async fn find_by_owner_ids(transaction: &mut Transaction<'_>, owner_ids: &[i32]) -> Result<Vec<GdriveOwner>> {
        let rows = transaction.query("SELECT id, domain, owner FROM gdrive_owners WHERE id = ANY($1)", &[&owner_ids]).await?;
        from_gdrive_owners(rows)
    }

    /// Return a `Vec<GdriveOwner>` for the corresponding list of `domain_ids`.
    /// There is no error on missing domains.
    pub async fn find_by_domain_ids(transaction: &mut Transaction<'_>, domain_ids: &[i16]) -> Result<Vec<GdriveOwner>> {
        let rows = transaction.query("SELECT id, domain, owner FROM gdrive_owners WHERE domain = ANY($1)", &[&domain_ids]).await?;
        from_gdrive_owners(rows)
    }
}

/// A new owner of Google Drive files
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NewGdriveOwner {
    /// The G Suite domain this owner is associated with
    pub domain: i16,
    /// Email or other identifying string
    pub owner: String,
}

impl NewGdriveOwner {
    /// Create a gdrive_owner in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_>) -> Result<GdriveOwner> {
        let rows = transaction.query(
            "INSERT INTO gdrive_owners (domain, owner) VALUES ($1::smallint, $2::text) RETURNING id",
            &[&self.domain, &self.owner]
        ).await?;
        let id = rows.get(0).unwrap().get(0);
        Ok(GdriveOwner { id, domain: self.domain, owner: self.owner.clone() })
    }
}

/// A file in Google Drive, as Google understands it
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GdriveFile {
    /// Google Drive's file_id
    pub id: String,
    /// The email address of the owner, or other identifier like "teamdrive"
    pub owner_id: Option<i32>,
    /// The MD5 hash
    #[serde(with = "SerHex::<Strict>")]
    pub md5: [u8; 16], // TODO: maybe [u32; 4]
    /// The CRC32C
    pub crc32c: u32,
    /// The size of this file in bytes
    pub size: i64,
    /// The time the file was last confirmed to still exist and have correct metadata
    pub last_probed: Option<DateTime<Utc>>,
}

/// Create a gdrive_file in the database.
/// Does not commit the transaction, you must do so yourself.
pub async fn create_gdrive_file(transaction: &mut Transaction<'_>, file: &GdriveFile) -> Result<()> {
    transaction.execute(
        "INSERT INTO gdrive_files (id, owner, md5, crc32c, size, last_probed)
         VALUES ($1::text, $2::int, $3::uuid, $4::int, $5::bigint, $6::timestamptz)",
        &[&file.id, &file.owner_id, &SixteenBytes { bytes: file.md5 }, &UnsignedInt4 { value: file.crc32c }, &file.size, &file.last_probed]
    ).await?;
    Ok(())
}

/// Remove gdrive files in the database.
/// Does not commit the transaction, you must do so yourself.
pub async fn remove_gdrive_files(transaction: &mut Transaction<'_>, ids: &[&str]) -> Result<()> {
    transaction.execute("DELETE FROM gdrive_files WHERE id = ANY($1::text[])", &[&ids]).await?;
    Ok(())
}

/// Return gdrive files with matching ids, in the same order as the ids.
pub async fn get_gdrive_files(transaction: &mut Transaction<'_>, ids: &[&str]) -> Result<Vec<GdriveFile>> {
    let rows = transaction.query("SELECT id, owner, md5, crc32c, size, last_probed FROM gdrive_files WHERE id = ANY($1)", &[&ids]).await?;
    let mut map: HashMap<String, GdriveFile> = HashMap::new();
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let file = GdriveFile {
            id: row.get(0),
            owner_id: row.get(1),
            md5: row.get::<_, SixteenBytes>(2).bytes,
            crc32c: row.get::<_, UnsignedInt4>(3).value,
            size: row.get(4),
            last_probed: row.get(5),
        };
        map.insert(file.id.clone(), file);
    }
    for id in ids {
        let file = map.remove(id.to_owned()).ok_or_else(|| anyhow!("duplicate id given"))?;
        out.push(file);
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;
    use crate::db::inode::tests::create_dummy_file;
    use crate::db::storage::gdrive::tests::create_dummy_domain;
    use crate::db::storage::gdrive::{Storage, Cipher};
    use atomic_counter::{AtomicCounter, RelaxedCounter};
    use once_cell::sync::Lazy;
    use crate::util;

    static OWNER_COUNTER: Lazy<RelaxedCounter> = Lazy::new(|| {
        RelaxedCounter::new(1)
    });


    pub(crate) async fn create_dummy_owner(transaction: &mut Transaction<'_>, domain: i16) -> Result<GdriveOwner> {
        let owner = format!("me-{}@example.com", OWNER_COUNTER.inc());
        Ok(NewGdriveOwner { domain, owner }.create(transaction).await?)
    }

    mod api {
        use super::*;

        // Can create gdrive files
        #[tokio::test]
        async fn test_create_gdrive_file() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let file1 = GdriveFile { id: "A".repeat(28),  owner_id: Some(owner.id), md5: [0; 16], crc32c: 0,   size: 1,    last_probed: None };
            let file2 = GdriveFile { id: "A".repeat(160), owner_id: None,           md5: [0; 16], crc32c: 100, size: 1000, last_probed: Some(util::now_no_nanos()) };
            create_gdrive_file(&mut transaction, &file1).await?;
            create_gdrive_file(&mut transaction, &file2).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            let files = get_gdrive_files(&mut transaction, &[file1.id.as_ref(), file2.id.as_ref()]).await?;
            assert_eq!(files, vec![file1.clone(), file2.clone()]);

            // Files are returned in the same order as ids
            let files = get_gdrive_files(&mut transaction, &[file2.id.as_ref(), file1.id.as_ref()]).await?;
            assert_eq!(files, vec![file2.clone(), file1.clone()]);

            // Empty list is OK
            let files = get_gdrive_files(&mut transaction, &[]).await?;
            assert_eq!(files, vec![]);

            // Duplicate id is not OK
            let result = get_gdrive_files(&mut transaction, &[file1.id.as_ref(), file2.id.as_ref(), file1.id.as_ref()]).await;
            assert_eq!(result.err().expect("expected an error").to_string(), "duplicate id given");

            Ok(())
        }

        // Can remove gdrive files not referenced by storage_gdrive
        #[tokio::test]
        async fn test_remove_gdrive_files() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let file = GdriveFile { id: "Q".repeat(28), owner_id: Some(owner.id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            remove_gdrive_files(&mut transaction, &[&file.id]).await?;
            transaction.commit().await?;

            Ok(())
        }

        // Cannot remove gdrive files that are referenced by storage_gdrive
        #[tokio::test]
        async fn test_cannot_remove_gdrive_files_still_referenced() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let file = GdriveFile { id: "M".repeat(28), owner_id: Some(owner.id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file).await?;
            // create_storage expects the domain to already be committed
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            let storage = Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file.clone()] };
            storage.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            let result = remove_gdrive_files(&mut transaction, &[&file.id]).await;
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                format!("db error: ERROR: gdrive_files={} is still referenced by storage_gdrive={}", file.id, dummy.id)
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
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let file = GdriveFile { id: "B".repeat(28), owner_id: Some(owner.id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file).await?;
            transaction.commit().await?;

            let new_id = format!("'{}'", "C".repeat(28));
            for (column, value) in &[
                ("id", new_id.as_str()),
                ("md5", "'0000-0000-0000-0000-0000-0000-0000-0001'::uuid"),
                ("crc32c", "1"),
                ("size", "2")
            ] {
                let transaction = start_transaction(&mut client).await?;
                let query = format!("UPDATE gdrive_files SET {} = {} WHERE id = $1", column, value);
                let result = transaction.execute(query.as_str(), &[&file.id]).await;
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id, md5, crc32c, or size");
            }

            Ok(())
        }

        /// Cannot TRUNCATE gdrive_files table
        #[tokio::test]
        async fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let owner = create_dummy_owner(&mut transaction, domain.id).await?;
            let file = GdriveFile { id: "D".repeat(28), owner_id: Some(owner.id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            assert_cannot_truncate(&mut transaction, "gdrive_files").await;

            Ok(())
        }
    }
}
