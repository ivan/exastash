//! CRUD operations for storage_gdrive entities in PostgreSQL

use anyhow::Result;
use futures_async_stream::for_await;
use sqlx::{Postgres, Transaction};
use serde::Serialize;
use serde_hex::{SerHex, Strict};
use uuid::Uuid;

pub mod file;

/// The encryption algorithm used to encrypt the chunks
#[must_use]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, sqlx::Type)]
#[sqlx(type_name = "cipher")]
pub enum Cipher {
    /// AES-128-CTR
    #[sqlx(rename = "AES_128_CTR")]
    #[serde(rename = "AES_128_CTR")]
    Aes128Ctr,
    /// AES-128-GCM
    #[sqlx(rename = "AES_128_GCM")]
    #[serde(rename = "AES_128_GCM")]
    Aes128Gcm,
}

/// A Google Drive folder into which files are uploaded
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct GdriveParent {
    /// Arbitrary name for the folder
    pub name: String,
    /// The Google Drive id for the folder
    pub parent: String,
    /// Whether the folder is full
    pub full: bool,
}

impl GdriveParent {
    /// Create an gdrive_parent entity in the database.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!(r#"INSERT INTO stash.gdrive_parents (name, parent, "full") VALUES ($1, $2, $3)"#, self.name, self.parent, self.full)
            .execute(transaction).await?;
        Ok(())
    }

    /// Find a gdrive_parent entity by name.
    pub async fn find_by_name(transaction: &mut Transaction<'_, Postgres>, name: &str) -> Result<Option<GdriveParent>> {
        let mut parents = sqlx::query_as!(GdriveParent, r#"SELECT name, parent, "full" FROM stash.gdrive_parents WHERE name = $1"#, name)
            .fetch_all(transaction).await?;
        Ok(parents.pop())
    }

    /// Find the first gdrive_parent that is not full.
    pub async fn find_first_non_full(transaction: &mut Transaction<'_, Postgres>) -> Result<Option<GdriveParent>> {
        Ok(sqlx::query_as!(GdriveParent, r#"SELECT name, parent, "full" FROM stash.gdrive_parents WHERE "full" = false"#)
            .fetch_optional(transaction).await?)
    }

    /// Set whether a parent is full or not
    pub async fn set_full(transaction: &mut Transaction<'_, Postgres>, name: &str, full: bool) -> Result<()> {
        sqlx::query!(r#"UPDATE stash.gdrive_parents SET "full" = $1 WHERE name = $2"#, full, name)
            .execute(transaction).await?;
        Ok(())
    }
}

/// A domain where Google Drive files are stored
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct GoogleDomain {
    /// ID for this domain
    pub id: i16,
    /// The domain name
    pub domain: String,
}

/// A new domain name
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NewGoogleDomain {
    /// The domain name
    pub domain: String,
}

impl NewGoogleDomain {
    /// Create a google_domain in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<GoogleDomain> {
        let id = sqlx::query_scalar!("INSERT INTO stash.google_domains (domain) VALUES ($1) RETURNING id", self.domain)
            .fetch_one(transaction).await?;
        Ok(GoogleDomain {
            id,
            domain: self.domain,
        })
    }
}

/// google_domain-specific descriptor that specifies where to place
/// new Google Drive files, and with which owner.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct GdriveFilePlacement {
    /// id of a google_domain
    pub domain: i16,
    /// id of a gdrive_owner
    pub owner: i32,
    /// name of a gdrive_parent
    pub parent: String,
}

impl GdriveFilePlacement {
    /// Create a gdrive_file_placement in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!("INSERT INTO stash.gdrive_file_placement (domain, owner, parent) VALUES ($1, $2, $3)", self.domain, self.owner, self.parent)
            .execute(transaction).await?;
        Ok(())
    }

    /// Remove this gdrive_file_placement from the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!("DELETE FROM stash.gdrive_file_placement WHERE domain = $1 AND owner = $2 AND parent = $3", self.domain, self.owner, self.parent)
            .execute(transaction).await?;
        Ok(())
    }

    /// Return a `Vec<GdriveFilePlacement>` for domain `domain`.
    /// There is no error if the domain id does not exist.
    /// Rows are always returned in random order.
    /// If limit is not `None`, returns max `N` rows.
    pub async fn find_by_domain(transaction: &mut Transaction<'_, Postgres>, domain: i16, limit: Option<i64>) -> Result<Vec<GdriveFilePlacement>> {
        let placements = sqlx::query_as!(GdriveFilePlacement, "
            SELECT domain, owner, parent FROM stash.gdrive_file_placement
            WHERE domain = $1
            ORDER BY random()
            LIMIT $2",
            domain, limit
        ).fetch_all(transaction).await?;
        Ok(placements)
    }

    /// Return a `Vec<GdriveFilePlacement>` if one exists in the database for this placement,
    /// and lock the row for update.
    pub async fn find_self_and_lock(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<Option<GdriveFilePlacement>> {
        let placement = sqlx::query_as!(GdriveFilePlacement, "
            SELECT domain, owner, parent FROM stash.gdrive_file_placement
            WHERE domain = $1 AND owner = $2 AND parent = $3
            FOR UPDATE",
            self.domain, self.owner, self.parent
        ).fetch_optional(transaction).await?;
        Ok(placement)
    }
}

/// A storage_gdrive entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Storage {
    /// The id of the exastash file for which this storage exists
    pub file_id: i64,
    /// The domain for the google account
    pub google_domain: i16,
    /// The encryption algorithm used to encrypt the chunks in gdrive
    pub cipher: Cipher,
    /// The cipher key used to encrypt the chunks in gdrive
    #[serde(with = "SerHex::<Strict>")]
    pub cipher_key: [u8; 16],
    /// An ordered list of gdrive file IDs
    pub gdrive_ids: Vec<String>,
}

impl From<StorageRow> for Storage {
    fn from(row: StorageRow) -> Self {
        Storage {
            file_id: row.file_id,
            google_domain: row.google_domain,
            cipher: row.cipher,
            cipher_key: *row.cipher_key.as_bytes(),
            gdrive_ids: row.gdrive_ids,
        }
    }
}

struct StorageRow {
    file_id: i64,
    google_domain: i16,
    cipher: Cipher,
    cipher_key: Uuid,
    gdrive_ids: Vec<String>,
}

impl Storage {
    /// Create an gdrive storage entity in the database.
    /// Note that the google domain must already exist.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!(
            "INSERT INTO stash.storage_gdrive (file_id, google_domain, cipher, cipher_key, gdrive_ids)
             VALUES ($1, $2, $3, $4, $5)",
             self.file_id,
             self.google_domain,
             self.cipher as _,
             Uuid::from_bytes(self.cipher_key),
             &self.gdrive_ids
        ).execute(transaction).await?;
        Ok(())
    }

    /// Remove storages with given `ids`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<()> {
        if file_ids.is_empty() {
            return Ok(());
        }
        sqlx::query!("DELETE FROM stash.storage_gdrive WHERE file_id = ANY($1)", file_ids)
            .execute(transaction).await?;
        Ok(())
    }

    /// Return a list of gdrive storage entities where the data for a file can be retrieved.
    pub async fn find_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        if file_ids.is_empty() {
            return Ok(vec![]);
        }
        // Note that we can get more than one row per unique file_id
        let cursor = sqlx::query_as!(StorageRow,
            r#"SELECT file_id, google_domain, cipher as "cipher: Cipher", cipher_key, gdrive_ids
             FROM stash.storage_gdrive
             WHERE file_id = ANY($1)"#,
             file_ids
        )
            .fetch(transaction);
        let mut storages = Vec::with_capacity(cursor.size_hint().1.unwrap_or(file_ids.len()));
        #[for_await]
        for row in cursor {
            let storage: Storage = row?.into();
            storages.push(storage);
        }
        Ok(storages)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::tests::{new_primary_pool, new_secondary_pool};
    use crate::db::inode::create_dummy_file;
    use file::GdriveFile;
    use atomic_counter::{AtomicCounter, RelaxedCounter};
    use once_cell::sync::Lazy;
    use serial_test::serial;

    static DOMAIN_COUNTER: Lazy<RelaxedCounter> = Lazy::new(|| {
        RelaxedCounter::new(1)
    });

    pub(crate) async fn create_dummy_domain(transaction: &mut Transaction<'_, Postgres>) -> Result<GoogleDomain> {
        let num = DOMAIN_COUNTER.inc();
        let domain = format!("{num}.example.com");
        NewGoogleDomain { domain }.create(transaction).await
    }

    mod api {
        use super::*;

        /// Test GdriveParent
        #[tokio::test]
        async fn test_gdrive_parent() -> Result<()> {
            let pool = new_primary_pool().await;

            // Can create a gdrive_parent
            let mut transaction = pool.begin().await?;
            let gdrive_parent = GdriveParent { name: "test_gdrive_parent".into(), parent: "this_is_not_a_real_gdrive_id".into(), full: false };
            gdrive_parent.create(&mut transaction).await?;
            transaction.commit().await?;

            // Can get the gdrive_parent we just created
            let mut transaction = pool.begin().await?;
            let maybe_gdrive_parent = GdriveParent::find_by_name(&mut transaction, "test_gdrive_parent").await?;
            assert_eq!(maybe_gdrive_parent, Some(gdrive_parent.clone()));

            // Can set the gdrive_parent to full = true
            let mut transaction = pool.begin().await?;
            GdriveParent::set_full(&mut transaction, "test_gdrive_parent", true).await?;
            let maybe_gdrive_parent = GdriveParent::find_by_name(&mut transaction, "test_gdrive_parent").await?;
            transaction.commit().await?;
            let gdrive_parent_full = GdriveParent { name: "test_gdrive_parent".into(), parent: "this_is_not_a_real_gdrive_id".into(), full: true };
            assert_eq!(maybe_gdrive_parent, Some(gdrive_parent_full));

            // Can set the gdrive_parent back to full = false
            let mut transaction = pool.begin().await?;
            GdriveParent::set_full(&mut transaction, "test_gdrive_parent", false).await?;
            let maybe_gdrive_parent = GdriveParent::find_by_name(&mut transaction, "test_gdrive_parent").await?;
            transaction.commit().await?;
            assert_eq!(maybe_gdrive_parent, Some(gdrive_parent.clone()));

            Ok(())
        }

        /// If we add a gdrive storage for a file, get_storages returns that storage
        #[tokio::test]
        async fn test_create_storage_get_storages() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file1 = GdriveFile { id: "X".repeat(28),  owner_id: None, md5: [0; 16], crc32c: 0,   size: 1,    last_probed: None };
            file1.create(&mut transaction).await?;
            let file2 = GdriveFile { id: "X".repeat(160), owner_id: None, md5: [0; 16], crc32c: 100, size: 1000, last_probed: None };
            file2.create(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, google_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file1.id, file2.id] };
            storage.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[dummy.id]).await?, vec![storage]);

            Ok(())
        }

        /// Cannot reference a nonexistent gdrive file
        #[tokio::test]
        async fn test_cannot_reference_nonexistent_gdrive_file() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file = GdriveFile { id: "FileNeverAddedToDatabase".into(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, google_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file.id] };
            let result = storage.create(&mut transaction).await;
            assert_eq!(
                result.expect_err("expected an error").to_string(),
                "error returned from database: gdrive_ids had 1 ids: {FileNeverAddedToDatabase} but only 0 of these are in gdrive_files"
            );

            Ok(())
        }

        /// Cannot reference a nonexistent gdrive file even when other gdrive files do exist
        #[tokio::test]
        async fn test_cannot_reference_nonexistent_gdrive_file_even_if_some_exist() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file1 = GdriveFile { id: "F".repeat(28), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            file1.create(&mut transaction).await?;
            let file2 = GdriveFile { id: "FileNeverAddedToDatabase".into(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, google_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file1.id, file2.id] };
            let result = storage.create(&mut transaction).await;
            assert_eq!(
                result.expect_err("expected an error").to_string(),
                "error returned from database: gdrive_ids had 2 ids: {FFFFFFFFFFFFFFFFFFFFFFFFFFFF,FileNeverAddedToDatabase} but only 1 of these are in gdrive_files"
            );

            Ok(())
        }

        /// Cannot have empty gdrive_files
        #[tokio::test]
        async fn test_cannot_have_empty_gdrive_file_list() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, google_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![] };
            let result = storage.create(&mut transaction).await;
            assert_eq!(
                result.expect_err("expected an error").to_string(),
                "error returned from database: new row for relation \"storage_gdrive\" violates check constraint \"storage_gdrive_gdrive_ids_check\""
            );

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::assert_cannot_truncate;

        /// Cannot UPDATE any row in storage_gdrive table
        #[tokio::test]
        async fn test_cannot_update() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let id1 = "Y".repeat(28);
            let id2 = "Z".repeat(28);
            let file1 = GdriveFile { id: id1.clone(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            file1.create(&mut transaction).await?;
            GdriveFile { id: id2.clone(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None }.create(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            Storage { file_id: dummy.id, google_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file1.id] }.create(&mut transaction).await?;
            transaction.commit().await?;

            let pairs = [
                ("file_id", "100"),
                ("google_domain", "100"),
                ("cipher", "'AES_128_CTR'::stash.cipher"),
                ("cipher_key", "'1111-1111-1111-1111-1111-1111-1111-1111'::uuid"),
                ("gdrive_ids", &format!("'{{\"{id1}\",\"{id2}\"}}'::text[]"))
            ];

            for (column, value) in &pairs {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE stash.storage_gdrive SET {column} = {value} WHERE file_id = $1");
                let result = sqlx::query(&query).bind(&dummy.id).execute(&mut transaction).await;
                assert_eq!(
                    result.expect_err("expected an error").to_string(),
                    "error returned from database: cannot change file_id, google_domain, cipher, cipher_key, or gdrive_ids"
                );
            }

            Ok(())
        }

        /// Cannot TRUNCATE storage_gdrive table
        #[tokio::test]
        #[serial]
        async fn test_cannot_truncate() -> Result<()> {
            let pool = new_secondary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file = GdriveFile { id: "T".repeat(28),  owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            file.create(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            Storage { file_id: dummy.id, google_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file.id] }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_cannot_truncate(&mut transaction, "stash.storage_gdrive").await;

            Ok(())
        }
    }
}
