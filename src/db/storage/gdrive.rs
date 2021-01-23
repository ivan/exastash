//! CRUD operations for storage_gdrive entities in PostgreSQL

use tracing::info;
use anyhow::Result;
use sqlx::{Postgres, Transaction, Row};
use sqlx::postgres::PgRow;
use serde::Serialize;
use serde_hex::{SerHex, Strict};
use uuid::Uuid;

pub mod file;

/// The encryption algorithm used to encrypt the chunks
#[must_use]
#[sqlx(type_name = "cipher")]
#[derive(Debug, Copy, Clone, PartialEq, Eq, sqlx::Type, Serialize)]
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
        sqlx::query(r#"INSERT INTO gdrive_parents (name, parent, "full") VALUES ($1::text, $2::text, $3::boolean)"#)
            .bind(&self.name)
            .bind(&self.parent)
            .bind(&self.full)
            .execute(transaction)
            .await?;
        Ok(())
    }

    /// Find a gdrive_parent entity by name.
    pub async fn find_by_name(transaction: &mut Transaction<'_, Postgres>, name: &str) -> Result<Option<GdriveParent>> {
        let query = r#"SELECT name, parent, "full" FROM gdrive_parents WHERE name = $1::text"#;
        let mut parents = sqlx::query_as::<_, GdriveParent>(query)
            .bind(name)
            .fetch_all(transaction).await?;
        Ok(parents.pop())
    }

    /// Find the first gdrive_parent that is not full.
    pub async fn find_first_non_full(transaction: &mut Transaction<'_, Postgres>) -> Result<Option<GdriveParent>> {
        let query = r#"SELECT name, parent, "full" FROM gdrive_parents WHERE "full" = false"#;
        Ok(sqlx::query_as::<_, GdriveParent>(query)
            .fetch_optional(transaction).await?)
    }

    /// Set whether a parent is full or not
    pub async fn set_full(transaction: &mut Transaction<'_, Postgres>, name: &str, full: bool) -> Result<()> {
        info!("setting full = {} on gdrive_parent name = {:?}", full, name);
        let query = r#"UPDATE gdrive_parents SET "full" = $1::boolean WHERE name = $2::text"#;
        sqlx::query(query)
            .bind(full)
            .bind(name)
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
        let row = sqlx::query("INSERT INTO google_domains (domain) VALUES ($1::text) RETURNING id")
            .bind(&self.domain)
            .fetch_one(transaction).await?;
        let id = row.get(0);
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
        let query = "INSERT INTO gdrive_file_placement (domain, owner, parent) VALUES ($1::smallint, $2::int, $3::text)";
        sqlx::query(query)
            .bind(&self.domain)
            .bind(&self.owner)
            .bind(&self.parent)
            .execute(transaction)
            .await?;
        Ok(())
    }

    /// Remove this gdrive_file_placement from the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        let stmt = "DELETE FROM gdrive_file_placement 
                    WHERE domain = $1::smallint AND owner = $2::int AND parent = $3::text";
        sqlx::query(stmt)
            .bind(self.domain)
            .bind(self.owner)
            .bind(&self.parent)
            .execute(transaction).await?;
        Ok(())
    }

    /// Return a `Vec<GdriveFilePlacement>` for domain `domain`.
    /// There is no error if the domain id does not exist.
    /// If limit is not `None`, returns max `N` random rows.
    pub async fn find_by_domain(transaction: &mut Transaction<'_, Postgres>, domain: i16, limit: Option<i32>) -> Result<Vec<GdriveFilePlacement>> {
        let limit_sql = match limit {
            None => "".into(),
            Some(num) => format!("ORDER BY random() LIMIT {num}")
        };
        let query = format!(
            "SELECT domain, owner, parent FROM gdrive_file_placement
             WHERE domain = $1::smallint
             {}", limit_sql
        );
        Ok(sqlx::query_as::<_, GdriveFilePlacement>(&query)
            .bind(domain)
            .fetch_all(transaction).await?)
    }

    /// Return a `Vec<GdriveFilePlacement>` if one exists in the database for this placement,
    /// and lock the row for update.
    pub async fn find_self_and_lock(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<Option<GdriveFilePlacement>> {
        let query = "SELECT domain, owner, parent FROM gdrive_file_placement
                     WHERE domain = $1::smallint AND owner = $2::int AND parent = $3::text
                     FOR UPDATE";
        Ok(sqlx::query_as::<_, GdriveFilePlacement>(&query)
            .bind(self.domain)
            .bind(self.owner)
            .bind(&self.parent)
            .fetch_optional(transaction).await?)
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

impl<'c> sqlx::FromRow<'c, PgRow> for Storage {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Storage {
            file_id: row.get("file_id"),
            google_domain: row.get("google_domain"),
            cipher: row.get::<Cipher, _>("cipher"),
            cipher_key: *row.get::<Uuid, _>("cipher_key").as_bytes(),
            gdrive_ids: row.get("gdrive_ids"),
        })
    }
}

impl Storage {
    /// Create an gdrive storage entity in the database.
    /// Note that the google domain must already exist.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query(
            "INSERT INTO storage_gdrive (file_id, google_domain, cipher, cipher_key, gdrive_ids)
             VALUES ($1::bigint, $2::smallint, $3::cipher, $4::uuid, $5::text[])"
        )
            .bind(&self.file_id)
            .bind(&self.google_domain)
            .bind(&self.cipher)
            .bind(Uuid::from_bytes(self.cipher_key))
            .bind(&self.gdrive_ids)
            .execute(transaction)
            .await?;
        Ok(())
    }

    /// Remove storages with given `ids`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<()> {
        let stmt = "DELETE FROM storage_gdrive WHERE file_id = ANY($1::bigint[])";
        sqlx::query(stmt)
            .bind(file_ids)
            .execute(transaction).await?;
        Ok(())
    }

    /// Return a list of gdrive storage entities where the data for a file can be retrieved.
    pub async fn find_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        // Note that we can get more than one row per unique file_id
        let storages = sqlx::query_as::<_, Storage>(
            "SELECT file_id, google_domain, cipher, cipher_key, gdrive_ids
             FROM storage_gdrive
             WHERE file_id = ANY($1::bigint[])"
        )
            .bind(file_ids)
            .fetch_all(transaction)
            .await?;
        Ok(storages)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::tests::{new_primary_pool, new_secondary_pool};
    use crate::db::inode::tests::create_dummy_file;
    use file::GdriveFile;
    use atomic_counter::{AtomicCounter, RelaxedCounter};
    use once_cell::sync::Lazy;
    use serial_test::serial;

    static DOMAIN_COUNTER: Lazy<RelaxedCounter> = Lazy::new(|| {
        RelaxedCounter::new(1)
    });

    pub(crate) async fn create_dummy_domain(mut transaction: &mut Transaction<'_, Postgres>) -> Result<GoogleDomain> {
        let num = DOMAIN_COUNTER.inc();
        let domain = format!("{num}.example.com");
        Ok(NewGoogleDomain { domain }.create(&mut transaction).await?)
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
                result.err().expect("expected an error").to_string(),
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
                result.err().expect("expected an error").to_string(),
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
                result.err().expect("expected an error").to_string(),
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
                ("cipher", "'AES_128_CTR'::cipher"),
                ("cipher_key", "'1111-1111-1111-1111-1111-1111-1111-1111'::uuid"),
                ("gdrive_ids", &format!("'{{\"{id1}\",\"{id2}\"}}'::text[]"))
            ];

            for (column, value) in &pairs {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE storage_gdrive SET {column} = {value} WHERE file_id = $1::bigint");
                let result = sqlx::query(&query).bind(&dummy.id).execute(&mut transaction).await;
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
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
            assert_cannot_truncate(&mut transaction, "storage_gdrive").await;

            Ok(())
        }
    }
}
