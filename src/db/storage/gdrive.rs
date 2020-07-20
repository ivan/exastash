//! CRUD operations for storage_gdrive entities in PostgreSQL

use anyhow::Result;
use sqlx::{Postgres, Transaction, Row};
use sqlx::postgres::PgRow;
use serde::Serialize;
use serde_hex::{SerHex, Strict};
use uuid::Uuid;

pub mod file;

/// The encryption algorithm used to encrypt the chunks
#[must_use]
#[sqlx(rename = "cipher")]
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
    /// Create an gdrive_folder entity in the database.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<Self> {
        sqlx::query("INSERT INTO gdrive_parents (name, parent, \"full\") VALUES ($1::text, $2::text, $3::boolean)")
            .bind(&self.name)
            .bind(&self.parent)
            .bind(&self.full)
            .execute(transaction)
            .await?;
        Ok(self)
    }

    /// Find a gdrive_parent entity by name.
    pub async fn find_by_name(transaction: &mut Transaction<'_, Postgres>, name: &str) -> Result<Option<GdriveParent>> {
        let query = "SELECT name, parent, \"full\" FROM gdrive_parents WHERE name = $1::text";
        let mut parents = sqlx::query_as::<_, GdriveParent>(query)
            .bind(name)
            .fetch_all(transaction).await?;
        Ok(parents.pop())
    }
}

/// A domain where Google Drive files are stored
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct GsuiteDomain {
    /// ID for this domain
    pub id: i16,
    /// The domain name
    pub domain: String,
}

/// A new domain name
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NewGsuiteDomain {
    /// The domain name
    pub domain: String,
}

impl NewGsuiteDomain {
    /// Create a gsuite_domain in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<GsuiteDomain> {
        let row = sqlx::query("INSERT INTO gsuite_domains (domain) VALUES ($1::text) RETURNING id")
            .bind(&self.domain)
            .fetch_one(transaction).await?;
        let id = row.get(0);
        Ok(GsuiteDomain {
            id,
            domain: self.domain,
        })
    }
}

/// G Suite domain-specific descriptor that specifies where to place new Google Drive
/// files, and with which owner.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct GdriveFilePlacement {
    /// Domain ID
    pub domain: i16,
    /// Owner ID
    pub owner: i32,
    /// Google Drive folder id
    pub parent: String,
}

impl GdriveFilePlacement {
    /// Create a gdrive_file_placement in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<GdriveFilePlacement> {
        let query = "INSERT INTO gdrive_file_placement (domain, owner, parent) VALUES ($1::smallint, $2::int, $3::text)";
        sqlx::query(query)
            .bind(&self.domain)
            .bind(&self.owner)
            .bind(&self.parent)
            .execute(transaction)
            .await?;
        Ok(self)
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
}

/// A storage_gdrive entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Storage {
    /// The id of the exastash file for which this storage exists
    pub file_id: i64,
    /// The domain for the gsuite account
    pub gsuite_domain: i16,
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
        Ok(
            Storage {
                file_id: row.get("file_id"),
                gsuite_domain: row.get("gsuite_domain"),
                cipher: row.get::<Cipher, _>("cipher"),
                cipher_key: *row.get::<Uuid, _>("cipher_key").as_bytes(),
                gdrive_ids: row.get("gdrive_ids"),
            }
        )
    }
}

impl Storage {
    /// Create an gdrive storage entity in the database.
    /// Note that the gsuite domain must already exist.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<Self> {
        sqlx::query(
            "INSERT INTO storage_gdrive (file_id, gsuite_domain, cipher, cipher_key, gdrive_ids)
             VALUES ($1::bigint, $2::smallint, $3::cipher, $4::uuid, $5::text[])"
        )
            .bind(&self.file_id)
            .bind(&self.gsuite_domain)
            .bind(&self.cipher)
            .bind(Uuid::from_bytes(self.cipher_key))
            .bind(&self.gdrive_ids)
            .execute(transaction)
            .await?;
        Ok(self)
    }

    /// Return a list of gdrive storage entities where the data for a file can be retrieved.
    pub async fn find_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        // Note that we can get more than one row per unique file_id
        let storages = sqlx::query_as::<_, Storage>(
            "SELECT file_id, gsuite_domain, cipher, cipher_key, gdrive_ids
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
    use crate::db::tests::{main_test_instance, truncate_test_instance};
    use crate::db::inode::tests::create_dummy_file;
    use file::GdriveFile;
    use atomic_counter::{AtomicCounter, RelaxedCounter};
    use once_cell::sync::Lazy;
    use serial_test::serial;

    static DOMAIN_COUNTER: Lazy<RelaxedCounter> = Lazy::new(|| {
        RelaxedCounter::new(1)
    });

    pub(crate) async fn create_dummy_domain(mut transaction: &mut Transaction<'_, Postgres>) -> Result<GsuiteDomain> {
        let num = DOMAIN_COUNTER.inc();
        let domain = format!("{num}.example.com");
        Ok(NewGsuiteDomain { domain }.create(&mut transaction).await?)
    }

    mod api {
        use super::*;

        /// If we add a gdrive storage for a file, get_storage returns that storage
        #[tokio::test]
        async fn test_create_storage_get_storage() -> Result<()> {
            let client = main_test_instance().await;

            let mut transaction = client.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file1 = GdriveFile { id: "X".repeat(28),  owner_id: None, md5: [0; 16], crc32c: 0,   size: 1,    last_probed: None }.create(&mut transaction).await?;
            let file2 = GdriveFile { id: "X".repeat(160), owner_id: None, md5: [0; 16], crc32c: 100, size: 1000, last_probed: None }.create(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file1.id, file2.id] }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = client.begin().await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[dummy.id]).await?, vec![storage]);

            Ok(())
        }

        /// Cannot reference a nonexistent gdrive file
        #[tokio::test]
        async fn test_cannot_reference_nonexistent_gdrive_file() -> Result<()> {
            let client = main_test_instance().await;

            let mut transaction = client.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file = GdriveFile { id: "FileNeverAddedToDatabase".into(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file.id] };
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
            let client = main_test_instance().await;

            let mut transaction = client.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file1 = GdriveFile { id: "F".repeat(28), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None }.create(&mut transaction).await?;
            let file2 = GdriveFile { id: "FileNeverAddedToDatabase".into(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file1.id, file2.id] };
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
            let client = main_test_instance().await;

            let mut transaction = client.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![] };
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
        use crate::db::tests::assert_cannot_truncate;

        /// Cannot UPDATE any row in storage_gdrive table
        #[tokio::test]
        async fn test_cannot_update() -> Result<()> {
            let client = main_test_instance().await;

            let mut transaction = client.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let id1 = "Y".repeat(28);
            let id2 = "Z".repeat(28);
            let file1 = GdriveFile { id: id1.clone(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None }.create(&mut transaction).await?;
            let _file2 = GdriveFile { id: id2.clone(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None }.create(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file1.id] }.create(&mut transaction).await?;
            transaction.commit().await?;

            let pairs = [
                ("file_id", "100"),
                ("gsuite_domain", "100"),
                ("cipher", "'AES_128_CTR'::cipher"),
                ("cipher_key", "'1111-1111-1111-1111-1111-1111-1111-1111'::uuid"),
                ("gdrive_ids", &format!("'{{\"{id1}\",\"{id2}\"}}'::text[]"))
            ];

            for (column, value) in &pairs {
                let mut transaction = client.begin().await?;
                let query = format!("UPDATE storage_gdrive SET {column} = {value} WHERE file_id = $1::bigint");
                let result = sqlx::query(&query).bind(&dummy.id).execute(&mut transaction).await;
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    "error returned from database: cannot change file_id, gsuite_domain, cipher, cipher_key, or gdrive_ids"
                );
            }

            Ok(())
        }

        /// Cannot TRUNCATE storage_gdrive table
        #[tokio::test]
        #[serial]
        async fn test_cannot_truncate() -> Result<()> {
            let pool = truncate_test_instance().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file = GdriveFile { id: "T".repeat(28),  owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None }.create(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![file.id] }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_cannot_truncate(&mut transaction, "storage_gdrive").await;

            Ok(())
        }
    }
}
