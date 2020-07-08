//! CRUD operations for storage_gdrive entities in PostgreSQL

use anyhow::Result;
use tokio_postgres::Transaction;
use postgres_types::{ToSql, FromSql};
use serde::Serialize;
use serde_hex::{SerHex, Strict};
use self::file::GdriveFile;
use crate::postgres::SixteenBytes;

pub mod file;

/// The encryption algorithm used to encrypt the chunks
#[must_use]
#[postgres(name = "cipher")]
#[derive(Debug, Copy, Clone, PartialEq, Eq, ToSql, FromSql, Serialize)]
pub enum Cipher {
    /// AES-128-CTR
    #[postgres(name = "AES_128_CTR")]
    #[serde(rename = "AES_128_CTR")]
    Aes128Ctr,
    /// AES-128-GCM
    #[postgres(name = "AES_128_GCM")]
    #[serde(rename = "AES_128_GCM")]
    Aes128Gcm,
}

/// A Google Drive folder into which files are uploaded
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    pub async fn create(self, transaction: &mut Transaction<'_>) -> Result<Self> {
        transaction.execute(
            "INSERT INTO gdrive_parents (name, parent, \"full\")
             VALUES ($1::text, $2::text, $3::boolean)",
            &[&self.name, &self.parent, &self.full]
        ).await?;
        Ok(self)
    }

    /// Find a gdrive_parent entity by name.
    pub async fn find_by_name(transaction: &mut Transaction<'_>, name: &str) -> Result<Option<GdriveParent>> {
        let mut rows = transaction.query(
            "SELECT name, parent, \"full\"
             FROM gdrive_parents
             WHERE name = $1::text", &[&name]
        ).await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = rows.pop().unwrap();

        Ok(Some(GdriveParent {
            name: row.get(0),
            parent: row.get(1),
            full: row.get(2),
        }))
    }
}

/// A domain where Google Drive files are stored
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    pub async fn create(self, transaction: &mut Transaction<'_>) -> Result<GsuiteDomain> {
        let rows = transaction.query("INSERT INTO gsuite_domains (domain) VALUES ($1::text) RETURNING id", &[&self.domain]).await?;
        let id = rows.get(0).unwrap().get(0);
        Ok(GsuiteDomain {
            id,
            domain: self.domain,
        })
    }
}

/// G Suite domain-specific descriptor that specifies where to place new Google Drive
/// files, and with which owner.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    pub async fn create(self, transaction: &mut Transaction<'_>) -> Result<GdriveFilePlacement> {
        transaction.execute(
            "INSERT INTO gdrive_file_placement (domain, owner, parent)
             VALUES ($1::smallint, $2::int, $3::text)",
            &[&self.domain, &self.owner, &self.parent]).await?;
        Ok(self)
    }

    /// Return a `Vec<GdriveFilePlacement>` for domain `domain`.
    /// There is no error if the domain id does not exist.
    /// If limit is not `None`, returns max `N` random rows.
    pub async fn find_by_domain(transaction: &mut Transaction<'_>, domain: i16, limit: Option<i32>) -> Result<Vec<GdriveFilePlacement>> {
        let limit_sql = match limit {
            None => "".into(),
            Some(num) => format!("ORDER BY random() LIMIT {}", num)
        };
        let sql = format!(
            "SELECT domain, owner, parent FROM gdrive_file_placement
             WHERE domain = $1::smallint
             {}", limit_sql);
        let rows = transaction.query(sql.as_str(), &[&domain]).await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(GdriveFilePlacement {
                domain: row.get(0),
                owner: row.get(1),
                parent: row.get(2),
            });
        }
        Ok(out)
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
    /// An ordered list of encrypted gdrive files which comprise the chunks
    pub gdrive_files: Vec<file::GdriveFile>,
}

impl Storage {
    /// Create an gdrive storage entity in the database.
    /// Note that the gsuite domain must already exist.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_>) -> Result<Self> {
        let gdrive_ids = self.gdrive_files.iter().map(|f| f.id.clone()).collect::<Vec<_>>();
        transaction.execute(
            "INSERT INTO storage_gdrive (file_id, gsuite_domain, cipher, cipher_key, gdrive_ids)
             VALUES ($1::bigint, $2::smallint, $3::cipher, $4::uuid, $5::text[])",
            &[&self.file_id, &self.gsuite_domain, &self.cipher, &SixteenBytes { bytes: self.cipher_key }, &gdrive_ids]
        ).await?;
        Ok(self)
    }

    /// Return a list of gdrive storage entities where the data for a file can be retrieved.
    pub async fn find_by_file_ids(mut transaction: &mut Transaction<'_>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        // Note that we can get more than one row per unique file_id
        let rows = transaction.query(
            "SELECT file_id, gsuite_domain, cipher, cipher_key, gdrive_ids
             FROM storage_gdrive
             WHERE file_id = ANY($1::bigint[])", &[&file_ids]
        ).await?;
        if rows.is_empty() {
            return Ok(vec![]);
        }

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let gdrive_file_ids: Vec<&str> = row.get(4);
            let gdrive_files = GdriveFile::find_by_ids_in_order(&mut transaction, &gdrive_file_ids[..]).await?;
            let file = Storage {
                file_id: row.get(0),
                gsuite_domain: row.get(1),
                cipher: row.get(2),
                cipher_key: row.get::<_, SixteenBytes>(3).bytes,
                gdrive_files
            };
            out.push(file);
        }
        Ok(out)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::{MAIN_TEST_INSTANCE, TRUNCATE_TEST_INSTANCE};
    use crate::db::inode::tests::create_dummy_file;
    use file::GdriveFile;
    use atomic_counter::{AtomicCounter, RelaxedCounter};
    use once_cell::sync::Lazy;
    use serial_test::serial;

    static DOMAIN_COUNTER: Lazy<RelaxedCounter> = Lazy::new(|| {
        RelaxedCounter::new(1)
    });

    pub(crate) async fn create_dummy_domain(mut transaction: &mut Transaction<'_>) -> Result<GsuiteDomain> {
        let domain = format!("{}.example.com", DOMAIN_COUNTER.inc());
        Ok(NewGsuiteDomain { domain }.create(&mut transaction).await?)
    }

    mod api {
        use super::*;

        /// If we add a gdrive storage for a file, get_storage returns that storage
        #[tokio::test]
        async fn test_create_storage_get_storage() -> Result<()> {
            let mut client = MAIN_TEST_INSTANCE.get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file1 = GdriveFile { id: "X".repeat(28),  owner_id: None, md5: [0; 16], crc32c: 0,   size: 1,    last_probed: None }.create(&mut transaction).await?;
            let file2 = GdriveFile { id: "X".repeat(160), owner_id: None, md5: [0; 16], crc32c: 100, size: 1000, last_probed: None }.create(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file1, file2] }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[dummy.id]).await?, vec![storage]);

            Ok(())
        }

        /// Cannot reference a nonexistent gdrive file
        #[tokio::test]
        async fn test_cannot_reference_nonexistent_gdrive_file() -> Result<()> {
            let mut client = MAIN_TEST_INSTANCE.get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file = GdriveFile { id: "FileNeverAddedToDatabase".into(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file] };
            let result = storage.create(&mut transaction).await;
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                "db error: ERROR: gdrive_ids had 1 ids: {FileNeverAddedToDatabase} but only 0 of these are in gdrive_files"
            );

            Ok(())
        }

        /// Cannot reference a nonexistent gdrive file even when other gdrive files do exist
        #[tokio::test]
        async fn test_cannot_reference_nonexistent_gdrive_file_even_if_some_exist() -> Result<()> {
            let mut client = MAIN_TEST_INSTANCE.get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file1 = GdriveFile { id: "F".repeat(28), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None }.create(&mut transaction).await?;
            let file2 = GdriveFile { id: "FileNeverAddedToDatabase".into(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file1, file2] };
            let result = storage.create(&mut transaction).await;
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                "db error: ERROR: gdrive_ids had 2 ids: {FFFFFFFFFFFFFFFFFFFFFFFFFFFF,FileNeverAddedToDatabase} but only 1 of these are in gdrive_files"
            );

            Ok(())
        }

        /// Cannot have empty gdrive_files
        #[tokio::test]
        async fn test_cannot_have_empty_gdrive_file_list() -> Result<()> {
            let mut client = MAIN_TEST_INSTANCE.get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![] };
            let result = storage.create(&mut transaction).await;
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                "db error: ERROR: new row for relation \"storage_gdrive\" violates check constraint \"storage_gdrive_gdrive_ids_check\""
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
            let mut client = MAIN_TEST_INSTANCE.get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let id1 = "Y".repeat(28);
            let id2 = "Z".repeat(28);
            let file1 = GdriveFile { id: id1.clone(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None }.create(&mut transaction).await?;
            let _file2 = GdriveFile { id: id2.clone(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None }.create(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file1] }.create(&mut transaction).await?;
            transaction.commit().await?;

            let pairs = [
                ("file_id", "100"),
                ("gsuite_domain", "100"),
                ("cipher", "'AES_128_CTR'::cipher"),
                ("cipher_key", "'1111-1111-1111-1111-1111-1111-1111-1111'::uuid"),
                ("gdrive_ids", &format!("'{{\"{}\",\"{}\"}}'::text[]", id1, id2))
            ];

            for (column, value) in &pairs {
                let transaction = start_transaction(&mut client).await?;
                let query = format!("UPDATE storage_gdrive SET {} = {} WHERE file_id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&dummy.id]).await;
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    "db error: ERROR: cannot change file_id, gsuite_domain, cipher, cipher_key, or gdrive_ids"
                );
            }

            Ok(())
        }

        /// Cannot TRUNCATE storage_gdrive table
        #[tokio::test]
        #[serial]
        async fn test_cannot_truncate() -> Result<()> {
            let mut client = TRUNCATE_TEST_INSTANCE.get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let file = GdriveFile { id: "T".repeat(28),  owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None }.create(&mut transaction).await?;
            let domain = create_dummy_domain(&mut transaction).await?;
            Storage { file_id: dummy.id, gsuite_domain: domain.id, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file] }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            assert_cannot_truncate(&mut transaction, "storage_gdrive").await;

            Ok(())
        }
    }
}
