//! CRUD operations for Google Drive files

use std::collections::HashMap;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use postgres::Transaction;
use crate::postgres::{SixteenBytes, UnsignedInt4};

/// Create a gdrive_owner in the database.
/// Does not commit the transaction, you must do so yourself.
pub fn create_owner(transaction: &mut Transaction<'_>, owner: &str) -> Result<i32> {
    let rows = transaction.query("INSERT INTO gdrive_owners (owner) VALUES ($1::text) RETURNING id", &[&owner])?;
    let id = rows.get(0).unwrap().get(0);
    Ok(id)
}

/// A file in Google Drive, as Google understands it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GdriveFile {
    /// Google Drive's file_id
    pub id: String,
    /// The email address of the owner, or other identifier like "teamdrive"
    pub owner_id: Option<i32>,
    /// The MD5 hash
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
pub fn create_gdrive_file(transaction: &mut Transaction<'_>, file: &GdriveFile) -> Result<()> {
    transaction.execute(
        "INSERT INTO gdrive_files (id, owner, md5, crc32c, size, last_probed)
         VALUES ($1::text, $2::int, $3::uuid, $4::int, $5::bigint, $6::timestamptz)",
        &[&file.id, &file.owner_id, &SixteenBytes { bytes: file.md5 }, &UnsignedInt4 { value: file.crc32c }, &file.size, &file.last_probed]
    )?;
    Ok(())
}

/// Remove gdrive files in the database.
/// Does not commit the transaction, you must do so yourself.
pub fn remove_gdrive_files(transaction: &mut Transaction<'_>, ids: &[&str]) -> Result<()> {
    transaction.execute("DELETE FROM gdrive_files WHERE id = ANY($1::text[])", &[&ids])?;
    Ok(())
}

/// Return gdrive files with matching ids, in the same order as the ids.
pub fn get_gdrive_files(transaction: &mut Transaction<'_>, ids: &[&str]) -> Result<Vec<GdriveFile>> {
    let rows = transaction.query("SELECT id, owner, md5, crc32c, size, last_probed FROM gdrive_files WHERE id = ANY($1)", &[&ids])?;
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
    for id in ids.iter() {
        let file = map.remove(id.clone()).ok_or_else(|| anyhow!("duplicate id given"))?;
        out.push(file);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;
    use crate::db::inode::tests::create_dummy_file;
    use crate::db::storage::gdrive::tests::create_dummy_domain;
    use crate::db::storage::gdrive::{create_storage, Storage, Cipher};
    use atomic_counter::{AtomicCounter, RelaxedCounter};
    use once_cell::sync::Lazy;
    use crate::util;

    static OWNER_COUNTER: Lazy<RelaxedCounter> = Lazy::new(|| {
        RelaxedCounter::new(1)
    });


    pub(crate) fn create_dummy_owner(mut transaction: &mut Transaction<'_>) -> Result<(i32, String)> {
        let owner = format!("me-{}@example.com", OWNER_COUNTER.inc());
        let owner_id = create_owner(&mut transaction, &owner)?;
        Ok((owner_id, owner))
    }

    mod api {
        use super::*;

        // Can create gdrive files
        #[test]
        fn test_create_gdrive_file() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let (owner_id, _) = create_dummy_owner(&mut transaction)?;
            let file1 = GdriveFile { id: "A".repeat(28),  owner_id: Some(owner_id), md5: [0; 16], crc32c: 0,   size: 1,    last_probed: None };
            let file2 = GdriveFile { id: "A".repeat(160), owner_id: None,           md5: [0; 16], crc32c: 100, size: 1000, last_probed: Some(util::now_no_nanos()) };
            create_gdrive_file(&mut transaction, &file1)?;
            create_gdrive_file(&mut transaction, &file2)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let files = get_gdrive_files(&mut transaction, &[file1.id.as_ref(), file2.id.as_ref()])?;
            assert_eq!(files, vec![file1.clone(), file2.clone()]);

            // Files are returned in the same order as ids
            let files = get_gdrive_files(&mut transaction, &[file2.id.as_ref(), file1.id.as_ref()])?;
            assert_eq!(files, vec![file2.clone(), file1.clone()]);

            // Empty list is OK
            let files = get_gdrive_files(&mut transaction, &[])?;
            assert_eq!(files, vec![]);

            // Duplicate id is not OK
            let result = get_gdrive_files(&mut transaction, &[file1.id.as_ref(), file2.id.as_ref(), file1.id.as_ref()]);
            assert_eq!(result.err().expect("expected an error").to_string(), "duplicate id given");

            Ok(())
        }

        // Can remove gdrive files not referenced by storage_gdrive
        #[test]
        fn test_remove_gdrive_files() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let (owner_id, _) = create_dummy_owner(&mut transaction)?;
            let file = GdriveFile { id: "Q".repeat(28), owner_id: Some(owner_id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            remove_gdrive_files(&mut transaction, &[&file.id])?;
            transaction.commit()?;

            Ok(())
        }

        // Cannot remove gdrive files that are referenced by storage_gdrive
        #[test]
        fn test_cannot_remove_gdrive_files_still_referenced() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let (owner_id, _) = create_dummy_owner(&mut transaction)?;
            let file = GdriveFile { id: "M".repeat(28), owner_id: Some(owner_id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file)?;
            let domain = create_dummy_domain(&mut transaction)?;
            // create_storage expects the domain to already be committed
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let storage = Storage { gsuite_domain: domain, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file.clone()] };
            create_storage(&mut transaction, inode, &storage)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let result = remove_gdrive_files(&mut transaction, &[&file.id]);
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                format!("db error: ERROR: gdrive_files={} is still referenced by storage_gdrive={}", file.id, inode.file_id()?)
            );

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::tests::assert_cannot_truncate;

        /// Cannot UPDATE any row in gdrive_files table
        #[test]
        fn test_cannot_update() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let (owner_id, _) = create_dummy_owner(&mut transaction)?;
            let file = GdriveFile { id: "B".repeat(28), owner_id: Some(owner_id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file)?;
            transaction.commit()?;

            let new_id = format!("'{}'", "C".repeat(28));
            for (column, value) in [("id", new_id.as_str()), ("md5", "'0000-0000-0000-0000-0000-0000-0000-0001'::uuid"), ("crc32c", "1"), ("size", "2")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE gdrive_files SET {} = {} WHERE id = $1", column, value);
                let result = transaction.execute(query.as_str(), &[&file.id]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id, md5, crc32c, or size");
            }

            Ok(())
        }

        /// Cannot TRUNCATE gdrive_files table
        #[test]
        fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let (owner_id, _) = create_dummy_owner(&mut transaction)?;
            let file = GdriveFile { id: "D".repeat(28), owner_id: Some(owner_id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_cannot_truncate(&mut transaction, "gdrive_files")?;

            Ok(())
        }
    }
}
