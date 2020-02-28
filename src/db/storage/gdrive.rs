use anyhow::Result;
use postgres::Transaction;
use postgres_types::{ToSql, FromSql};
use crate::db::inode::Inode;
use crate::postgres::SixteenBytes;

pub(crate) mod file;

#[postgres(name = "cipher")]
#[derive(Debug, Clone, PartialEq, Eq, ToSql, FromSql)]
pub(crate) enum Cipher {
    #[postgres(name = "AES_128_CTR")]
    Aes128Ctr,
    #[postgres(name = "AES_128_GCM")]
    Aes128Gcm,
}

/// Creates a gsuite domain entity in the database.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_domain(transaction: &mut Transaction<'_>, domain: &str) -> Result<i16> {
    let rows = transaction.query("INSERT INTO gsuite_domains (domain) VALUES ($1::text) RETURNING id", &[&domain])?;
    let id = rows.get(0).unwrap().get(0);
    Ok(id)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Storage {
    pub gsuite_domain: String,
    pub cipher: Cipher,
    pub cipher_key: [u8; 16],
    pub gdrive_files: Vec<file::GdriveFile>,
}

/// Creates an gdrive storage entity in the database.
/// Does not commit the transaction, you must do so yourself.
/// Note that the gsuite domain must already exist.
/// Note that you must call file::create_gdrive_file for each gdrive file beforehand.
pub(crate) fn create_storage(transaction: &mut Transaction<'_>, inode: Inode, storage: &Storage) -> Result<()> {
    let file_id = inode.file_id()?;
    let gdrive_ids = storage.gdrive_files.iter().map(|f| f.id.clone()).collect::<Vec<_>>();
    let domain: i16 = transaction.query_one(
        "SELECT id from gsuite_domains WHERE domain = $1", &[&storage.gsuite_domain]
    )?.get(0);
    transaction.execute(
        "INSERT INTO storage_gdrive (file_id, gsuite_domain, cipher, cipher_key, gdrive_ids)
         VALUES ($1::bigint, $2::smallint, $3::cipher, $4::uuid, $5::text[])",
        &[&inode.file_id()?, &domain, &storage.cipher, &SixteenBytes { bytes: storage.cipher_key }, &gdrive_ids]
    )?;
    Ok(())
}

/// Returns a list of gdrive storage entities where the data for a file can be retrieved.
pub(crate) fn get_storage(mut transaction: &mut Transaction<'_>, inode: Inode) -> Result<Vec<Storage>> {
    let file_id = inode.file_id()?;

    transaction.execute("SET TRANSACTION READ ONLY", &[])?;
    let rows = transaction.query("
        SELECT    domain, cipher, cipher_key, gdrive_ids
        FROM      storage_gdrive
        LEFT JOIN gsuite_domains ON storage_gdrive.gsuite_domain = gsuite_domains.id
        WHERE     file_id = $1
    ", &[&file_id])?;
    if rows.len() == 0 {
        return Ok(vec![]);
    }

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let gdrive_file_ids: Vec<&str> = row.get(3);
        let gdrive_files = file::get_gdrive_files(&mut transaction, &gdrive_file_ids[..])?;
        let file = Storage {
            gsuite_domain: row.get(0),
            cipher: row.get(1),
            cipher_key: row.get::<_, SixteenBytes>(2).bytes,
            gdrive_files
        };
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
    use file::{create_gdrive_file, GdriveFile};
    use atomic_counter::{AtomicCounter, RelaxedCounter};
    use once_cell::sync::Lazy;

    static DOMAIN_COUNTER: Lazy<RelaxedCounter> = Lazy::new(|| {
        RelaxedCounter::new(1)
    });


    pub(crate) fn create_dummy_domain(mut transaction: &mut Transaction<'_>) -> Result<String> {
        let domain = format!("{}.example.com", DOMAIN_COUNTER.inc());
        create_domain(&mut transaction, &domain)?;
        Ok(domain)
    }

    mod api {
        use super::*;

        /// If we add a gdrive storage for a file, get_storage returns that storage
        #[test]
        fn test_create_storage_get_storage() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let file1 = GdriveFile { id: "X".repeat(28),  owner_id: None, md5: [0; 16], crc32c: 0,   size: 1,    last_probed: None };
            let file2 = GdriveFile { id: "X".repeat(160), owner_id: None, md5: [0; 16], crc32c: 100, size: 1000, last_probed: None };
            create_gdrive_file(&mut transaction, &file1)?;
            create_gdrive_file(&mut transaction, &file2)?;
            let domain = create_dummy_domain(&mut transaction)?;
            let storage = Storage { gsuite_domain: domain, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file1, file2] };
            create_storage(&mut transaction, inode, &storage)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(get_storage(&mut transaction, inode)?, vec![storage]);

            Ok(())
        }

        /// Cannot reference a nonexistent gdrive file
        #[test]
        fn test_cannot_reference_nonexistent_gdrive_file() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let file = GdriveFile { id: "FileNeverAddedToDatabase".into(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            let domain = create_dummy_domain(&mut transaction)?;
            let storage = Storage { gsuite_domain: domain, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file] };
            let result = create_storage(&mut transaction, inode, &storage);
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                "db error: ERROR: gdrive_ids had 1 ids: {FileNeverAddedToDatabase} but only 0 of these are in gdrive_files"
            );

            Ok(())
        }

        /// Cannot reference a nonexistent gdrive file even when other gdrive files do exist
        #[test]
        fn test_cannot_reference_nonexistent_gdrive_file_even_if_some_exist() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let file1 = GdriveFile { id: "F".repeat(28), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file1)?;
            let file2 = GdriveFile { id: "FileNeverAddedToDatabase".into(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            let domain = create_dummy_domain(&mut transaction)?;
            let storage = Storage { gsuite_domain: domain, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file1, file2] };
            let result = create_storage(&mut transaction, inode, &storage);
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                "db error: ERROR: gdrive_ids had 2 ids: {FFFFFFFFFFFFFFFFFFFFFFFFFFFF,FileNeverAddedToDatabase} but only 1 of these are in gdrive_files"
            );

            Ok(())
        }

        /// Cannot have empty gdrive_files
        #[test]
        fn test_cannot_have_empty_gdrive_file_list() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let domain = create_dummy_domain(&mut transaction)?;
            let storage = Storage { gsuite_domain: domain, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![] };
            let result = create_storage(&mut transaction, inode, &storage);
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

        /// Cannot UPDATE any row in storage_gdrive table
        #[test]
        fn test_cannot_update() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let id1 = "Y".repeat(28);
            let id2 = "Z".repeat(28);
            let file1 = GdriveFile { id: id1.clone(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            let file2 = GdriveFile { id: id2.clone(), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file1)?;
            create_gdrive_file(&mut transaction, &file2)?;
            let domain = create_dummy_domain(&mut transaction)?;
            let storage = Storage { gsuite_domain: domain, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file1] };
            create_storage(&mut transaction, inode, &storage)?;
            transaction.commit()?;

            let pairs = [
                ("file_id", "100"),
                ("gsuite_domain", "100"),
                ("cipher", "'AES_128_CTR'::cipher"),
                ("cipher_key", "'1111-1111-1111-1111-1111-1111-1111-1111'::uuid"),
                ("gdrive_ids", &format!("'{{\"{}\",\"{}\"}}'::text[]", id1, id2))
            ];

            for (column, value) in pairs.iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE storage_gdrive SET {} = {} WHERE file_id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&inode.file_id()?]);
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    "db error: ERROR: cannot change file_id, gsuite_domain, cipher, cipher_key, or gdrive_ids"
                );
            }

            Ok(())
        }

        /// Cannot TRUNCATE storage_gdrive table
        #[test]
        fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let file = GdriveFile { id: "T".repeat(28),  owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file)?;
            let domain = create_dummy_domain(&mut transaction)?;
            let storage = Storage { gsuite_domain: domain, cipher: Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![file] };
            create_storage(&mut transaction, inode, &storage)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let result = transaction.execute("TRUNCATE storage_gdrive", &[]);
            assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: truncate is forbidden");

            Ok(())
        }
    }
}
