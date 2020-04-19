//! CRUD operations for the storage_* entities in PostgreSQL

pub mod inline;
pub mod gdrive;
pub mod internetarchive;

use anyhow::Result;
use postgres::Transaction;
use serde::Serialize;

/// A storage entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum Storage {
    /// A storage entity stored directly in the database
    #[serde(rename = "inline")]
    Inline(inline::Storage),
    /// A storage entity backed by Google Drive
    #[serde(rename = "gdrive")]
    Gdrive(gdrive::Storage),
    /// A storage entity backed by a file accessible at Internet Archive
    #[serde(rename = "internetarchive")]
    InternetArchive(internetarchive::Storage),
}

/// Return a list of places where the data for a file can be retrieved
pub fn get_storage(transaction: &mut Transaction<'_>, file_ids: &[i64]) -> Result<Vec<Storage>> {
    // We want point-in-time consistency for all the queries below
    transaction.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])?;
    let inline = inline::Storage::find_by_file_ids(transaction, file_ids)?
        .into_iter().map(Storage::Inline).collect::<Vec<_>>();
    let gdrive = gdrive::Storage::find_by_file_ids(transaction, file_ids)?
        .into_iter().map(Storage::Gdrive).collect::<Vec<_>>();
    let internetarchive = internetarchive::Storage::find_by_file_ids(transaction, file_ids)?
        .into_iter().map(Storage::InternetArchive).collect::<Vec<_>>();

    Ok([
        &inline[..],
        &gdrive[..],
        &internetarchive[..],
    ].concat())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;
    use crate::db::inode::tests::create_dummy_file;

    mod api {
        use super::*;

        /// If there is no storage for a file, get_storage returns an empty Vec
        #[test]
        fn test_no_storage() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let file_id = create_dummy_file(&mut transaction)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(get_storage(&mut transaction, &[file_id])?, vec![]);

            Ok(())
        }

        /// If we add four storages for a file, get_storage returns all of them, in order of:
        /// inline, gdrive, internetarchive
        #[test]
        fn test_create_storage_and_get_storage() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            
            // internetarchive
            let file_id = create_dummy_file(&mut transaction)?;
            let storage1 = internetarchive::Storage { file_id, ia_item: "item1".into(), pathname: "path1".into(), darked: false, last_probed: None };
            let storage2 = internetarchive::Storage { file_id, ia_item: "item2".into(), pathname: "path2".into(), darked: true, last_probed: None };
            storage1.create(&mut transaction)?;
            storage2.create(&mut transaction)?;

            // gdrive
            let gdrive_file = gdrive::file::GdriveFile { id: "I".repeat(28), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            gdrive::file::create_gdrive_file(&mut transaction, &gdrive_file)?;
            let domain = gdrive::tests::create_dummy_domain(&mut transaction)?;
            let storage3 = gdrive::Storage { file_id, gsuite_domain: domain, cipher: gdrive::Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![gdrive_file] };
            storage3.create(&mut transaction)?;

            // inline
            let storage4 = inline::Storage { file_id, content: "hello".into() };
            storage4.create(&mut transaction)?;

            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(get_storage(&mut transaction, &[file_id])?, vec![
                Storage::Inline(storage4),
                Storage::Gdrive(storage3),
                Storage::InternetArchive(storage1),
                Storage::InternetArchive(storage2),
            ]);

            Ok(())
        }
    }
}
