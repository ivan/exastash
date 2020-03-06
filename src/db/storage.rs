mod inline;
mod gdrive;
mod internetarchive;

use anyhow::Result;
use postgres::Transaction;
use crate::db::inode::Inode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Storage {
    Inline(inline::Storage),
    Gdrive(gdrive::Storage),
    InternetArchive(internetarchive::Storage),
}

/// Returns a list of places where the data for a file can be retrieved
pub fn get_storage(transaction: &mut Transaction<'_>, inode: Inode) -> Result<Vec<Storage>> {
    let file_id = inode.file_id();

    transaction.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])?;
    let inline = inline::get_storage(transaction, inode)?
        .into_iter().map(Storage::Inline).collect::<Vec<_>>();
    let gdrive = gdrive::get_storage(transaction, inode)?
        .into_iter().map(Storage::Gdrive).collect::<Vec<_>>();
    let internetarchive = internetarchive::get_storage(transaction, inode)?
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
            let inode = create_dummy_file(&mut transaction)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(get_storage(&mut transaction, inode)?, vec![]);

            Ok(())
        }

        /// If we add four storages for a file, get_storage returns all of them, in order of:
        /// inline, gdrive, internetarchive
        #[test]
        fn test_create_storage_and_get_storage() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            
            // internetarchive
            let inode = create_dummy_file(&mut transaction)?;
            let storage1 = internetarchive::Storage { ia_item: "item1".into(), pathname: "path1".into(), darked: false, last_probed: None };
            let storage2 = internetarchive::Storage { ia_item: "item2".into(), pathname: "path2".into(), darked: true, last_probed: None };
            internetarchive::create_storage(&mut transaction, inode, &storage1)?;
            internetarchive::create_storage(&mut transaction, inode, &storage2)?;

            // gdrive
            let gdrive_file = gdrive::file::GdriveFile { id: "I".repeat(28), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            gdrive::file::create_gdrive_file(&mut transaction, &gdrive_file)?;
            let domain = gdrive::tests::create_dummy_domain(&mut transaction)?;
            let storage3 = gdrive::Storage { gsuite_domain: domain, cipher: gdrive::Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_files: vec![gdrive_file] };
            gdrive::create_storage(&mut transaction, inode, &storage3)?;

            // inline
            let storage4 = inline::Storage { content: "hello".into() };
            inline::create_storage(&mut transaction, inode, &storage4)?;

            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(get_storage(&mut transaction, inode)?, vec![
                Storage::Inline(storage4),
                Storage::Gdrive(storage3),
                Storage::InternetArchive(storage1),
                Storage::InternetArchive(storage2),
            ]);

            Ok(())
        }
    }
}
