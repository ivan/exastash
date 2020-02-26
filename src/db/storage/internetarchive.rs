use anyhow::Result;
use chrono::{DateTime, Utc};
use postgres::Transaction;
use crate::db::inode::Inode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Storage {
    ia_item: String,
    pathname: String,
    darked: bool,
    last_probed: Option<DateTime<Utc>>,
}

pub(crate) fn create_storage(transaction: &mut Transaction<'_>, inode: Inode, storage: &Storage) -> Result<()> {
    let file_id = inode.file_id()?;
    transaction.execute(
        "INSERT INTO storage_internetarchive (file_id, ia_item, pathname, darked, last_probed)
         VALUES ($1::bigint, $2::text, $3::text, $4::boolean, $5::timestamptz)",
        &[&file_id, &storage.ia_item, &storage.pathname, &storage.darked, &storage.last_probed]
    )?;
    Ok(())
}

pub(crate) fn get_storage(transaction: &mut Transaction<'_>, inode: Inode) -> Result<Vec<Storage>> {
    let file_id = inode.file_id()?;
    transaction.execute("SET TRANSACTION READ ONLY", &[])?;
    let rows = transaction.query("SELECT ia_item, pathname, darked, last_probed FROM storage_internetarchive WHERE file_id = $1::bigint", &[&file_id])?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let storage = Storage {
            ia_item: row.get(0),
            pathname: row.get(1),
            darked: row.get(2),
            last_probed: row.get(3),
        };
        out.push(storage);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;
    use crate::db::inode::tests::create_dummy_file;

    mod api {
        use super::*;

        /// If there is no internetarchive storage for a file, get_storage returns an empty Vec
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

        /// If we add one internetarchive storage for a file, get_storage returns just that storage
        #[test]
        fn test_create_storage_and_get_storage() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let storage = Storage { ia_item: "item".into(), pathname: "path".into(), darked: false, last_probed: None };
            create_storage(&mut transaction, inode, &storage)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(get_storage(&mut transaction, inode)?, vec![storage]);

            Ok(())
        }

        /// If we add multiple internetarchive storage for a file, get_storage returns those storages
        #[test]
        fn test_multiple_create_storage_and_get_storage() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let storage1 = Storage { ia_item: "item1".into(), pathname: "path".into(), darked: false, last_probed: None };
            let storage2 = Storage { ia_item: "item2".into(), pathname: "path".into(), darked: true, last_probed: Some(util::now_no_nanos()) };
            create_storage(&mut transaction, inode, &storage1)?;
            create_storage(&mut transaction, inode, &storage2)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(get_storage(&mut transaction, inode)?, vec![storage1, storage2]);

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;

        /// Cannot TRUNCATE storage_internetarchive table
        #[test]
        fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let result = transaction.execute("TRUNCATE storage_internetarchive", &[]);
            assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: truncate is forbidden");

            Ok(())
        }

        /// Cannot UPDATE file_id, ia_item, or pathname on storage_internetarchive table
        #[test]
        fn test_cannot_change_immutables() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let storage = Storage { ia_item: "item".into(), pathname: "path".into(), darked: false, last_probed: None };
            create_storage(&mut transaction, inode, &storage)?;
            transaction.commit()?;

            for (column, value) in [("file_id", "100"), ("ia_item", "'new'"), ("pathname", "'new'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE storage_internetarchive SET {} = {} WHERE file_id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&inode.file_id()?]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change file_id, ia_item, or pathname");
            }

            Ok(())
        }
    }
}
