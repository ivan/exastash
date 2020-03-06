use anyhow::Result;
use postgres::Transaction;
use crate::db::inode::Inode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Storage {
    pub content: Vec<u8>,
}

/// Creates an inline storage entity in the database.
/// Does not commit the transaction, you must do so yourself.
pub fn create_storage(transaction: &mut Transaction<'_>, inode: Inode, storage: &Storage) -> Result<()> {
    let file_id = inode.file_id()?;
    transaction.execute(
        "INSERT INTO storage_inline (file_id, content)
         VALUES ($1::bigint, $2::bytea)",
        &[&file_id, &storage.content]
    )?;
    Ok(())
}

/// Returns a list of inline storage entities containing the data for a file.
pub fn get_storage(transaction: &mut Transaction<'_>, inode: Inode) -> Result<Vec<Storage>> {
    let file_id = inode.file_id()?;
    let rows = transaction.query("SELECT content FROM storage_inline WHERE file_id = $1::bigint", &[&file_id])?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let storage = Storage {
            content: row.get(0),
        };
        out.push(storage);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;
    use crate::db::inode::tests::create_dummy_file;

    mod api {
        use super::*;

        /// If there is no inline storage for a file, get_storage returns an empty Vec
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

        /// If we add an inline storage for a file, get_storage returns that storage
        #[test]
        fn test_create_storage_and_get_storage() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let storage = Storage { content: "some content".into() };
            create_storage(&mut transaction, inode, &storage)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(get_storage(&mut transaction, inode)?, vec![storage]);

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::tests::assert_cannot_truncate;

        /// Cannot UPDATE file_id on storage_inline table
        #[test]
        fn test_cannot_change_immutables() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dummy_file(&mut transaction)?;
            let storage = Storage { content: "hello".into() };
            create_storage(&mut transaction, inode, &storage)?;
            transaction.commit()?;

            for (column, value) in [("file_id", "100")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE storage_inline SET {} = {} WHERE file_id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&inode.file_id()?]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change file_id");
            }

            Ok(())
        }

        /// Cannot TRUNCATE storage_inline table
        #[test]
        fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            assert_cannot_truncate(&mut transaction, "storage_inline")?;

            Ok(())
        }
    }
}
