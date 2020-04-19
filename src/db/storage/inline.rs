//! CRUD operations for storage_inline entities in PostgreSQL

use anyhow::Result;
use postgres::Transaction;
use serde::Serialize;

/// A storage_inline entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Storage {
    /// The id of the exastash file for which this storage exists
    pub file_id: i64,
    /// The content for this file
    pub content: Vec<u8>,
}

impl Storage {
    /// Create an inline storage entity in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub fn create(&self, transaction: &mut Transaction<'_>) -> Result<()> {
        transaction.execute(
            "INSERT INTO storage_inline (file_id, content)
             VALUES ($1::bigint, $2::bytea)",
            &[&self.file_id, &self.content]
        )?;
        Ok(())
    }

    /// Return a list of inline storage entities containing the data for a file.
    pub fn find_by_file_ids(transaction: &mut Transaction<'_>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        let rows = transaction.query(
            "SELECT file_id, content FROM storage_inline
             WHERE file_id = ANY($1::bigint[])", &[&file_ids]
        )?;
        assert!(rows.len() <= file_ids.len(), "received more rows than expected");
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let storage = Storage {
                file_id: row.get(0),
                content: row.get(1),
            };
            out.push(storage);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;
    use crate::db::inode::tests::create_dummy_file;

    mod api {
        use super::*;

        /// If there is no inline storage for a file, find_by_file_ids returns an empty Vec
        #[test]
        fn test_no_storage() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let file_id = create_dummy_file(&mut transaction)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[file_id])?, vec![]);

            Ok(())
        }

        /// If we add an inline storage for a file, find_by_file_ids returns that storage
        #[test]
        fn test_create_storage_and_get_storage() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let file_id = create_dummy_file(&mut transaction)?;
            let storage = Storage { file_id, content: "some content".into() };
            storage.create(&mut transaction)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[file_id])?, vec![storage]);

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
            let file_id = create_dummy_file(&mut transaction)?;
            let storage = Storage { file_id, content: "hello".into() };
            storage.create(&mut transaction)?;
            transaction.commit()?;

            for (column, value) in [("file_id", "100")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE storage_inline SET {} = {} WHERE file_id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&file_id]);
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
