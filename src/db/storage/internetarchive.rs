//! CRUD operations for storage_internetarchive entities in PostgreSQL

use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio_postgres::Transaction;
use serde::Serialize;

/// A storage_internetarchive entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Storage {
    /// The id of the exastash file for which this storage exists
    pub file_id: i64,
    /// The Internet Archive item containing this file
    pub ia_item: String,
    /// The path to the file inside the item
    pub pathname: String,
    /// Whether the Internet Archive item is darked (inaccessible)
    pub darked: bool,
    /// The time Internet Archive was last probed to check if this file is still accessible
    pub last_probed: Option<DateTime<Utc>>,
}

impl Storage {
    /// Create an internetarchive storage entity in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_>) -> Result<()> {
        transaction.execute(
            "INSERT INTO storage_internetarchive (file_id, ia_item, pathname, darked, last_probed)
             VALUES ($1::bigint, $2::text, $3::text, $4::boolean, $5::timestamptz)",
            &[&self.file_id, &self.ia_item, &self.pathname, &self.darked, &self.last_probed]
        ).await?;
        Ok(())
    }

    /// Get internetarchive storage entities by exastash file ids.
    /// Entities which are not found will not be included in the resulting `Vec`.
    pub async fn find_by_file_ids(transaction: &mut Transaction<'_>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        // Note that we can get more than one row per unique file_id
        let rows = transaction.query(
            "SELECT file_id, ia_item, pathname, darked, last_probed
             FROM storage_internetarchive
             WHERE file_id = ANY($1::bigint[])",
            &[&file_ids]
        ).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let storage = Storage {
                file_id: row.get(0),
                ia_item: row.get(1),
                pathname: row.get(2),
                darked: row.get(3),
                last_probed: row.get(4),
            };
            out.push(storage);
        }
        Ok(out)    
    }
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

        /// If there is no internetarchive storage for a file, find_by_file_ids returns an empty Vec
        #[tokio::test]
        async fn test_no_storage() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let file_id = create_dummy_file(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[file_id]).await?, vec![]);

            Ok(())
        }

        /// If we add one internetarchive storage for a file, find_by_file_ids returns just that storage
        #[tokio::test]
        async fn test_create_storage_and_get_storage() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let file_id = create_dummy_file(&mut transaction).await?;
            let storage = Storage { file_id, ia_item: "item".into(), pathname: "path".into(), darked: false, last_probed: None };
            storage.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[file_id]).await?, vec![storage]);

            Ok(())
        }

        /// If we add multiple internetarchive storage for a file, find_by_file_ids returns those storages
        #[tokio::test]
        async fn test_multiple_create_storage_and_get_storage() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let file_id = create_dummy_file(&mut transaction).await?;
            let storage1 = Storage { file_id, ia_item: "item1".into(), pathname: "path".into(), darked: false, last_probed: None };
            let storage2 = Storage { file_id, ia_item: "item2".into(), pathname: "path".into(), darked: true, last_probed: Some(util::now_no_nanos()) };
            storage1.create(&mut transaction).await?;
            storage2.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[file_id]).await?, vec![storage1, storage2]);

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::tests::assert_cannot_truncate;

        /// Cannot UPDATE file_id, ia_item, or pathname on storage_internetarchive table
        #[tokio::test]
        async fn test_cannot_change_immutables() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let file_id = create_dummy_file(&mut transaction).await?;
            let storage = Storage { file_id, ia_item: "item".into(), pathname: "path".into(), darked: false, last_probed: None };
            storage.create(&mut transaction).await?;
            transaction.commit().await?;

            for (column, value) in [("file_id", "100"), ("ia_item", "'new'"), ("pathname", "'new'")].iter() {
                let transaction = start_transaction(&mut client).await?;
                let query = format!("UPDATE storage_internetarchive SET {} = {} WHERE file_id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&file_id]).await;
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change file_id, ia_item, or pathname");
            }

            Ok(())
        }

        /// Cannot TRUNCATE storage_internetarchive table
        #[tokio::test]
        async fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            assert_cannot_truncate(&mut transaction, "storage_internetarchive").await;

            Ok(())
        }
    }
}
