//! CRUD operations for storage_internetarchive entities in PostgreSQL

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction};
use serde::Serialize;

/// A storage_internetarchive entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
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
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!("
            INSERT INTO stash.storage_internetarchive (file_id, ia_item, pathname, darked, last_probed)
            VALUES ($1, $2::text, $3::text, $4, $5)",
            self.file_id,
            self.ia_item,
            self.pathname,
            self.darked,
            self.last_probed
        ).execute(transaction).await?;
        Ok(())
    }

    /// Remove storages with given `ids`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<()> {
        if file_ids.is_empty() {
            return Ok(());
        }
        sqlx::query!("DELETE FROM stash.storage_internetarchive WHERE file_id = ANY($1)", file_ids)
            .execute(transaction).await?;
        Ok(())
    }

    /// Get internetarchive storage entities by exastash file ids.
    /// Entities which are not found will not be included in the resulting `Vec`.
    pub async fn find_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        if file_ids.is_empty() {
            return Ok(vec![]);
        }
        // Note that we can get more than one row per unique file_id
        let storages = sqlx::query_as!(Storage, "
            SELECT file_id, ia_item, pathname, darked, last_probed
            FROM stash.storage_internetarchive
            WHERE file_id = ANY($1)",
            file_ids
        ).fetch_all(transaction).await?;
        Ok(storages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use crate::db::tests::{new_primary_pool, new_secondary_pool};
    use crate::db::inode::create_dummy_file;
    use serial_test::serial;

    mod api {
        use super::*;

        /// If there is no internetarchive storage for a file, find_by_file_ids returns an empty Vec
        #[tokio::test]
        async fn test_no_storage() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[dummy.id]).await?, vec![]);

            Ok(())
        }

        /// If we add one internetarchive storage for a file, find_by_file_ids returns just that storage
        #[tokio::test]
        async fn test_create_storage_and_get_storage() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, ia_item: "item".into(), pathname: "path".into(), darked: false, last_probed: None };
            storage.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[dummy.id]).await?, vec![storage]);

            Ok(())
        }

        /// If we add multiple internetarchive storage for a file, find_by_file_ids returns those storages
        #[tokio::test]
        async fn test_multiple_create_storage_and_get_storage() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let storage1 = Storage { file_id: dummy.id, ia_item: "item1".into(), pathname: "path".into(), darked: false, last_probed: None };
            storage1.create(&mut transaction).await?;
            let storage2 = Storage { file_id: dummy.id, ia_item: "item2".into(), pathname: "path".into(), darked: true, last_probed: Some(util::now_no_nanos()) };
            storage2.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[dummy.id]).await?, vec![storage1, storage2]);

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::assert_cannot_truncate;

        /// Cannot UPDATE file_id, ia_item, or pathname on storage_internetarchive table
        #[tokio::test]
        async fn test_cannot_change_immutables() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            Storage { file_id: dummy.id, ia_item: "item".into(), pathname: "path".into(), darked: false, last_probed: None }.create(&mut transaction).await?;
            transaction.commit().await?;

            for (column, value) in [("file_id", "100"), ("ia_item", "'new'"), ("pathname", "'new'")] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE stash.storage_internetarchive SET {column} = {value} WHERE file_id = $1");
                let result = sqlx::query(&query).bind(&dummy.id).execute(&mut transaction).await;
                assert_eq!(result.expect_err("expected an error").to_string(), "error returned from database: cannot change file_id, ia_item, or pathname");
            }

            Ok(())
        }

        /// Cannot TRUNCATE storage_internetarchive table
        #[tokio::test]
        #[serial]
        async fn test_cannot_truncate() -> Result<()> {
            let pool = new_secondary_pool().await;

            let mut transaction = pool.begin().await?;
            assert_cannot_truncate(&mut transaction, "stash.storage_internetarchive").await;

            Ok(())
        }
    }
}
