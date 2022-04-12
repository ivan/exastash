//! CRUD operations for storage_inline entities in PostgreSQL

use anyhow::Result;
use sqlx::{Postgres, Transaction};
use serde::Serialize;

/// A storage_inline entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct Storage {
    /// The id of the exastash file for which this storage exists
    pub file_id: i64,
    /// The zstd-compressed content for this file
    #[serde(skip_serializing)]
    pub content_zstd: Vec<u8>,
}

impl Storage {
    /// Create an inline storage entity in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!("
            INSERT INTO stash.storage_inline (file_id, content_zstd)
            VALUES ($1, $2)",
            &self.file_id, &self.content_zstd
        ).execute(transaction).await?;
        Ok(())
    }

    /// Create an inline storage entity in the database if an entity with `file_id` does not already exist.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn maybe_create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!("
            INSERT INTO stash.storage_inline (file_id, content_zstd)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING",
            &self.file_id, &self.content_zstd
        ).execute(transaction).await?;
        Ok(())
    }

    /// Remove storages with given `ids`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<()> {
        if file_ids.is_empty() {
            return Ok(());
        }
        sqlx::query!("
            DELETE FROM stash.storage_inline
            WHERE file_id = ANY($1)", file_ids
        ).execute(transaction).await?;
        Ok(())
    }

    /// Return a list of inline storage entities containing the data for a file.
    pub async fn find_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        if file_ids.is_empty() {
            return Ok(vec![]);
        }
        let storages =
            sqlx::query_as!(Storage, "
                SELECT file_id, content_zstd
                FROM stash.storage_inline
                WHERE file_id = ANY($1)", file_ids
            ).fetch_all(transaction).await?;
        Ok(storages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::tests::{new_primary_pool, new_secondary_pool};
    use crate::db::inode::create_dummy_file;
    use serial_test::serial;

    mod api {
        use super::*;

        /// If there is no inline storage for a file, find_by_file_ids returns an empty Vec
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

        /// If we add an inline storage for a file, find_by_file_ids returns that storage
        #[tokio::test]
        async fn test_create_storage_and_get_storage() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, content_zstd: "invalid zstd is ok".into() };
            storage.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[dummy.id]).await?, vec![storage]);

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::assert_cannot_truncate;

        /// Cannot UPDATE file_id on storage_inline table
        #[tokio::test]
        async fn test_cannot_change_immutables() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            Storage { file_id: dummy.id, content_zstd: "invalid zstd is ok".into() }.create(&mut transaction).await?;
            transaction.commit().await?;

            for (column, value) in [("file_id", "100")] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE stash.storage_inline SET {column} = {value} WHERE file_id = $1");
                let result = sqlx::query(&query).bind(&dummy.id).execute(&mut transaction).await;
                assert_eq!(result.expect_err("expected an error").to_string(), "error returned from database: cannot change file_id");
            }

            Ok(())
        }

        /// Cannot TRUNCATE storage_inline table
        #[tokio::test]
        #[serial]
        async fn test_cannot_truncate() -> Result<()> {
            let pool = new_secondary_pool().await;

            let mut transaction = pool.begin().await?;
            assert_cannot_truncate(&mut transaction, "stash.storage_inline").await;

            Ok(())
        }
    }
}
