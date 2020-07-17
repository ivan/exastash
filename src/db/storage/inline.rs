//! CRUD operations for storage_inline entities in PostgreSQL

use anyhow::Result;
use sqlx::{Postgres, Transaction};
use serde::Serialize;

/// A storage_inline entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct Storage {
    /// The id of the exastash file for which this storage exists
    pub file_id: i64,
    /// The content for this file
    #[serde(skip_serializing)]
    pub content: Vec<u8>,
}

impl Storage {
    /// Create an inline storage entity in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<Self> {
        sqlx::query(
            "INSERT INTO storage_inline (file_id, content)
             VALUES ($1::bigint, $2::bytea)",
        )
            .bind(&self.file_id)
            .bind(&self.content)
            .execute(transaction)
            .await?;
        Ok(self)
    }

    /// Return a list of inline storage entities containing the data for a file.
    pub async fn find_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        Ok(sqlx::query_as::<_, Storage>(
            "SELECT file_id, content FROM storage_inline
             WHERE file_id = ANY($1::bigint[])"
        )
            .bind(file_ids)
            .fetch_all(transaction).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::tests::{main_test_instance, truncate_test_instance};
    use crate::db::inode::tests::create_dummy_file;
    use serial_test::serial;

    mod api {
        use super::*;

        /// If there is no inline storage for a file, find_by_file_ids returns an empty Vec
        #[tokio::test]
        async fn test_no_storage() -> Result<()> {
            let client = main_test_instance().await;

            let mut transaction = client.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = client.begin().await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[dummy.id]).await?, vec![]);

            Ok(())
        }

        /// If we add an inline storage for a file, find_by_file_ids returns that storage
        #[tokio::test]
        async fn test_create_storage_and_get_storage() -> Result<()> {
            let client = main_test_instance().await;

            let mut transaction = client.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            let storage = Storage { file_id: dummy.id, content: "some content".into() }.create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = client.begin().await?;
            assert_eq!(Storage::find_by_file_ids(&mut transaction, &[dummy.id]).await?, vec![storage]);

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::tests::assert_cannot_truncate;

        /// Cannot UPDATE file_id on storage_inline table
        #[tokio::test]
        async fn test_cannot_change_immutables() -> Result<()> {
            let client = main_test_instance().await;

            let mut transaction = client.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            Storage { file_id: dummy.id, content: "hello".into() }.create(&mut transaction).await?;
            transaction.commit().await?;

            for (column, value) in &[("file_id", "100")] {
                let mut transaction = client.begin().await?;
                let query = format!("UPDATE storage_inline SET {column} = {value} WHERE file_id = $1::bigint");
                let result = sqlx::query(&query).bind(&dummy.id).execute(&mut transaction).await;
                assert_eq!(result.err().expect("expected an error").to_string(), "error returned from database: cannot change file_id");
            }

            Ok(())
        }

        /// Cannot TRUNCATE storage_inline table
        #[tokio::test]
        #[serial]
        async fn test_cannot_truncate() -> Result<()> {
            let client = truncate_test_instance().await;

            let mut transaction = client.begin().await?;
            assert_cannot_truncate(&mut transaction, "storage_inline").await;

            Ok(())
        }
    }
}
