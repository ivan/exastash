//! CRUD operations for storage_namedfiles entities in PostgreSQL

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction};
use serde::Serialize;

/// A storage_namedfiles entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct Storage {
    /// The id of the exastash file for which this storage exists
    pub file_id: i64,
    /// The name of the location containing this file
    pub location: String,
    /// The path to the file inside the location
    pub pathname: String,
    /// The time the location was last probed to check if this file is still accessible
    pub last_probed: Option<DateTime<Utc>>,
}

impl Storage {
    /// Create an namedfiles storage entity in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!(r#"
            INSERT INTO stash.storage_namedfiles (file_id, location, pathname, last_probed)
            VALUES ($1, $2, $3, $4)"#,
            self.file_id, self.location, self.pathname, self.last_probed
        ).execute(&mut **transaction).await?;
        Ok(())
    }

    /// Delete the database references to namedfiles storages with given `file_ids`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn delete_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<()> {
        if file_ids.is_empty() {
            return Ok(());
        }
        sqlx::query!(r#"
            DELETE FROM stash.storage_namedfiles WHERE file_id = ANY($1)"#, file_ids
        ).execute(&mut **transaction).await?;
        Ok(())
    }

    /// Get namedfiles storage entities with the given `file_ids`.
    /// Entities which are not found will not be included in the resulting `Vec`.
    pub async fn find_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        if file_ids.is_empty() {
            return Ok(vec![]);
        }
        // Note that we can get more than one row per unique file_id
        let storages = sqlx::query_as!(Storage, r#"
            SELECT file_id, location, pathname, last_probed
            FROM stash.storage_namedfiles
            WHERE file_id = ANY($1)"#, file_ids
        ).fetch_all(&mut **transaction).await?;
        Ok(storages)
    }
}
