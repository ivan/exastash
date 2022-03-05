//! CRUD operations for storage_fofs entities in PostgreSQL

use tracing::info;
use anyhow::Result;
use sqlx::{Postgres, Transaction};
use serde::Serialize;

/// A pile entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct Pile {
    /// Unique pile id
    id: i32,
    /// The number of files to place in each cell before marking it full and making a new cell
    files_per_cell: i32,
    /// The machine on which the pile is stored
    hostname: String,
    /// The absolute path to the root directory of the pile on the machine
    path: String,
}

impl Pile {
    /// Return a `Vec<Pile>` for the corresponding list of pile `ids`.
    /// There is no error on missing piles.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i32]) -> Result<Vec<Pile>> {
        if ids.is_empty() {
            return Ok(vec![])
        }
        let piles = sqlx::query_as!(Pile, "
            SELECT id, files_per_cell, hostname, path FROM stash.piles WHERE id = ANY($1)", ids
        ).fetch_all(transaction).await?;
        Ok(piles)
    }
}

/// A new pile entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct NewPile {
    /// The number of files to place in each cell before marking it full and making a new cell
    files_per_cell: i32,
    /// The machine on which the pile is stored
    hostname: String,
    /// The absolute path to the root directory of the pile on the machine
    path: String,
}

impl NewPile {
    /// Create an pile entity in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!(
            "INSERT INTO stash.piles (files_per_cell, hostname, path) VALUES ($1, $2::text, $3)",
            self.files_per_cell, self.hostname, self.path
        ).execute(transaction).await?;
        Ok(())
    }
}



/// A cell entity
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct Cell {
    /// Unique cell id
    id: i32,
    /// The pile we are parented in
    pile_id: i32,
    /// Whether we are full because we have reached the per-cell file limit
    full: bool,
}

impl Cell {
    /// Set whether a cell is full or not
    pub async fn set_full(transaction: &mut Transaction<'_, Postgres>, id: i32, full: bool) -> Result<()> {
        info!("setting full = {} on cell id = {:?}", full, id);
        sqlx::query!(r#"UPDATE stash.cells SET "full" = $1 WHERE id = $2"#, full, id)
            .execute(transaction).await?;
        Ok(())
    }
}

/// A new cell entity
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
pub struct NewCell {
    /// The pile we are parented in
    pile_id: i32,
}

impl NewCell {
    /// Create an cell entity in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!("INSERT INTO stash.cells (pile_id) VALUES ($1)", self.pile_id).execute(transaction).await?;
        Ok(())
    }
}



/// A storage_fofs entity
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct Storage {
    /// The id of the exastash file for which this storage exists
    pub file_id: i64,
    /// The fofs cell that contains a copy of this file
    pub cell_id: i32,
}

impl Storage {
    /// Create an fofs storage entity in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!("
            INSERT INTO stash.storage_fofs (file_id, cell_id)
            VALUES ($1, $2)",
            self.file_id,
            self.cell_id,
        ).execute(transaction).await?;
        Ok(())
    }

    /// Remove storages with given `ids`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<()> {
        if file_ids.is_empty() {
            return Ok(());
        }
        sqlx::query!("DELETE FROM stash.storage_fofs WHERE file_id = ANY($1)", file_ids)
            .execute(transaction).await?;
        Ok(())
    }

    /// Get fofs storage entities by exastash file ids.
    /// Entities which are not found will not be included in the resulting `Vec`.
    pub async fn find_by_file_ids(transaction: &mut Transaction<'_, Postgres>, file_ids: &[i64]) -> Result<Vec<Storage>> {
        if file_ids.is_empty() {
            return Ok(vec![]);
        }
        // Note that we can get more than one row per unique file_id
        let storages = sqlx::query_as!(Storage, "
            SELECT file_id, cell_id
            FROM stash.storage_fofs
            WHERE file_id = ANY($1)",
            file_ids
        ).fetch_all(transaction).await?;
        Ok(storages)
    }
}
