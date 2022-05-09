//! CRUD operations for storage_fofs entities in PostgreSQL

use anyhow::Result;
use sqlx::{Postgres, Transaction};
use sqlx::types::Decimal;
use serde::Serialize;

/// A pile entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct Pile {
    /// Unique pile id
    pub id: i32,
    /// The number of files to place in each cell before marking it full and making a new cell.
    /// For performance reasons, this is not strictly enforced unless fullness_check_ratio = 1;
    /// the cell may go over the threshold.
    ///
    /// A typical value is 10000.
    pub files_per_cell: i32,
    /// The machine on which the pile is stored
    pub hostname: String,
    /// The absolute path to the root directory of the pile on the machine
    pub path: String,
    /// How often to check whether a cell in this pile has reached capacity before
    /// marking it full; 0 = never, 1 = always
    ///
    /// For files_per_cell = 10000, a typical value for fullness_check_ratio is 0.01,
    /// thus causing ~100 listdir calls on a 10000-sized cell as it grows to capacity.
    pub fullness_check_ratio: Decimal,
}

impl Pile {
    /// Return a `Vec<Pile>` for the corresponding list of pile `ids`.
    /// There is no error on missing piles.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i32]) -> Result<Vec<Pile>> {
        if ids.is_empty() {
            return Ok(vec![])
        }
        let piles = sqlx::query_as!(Pile, "
            SELECT id, files_per_cell, hostname, path, fullness_check_ratio FROM stash.piles WHERE id = ANY($1)", ids
        ).fetch_all(transaction).await?;
        Ok(piles)
    }
}

/// A new pile entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct NewPile {
    /// The number of files to place in each cell before marking it full and making a new cell.
    /// For performance reasons, this is not strictly enforced unless fullness_check_ratio = 1;
    /// the cell may go over the threshold.
    ///
    /// A typical value is 10000.
    pub files_per_cell: i32,
    /// The machine on which the pile is stored
    pub hostname: String,
    /// The absolute path to the root directory of the pile on the machine
    pub path: String,
    /// How often to check whether a cell in this pile has reached capacity before
    /// marking it full; 0 = never, 1 = always
    ///
    /// For files_per_cell = 10000, a typical value for fullness_check_ratio is 0.01,
    /// thus causing ~100 listdir calls on a 10000-sized cell as it grows to capacity.
    pub fullness_check_ratio: Decimal,
}

impl NewPile {
    /// Create an pile entity in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!(
            "INSERT INTO stash.piles (files_per_cell, hostname, path, fullness_check_ratio) VALUES ($1, $2::text, $3, $4)",
            self.files_per_cell, self.hostname, self.path, self.fullness_check_ratio
        ).execute(transaction).await?;
        Ok(())
    }
}



/// A cell entity
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub struct Cell {
    /// Unique cell id
    pub id: i32,
    /// The pile we are parented in
    pub pile_id: i32,
    /// Whether we are full because we have reached the per-cell file limit
    pub full: bool,
}

impl Cell {
    /// Get cell entities where `id` is any of `cell_ids`.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, cell_ids: &[i32]) -> Result<Vec<Cell>> {
        if cell_ids.is_empty() {
            return Ok(vec![]);
        }
        let cells = sqlx::query_as!(Cell, r#"
            SELECT id, pile_id, "full"
            FROM stash.cells
            WHERE id = ANY($1)"#,
            cell_ids
        ).fetch_all(transaction).await?;
        Ok(cells)
    }

    /// Get cell entities where `pile_id` is any of `pile_ids` and `full` = the given `full`.
    /// Entities which are not found will not be included in the resulting `Vec`.
    pub async fn find_by_pile_ids_and_fullness(transaction: &mut Transaction<'_, Postgres>, pile_ids: &[i32], full: bool) -> Result<Vec<Cell>> {
        if pile_ids.is_empty() {
            return Ok(vec![]);
        }
        let cells = sqlx::query_as!(Cell, r#"
            SELECT id, pile_id, "full"
            FROM stash.cells
            WHERE pile_id = ANY($1) AND "full" = $2"#,
            pile_ids, full
        ).fetch_all(transaction).await?;
        Ok(cells)
    }

    /// Set whether a cell is full or not
    pub async fn set_full(transaction: &mut Transaction<'_, Postgres>, id: i32, full: bool) -> Result<()> {
        sqlx::query!(r#"UPDATE stash.cells SET "full" = $1 WHERE id = $2"#, full, id)
            .execute(transaction).await?;
        Ok(())
    }
}

/// A new cell entity
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
pub struct NewCell {
    /// The pile we are parented in
    pub pile_id: i32,
}

impl NewCell {
    /// Create an cell entity in the database.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<Cell> {
        let id = sqlx::query_scalar!("
            INSERT INTO stash.cells (pile_id)
            VALUES ($1)
            RETURNING id",
            self.pile_id
        ).fetch_one(transaction).await?;
        assert!(id >= 1);
        Ok(Cell {
            id,
            pile_id: self.pile_id,
            full: false,
        })
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
