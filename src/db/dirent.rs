//! CRUD operations for dirent entities in PostgreSQL

use crate::db::inode::InodeId;
use anyhow::{bail, Error, Result};
use futures::{StreamExt, TryStreamExt};
use sqlx::{Postgres, Transaction};

/// A (dir, file, symlink) tuple that is useful when interacting with
/// the dirents table.
#[must_use]
#[derive(Debug, Copy, Clone)]
pub struct InodeTuple(pub Option<i64>, pub Option<i64>, pub Option<i64>);

impl From<InodeId> for InodeTuple {
    fn from(inode: InodeId) -> InodeTuple {
        match inode {
            InodeId::Dir(id)     => InodeTuple(Some(id), None, None),
            InodeId::File(id)    => InodeTuple(None, Some(id), None),
            InodeId::Symlink(id) => InodeTuple(None, None, Some(id)),
        }
    }
}

impl TryFrom<InodeTuple> for InodeId {
    type Error = Error;

    fn try_from(tuple: InodeTuple) -> Result<InodeId> {
        match tuple {
            InodeTuple(Some(id), None, None) => Ok(InodeId::Dir(id)),
            InodeTuple(None, Some(id), None) => Ok(InodeId::File(id)),
            InodeTuple(None, None, Some(id)) => Ok(InodeId::Symlink(id)),
            _                                => bail!("tuple {:?} does not have exactly 1 Some", tuple),
        }
    }
}

/// A directory entry
#[derive(Debug, PartialEq, Eq)]
pub struct Dirent {
    /// The parent directory
    pub parent: i64,
    /// The basename (i.e. file name, not the whole path)
    pub basename: String,
    /// The inode the entry points to
    pub child: InodeId
}

impl From<DirentRow> for Dirent {
    fn from(row: DirentRow) -> Self {
        let tuple = InodeTuple(row.child_dir, row.child_file, row.child_symlink);
        let inode_id: InodeId = tuple.try_into().unwrap();
        Dirent::new(row.parent, row.basename, inode_id)
    }
}

struct DirentRow {
    parent: i64,
    basename: String,
    child_dir: Option<i64>,
    child_file: Option<i64>,
    child_symlink: Option<i64>,
}

impl Dirent {
    /// Return a `Dirent` with the given `basename` and `child` inode
    pub fn new<S: Into<String>>(parent: i64, basename: S, child: InodeId) -> Dirent {
        Dirent { parent, basename: basename.into(), child }
    }

    /// Create a directory entry.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        let InodeTuple(child_dir, child_file, child_symlink) = self.child.into();
        sqlx::query!(r#"
            INSERT INTO stash.dirents (parent, basename, child_dir, child_file, child_symlink)
            VALUES ($1, $2::text, $3, $4, $5)"#,
            self.parent, &self.basename, child_dir, child_file, child_symlink
        ).execute(transaction).await?;
        Ok(())
    }

    /// Remove this directory entry, moving it to the dirents_history table.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query!(r#"
            DELETE FROM stash.dirents
            WHERE parent = $1 AND basename = $2"#, self.parent, &self.basename
        ).execute(transaction).await?;
        Ok(())
    }

    /// Remove a directory entry by `parent` and `basename`, moving it to the dirents_history table.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove_by_parent_basename(transaction: &mut Transaction<'_, Postgres>, parent: i64, basename: &str) -> Result<()> {
        sqlx::query!(r#"
            DELETE FROM stash.dirents
            WHERE parent = $1 AND basename = $2"#, parent, basename
        ).execute(transaction).await?;
        Ok(())
    }

    /// Remove a directory entry by `child_dir`, moving it to the dirents_history table.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove_by_child_dir(transaction: &mut Transaction<'_, Postgres>, child_dir: i64) -> Result<()> {
        sqlx::query!(r#"
            DELETE FROM stash.dirents
            WHERE child_dir = $1"#, child_dir
        ).execute(transaction).await?;
        Ok(())
    }

    /// Return a `Vec<Dirent>` for all `Dirent`s with the given parents.
    /// There is no error on missing parents.
    pub async fn find_by_parents(transaction: &mut Transaction<'_, Postgres>, parents: &[i64]) -> Result<Vec<Dirent>> {
        // `child_dir IS DISTINCT FROM 1` filters out the root directory self-reference
        let dirents = sqlx::query_as!(DirentRow, r#"
            SELECT parent, basename, child_dir, child_file, child_symlink
            FROM stash.dirents
            WHERE
                parent = ANY($1) AND
                child_dir IS DISTINCT FROM 1"#,
            parents
        )
            .fetch(transaction)
            .map(|result| result.map(|row| row.into()))
            .try_collect().await?;
        Ok(dirents)
    }

    /// Return an `Option<Dirent>` if a `Dirent` exists with the given `parent` and `basename`.
    pub async fn find_by_parent_and_basename(transaction: &mut Transaction<'_, Postgres>, parent: i64, basename: &str) -> Result<Option<Dirent>> {
        // `child_dir IS DISTINCT FROM 1` filters out the root directory self-reference
        let row = sqlx::query_as!(DirentRow,
            "SELECT parent, basename, child_dir, child_file, child_symlink
             FROM stash.dirents
             WHERE parent = $1 AND basename = $2 AND child_dir IS DISTINCT FROM 1", parent, basename
        )
            .fetch_optional(transaction).await?;
        Ok(row.map(Into::into))
    }

    /// Return a `Vec` of `Dirent`s for all dirents that exist with given `parent` and any one of `basenames`.
    pub async fn find_by_parent_and_basenames(transaction: &mut Transaction<'_, Postgres>, parent: i64, basenames: &[&str]) -> Result<Vec<Dirent>> {
        // sqlx::query_as! insists on String
        let basenames: Vec<String> = basenames.iter().map(|s| s.to_string()).collect();
        // `child_dir IS DISTINCT FROM 1` filters out the root directory self-reference
        let dirents = sqlx::query_as!(DirentRow, r#"
            SELECT parent, basename, child_dir, child_file, child_symlink
            FROM stash.dirents
            WHERE
                parent = $1 AND
                basename = ANY($2) AND
                child_dir IS DISTINCT FROM 1"#,
            parent, &basenames
        )
            .fetch(transaction)
            .map(|result| result.map(|row| row.into()))
            .try_collect().await?;
        Ok(dirents)
    }

    /// Return an `Option<Dirent>` if a `Dirent` exists with the given `child_dir`.
    pub async fn find_by_child_dir(transaction: &mut Transaction<'_, Postgres>, child_dir: i64) -> Result<Option<Dirent>> {
        let maybe_dirent = sqlx::query_as!(DirentRow, r#"
            SELECT parent, basename, child_dir, child_file, child_symlink
            FROM stash.dirents
            WHERE child_dir = $1"#, child_dir
        )
            .fetch_optional(transaction).await?
            .map(Into::into);
        Ok(maybe_dirent)
    }

    /// Return a count of the number of dirents in the database.
    pub async fn count(transaction: &mut Transaction<'_, Postgres>) -> Result<i64> {
        let count: i64 = sqlx::query_scalar!("SELECT COUNT(parent) FROM stash.dirents")
            .fetch_one(transaction).await?
            .unwrap();
        Ok(count)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::inode;
    use crate::db::tests::{new_primary_pool, new_secondary_pool};
    use chrono::Utc;
    use atomic_counter::{AtomicCounter, RelaxedCounter};
    use once_cell::sync::Lazy;
    use serial_test::serial;

    static BASENAME_COUNTER: Lazy<RelaxedCounter> = Lazy::new(|| {
        RelaxedCounter::new(1)
    });

    pub(crate) fn make_basename(prefix: &str) -> String {
        let num = BASENAME_COUNTER.inc();
        format!("{prefix}_{num}")
    }

    mod api {
        use super::*;

        #[tokio::test]
        async fn test_create_dirent_and_list_dir() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let birth = inode::Birth::here_and_now();
            let parent = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(1, make_basename("parent"), InodeId::Dir(parent.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            let child_file = inode::NewFile { size: 0, executable: false, mtime: Utc::now(), birth: birth.clone(), b3sum: None }.create(&mut transaction).await?;
            let child_symlink = inode::NewSymlink { target: "target".into(), mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(parent.id, "child_dir", InodeId::Dir(child_dir.id)).create(&mut transaction).await?;
            Dirent::new(parent.id, "child_file", InodeId::File(child_file.id)).create(&mut transaction).await?;
            Dirent::new(parent.id, "child_symlink", InodeId::Symlink(child_symlink.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_eq!(Dirent::find_by_parents(&mut transaction, &[child_dir.id]).await?, vec![]);
            assert_eq!(Dirent::find_by_parents(&mut transaction, &[parent.id]).await?, vec![
                Dirent::new(parent.id, "child_dir", InodeId::Dir(child_dir.id)),
                Dirent::new(parent.id, "child_file", InodeId::File(child_file.id)),
                Dirent::new(parent.id, "child_symlink", InodeId::Symlink(child_symlink.id)),
            ]);

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::assert_cannot_truncate;

        /// Cannot have child_dir equal to parent
        #[tokio::test]
        async fn test_cannot_have_child_dir_equal_to_parent() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let parent = inode::NewDir { mtime: Utc::now(), birth: inode::Birth::here_and_now() }.create(&mut transaction).await?;
            let result = Dirent::new(parent.id, "self", InodeId::Dir(parent.id)).create(&mut transaction).await;
            assert_eq!(
                result.expect_err("expected an error").to_string(),
                "error returned from database: new row for relation \"dirents\" violates check constraint \"dirents_check\""
            );

            Ok(())
        }

        /// Cannot insert more than one dirent per transaction (otherwise cycles could be created)
        #[tokio::test]
        async fn test_cannot_create_more_than_one_dirent() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let birth  = inode::Birth::here_and_now();
            let one    = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            let two    = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(one.id, make_basename("two"), InodeId::Dir(two.id)).create(&mut transaction).await?;
            let result = Dirent::new(two.id, make_basename("one"), InodeId::Dir(one.id)).create(&mut transaction).await;
            assert_eq!(
                result.expect_err("expected an error").to_string(),
                "error returned from database: cannot insert or delete more than one dirent with a child_dir per transaction"
            );

            Ok(())
        }

        /// Cannot create a dirents cycle by removing a dirent and creating a replacement
        #[tokio::test]
        async fn test_cannot_create_dirents_cycle() -> Result<()> {
            let pool = new_primary_pool().await;

            let birth  = inode::Birth::here_and_now();

            let mut transaction = pool.begin().await?;
            let test = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(1, make_basename("test_cannot_create_dirents_cycle"), InodeId::Dir(test.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let a = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(test.id, "a", InodeId::Dir(a.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let b = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(a.id, "b", InodeId::Dir(b.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let c = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(b.id, "c", InodeId::Dir(c.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            // Try to remove a -> b, add c -> b (which would create a cycle b -> c -> b)
            let mut transaction = pool.begin().await?;
            Dirent::remove_by_parent_basename(&mut transaction, a.id, "b").await?;
            let result = Dirent::new(c.id, "b", InodeId::Dir(b.id)).create(&mut transaction).await;
            assert_eq!(
                result.expect_err("expected an error").to_string(),
                "error returned from database: cannot insert or delete more than one dirent with a child_dir per transaction"
            );

            Ok(())
        }

        /// Cannot UPDATE any row in dirents table
        #[tokio::test]
        async fn test_cannot_update() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let birth = inode::Birth::here_and_now();
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(1, make_basename("child_dir"), InodeId::Dir(child_dir.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            for (column, value) in [("parent", "100"), ("basename", "'new'"), ("child_dir", "1"), ("child_file", "1"), ("child_symlink", "1")] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE stash.dirents SET {column} = {value} WHERE parent = $1 AND child_dir = $2");
                let result = sqlx::query(&query).bind(1i64).bind(child_dir.id).execute(&mut transaction).await;
                assert_eq!(
                    result.expect_err("expected an error").to_string(),
                    "error returned from database: cannot change parent, basename, or child_*"
                );
            }

            Ok(())
        }

        /// Cannot TRUNCATE dirents table
        #[tokio::test]
        #[serial]
        async fn test_cannot_truncate() -> Result<()> {
            let pool = new_secondary_pool().await;

            let mut transaction = pool.begin().await?;
            let birth = inode::Birth::here_and_now();
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(1, make_basename("child_dir"), InodeId::Dir(child_dir.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            assert_cannot_truncate(&mut transaction, "stash.dirents").await;

            Ok(())
        }

        /// Directory cannot be a child twice in some directory
        #[tokio::test]
        async fn test_directory_cannot_have_more_than_one_basename() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let birth = inode::Birth::here_and_now();
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(1, make_basename("child_dir"), InodeId::Dir(child_dir.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let result = Dirent::new(1, make_basename("child_dir_again"), InodeId::Dir(child_dir.id)).create(&mut transaction).await;
            assert_eq!(
                result.expect_err("expected an error").to_string(),
                "error returned from database: duplicate key value violates unique constraint \"dirents_child_dir_index\""
            );

            Ok(())
        }

        /// Directory cannot be a child of more than one parent
        #[tokio::test]
        async fn test_directory_cannot_be_multiparented() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let birth = inode::Birth::here_and_now();
            let middle = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(1, make_basename("middle"), InodeId::Dir(middle.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let child = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(middle.id, make_basename("child"), InodeId::Dir(child.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let result = Dirent::new(1, make_basename("child"), InodeId::Dir(child.id)).create(&mut transaction).await;
            assert_eq!(
                result.expect_err("expected an error").to_string(),
                "error returned from database: duplicate key value violates unique constraint \"dirents_child_dir_index\""
            );

            Ok(())
        }

        /// Basename cannot be "", "/", ".", or ".."
        /// Basename cannot be > 255 bytes
        #[tokio::test]
        async fn test_basename_cannot_be_specials_or_too_long() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let birth = inode::Birth::here_and_now();
            let parent = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(1, make_basename("parent"), InodeId::Dir(parent.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            for basename in ["", "/", ".", "..", &"x".repeat(256)] {
                let mut transaction = pool.begin().await?;
                // Avoid using a child dir because the mutual FK results in "deadlock detected"
                // some of the time instead of the error we want to see
                let child = inode::NewFile { mtime: Utc::now(), birth: birth.clone(), size: 0, executable: false, b3sum: None }.create(&mut transaction).await?;
                let result = Dirent::new(parent.id, basename.to_string(), InodeId::Dir(child.id)).create(&mut transaction).await;
                assert_eq!(
                    result.expect_err("expected an error").to_string(),
                    "error returned from database: value for domain stash.linux_basename violates check constraint \"linux_basename_check\""
                );
            }

            Ok(())
        }
    }
}
