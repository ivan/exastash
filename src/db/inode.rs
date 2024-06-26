//! CRUD operations for dir, file, and symlink entities in PostgreSQL

use futures::StreamExt;
use futures::TryStreamExt;
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction};
use serde::Serialize;
use std::collections::HashMap;
use crate::EXASTASH_VERSION;
use crate::db;
use crate::util;
use crate::db::dirent::Dirent;

/// A dir, file, or symlink
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum InodeId {
    /// A directory
    Dir(i64),
    /// A regular file
    File(i64),
    /// A symbolic link
    Symlink(i64),
}

impl InodeId {
    /// Return the directory id for this inode, if it is one
    pub fn dir_id(self) -> Result<i64> {
        match self {
            InodeId::Dir(id) => Ok(id),
            _ => bail!("{:?} is not a dir", self),
        }
    }

    /// Return the file id for this inode, if it is one
    pub fn file_id(self) -> Result<i64> {
        match self {
            InodeId::File(id) => Ok(id),
            _ => bail!("{:?} is not a file", self),
        }
    }

    /// Return the symlink id for this inode, if it is one
    pub fn symlink_id(self) -> Result<i64> {
        match self {
            InodeId::Symlink(id) => Ok(id),
            _ => bail!("{:?} is not a symlink", self),
        }
    }
}

/// birth_time, birth_version, and birth_hostname for a dir/file/symlink
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Birth {
    /// The time at which a dir, file, or symlink was created
    pub time: DateTime<Utc>,
    /// The exastash version with which a dir, file, or symlink was a created
    pub version: i16,
    /// The hostname of the machine on which a dir, file, or symlink was a created
    pub hostname: String,
}

impl Birth {
    /// Return a `Birth` with time set to now, version set to the current exastash version,
    /// and hostname set to the machine's hostname.
    pub fn here_and_now() -> Birth {
        Birth { time: util::now_no_nanos(), version: EXASTASH_VERSION, hostname: util::get_hostname() }
    }
}

/// A directory
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Dir {
    /// ID
    pub id: i64,
    /// Modification time
    pub mtime: DateTime<Utc>,
    /// Birth information
    pub birth: Birth,
}

#[derive(Debug)]
struct DirRow {
    id: i64,
    mtime: DateTime<Utc>,
    birth_time: DateTime<Utc>,
    birth_version: i16,
    birth_hostname: String,
}

impl From<DirRow> for Dir {
    fn from(row: DirRow) -> Self {
        let mtime = row.mtime;
        util::assert_without_nanos(mtime);
        Dir {
            id: row.id,
            mtime,
            birth: Birth {
                time: row.birth_time,
                version: row.birth_version,
                hostname: row.birth_hostname,
            }
        }
    }
}

impl Dir {
    /// Return a `Vec<Dir>` for the corresponding list of dir `ids`.
    /// There is no error on missing dirs.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<Vec<Dir>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let dirs = sqlx::query_as!(DirRow, r#"
            SELECT id, mtime, birth_time, birth_version, birth_hostname
            FROM stash.dirs
            WHERE id = ANY($1)"#, ids
        )
            .fetch(&mut **transaction)
            .map(|result| result.map(|row| row.into()))
            .try_collect().await?;
        Ok(dirs)
    }

    /// Delete dirs with given `ids`.
    ///
    /// Note that that foreign key constraints in the database require removing
    /// the associated dirents first (where `child_dir` is one of the `ids`).
    ///
    /// Does not commit the transaction, you must do so yourself.
    pub async fn delete(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query!(r#"
            DELETE FROM stash.dirs WHERE id = ANY($1)"#, ids
        ).execute(&mut **transaction).await?;
        Ok(())
    }

    /// Return a count of the number of dirs in the database.
    pub async fn count(transaction: &mut Transaction<'_, Postgres>) -> Result<i64> {
        let count: i64 = sqlx::query_scalar!("SELECT COUNT(id) FROM stash.dirs")
            .fetch_one(&mut **transaction).await?
            .unwrap();
        Ok(count)
    }
}

/// A new directory
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDir {
    /// Modification time
    pub mtime: DateTime<Utc>,
    /// Birth information
    pub birth: Birth,
}

impl NewDir {
    /// Create an entry for a directory in the database and return a `Dir`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<Dir> {
        let id = sqlx::query_scalar!(r#"
            INSERT INTO stash.dirs (mtime, birth_time, birth_version, birth_hostname)
            VALUES ($1, $2, $3, $4::text)
            RETURNING id"#,
            self.mtime, self.birth.time, self.birth.version, &self.birth.hostname
        ).fetch_one(&mut **transaction).await?;
        assert!(id >= 1);
        Ok(Dir {
            id,
            mtime: self.mtime,
            birth: self.birth,
        })
    }
}

/// A file
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct File {
    /// ID
    pub id: i64,
    /// Modification time
    pub mtime: DateTime<Utc>,
    /// Birth information
    pub birth: Birth,
    /// Size of the file in bytes
    pub size: i64,
    /// Whether the file is marked executable
    pub executable: bool,
    /// b3sum (BLAKE3 hash) for the full content of the file
    pub b3sum: Option<[u8; 32]>,
}

#[derive(Debug)]
struct FileRow {
    id: i64,
    mtime: DateTime<Utc>,
    birth_time: DateTime<Utc>,
    birth_version: i16,
    birth_hostname: String,
    size: i64,
    executable: bool,
    b3sum: Option<Vec<u8>>,
}

impl From<FileRow> for File {
    fn from(row: FileRow) -> Self {
        let mtime = row.mtime;
        util::assert_without_nanos(mtime);
        let b3sum = row.b3sum
            .map(|o| o.try_into().expect("b3sum from postgres wasn't 32 bytes?"));
        File {
            id: row.id,
            mtime,
            birth: Birth {
                time: row.birth_time,
                version: row.birth_version,
                hostname: row.birth_hostname,
            },
            size: row.size,
            executable: row.executable,
            b3sum,
        }
    }
}

impl File {
    /// Return a `Vec<File>` for the corresponding list of file `ids`.
    /// There is no error on missing files.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<Vec<File>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let files = sqlx::query_as!(FileRow, r#"
            SELECT id, mtime, size, executable, birth_time, birth_version, birth_hostname, b3sum
            FROM stash.files
            WHERE id = ANY($1)"#, ids
        )
            .fetch(&mut **transaction)
            .map(|result| result.map(|row| row.into()))
            .try_collect().await?;
        Ok(files)
    }

    /// Return a new, unique id for a file.  Caller can take this id and `create()` a `File` with it later.
    pub async fn next_id(transaction: &mut Transaction<'_, Postgres>) -> Result<i64> {
        db::nextval(transaction, "stash.files_id_seq").await
    }

    /// Set the b3sum for a file that may not have one already
    pub async fn set_b3sum(transaction: &mut Transaction<'_, Postgres>, file_id: i64, b3sum: &[u8; 32]) -> Result<()> {
        sqlx::query!(r#"
            UPDATE stash.files SET b3sum = $1 WHERE id = $2"#,
            b3sum.as_ref(), file_id
        ).execute(&mut **transaction).await?;
        Ok(())
    }

    /// Create an entry for a file in the database and return self.
    /// This is very similar to `NewFile::create` but creates a file with a specific `id`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        assert!(self.size >= 0, "size must be >= 0");
        sqlx::query!(r#"
            INSERT INTO stash.files (id, mtime, size, executable, birth_time, birth_version, birth_hostname, b3sum)
            OVERRIDING SYSTEM VALUE
            VALUES ($1, $2, $3, $4, $5, $6, $7::text, $8)"#,
            self.id, self.mtime, self.size, self.executable, self.birth.time,
            self.birth.version, &self.birth.hostname, self.b3sum.map(Vec::from)
        ).execute(&mut **transaction).await?;
        Ok(())
    }

    /// Delete files with given `ids`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn delete(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query!(r#"
            DELETE FROM stash.files WHERE id = ANY($1)"#, ids
        ).execute(&mut **transaction).await?;
        Ok(())
    }

    /// Return a count of the number of files in the database.
    pub async fn count(transaction: &mut Transaction<'_, Postgres>) -> Result<i64> {
        let count: i64 = sqlx::query_scalar!("SELECT COUNT(id) FROM stash.files")
            .fetch_one(&mut **transaction).await?
            .unwrap();
        Ok(count)
    }
}

/// A new file
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewFile {
    /// Modification time
    pub mtime: DateTime<Utc>,
    /// Birth information
    pub birth: Birth,
    /// Size of the file in bytes
    pub size: i64,
    /// Whether the file is marked executable
    pub executable: bool,
    /// b3sum (BLAKE3 hash) for the full content of the file
    pub b3sum: Option<[u8; 32]>,
}

impl NewFile {
    /// Create an entry for a file in the database and return a `File`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<File> {
        assert!(self.size >= 0, "size must be >= 0");
        let id = sqlx::query_scalar!(r#"
            INSERT INTO stash.files (mtime, size, executable, birth_time, birth_version, birth_hostname, b3sum)
            VALUES ($1, $2, $3, $4, $5, $6::text, $7)
            RETURNING id"#,
            self.mtime, self.size, self.executable, self.birth.time,
            self.birth.version, &self.birth.hostname, self.b3sum.map(Vec::from)
        ).fetch_one(&mut **transaction).await?;
        assert!(id >= 1);
        Ok(File {
            id,
            mtime: self.mtime,
            birth: self.birth,
            size: self.size,
            executable: self.executable,
            b3sum: self.b3sum,
        })
    }
}

/// A symbolic link
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Symlink {
    /// ID
    pub id: i64,
    /// Modification time
    pub mtime: DateTime<Utc>,
    /// Birth information
    pub birth: Birth,
    /// Target path
    pub target: String,
}

#[derive(Debug)]
struct SymlinkRow {
    id: i64,
    mtime: DateTime<Utc>,
    birth_time: DateTime<Utc>,
    birth_version: i16,
    birth_hostname: String,
    target: String,
}

impl From<SymlinkRow> for Symlink {
    fn from(row: SymlinkRow) -> Self {
        let mtime = row.mtime;
        util::assert_without_nanos(mtime);
        Symlink {
            id: row.id,
            mtime,
            birth: Birth {
                time: row.birth_time,
                version: row.birth_version,
                hostname: row.birth_hostname,
            },
            target: row.target,
        }
    }
}

impl Symlink {
    /// Return a `Vec<Symlink>` for the corresponding list of symlink `ids`.
    /// There is no error on missing symlinks.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<Vec<Symlink>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let symlinks = sqlx::query_as!(SymlinkRow, r#"
            SELECT id, mtime, target, birth_time, birth_version, birth_hostname
            FROM stash.symlinks
            WHERE id = ANY($1)"#, ids
        )
            .fetch(&mut **transaction)
            .map(|result| result.map(|row| row.into()))
            .try_collect().await?;
        Ok(symlinks)
    }

    /// Delete symlinks with given `ids`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn delete(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query!(r#"
            DELETE FROM stash.symlinks WHERE id = ANY($1)"#, ids
        ).execute(&mut **transaction).await?;
        Ok(())
    }

    /// Return a count of the number of symlinks in the database.
    pub async fn count(transaction: &mut Transaction<'_, Postgres>) -> Result<i64> {
        let count: i64 = sqlx::query_scalar!("SELECT COUNT(id) FROM stash.symlinks")
            .fetch_one(&mut **transaction).await?
            .unwrap();
        Ok(count)
    }
}

/// A new symbolic link
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSymlink {
    /// Modification time
    pub mtime: DateTime<Utc>,
    /// Birth information
    pub birth: Birth,
    /// Target path
    pub target: String,
}

impl NewSymlink {
    /// Create an entry for a symlink in the database and return a `Symlink`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<Symlink> {
        let id = sqlx::query_scalar!(r#"
            INSERT INTO stash.symlinks (mtime, target, birth_time, birth_version, birth_hostname)
            VALUES ($1, $2::text, $3, $4, $5::text)
            RETURNING id"#,
            self.mtime, self.target, self.birth.time, self.birth.version, self.birth.hostname
        ).fetch_one(&mut **transaction).await?;
        assert!(id >= 1);
        Ok(Symlink {
            id,
            mtime: self.mtime,
            birth: self.birth,
            target: self.target,
        })
    }
}

/// A dir, file, or symlink
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Inode {
    /// A directory
    Dir(Dir),
    /// A file
    File(File),
    /// A symbolic link
    Symlink(Symlink),
}

impl Inode {
    /// Return the directory for this inode, if it is one
    pub fn dir(&self) -> Result<&Dir> {
        match self {
            Inode::Dir(dir) => Ok(dir),
            _ => bail!("{:?} is not a dir", self),
        }
    }

    /// Return the file for this inode, if it is one
    pub fn file(&self) -> Result<&File> {
        match self {
            Inode::File(file) => Ok(file),
            _ => bail!("{:?} is not a file", self),
        }
    }

    /// Return the symlink for this inode, if it is one
    pub fn symlink(&self) -> Result<&Symlink> {
        match self {
            Inode::Symlink(symlink) => Ok(symlink),
            _ => bail!("{:?} is not a symlink", self),
        }
    }

    /// Return the mtime for this inode
    pub fn mtime(&self) -> DateTime<Utc> {
        match self {
            Inode::Dir(dir) => dir.mtime,
            Inode::File(file) => file.mtime,
            Inode::Symlink(symlink) => symlink.mtime,
        }
    }

    /// Return `Some(size)` for this inode if it is a file, otherwise `None`
    pub fn size(&self) -> Option<i64> {
        match self {
            Inode::File(file) => Some(file.size),
            _ => None,
        }
    }

    /// Return HashMaps of InodeId -> Inode for the corresponding list of `InodeId`.
    /// There is no error on missing inodes.
    pub async fn find_by_inode_ids(transaction: &mut Transaction<'_, Postgres>, inode_ids: &[InodeId]) -> Result<HashMap<InodeId, Inode>> {
        let mut dir_ids = vec![];
        let mut file_ids = vec![];
        let mut symlink_ids = vec![];
        for inode_id in inode_ids {
            match inode_id {
                InodeId::Dir(id)     => { dir_ids.push(*id); }
                InodeId::File(id)    => { file_ids.push(*id); }
                InodeId::Symlink(id) => { symlink_ids.push(*id); }
            }
        }
        let mut out = HashMap::new();
        for dir in Dir::find_by_ids(transaction, &dir_ids).await? {
            out.insert(InodeId::Dir(dir.id), Inode::Dir(dir));
        }
        for file in File::find_by_ids(transaction, &file_ids).await? {
            out.insert(InodeId::File(file.id), Inode::File(file));
        }
        for symlink in Symlink::find_by_ids(transaction, &symlink_ids).await? {
            out.insert(InodeId::Symlink(symlink.id), Inode::Symlink(symlink));
        }
        Ok(out)
    }
}

mod dummy {
    use super::*;
    use atomic_counter::{AtomicCounter, RelaxedCounter};
    use once_cell::sync::Lazy;

    /// Create a dummy file for use in tests.
    pub async fn create_dummy_file(transaction: &mut Transaction<'_, Postgres>) -> Result<File> {
        NewFile { executable: false, size: 0, mtime: Utc::now(), birth: Birth::here_and_now(), b3sum: None }.create(transaction).await
    }

    static BASENAME_COUNTER: Lazy<RelaxedCounter> = Lazy::new(|| {
        RelaxedCounter::new(1)
    });

    pub(crate) fn make_basename(prefix: &str) -> String {
        let num = BASENAME_COUNTER.inc();
        format!("{prefix}_{num}")
    }

    /// Create a dummy dir for use in tests.
    pub async fn create_dummy_dir(transaction: &mut Transaction<'_, Postgres>, basename_prefix: &str) -> Result<Dir> {
        let dir = NewDir { mtime: Utc::now(), birth: Birth::here_and_now() }.create(transaction).await?;
        Dirent::new(1, make_basename(basename_prefix), InodeId::Dir(dir.id)).create(transaction).await?;
        Ok(dir)
    }
}

pub use dummy::{create_dummy_file, create_dummy_dir};

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::tests::{new_primary_pool, new_secondary_pool};
    use serial_test::serial;

    mod api {
        use super::*;

        /// Dir::find_by_ids returns empty Vec when given no ids
        #[tokio::test]
        async fn test_dir_find_by_ids_empty() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let dirs = Dir::find_by_ids(&mut transaction, &[]).await?;
            assert_eq!(dirs, vec![]);
            Ok(())
        }

        /// Dir::find_by_ids returns Vec with `Dir`s for corresponding ids
        #[tokio::test]
        async fn test_dir_find_by_ids_nonempty() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let dir = NewDir { mtime: util::now_no_nanos(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            let nonexistent_id = 0;
            let dirs = Dir::find_by_ids(&mut transaction, &[dir.id, nonexistent_id]).await?;
            assert_eq!(dirs, vec![dir]);
            Ok(())
        }

        /// Dir::delete removes dirs with given `ids`.
        #[tokio::test]
        async fn test_dir_delete() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;

            let dir = NewDir { mtime: util::now_no_nanos(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            let dirs = Dir::find_by_ids(&mut transaction, &[dir.id]).await?;
            assert_eq!(dirs, vec![dir.clone()]);

            Dir::delete(&mut transaction, &[dir.id]).await?;
            let dirs = Dir::find_by_ids(&mut transaction, &[dir.id]).await?;
            assert_eq!(dirs, vec![]);

            Ok(())
        }

        /// Cannot create dir without it being a child_dir of something in dirents
        #[tokio::test]
        async fn test_cannot_create_dir_without_dirent() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            _ = NewDir { mtime: util::now_no_nanos(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            let result = transaction.commit().await;
            let msg = result.expect_err("expected an error").to_string();
            assert_eq!(msg, "error returned from database: insert or update on table \"dirs\" violates foreign key constraint \"dirs_id_fkey\"");
            Ok(())
        }

        /// File::find_by_ids returns empty Vec when given no ids
        #[tokio::test]
        async fn test_file_find_by_ids_empty() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let files = File::find_by_ids(&mut transaction, &[]).await?;
            assert_eq!(files, vec![]);
            Ok(())
        }

        /// File::find_by_ids returns Vec with `File`s for corresponding ids
        #[tokio::test]
        async fn test_file_find_by_ids_nonempty() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let file = NewFile { executable: false, size: 0, mtime: util::now_no_nanos(), birth: Birth::here_and_now(), b3sum: Some([1; 32]) }
                .create(&mut transaction).await?;
            let nonexistent_id = 0;
            let files = File::find_by_ids(&mut transaction, &[file.id, nonexistent_id]).await?;
            assert_eq!(files, vec![file]);
            Ok(())
        }

        /// Dir::delete removes dirs with given `ids`.
        #[tokio::test]
        async fn test_file_delete() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;

            let file = NewFile { executable: false, size: 0, mtime: util::now_no_nanos(), birth: Birth::here_and_now(), b3sum: None }
                .create(&mut transaction).await?;
            let files = File::find_by_ids(&mut transaction, &[file.id]).await?;
            assert_eq!(files, vec![file.clone()]);

            File::delete(&mut transaction, &[file.id]).await?;
            let files = File::find_by_ids(&mut transaction, &[file.id]).await?;
            assert_eq!(files, vec![]);

            Ok(())
        }

        /// Symlink::find_by_ids returns empty Vec when given no ids
        #[tokio::test]
        async fn test_symlink_find_by_ids_empty() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let files = Symlink::find_by_ids(&mut transaction, &[]).await?;
            assert_eq!(files, vec![]);
            Ok(())
        }

        /// Symlink::find_by_ids returns Vec with `Dir`s for corresponding ids
        #[tokio::test]
        async fn test_symlink_find_by_ids_nonempty() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let symlink = NewSymlink { target: "test".into(), mtime: util::now_no_nanos(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            let nonexistent_id = 0;
            let files = Symlink::find_by_ids(&mut transaction, &[symlink.id, nonexistent_id]).await?;
            assert_eq!(files, vec![symlink]);
            Ok(())
        }

        /// Dir::delete removes dirs with given `ids`.
        #[tokio::test]
        async fn test_symlink_delete() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;

            let symlink = NewSymlink { target: "test".into(), mtime: util::now_no_nanos(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            let symlinks = Symlink::find_by_ids(&mut transaction, &[symlink.id]).await?;
            assert_eq!(symlinks, vec![symlink.clone()]);

            Symlink::delete(&mut transaction, &[symlink.id]).await?;
            let symlinks = Symlink::find_by_ids(&mut transaction, &[symlink.id]).await?;
            assert_eq!(symlinks, vec![]);

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::assert_cannot_truncate;

        /// Cannot TRUNCATE dirs, files, or symlinks tables
        #[tokio::test]
        #[serial]
        async fn test_cannot_truncate() -> Result<()> {
            let pool = new_secondary_pool().await;
            for table in ["dirs", "files", "symlinks"] {
                let mut transaction = pool.begin().await?;
                assert_cannot_truncate(&mut transaction, &format!("stash.{table}")).await;
            }
            Ok(())
        }

        /// Can change mtime on a dir
        #[tokio::test]
        #[serial]
        async fn test_can_change_dir_mutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE stash.dirs SET mtime = now() WHERE id = $1").bind(1i64).execute(&mut *transaction).await?;
            transaction.commit().await?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a dir
        #[tokio::test]
        #[serial]
        async fn test_cannot_change_dir_immutables() -> Result<()> {
            let pool = new_primary_pool().await;
            for (column, value) in [("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE stash.dirs SET {column} = {value} WHERE id = $1");
                let result = sqlx::query(&query).bind(1i64).execute(&mut *transaction).await;
                let msg = result.expect_err("expected an error").to_string();
                if column == "id" {
                    assert_eq!(msg, "error returned from database: column \"id\" can only be updated to DEFAULT");
                } else {
                    assert_eq!(msg, "error returned from database: cannot change id or birth_*");
                }
            }
            Ok(())
        }

        /// Can change size, mtime, and executable on a file
        #[tokio::test]
        async fn test_can_change_file_mutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let file = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now(), b3sum: None }.create(&mut transaction).await?;
            transaction.commit().await?;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE stash.files SET mtime = now() WHERE id = $1").bind(file.id).execute(&mut *transaction).await?;
            transaction.commit().await?;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE stash.files SET size = 100000 WHERE id = $1").bind(file.id).execute(&mut *transaction).await?;
            transaction.commit().await?;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE stash.files SET executable = true WHERE id = $1").bind(file.id).execute(&mut *transaction).await?;
            transaction.commit().await?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a file
        #[tokio::test]
        #[serial]
        async fn test_cannot_change_file_immutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let file = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now(), b3sum: None }.create(&mut transaction).await?;
            transaction.commit().await?;
            for (column, value) in [("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE stash.files SET {column} = {value} WHERE id = $1");
                let result = sqlx::query(&query).bind(file.id).execute(&mut *transaction).await;
                let msg = result.expect_err("expected an error").to_string();
                if column == "id" {
                    assert_eq!(msg, "error returned from database: column \"id\" can only be updated to DEFAULT");
                } else {
                    assert_eq!(msg, "error returned from database: cannot change id or birth_*");
                }
            }
            Ok(())
        }

        /// Can change mtime on a symlink
        #[tokio::test]
        async fn test_can_change_symlink_mutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let symlink = NewSymlink { target: "old".into(), mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.commit().await?;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE stash.symlinks SET mtime = now() WHERE id = $1").bind(symlink.id).execute(&mut *transaction).await?;
            transaction.commit().await?;
            Ok(())
        }

        /// Cannot change id, symlink_target, birth_time, birth_version, or birth_hostname on a symlink
        #[tokio::test]
        #[serial]
        async fn test_cannot_change_symlink_immutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let symlink = NewSymlink { target: "old".into(), mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.commit().await?;
            for (column, value) in [("id", "100"), ("target", "'new'"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE stash.symlinks SET {column} = {value} WHERE id = $1");
                let result = sqlx::query(&query).bind(symlink.id).execute(&mut *transaction).await;
                let msg = result.expect_err("expected an error").to_string();
                if column == "id" {
                    assert_eq!(msg, "error returned from database: column \"id\" can only be updated to DEFAULT");
                } else {
                    assert_eq!(msg, "error returned from database: cannot change id, target, or birth_*");
                }
            }
            Ok(())
        }
    }
}
