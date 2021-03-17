//! CRUD operations for dir, file, and symlink entities in PostgreSQL

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction, Row, postgres::PgRow};
use serde::Serialize;
use std::collections::HashMap;
use std::convert::TryInto;
use crate::EXASTASH_VERSION;
use crate::db;
use crate::util;

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

impl<'c> sqlx::FromRow<'c, PgRow> for Dir {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let mtime = row.get("mtime");
        util::assert_without_nanos(mtime);
        Ok(Dir {
            id: row.get("id"),
            mtime,
            birth: Birth {
                time: row.get("birth_time"),
                version: row.get("birth_version"),
                hostname: row.get("birth_hostname"),
            }
        })
    }
}

impl Dir {
    /// Return a `Vec<Dir>` for the corresponding list of dir `ids`.
    /// There is no error on missing dirs.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<Vec<Dir>> {
        if ids.is_empty() {
            return Ok(vec![])
        }
        let query = "SELECT id, mtime, birth_time, birth_version, birth_hostname FROM stash.dirs WHERE id = ANY($1::bigint[])";
        Ok(sqlx::query_as::<_, Dir>(query).bind(ids).fetch_all(transaction).await?)
    }

    /// Remove dirs with given `ids`.
    ///
    /// Note that that foreign key constraints in the database require removing
    /// the associated dirents first (where `child_dir` is one of the `ids`).
    ///
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<()> {
        let stmt = "DELETE FROM stash.dirs WHERE id = ANY($1::bigint[])";
        sqlx::query(stmt)
            .bind(ids)
            .execute(transaction).await?;
        Ok(())
    }

    /// Return a count of the number of dirs in the database.
    pub async fn count(transaction: &mut Transaction<'_, Postgres>) -> Result<i64> {
        let count: i64 = sqlx::query("SELECT COUNT(id) FROM stash.dirs")
            .fetch_one(transaction).await?
            .get(0);
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
        let query = "INSERT INTO stash.dirs (mtime, birth_time, birth_version, birth_hostname)
                     VALUES ($1::timestamptz, $2::timestamptz, $3::smallint, $4::text)
                     RETURNING id";
        let row = sqlx::query(query)
            .bind(self.mtime)
            .bind(self.birth.time)
            .bind(self.birth.version)
            .bind(&self.birth.hostname)
            .fetch_one(transaction).await?;
        let id: i64 = row.get(0);
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

impl<'c> sqlx::FromRow<'c, PgRow> for File {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let mtime = row.get("mtime");
        util::assert_without_nanos(mtime);
        let b3sum = row.get::<Option<Vec<u8>>, _>("b3sum")
            .map(|o| o.try_into().expect("b3sum from postgres wasn't 32 bytes?"));
        Ok(File {
            id: row.get("id"),
            mtime,
            birth: Birth {
                time: row.get("birth_time"),
                version: row.get("birth_version"),
                hostname: row.get("birth_hostname"),
            },
            size: row.get("size"),
            executable: row.get("executable"),
            b3sum,
        })
    }
}

impl File {
    /// Return a `Vec<File>` for the corresponding list of file `ids`.
    /// There is no error on missing files.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<Vec<File>> {
        if ids.is_empty() {
            return Ok(vec![])
        }
        let query = "SELECT id, mtime, size, executable, birth_time, birth_version, birth_hostname, b3sum FROM stash.files WHERE id = ANY($1::bigint[])";
        Ok(sqlx::query_as::<_, File>(query).bind(ids).fetch_all(transaction).await?)
    }

    /// Return a new, unique id for a file.  Caller can take this id and `create()` a `File` with it later.
    pub async fn next_id(transaction: &mut Transaction<'_, Postgres>) -> Result<i64> {
        db::nextval(transaction, "stash.files_id_seq").await
    }

    /// Set the b3sum for a file that may not have one already
    pub async fn set_b3sum(transaction: &mut Transaction<'_, Postgres>, file_id: i64, b3sum: &[u8; 32]) -> Result<()> {
        let query = "UPDATE stash.files SET b3sum = $1::bytea WHERE id = $2::bigint";
        sqlx::query(query)
            .bind(b3sum.as_ref())
            .bind(file_id)
            .execute(transaction).await?;
        Ok(())
    }

    /// Create an entry for a file in the database and return self.
    /// This is very similar to `NewFile::create` but creates a file with a specific `id`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(&self, transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
        assert!(self.size >= 0, "size must be >= 0");
        let query = "INSERT INTO stash.files (id, mtime, size, executable, birth_time, birth_version, birth_hostname, b3sum)
                     OVERRIDING SYSTEM VALUE
                     VALUES ($1::bigint, $2::timestamptz, $3::bigint, $4::boolean, $5::timestamptz, $6::smallint, $7::text, $8::bytea)";
        sqlx::query(query)
            .bind(self.id)
            .bind(self.mtime)
            .bind(self.size)
            .bind(self.executable)
            .bind(self.birth.time)
            .bind(self.birth.version)
            .bind(&self.birth.hostname)
            .bind(self.b3sum.map(Vec::from))
            .execute(transaction).await?;
        Ok(())
    }

    /// Remove files with given `ids`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<()> {
        let stmt = "DELETE FROM stash.files WHERE id = ANY($1::bigint[])";
        sqlx::query(stmt)
            .bind(ids)
            .execute(transaction).await?;
        Ok(())
    }

    /// Return a count of the number of files in the database.
    pub async fn count(transaction: &mut Transaction<'_, Postgres>) -> Result<i64> {
        let count: i64 = sqlx::query("SELECT COUNT(id) FROM stash.files")
            .fetch_one(transaction).await?
            .get(0);
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
        let query = "INSERT INTO stash.files (mtime, size, executable, birth_time, birth_version, birth_hostname, b3sum)
                     VALUES ($1::timestamptz, $2::bigint, $3::boolean, $4::timestamptz, $5::smallint, $6::text, $7::bytea)
                     RETURNING id";
        let row = sqlx::query(query)
            .bind(self.mtime)
            .bind(self.size)
            .bind(self.executable)
            .bind(self.birth.time)
            .bind(self.birth.version)
            .bind(&self.birth.hostname)
            .bind(self.b3sum.map(Vec::from))
            .fetch_one(transaction).await?;
        let id: i64 = row.get(0);
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

impl<'c> sqlx::FromRow<'c, PgRow> for Symlink {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let mtime = row.get("mtime");
        util::assert_without_nanos(mtime);
        Ok(Symlink {
            id: row.get("id"),
            mtime,
            birth: Birth {
                time: row.get("birth_time"),
                version: row.get("birth_version"),
                hostname: row.get("birth_hostname"),
            },
            target: row.get("target"),
        })
    }
}

impl Symlink {
    /// Return a `Vec<Symlink>` for the corresponding list of symlink `ids`.
    /// There is no error on missing symlinks.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<Vec<Symlink>> {
        if ids.is_empty() {
            return Ok(vec![])
        }
        let query = "SELECT id, mtime, target, birth_time, birth_version, birth_hostname FROM stash.symlinks WHERE id = ANY($1::bigint[])";
        Ok(sqlx::query_as::<_, Symlink>(query).bind(ids).fetch_all(transaction).await?)
    }

    /// Remove symlinks with given `ids`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn remove(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<()> {
        let stmt = "DELETE FROM stash.symlinks WHERE id = ANY($1::bigint[])";
        sqlx::query(stmt)
            .bind(ids)
            .execute(transaction).await?;
        Ok(())
    }

    /// Return a count of the number of symlinks in the database.
    pub async fn count(transaction: &mut Transaction<'_, Postgres>) -> Result<i64> {
        let count: i64 = sqlx::query("SELECT COUNT(id) FROM stash.symlinks")
            .fetch_one(transaction).await?
            .get(0);
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
        let query = "INSERT INTO stash.symlinks (mtime, target, birth_time, birth_version, birth_hostname)
                     VALUES ($1::timestamptz, $2::text, $3::timestamptz, $4::smallint, $5::text)
                     RETURNING id";
        let row = sqlx::query(query)
            .bind(self.mtime)
            .bind(&self.target)
            .bind(self.birth.time)
            .bind(self.birth.version)
            .bind(&self.birth.hostname)
            .fetch_one(transaction).await?;
        let id: i64 = row.get(0);
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

/// Create a dummy file for use in tests.
pub async fn create_dummy_file(transaction: &mut Transaction<'_, Postgres>) -> Result<File> {
    NewFile { executable: false, size: 0, mtime: Utc::now(), birth: Birth::here_and_now(), b3sum: None }.create(transaction).await
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::tests::{new_primary_pool, new_secondary_pool};
    use serial_test::serial;

    mod api {
        use super::*;
        use crate::util;

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

        /// Dir::remove removes dirs with given `ids`.
        #[tokio::test]
        async fn test_dir_remove() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;

            let dir = NewDir { mtime: util::now_no_nanos(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            let dirs = Dir::find_by_ids(&mut transaction, &[dir.id]).await?;
            assert_eq!(dirs, vec![dir.clone()]);

            Dir::remove(&mut transaction, &[dir.id]).await?;
            let dirs = Dir::find_by_ids(&mut transaction, &[dir.id]).await?;
            assert_eq!(dirs, vec![]);

            Ok(())
        }

        /// Cannot create dir without it being a child_dir of something in dirents
        #[tokio::test]
        async fn test_cannot_create_dir_without_dirent() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let _ = NewDir { mtime: util::now_no_nanos(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            let result = transaction.commit().await;
            let msg = result.err().expect("expected an error").to_string();
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

        /// Dir::remove removes dirs with given `ids`.
        #[tokio::test]
        async fn test_file_remove() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;

            let file = NewFile { executable: false, size: 0, mtime: util::now_no_nanos(), birth: Birth::here_and_now(), b3sum: None }
                .create(&mut transaction).await?;
            let files = File::find_by_ids(&mut transaction, &[file.id]).await?;
            assert_eq!(files, vec![file.clone()]);

            File::remove(&mut transaction, &[file.id]).await?;
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

        /// Dir::remove removes dirs with given `ids`.
        #[tokio::test]
        async fn test_symlink_remove() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;

            let symlink = NewSymlink { target: "test".into(), mtime: util::now_no_nanos(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            let symlinks = Symlink::find_by_ids(&mut transaction, &[symlink.id]).await?;
            assert_eq!(symlinks, vec![symlink.clone()]);

            Symlink::remove(&mut transaction, &[symlink.id]).await?;
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
            for table in &["dirs", "files", "symlinks"] {
                let mut transaction = pool.begin().await?;
                assert_cannot_truncate(&mut transaction, &format!("stash.{table}")).await;
            }
            Ok(())
        }

        /// Can change mtime on a dir
        #[tokio::test]
        async fn test_can_change_dir_mutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE stash.dirs SET mtime = now() WHERE id = $1::bigint").bind(&1i64).execute(&mut transaction).await?;
            transaction.commit().await?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a dir
        #[tokio::test]
        async fn test_cannot_change_dir_immutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let transaction = pool.begin().await?;
            transaction.commit().await?;
            for (column, value) in &[("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE stash.dirs SET {column} = {value} WHERE id = $1::bigint");
                let result = sqlx::query(&query).bind(&1i64).execute(&mut transaction).await;
                let msg = result.err().expect("expected an error").to_string();
                if *column == "id" {
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
            sqlx::query("UPDATE stash.files SET mtime = now() WHERE id = $1::bigint").bind(&file.id).execute(&mut transaction).await?;
            transaction.commit().await?;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE stash.files SET size = 100000 WHERE id = $1::bigint").bind(&file.id).execute(&mut transaction).await?;
            transaction.commit().await?;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE stash.files SET executable = true WHERE id = $1::bigint").bind(&file.id).execute(&mut transaction).await?;
            transaction.commit().await?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a file
        #[tokio::test]
        async fn test_cannot_change_file_immutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let file = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now(), b3sum: None }.create(&mut transaction).await?;
            transaction.commit().await?;
            for (column, value) in &[("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE stash.files SET {column} = {value} WHERE id = $1::bigint");
                let result = sqlx::query(&query).bind(&file.id).execute(&mut transaction).await;
                let msg = result.err().expect("expected an error").to_string();
                if *column == "id" {
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
            sqlx::query("UPDATE stash.symlinks SET mtime = now() WHERE id = $1::bigint").bind(&symlink.id).execute(&mut transaction).await?;
            transaction.commit().await?;
            Ok(())
        }

        /// Cannot change id, symlink_target, birth_time, birth_version, or birth_hostname on a symlink
        #[tokio::test]
        async fn test_cannot_change_symlink_immutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let symlink = NewSymlink { target: "old".into(), mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.commit().await?;
            for (column, value) in &[("id", "100"), ("target", "'new'"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE stash.symlinks SET {column} = {value} WHERE id = $1::bigint");
                let result = sqlx::query(&query).bind(&symlink.id).execute(&mut transaction).await;
                let msg = result.err().expect("expected an error").to_string();
                if *column == "id" {
                    assert_eq!(msg, "error returned from database: column \"id\" can only be updated to DEFAULT");
                } else {
                    assert_eq!(msg, "error returned from database: cannot change id, target, or birth_*");
                }
            }
            Ok(())
        }
    }
}
