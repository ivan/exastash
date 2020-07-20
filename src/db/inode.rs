//! CRUD operations for dir, file, and symlink entities in PostgreSQL

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction, Row, postgres::PgRow};
use serde::Serialize;
use crate::EXASTASH_VERSION;
use crate::util;

/// A dir, file, or symlink
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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
        Ok(
            Dir {
                id: row.get("id"),
                mtime: row.get("mtime"),
                birth: Birth {
                    time: row.get("birth_time"),
                    version: row.get("birth_version"),
                    hostname: row.get("birth_hostname"),
                }
            }
        )
    }
}

impl Dir {
    /// Return a `Vec<Dir>` for the corresponding list of dir `ids`.
    /// There is no error on missing dirs.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<Vec<Dir>> {
        let query = "SELECT id, mtime, birth_time, birth_version, birth_hostname FROM dirs WHERE id = ANY($1::bigint[])";
        Ok(sqlx::query_as::<_, Dir>(query).bind(ids).fetch_all(transaction).await?)
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
        let query = "INSERT INTO dirs (mtime, birth_time, birth_version, birth_hostname)
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
}

impl<'c> sqlx::FromRow<'c, PgRow> for File {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(
            File {
                id: row.get("id"),
                mtime: row.get("mtime"),
                birth: Birth {
                    time: row.get("birth_time"),
                    version: row.get("birth_version"),
                    hostname: row.get("birth_hostname"),
                },
                size: row.get("size"),
                executable: row.get("executable"),
            }
        )
    }
}

impl File {
    /// Return a `Vec<File>` for the corresponding list of file `ids`.
    /// There is no error on missing files.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<Vec<File>> {
        let query = "SELECT id, mtime, size, executable, birth_time, birth_version, birth_hostname FROM files WHERE id = ANY($1::bigint[])";
        Ok(sqlx::query_as::<_, File>(query).bind(ids).fetch_all(transaction).await?)
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
}

impl NewFile {
    /// Create an entry for a file in the database and return a `File`.
    /// Does not commit the transaction, you must do so yourself.
    pub async fn create(self, transaction: &mut Transaction<'_, Postgres>) -> Result<File> {
        assert!(self.size >= 0, "size must be >= 0");
        let query = "INSERT INTO files (mtime, size, executable, birth_time, birth_version, birth_hostname)
                     VALUES ($1::timestamptz, $2::bigint, $3::boolean, $4::timestamptz, $5::smallint, $6::text)
                     RETURNING id";
        let row = sqlx::query(query)
            .bind(self.mtime)
            .bind(self.size)
            .bind(self.executable)
            .bind(self.birth.time)
            .bind(self.birth.version)
            .bind(&self.birth.hostname)
            .fetch_one(transaction).await?;
        let id: i64 = row.get(0);
        assert!(id >= 1);
        Ok(File {
            id,
            mtime: self.mtime,
            birth: self.birth,
            size: self.size,
            executable: self.executable,
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
        Ok(
            Symlink {
                id: row.get("id"),
                mtime: row.get("mtime"),
                birth: Birth {
                    time: row.get("birth_time"),
                    version: row.get("birth_version"),
                    hostname: row.get("birth_hostname"),
                },
                target: row.get("target"),
            }
        )
    }
}

impl Symlink {
    /// Return a `Vec<Symlink>` for the corresponding list of symlink `ids`.
    /// There is no error on missing symlinks.
    pub async fn find_by_ids(transaction: &mut Transaction<'_, Postgres>, ids: &[i64]) -> Result<Vec<Symlink>> {
        let query = "SELECT id, mtime, target, birth_time, birth_version, birth_hostname FROM symlinks WHERE id = ANY($1::bigint[])";
        Ok(sqlx::query_as::<_, Symlink>(query).bind(ids).fetch_all(transaction).await?)
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
        let query = "INSERT INTO symlinks (mtime, target, birth_time, birth_version, birth_hostname)
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
    /// Return a `Vec<Inode>` for the corresponding list of `InodeId`.
    /// There is no error on missing inodes.
    pub async fn find_by_inode_ids(transaction: &mut Transaction<'_, Postgres>, inode_ids: &[InodeId]) -> Result<Vec<Inode>> {
        let mut out = Vec::with_capacity(inode_ids.len());

        let dir_ids:     Vec<i64> = inode_ids.iter().filter_map(|inode_id| if let InodeId::Dir(id)     = inode_id { Some(*id) } else { None } ).collect();
        let file_ids:    Vec<i64> = inode_ids.iter().filter_map(|inode_id| if let InodeId::File(id)    = inode_id { Some(*id) } else { None } ).collect();
        let symlink_ids: Vec<i64> = inode_ids.iter().filter_map(|inode_id| if let InodeId::Symlink(id) = inode_id { Some(*id) } else { None } ).collect();

        // TODO: run these in parallel
        out.extend(Dir::find_by_ids(transaction, &dir_ids).await?.into_iter().map(Inode::Dir));
        out.extend(File::find_by_ids(transaction, &file_ids).await?.into_iter().map(Inode::File));
        out.extend(Symlink::find_by_ids(transaction, &symlink_ids).await?.into_iter().map(Inode::Symlink));

        Ok(out)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::tests::{new_primary_pool, new_secondary_pool};
    use serial_test::serial;

    pub(crate) async fn create_dummy_file(transaction: &mut Transaction<'_, Postgres>) -> Result<File> {
        NewFile { executable: false, size: 0, mtime: Utc::now(), birth: Birth::here_and_now() }.create(transaction).await
    }

    mod api {
        use super::*;
        use crate::util;

        /// Dir::find_by_ids returns empty Vec when given no ids
        #[tokio::test]
        async fn test_dir_find_by_ids_empty() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let files = Dir::find_by_ids(&mut transaction, &[]).await?;
            assert_eq!(files, vec![]);
            Ok(())
        }

        /// Dir::find_by_ids returns Vec with `Dir`s for corresponding ids
        #[tokio::test]
        async fn test_dir_find_by_ids_nonempty() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let dir = NewDir { mtime: util::now_no_nanos(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            let nonexistent_id = 0;
            let files = Dir::find_by_ids(&mut transaction, &[dir.id, nonexistent_id]).await?;
            assert_eq!(files, vec![dir]);
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
            let file = NewFile { executable: false, size: 0, mtime: util::now_no_nanos(), birth: Birth::here_and_now() }
                .create(&mut transaction).await?;
            let nonexistent_id = 0;
            let files = File::find_by_ids(&mut transaction, &[file.id, nonexistent_id]).await?;
            assert_eq!(files, vec![file]);
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
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::tests::assert_cannot_truncate;

        /// Cannot TRUNCATE dirs, files, or symlinks tables
        #[tokio::test]
        #[serial]
        async fn test_cannot_truncate() -> Result<()> {
            let pool = new_secondary_pool().await;
            for table in &["dirs", "files", "symlinks"] {
                let mut transaction = pool.begin().await?;
                assert_cannot_truncate(&mut transaction, table).await;
            }
            Ok(())
        }

        /// Can change mtime on a dir
        #[tokio::test]
        async fn test_can_change_dir_mutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE dirs SET mtime = now() WHERE id = $1::bigint").bind(&1i64).execute(&mut transaction).await?;
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
                let query = format!("UPDATE dirs SET {column} = {value} WHERE id = $1::bigint");
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
            let file = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.commit().await?;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE files SET mtime = now() WHERE id = $1::bigint").bind(&file.id).execute(&mut transaction).await?;
            transaction.commit().await?;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE files SET size = 100000 WHERE id = $1::bigint").bind(&file.id).execute(&mut transaction).await?;
            transaction.commit().await?;
            let mut transaction = pool.begin().await?;
            sqlx::query("UPDATE files SET executable = true WHERE id = $1::bigint").bind(&file.id).execute(&mut transaction).await?;
            transaction.commit().await?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a file
        #[tokio::test]
        async fn test_cannot_change_file_immutables() -> Result<()> {
            let pool = new_primary_pool().await;
            let mut transaction = pool.begin().await?;
            let file = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.commit().await?;
            for (column, value) in &[("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")] {
                let mut transaction = pool.begin().await?;
                let query = format!("UPDATE files SET {column} = {value} WHERE id = $1::bigint");
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
            sqlx::query("UPDATE symlinks SET mtime = now() WHERE id = $1::bigint").bind(&symlink.id).execute(&mut transaction).await?;
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
                let query = format!("UPDATE symlinks SET {column} = {value} WHERE id = $1::bigint");
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
