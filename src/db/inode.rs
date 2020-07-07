//! CRUD operations for dir, file, and symlink entities in PostgreSQL

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use tokio_postgres::Transaction;
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

impl Dir {
    /// Return a `Vec<Dir>` for the corresponding list of dir `ids`.
    /// There is no error on missing dirs.
    pub async fn find_by_ids(transaction: &mut Transaction<'_>, ids: &[i64]) -> Result<Vec<Dir>> {
        let rows = transaction.query(
            "SELECT id, mtime, birth_time, birth_version, birth_hostname
             FROM dirs
             WHERE id = ANY($1::bigint[])",
            &[&ids]
        ).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(
                Dir {
                    id: row.get(0),
                    mtime: row.get(1),
                    birth: Birth {
                        time: row.get(2),
                        version: row.get(3),
                        hostname: row.get(4),
                    }
                }
            );
        }
        Ok(out)
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
    pub async fn create(self, transaction: &mut Transaction<'_>) -> Result<Dir> {
        let rows = transaction.query(
            "INSERT INTO dirs (mtime, birth_time, birth_version, birth_hostname)
             VALUES ($1::timestamptz, $2::timestamptz, $3::smallint, $4::text)
             RETURNING id", &[&self.mtime, &self.birth.time, &self.birth.version, &self.birth.hostname]
        ).await?;
        let id: i64 = rows[0].get(0);
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

impl File {
    /// Return a `Vec<File>` for the corresponding list of file `ids`.
    /// There is no error on missing files.
    pub async fn find_by_ids(transaction: &mut Transaction<'_>, ids: &[i64]) -> Result<Vec<File>> {
        let rows = transaction.query(
            "SELECT id, mtime, size, executable, birth_time, birth_version, birth_hostname
             FROM files
             WHERE id = ANY($1::bigint[])",
            &[&ids]
        ).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(
                File {
                    id: row.get(0),
                    mtime: row.get(1),
                    size: row.get(2),
                    executable: row.get(3),
                    birth: Birth {
                        time: row.get(4),
                        version: row.get(5),
                        hostname: row.get(6),
                    }
                }
            );
        }
        Ok(out)
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
    pub async fn create(self, transaction: &mut Transaction<'_>) -> Result<File> {
        assert!(self.size >= 0, "size must be >= 0");
        let rows = transaction.query(
            "INSERT INTO files (mtime, size, executable, birth_time, birth_version, birth_hostname)
             VALUES ($1::timestamptz, $2::bigint, $3::boolean, $4::timestamptz, $5::smallint, $6::text)
             RETURNING id", &[&self.mtime, &self.size, &self.executable, &self.birth.time, &self.birth.version, &self.birth.hostname]
        ).await?;
        let id: i64 = rows[0].get(0);
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

impl Symlink {
    /// Return a `Vec<Symlink>` for the corresponding list of symlink `ids`.
    /// There is no error on missing symlinks.
    pub async fn find_by_ids(transaction: &mut Transaction<'_>, ids: &[i64]) -> Result<Vec<Symlink>> {
        let rows = transaction.query(
            "SELECT id, mtime, target, birth_time, birth_version, birth_hostname
             FROM symlinks
             WHERE id = ANY($1::bigint[])",
            &[&ids]
        ).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(
                Symlink {
                    id: row.get(0),
                    mtime: row.get(1),
                    target: row.get(2),
                    birth: Birth {
                        time: row.get(3),
                        version: row.get(4),
                        hostname: row.get(5),
                    }
                }
            );
        }
        Ok(out)
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
    pub async fn create(self, transaction: &mut Transaction<'_>) -> Result<Symlink> {
        let rows = transaction.query(
            "INSERT INTO symlinks (mtime, target, birth_time, birth_version, birth_hostname)
             VALUES ($1::timestamptz, $2::text, $3::timestamptz, $4::smallint, $5::text)
             RETURNING id", &[&self.mtime, &self.target, &self.birth.time, &self.birth.version, &self.birth.hostname]
        ).await?;
        let id: i64 = rows[0].get(0);
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
    pub async fn find_by_inode_ids(transaction: &mut Transaction<'_>, inode_ids: &[InodeId]) -> Result<Vec<Inode>> {
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
    use crate::db::start_transaction;
    use crate::db::tests::get_client;

    pub(crate) async fn create_dummy_file(transaction: &mut Transaction<'_>) -> Result<File> {
        NewFile { executable: false, size: 0, mtime: Utc::now(), birth: Birth::here_and_now() }.create(transaction).await
    }

    mod api {
        use super::*;
        use crate::util;

        /// Dir::find_by_ids returns empty Vec when given no ids
        #[tokio::test]
        async fn test_dir_find_by_ids_empty() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
            let files = Dir::find_by_ids(&mut transaction, &[]).await?;
            assert_eq!(files, vec![]);
            Ok(())
        }

        /// Dir::find_by_ids returns Vec with `Dir`s for corresponding ids
        #[tokio::test]
        async fn test_dir_find_by_ids_nonempty() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
            let dir = NewDir { mtime: util::now_no_nanos(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            let nonexistent_id = 0;
            let files = Dir::find_by_ids(&mut transaction, &[dir.id, nonexistent_id]).await?;
            assert_eq!(files, vec![dir]);
            Ok(())
        }

        /// File::find_by_ids returns empty Vec when given no ids
        #[tokio::test]
        async fn test_file_find_by_ids_empty() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
            let files = File::find_by_ids(&mut transaction, &[]).await?;
            assert_eq!(files, vec![]);
            Ok(())
        }

        /// File::find_by_ids returns Vec with `File`s for corresponding ids
        #[tokio::test]
        async fn test_file_find_by_ids_nonempty() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
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
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
            let files = Symlink::find_by_ids(&mut transaction, &[]).await?;
            assert_eq!(files, vec![]);
            Ok(())
        }

        /// Symlink::find_by_ids returns Vec with `Dir`s for corresponding ids
        #[tokio::test]
        async fn test_symlink_find_by_ids_nonempty() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
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
        async fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client().await;
            for table in &["dirs", "files", "symlinks"] {
                let mut transaction = start_transaction(&mut client).await?;
                assert_cannot_truncate(&mut transaction, table).await;
            }
            Ok(())
        }

        /// Can change mtime on a dir
        #[tokio::test]
        async fn test_can_change_dir_mutables() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
            let dir = NewDir { mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.execute("UPDATE dirs SET mtime = now() WHERE id = $1::bigint", &[&dir.id]).await?;
            transaction.commit().await?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a dir
        #[tokio::test]
        async fn test_cannot_change_dir_immutables() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
            let dir = NewDir { mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.commit().await?;
            for (column, value) in &[("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")] {
                let transaction = start_transaction(&mut client).await?;
                let query = format!("UPDATE dirs SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&dir.id]).await;
                let msg = result.err().expect("expected an error").to_string();
                if *column == "id" {
                    assert_eq!(msg, "db error: ERROR: column \"id\" can only be updated to DEFAULT");
                } else {
                    assert_eq!(msg, "db error: ERROR: cannot change id or birth_*");
                }
            }
            Ok(())
        }

        /// Can change size, mtime, and executable on a file
        #[tokio::test]
        async fn test_can_change_file_mutables() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
            let file = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.commit().await?;
            let transaction = start_transaction(&mut client).await?;
            transaction.execute("UPDATE files SET mtime = now() WHERE id = $1::bigint", &[&file.id]).await?;
            transaction.commit().await?;
            let transaction = start_transaction(&mut client).await?;
            transaction.execute("UPDATE files SET size = 100000 WHERE id = $1::bigint", &[&file.id]).await?;
            transaction.commit().await?;
            let transaction = start_transaction(&mut client).await?;
            transaction.execute("UPDATE files SET executable = true WHERE id = $1::bigint", &[&file.id]).await?;
            transaction.commit().await?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a file
        #[tokio::test]
        async fn test_cannot_change_file_immutables() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
            let file = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.commit().await?;
            for (column, value) in &[("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")] {
                let transaction = start_transaction(&mut client).await?;
                let query = format!("UPDATE files SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&file.id]).await;
                let msg = result.err().expect("expected an error").to_string();
                if *column == "id" {
                    assert_eq!(msg, "db error: ERROR: column \"id\" can only be updated to DEFAULT");
                } else {
                    assert_eq!(msg, "db error: ERROR: cannot change id or birth_*");
                }
            }
            Ok(())
        }

        /// Can change mtime on a symlink
        #[tokio::test]
        async fn test_can_change_symlink_mutables() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
            let symlink = NewSymlink { target: "old".into(), mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.commit().await?;
            let transaction = start_transaction(&mut client).await?;
            transaction.execute("UPDATE symlinks SET mtime = now() WHERE id = $1::bigint", &[&symlink.id]).await?;
            transaction.commit().await?;
            Ok(())
        }

        /// Cannot change id, symlink_target, birth_time, birth_version, or birth_hostname on a symlink
        #[tokio::test]
        async fn test_cannot_change_symlink_immutables() -> Result<()> {
            let mut client = get_client().await;
            let mut transaction = start_transaction(&mut client).await?;
            let symlink = NewSymlink { target: "old".into(), mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction).await?;
            transaction.commit().await?;
            for (column, value) in &[("id", "100"), ("target", "'new'"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")] {
                let transaction = start_transaction(&mut client).await?;
                let query = format!("UPDATE symlinks SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&symlink.id]).await;
                let msg = result.err().expect("expected an error").to_string();
                if *column == "id" {
                    assert_eq!(msg, "db error: ERROR: column \"id\" can only be updated to DEFAULT");
                } else {
                    assert_eq!(msg, "db error: ERROR: cannot change id, target, or birth_*");
                }
            }
            Ok(())
        }
    }
}
