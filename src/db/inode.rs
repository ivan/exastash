//! CRUD operations for dir, file, and symlink entities in PostgreSQL

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use postgres::Transaction;
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dir {
    /// ID
    pub id: i64,
    /// Modification time
    pub mtime: DateTime<Utc>,
    /// Birth information
    pub birth: Birth,
}

impl Dir {
    fn find_by_ids(transaction: &mut Transaction<'_>, ids: &[i64]) -> Result<Vec<Dir>> {
        let rows = transaction.query(
            "SELECT id, mtime, birth_time, birth_version, birth_hostname
             FROM dirs
             WHERE id = ANY($1::bigint[])",
            &[&ids]
        )?;
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDir {
    /// Modification time
    pub mtime: DateTime<Utc>,
    /// Birth information
    pub birth: Birth,
}

impl NewDir {
    /// Create an entry for a directory in the database and return its id.
    /// Does not commit the transaction, you must do so yourself.
    pub fn create(&self, transaction: &mut Transaction<'_>) -> Result<i64> {
        let rows = transaction.query(
            "INSERT INTO dirs (mtime, birth_time, birth_version, birth_hostname)
             VALUES ($1::timestamptz, $2::timestamptz, $3::smallint, $4::text)
             RETURNING id", &[&self.mtime, &self.birth.time, &self.birth.version, &self.birth.hostname]
        )?;
        let id: i64 = rows[0].get(0);
        assert!(id >= 1);
        Ok(id)
    }

    /// Return a `Dir` based on this `NewDir` with the given `id`
    pub fn to_dir(&self, id: i64) -> Dir {
        Dir {
            id,
            mtime: self.mtime,
            birth: self.birth.clone(),
        }
    }
}

/// A file
#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn find_by_ids(transaction: &mut Transaction<'_>, ids: &[i64]) -> Result<Vec<File>> {
        let rows = transaction.query(
            "SELECT id, mtime, size, executable, birth_time, birth_version, birth_hostname
             FROM files
             WHERE id = ANY($1::bigint[])",
            &[&ids]
        )?;
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
    /// Create an entry for a file in the database and return its id.
    /// Does not commit the transaction, you must do so yourself.
    pub fn create(&self, transaction: &mut Transaction<'_>) -> Result<i64> {
        assert!(self.size >= 0, "size must be >= 0");
        let rows = transaction.query(
            "INSERT INTO files (mtime, size, executable, birth_time, birth_version, birth_hostname)
             VALUES ($1::timestamptz, $2::bigint, $3::boolean, $4::timestamptz, $5::smallint, $6::text)
             RETURNING id", &[&self.mtime, &self.size, &self.executable, &self.birth.time, &self.birth.version, &self.birth.hostname]
        )?;
        let id: i64 = rows[0].get(0);
        assert!(id >= 1);
        Ok(id)
    }

    /// Return a `File` based on this `NewFile` with the given `id`
    pub fn to_file(&self, id: i64) -> File {
        File {
            id,
            mtime: self.mtime,
            birth: self.birth.clone(),
            size: self.size,
            executable: self.executable,
        }
    }
}

/// A symbolic link
#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn find_by_ids(transaction: &mut Transaction<'_>, ids: &[i64]) -> Result<Vec<Symlink>> {
        let rows = transaction.query(
            "SELECT id, mtime, target, birth_time, birth_version, birth_hostname
             FROM symlinks
             WHERE id = ANY($1::bigint[])",
            &[&ids]
        )?;
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
    /// Create an entry for a symlink in the database and return its id.
    /// Does not commit the transaction, you must do so yourself.
    pub fn create(&self, transaction: &mut Transaction<'_>) -> Result<i64> {
        let rows = transaction.query(
            "INSERT INTO symlinks (mtime, target, birth_time, birth_version, birth_hostname)
             VALUES ($1::timestamptz, $2::text, $3::timestamptz, $4::smallint, $5::text)
             RETURNING id", &[&self.mtime, &self.target, &self.birth.time, &self.birth.version, &self.birth.hostname]
        )?;
        let id: i64 = rows[0].get(0);
        assert!(id >= 1);
        Ok(id)
    }

    /// Return a `Symlink` based on this `NewSymlink` with the given `id`
    pub fn to_symlink(&self, id: i64) -> Symlink {
        Symlink {
            id,
            mtime: self.mtime,
            birth: self.birth.clone(),
            target: self.target.clone(),
        }
    }
}

/// A dir, file, or symlink
#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub fn find_by_inode_ids(transaction: &mut Transaction<'_>, inode_ids: &[InodeId]) -> Result<Vec<Inode>> {
        let mut out = Vec::with_capacity(inode_ids.len());

        let dir_ids:     Vec<i64> = inode_ids.iter().filter_map(|inode_id| if let InodeId::Dir(id)     = inode_id { Some(*id) } else { None } ).collect();
        let file_ids:    Vec<i64> = inode_ids.iter().filter_map(|inode_id| if let InodeId::File(id)    = inode_id { Some(*id) } else { None } ).collect();
        let symlink_ids: Vec<i64> = inode_ids.iter().filter_map(|inode_id| if let InodeId::Symlink(id) = inode_id { Some(*id) } else { None } ).collect();

        // TODO: run these in parallel
        out.extend(Dir::find_by_ids(transaction, &dir_ids)?.into_iter().map(Inode::Dir));
        out.extend(File::find_by_ids(transaction, &file_ids)?.into_iter().map(Inode::File));
        out.extend(Symlink::find_by_ids(transaction, &symlink_ids)?.into_iter().map(Inode::Symlink));

        Ok(out)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;

    pub(crate) fn create_dummy_file(transaction: &mut Transaction<'_>) -> Result<i64> {
        let file = NewFile { executable: false, size: 0, mtime: Utc::now(), birth: Birth::here_and_now() };
        file.create(transaction)
    }

    mod api {
        use super::*;
        use crate::util;

        /// Dir::find_by_ids returns empty Vec when given no ids
        #[test]
        fn test_dir_find_by_ids_empty() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let files = Dir::find_by_ids(&mut transaction, &[])?;
            assert_eq!(files, vec![]);
            Ok(())
        }

        /// Dir::find_by_ids returns Vec with `Dir`s for corresponding ids
        #[test]
        fn test_dir_find_by_ids_nonempty() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let new = NewDir { mtime: util::now_no_nanos(), birth: Birth::here_and_now() };
            let id = new.create(&mut transaction)?;
            let nonexistent_id = 0;
            let files = Dir::find_by_ids(&mut transaction, &[id, nonexistent_id])?;
            assert_eq!(files, vec![
                new.to_dir(id)
            ]);
            Ok(())
        }

        /// File::find_by_ids returns empty Vec when given no ids
        #[test]
        fn test_file_find_by_ids_empty() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let files = File::find_by_ids(&mut transaction, &[])?;
            assert_eq!(files, vec![]);
            Ok(())
        }

        /// File::find_by_ids returns Vec with `File`s for corresponding ids
        #[test]
        fn test_file_find_by_ids_nonempty() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let new = NewFile { executable: false, size: 0, mtime: util::now_no_nanos(), birth: Birth::here_and_now() };
            let id = new.create(&mut transaction)?;
            let nonexistent_id = 0;
            let files = File::find_by_ids(&mut transaction, &[id, nonexistent_id])?;
            assert_eq!(files, vec![
                new.to_file(id)
            ]);
            Ok(())
        }

        /// Symlink::find_by_ids returns empty Vec when given no ids
        #[test]
        fn test_symlink_find_by_ids_empty() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let files = Symlink::find_by_ids(&mut transaction, &[])?;
            assert_eq!(files, vec![]);
            Ok(())
        }

        /// Symlink::find_by_ids returns Vec with `Dir`s for corresponding ids
        #[test]
        fn test_symlink_find_by_ids_nonempty() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let new = NewSymlink { target: "test".into(), mtime: util::now_no_nanos(), birth: Birth::here_and_now() };
            let id = new.create(&mut transaction)?;
            let nonexistent_id = 0;
            let files = Symlink::find_by_ids(&mut transaction, &[id, nonexistent_id])?;
            assert_eq!(files, vec![
                new.to_symlink(id)
            ]);
            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::tests::assert_cannot_truncate;

        /// Cannot TRUNCATE dirs, files, or symlinks tables
        #[test]
        fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client();
            for table in ["dirs", "files", "symlinks"].iter() {
                let mut transaction = start_transaction(&mut client)?;
                assert_cannot_truncate(&mut transaction, table)?;
            }
            Ok(())
        }

        /// Can change mtime on a dir
        #[test]
        fn test_can_change_dir_mutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let dir_id = NewDir { mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction)?;
            transaction.execute("UPDATE dirs SET mtime = now() WHERE id = $1::bigint", &[&dir_id])?;
            transaction.commit()?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a dir
        #[test]
        fn test_cannot_change_dir_immutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let dir_id = NewDir { mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction)?;
            transaction.commit()?;
            for (column, value) in [("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE dirs SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&dir_id]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id or birth_*");
            }
            Ok(())
        }

        /// Can change size, mtime, and executable on a file
        #[test]
        fn test_can_change_file_mutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let file_id = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction)?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET mtime = now() WHERE id = $1::bigint", &[&file_id])?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET size = 100000 WHERE id = $1::bigint", &[&file_id])?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET executable = true WHERE id = $1::bigint", &[&file_id])?;
            transaction.commit()?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a file
        #[test]
        fn test_cannot_change_file_immutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let file_id = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction)?;
            transaction.commit()?;
            for (column, value) in [("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE files SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&file_id]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id or birth_*");
            }
            Ok(())
        }

        /// Can change mtime on a symlink
        #[test]
        fn test_can_change_symlink_mutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let symlink_id = NewSymlink { target: "old".into(), mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction)?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE symlinks SET mtime = now() WHERE id = $1::bigint", &[&symlink_id])?;
            transaction.commit()?;
            Ok(())
        }

        /// Cannot change id, symlink_target, birth_time, birth_version, or birth_hostname on a symlink
        #[test]
        fn test_cannot_change_symlink_immutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let symlink_id = NewSymlink { target: "old".into(), mtime: Utc::now(), birth: Birth::here_and_now() }.create(&mut transaction)?;
            transaction.commit()?;
            for (column, value) in [("id", "100"), ("target", "'new'"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE symlinks SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&symlink_id]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id, target, or birth_*");
            }
            Ok(())
        }
    }
}
