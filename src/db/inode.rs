//! CRUD operations for dir, file, and symlink entities in PostgreSQL

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use postgres::Transaction;
use crate::EXASTASH_VERSION;
use crate::util;

/// A dir, file, or symlink
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Inode {
    /// A directory
    Dir(i64),
    /// A regular file
    File(i64),
    /// A symbolic link
    Symlink(i64),
}

impl Inode {
    /// Returns the directory id for this inode, if it is one
    pub fn dir_id(self) -> Result<i64> {
        match self {
            Inode::Dir(id) => Ok(id),
            _ => bail!("{:?} is not a dir", self),
        }
    }

    /// Returns the file id for this inode, if it is one
    pub fn file_id(self) -> Result<i64> {
        match self {
            Inode::File(id) => Ok(id),
            _ => bail!("{:?} is not a file", self),
        }
    }

    /// Returns the symlink id for this inode, if it is one
    pub fn symlink_id(self) -> Result<i64> {
        match self {
            Inode::Symlink(id) => Ok(id),
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
    /// Returns a `Birth` with time set to now, version set to the current exastash version,
    /// and hostname set to the machine's hostname.
    pub fn here_and_now() -> Birth {
        Birth { time: Utc::now(), version: EXASTASH_VERSION, hostname: util::get_hostname() }
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
    pub fn create(&self, transaction: &mut Transaction<'_>) -> Result<Inode> {
        let rows = transaction.query(
            "INSERT INTO dirs (mtime, birth_time, birth_version, birth_hostname)
            VALUES ($1::timestamptz, $2::timestamptz, $3::smallint, $4::text)
            RETURNING id", &[&self.mtime, &self.birth.time, &self.birth.version, &self.birth.hostname]
        )?;
        let id: i64 = rows[0].get(0);
        assert!(id >= 1);
        Ok(Inode::Dir(id))
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
    pub fn create(&self, transaction: &mut Transaction<'_>) -> Result<Inode> {
        assert!(self.size >= 0, "size must be >= 0");
        let rows = transaction.query(
            "INSERT INTO files (mtime, size, executable, birth_time, birth_version, birth_hostname)
            VALUES ($1::timestamptz, $2::bigint, $3::boolean, $4::timestamptz, $5::smallint, $6::text)
            RETURNING id", &[&self.mtime, &self.size, &self.executable, &self.birth.time, &self.birth.version, &self.birth.hostname]
        )?;
        let id: i64 = rows[0].get(0);
        assert!(id >= 1);
        Ok(Inode::File(id))
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
    pub fn create(&self, transaction: &mut Transaction<'_>) -> Result<Inode> {
        let rows = transaction.query(
            "INSERT INTO symlinks (mtime, symlink_target, birth_time, birth_version, birth_hostname)
            VALUES ($1::timestamptz, $2::text, $3::timestamptz, $4::smallint, $5::text)
            RETURNING id", &[&self.mtime, &self.target, &self.birth.time, &self.birth.version, &self.birth.hostname]
        )?;
        let id: i64 = rows[0].get(0);
        assert!(id >= 1);
        Ok(Inode::Symlink(id))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;

    pub(crate) fn create_dummy_file(transaction: &mut Transaction<'_>) -> Result<Inode> {
        let file = NewFile { executable: false, size: 0, mtime: Utc::now(), birth: Birth::here_and_now() };
        file.create(transaction)
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
            let dir = NewDir { mtime: Utc::now(), birth: Birth::here_and_now() };
            let inode = dir.create(&mut transaction)?;
            transaction.execute("UPDATE dirs SET mtime = now() WHERE id = $1::bigint", &[&inode.dir_id()?])?;
            transaction.commit()?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a dir
        #[test]
        fn test_cannot_change_dir_immutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let dir = NewDir { mtime: Utc::now(), birth: Birth::here_and_now() };
            let inode = dir.create(&mut transaction)?;
            transaction.commit()?;
            for (column, value) in [("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE dirs SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&inode.dir_id()?]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id or birth_*");
            }
            Ok(())
        }

        /// Can change size, mtime, and executable on a file
        #[test]
        fn test_can_change_file_mutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let file = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now() };
            let inode = file.create(&mut transaction)?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET mtime = now() WHERE id = $1::bigint", &[&inode.file_id()?])?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET size = 100000 WHERE id = $1::bigint", &[&inode.file_id()?])?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET executable = true WHERE id = $1::bigint", &[&inode.file_id()?])?;
            transaction.commit()?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a file
        #[test]
        fn test_cannot_change_file_immutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let file = NewFile { size: 0, executable: false, mtime: Utc::now(), birth: Birth::here_and_now() };
            let inode = file.create(&mut transaction)?;
            transaction.commit()?;
            for (column, value) in [("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE files SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&inode.file_id()?]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id or birth_*");
            }
            Ok(())
        }

        /// Can change mtime on a symlink
        #[test]
        fn test_can_change_symlink_mutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let symlink = NewSymlink { target: "old".into(), mtime: Utc::now(), birth: Birth::here_and_now() };
            let inode = symlink.create(&mut transaction)?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE symlinks SET mtime = now() WHERE id = $1::bigint", &[&inode.symlink_id()?])?;
            transaction.commit()?;
            Ok(())
        }

        /// Cannot change id, symlink_target, birth_time, birth_version, or birth_hostname on a symlink
        #[test]
        fn test_cannot_change_symlink_immutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let symlink = NewSymlink { target: "old".into(), mtime: Utc::now(), birth: Birth::here_and_now() };
            let inode = symlink.create(&mut transaction)?;
            transaction.commit()?;
            for (column, value) in [("id", "100"), ("symlink_target", "'new'"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE symlinks SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&inode.symlink_id()?]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id, symlink_target, or birth_*");
            }
            Ok(())
        }
    }
}
