use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use postgres::Transaction;
use crate::EXASTASH_VERSION;
use crate::util;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum Inode {
    Dir(i64),
    File(i64),
    Symlink(i64),
}

impl Inode {
    fn to_dir_id(self) -> Result<i64> {
        match self {
            Inode::Dir(id) => Ok(id),
            _ => bail!("{:?} is not a dir", self),
        }
    }

    fn to_file_id(self) -> Result<i64> {
        match self {
            Inode::File(id) => Ok(id),
            _ => bail!("{:?} is not a file", self),
        }
    }

    fn to_symlink_id(self) -> Result<i64> {
        match self {
            Inode::Symlink(id) => Ok(id),
            _ => bail!("{:?} is not a symlink", self),
        }
    }
}

/// birth_time, birth_version, and birth_hostname for a dir/file/symlink
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Birth {
    /// The time at which a dir, file, or symlink was created
    time: DateTime<Utc>,
    /// The exastash version with which a dir, file, or symlink was a created
    version: i16,
    /// The hostname of the machine on which a dir, file, or symlink was a created
    hostname: String,
}

impl Birth {
    pub(crate) fn here_and_now() -> Birth {
        Birth { time: Utc::now(), version: EXASTASH_VERSION, hostname: util::get_hostname() }
    }
}

/// Create an entry for a directory in the database and return its id.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_dir(transaction: &mut Transaction, mtime: DateTime<Utc>, birth: &Birth) -> Result<Inode> {
    let rows = transaction.query(
        "INSERT INTO dirs (mtime, birth_time, birth_version, birth_hostname)
         VALUES ($1::timestamptz, $2::timestamptz, $3::smallint, $4::text)
         RETURNING id", &[&mtime, &birth.time, &birth.version, &birth.hostname]
    )?;
    let id: i64 = rows[0].get(0);
    assert!(id >= 1);
    Ok(Inode::Dir(id))
}

/// Create an entry for a file in the database and return its id.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_file(transaction: &mut Transaction, mtime: DateTime<Utc>, size: i64, executable: bool, birth: &Birth) -> Result<Inode> {
    assert!(size >= 0, "size must be >= 0");
    let rows = transaction.query(
        "INSERT INTO files (mtime, size, executable, birth_time, birth_version, birth_hostname)
         VALUES ($1::timestamptz, $2::bigint, $3::boolean, $4::timestamptz, $5::smallint, $6::text)
         RETURNING id", &[&mtime, &size, &executable, &birth.time, &birth.version, &birth.hostname]
    )?;
    let id: i64 = rows[0].get(0);
    assert!(id >= 1);
    Ok(Inode::File(id))
}

/// Create an entry for a symlink in the database and return its id.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_symlink(transaction: &mut Transaction, mtime: DateTime<Utc>, target: &str, birth: &Birth) -> Result<Inode> {
    let rows = transaction.query(
        "INSERT INTO symlinks (mtime, symlink_target, birth_time, birth_version, birth_hostname)
         VALUES ($1::timestamptz, $2::text, $3::timestamptz, $4::smallint, $5::text)
         RETURNING id", &[&mtime, &target, &birth.time, &birth.version, &birth.hostname]
    )?;
    let id: i64 = rows[0].get(0);
    assert!(id >= 1);
    Ok(Inode::Symlink(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;

        /// Cannot TRUNCATE dirs, files, or symlinks tables
        #[test]
        fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client();
            for table in ["dirs", "files", "symlinks"].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("TRUNCATE {} CASCADE", table);
                let result = transaction.execute(query.as_str(), &[]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: truncate is forbidden");
            }
            Ok(())
        }

        /// Can change mtime on a dir
        #[test]
        fn test_can_change_dir_mutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dir(&mut transaction, Utc::now(), &Birth::here_and_now())?;
            transaction.execute("UPDATE dirs SET mtime = now() WHERE id = $1::bigint", &[&inode.to_dir_id()?])?;
            transaction.commit()?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a dir
        #[test]
        fn test_cannot_change_dir_immutables() -> Result<()> {
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let inode = create_dir(&mut transaction, Utc::now(), &Birth::here_and_now())?;
            transaction.commit()?;
            for (column, value) in [("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE dirs SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&inode.to_dir_id()?]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id or birth_* columns");
            }
            Ok(())
        }

        /// Can change size, mtime, and executable on a file
        #[test]
        fn test_can_change_file_mutables() -> Result<()> {
            let size = 0;
            let executable = false;
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let inode = create_file(&mut transaction, Utc::now(), size, executable, &Birth::here_and_now())?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET mtime = now() WHERE id = $1::bigint", &[&inode.to_file_id()?])?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET size = 100000 WHERE id = $1::bigint", &[&inode.to_file_id()?])?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET executable = true WHERE id = $1::bigint", &[&inode.to_file_id()?])?;
            transaction.commit()?;
            Ok(())
        }

        /// Cannot change id, birth_time, birth_version, or birth_hostname on a file
        #[test]
        fn test_cannot_change_file_immutables() -> Result<()> {
            let size = 0;
            let executable = false;
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let inode = create_file(&mut transaction, Utc::now(), size, executable, &Birth::here_and_now())?;
            transaction.commit()?;
            for (column, value) in [("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE files SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&inode.to_file_id()?]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id or birth_* columns");
            }
            Ok(())
        }

        /// Can change mtime on a symlink
        #[test]
        fn test_can_change_symlink_mutables() -> Result<()> {
            let target = "old";
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let inode = create_symlink(&mut transaction, Utc::now(), target, &Birth::here_and_now())?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE symlinks SET mtime = now() WHERE id = $1::bigint", &[&inode.to_symlink_id()?])?;
            transaction.commit()?;
            Ok(())
        }

        /// Cannot change id, symlink_target, birth_time, birth_version, or birth_hostname on a symlink
        #[test]
        fn test_cannot_change_symlink_immutables() -> Result<()> {
            let target = "old";
            let mut client = get_client();
            let mut transaction = start_transaction(&mut client)?;
            let inode = create_symlink(&mut transaction, Utc::now(), target, &Birth::here_and_now())?;
            transaction.commit()?;
            for (column, value) in [("id", "100"), ("symlink_target", "'new'"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE symlinks SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&inode.to_symlink_id()?]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id, symlink_target, or birth_* columns");
            }
            Ok(())
        }
    }
}
