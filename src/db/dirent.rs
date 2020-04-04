//! CRUD operations for dirent entities in PostgreSQL

use crate::db::inode::InodeId;
use anyhow::{bail, Result};
use postgres::Transaction;

/// A (dir, file, symlink) tuple that is useful when interacting with
/// the dirents table.
#[derive(Debug, Copy, Clone)]
pub struct InodeTuple(pub Option<i64>, pub Option<i64>, pub Option<i64>);

impl InodeTuple {
    /// Converts an `InodeTuple` to an `InodeId`.
    /// Exactly one value must be Some, else this returns an error.
    pub fn to_inode_id(self) -> Result<InodeId> {
        match self {
            InodeTuple(Some(id), None, None) => Ok(InodeId::Dir(id)),
            InodeTuple(None, Some(id), None) => Ok(InodeId::File(id)),
            InodeTuple(None, None, Some(id)) => Ok(InodeId::Symlink(id)),
            _                                => bail!("tuple {:?} does not have exactly 1 Some", self),
        }
    }

    /// Converts an `InodeId` to an `InodeTuple`.
    /// One value will be Some, the rest will be None.
    pub fn from_inode_id(inode: InodeId) -> InodeTuple {
        match inode {
            InodeId::Dir(id)     => InodeTuple(Some(id), None, None),
            InodeId::File(id)    => InodeTuple(None, Some(id), None),
            InodeId::Symlink(id) => InodeTuple(None, None, Some(id)),
        }
    }
}

/// A directory entry
#[derive(Debug, PartialEq, Eq)]
pub struct Dirent {
    /// The parent directory
    pub parent: InodeId,
    /// The basename (i.e. file name, not the whole path)
    pub basename: String,
    /// The inode the entry points to
    pub child: InodeId
}

impl Dirent {
    /// Return a `Dirent` with the given `basename` and `child` inode
    pub fn new<S: Into<String>>(parent: InodeId, basename: S, child: InodeId) -> Dirent {
        Dirent { parent, basename: basename.into(), child }
    }

    /// Create a directory entry.
    /// Does not commit the transaction, you must do so yourself.
    pub fn create(&self, transaction: &mut Transaction<'_>) -> Result<()> {
        let parent_id = self.parent.dir_id()?;
        let InodeTuple(child_dir, child_file, child_symlink) = InodeTuple::from_inode_id(self.child);
        transaction.execute(
            "INSERT INTO dirents (parent, basename, child_dir, child_file, child_symlink)
             VALUES ($1::bigint, $2::text, $3::bigint, $4::bigint, $5::bigint)",
            &[&parent_id, &self.basename, &child_dir, &child_file, &child_symlink]
        )?;
        Ok(())
    }
}

/// Return the children of a directory.
pub fn list_dir(transaction: &mut Transaction<'_>, parent: InodeId) -> Result<Vec<Dirent>> {
    let parent_id = parent.dir_id()?;
    let rows = transaction.query(
        "SELECT parent, basename, child_dir, child_file, child_symlink FROM dirents
         WHERE parent = $1::bigint", &[&parent_id])?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let parent: InodeId = InodeId::Dir(row.get(0));
        let basename: String = row.get(1);
        let tuple = InodeTuple(row.get(2), row.get(3), row.get(4));
        let inode = tuple.to_inode_id()?;
        let dirent = Dirent::new(parent, basename, inode);
        out.push(dirent);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::inode;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;
    use chrono::Utc;

    mod api {
        use super::*;

        #[test]
        fn test_create_dirent_and_list_dir() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let birth = inode::Birth::here_and_now();
            let parent = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            let child_file = inode::NewFile { size: 0, executable: false, mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            let child_symlink = inode::NewSymlink { target: "target".into(), mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            Dirent::new(parent, "child_dir", child_dir).create(&mut transaction)?;
            Dirent::new(parent, "child_file", child_file).create(&mut transaction)?;
            Dirent::new(parent, "child_symlink", child_symlink).create(&mut transaction)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(list_dir(&mut transaction, child_dir)?, vec![]);
            assert_eq!(list_dir(&mut transaction, parent)?, vec![
                Dirent::new(parent, "child_dir", child_dir),
                Dirent::new(parent, "child_file", child_file),
                Dirent::new(parent, "child_symlink", child_symlink),
            ]);

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;
        use crate::db::tests::assert_cannot_truncate;

        /// Cannot have child_dir equal to parent
        #[test]
        fn test_cannot_have_child_dir_equal_to_parent() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let parent = inode::NewDir { mtime: Utc::now(), birth: inode::Birth::here_and_now() }.create(&mut transaction)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let result = Dirent::new(parent, "self", parent).create(&mut transaction);
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                "db error: ERROR: new row for relation \"dirents\" violates check constraint \"dirents_check\""
            );

            Ok(())
        }

        /// Cannot UPDATE any row in dirents table
        #[test]
        fn test_cannot_update() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let birth = inode::Birth::here_and_now();
            let parent = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            Dirent::new(parent, "child_dir", child_dir).create(&mut transaction)?;
            transaction.commit()?;

            for (column, value) in [("parent", "100"), ("basename", "'new'"), ("child_dir", "1"), ("child_file", "1"), ("child_symlink", "1")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE dirents SET {} = {} WHERE parent = $1::bigint AND child_dir = $2::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&parent.dir_id()?, &child_dir.dir_id()?]);
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    "db error: ERROR: cannot change parent, basename, or child_*"
                );
            }

            Ok(())
        }

        /// Cannot TRUNCATE dirents table
        #[test]
        fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let birth = inode::Birth::here_and_now();
            let parent = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            Dirent::new(parent, "child_dir", child_dir).create(&mut transaction)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_cannot_truncate(&mut transaction, "dirents")?;

            Ok(())
        }

        /// Directory cannot be a child twice in some directory
        #[test]
        fn test_directory_cannot_have_more_than_one_basename() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let birth = inode::Birth::here_and_now();
            let parent = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            Dirent::new(parent, "child_dir", child_dir).create(&mut transaction)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let result = Dirent::new(parent, "child_dir_again", child_dir).create(&mut transaction);
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                "db error: ERROR: duplicate key value violates unique constraint \"dirents_child_dir_index\""
            );

            Ok(())
        }

        /// Directory cannot be a child of more than one parent
        #[test]
        fn test_directory_cannot_be_multiparented() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let birth = inode::Birth::here_and_now();
            let parent = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            let middle = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            let child = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            Dirent::new(parent, "middle", middle).create(&mut transaction)?;
            Dirent::new(middle, "child", child).create(&mut transaction)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let result = Dirent::new(parent, "child", child).create(&mut transaction);
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                "db error: ERROR: duplicate key value violates unique constraint \"dirents_child_dir_index\""
            );

            Ok(())
        }

        /// Basename cannot be "", "/", ".", or ".."
        /// Basename cannot be > 255 bytes
        #[test]
        fn test_basename_cannot_be_specials_or_too_long() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let birth = inode::Birth::here_and_now();
            let parent = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            let child = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction)?;
            transaction.commit()?;

            for basename in ["", "/", ".", "..", &"x".repeat(256)].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let result = Dirent::new(parent, basename.to_string(), child).create(&mut transaction);
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    "db error: ERROR: value for domain linux_basename violates check constraint \"linux_basename_check\""
                );
            }

            Ok(())
        }
    }
}
