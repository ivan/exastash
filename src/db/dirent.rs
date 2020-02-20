use anyhow::{bail, Result};
use postgres::Transaction;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum Inode {
    Dir(i64),
    File(i64),
    Symlink(i64),
}

impl Inode {
    /// Converts a (dir, file, symlink) tuple from the database.
    /// Exactly one must be Some, else this panics.
    fn from_tuple(child_dir: Option<i64>, child_file: Option<i64>, child_symlink: Option<i64>) -> Inode {
        let tuple = (child_dir, child_file, child_symlink);
        match tuple {
            (Some(id), None, None) => Inode::Dir(id),
            (None, Some(id), None) => Inode::File(id),
            (None, None, Some(id)) => Inode::Symlink(id),
            _                      => panic!("unexpected tuple {:?}", tuple),
        }
    }

    /// Returns a (dir, file, symlink) tuple for use with the database.
    /// One will be Some, the rest will be None.
    fn to_tuple(self) -> (Option<i64>, Option<i64>, Option<i64>) {
        match self {
            Inode::Dir(id)     => (Some(id), None, None),
            Inode::File(id)    => (None, Some(id), None),
            Inode::Symlink(id) => (None, None, Some(id)),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Dirent {
    basename: String,
    child: Inode
}

impl Dirent {
    fn new(basename: String, child: Inode) -> Dirent {
        Dirent { basename, child }
    }
}

/// Create a directory entry.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_dirent(transaction: &mut Transaction, parent: Inode, dirent: &Dirent) -> Result<()> {
    let parent_id = match parent {
        Inode::Dir(id) => id,
        _ => bail!("parent must be a directory"),
    };
    let (child_dir, child_file, child_symlink) = dirent.child.to_tuple();
    transaction.execute(
        "INSERT INTO dirents (parent, basename, child_dir, child_file, child_symlink)
         VALUES ($1::bigint, $2::text, $3::bigint, $4::bigint, $5::bigint)",
        &[&parent_id, &dirent.basename, &child_dir, &child_file, &child_symlink]
    )?;
    Ok(())
}

/// Returns the children of a directory.
pub(crate) fn list_dir(transaction: &mut Transaction, parent: Inode) -> Result<Vec<Dirent>> {
    let parent_id = match parent {
        Inode::Dir(id) => id,
        _ => bail!("parent must be a directory"),
    };

    let rows = transaction.query("SELECT basename, child_dir, child_file, child_symlink FROM dirents WHERE parent = $1::bigint", &[&parent_id])?;
    let mut out = vec![];
    for row in rows {
        let basename: String = row.get(0);
        let child_dir: Option<i64> = row.get(1);
        let child_file: Option<i64> = row.get(2);
        let child_symlink: Option<i64> = row.get(3);
        let inode = Inode::from_tuple(child_dir, child_file, child_symlink);
        let dirent = Dirent::new(basename, inode);
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
        fn test_create_dirent() -> Result<()> {
            let mut client = get_client();

            // Create two directories
            let mut transaction = start_transaction(&mut client)?;
            let parent = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            let child  = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            let dirent = Dirent::new("child".to_owned(), Inode::Dir(child));
            create_dirent(&mut transaction, Inode::Dir(parent), &dirent)?;
            transaction.commit()?;
            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
    }
}
