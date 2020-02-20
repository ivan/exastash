use anyhow::{bail, Result};
use postgres::Transaction;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum Inode {
    Dir(i64),
    File(i64),
    Symlink(i64),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Dirent<'a> {
    basename: &'a str,
    child: Inode
}

impl<'a> Dirent<'a> {
    fn new(basename: &'a str, child: Inode) -> Dirent<'a> {
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
    let (child_dir, child_file, child_symlink) = match dirent.child {
        Inode::Dir(id)     => (Some(id), None, None),
        Inode::File(id)    => (None, Some(id), None),
        Inode::Symlink(id) => (None, None, Some(id)),
    };
    transaction.execute(
        "INSERT INTO dirents (parent, basename, child_dir, child_file, child_symlink)
         VALUES ($1::bigint, $2::text, $3::bigint, $4::bigint, $5::bigint)",
        &[&parent_id, &dirent.basename, &child_dir, &child_file, &child_symlink]
    )?;
    Ok(())
}

/// Returns  Vec of (basename: String, child: Inode)
pub(crate) fn list_dir(transaction: &mut Transaction, parent: &Inode) -> Result<()> {
    let parent_id = match parent {
        Inode::Dir(id) => id,
        _ => bail!("parent must be a directory"),
    };

    Ok(())
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
            let dirent = Dirent::new("child", Inode::Dir(child));
            create_dirent(&mut transaction, Inode::Dir(parent), &dirent)?;
            transaction.commit()?;
            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
    }
}
