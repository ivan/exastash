use anyhow::Result;
use postgres::Transaction;

pub(crate) enum Inode {
    Dir(i64),
    File(i64),
    Symlink(i64),
}

/// Create a directory entry.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_dirent(transaction: &mut Transaction, parent: i64, basename: &str, child: &Inode) -> Result<()> {
    let (child_dir, child_file, child_symlink) = match child {
        Inode::Dir(id)     => (Some(id), None, None),
        Inode::File(id)    => (None, Some(id), None),
        Inode::Symlink(id) => (None, None, Some(id)),
    };
    transaction.execute(
        "INSERT INTO dirents (parent, basename, child_dir, child_file, child_symlink)
         VALUES ($1::bigint, $2::text, $3::bigint, $4::bigint, $5::bigint)",
        &[&parent, &basename, &child_dir, &child_file, &child_symlink]
    )?;
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

            let basename = "child";
            let child = Inode::Dir(child);
            create_dirent(&mut transaction, parent, basename, &child)?;
            transaction.commit()?;
            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
    }
}
