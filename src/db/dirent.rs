use crate::db::inode::Inode;
use anyhow::Result;
use postgres::Transaction;

/// A (dir, file, symlink) tuple that is useful when interacting with
/// the dirents table.
#[derive(Debug)]
struct InodeTuple(Option<i64>, Option<i64>, Option<i64>);

impl InodeTuple {
    /// Converts an InodeTuple to an Inode.
    /// Exactly one value must be Some, else this panics.
    fn to_inode(self) -> Inode {
        match self {
            InodeTuple(Some(id), None, None) => Inode::Dir(id),
            InodeTuple(None, Some(id), None) => Inode::File(id),
            InodeTuple(None, None, Some(id)) => Inode::Symlink(id),
            _                                => panic!("tuple {:?} does not have exactly 1 Some", self),
        }
    }

    /// Converts an Inode to an InodeTuple.
    /// One value will be Some, the rest will be None.
    fn from_inode(inode: Inode) -> InodeTuple {
        match inode {
            Inode::Dir(id)     => InodeTuple(Some(id), None, None),
            Inode::File(id)    => InodeTuple(None, Some(id), None),
            Inode::Symlink(id) => InodeTuple(None, None, Some(id)),
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
    let parent_id = parent.dir_id()?;
    let InodeTuple(child_dir, child_file, child_symlink) = InodeTuple::from_inode(dirent.child);
    transaction.execute(
        "INSERT INTO dirents (parent, basename, child_dir, child_file, child_symlink)
         VALUES ($1::bigint, $2::text, $3::bigint, $4::bigint, $5::bigint)",
        &[&parent_id, &dirent.basename, &child_dir, &child_file, &child_symlink]
    )?;
    Ok(())
}

/// Returns the children of a directory.
pub(crate) fn list_dir(transaction: &mut Transaction, parent: Inode) -> Result<Vec<Dirent>> {
    let parent_id = parent.dir_id()?;
    transaction.execute("SET TRANSACTION READ ONLY", &[])?;
    let rows = transaction.query("SELECT basename, child_dir, child_file, child_symlink FROM dirents WHERE parent = $1::bigint", &[&parent_id])?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let basename: String = row.get(0);
        let tuple = InodeTuple(row.get(1), row.get(2), row.get(3));
        let inode = tuple.to_inode();
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
        fn test_create_dirent_and_list_dir() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let parent = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            let child_dir = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            let child_file = inode::create_file(&mut transaction, Utc::now(), 0, false, &inode::Birth::here_and_now())?;
            let child_symlink = inode::create_symlink(&mut transaction, Utc::now(), "target", &inode::Birth::here_and_now())?;
            create_dirent(&mut transaction, parent, &Dirent::new("child_dir".to_owned(), child_dir))?;
            create_dirent(&mut transaction, parent, &Dirent::new("child_file".to_owned(), child_file))?;
            create_dirent(&mut transaction, parent, &Dirent::new("child_symlink".to_owned(), child_symlink))?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            assert_eq!(list_dir(&mut transaction, child_dir)?, vec![]);
            assert_eq!(list_dir(&mut transaction, parent)?, vec![
                Dirent::new("child_dir".to_owned(), child_dir),
                Dirent::new("child_file".to_owned(), child_file),
                Dirent::new("child_symlink".to_owned(), child_symlink),
            ]);

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;

        /// Cannot have child_dir equal to parent
        #[test]
        fn test_cannot_have_child_dir_equal_to_parent() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let parent = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let result = create_dirent(&mut transaction, parent, &Dirent::new("self".to_owned(), parent));
            assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: new row for relation \"dirents\" violates check constraint \"dirents_check\"");

            Ok(())
        }

        /// Cannot UPDATE any row in dirents table
        #[test]
        fn test_cannot_update() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let parent = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            let child_dir = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            create_dirent(&mut transaction, parent, &Dirent::new("child_dir".to_owned(), child_dir))?;
            transaction.commit()?;

            for (column, value) in [("parent", "100"), ("basename", "'new'"), ("child_dir", "1"), ("child_file", "1"), ("child_symlink", "1")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE dirents SET {} = {} WHERE parent = $1::bigint AND child_dir = $2::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&parent.dir_id()?, &child_dir.dir_id()?]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change parent, basename, or child_*");
            }

            Ok(())
        }

        /// Cannot TRUNCATE dirents table
        #[test]
        fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let parent = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            let child_dir = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            create_dirent(&mut transaction, parent, &Dirent::new("child_dir".to_owned(), child_dir))?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let result = transaction.execute("TRUNCATE dirents", &[]);
            assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: truncate is forbidden");

            Ok(())
        }

        /// Directory cannot be a child twice in some directory
        #[test]
        fn test_directory_cannot_have_more_than_one_basename() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let parent = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            let child_dir = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            create_dirent(&mut transaction, parent, &Dirent::new("child_dir".to_owned(), child_dir))?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let result = create_dirent(&mut transaction, parent, &Dirent::new("child_dir_again".to_owned(), child_dir));
            assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: duplicate key value violates unique constraint \"dirents_child_dir_index\"");

            Ok(())
        }

        /// Directory cannot be a child of more than one parent
        #[test]
        fn test_directory_cannot_be_multiparented() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let parent = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            let middle = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            let child = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            create_dirent(&mut transaction, parent, &Dirent::new("middle".to_owned(), middle))?;
            create_dirent(&mut transaction, middle, &Dirent::new("child".to_owned(), child))?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let result = create_dirent(&mut transaction, parent, &Dirent::new("child".to_owned(), child));
            assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: duplicate key value violates unique constraint \"dirents_child_dir_index\"");

            Ok(())
        }

        /// Basename cannot be "", "/", ".", or ".."
        /// Basename cannot be > 255 bytes
        #[test]
        fn test_basename_cannot_be_specials_or_too_long() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let parent = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            let child = inode::create_dir(&mut transaction, Utc::now(), &inode::Birth::here_and_now())?;
            transaction.commit()?;

            for basename in ["", "/", ".", "..", &"x".repeat(256)].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let result = create_dirent(&mut transaction, parent, &Dirent::new(basename.to_string(), child));
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: value for domain linux_basename violates check constraint \"linux_basename_check\"");
            }

            Ok(())
        }
    }
}
