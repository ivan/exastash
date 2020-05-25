//! Functions for walking a path from a base_dir

use anyhow::{anyhow, Result};
use tokio_postgres::Transaction;
use crate::db::dirent::InodeTuple;
use crate::db::inode::InodeId;

/// Return the inode referenced by some path segments, starting from some base directory.
/// Does not resolve symlinks.
/// 
/// TODO: speed this up by farming it out to a PL/pgSQL function
pub async fn walk_path(transaction: &mut Transaction<'_>, base_dir: i64, path_components: &[&str]) -> Result<InodeId> {
    let mut current_inode = InodeId::Dir(base_dir);
    for component in path_components {
        let rows = transaction.query("
            SELECT child_dir, child_file, child_symlink FROM dirents
            WHERE parent = $1::bigint AND basename = $2::text", &[&current_inode.dir_id()?, &component]).await?;
        assert!(rows.len() <= 1, "expected <= 1 rows");
        let dir_id = current_inode.dir_id()?;
        let row = rows.get(0).ok_or_else(|| anyhow!("no such dirent {:?} under dir {:?}", component, dir_id))?;
        current_inode = InodeTuple(row.get(0), row.get(1), row.get(2)).to_inode_id()?;
    }
    Ok(current_inode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;
    use crate::db::dirent::Dirent;
    use chrono::Utc;
    use crate::db::inode;

    mod api {
        use super::*;

        #[tokio::test]
        async fn test_walk_path() -> Result<()> {
            let mut client = get_client().await;

            let mut transaction = start_transaction(&mut client).await?;
            let birth = inode::Birth::here_and_now();
            let root_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            let child_file = inode::NewFile { size: 0, executable: false, mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            let child_symlink = inode::NewSymlink { target: "target".into(), mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(root_dir, "child_dir", InodeId::Dir(child_dir)).create(&mut transaction).await?;
            Dirent::new(root_dir, "child_file", InodeId::File(child_file)).create(&mut transaction).await?;
            Dirent::new(root_dir, "child_symlink", InodeId::Symlink(child_symlink)).create(&mut transaction).await?;
            // Give child_file a second location as well
            Dirent::new(child_dir, "child_file", InodeId::File(child_file)).create(&mut transaction).await?;
            // Give child_symlink a second location as well
            Dirent::new(child_dir, "child_symlink", InodeId::Symlink(child_symlink)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = start_transaction(&mut client).await?;

            // walk_path returns the base_dir if there are no components to walk
            assert_eq!(walk_path(&mut transaction, root_dir, &[]).await?, InodeId::Dir(root_dir));

            // walk_path returns an InodeId::Dir if segments point to a dir
            assert_eq!(walk_path(&mut transaction, root_dir, &["child_dir"]).await?, InodeId::Dir(child_dir));

            // walk_path returns an InodeId::File if segments point to a file
            assert_eq!(walk_path(&mut transaction, root_dir, &["child_file"]).await?, InodeId::File(child_file));
            assert_eq!(walk_path(&mut transaction, root_dir, &["child_dir", "child_file"]).await?, InodeId::File(child_file));

            // walk_path returns an InodeId::Symlink if segments point to a symlink
            assert_eq!(walk_path(&mut transaction, root_dir, &["child_symlink"]).await?, InodeId::Symlink(child_symlink));
            assert_eq!(walk_path(&mut transaction, root_dir, &["child_dir", "child_symlink"]).await?, InodeId::Symlink(child_symlink));

            // walk_path returns an error if some segment is not found
            for (parent, segments) in &[
                (root_dir, vec![""]),
                (root_dir, vec!["nonexistent"]),
                (child_dir, vec!["child_dir", "nonexistent"]),
            ] {
                let result = walk_path(&mut transaction, root_dir, &segments).await;
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    format!("no such dirent {:?} under dir {:?}", segments.last().unwrap(), parent)
                );
            }

            // walk_path returns an error if trying to walk down a file or symlink
            for (parent, not_a_dir, segments) in &[
                (root_dir, InodeId::File(child_file), vec!["child_file", "further"]),
                (root_dir, InodeId::Symlink(child_symlink), vec!["child_symlink", "further"]),
            ] {
                let result = walk_path(&mut transaction, *parent, &segments).await;
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    format!("{:?} is not a dir", not_a_dir)
                );
            }

            Ok(())
        }
    }
}
