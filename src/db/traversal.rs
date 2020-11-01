//! Functions for walking a path from a base_dir

use chrono::Utc;
use anyhow::{anyhow, bail, Result};
use sqlx::{Postgres, Transaction};
use crate::db::dirent::Dirent;
use crate::db::inode::{InodeId, NewDir, Birth};
use crate::Error;

/// Returns the inode referenced by the last path segment, starting from some base directory.
/// Does not resolve symlinks.
pub async fn resolve_inode<S: AsRef<str> + ToString + Clone>(transaction: &mut Transaction<'_, Postgres>, base_dir: i64, path_components: &[S]) -> Result<InodeId> {
    let mut current_inode = InodeId::Dir(base_dir);
    for component in path_components {
        let dir_id = current_inode.dir_id()?;
        if let Some(dirent) = Dirent::find_by_parent_and_basename(transaction, dir_id, component.as_ref()).await? {
            current_inode = dirent.child;
        } else {
            bail!(Error::NoDirent { parent: dir_id, basename: component.to_string() });
        }
    }
    Ok(current_inode)
}

/// Returns the dirent referenced by the last path segment, starting from some base directory.
/// Does not resolve symlinks.
pub async fn resolve_dirent<S: AsRef<str> + ToString + Clone>(transaction: &mut Transaction<'_, Postgres>, base_dir: i64, path_components: &[S]) -> Result<Dirent> {
    let mut current_inode = InodeId::Dir(base_dir);
    let mut last_dirent = None;
    for component in path_components {
        let dir_id = current_inode.dir_id()?;
        if let Some(dirent) = Dirent::find_by_parent_and_basename(transaction, dir_id, component.as_ref()).await? {
            current_inode = dirent.child;
            last_dirent = Some(dirent);
        } else {
            bail!(Error::NoDirent { parent: dir_id, basename: component.to_string() });
        }
    }
    Ok(last_dirent.ok_or_else(|| anyhow!("resolve_dirent: need at least one path segment to traverse"))?)
}

/// Resolve path_components but also create new directories as needed, like `mkdir -p`.
/// Does not commit the transaction, you must do so yourself.
pub async fn make_dirs<S: AsRef<str> + ToString + Clone>(transaction: &mut Transaction<'_, Postgres>, base_dir: i64, path_components: &[S]) -> Result<InodeId> {
    let mut current_inode = InodeId::Dir(base_dir);
    for component in path_components {
        let dir_id = current_inode.dir_id()?;
        if let Some(dirent) = Dirent::find_by_parent_and_basename(transaction, dir_id, component.as_ref()).await? {
            current_inode = dirent.child;
        } else {
            let mtime = Utc::now();
            let birth = Birth::here_and_now();
            let dir = NewDir { mtime, birth }.create(transaction).await?;
            Dirent::new(dir_id, component.as_ref(), InodeId::Dir(dir.id)).create(transaction).await?;

            current_inode = InodeId::Dir(dir.id);
        }
    }
    Ok(current_inode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::tests::new_primary_pool;
    use crate::db::dirent::Dirent;
    use chrono::Utc;
    use crate::db::inode;
    use crate::db::dirent::tests::make_basename;

    mod api {
        use super::*;

        #[tokio::test]
        async fn test_resolve_inode() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let birth = inode::Birth::here_and_now();
            let root_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(1, make_basename("root_dir"), InodeId::Dir(root_dir.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            let child_file = inode::NewFile { size: 0, executable: false, mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            let child_symlink = inode::NewSymlink { target: "target".into(), mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(root_dir.id, "child_dir", InodeId::Dir(child_dir.id)).create(&mut transaction).await?;
            Dirent::new(root_dir.id, "child_file", InodeId::File(child_file.id)).create(&mut transaction).await?;
            Dirent::new(root_dir.id, "child_symlink", InodeId::Symlink(child_symlink.id)).create(&mut transaction).await?;
            // Give child_file a second location as well
            Dirent::new(child_dir.id, "child_file", InodeId::File(child_file.id)).create(&mut transaction).await?;
            // Give child_symlink a second location as well
            Dirent::new(child_dir.id, "child_symlink", InodeId::Symlink(child_symlink.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;

            // resolve_inode returns the base_dir if there are no components to walk
            let no_components: Vec<&str> = vec![];
            assert_eq!(resolve_inode(&mut transaction, root_dir.id, &no_components).await?, InodeId::Dir(root_dir.id));

            // resolve_inode returns an InodeId::Dir if segments point to a dir
            assert_eq!(resolve_inode(&mut transaction, root_dir.id, &["child_dir"]).await?, InodeId::Dir(child_dir.id));

            // resolve_inode returns an InodeId::File if segments point to a file
            assert_eq!(resolve_inode(&mut transaction, root_dir.id, &["child_file"]).await?, InodeId::File(child_file.id));
            assert_eq!(resolve_inode(&mut transaction, root_dir.id, &["child_dir", "child_file"]).await?, InodeId::File(child_file.id));

            // resolve_inode returns an InodeId::Symlink if segments point to a symlink
            assert_eq!(resolve_inode(&mut transaction, root_dir.id, &["child_symlink"]).await?, InodeId::Symlink(child_symlink.id));
            assert_eq!(resolve_inode(&mut transaction, root_dir.id, &["child_dir", "child_symlink"]).await?, InodeId::Symlink(child_symlink.id));

            // resolve_inode returns an error if some segment is not found
            for (parent, segments) in &[
                (root_dir.id, vec![""]),
                (root_dir.id, vec!["nonexistent"]),
                (child_dir.id, vec!["child_dir", "nonexistent"]),
            ] {
                let result = resolve_inode(&mut transaction, root_dir.id, &segments).await;
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    format!("no such dirent {:?} under dir {:?}", segments.last().unwrap(), parent)
                );
            }

            // resolve_inode returns an error if trying to walk down a file or symlink
            for (parent, not_a_dir, segments) in &[
                (root_dir.id, InodeId::File(child_file.id), vec!["child_file", "further"]),
                (root_dir.id, InodeId::Symlink(child_symlink.id), vec!["child_symlink", "further"]),
            ] {
                let result = resolve_inode(&mut transaction, *parent, &segments).await;
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    format!("{:?} is not a dir", not_a_dir)
                );
            }

            Ok(())
        }

        #[tokio::test]
        async fn test_resolve_dirent() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let birth = inode::Birth::here_and_now();
            let root_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(1, make_basename("root_dir"), InodeId::Dir(root_dir.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;
            let child_dir = inode::NewDir { mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            let child_file = inode::NewFile { size: 0, executable: false, mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            let child_symlink = inode::NewSymlink { target: "target".into(), mtime: Utc::now(), birth: birth.clone() }.create(&mut transaction).await?;
            Dirent::new(root_dir.id, "child_dir", InodeId::Dir(child_dir.id)).create(&mut transaction).await?;
            Dirent::new(root_dir.id, "child_file", InodeId::File(child_file.id)).create(&mut transaction).await?;
            Dirent::new(root_dir.id, "child_symlink", InodeId::Symlink(child_symlink.id)).create(&mut transaction).await?;
            // Give child_file a second location as well
            Dirent::new(child_dir.id, "child_file", InodeId::File(child_file.id)).create(&mut transaction).await?;
            // Give child_symlink a second location as well
            Dirent::new(child_dir.id, "child_symlink", InodeId::Symlink(child_symlink.id)).create(&mut transaction).await?;
            transaction.commit().await?;

            let mut transaction = pool.begin().await?;

            // resolve_dirent returns an error if there are no components to walk
            let no_components: Vec<&str> = vec![];
            let result = resolve_dirent(&mut transaction, root_dir.id, &no_components).await;
            assert_eq!(
                result.err().expect("expected an error").to_string(),
                "resolve_dirent: need at least one path segment to traverse"
            );

            // resolve_dirent returns a Dirent with an InodeId::Dir child if segments point to a dir
            assert_eq!(resolve_dirent(&mut transaction, root_dir.id, &["child_dir"]).await?.child, InodeId::Dir(child_dir.id));

            // resolve_dirent returns a Dirent with an InodeId::File child if segments point to a file
            assert_eq!(resolve_dirent(&mut transaction, root_dir.id, &["child_file"]).await?.child, InodeId::File(child_file.id));
            assert_eq!(resolve_dirent(&mut transaction, root_dir.id, &["child_dir", "child_file"]).await?.child, InodeId::File(child_file.id));

            // resolve_dirent returns a Dirent with an InodeId::Symlink child if segments point to a symlink
            assert_eq!(resolve_dirent(&mut transaction, root_dir.id, &["child_symlink"]).await?.child, InodeId::Symlink(child_symlink.id));
            assert_eq!(resolve_dirent(&mut transaction, root_dir.id, &["child_dir", "child_symlink"]).await?.child, InodeId::Symlink(child_symlink.id));

            // resolve_dirent returns an error if some segment is not found
            for (parent, segments) in &[
                (root_dir.id, vec![""]),
                (root_dir.id, vec!["nonexistent"]),
                (child_dir.id, vec!["child_dir", "nonexistent"]),
            ] {
                let result = resolve_dirent(&mut transaction, root_dir.id, &segments).await;
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    format!("no such dirent {:?} under dir {:?}", segments.last().unwrap(), parent)
                );
            }

            // resolve_dirent returns an error if trying to walk down a file or symlink
            for (parent, not_a_dir, segments) in &[
                (root_dir.id, InodeId::File(child_file.id), vec!["child_file", "further"]),
                (root_dir.id, InodeId::Symlink(child_symlink.id), vec!["child_symlink", "further"]),
            ] {
                let result = resolve_dirent(&mut transaction, *parent, &segments).await;
                assert_eq!(
                    result.err().expect("expected an error").to_string(),
                    format!("{:?} is not a dir", not_a_dir)
                );
            }

            Ok(())
        }
    }
}
