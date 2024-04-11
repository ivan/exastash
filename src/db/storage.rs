//! CRUD operations for the storage_* entities in PostgreSQL

pub mod fofs;
pub mod inline;
pub mod gdrive;
pub mod namedfiles;
pub mod internetarchive;

use crate::db;
use anyhow::Result;
use serde::Serialize;
use futures::try_join;

/// A storage entity
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum Storage {
    /// A storage entity backed by a file on a filesystem we control
    #[serde(rename = "fofs")]
    Fofs(fofs::Storage),
    /// A storage entity stored directly in the database
    #[serde(rename = "inline")]
    Inline(inline::Storage),
    /// A storage entity backed by Google Drive
    #[serde(rename = "gdrive")]
    Gdrive(gdrive::Storage),
    /// A storage entity backed by a file at some location
    #[serde(rename = "namedfiles")]
    NamedFiles(namedfiles::Storage),
    /// A storage entity backed by a file accessible at Internet Archive
    #[serde(rename = "internetarchive")]
    InternetArchive(internetarchive::Storage),
}

/// Like storage, but containing additional information for some types,
/// to avoid round trips to the database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum StorageView {
    /// A storage entity backed by a file on a filesystem we control
    #[serde(rename = "fofs")]
    Fofs(fofs::StorageView),
    /// A storage entity stored directly in the database
    #[serde(rename = "inline")]
    Inline(inline::Storage),
    /// A storage entity backed by Google Drive
    #[serde(rename = "gdrive")]
    Gdrive(gdrive::Storage),
    /// A storage entity backed by a file at some location
    #[serde(rename = "namedfiles")]
    NamedFiles(namedfiles::Storage),
    /// A storage entity backed by a file accessible at Internet Archive
    #[serde(rename = "internetarchive")]
    InternetArchive(internetarchive::Storage),
}

macro_rules! find_by_file_ids {
    ($pool:ident, $t:ty, $variant:path, $ids:ident) => {
        async {
            let mut transaction = $pool.begin().await?;
            let storages = <$t>::find_by_file_ids(&mut transaction, $ids).await?
                .into_iter().map($variant).collect::<Vec<_>>();
            transaction.commit().await?; // close read-only transaction
            anyhow::Ok(storages)
        }
    }
}

/// Return a list of places where the data for a file can be retrieved
pub async fn get_storages(file_ids: &[i64]) -> Result<Vec<Storage>> {
    let pool = db::pgpool().await;

    let (fofs, inline, gdrive, namedfiles, internetarchive) = try_join!(
        find_by_file_ids!(pool, inline::Storage,          Storage::Inline,          file_ids),
        find_by_file_ids!(pool, fofs::Storage,            Storage::Fofs,            file_ids),
        find_by_file_ids!(pool, gdrive::Storage,          Storage::Gdrive,          file_ids),
        find_by_file_ids!(pool, namedfiles::Storage,      Storage::NamedFiles,      file_ids),
        find_by_file_ids!(pool, internetarchive::Storage, Storage::InternetArchive, file_ids)
    )?;

    Ok([
        &inline[..],
        &fofs[..],
        &gdrive[..],
        &namedfiles[..],
        &internetarchive[..],
    ].concat())
}

/// Return a list of places where the data for a file can be retrieved
pub async fn get_storage_views(file_ids: &[i64]) -> Result<Vec<StorageView>> {
    let pool = db::pgpool().await;

    let (fofs, inline, gdrive, namedfiles, internetarchive) = try_join!(
        find_by_file_ids!(pool, inline::Storage,          StorageView::Inline,          file_ids),
        find_by_file_ids!(pool, fofs::StorageView,        StorageView::Fofs,            file_ids),
        find_by_file_ids!(pool, gdrive::Storage,          StorageView::Gdrive,          file_ids),
        find_by_file_ids!(pool, namedfiles::Storage,      StorageView::NamedFiles,      file_ids),
        find_by_file_ids!(pool, internetarchive::Storage, StorageView::InternetArchive, file_ids)
    )?;

    Ok([
        &inline[..],
        &fofs[..],
        &gdrive[..],
        &namedfiles[..],
        &internetarchive[..],
    ].concat())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::tests::new_primary_pool;
    use crate::db::inode::create_dummy_file;

    mod api {
        use super::*;

        /// If there is no storage for a file, get_storages returns an empty Vec
        #[tokio::test]
        async fn test_no_storage() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;
            let dummy = create_dummy_file(&mut transaction).await?;
            transaction.commit().await?;

            assert_eq!(get_storages(&[dummy.id]).await?, vec![]);

            Ok(())
        }

        /// If we add four storages for a file, get_storages returns all of them, in order of:
        /// fofs, inline, gdrive, internetarchive
        #[tokio::test]
        async fn test_create_storage_and_get_storage() -> Result<()> {
            let pool = new_primary_pool().await;

            let mut transaction = pool.begin().await?;

            let dummy = create_dummy_file(&mut transaction).await?;

            // internetarchive
            let storage1 = internetarchive::Storage { file_id: dummy.id, ia_item: "item1".into(), pathname: "path1".into(), darked: false, last_probed: None };
            storage1.create(&mut transaction).await?;
            let storage2 = internetarchive::Storage { file_id: dummy.id, ia_item: "item2".into(), pathname: "path2".into(), darked: true, last_probed: None };
            storage2.create(&mut transaction).await?;

            // namedfiles
            let storage6 = namedfiles::Storage { file_id: dummy.id, location: "loc1".into(), pathname: "path1".into(), last_probed: None };
            storage6.create(&mut transaction).await?;
            let storage7 = namedfiles::Storage { file_id: dummy.id, location: "loc2".into(), pathname: "path2".into(), last_probed: None };
            storage7.create(&mut transaction).await?;

            // gdrive
            let gdrive_file = gdrive::file::GdriveFile { id: "I".repeat(28), owner_id: None, md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            gdrive_file.create(&mut transaction).await?;
            let domain = gdrive::tests::create_dummy_domain(&mut transaction).await?;
            let storage3 = gdrive::Storage { file_id: dummy.id, google_domain: domain.id, cipher: gdrive::Cipher::Aes128Gcm, cipher_key: [0; 16], gdrive_ids: vec![gdrive_file.id.clone()] };
            storage3.create(&mut transaction).await?;

            // inline
            let storage4 = inline::Storage { file_id: dummy.id, content_zstd: "invalid zstd is ok".into() };
            storage4.create(&mut transaction).await?;

            // fofs
            let pile = fofs::NewPile { files_per_cell: 10, hostname: "localhost".into(), path: "/tmp/fake-fofs".into(), fullness_check_ratio: 1.into() }.create(&mut transaction).await?;
            let cell = fofs::NewCell { pile_id: pile.id }.create(&mut transaction).await?;
            let storage5 = fofs::Storage { file_id: dummy.id, cell_id: cell.id };
            storage5.create(&mut transaction).await?;
            transaction.commit().await?;

            assert_eq!(get_storages(&[dummy.id]).await?, vec![
                Storage::Fofs(storage5),
                Storage::Inline(storage4),
                Storage::Gdrive(storage3),
                Storage::NamedFiles(storage6),
                Storage::NamedFiles(storage7),
                Storage::InternetArchive(storage1),
                Storage::InternetArchive(storage2),
            ]);

            Ok(())
        }
    }
}
