//! Functions to delete storage

use anyhow::Result;
use crate::db;
use crate::util;
use crate::storage::StoragesDescriptor;
use tracing::info;

/// Delete storages for a file and remove them to the database.
pub async fn delete_storages(file_id: i64, undesired: &StoragesDescriptor) -> Result<()> {
    if undesired.is_empty() {
        return Ok(());
    }

    let pool = db::pgpool().await;

    if !undesired.fofs.is_empty() {
        let mut transaction = pool.begin().await?;
        let storage_views = db::storage::fofs::StorageView::find_by_file_ids(&mut transaction, &[file_id]).await?;
        transaction.commit().await?;

        let my_hostname = util::get_hostname();
        for view in storage_views {
            info!(file_id, pile_id = view.pile_id, cell_id = view.cell_id, "deleting storage_fofs for file");
            if view.pile_hostname != my_hostname {
                unimplemented!("deleting from another machine");
            }
            let mut transaction = pool.begin().await?;
            db::storage::fofs::Storage::delete_by_file_id_and_cell_id(&mut transaction, file_id, view.cell_id).await?;
            transaction.commit().await?;
            // Above, we remove the database reference first to avoid the possibility
            // of the database pointing to nonexistent storages.
            let fname = format!("{}/{}/{}/{file_id}", view.pile_path, view.pile_id, view.cell_id);
            tokio::fs::remove_file(fname).await?;
        }
    }
    if undesired.inline {
        info!(file_id, "deleting storage_inline for file");
        let mut transaction = pool.begin().await?;
        db::storage::inline::Storage::delete_by_file_ids(&mut transaction, &[file_id]).await?;
        transaction.commit().await?;
    }
    if !undesired.gdrive.is_empty() {
        unimplemented!("removing a storage_gdrive");
    }

    Ok(())
}
