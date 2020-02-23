mod inline;
mod gdrive;
mod internetarchive;

use anyhow::Result;
use postgres::Transaction;
use crate::db::inode::Inode;

pub(crate) enum Storage {
    Inline(inline::Storage),
    Gdrive(gdrive::Storage),
    InternetArchive(internetarchive::Storage),
}

/// Returns a list of places where the data for a file can be retrieved
pub(crate) fn get_storage(transaction: &mut Transaction, inode: Inode) -> Result<Vec<Storage>> {
    let file_id = inode.file_id();

    let inline = inline::get_storage(transaction, inode)?;
    let gdrive = gdrive::get_storage(transaction, inode)?;
    let internetarchive = internetarchive::get_storage(transaction, inode)?;

    Ok(vec![])
}
