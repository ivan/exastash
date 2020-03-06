mod inline;
mod gdrive;
mod internetarchive;

use anyhow::Result;
use postgres::Transaction;
use crate::db::inode::Inode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Storage {
    Inline(inline::Storage),
    Gdrive(gdrive::Storage),
    InternetArchive(internetarchive::Storage),
}

/// Returns a list of places where the data for a file can be retrieved
pub fn get_storage(transaction: &mut Transaction<'_>, inode: Inode) -> Result<Vec<Storage>> {
    let file_id = inode.file_id();

    transaction.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])?;
    let inline = inline::get_storage(transaction, inode)?
        .into_iter().map(Storage::Inline).collect::<Vec<_>>();
    let gdrive = gdrive::get_storage(transaction, inode)?
        .into_iter().map(Storage::Gdrive).collect::<Vec<_>>();
    let internetarchive = internetarchive::get_storage(transaction, inode)?
        .into_iter().map(Storage::InternetArchive).collect::<Vec<_>>();

    Ok([
        &inline[..],
        &gdrive[..],
        &internetarchive[..],
    ].concat())
}
