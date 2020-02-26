use anyhow::Result;
use postgres::Transaction;
use crate::db::inode::Inode;

pub(crate) mod file;

pub(crate) struct Storage {
    // TODO
}

/// Returns a list of places where the data for a file can be retrieved
pub(crate) fn get_storage(transaction: &mut Transaction<'_>, inode: Inode) -> Result<Vec<Storage>> {
    let file_id = inode.file_id();
    Ok(vec![])
}
