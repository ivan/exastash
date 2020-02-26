use anyhow::Result;
use chrono::{DateTime, Utc};
use postgres::Transaction;
use crate::db::inode::Inode;
use crate::postgres::{Md5, Crc32c};

/// Create a gdrive_owner in the database.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_owner(transaction: &mut Transaction, owner: &str) -> Result<i32> {
    let rows = transaction.query("INSERT INTO gdrive_owners (owner) VALUES ($1::text) RETURNING id", &[&owner])?;
    let id = rows.get(0).unwrap().get(0);
    Ok(id)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GdriveFile {
    id: String,
    owner_id: Option<i32>,
    md5: [u8; 16], // TODO: maybe [u32; 4]
    crc32c: u32,
    size: i64,
    last_probed: Option<DateTime<Utc>>,
}

/// Create a gdrive_file in the database.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_gdrive_file(transaction: &mut Transaction, file: &GdriveFile) -> Result<()> {
    transaction.execute(
        "INSERT INTO gdrive_files (id, owner, md5, crc32c, size, last_probed)
         VALUES ($1::text, $2::int, $3::uuid, $4::int, $5::bigint, $6::timestamptz)",
        &[&file.id, &file.owner_id, &Md5 { bytes: file.md5 }, &Crc32c { v: file.crc32c }, &file.size, &file.last_probed]
    )?;
    Ok(())
}

/// Returns gdrive files with matching ids.
pub(crate) fn get_gdrive_files(transaction: &mut Transaction, ids: &[&str]) -> Result<Vec<GdriveFile>> {
    transaction.execute("SET TRANSACTION READ ONLY", &[])?;
    let rows = transaction.query("SELECT id, owner, md5, crc32c, size, last_probed FROM gdrive_files WHERE id = ANY($1)", &[&ids])?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let md5: Md5 = row.get(2);
        let crc32c: Crc32c = row.get(3);
        let file = GdriveFile {
            id: row.get(0),
            owner_id: row.get(1),
            md5: md5.bytes,
            crc32c: crc32c.v,
            size: row.get(4),
            last_probed: row.get(5),
        };
        out.push(file);
    }
    Ok(out)
}

pub(crate) struct Storage {
    // TODO
}

/// Returns a list of places where the data for a file can be retrieved
pub(crate) fn get_storage(transaction: &mut Transaction, inode: Inode) -> Result<Vec<Storage>> {
    let file_id = inode.file_id();
    Ok(vec![])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;
    use crate::util;

    mod api {
        use super::*;

        #[test]
        fn test_create_gdrive_file() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let owner_id = create_owner(&mut transaction, "me@domain")?;
            let md5 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
            let file1 = GdriveFile { id: "A".repeat(28), owner_id: Some(owner_id), md5: md5.clone(), crc32c: 0, size: 1, last_probed: None };
            let file2 = GdriveFile { id: "A".repeat(160), owner_id: None, md5: md5.clone(), crc32c: 100, size: 1000, last_probed: Some(util::now_no_nanos()) };
            create_gdrive_file(&mut transaction, &file1)?;
            create_gdrive_file(&mut transaction, &file2)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let files = get_gdrive_files(&mut transaction, &[file1.id.as_ref(), file2.id.as_ref()])?;
            assert_eq!(files, vec![file1, file2]);

            Ok(())
        }
    }
}
