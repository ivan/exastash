use anyhow::Result;
use postgres::Transaction;
use postgres_types::{ToSql, FromSql};
use crate::db::inode::Inode;
use crate::postgres::SixteenBytes;

pub(crate) mod file;

#[postgres(name = "stash.cipher")]
#[derive(Debug, ToSql, FromSql)]
pub(crate) enum Cipher {
    #[postgres(name = "AES_128_CTR")]
    Aes128Ctr,
    #[postgres(name = "AES_128_GCM")]
    Aes128Gcm,
}

/// Creates a gsuite domain entity in the database.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_domain(transaction: &mut Transaction<'_>, domain: &str) -> Result<i32> {
    let rows = transaction.query("INSERT INTO gsuite_domains (domain) VALUES ($1::text) RETURNING id", &[&domain])?;
    let id = rows.get(0).unwrap().get(0);
    Ok(id)
}

pub(crate) struct Storage {
    pub gsuite_domain: String,
    pub cipher: Cipher,
    pub cipher_key: [u8; 16],
    pub gdrive_files: Vec<file::GdriveFile>,
}

/// Creates an gdrive storage entity in the database.
/// Does not commit the transaction, you must do so yourself.
/// Note that you must call file::create_gdrive_file for each gdrive file beforehand.
pub(crate) fn create_storage(transaction: &mut Transaction<'_>, inode: Inode, storage: &Storage) -> Result<()> {
    let file_id = inode.file_id()?;
    let gdrive_ids = storage.gdrive_files.iter().map(|f| f.id.clone()).collect::<Vec<_>>();
    let domain: i16 = transaction.query_one(
        "SELECT id from gsuite_domains WHERE domain = $1", &[&storage.gsuite_domain]
    )?.get(0);
    transaction.execute(
        "INSERT INTO storage_gdrive (domain, cipher, cipher_key, gdrive_ids)
         VALUES ($1::smallint, $2::cipher, $3::uuid, $4::bigint[])",
        &[&domain, &storage.cipher, &SixteenBytes { bytes: storage.cipher_key }, &gdrive_ids]
    )?;
    Ok(())
}

/// Returns a list of gdrive storage entities where the data for a file can be retrieved.
pub(crate) fn get_storage(mut transaction: &mut Transaction<'_>, inode: Inode) -> Result<Vec<Storage>> {
    let file_id = inode.file_id()?;

    transaction.execute("SET TRANSACTION READ ONLY", &[])?;
    let rows = transaction.query("
        SELECT domain, cipher, cipher_key, gdrive_ids FROM storage_gdrive WHERE file_id = $1
        LEFT JOIN gsuite_domains ON gdrive_files.gsuite_domain = gsuite_domains.id
    ", &[&file_id])?;
    if rows.len() == 0 {
        return Ok(vec![]);
    }

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let gdrive_file_ids: Vec<&str> = row.get(3);
        let gdrive_files = file::get_gdrive_files(&mut transaction, &gdrive_file_ids[..])?;
        let file = Storage {
            gsuite_domain: row.get(0),
            cipher: row.get(1),
            cipher_key: row.get::<_, SixteenBytes>(2).bytes,
            gdrive_files
        };
        out.push(file);
    }
    Ok(out)
}
