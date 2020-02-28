use std::collections::HashMap;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use postgres::Transaction;
use crate::postgres::{SixteenBytes, Crc32c};

/// Create a gdrive_owner in the database.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_owner(transaction: &mut Transaction<'_>, owner: &str) -> Result<i32> {
    let rows = transaction.query("INSERT INTO gdrive_owners (owner) VALUES ($1::text) RETURNING id", &[&owner])?;
    let id = rows.get(0).unwrap().get(0);
    Ok(id)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GdriveFile {
    pub id: String,
    pub owner_id: Option<i32>,
    pub md5: [u8; 16], // TODO: maybe [u32; 4]
    pub crc32c: u32,
    pub size: i64,
    pub last_probed: Option<DateTime<Utc>>,
}

/// Create a gdrive_file in the database.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn create_gdrive_file(transaction: &mut Transaction<'_>, file: &GdriveFile) -> Result<()> {
    transaction.execute(
        "INSERT INTO gdrive_files (id, owner, md5, crc32c, size, last_probed)
         VALUES ($1::text, $2::int, $3::uuid, $4::int, $5::bigint, $6::timestamptz)",
        &[&file.id, &file.owner_id, &SixteenBytes { bytes: file.md5 }, &Crc32c { v: file.crc32c }, &file.size, &file.last_probed]
    )?;
    Ok(())
}

/// Remove gdrive files in the database.
/// Does not commit the transaction, you must do so yourself.
pub(crate) fn remove_gdrive_files(transaction: &mut Transaction<'_>, ids: &[&str]) -> Result<()> {
    transaction.execute("DELETE FROM gdrive_files WHERE id = ANY($1::text[])", &[&ids])?;
    Ok(())
}

/// Returns gdrive files with matching ids, in the same order as the ids.
pub(crate) fn get_gdrive_files(transaction: &mut Transaction<'_>, ids: &[&str]) -> Result<Vec<GdriveFile>> {
    transaction.execute("SET TRANSACTION READ ONLY", &[])?;
    let rows = transaction.query("SELECT id, owner, md5, crc32c, size, last_probed FROM gdrive_files WHERE id = ANY($1)", &[&ids])?;
    let mut map: HashMap<String, GdriveFile> = HashMap::new();
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let file = GdriveFile {
            id: row.get(0),
            owner_id: row.get(1),
            md5: row.get::<_, SixteenBytes>(2).bytes,
            crc32c: row.get::<_, Crc32c>(3).v,
            size: row.get(4),
            last_probed: row.get(5),
        };
        map.insert(file.id.clone(), file);
    }
    for id in ids.iter() {
        let file = map.remove(id.clone()).ok_or_else(|| anyhow!("duplicate id given"))?;
        out.push(file);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::start_transaction;
    use crate::db::tests::get_client;
    use crate::util;

    mod api {
        use super::*;

        // Can create gdrive files
        #[test]
        fn test_create_gdrive_file() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let owner_id = create_owner(&mut transaction, "me@domain1")?;
            let file1 = GdriveFile { id: "A".repeat(28),  owner_id: Some(owner_id), md5: [0; 16], crc32c: 0,   size: 1,    last_probed: None };
            let file2 = GdriveFile { id: "A".repeat(160), owner_id: None,           md5: [0; 16], crc32c: 100, size: 1000, last_probed: Some(util::now_no_nanos()) };
            create_gdrive_file(&mut transaction, &file1)?;
            create_gdrive_file(&mut transaction, &file2)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let files = get_gdrive_files(&mut transaction, &[file1.id.as_ref(), file2.id.as_ref()])?;
            assert_eq!(files, vec![file1.clone(), file2.clone()]);

            // Files are returned in the same order as ids
            let files = get_gdrive_files(&mut transaction, &[file2.id.as_ref(), file1.id.as_ref()])?;
            assert_eq!(files, vec![file2.clone(), file1.clone()]);

            // Empty list is OK
            let files = get_gdrive_files(&mut transaction, &[])?;
            assert_eq!(files, vec![]);

            // Duplicate id is not OK
            let result = get_gdrive_files(&mut transaction, &[file1.id.as_ref(), file2.id.as_ref(), file1.id.as_ref()]);
            assert_eq!(result.err().expect("expected an error").to_string(), "duplicate id given");

            Ok(())
        }

        // Can remove gdrive files not referenced by storage_gdrive
        #[test]
        fn test_remove_gdrive_files() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let owner_id = create_owner(&mut transaction, "me@domain2")?;
            let file = GdriveFile { id: "Q".repeat(28), owner_id: Some(owner_id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            remove_gdrive_files(&mut transaction, &[&file.id])?;
            transaction.commit()?;

            Ok(())
        }
    }

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;

        /// Cannot UPDATE any row in gdrive_files table
        #[test]
        fn test_cannot_update() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let owner_id = create_owner(&mut transaction, "me@domain3")?;
            let file = GdriveFile { id: "B".repeat(28), owner_id: Some(owner_id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file)?;
            transaction.commit()?;

            let new_id = format!("'{}'", "C".repeat(28));
            for (column, value) in [("id", new_id.as_str()), ("md5", "'0000-0000-0000-0000-0000-0000-0000-0001'::uuid"), ("crc32c", "1"), ("size", "2")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE gdrive_files SET {} = {} WHERE id = $1", column, value);
                let result = transaction.execute(query.as_str(), &[&file.id]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id, md5, crc32c, or size");
            }

            Ok(())
        }

        /// Cannot TRUNCATE gdrive_files table
        #[test]
        fn test_cannot_truncate() -> Result<()> {
            let mut client = get_client();

            let mut transaction = start_transaction(&mut client)?;
            let owner_id = create_owner(&mut transaction, "me@domain4")?;
            let file = GdriveFile { id: "D".repeat(28), owner_id: Some(owner_id), md5: [0; 16], crc32c: 0, size: 1, last_probed: None };
            create_gdrive_file(&mut transaction, &file)?;
            transaction.commit()?;

            let mut transaction = start_transaction(&mut client)?;
            let result = transaction.execute("TRUNCATE gdrive_files", &[]);
            assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: truncate is forbidden");

            Ok(())
        }
    }
}
