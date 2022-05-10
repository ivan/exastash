//! Operations for printing the info for a file, dir, or symlink

use anyhow::Result;
use serde::Serialize;
use chrono::DateTime;
use chrono::Utc;
use serde_hex::{SerHexOpt, Strict};
use crate::db::inode::{Inode, Dir, Symlink, Birth};
use crate::db::storage::{Storage, get_storages};

#[derive(Serialize)]
struct FileWithStorages<'a> {
    id: i64,
    mtime: DateTime<Utc>,
    birth: &'a Birth,
    size: i64,
    executable: bool,
    storages: Vec<Storage>,
    #[serde(with = "SerHexOpt::<Strict>")]
    b3sum: Option<[u8; 32]>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum InodeWithStorages<'a> {
    #[serde(rename = "dir")]
    Dir(&'a Dir),
    #[serde(rename = "file")]
    File(&'a FileWithStorages<'a>),
    #[serde(rename = "symlink")]
    Symlink(&'a Symlink),
}

/// Return information about a file, dir, or symlink in JSON format
pub async fn json_info(inode: &Inode) -> Result<String> {
    let fws;
    let inode = match inode {
        Inode::File(file) => {
            let storages = get_storages(&[file.id]).await?;
            fws = FileWithStorages {
                id: file.id,
                mtime: file.mtime,
                birth: &file.birth,
                size: file.size,
                executable: file.executable,
                storages,
                b3sum: file.b3sum,
            };
            InodeWithStorages::File(&fws)
        }
        Inode::Dir(dir) => InodeWithStorages::Dir(dir),
        Inode::Symlink(symlink) => InodeWithStorages::Symlink(symlink),
    };

    let json = serde_json::to_string_pretty(&inode)?;
    Ok(json)
}
