//! Operations for printing the info for a file, dir, or symlink

use anyhow::Result;
use serde::Serialize;
use chrono::DateTime;
use chrono::Utc;
use sqlx::{Postgres, Transaction};
use crate::db::inode::{Inode, Dir, Symlink, Birth};
use crate::db::storage::{Storage, get_storage};

/// Return information about a file, dir, or symlink in JSON format
pub async fn json_info(transaction: &mut Transaction<'_, Postgres>, inode: Inode) -> Result<String> {
    #[derive(Serialize)]
    struct FileWithStorages {
        id: i64,
        mtime: DateTime<Utc>,
        birth: Birth,
        size: i64,
        executable: bool,
        storages: Vec<Storage>,
    }

    #[derive(Serialize)]
    #[serde(tag = "type")]
    enum InodeWithStorages {
        #[serde(rename = "dir")]
        Dir(Dir),
        #[serde(rename = "file")]
        File(FileWithStorages),
        #[serde(rename = "symlink")]
        Symlink(Symlink),
    }

    let inode = match inode {
        Inode::File(file) => {
            let storages = get_storage(transaction, &[file.id]).await?;
            InodeWithStorages::File(FileWithStorages {
                id: file.id,
                mtime: file.mtime,
                birth: file.birth,
                size: file.size,
                executable: file.executable,
                storages,
            })
        }
        Inode::Dir(dir) => InodeWithStorages::Dir(dir),
        Inode::Symlink(symlink) => InodeWithStorages::Symlink(symlink),
    };

    let json = serde_json::to_string_pretty(&inode)?;
    Ok(json)
}
