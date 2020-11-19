//! terastash-like operations for manipulating the stash based on
//! a partial mirror on the local filesystem

use anyhow::{anyhow, bail};
use sqlx::{Postgres, Transaction};
use anyhow::Result;
use crate::config::Config;
use crate::db::inode::InodeId;
use crate::db::traversal;
use crate::util;

/// Resolve some local absolute path to a root directory and path components that can
/// be used to descend back to the exastash equivalent of the machine-local path
///
/// Example:
/// path_roots has /a/b -> 1
/// resolve_root_of_local_path(config, ["a", "b", "c", "d"]) -> (1, idx 2 - indicating ["c", "d"])
pub fn resolve_root_of_local_path<S: AsRef<str> + ToString + Clone>(config: &Config, path_components: &[S]) -> Result<(i64, usize)> {
    let mut idx = path_components.len();
    // Need a Vec<String> to query the HashMap, can't use &[&str]
    let mut candidate: Vec<String> = path_components
        .iter()
        .cloned()
        .map(|s| s.to_string())
        .collect();
    let path_components_joinable = candidate.clone();
    loop {
        if let Some(path_value) = config.path_roots.get(&candidate) {
            let dir_id = path_value.dir_id;
            return Ok((dir_id, idx));
        }
        if candidate.len() == 0 {
            break;
        }
        candidate.pop();
        idx -= 1;
    }
    let path = format!("/{}", path_components_joinable.join("/"));
    bail!("no entry in path_roots could serve as the base dir for {}", path);
}

/// Resolve some local absolute path to its exastash equivalent
pub async fn resolve_local_absolute_path<S: AsRef<str> + ToString + Clone>(config: &Config, transaction: &mut Transaction<'_, Postgres>, path_components: &[S]) -> Result<InodeId> {
    let (root_dir, idx) = resolve_root_of_local_path(config, path_components)?;
    traversal::resolve_inode(transaction, root_dir, &path_components[idx..]).await
}

/// Resolve some local relative path argument to normalized path components
pub fn resolve_local_path_to_path_components(path_arg: Option<&str>) -> Result<Vec<String>> {
    let mut path = std::env::current_dir()?;
    if let Some(p) = path_arg {
        path = path.join(p);
    }
    let path = util::normalize_path(&path);

    let s = path
        .to_str()
        .ok_or_else(|| anyhow!("could not convert path {:?} to UTF-8", path))?;
    Ok(util::utf8_path_to_components(s))
}

/// Resolve normalized path components to its exastash equivalent inode
pub async fn resolve_path_components<S: AsRef<str> + ToString + Clone>(config: &Config, transaction: &mut Transaction<'_, Postgres>, path_components: &[S]) -> Result<InodeId> {
    resolve_local_absolute_path(&config, transaction, path_components).await
}

/// Resolve some local relative path argument to its exastash equivalent inode
pub async fn resolve_local_path_arg(config: &Config, transaction: &mut Transaction<'_, Postgres>, path_arg: Option<&str>) -> Result<InodeId> {
    let path_components = resolve_local_path_to_path_components(path_arg)?;
    resolve_path_components(config, transaction, &path_components).await
}
