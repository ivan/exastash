//! terastash-like operations for manipulating the stash based on
//! a partial mirror on the local filesystem

use anyhow::{anyhow, bail};
use std::fs;
use std::collections::HashMap;
use sqlx::{Postgres, Transaction};
use serde_derive::Deserialize;
use anyhow::Result;
use directories::ProjectDirs;
use crate::db::inode::InodeId;
use crate::db::traversal::walk_path;
use crate::util;

#[derive(Deserialize, Debug)]
struct RawConfig {
    /// map of paths -> dir id
    ts_paths: HashMap<String, i64>,
}

/// Machine-local exastash configuration
#[derive(Deserialize, Debug)]
pub struct Config {
    /// map of path components -> dir id
    ts_paths: HashMap<Vec<String>, i64>,
}

fn utf8_path_to_components(path: &str) -> Vec<String> {
    assert!(path.starts_with('/'));
    path
        .split('/')
        .skip(1)
        .map(String::from)
        .collect()
}

impl From<RawConfig> for Config {
    fn from(raw_config: RawConfig) -> Self {
        Config {
            ts_paths: raw_config.ts_paths
                .into_iter()
                .map(|(k, v)| (utf8_path_to_components(&k), v))
                .collect()
        }
    }
}

/// Return the machine-local exastash configuration
pub fn get_config() -> Result<Config> {
    let project_dirs = ProjectDirs::from("", "",  "exastash").unwrap();
    let config_dir = project_dirs.config_dir();
    let config_file = config_dir.join("config.toml");
    let bytes = fs::read_to_string(config_file)?;
    let raw_config: RawConfig = toml::from_str(&bytes)?;
    let config = raw_config.into();
    Ok(config)
}

/// Resolve some local absolute path to a root directory and path components that can
/// be used to descend back to the exastash equivalent of the machine-local path
///
/// Example:
/// ts_paths has /a/b -> 1
/// resolve_root_of_local_path(config, ["a", "b", "c", "d"]) -> (1, ["c", "d"])
pub fn resolve_root_of_local_path<'a>(config: &Config, path_components: &'a [&'a str]) -> Result<(i64, &'a [&'a str])> {
    let mut idx = path_components.len();
    // Need a Vec<String> to query the HashMap, can't use &[&str]
    let mut candidate: Vec<String> = path_components
        .into_iter()
        .cloned()
        .map(String::from)
        .collect();
    loop {
        if let Some(dir_id) = config.ts_paths.get(&candidate) {
            return Ok((*dir_id, &path_components[idx..]));
        }
        if candidate.len() == 0 {
            break;
        }
        candidate.pop();
        idx -= 1;
    }
    let path = format!("/{}", path_components.join("/"));
    bail!("no entry in ts_paths could serve as the base dir for {}", path);
}

/// Resolve some local absolute path to its exastash equivalent
pub async fn resolve_local_absolute_path(config: &Config, transaction: &mut Transaction<'_, Postgres>, path_components: &[&str]) -> Result<InodeId> {
    let (root_dir, components) = resolve_root_of_local_path(config, path_components)?;
    walk_path(transaction, root_dir, components).await
}

/// Resolve some local relative path argument to its exastash equivalent
pub async fn resolve_local_path_arg(config: &Config, transaction: &mut Transaction<'_, Postgres>, path_arg: Option<&str>) -> Result<InodeId> {
    let mut path = std::env::current_dir()?;
    if let Some(p) = path_arg {
        path = path.join(p);
    }
    let path = util::normalize_path(&path);

    let s = path
        .to_str()
        .ok_or_else(|| anyhow!("could not convert path {:?} to UTF-8", path))?;
    assert!(s.starts_with('/'));
    let path_components: Vec<&str> =
        s
        .split('/')
        .skip(1)
        .collect();

    let inode_id = resolve_local_absolute_path(&config, transaction, &path_components).await?;
    Ok(inode_id)
}