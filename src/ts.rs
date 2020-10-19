//! terastash-like operations for manipulating the stash based on
//! a partial mirror on the local filesystem

use anyhow::bail;
use std::fs;
use std::collections::HashMap;
use sqlx::{Postgres, Transaction};
use serde_derive::Deserialize;
use serde::de::{Deserialize, Deserializer};
use anyhow::Result;
use directories::ProjectDirs;
use crate::db::inode::InodeId;
use crate::db::traversal::walk_path;

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

/// Resolve some local path to a root directory and path components that can
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
    bail!("no entry in ts_paths could serve as the base dir for #{:?}", path_components);
}

/// Resolve some local path to its exastash equivalent
pub async fn resolve_local_path(config: &Config, transaction: &mut Transaction<'_, Postgres>, path_components: &[&str]) -> Result<InodeId> {
    let (root_dir, components) = resolve_root_of_local_path(config, path_components)?;
    walk_path(transaction, root_dir, components).await
}
