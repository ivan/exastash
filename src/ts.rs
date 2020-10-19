use std::fs;
use std::collections::HashMap;
use sqlx::{Postgres, Transaction};
use serde_derive::Deserialize;
use anyhow::Result;
use crate::db::inode::InodeId;
use crate::db::traversal::walk_path;

/// Machine-local exastash configuration
#[derive(Deserialize, Debug)]
pub struct Config {
    ts_paths: HashMap<String, i64>,
}

/// Return the machine-local exastash configuration
pub fn get_config() -> Result<Config> {
    use directories::ProjectDirs;
    let project_dirs = ProjectDirs::from("", "",  "exastash").unwrap();
    let config_dir   = project_dirs.config_dir();
    let config_file  = config_dir.join("config.toml");
    let bytes        = fs::read_to_string(config_file)?;
    let config       = toml::from_str(&bytes)?;
    Ok(config)
}


/// Resolve some local path to a root directory and path components that can
/// be used to descend back to the exastash equivalent of the machine-local path
pub fn resolve_root_of_local_path(config: &Config, path: &str) -> Result<(i64, Vec<String>)> {
    Ok((0, vec![]))
    // get 'up variants' of path until we find a match in config
    // if no match, return None
}

/// Resolve some local path to its exastash equivalent
pub async fn resolve_local_path(config: &Config, transaction: &mut Transaction<'_, Postgres>, path: &str) -> Result<InodeId> {
    let (root_dir, components) = resolve_root_of_local_path(config, path)?;
    let path_components: Vec<&str> = components.iter().map(String::as_str).collect();
    walk_path(transaction, root_dir, &path_components).await
}
