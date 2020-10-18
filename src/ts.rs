use std::fs;
use std::collections::HashMap;
use serde_derive::Deserialize;
use anyhow::Result;

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
