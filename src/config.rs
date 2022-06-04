//! code for loading ~/.config/exastash/config.toml

use std::fs;
use std::collections::HashMap;
use anyhow::Result;
use serde_derive::Deserialize;
use directories::ProjectDirs;
use crate::util;

/// A value in the [path_roots] section of config.toml
#[derive(Clone, Deserialize, Debug, PartialEq, Eq)]
pub struct PathRootsValue {
    /// The dir_id to use as the root at this path
    pub dir_id: i64,
    /// A list of additional requirements to impose on only _new_ dirents
    #[serde(default)]
    pub new_dirent_requirements: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct RawConfig {
    /// A map of local paths -> PathRootsValue containing a dir_id to use as the root
    path_roots: HashMap<String, PathRootsValue>,
}

/// Machine-local exastash configuration
#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct Config {
    /// map of path components -> dir id
    pub path_roots: HashMap<Vec<String>, PathRootsValue>,
}

impl From<RawConfig> for Config {
    fn from(raw_config: RawConfig) -> Self {
        Config {
            path_roots: raw_config.path_roots
                .into_iter()
                .map(|(k, v)| (util::utf8_path_to_components(&k), v))
                .collect()
        }
    }
}

/// Return a Config parsed from a string containing toml configuration
fn parse_config(content: &str) -> Result<Config> {
    let raw_config = toml::from_str::<RawConfig>(content)?;
    let config     = raw_config.into();
    Ok(config)
}

/// Return the machine-local exastash configuration
pub fn get_config() -> Result<Config> {
    let project_dirs = ProjectDirs::from("", "",  "exastash").unwrap();
    let config_dir   = project_dirs.config_dir();
    let config_file  = config_dir.join("config.toml");
    let content      = fs::read_to_string(config_file)?;
    parse_config(&content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use literally::hmap;

    #[test]
    fn test_parse_config() -> Result<()> {
        let config = parse_config(r#"
            [path_roots]
            "/some/path" = { dir_id = 1 }
            "/other/path" = { dir_id = 2, new_dirent_requirements = ["windows_compatible"] }
            # Not a good idea, but test the parse
            "/" = { dir_id = 3 }
        "#)?;

        let expected_path_roots = hmap!{
            vec!["some".into(),  "path".into()] => PathRootsValue { dir_id: 1, new_dirent_requirements: vec![] },
            vec!["other".into(), "path".into()] => PathRootsValue { dir_id: 2, new_dirent_requirements: vec![String::from("windows_compatible")] },
            vec![] => PathRootsValue { dir_id: 3, new_dirent_requirements: vec![] },
        };

        assert_eq!(config, Config { path_roots: expected_path_roots });

        Ok(())
    }
}
