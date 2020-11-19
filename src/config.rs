//! code for loading ~/.config/exastash/*

use std::fs;
use std::convert::{TryFrom, TryInto};
use std::collections::HashMap;
use anyhow::{bail, Result};
use serde_derive::Deserialize;
use tracing::info;
use quick_js::{Context, JsValue};
use directories::ProjectDirs;
use custom_debug_derive::Debug as CustomDebug;
use crate::util::{self, elide};
use crate::storage_write::{DesiredStorage, RelevantFileMetadata};

/// A value in the [path_roots] section of config.toml
#[derive(Deserialize, Debug, PartialEq, Eq)]
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
    let raw_config = toml::from_str::<RawConfig>(&content)?;
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

impl TryFrom<JsValue> for DesiredStorage {
    type Error = anyhow::Error;

    /// Convert JS object e.g. {inline: true, gdrive: [1]} to a DesiredStorage
    fn try_from(js_obj: JsValue) -> Result<DesiredStorage> {
        let mut desired_storage = DesiredStorage { inline: false, gdrive: vec![] };

        if let JsValue::Object(map) = js_obj {
            if let Some(val) = map.get("gdrive") {
                if let JsValue::Array(gdrive_ids) = val {
                    for val in gdrive_ids {
                        if let JsValue::Int(gdrive_id) = val {
                            let gdrive_id = i16::try_from(*gdrive_id)?;
                            desired_storage.gdrive.push(gdrive_id);
                        } else {
                            bail!("newFileStorages returned an object with property \
                                   'gdrive' but some array element was not an integer");
                        }
                    }
                } else {
                    bail!("newFileStorages returned an object with property \
                           'gdrive' but value was not an array");
                }
            }
            if let Some(val) = map.get("inline") {
                if let JsValue::Bool(inline) = val {
                    desired_storage.inline = *inline;
                } else {
                    bail!("newFileStorages returned an object with property \
                           'inline' but value was not a boolean");
                }
            }
        } else {
            bail!("newFileStorages did not return an object");
        }

        Ok(desired_storage)
    }
}

/// Policy object that can be used to make decisions about file placement
#[derive(CustomDebug)]
pub struct Policy {
    #[debug(with = "elide")]
    js_context: Context,
}

impl Policy {
    /// Call policy.js's newFileStorages and convert the result to a DesiredStorage
    pub fn new_file_storages(&self, stash_path: &[&str], metadata: &RelevantFileMetadata) -> Result<DesiredStorage> {
        let mut properties: HashMap<String, JsValue> = HashMap::new();
        let stash_path_js = stash_path
            .iter()
            .map(|&s| JsValue::String(s.into()))
            .collect();
        properties.insert("stashPath".into(),  JsValue::Array(stash_path_js));
        properties.insert("size".into(),       JsValue::BigInt(metadata.size.into()));
        properties.insert("mtime".into(),      JsValue::Date(metadata.mtime));
        properties.insert("executable".into(), JsValue::Bool(metadata.executable));

        let desired_storages = self.js_context.call_function("newFileStorages", vec![JsValue::Object(properties)])?.try_into()?;
        info!("policy.js:newFileStorages returned {:?} for stash_path={:?}", desired_storages, stash_path);
        Ok(desired_storages)
    }
}

pub(crate) fn parse_policy(script: &str) -> Result<Policy> {
    let js_context = Context::builder().console(quick_js::console::LogConsole).build().unwrap();
    js_context.eval(script)?;
    Ok(Policy { js_context })
}

/// Return a Policy object that can be used to make decisions about file placement
pub fn get_policy() -> Result<Policy> {
    let project_dirs = ProjectDirs::from("", "",  "exastash").unwrap();
    let config_dir   = project_dirs.config_dir();
    let policy_file  = config_dir.join("policy.js");
    let script       = fs::read_to_string(policy_file)?;
    parse_policy(&script)
}

#[cfg(test)]
mod tests {
    use super::*;

    mod config {
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
                vec!["some".into(), "path".into()] => PathRootsValue { dir_id: 1, new_dirent_requirements: vec![] },
                vec!["other".into(), "path".into()] => PathRootsValue { dir_id: 2, new_dirent_requirements: vec![String::from("windows_compatible")] },
                vec![] => PathRootsValue { dir_id: 3, new_dirent_requirements: vec![] },
            };

            assert_eq!(config, Config { path_roots: expected_path_roots });

            Ok(())
        }
    }

    mod policy {
        use super::*;
        use chrono::Utc;

        #[test]
        fn test_parse_policy() -> Result<()> {
            let script = r#"
                function newFileStorages({ stashPath, size, mtime, executable }) {
                    return {inline: true};
                }
            "#;
            parse_policy(script)?;

            Ok(())
        }

        #[test]
        fn test_new_file_storages() -> Result<()> {
            let script = r#"
                function newFileStorages({ stashPath, size, mtime, executable }) {
                    let path = stashPath.join("/");
                    if (path.endsWith(".json")) {
                        // Not something we'd do in practice
                        return {inline: true, gdrive: [1]};
                    } else if (size > 100 || path.endsWith(".jpg")) {
                        return {gdrive: [1, 2]};
                    } else {
                        return {inline: true};
                    }
                }
            "#;
            let policy = parse_policy(script)?;

            assert_eq!(
                policy.new_file_storages(&["parent", "something.json"], &RelevantFileMetadata { size: 0, mtime: Utc::now(), executable: false })?,
                DesiredStorage { inline: true, gdrive: vec![1] }
            );

            assert_eq!(
                policy.new_file_storages(&["something.jpg"], &RelevantFileMetadata { size: 0, mtime: Utc::now(), executable: false })?,
                DesiredStorage { inline: false, gdrive: vec![1, 2] }
            );
            assert_eq!(
                policy.new_file_storages(&["something"], &RelevantFileMetadata { size: 101, mtime: Utc::now(), executable: false })?,
                DesiredStorage { inline: false, gdrive: vec![1, 2] }
            );

            assert_eq!(
                policy.new_file_storages(&["small"], &RelevantFileMetadata { size: 50, mtime: Utc::now(), executable: false })?,
                DesiredStorage { inline: true, gdrive: vec![] }
            );

            Ok(())
        }
    }
}
