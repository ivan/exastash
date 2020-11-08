//! code for loading ~/.config/exastash/*

use std::fs;
use std::convert::{TryFrom, TryInto};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use anyhow::{bail, Result};
use serde_derive::Deserialize;
use quick_js::{Context, JsValue};
use directories::ProjectDirs;
use custom_debug_derive::Debug as CustomDebug;
use crate::util::elide;

#[derive(Deserialize, Debug)]
struct RawConfig {
    /// map of paths -> dir id
    ts_paths: HashMap<String, i64>,
}

/// Machine-local exastash configuration
#[derive(Deserialize, Debug)]
pub struct Config {
    /// map of path components -> dir id
    pub ts_paths: HashMap<Vec<String>, i64>,
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

struct DesiredStorage {
    pub inline: bool,
    pub gdrive: Vec<i16>,
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
    fn new_file_storages(&self, stash_path: &str, size: i64, mtime: DateTime<Utc>, executable: bool) -> Result<DesiredStorage> {
        let mut properties: HashMap<String, JsValue> = HashMap::new();
        properties.insert("stashPath".into(),  JsValue::String(stash_path.into()));
        properties.insert("size".into(),       JsValue::BigInt(size.into()));
        properties.insert("mtime".into(),      JsValue::Date(mtime));
        properties.insert("executable".into(), JsValue::Bool(executable));

        self.js_context.call_function("newFileStorages", vec![JsValue::Object(properties)])?.try_into()
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

    #[test]
    fn test_parse_policy() -> Result<()> {
        let script = r#"
            function newFileStorages(stashPath, fileSize) {
                return {inline: true};
            }
        "#;
        parse_policy(script)?;

        Ok(())
    }
}
