//! code for loading ~/.config/exastash/policy.js

use std::fs;
use std::collections::{HashMap, HashSet};
use anyhow::{bail, Result};
use tracing::info;
use quick_js::{Context, JsValue};
use directories::ProjectDirs;
use custom_debug_derive::Debug as CustomDebug;
use crate::util::elide;
use crate::storage::write::{StoragesDescriptor, RelevantFileMetadata};

impl TryFrom<JsValue> for StoragesDescriptor {
    type Error = anyhow::Error;

    /// Convert JS object e.g. {inline: true, gdrive: [1]} to a StoragesDescriptor
    fn try_from(js_obj: JsValue) -> Result<StoragesDescriptor> {
        let mut desired_storage = StoragesDescriptor { inline: false, fofs: HashSet::new(), gdrive: HashSet::new() };

        if let JsValue::Object(map) = js_obj {
            if let Some(val) = map.get("inline") {
                if let JsValue::Bool(inline) = val {
                    desired_storage.inline = *inline;
                } else {
                    bail!("new_file_storages returned an object with property \
                           'inline' but value was not a boolean");
                }
            }
            if let Some(val) = map.get("fofs") {
                if let JsValue::Array(fofs_ids) = val {
                    for val in fofs_ids {
                        if let JsValue::Int(fofs_pile_id) = val {
                            desired_storage.fofs.insert(*fofs_pile_id);
                        } else {
                            bail!("new_file_storages returned an object with property \
                                   'fofs' but some array element was not an integer");
                        }
                    }
                } else {
                    bail!("new_file_storages returned an object with property \
                           'fofs' but value was not an array");
                }
            }
            if let Some(val) = map.get("gdrive") {
                if let JsValue::Array(gdrive_ids) = val {
                    for val in gdrive_ids {
                        if let JsValue::Int(gdrive_id) = val {
                            let gdrive_id = i16::try_from(*gdrive_id)?;
                            desired_storage.gdrive.insert(gdrive_id);
                        } else {
                            bail!("new_file_storages returned an object with property \
                                   'gdrive' but some array element was not an integer");
                        }
                    }
                } else {
                    bail!("new_file_storages returned an object with property \
                           'gdrive' but value was not an array");
                }
            }
        } else {
            bail!("new_file_storages did not return an object");
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
    /// Call policy.js's `new_file_storages` and convert the result to a `StoragesDescriptor`.
    /// These are the storages into which the new file should be stored.
    pub fn new_file_storages(&self, stash_path: &[&str], metadata: &RelevantFileMetadata) -> Result<StoragesDescriptor> {
        let mut properties: HashMap<String, JsValue> = HashMap::new();
        let stash_path_js = stash_path
            .iter()
            .map(|&s| JsValue::String(s.into()))
            .collect();
        properties.insert("stash_path".into(), JsValue::Array(stash_path_js));
        properties.insert("size".into(),       JsValue::BigInt(metadata.size.into()));
        properties.insert("mtime".into(),      JsValue::Date(metadata.mtime));
        properties.insert("executable".into(), JsValue::Bool(metadata.executable));

        let args = vec![JsValue::Object(properties)];
        let desired_storages = self.js_context.call_function("new_file_storages", args)?.try_into()?;
        info!("policy.js:new_file_storages returned {:?} for stash_path={:?}", desired_storages, stash_path);
        Ok(desired_storages)
    }

    /// Call policy.js's `fofs_base_url` and convert the result to a `String`.
    /// The string is the URL at which a remote (i.e. not on localhost) fofs pile is reachable
    pub fn fofs_base_url(&self, pile_hostname: &str) -> Result<String> {
        let args = vec![JsValue::String(pile_hostname.to_string())];
        let base_url = self.js_context.call_function("fofs_base_url", args)?.try_into()?;
        Ok(base_url)
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
    use literally::hset;
    use chrono::Utc;

    #[test]
    fn test_parse_policy() -> Result<()> {
        let script = r#"
            function new_file_storages({ stash_path, size, mtime, executable }) {
                return {inline: true};
            }
        "#;
        parse_policy(script)?;

        Ok(())
    }

    #[test]
    fn test_new_file_storages() -> Result<()> {
        let script = r#"
            function new_file_storages({ stash_path, size, mtime, executable }) {
                let path = stash_path.join("/");
                if (path.endsWith(".json")) {
                    // Not something we'd do in practice
                    return {inline: true, gdrive: [1], fofs: [2]};
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
            StoragesDescriptor { inline: true, fofs: hset![2], gdrive: hset![1_i16] }
        );

        assert_eq!(
            policy.new_file_storages(&["something.jpg"], &RelevantFileMetadata { size: 0, mtime: Utc::now(), executable: false })?,
            StoragesDescriptor { inline: false, fofs: hset![], gdrive: hset![1_i16, 2_i16] }
        );
        assert_eq!(
            policy.new_file_storages(&["something"], &RelevantFileMetadata { size: 101, mtime: Utc::now(), executable: false })?,
            StoragesDescriptor { inline: false, fofs: hset![], gdrive: hset![1_i16, 2_i16] }
        );
        assert_eq!(
            policy.new_file_storages(&["第四十七集 动漫 怪物弹珠二０十六 (中文简体字幕)-qD8VHZ3lxBw.webm"], &RelevantFileMetadata { size: 101, mtime: Utc::now(), executable: false })?,
            StoragesDescriptor { inline: false, fofs: hset![], gdrive: hset![1_i16, 2_i16] }
        );
        assert_eq!(
            policy.new_file_storages(&["Sam Needham 'Life is a Journey' - Crankworx Whistler Deep Summer Photo Challenge 2015-WVA3QDiy7Bc.jpg"], &RelevantFileMetadata { size: 0, mtime: Utc::now(), executable: false })?,
            StoragesDescriptor { inline: false, fofs: hset![], gdrive: hset![1_i16, 2_i16] }
        );

        assert_eq!(
            policy.new_file_storages(&["small"], &RelevantFileMetadata { size: 50, mtime: Utc::now(), executable: false })?,
            StoragesDescriptor { inline: true, fofs: hset![], gdrive: hset![] }
        );

        Ok(())
    }

    #[test]
    fn test_fofs_base_url() -> Result<()> {
        let script = r#"
            function fofs_base_url(pile_hostname) {
                return `http://${pile_hostname}.wg:31415`;
            }
        "#;
        let policy = parse_policy(script)?;
        assert_eq!(policy.fofs_base_url("somehost")?, String::from("http://somehost.wg:31415"));

        Ok(())
    }
}
