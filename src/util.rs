//! Utility functions

use std::env;
use std::fmt;
use std::path::{Path, PathBuf};
use std::path::Component;
use anyhow::{anyhow, Result, Context};
use chrono::{DateTime, Utc, Timelike};

pub(crate) fn env_var(var: &str) -> Result<String> {
    env::var(var).with_context(|| anyhow!("Could not get variable {:?} from environment", var))
}

pub(crate) fn get_hostname() -> String {
    let os_string = gethostname::gethostname();
    let hostname = os_string.to_str().expect("hostname on this machine was not valid UTF-8");
    hostname.to_owned()
}

/// Make a chrono::Utc merely microsecond-precise, making it suitable for
/// round-tripping through PostgreSQL's timestamptz.
pub(crate) fn without_nanos<T: chrono::TimeZone>(dt: DateTime<T>) -> DateTime<T> {
    let new_nanos = 1000 * (dt.timestamp_subsec_nanos() / 1000);
    assert_eq!(new_nanos % 1000, 0);
    dt.with_nanosecond(new_nanos).unwrap()
}

/// chrono::Utc::now() but with the nanoseconds rounded off to microsecond
/// precision, suitable for round-tripping through PostgreSQL's timestamptz.
pub(crate) fn now_no_nanos() -> DateTime<Utc> {
    without_nanos(Utc::now())
}

// Copied from https://github.com/qryxip/snowchains/blob/dcd76c1dbb87eea239ba17f28b44ee11fdd3fd80/src/macros.rs

/// Return a Lazy<Regex> for the given regexp string
#[macro_export]
macro_rules! lazy_regex {
    ($expr:expr) => {{
        static REGEX: ::once_cell::sync::Lazy<::regex::Regex> =
            ::once_cell::sync::Lazy::new(|| ::regex::Regex::new($expr).unwrap());
        &REGEX
    }};
    ($expr:expr,) => {
        lazy_regex!($expr)
    };
}

// Copied from https://github.com/rust-lang/cargo/blob/af64bd644982cc43b231fb39d7e19f697ec8680d/src/cargo/util/paths.rs#L61

/// Like `std::path::Path::canonicalize`, but don't actually check for
/// the existence of anything on the filesystem
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

/// Convert an absolute path to a Vec of String path components
pub(crate) fn utf8_path_to_components(path: &str) -> Vec<String> {
    assert!(path.starts_with('/'));
    let mut parts: Vec<String> = path
        .split('/')
        .skip(1)
        .map(String::from)
        .collect();
    if parts.get(0).unwrap() == "" {
        parts.pop();
    }
    parts
}

// For use with custom_debug_derive::Debug + #[debug(with = "elide")]
#[inline]
pub(crate) fn elide<T>(_: &T, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "...")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_without_nanos() {
        let dt000_000 = chrono::DateTime::parse_from_rfc3339("1996-12-19T16:39:57.000000000+00:00").unwrap();
        let dt001_000 = chrono::DateTime::parse_from_rfc3339("1996-12-19T16:39:57.000001000+00:00").unwrap();
        let dt001_999 = chrono::DateTime::parse_from_rfc3339("1996-12-19T16:39:57.000001999+00:00").unwrap();
        assert_ne!(dt000_000, dt001_000);
        assert_ne!(dt001_000, dt001_999);

        assert_eq!(without_nanos(dt001_999), dt001_000);
        assert_eq!(without_nanos(dt001_000), dt001_000);
    }
}
