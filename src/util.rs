use std::env;
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

/// chrono::Utc::now() but with the nanoseconds rounded off to microsecond
/// precision, suitable for round-tripping through PostgreSQL's timestamptz.
pub(crate) fn now_no_nanos() -> DateTime<Utc> {
    let dt = Utc::now();
    let new_nanos = 1000 * (dt.timestamp_subsec_nanos() / 1000);
    assert_eq!(new_nanos % 1000, 0);
    dt.with_nanosecond(new_nanos).unwrap()
}

// Copied from https://github.com/qryxip/snowchains/blob/dcd76c1dbb87eea239ba17f28b44ee11fdd3fd80/src/macros.rs
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
