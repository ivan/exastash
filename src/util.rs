use std::env;
use anyhow::{anyhow, Result, Context};

pub(crate) fn env_var(var: &str) -> Result<String> {
    env::var(var).with_context(|| anyhow!("Could not get variable {:?} from environment", var))
}

pub(crate) fn get_hostname() -> String {
    let os_string = gethostname::gethostname();
    let hostname = os_string.to_str().expect("hostname on this machine was not valid UTF-8");
    hostname.to_owned()
}
