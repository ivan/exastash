// TODO: remove after initial development
#![allow(dead_code)]
#![allow(unused_variables)]

pub(crate) mod conceal_size;
pub(crate) mod ranges;
pub(crate) mod retry;
pub(crate) mod util;
pub(crate) mod db;
pub(crate) mod postgres;

/// Rows in database will be created with birth_version set to this value.
/// See exastash_versions.sql.
pub(crate) const EXASTASH_VERSION: i16 = 41;
