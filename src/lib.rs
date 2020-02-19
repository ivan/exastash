// TODO: remove after initial development
#![allow(dead_code)]
#![allow(unused_variables)]

pub mod conceal_size;
pub mod ranges;
pub mod retry;
pub mod db;

/// Rows in database will be created with birth_version set to this value.
/// See exastash_versions.sql.
pub(crate) const EXASTASH_VERSION: u16 = 41;
