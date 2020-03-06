//! exastash, a filesystem for archiving petabytes

#![warn(
	nonstandard_style,
	rust_2018_compatibility,
	rust_2018_idioms,
	unused,
	macro_use_extern_crate,
	missing_copy_implementations,
	missing_debug_implementations,
	missing_docs,
	trivial_casts,
	trivial_numeric_casts,
	unused_import_braces,
	unused_lifetimes,
	variant_size_differences,
	clippy::pedantic
)]
// TODO: remove after initial development
#![allow(dead_code)]
#![allow(unused_variables)]

pub(crate) mod conceal_size;
pub(crate) mod ranges;
pub(crate) mod retry;
pub(crate) mod util;
pub mod db;
pub(crate) mod postgres;

/// Rows in database will be created with birth_version set to this value.
/// See exastash_versions.sql.
pub const EXASTASH_VERSION: i16 = 41;
