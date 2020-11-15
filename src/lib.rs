//! exastash, a filesystem for archiving petabytes

#![feature(
    async_closure,
    generators,
    proc_macro_hygiene,
    stmt_expr_attributes,
    format_args_capture,
)]

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
    variant_size_differences,
)]
#![allow(
    clippy::len_zero,
    // TODO: remove after initial development
    dead_code,
)]

pub(crate) mod conceal_size;
pub(crate) mod ranges;
pub(crate) mod retry;
pub mod util;
pub mod db;
//pub mod fuse;

pub mod path;
pub(crate) mod blake3;
pub mod config;
pub(crate) mod gdrive;
pub(crate) mod crypto;
pub mod info;
pub mod oauth;
pub mod storage_read;
pub mod storage_write;

/// Rows in database will be created with birth_version set to this value.
/// See `exastash_versions.sql`.
pub const EXASTASH_VERSION: i16 = 41;

/// exastash error
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// A directory entry was expected but could not be found
    #[error("no such dirent {basename:?} under dir {parent:?}")]
    NoDirent {
        /// The parent for the expected directory entry
        parent: i64,
        /// The basename for the expected directory entry
        basename: String,
    },
}
