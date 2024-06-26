//! Storage management

pub mod read;
pub mod write;
pub mod delete;

use std::fs::Metadata;
use std::collections::HashSet;
use anyhow::Result;
use chrono::{DateTime, Utc};
use crate::util;

/// Descriptor indicating which storages should be created or deleted
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct StoragesDescriptor {
    /// A set of fofs pile ids in which to store the file
    pub fofs: HashSet<i32>,
    /// Whether to store inline in the database
    pub inline: bool,
    /// A set of google_domain ids in which to store the file
    pub gdrive: HashSet<i16>,
}

impl StoragesDescriptor {
    /// How many storages we want to store to
    pub fn len(&self) -> usize {
        let mut total = 0;
        if self.inline {
            total += 1;
        }
        total += self.fofs.len();
        total += self.gdrive.len();
        total
    }

    /// Whether we lack any storages to store to
    pub fn is_empty(&self) -> bool {
        if self.inline || !self.fofs.is_empty() || !self.gdrive.is_empty() {
            return false;
        }
        true
    }
}



/// Local file metadata that can be stored in exastash
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RelevantFileMetadata {
    /// Size of the local file in bytes
    pub size: i64,
    /// The mtime of the local file, precision only up to microseconds
    pub mtime: DateTime<Utc>,
    /// Whether the local file is executable
    pub executable: bool,
}

impl TryFrom<&Metadata> for RelevantFileMetadata {
    type Error = anyhow::Error;

    fn try_from(attr: &Metadata) -> Result<RelevantFileMetadata> {
        use std::os::unix::fs::PermissionsExt;

        // Zero out the nanoseconds so that a RelevantFileMetadata's mtime
        // can be compared directly with a timestamptz from PostgreSQL.
        let mtime = util::without_nanos(attr.modified()?.into());
        let size = attr.len() as i64;
        let permissions = attr.permissions();
        let executable = permissions.mode() & 0o100 != 0;
        Ok(RelevantFileMetadata { size, mtime, executable })
    }
}

impl TryFrom<Metadata> for RelevantFileMetadata {
    type Error = anyhow::Error;

    fn try_from(attr: Metadata) -> Result<RelevantFileMetadata> {
        (&attr).try_into()
    }
}
