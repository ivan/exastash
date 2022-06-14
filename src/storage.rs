//! Storage management

pub mod read;
pub mod write;
pub mod delete;

use std::collections::HashSet;

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
