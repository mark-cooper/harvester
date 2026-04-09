//! OAI domain model: record/repository keys, lifecycle status enums, and
//! the events that drive db transitions.

mod events;
mod record;
mod status;

use std::path::PathBuf;

pub use events::{HarvestEvent, IndexEvent, RecordAction};
pub use record::{OaiRecordId, OaiRecordImport};
pub use status::{OaiIndexStatus, OaiRecordStatus};

#[derive(Debug, Clone)]
pub struct OaiConfig {
    pub data_dir: PathBuf,
    pub repo: RepositoryKey,
    pub oai_timeout: u64,
    pub oai_retries: u32,
}

/// Identifies an (endpoint, metadata_prefix) pair — the scope under which all
/// records in `oai_records` live. Most db operations are keyed by this pair.
#[derive(Debug, Clone)]
pub struct RepositoryKey {
    pub endpoint: String,
    pub metadata_prefix: String,
}

impl RepositoryKey {
    pub fn new(endpoint: impl Into<String>, metadata_prefix: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            metadata_prefix: metadata_prefix.into(),
        }
    }

    /// Borrow this repo together with a record identifier.
    pub fn record<'a>(&'a self, identifier: &'a str) -> RecordKey<'a> {
        RecordKey {
            repo: self,
            identifier,
        }
    }
}

/// Identifies a single record within a repository. Borrowed view used by
/// per-record db operations (transitions, lookups).
#[derive(Debug, Clone, Copy)]
pub struct RecordKey<'a> {
    pub repo: &'a RepositoryKey,
    pub identifier: &'a str,
}
