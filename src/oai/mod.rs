//! OAI domain model: scope, lifecycle status enums, record types, and the
//! events that drive db transitions.

mod events;
mod record;
mod status;

use std::path::PathBuf;

pub use events::{HarvestEvent, IndexEvent, RecordAction};
pub use record::{OaiHeader, OaiRecord};
pub use status::{OaiIndexStatus, OaiRecordStatus};

#[derive(Debug, Clone)]
pub struct OaiConfig {
    pub data_dir: PathBuf,
    pub scope: OaiScope,
    pub oai_timeout: u64,
    pub oai_retries: u32,
}

/// Identifies an (endpoint, metadata_prefix) pair — the scope under which all
/// records in `oai_records` live. Most db operations are keyed by this pair.
#[derive(Debug, Clone)]
pub struct OaiScope {
    pub endpoint: String,
    pub metadata_prefix: String,
}

impl OaiScope {
    pub fn new(endpoint: impl Into<String>, metadata_prefix: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            metadata_prefix: metadata_prefix.into(),
        }
    }
}
