use std::path::PathBuf;

use oai_pmh::client::response::Header;

#[derive(Debug, Clone)]
pub struct OaiConfig {
    pub data_dir: PathBuf,
    pub endpoint: String,
    pub metadata_prefix: String,
}

/// Record info needed for download and metadata extraction
#[derive(sqlx::FromRow)]
pub struct OaiRecordId {
    pub identifier: String,
    pub fingerprint: String,
}

impl OaiRecordId {
    pub fn path(&self) -> PathBuf {
        PathBuf::from(&self.fingerprint[0..2])
            .join(&self.fingerprint[2..4])
            .join(format!("{}.xml", self.fingerprint))
    }
}

/// Record info needed for import
#[derive(Debug)]
pub struct OaiRecordImport {
    pub identifier: String,
    pub datestamp: String,
    pub status: String,
}

impl OaiRecordImport {
    pub fn new(identifier: String, datestamp: String, status: String) -> Self {
        Self {
            identifier,
            datestamp,
            status,
        }
    }
}

impl From<Header> for OaiRecordImport {
    fn from(value: Header) -> Self {
        Self::new(
            value.identifier,
            value.datestamp,
            value
                .status
                .unwrap_or_else(|| OaiRecordStatus::Pending.to_string()),
        )
    }
}

/// Harvest lifecycle states for `oai_records.status`.
///
/// Expected transitions:
/// - import: `* -> pending|deleted` for changed records (`failed` records are intentionally sticky)
/// - download: `pending -> available|failed`
/// - metadata: `available -> parsed|failed`
///
/// Index lifecycle ownership:
/// - metadata success also resets index lifecycle (`index_status -> pending`)
/// - import of deleted records also requeues index lifecycle (`index_status -> pending`)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OaiRecordStatus {
    Available,
    Deleted,
    Failed,
    Parsed,
    Pending,
}

impl OaiRecordStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OaiRecordStatus::Available => "available",
            OaiRecordStatus::Deleted => "deleted",
            OaiRecordStatus::Failed => "failed",
            OaiRecordStatus::Parsed => "parsed",
            OaiRecordStatus::Pending => "pending",
        }
    }
}

impl std::fmt::Display for OaiRecordStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Index lifecycle states for `oai_records.index_status`.
///
/// Expected transitions:
/// - metadata success: `* -> pending` when a record becomes `parsed`
/// - import deleted: `* -> pending` when a record becomes `deleted`
/// - index run: `pending|index_failed -> indexed|index_failed`
/// - purge run: `pending|purge_failed -> purged|purge_failed`
/// - CLI reindex: `* -> pending` for matching `parsed|deleted` records
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OaiIndexStatus {
    IndexFailed,
    Indexed,
    Pending,
    Purged,
    PurgeFailed,
}

impl OaiIndexStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OaiIndexStatus::IndexFailed => "index_failed",
            OaiIndexStatus::Indexed => "indexed",
            OaiIndexStatus::Pending => "pending",
            OaiIndexStatus::Purged => "purged",
            OaiIndexStatus::PurgeFailed => "purge_failed",
        }
    }
}

impl std::fmt::Display for OaiIndexStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
