use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use oai_pmh::client::response::Header;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct OaiConfig {
    pub data_dir: PathBuf,
    pub endpoint: String,
    pub metadata_prefix: String,
    pub oai_timeout: u64,
    pub oai_retries: u32,
}

/// Record info needed for download and metadata extraction
#[derive(sqlx::FromRow)]
pub struct OaiRecordId {
    pub identifier: String,
    pub fingerprint: String,
    pub status: OaiRecordStatus,
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
    pub status: OaiRecordStatus,
}

impl From<Header> for OaiRecordImport {
    fn from(value: Header) -> Self {
        let status = match value.status.as_deref() {
            Some("deleted") => OaiRecordStatus::Deleted,
            Some(other) => {
                warn!(
                    identifier = %value.identifier,
                    header_status = %other,
                    "Unexpected OAI header status; defaulting to pending"
                );
                OaiRecordStatus::Pending
            }
            _ => OaiRecordStatus::Pending,
        };
        Self {
            identifier: value.identifier,
            datestamp: value.datestamp,
            status,
        }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
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

impl FromStr for OaiRecordStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "available" => Ok(OaiRecordStatus::Available),
            "deleted" => Ok(OaiRecordStatus::Deleted),
            "failed" => Ok(OaiRecordStatus::Failed),
            "parsed" => Ok(OaiRecordStatus::Parsed),
            "pending" => Ok(OaiRecordStatus::Pending),
            _ => Err(format!("unknown OAI record status: {value}")),
        }
    }
}

impl fmt::Display for OaiRecordStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl fmt::Display for OaiIndexStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Events that drive `oai_records.status` transitions.
#[derive(Debug)]
pub enum HarvestEvent<'a> {
    DownloadSucceeded,
    DownloadFailed { message: &'a str },
    MetadataExtracted { metadata: serde_json::Value },
    MetadataFailed { message: &'a str },
    HarvestRetryRequested,
}

impl HarvestEvent<'_> {
    pub fn transition(&self) -> HarvestTransition {
        match self {
            Self::DownloadSucceeded => HarvestTransition {
                from: OaiRecordStatus::Pending,
                to: OaiRecordStatus::Available,
            },
            Self::DownloadFailed { .. } => HarvestTransition {
                from: OaiRecordStatus::Pending,
                to: OaiRecordStatus::Failed,
            },
            Self::MetadataExtracted { .. } => HarvestTransition {
                from: OaiRecordStatus::Available,
                to: OaiRecordStatus::Parsed,
            },
            Self::MetadataFailed { .. } => HarvestTransition {
                from: OaiRecordStatus::Available,
                to: OaiRecordStatus::Failed,
            },
            Self::HarvestRetryRequested => HarvestTransition {
                from: OaiRecordStatus::Failed,
                to: OaiRecordStatus::Pending,
            },
        }
    }
}

pub struct HarvestTransition {
    pub from: OaiRecordStatus,
    pub to: OaiRecordStatus,
}

/// Events that drive `oai_records.index_status` transitions.
pub enum IndexEvent<'a> {
    IndexSucceeded,
    IndexFailed { message: &'a str },
    PurgeSucceeded,
    PurgeFailed { message: &'a str },
    ReindexRequested,
}

impl IndexEvent<'_> {
    pub fn transition(&self) -> IndexTransition {
        match self {
            Self::IndexSucceeded => IndexTransition::SingleRecord {
                required_status: OaiRecordStatus::Parsed,
                from: (OaiIndexStatus::Pending, OaiIndexStatus::IndexFailed),
                to: OaiIndexStatus::Indexed,
            },
            Self::IndexFailed { .. } => IndexTransition::SingleRecord {
                required_status: OaiRecordStatus::Parsed,
                from: (OaiIndexStatus::Pending, OaiIndexStatus::IndexFailed),
                to: OaiIndexStatus::IndexFailed,
            },
            Self::PurgeSucceeded => IndexTransition::SingleRecord {
                required_status: OaiRecordStatus::Deleted,
                from: (OaiIndexStatus::Pending, OaiIndexStatus::PurgeFailed),
                to: OaiIndexStatus::Purged,
            },
            Self::PurgeFailed { .. } => IndexTransition::SingleRecord {
                required_status: OaiRecordStatus::Deleted,
                from: (OaiIndexStatus::Pending, OaiIndexStatus::PurgeFailed),
                to: OaiIndexStatus::PurgeFailed,
            },
            Self::ReindexRequested => IndexTransition::BatchReset {
                eligible_record_statuses: &[OaiRecordStatus::Parsed, OaiRecordStatus::Deleted],
                to: OaiIndexStatus::Pending,
            },
        }
    }
}

pub enum IndexTransition {
    /// Single-record: requires a specific record status, accepts either of two
    /// index predecessor states, transitions to `to`.
    SingleRecord {
        required_status: OaiRecordStatus,
        from: (OaiIndexStatus, OaiIndexStatus),
        to: OaiIndexStatus,
    },
    /// Batch reset: matches records whose status is in `eligible_record_statuses`,
    /// resets index_status to `to` regardless of current index_status.
    BatchReset {
        eligible_record_statuses: &'static [OaiRecordStatus],
        to: OaiIndexStatus,
    },
}

/// Compile-time check: adding a new OaiRecordStatus variant without updating
/// transitions causes a non-exhaustive match error here.
const fn _assert_status_coverage() {
    let statuses = [
        OaiRecordStatus::Available,
        OaiRecordStatus::Deleted,
        OaiRecordStatus::Failed,
        OaiRecordStatus::Parsed,
        OaiRecordStatus::Pending,
    ];
    let mut i = 0;
    while i < statuses.len() {
        match statuses[i] {
            OaiRecordStatus::Available => {}
            OaiRecordStatus::Deleted => {}
            OaiRecordStatus::Failed => {}
            OaiRecordStatus::Parsed => {}
            OaiRecordStatus::Pending => {}
        }
        i += 1;
    }
}

/// Compile-time check: adding a new OaiIndexStatus variant without updating
/// transitions causes a non-exhaustive match error here.
const fn _assert_index_status_coverage() {
    let statuses = [
        OaiIndexStatus::IndexFailed,
        OaiIndexStatus::Indexed,
        OaiIndexStatus::Pending,
        OaiIndexStatus::Purged,
        OaiIndexStatus::PurgeFailed,
    ];
    let mut i = 0;
    while i < statuses.len() {
        match statuses[i] {
            OaiIndexStatus::IndexFailed => {}
            OaiIndexStatus::Indexed => {}
            OaiIndexStatus::Pending => {}
            OaiIndexStatus::Purged => {}
            OaiIndexStatus::PurgeFailed => {}
        }
        i += 1;
    }
}
