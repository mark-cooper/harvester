use std::fmt;

use super::status::OaiRecordStatus;

/// Events that drive single-record `oai_records.status` transitions.
///
/// Batch operations (e.g. retry of all failed records) live as standalone
/// db functions rather than enum variants.
#[derive(Debug)]
pub enum HarvestEvent<'a> {
    DownloadSucceeded,
    DownloadFailed { message: &'a str },
    MetadataExtracted { metadata: serde_json::Value },
    MetadataFailed { message: &'a str },
}

/// Events that drive single-record `oai_records.index_status` transitions.
///
/// Batch operations (e.g. reindex of a whole repository) live as standalone
/// db functions rather than enum variants.
#[derive(Debug)]
pub enum IndexEvent<'a> {
    IndexSucceeded,
    IndexFailed { message: &'a str },
    PurgeSucceeded,
    PurgeFailed { message: &'a str },
}

/// Whether a fetched index candidate should be sent to the index (parsed
/// records) or purged from it (deleted records).
#[derive(Debug, Clone, Copy)]
pub enum RecordAction {
    Index,
    Delete,
}

impl RecordAction {
    /// Map a record's harvest status to the index action it implies.
    /// Panics on other statuses — the indexer's fetch query is responsible
    /// for filtering to only `Parsed` and `Deleted`.
    pub fn for_status(status: OaiRecordStatus) -> Self {
        match status {
            OaiRecordStatus::Parsed => Self::Index,
            OaiRecordStatus::Deleted => Self::Delete,
            _ => unreachable!("only parsed and deleted records should be fetched for indexing"),
        }
    }

    pub fn success_event(self) -> IndexEvent<'static> {
        match self {
            Self::Index => IndexEvent::IndexSucceeded,
            Self::Delete => IndexEvent::PurgeSucceeded,
        }
    }

    pub fn failure_event(self, message: &str) -> IndexEvent<'_> {
        match self {
            Self::Index => IndexEvent::IndexFailed { message },
            Self::Delete => IndexEvent::PurgeFailed { message },
        }
    }
}

impl fmt::Display for RecordAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Index => "index",
            Self::Delete => "delete",
        })
    }
}
