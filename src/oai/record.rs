use std::path::PathBuf;

use oai_pmh::client::response::Header;
use tracing::warn;

use super::status::OaiRecordStatus;

/// Record info needed for download and metadata extraction.
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

/// Record info needed for import.
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
