use std::path::PathBuf;

use oai_pmh::client::response::Header;
use tracing::warn;

use super::status::OaiRecordStatus;

/// A record row as fetched from the DB for download, parse, index, or delete
/// work. Carries enough to locate the on-disk file (`fingerprint`) and dispatch
/// the right action (`status`).
#[derive(sqlx::FromRow)]
pub struct OaiRecord {
    pub identifier: String,
    pub fingerprint: String,
    pub status: OaiRecordStatus,
}

impl OaiRecord {
    pub fn path(&self) -> PathBuf {
        PathBuf::from(&self.fingerprint[0..2])
            .join(&self.fingerprint[2..4])
            .join(format!("{}.xml", self.fingerprint))
    }
}

/// The discovery-time projection of an OAI-PMH response header into our
/// status enum. Inserted into `oai_records` as new records are discovered.
#[derive(Debug)]
pub struct OaiHeader {
    pub identifier: String,
    pub datestamp: String,
    pub status: OaiRecordStatus,
}

impl From<Header> for OaiHeader {
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
