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
                .unwrap_or_else(|| OaiRecordStatus::PENDING.to_string()),
        )
    }
}

pub enum OaiRecordStatus {
    AVAILABLE,
    DELETED,
    FAILED,
    INDEXED,
    PARSED,
    PENDING,
}

impl OaiRecordStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OaiRecordStatus::AVAILABLE => "available",
            OaiRecordStatus::DELETED => "deleted",
            OaiRecordStatus::FAILED => "failed",
            OaiRecordStatus::INDEXED => "indexed",
            OaiRecordStatus::PARSED => "parsed",
            OaiRecordStatus::PENDING => "pending",
        }
    }
}

impl std::fmt::Display for OaiRecordStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
