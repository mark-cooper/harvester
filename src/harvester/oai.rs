use std::path::PathBuf;

use oai_pmh::client::response::Header;

#[derive(Debug, Clone)]
pub struct OaiConfig {
    pub data_dir: PathBuf,
    pub endpoint: String,
    pub metadata_prefix: String,
}

#[derive(sqlx::FromRow)]
pub struct OaiRecord {
    pub endpoint: String,
    pub metadata_prefix: String,
    pub identifier: String,
    pub fingerprint: String,
    pub status: String,
}

impl OaiRecord {
    pub fn path(&self) -> PathBuf {
        PathBuf::from(&self.fingerprint[0..2])
            .join(&self.fingerprint[2..4])
            .join(format!("{}.xml", self.fingerprint))
    }
}

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
            value.status.unwrap_or_else(|| "pending".to_string()),
        )
    }
}
