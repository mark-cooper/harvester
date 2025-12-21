use oai_pmh::client::response::Header;

#[derive(Debug, Clone)]
pub struct OaiConfig {
    pub endpoint: String,
    pub metadata_prefix: String,
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

#[derive(sqlx::FromRow)]
pub struct OaiRecordStatus {
    pub endpoint: String,
    pub metadata_prefix: String,
    pub identifier: String,
    pub status: String,
}
