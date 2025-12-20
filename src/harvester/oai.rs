use oai_pmh::client::response::Header;

#[derive(Debug, Clone)]
pub struct OaiConfig {
    pub endpoint: String,
    pub metadata_prefix: String,
}

#[derive(Debug)]
pub struct OaiRecord {
    pub endpoint: String,
    pub metadata_prefix: String,
    pub identifier: String,
    pub datestamp: String,
    pub status: String,
    pub last_checked_at: chrono::DateTime<chrono::Utc>,
    pub message: Option<String>,
    pub metadata: Option<String>,
    pub summary: Option<String>,
}

impl OaiRecord {
    pub fn new(
        endpoint: String,
        metadata_prefix: String,
        identifier: String,
        datestamp: String,
        status: String,
    ) -> Self {
        Self {
            endpoint,
            metadata_prefix,
            identifier,
            datestamp,
            status,
            last_checked_at: chrono::Utc::now(),
            message: None,
            metadata: None,
            summary: None,
        }
    }

    pub fn from_header(header: Header, config: &OaiConfig) -> Self {
        Self::new(
            config.endpoint.clone(),
            config.metadata_prefix.clone(),
            header.identifier,
            header.datestamp,
            header.status.unwrap_or_else(|| "pending".to_string()),
        )
    }
}
