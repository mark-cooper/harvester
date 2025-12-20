use oai_pmh::client::response::Header;

#[derive(Debug)]
pub struct OaiConfig {
    pub endpoint: String,
    pub metadata_prefix: String,
}

#[derive(Debug, Clone)]
pub struct OaiRecord {
    endpoint: String,
    identifier: String,
    datestamp: String,
    status: String,
    last_checked_at: chrono::DateTime<chrono::Utc>,
    message: String,
}

impl OaiRecord {
    pub fn new(endpoint: String, identifier: String, datestamp: String, status: String) -> Self {
        Self {
            endpoint,
            identifier,
            datestamp,
            status: status,
            last_checked_at: chrono::Utc::now(),
            message: "".into(),
        }
    }

    pub fn endpoint(&mut self, endpoint: String) -> &Self {
        self.endpoint = endpoint;
        self
    }
}

impl From<Header> for OaiRecord {
    fn from(value: Header) -> Self {
        Self::new(
            "".into(),
            value.identifier,
            value.datestamp,
            match value.status {
                Some(status) => status,
                None => "pending".to_string(),
            },
        )
    }
}
