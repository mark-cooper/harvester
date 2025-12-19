mod download;
mod import;

#[derive(Debug)]
pub struct OaiConfig {
    pub endpoint: String,
    pub metadata_prefix: String,
}

#[derive(Debug)]
pub struct Harvester {
    config: OaiConfig,
    // TODO: db pool
}

#[derive(Debug)]
pub enum HarvesterError {
    ConnectionError(String),
}

impl std::error::Error for HarvesterError {}
impl std::fmt::Display for HarvesterError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HarvesterError::ConnectionError(e) => write!(f, "failed to connect {}", e),
        }
    }
}

impl Harvester {
    pub fn new(config: OaiConfig) -> Self {
        Self { config }
    }

    pub fn download(&self) -> Result<i32, HarvesterError> {
        download::run(&self)
    }

    pub fn import(&self) -> Result<i32, HarvesterError> {
        import::run(&self)
    }
}
