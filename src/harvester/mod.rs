mod download;
mod import;
mod metadata;
mod oai;
pub use oai::OaiConfig;

#[derive(Debug)]
pub struct Harvester {
    config: OaiConfig,
    // TODO: db pool
}

impl Harvester {
    pub fn new(config: OaiConfig) -> Self {
        Self { config }
    }

    pub fn download(&self) -> anyhow::Result<()> {
        download::run(&self)
    }

    pub fn import(&self) -> anyhow::Result<()> {
        import::run(&self)
    }

    pub fn metadata(&self, rules: String) -> anyhow::Result<()> {
        metadata::run(&self, rules)
    }
}
