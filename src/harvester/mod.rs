mod download;
mod import;
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

    pub fn download(&self) -> anyhow::Result<i32> {
        download::run(&self)
    }

    pub fn import(&self) -> anyhow::Result<i32> {
        import::run(&self)
    }
}
