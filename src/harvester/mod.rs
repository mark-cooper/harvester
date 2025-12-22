mod download;
mod import;
mod metadata;
mod oai;
pub use oai::OaiConfig;

use sqlx::PgPool;

pub struct Harvester {
    config: OaiConfig,
    pool: PgPool,
}

impl Harvester {
    pub fn new(config: OaiConfig, pool: PgPool) -> Self {
        Self { config, pool }
    }

    pub async fn download(&self) -> anyhow::Result<()> {
        download::run(&self).await
    }

    pub async fn import(&self) -> anyhow::Result<()> {
        import::run(&self).await
    }

    pub async fn metadata(&self, rules: String) -> anyhow::Result<()> {
        metadata::run(&self, rules).await
    }
}
