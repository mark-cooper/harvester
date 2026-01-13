mod download;
mod import;
mod indexer;
mod metadata;
mod oai;
mod rules;

use std::path::PathBuf;

pub use indexer::IndexerConfig;
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

    pub async fn metadata(&self, rules: PathBuf) -> anyhow::Result<()> {
        metadata::run(&self, rules).await
    }
}

pub struct Indexer {
    config: IndexerConfig,
    pool: PgPool,
}

impl Indexer {
    pub fn new(config: IndexerConfig, pool: PgPool) -> Self {
        Self { config, pool }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        indexer::run(&self).await
    }
}
