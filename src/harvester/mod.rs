mod download;
mod import;
mod metadata;
pub mod oai;
mod rules;

use std::path::PathBuf;

pub use indexer::arclight::{ArcLightIndexer, ArcLightIndexerConfig};
pub use oai::{OaiConfig, OaiRecordId};

use sqlx::PgPool;

use crate::indexer;

pub struct Harvester {
    config: OaiConfig,
    pool: PgPool,
}

impl Harvester {
    pub fn new(config: OaiConfig, pool: PgPool) -> Self {
        Self { config, pool }
    }

    pub async fn download(&self) -> anyhow::Result<()> {
        download::run(self).await
    }

    pub async fn import(&self) -> anyhow::Result<()> {
        import::run(self).await
    }

    pub async fn metadata(&self, rules: PathBuf) -> anyhow::Result<()> {
        metadata::run(self, rules).await
    }
}
