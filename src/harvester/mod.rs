mod download;
mod import;
mod metadata;
pub mod oai;
mod rules;

use std::path::{self, PathBuf};

use clap::Args;
pub use indexer::arclight::{
    ArcLightArgs, ArcLightIndexer, ArcLightIndexerConfig, IndexSelectionMode,
};
pub use oai::{OaiConfig, OaiRecordId};

use sqlx::PgPool;

use crate::indexer;

#[derive(Debug, Args)]
pub struct HarvesterArgs {
    /// OAI endpoint url
    pub endpoint: String,

    /// Base directory for downloads
    #[arg(short, long, default_value = "data")]
    pub dir: PathBuf,

    /// OAI metadata prefix
    #[arg(short, long)]
    pub metadata_prefix: String,

    /// XML scanning rules file
    #[arg(short, long)]
    pub rules: Option<PathBuf>,
}

pub struct Harvester {
    config: OaiConfig,
    pool: PgPool,
}

impl Harvester {
    pub fn new(config: OaiConfig, pool: PgPool) -> Self {
        Self { config, pool }
    }

    async fn download(&self) -> anyhow::Result<()> {
        download::run(self).await
    }

    async fn import(&self) -> anyhow::Result<()> {
        import::run(self).await
    }

    async fn metadata(&self, rules: PathBuf) -> anyhow::Result<()> {
        metadata::run(self, rules).await
    }

    pub async fn run(&self, rules: Option<PathBuf>) -> anyhow::Result<()> {
        self.import().await?;
        self.download().await?;

        if let Some(rules) = rules {
            let rules = path::absolute(rules)?;
            if !rules.is_file() {
                anyhow::bail!("rules file was not found");
            }
            self.metadata(rules).await?;
        }

        // TODO: generate a summary of the ead xml content
        // self.summarize().await?;
        Ok(())
    }
}
