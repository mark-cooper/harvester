pub mod cli;
mod download;
mod import;
mod metadata;
mod rules;

use std::path::{self, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::oai::OaiConfig;

use sqlx::PgPool;

pub struct Harvester {
    config: OaiConfig,
    pool: PgPool,
    shutdown: Arc<AtomicBool>,
}

impl Harvester {
    pub fn new(config: OaiConfig, pool: PgPool, shutdown: Arc<AtomicBool>) -> Self {
        Self {
            config,
            pool,
            shutdown,
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
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
        if self.is_shutdown() {
            return Ok(());
        }
        self.download().await?;
        if self.is_shutdown() {
            return Ok(());
        }

        if let Some(rules) = rules {
            let rules = path::absolute(rules)?;
            if !rules.is_file() {
                anyhow::bail!("rules file was not found");
            }
            self.metadata(rules).await?;
        }

        Ok(())
    }
}
