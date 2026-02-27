pub mod cli;
mod download;
mod import;
mod metadata;
mod rules;

use std::path::{self, PathBuf};

use crate::oai::OaiConfig;

use sqlx::PgPool;

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

        Ok(())
    }
}
