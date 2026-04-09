pub mod cli;
mod download;
mod import;
mod metadata;
mod rules;

use std::path::{self, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sqlx::PgPool;
use tracing::info;

use crate::OaiRecordId;
use crate::batch;
use crate::db::harvester::{fetch, transition};
use crate::oai::{HarvestEvent, OaiConfig, OaiRecordStatus};

const CONCURRENT_DOWNLOADS: usize = 10;

pub async fn perform(harvester: &Harvester, rules: Option<PathBuf>) -> anyhow::Result<()> {
    import::run(harvester).await?;
    if harvester.is_shutdown() {
        return Ok(());
    }

    download::run(harvester).await?;
    if harvester.is_shutdown() {
        return Ok(());
    }

    if let Some(rules) = rules {
        let rules = path::absolute(rules)?;
        if !rules.is_file() {
            anyhow::bail!("rules file was not found");
        }
        metadata::run(harvester, rules).await?;
    }

    Ok(())
}

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

    async fn batched(
        &self,
        status: OaiRecordStatus,
        label: &str,
        process: impl AsyncFn(&[OaiRecordId]) -> BatchStats,
    ) -> anyhow::Result<()> {
        let all = batch::run(
            || self.is_shutdown(),
            async |last_identifier| {
                fetch(&self.pool, &self.config.repo, status, last_identifier)
                    .await
                    .map_err(Into::into)
            },
            &process,
        )
        .await?;

        let total_processed: usize = all.iter().map(|s| s.processed).sum();
        let total_failed: usize = all.iter().map(|s| s.failed).sum();
        info!("{label} {total_processed} records (failed: {total_failed})");
        Ok(())
    }

    async fn update<'a>(
        &self,
        record: &OaiRecordId,
        event: &HarvestEvent<'a>,
    ) -> anyhow::Result<bool> {
        let key = self.config.repo.record(&record.identifier);

        match transition(&self.pool, key, event).await {
            Ok(result) => {
                if result.rows_affected() == 0 {
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
            Err(_) => Ok(false),
        }
    }
}

#[derive(Default)]
struct BatchStats {
    processed: usize,
    failed: usize,
}

impl BatchStats {
    fn from_results(results: impl IntoIterator<Item = anyhow::Result<bool>>) -> Self {
        let mut stats = Self::default();
        for result in results {
            match result {
                Ok(true) => stats.processed += 1,
                Ok(false) | Err(_) => stats.failed += 1,
            }
        }
        stats
    }
}
