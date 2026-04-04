pub mod cli;
mod download;
mod import;
mod metadata;
mod rules;

use std::future::Future;
use std::path::{self, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::PgPool;
use tracing::info;

use crate::OaiRecordId;
use crate::batch;
use crate::db::harvester::{FetchRecordsParams, RecordTransitionParams, fetch, transition};
use crate::oai::{HarvestEvent, OaiConfig, OaiRecordStatus};

const CONCURRENT_DOWNLOADS: usize = 10;

async fn oai_timeout<T>(
    label: &str,
    duration: Duration,
    future: impl Future<Output = T>,
) -> anyhow::Result<T> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| anyhow::anyhow!("OAI {} timed out after {}s", label, duration.as_secs()))
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

    async fn run_batched(
        &self,
        status: OaiRecordStatus,
        label: &str,
        process: impl AsyncFn(&[OaiRecordId]) -> BatchStats,
    ) -> anyhow::Result<()> {
        let all = batch::run(
            || self.is_shutdown(),
            async |last_identifier| {
                let params = FetchRecordsParams {
                    endpoint: &self.config.endpoint,
                    metadata_prefix: &self.config.metadata_prefix,
                    status,
                    last_identifier,
                };
                fetch(&self.pool, params).await.map_err(Into::into)
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
        let params = RecordTransitionParams {
            endpoint: self.config.endpoint.as_str(),
            metadata_prefix: self.config.metadata_prefix.as_str(),
            identifier: &record.identifier,
        };

        match transition(&self.pool, params, event).await {
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
