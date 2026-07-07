pub mod cli;
mod download;
mod import;
mod metadata;
mod rules;

use std::path::{self, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sqlx::PgPool;
use tracing::{error, info};

use crate::OaiRecord;
use crate::batch;
use crate::db::harvester::{fetch, transition};
use crate::db::runs::{self, RunStats};
use crate::oai::{HarvestEvent, OaiConfig, OaiRecordStatus};

const CONCURRENT_DOWNLOADS: usize = 10;

pub async fn perform(harvester: &Harvester, rules: Option<PathBuf>) -> anyhow::Result<()> {
    let run_id = runs::start(
        &harvester.pool,
        runs::KIND_HARVEST,
        &harvester.config.scope,
        "",
    )
    .await?;

    let mut stats = RunStats::default();
    let result = perform_inner(harvester, rules, &mut stats).await;

    let (outcome, error_sample) = match &result {
        Ok(()) => (runs::OUTCOME_COMPLETED, String::new()),
        Err(e) => (
            runs::OUTCOME_FAILED,
            e.to_string().chars().take(400).collect(),
        ),
    };
    if let Err(e) = runs::finish(&harvester.pool, run_id, outcome, &stats, &error_sample).await {
        error!("Failed to record run {run_id}: {e}");
    }

    result
}

async fn perform_inner(
    harvester: &Harvester,
    rules: Option<PathBuf>,
    stats: &mut RunStats,
) -> anyhow::Result<()> {
    let import_stats = import::run(harvester).await?;
    stats.processed = import_stats.processed;
    stats.imported = import_stats.imported;
    stats.deleted = import_stats.deleted;
    if harvester.is_shutdown() {
        return Ok(());
    }

    let download_stats = download::run(harvester).await?;
    stats.failed += download_stats.failed;
    if harvester.is_shutdown() {
        return Ok(());
    }

    if let Some(rules) = rules {
        let rules = path::absolute(rules)?;
        if !rules.is_file() {
            anyhow::bail!("rules file was not found");
        }
        let metadata_stats = metadata::run(harvester, rules).await?;
        stats.failed += metadata_stats.failed;
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
        process: impl AsyncFn(&[OaiRecord]) -> BatchStats,
    ) -> anyhow::Result<BatchStats> {
        let all = batch::run(
            || self.is_shutdown(),
            async |last_identifier| {
                fetch(&self.pool, &self.config.scope, status, last_identifier)
                    .await
                    .map_err(Into::into)
            },
            &process,
        )
        .await?;

        let total = BatchStats {
            processed: all.iter().map(|s| s.processed).sum(),
            failed: all.iter().map(|s| s.failed).sum(),
        };
        info!("{label} {} records (failed: {})", total.processed, total.failed);
        Ok(total)
    }

    async fn update<'a>(
        &self,
        record: &OaiRecord,
        event: &HarvestEvent<'a>,
    ) -> anyhow::Result<bool> {
        match transition(&self.pool, &self.config.scope, &record.identifier, event).await {
            Ok(rows_affected) => Ok(rows_affected > 0),
            Err(error) => {
                error!(
                    "harvest transition for {} failed: {error} ({:?})",
                    record.identifier, event
                );
                Ok(false)
            }
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
