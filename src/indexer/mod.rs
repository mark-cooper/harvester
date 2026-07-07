pub mod arclight;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures::{StreamExt, future::BoxFuture, stream};
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::{
    OaiRecord, batch,
    db::indexer::{FetchIndexCandidatesParams, fetch, repository_exists, transition},
    db::runs::{self, RunStats},
    oai::{IndexEvent, OaiScope, RecordAction},
};

const CONCURRENCY: usize = 10;

/// Default attempts budget for failed records in standard runs. Records
/// at/above this many failed attempts are quarantined until `--retry` or
/// `--reindex`.
pub const DEFAULT_MAX_INDEX_ATTEMPTS: i32 = 5;

/// Abort the run once this many records have failed with no success in
/// between. Catches environment-wide breakage (dead Solr, broken traject)
/// before it burns an attempt on the whole corpus.
const CIRCUIT_BREAKER_THRESHOLD: usize = 100;

pub trait Indexer: Sync {
    /// Cheap environment check run once before the first record. Failing here
    /// aborts the run without consuming any record's attempt budget.
    fn preflight<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn index_record<'a>(&'a self, record: &'a OaiRecord) -> BoxFuture<'a, anyhow::Result<()>>;
    fn delete_record<'a>(&'a self, record: &'a OaiRecord) -> BoxFuture<'a, anyhow::Result<()>>;
}

pub struct IndexRunnerConfig {
    pub scope: OaiScope,
    pub source_repository: String,
    pub run_options: IndexRunOptions,
    pub preview: bool,
}

pub struct IndexRunner<T: Indexer> {
    indexer: T,
    config: IndexRunnerConfig,
    pool: PgPool,
    shutdown: Arc<AtomicBool>,
    consecutive_failures: AtomicUsize,
}

impl<T: Indexer> IndexRunner<T> {
    pub fn new(
        indexer: T,
        config: IndexRunnerConfig,
        pool: PgPool,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            indexer,
            config,
            pool,
            shutdown,
            consecutive_failures: AtomicUsize::new(0),
        }
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    fn breaker_tripped(&self) -> bool {
        self.consecutive_failures.load(Ordering::Relaxed) >= CIRCUIT_BREAKER_THRESHOLD
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let run_id = runs::start(
            &self.pool,
            runs::KIND_INDEX,
            &self.config.scope,
            &self.config.source_repository,
        )
        .await?;

        let mut stats = ProcessStats::default();
        let result = self.run_inner(&mut stats).await;

        let outcome = match &result {
            Ok(()) => runs::OUTCOME_COMPLETED,
            Err(_) => runs::OUTCOME_FAILED,
        };
        let error_sample = match (&stats.first_error, &result) {
            (Some(message), _) => truncate_middle(message, 200, 200),
            (None, Err(error)) => truncate_middle(&error.to_string(), 200, 200),
            (None, Ok(())) => String::new(),
        };
        let run_stats = RunStats {
            processed: stats.indexed + stats.deleted + stats.failed,
            imported: stats.indexed,
            deleted: stats.deleted,
            failed: stats.failed,
        };
        if let Err(error) =
            runs::finish(&self.pool, run_id, outcome, &run_stats, &error_sample).await
        {
            error!("Failed to record run {run_id}: {error}");
        }

        result
    }

    async fn run_inner(&self, stats: &mut ProcessStats) -> anyhow::Result<()> {
        if !self.config.preview {
            self.indexer.preflight().await.map_err(|error| {
                anyhow::anyhow!("preflight failed, aborting run before consuming attempts: {error}")
            })?;
        }

        if !repository_exists(&self.pool, &self.config.source_repository).await? {
            warn!(
                "No matching repository was found for: {}",
                self.config.source_repository
            );
            return Ok(());
        }

        *stats = self.process_records().await?;

        info!("Indexed records: {}", stats.indexed);
        info!("Deleted records: {}", stats.deleted);
        info!("Failed index operations: {}", stats.failed);

        if self.breaker_tripped() {
            anyhow::bail!(
                "circuit breaker: aborted after {} consecutive failures (environment problem?)",
                self.consecutive_failures.load(Ordering::Relaxed)
            );
        }

        if stats.failed > 0 {
            anyhow::bail!("index run completed with {} failed record(s)", stats.failed);
        }

        Ok(())
    }

    async fn process_records(&self) -> anyhow::Result<ProcessStats> {
        let all = batch::run(
            || self.is_shutdown(),
            async |last_identifier| self.fetch_batch(last_identifier).await,
            async |batch: &[OaiRecord]| self.process_batch(batch).await,
        )
        .await?;

        let mut total = ProcessStats::default();
        for stats in all {
            total.merge(stats);
        }
        Ok(total)
    }

    async fn fetch_batch(&self, last_identifier: Option<&str>) -> anyhow::Result<Vec<OaiRecord>> {
        // Stop paginating once the breaker trips; run_inner turns this into a
        // run-level error after stats are reported.
        if self.breaker_tripped() {
            return Ok(Vec::new());
        }

        let params = FetchIndexCandidatesParams {
            scope: &self.config.scope,
            source_repository: &self.config.source_repository,
            selection_mode: self.config.run_options.selection_mode,
            max_attempts: self.config.run_options.max_attempts,
            message_filter: self.config.run_options.message_filter.as_deref(),
            last_identifier,
        };

        Ok(fetch(&self.pool, params).await?)
    }

    async fn process_batch(&self, batch: &[OaiRecord]) -> ProcessStats {
        let mut stats = ProcessStats::default();
        let items = batch
            .iter()
            .map(|r| (r, RecordAction::for_status(r.status)));

        if self.config.preview {
            for (record, action) in items {
                info!("Would {action} record: {}", record.identifier);
                stats.record_success(action);
            }
            return stats;
        }

        let results: Vec<_> = stream::iter(items)
            .map(|(record, action)| async move {
                let result = match action {
                    RecordAction::Index => self.indexer.index_record(record).await,
                    RecordAction::Delete => self.indexer.delete_record(record).await,
                };
                (record, action, result)
            })
            .buffer_unordered(CONCURRENCY)
            .collect()
            .await;

        for (record, action, result) in results {
            match result {
                Ok(()) => {
                    let event = action.success_event();
                    if let Ok(true) = self.update(record, &event).await {
                        stats.record_success(action);
                    }
                }
                Err(e) => {
                    stats.failed += 1;
                    let message = truncate_middle(&e.to_string(), 1000, 500);
                    error!(
                        "Failed to {action} record {}: {}",
                        record.identifier, message
                    );
                    stats.first_error.get_or_insert_with(|| message.clone());
                    let event = action.failure_event(&message);
                    let _ = self.update(record, &event).await;
                }
            }
        }

        // Batches processed concurrently have no meaningful intra-batch order:
        // count a fully-failed batch as consecutive, any success resets.
        if stats.failed > 0 && stats.indexed == 0 && stats.deleted == 0 {
            self.consecutive_failures
                .fetch_add(stats.failed, Ordering::Relaxed);
        } else {
            self.consecutive_failures.store(0, Ordering::Relaxed);
        }

        stats
    }

    async fn update(&self, record: &OaiRecord, event: &IndexEvent<'_>) -> anyhow::Result<bool> {
        match transition(&self.pool, record.id, event).await {
            Ok(rows_affected) => {
                if rows_affected == 0 {
                    warn!(
                        "index transition for {} did not apply (stale state?): {:?}",
                        record.identifier, event
                    );
                }
                Ok(rows_affected > 0)
            }
            Err(error) => {
                error!(
                    "index transition for {} failed: {error} ({:?})",
                    record.identifier, event
                );
                Ok(false)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexRunOptions {
    pub(crate) selection_mode: IndexSelectionMode,
    pub(crate) message_filter: Option<String>,
    pub(crate) max_attempts: Option<i32>,
}

impl IndexRunOptions {
    pub fn failed_only(message_filter: Option<String>, max_attempts: Option<i32>) -> Self {
        Self {
            selection_mode: IndexSelectionMode::FailedOnly,
            message_filter,
            max_attempts,
        }
    }

    /// Standard run: pending records plus failed records under the attempts
    /// budget (`None` = unlimited retries).
    pub fn standard(max_attempts: Option<i32>) -> Self {
        Self {
            selection_mode: IndexSelectionMode::Standard,
            message_filter: None,
            max_attempts,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum IndexSelectionMode {
    FailedOnly,
    Standard,
}

#[derive(Default)]
struct ProcessStats {
    indexed: usize,
    deleted: usize,
    failed: usize,
    first_error: Option<String>,
}

impl ProcessStats {
    fn record_success(&mut self, action: RecordAction) {
        match action {
            RecordAction::Index => self.indexed += 1,
            RecordAction::Delete => self.deleted += 1,
        }
    }

    fn merge(&mut self, other: Self) {
        self.indexed += other.indexed;
        self.deleted += other.deleted;
        self.failed += other.failed;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }
}

/// Get head & tail of string for debugging shelled-out cmds
fn truncate_middle(s: &str, head: usize, tail: usize) -> String {
    let total = s.chars().count();
    if total <= head + tail {
        return s.to_string();
    }

    let head_end = s.char_indices().nth(head).unwrap().0;
    let tail_start = s.char_indices().nth(total - tail).unwrap().0;

    format!(
        "{}\n... <{} chars omitted> ...\n{}",
        &s[..head_end],
        total - head - tail,
        &s[tail_start..],
    )
}
