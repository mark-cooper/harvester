pub mod arclight;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::{StreamExt, future::BoxFuture, stream};
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::{
    OaiRecord, batch,
    db::indexer::{FetchIndexCandidatesParams, fetch, repository_exists, transition},
    oai::{IndexEvent, OaiScope, RecordAction},
};

const CONCURRENCY: usize = 10;

pub trait Indexer: Sync {
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
        }
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        if !repository_exists(&self.pool, &self.config.source_repository).await? {
            warn!(
                "No matching repository was found for: {}",
                self.config.source_repository
            );
            return Ok(());
        }

        let stats = self.process_records().await?;

        info!("Indexed records: {}", stats.indexed);
        info!("Deleted records: {}", stats.deleted);
        info!("Failed index operations: {}", stats.failed);

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

        Ok(ProcessStats {
            indexed: all.iter().map(|s| s.indexed).sum(),
            deleted: all.iter().map(|s| s.deleted).sum(),
            failed: all.iter().map(|s| s.failed).sum(),
        })
    }

    async fn fetch_batch(&self, last_identifier: Option<&str>) -> anyhow::Result<Vec<OaiRecord>> {
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
                    let message = truncate_middle(&e.to_string(), 200, 200);
                    error!("Failed to process: {}", message);
                    let event = action.failure_event(&message);
                    let _ = self.update(record, &event).await;
                }
            }
        }

        stats
    }

    async fn update(&self, record: &OaiRecord, event: &IndexEvent<'_>) -> anyhow::Result<bool> {
        match transition(&self.pool, &self.config.scope, &record.identifier, event).await {
            Ok(result) => Ok(result.rows_affected() > 0),
            Err(_) => Ok(false),
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

    pub fn pending_only() -> Self {
        Self {
            selection_mode: IndexSelectionMode::PendingOnly,
            message_filter: None,
            max_attempts: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum IndexSelectionMode {
    FailedOnly,
    PendingOnly,
}

#[derive(Default)]
struct ProcessStats {
    indexed: usize,
    deleted: usize,
    failed: usize,
}

impl ProcessStats {
    fn record_success(&mut self, action: RecordAction) {
        match action {
            RecordAction::Index => self.indexed += 1,
            RecordAction::Delete => self.deleted += 1,
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
