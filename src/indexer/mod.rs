pub mod arclight;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::{StreamExt, future::BoxFuture, stream};
use sqlx::PgPool;
use tracing::{error, info};

use crate::{
    OaiRecordId, batch,
    db::indexer::{FetchIndexCandidatesParams, UpdateIndexStatusParams, fetch, transition},
    oai::IndexEvent,
    oai::OaiRecordStatus,
};

const CONCURRENCY: usize = 10;

pub trait Indexer: Sync {
    fn index_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>;
    fn delete_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>;
}

pub struct IndexRunnerConfig {
    pub endpoint: String,
    pub metadata_prefix: String,
    pub oai_repository: String,
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

    pub async fn run(&self) -> anyhow::Result<()> {
        let stats = self.process_records().await?;

        info!("Indexed records: {}", stats.indexed);
        info!("Deleted records: {}", stats.deleted);
        info!("Failed index operations: {}", stats.failed);

        if stats.failed > 0 {
            anyhow::bail!("index run completed with {} failed record(s)", stats.failed);
        }

        Ok(())
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    async fn fetch_batch(&self, last_identifier: Option<&str>) -> anyhow::Result<Vec<OaiRecordId>> {
        let params = FetchIndexCandidatesParams {
            endpoint: &self.config.endpoint,
            metadata_prefix: &self.config.metadata_prefix,
            oai_repository: &self.config.oai_repository,
            selection_mode: self.config.run_options.selection_mode,
            max_attempts: self.config.run_options.max_attempts,
            message_filter: self.config.run_options.message_filter.as_deref(),
            last_identifier,
        };

        Ok(fetch(&self.pool, params).await?)
    }

    async fn process_batch(&self, batch: &[OaiRecordId]) -> ProcessStats {
        let mut stats = ProcessStats {
            indexed: 0,
            deleted: 0,
            failed: 0,
        };

        if self.config.preview {
            for record in batch {
                match action_for(record.status) {
                    RecordAction::Index => {
                        info!("Would index record: {}", record.identifier);
                        stats.indexed += 1;
                    }
                    RecordAction::Delete => {
                        info!("Would delete record: {}", record.identifier);
                        stats.deleted += 1;
                    }
                }
            }
        } else {
            let results: Vec<_> = stream::iter(batch)
                .map(|record| async move {
                    let result = match action_for(record.status) {
                        RecordAction::Index => self.indexer.index_record(record).await,
                        RecordAction::Delete => self.indexer.delete_record(record).await,
                    };

                    (record, result)
                })
                .buffer_unordered(CONCURRENCY)
                .collect()
                .await;

            for (record, result) in results {
                let action = action_for(record.status);
                match result {
                    Ok(()) => {
                        let event = success_event(action);
                        if let Ok(true) = self.update(record, &event).await {
                            match action {
                                RecordAction::Index => stats.indexed += 1,
                                RecordAction::Delete => stats.deleted += 1,
                            }
                        }
                    }
                    Err(e) => {
                        stats.failed += 1;
                        let message = truncate_middle(&e.to_string(), 200, 200);
                        error!("Failed to process: {}", message);
                        let event = failure_event(action, &message);
                        let _ = self.update(record, &event).await;
                    }
                }
            }
        }

        stats
    }

    async fn process_records(&self) -> anyhow::Result<ProcessStats> {
        let all = batch::run(
            || self.is_shutdown(),
            async |last_identifier| self.fetch_batch(last_identifier).await,
            async |batch: &[OaiRecordId]| self.process_batch(batch).await,
        )
        .await?;

        Ok(ProcessStats {
            indexed: all.iter().map(|s| s.indexed).sum(),
            deleted: all.iter().map(|s| s.deleted).sum(),
            failed: all.iter().map(|s| s.failed).sum(),
        })
    }

    async fn update(&self, record: &OaiRecordId, event: &IndexEvent<'_>) -> anyhow::Result<bool> {
        let params = UpdateIndexStatusParams {
            endpoint: &self.config.endpoint,
            metadata_prefix: &self.config.metadata_prefix,
            identifier: &record.identifier,
        };

        match transition(&self.pool, params, event).await {
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

struct ProcessStats {
    indexed: usize,
    deleted: usize,
    failed: usize,
}

#[derive(Clone, Copy)]
enum RecordAction {
    Index,
    Delete,
}

fn action_for(status: OaiRecordStatus) -> RecordAction {
    match status {
        OaiRecordStatus::Parsed => RecordAction::Index,
        OaiRecordStatus::Deleted => RecordAction::Delete,
        _ => unreachable!("only parsed and deleted records should be fetched for indexing"),
    }
}

fn success_event(action: RecordAction) -> IndexEvent<'static> {
    match action {
        RecordAction::Index => IndexEvent::IndexSucceeded,
        RecordAction::Delete => IndexEvent::PurgeSucceeded,
    }
}

fn failure_event<'a>(action: RecordAction, message: &'a str) -> IndexEvent<'a> {
    match action {
        RecordAction::Index => IndexEvent::IndexFailed { message },
        RecordAction::Delete => IndexEvent::PurgeFailed { message },
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
