pub mod arclight;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::{StreamExt, future::BoxFuture, stream};
use sqlx::PgPool;
use tracing::{error, info};

use crate::{
    OaiRecordId,
    db::indexer::{FetchIndexCandidatesParams, UpdateIndexStatusParams, fetch, transition},
    oai::IndexEvent,
    oai::{OaiIndexStatus, OaiRecordStatus},
};

const BATCH_SIZE: usize = 100;
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

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
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

    pub async fn run(&self) -> anyhow::Result<()> {
        let indexed = process_records(self, RecordPhase::Index).await?;
        let deleted = process_records(self, RecordPhase::Purge).await?;

        info!("Indexed records: {}", indexed.succeeded);
        info!("Deleted records: {}", deleted.succeeded);
        info!("Failed index operations: {}", indexed.failed);
        info!("Failed delete operations: {}", deleted.failed);

        let total_failed = indexed.failed + deleted.failed;
        if total_failed > 0 {
            anyhow::bail!("index run completed with {} failed record(s)", total_failed);
        }

        Ok(())
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
pub(crate) enum IndexSelectionMode {
    FailedOnly,
    PendingOnly,
}

struct ProcessStats {
    succeeded: usize,
    failed: usize,
}

/// Index worker phases and their expected transitions:
/// - `Index`: `(status=parsed, index_status=pending|index_failed) -> indexed|index_failed`
/// - `Purge`: `(status=deleted, index_status=pending|purge_failed) -> purged|purge_failed`
enum RecordPhase {
    Index,
    Purge,
}

async fn process_records<T: Indexer>(
    runner: &IndexRunner<T>,
    phase: RecordPhase,
) -> anyhow::Result<ProcessStats> {
    let mut last_identifier: Option<String> = None;
    let mut succeeded = 0usize;
    let mut failed = 0usize;

    loop {
        if runner.is_shutdown() {
            break;
        }
        let batch = fetch_batch(runner, &phase, last_identifier.as_deref()).await?;
        if batch.is_empty() {
            break;
        }

        last_identifier = batch.last().map(|r| r.identifier.clone());

        if runner.config.preview {
            for record in &batch {
                let label = match phase {
                    RecordPhase::Index => "index",
                    RecordPhase::Purge => "delete",
                };
                info!("Would {} record: {}", label, record.identifier);
            }
            succeeded += batch.len();
        } else {
            let results: Vec<_> = stream::iter(&batch)
                .map(|record| {
                    let fut = match phase {
                        RecordPhase::Index => runner.indexer.index_record(record),
                        RecordPhase::Purge => runner.indexer.delete_record(record),
                    };
                    async move { (record, fut.await) }
                })
                .buffer_unordered(CONCURRENCY)
                .collect()
                .await;

            for (record, result) in results {
                match result {
                    Ok(()) => {
                        let event = match phase {
                            RecordPhase::Index => IndexEvent::IndexSucceeded,
                            RecordPhase::Purge => IndexEvent::PurgeSucceeded,
                        };
                        if let Ok(true) = runner.update(record, &event).await {
                            succeeded += 1;
                        }
                    }
                    Err(e) => {
                        failed += 1;
                        let message = truncate_middle(&e.to_string(), 200, 200);
                        error!("Failed to process: {}", message);
                        let event = match phase {
                            RecordPhase::Index => IndexEvent::IndexFailed { message: &message },
                            RecordPhase::Purge => IndexEvent::PurgeFailed { message: &message },
                        };
                        let _ = runner.update(record, &event).await;
                    }
                }
            }
        }

        if batch.len() < BATCH_SIZE {
            break;
        }
    }

    Ok(ProcessStats { succeeded, failed })
}

async fn fetch_batch<T: Indexer>(
    runner: &IndexRunner<T>,
    phase: &RecordPhase,
    last_identifier: Option<&str>,
) -> anyhow::Result<Vec<OaiRecordId>> {
    let (status, index_status) = match (phase, runner.config.run_options.selection_mode) {
        (RecordPhase::Index, IndexSelectionMode::PendingOnly) => {
            (OaiRecordStatus::Parsed, OaiIndexStatus::Pending)
        }
        (RecordPhase::Index, IndexSelectionMode::FailedOnly) => {
            (OaiRecordStatus::Parsed, OaiIndexStatus::IndexFailed)
        }
        (RecordPhase::Purge, IndexSelectionMode::PendingOnly) => {
            (OaiRecordStatus::Deleted, OaiIndexStatus::Pending)
        }
        (RecordPhase::Purge, IndexSelectionMode::FailedOnly) => {
            (OaiRecordStatus::Deleted, OaiIndexStatus::PurgeFailed)
        }
    };

    let params = FetchIndexCandidatesParams {
        endpoint: &runner.config.endpoint,
        metadata_prefix: &runner.config.metadata_prefix,
        oai_repository: &runner.config.oai_repository,
        status,
        index_status,
        max_attempts: runner.config.run_options.max_attempts,
        message_filter: runner.config.run_options.message_filter.as_deref(),
        last_identifier,
    };

    Ok(fetch(&runner.pool, params).await?)
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
