pub mod arclight;

use std::process::Command;

use futures::{StreamExt, future::BoxFuture, stream};
use sqlx::PgPool;

use crate::{
    OaiRecordId,
    db::{
        FetchIndexCandidatesParams, UpdateIndexFailureParams, UpdateIndexStatusParams,
        do_mark_index_failure_query, do_mark_index_success_query, do_mark_purge_failure_query,
        do_mark_purge_success_query, fetch_failed_records_for_indexing,
        fetch_failed_records_for_purging, fetch_pending_records_for_indexing,
        fetch_pending_records_for_purging,
    },
};

const BATCH_SIZE: usize = 100;
const CONCURRENCY: usize = 10;

pub struct IndexerContext {
    pool: PgPool,
    endpoint: String,
    metadata_prefix: String,
    oai_repository: String,
    selection_mode: IndexSelectionMode,
    message_filter: Option<String>,
    max_attempts: Option<i32>,
    preview: bool,
}

impl IndexerContext {
    pub fn new(
        pool: PgPool,
        endpoint: String,
        metadata_prefix: String,
        oai_repository: String,
        run_options: IndexRunOptions,
        preview: bool,
    ) -> Self {
        Self {
            pool,
            endpoint,
            metadata_prefix,
            oai_repository,
            selection_mode: run_options.selection_mode,
            message_filter: run_options.message_filter,
            max_attempts: run_options.max_attempts,
            preview,
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
pub(crate) enum IndexSelectionMode {
    FailedOnly,
    PendingOnly,
}

struct ProcessStats {
    succeeded: usize,
    failed: usize,
}

enum RecordPhase {
    Index,
    Purge,
}

pub(crate) fn ensure_traject_available() -> anyhow::Result<()> {
    let status = Command::new("traject").args(["--version"]).status()?;

    if !status.success() {
        anyhow::bail!("traject failed with exit code: {:?}", status.code());
    }

    Ok(())
}

pub trait Indexer: Sync {
    fn index_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>;
    fn delete_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>;
}

pub async fn run<T: Indexer>(ctx: &IndexerContext, indexer: &T) -> anyhow::Result<()> {
    let indexed = process_records(ctx, indexer, RecordPhase::Index).await?;
    let deleted = process_records(ctx, indexer, RecordPhase::Purge).await?;

    println!("Indexed records: {}", indexed.succeeded);
    println!("Deleted records: {}", deleted.succeeded);
    println!("Failed index operations: {}", indexed.failed);
    println!("Failed delete operations: {}", deleted.failed);

    let total_failed = indexed.failed + deleted.failed;
    if total_failed > 0 {
        anyhow::bail!("index run completed with {} failed record(s)", total_failed);
    }

    Ok(())
}

async fn fetch_batch(
    ctx: &IndexerContext,
    phase: &RecordPhase,
    last_identifier: Option<&str>,
) -> anyhow::Result<Vec<OaiRecordId>> {
    let params = FetchIndexCandidatesParams {
        endpoint: &ctx.endpoint,
        metadata_prefix: &ctx.metadata_prefix,
        oai_repository: &ctx.oai_repository,
        max_attempts: ctx.max_attempts,
        message_filter: ctx.message_filter.as_deref(),
        last_identifier,
    };

    Ok(match (phase, ctx.selection_mode) {
        (RecordPhase::Index, IndexSelectionMode::PendingOnly) => {
            fetch_pending_records_for_indexing(&ctx.pool, params).await?
        }
        (RecordPhase::Index, IndexSelectionMode::FailedOnly) => {
            fetch_failed_records_for_indexing(&ctx.pool, params).await?
        }
        (RecordPhase::Purge, IndexSelectionMode::PendingOnly) => {
            fetch_pending_records_for_purging(&ctx.pool, params).await?
        }
        (RecordPhase::Purge, IndexSelectionMode::FailedOnly) => {
            fetch_failed_records_for_purging(&ctx.pool, params).await?
        }
    })
}

async fn mark_success(
    ctx: &IndexerContext,
    phase: &RecordPhase,
    record: &OaiRecordId,
) -> anyhow::Result<()> {
    let params = UpdateIndexStatusParams {
        endpoint: &ctx.endpoint,
        metadata_prefix: &ctx.metadata_prefix,
        identifier: &record.identifier,
    };

    match phase {
        RecordPhase::Index => do_mark_index_success_query(&ctx.pool, params).await?,
        RecordPhase::Purge => do_mark_purge_success_query(&ctx.pool, params).await?,
    };

    Ok(())
}

async fn mark_failure(
    ctx: &IndexerContext,
    phase: &RecordPhase,
    record: &OaiRecordId,
    message: &str,
) -> anyhow::Result<()> {
    let params = UpdateIndexFailureParams {
        endpoint: &ctx.endpoint,
        metadata_prefix: &ctx.metadata_prefix,
        identifier: &record.identifier,
        message,
    };

    match phase {
        RecordPhase::Index => do_mark_index_failure_query(&ctx.pool, params).await?,
        RecordPhase::Purge => do_mark_purge_failure_query(&ctx.pool, params).await?,
    };

    Ok(())
}

async fn process_records<T: Indexer>(
    ctx: &IndexerContext,
    indexer: &T,
    phase: RecordPhase,
) -> anyhow::Result<ProcessStats> {
    let mut last_identifier: Option<String> = None;
    let mut succeeded = 0usize;
    let mut failed = 0usize;

    loop {
        let batch = fetch_batch(ctx, &phase, last_identifier.as_deref()).await?;
        if batch.is_empty() {
            break;
        }

        last_identifier = batch.last().map(|r| r.identifier.clone());

        if ctx.preview {
            for record in &batch {
                let label = match phase {
                    RecordPhase::Index => "index",
                    RecordPhase::Purge => "delete",
                };
                println!("Would {} record: {}", label, record.identifier);
            }
            succeeded += batch.len();
        } else {
            let results: Vec<_> = stream::iter(&batch)
                .map(|record| {
                    let fut = match phase {
                        RecordPhase::Index => indexer.index_record(record),
                        RecordPhase::Purge => indexer.delete_record(record),
                    };
                    async move { (record, fut.await) }
                })
                .buffer_unordered(CONCURRENCY)
                .collect()
                .await;

            for (record, result) in results {
                match result {
                    Ok(()) => {
                        if let Err(e) = mark_success(ctx, &phase, record).await {
                            failed += 1;
                            eprintln!("Failed to mark success for {}: {}", record.identifier, e);
                        } else {
                            succeeded += 1;
                        }
                    }
                    Err(e) => {
                        failed += 1;
                        let message = truncate_middle(&e.to_string(), 200, 200);
                        eprintln!("Failed to process: {}", message);
                        if let Err(db_err) = mark_failure(ctx, &phase, record, &message).await {
                            eprintln!(
                                "Failed to mark failure for {}: {}",
                                record.identifier, db_err
                            );
                        }
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
