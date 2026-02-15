pub mod arclight;

use futures::{StreamExt, future::BoxFuture, stream};

use crate::harvester::oai::OaiRecordId;

const BATCH_SIZE: usize = 100;
const CONCURRENCY: usize = 10;

struct ProcessStats {
    succeeded: usize,
    failed: usize,
}

pub trait Indexer: Sync {
    fn fetch_records_to_index<'a>(
        &'a self,
        last_identifier: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>>;
    fn fetch_records_to_purge<'a>(
        &'a self,
        last_identifier: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>>;
    fn index_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>;
    fn delete_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>;
}

pub async fn run<T: Indexer>(indexer: &T) -> anyhow::Result<()> {
    let indexed = process_records(indexer, T::fetch_records_to_index, T::index_record).await?;
    let deleted = process_records(indexer, T::fetch_records_to_purge, T::delete_record).await?;

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

async fn process_records<T: Indexer>(
    indexer: &T,
    fetch: for<'a> fn(&'a T, Option<&'a str>) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>>,
    action: for<'a> fn(&'a T, &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>,
) -> anyhow::Result<ProcessStats> {
    let mut last_identifier: Option<String> = None;
    let mut succeeded = 0usize;
    let mut failed = 0usize;

    loop {
        let batch = fetch(indexer, last_identifier.as_deref()).await?;
        if batch.is_empty() {
            break;
        }

        last_identifier = batch.last().map(|r| r.identifier.clone());

        let results: Vec<_> = stream::iter(&batch)
            .map(|record| action(indexer, record))
            .buffer_unordered(CONCURRENCY)
            .collect()
            .await;

        for result in results {
            match result {
                Ok(()) => succeeded += 1,
                Err(e) => {
                    failed += 1;
                    eprintln!("Failed to process: {}", e);
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
