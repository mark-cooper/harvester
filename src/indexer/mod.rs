pub mod arclight;

use futures::{StreamExt, future::BoxFuture, stream};

use crate::harvester::oai::{OaiRecordId, OaiRecordStatus};

const BATCH_SIZE: usize = 100;
const CONCURRENCY: usize = 10;

pub trait Indexer: Sync {
    fn fetch_records<'a>(
        &'a self,
        status: &'a str,
        last_identifier: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>>;
    fn index_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>;
    fn delete_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>;
}

pub async fn run<T: Indexer>(indexer: &T) -> anyhow::Result<()> {
    let total_indexed =
        process_records(indexer, OaiRecordStatus::Parsed.as_str(), T::index_record).await?;
    let total_deleted =
        process_records(indexer, OaiRecordStatus::Deleted.as_str(), T::delete_record).await?;

    println!("Indexed records: {}", total_indexed);
    println!("Deleted records: {}", total_deleted);
    Ok(())
}

async fn process_records<T: Indexer>(
    indexer: &T,
    status: &str,
    action: for<'a> fn(&'a T, &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>,
) -> anyhow::Result<usize> {
    let mut last_identifier: Option<String> = None;
    let mut total = 0usize;

    loop {
        let batch = indexer
            .fetch_records(status, last_identifier.as_deref())
            .await?;
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
                Ok(()) => total += 1,
                Err(e) => eprintln!("Failed to process: {}", e),
            }
        }

        if batch.len() < BATCH_SIZE {
            break;
        }
    }

    Ok(total)
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
