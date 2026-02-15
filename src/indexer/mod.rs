pub mod arclight;

use futures::{StreamExt, future::BoxFuture, stream};

use crate::harvester::oai::OaiRecordId;

const BATCH_SIZE: usize = 100;
const CONCURRENCY: usize = 10;

struct ProcessStats {
    succeeded: usize,
    failed: usize,
}

type FetchFn<T> =
    for<'a> fn(&'a T, Option<&'a str>) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>>;
type ActionFn<T> = for<'a> fn(&'a T, &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>>;

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
    fetch: FetchFn<T>,
    action: ActionFn<T>,
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

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use tokio::sync::Mutex;

    use super::*;

    struct MockIndexer {
        index_batches: Mutex<Vec<Vec<OaiRecordId>>>,
        purge_batches: Mutex<Vec<Vec<OaiRecordId>>>,
        fail_index_ids: HashSet<String>,
        fail_purge_ids: HashSet<String>,
        index_calls: Arc<AtomicUsize>,
        purge_calls: Arc<AtomicUsize>,
    }

    impl MockIndexer {
        fn new(
            index_batches: Vec<Vec<OaiRecordId>>,
            purge_batches: Vec<Vec<OaiRecordId>>,
            fail_index_ids: HashSet<String>,
            fail_purge_ids: HashSet<String>,
        ) -> Self {
            Self {
                index_batches: Mutex::new(index_batches),
                purge_batches: Mutex::new(purge_batches),
                fail_index_ids,
                fail_purge_ids,
                index_calls: Arc::new(AtomicUsize::new(0)),
                purge_calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn record(identifier: &str) -> OaiRecordId {
            OaiRecordId {
                identifier: identifier.to_string(),
                fingerprint: identifier.to_string(),
            }
        }
    }

    impl Indexer for MockIndexer {
        fn fetch_records_to_index<'a>(
            &'a self,
            _last_identifier: Option<&'a str>,
        ) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>> {
            Box::pin(async move {
                let mut guard = self.index_batches.lock().await;
                Ok(if guard.is_empty() {
                    vec![]
                } else {
                    guard.remove(0)
                })
            })
        }

        fn fetch_records_to_purge<'a>(
            &'a self,
            _last_identifier: Option<&'a str>,
        ) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>> {
            Box::pin(async move {
                let mut guard = self.purge_batches.lock().await;
                Ok(if guard.is_empty() {
                    vec![]
                } else {
                    guard.remove(0)
                })
            })
        }

        fn index_record<'a>(
            &'a self,
            record: &'a OaiRecordId,
        ) -> BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async move {
                self.index_calls.fetch_add(1, Ordering::SeqCst);
                if self.fail_index_ids.contains(&record.identifier) {
                    anyhow::bail!("index failed for {}", record.identifier);
                }
                Ok(())
            })
        }

        fn delete_record<'a>(
            &'a self,
            record: &'a OaiRecordId,
        ) -> BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async move {
                self.purge_calls.fetch_add(1, Ordering::SeqCst);
                if self.fail_purge_ids.contains(&record.identifier) {
                    anyhow::bail!("delete failed for {}", record.identifier);
                }
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn run_fails_when_any_record_fails_but_continues_processing() {
        let indexer = MockIndexer::new(
            vec![vec![MockIndexer::record("a"), MockIndexer::record("b")]],
            vec![vec![MockIndexer::record("c")]],
            HashSet::from([String::from("b")]),
            HashSet::new(),
        );

        let result = run(&indexer).await;

        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(message.contains("1 failed record"));
        assert_eq!(indexer.index_calls.load(Ordering::SeqCst), 2);
        assert_eq!(indexer.purge_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_succeeds_when_all_records_succeed() {
        let indexer = MockIndexer::new(
            vec![vec![MockIndexer::record("a")]],
            vec![vec![MockIndexer::record("b")]],
            HashSet::new(),
            HashSet::new(),
        );

        let result = run(&indexer).await;

        assert!(result.is_ok());
        assert_eq!(indexer.index_calls.load(Ordering::SeqCst), 1);
        assert_eq!(indexer.purge_calls.load(Ordering::SeqCst), 1);
    }
}
