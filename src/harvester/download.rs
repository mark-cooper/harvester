use std::path::PathBuf;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use oai_pmh::{Client, GetRecordArgs};
use tokio::fs;
use tokio::time::timeout;
use tracing::{error, info, warn};

use crate::db::{
    FetchRecordsParams, RecordFailureParams, RecordTransitionParams,
    do_mark_download_failure_query, do_mark_download_success_query, fetch_records_by_status,
};
use crate::oai::{OaiRecordId, OaiRecordStatus};

use super::Harvester;

const BATCH_SIZE: usize = 100;
const CONCURRENT_DOWNLOADS: usize = 10;

#[derive(Default)]
struct BatchStats {
    downloaded: usize,
    failed: usize,
    failed_to_mark: usize,
}

enum RecordResult {
    Downloaded,
    Failed,
    FailedToMark,
}

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let client = Client::new(&harvester.config.endpoint)?;
    let mut last_identifier: Option<String> = None;
    let mut total_downloaded = 0usize;
    let mut total_failed = 0usize;
    let mut total_failed_to_mark = 0usize;

    loop {
        let params = FetchRecordsParams {
            endpoint: &harvester.config.endpoint,
            metadata_prefix: &harvester.config.metadata_prefix,
            status: OaiRecordStatus::Pending,
            last_identifier: last_identifier.as_deref(),
        };
        let batch = fetch_records_by_status(&harvester.pool, params).await?;
        if batch.is_empty() {
            break;
        }

        last_identifier = batch.last().map(|r| r.identifier.clone());
        let batch_stats = process_batch(&client, harvester, &batch).await;
        total_downloaded += batch_stats.downloaded;
        total_failed += batch_stats.failed;
        total_failed_to_mark += batch_stats.failed_to_mark;

        if batch.len() < BATCH_SIZE {
            break;
        }
    }

    info!(
        "Downloaded {} records (failed: {}, failed-to-mark: {})",
        total_downloaded, total_failed, total_failed_to_mark
    );
    Ok(())
}

async fn process_batch(
    client: &Client,
    harvester: &Harvester,
    records: &[OaiRecordId],
) -> BatchStats {
    let results: Vec<_> = stream::iter(records)
        .map(|record| download_record(client, harvester, record))
        .buffer_unordered(CONCURRENT_DOWNLOADS)
        .collect()
        .await;

    let mut stats = BatchStats::default();
    for result in results {
        match result {
            RecordResult::Downloaded => stats.downloaded += 1,
            RecordResult::Failed => stats.failed += 1,
            RecordResult::FailedToMark => stats.failed_to_mark += 1,
        }
    }

    stats
}

async fn download_record(
    client: &Client,
    harvester: &Harvester,
    record: &OaiRecordId,
) -> RecordResult {
    let duration = Duration::from_secs(harvester.config.oai_timeout);
    let max_retries = harvester.config.oai_retries;

    let result = {
        let mut attempts = 0u32;
        loop {
            let args = GetRecordArgs::new(&record.identifier, &harvester.config.metadata_prefix);
            match timeout(duration, client.get_record(args)).await {
                Ok(result) => break result,
                Err(_) if attempts < max_retries => {
                    attempts += 1;
                    warn!(
                        "OAI get_record timed out for {}, retry {}/{}",
                        record.identifier, attempts, max_retries
                    );
                    tokio::time::sleep(Duration::from_millis(500 * 2u64.pow(attempts - 1))).await;
                }
                Err(_) => {
                    let message = format!(
                        "OAI get_record timed out after {}s",
                        harvester.config.oai_timeout
                    );
                    return mark_record_failed(harvester, record, &message).await;
                }
            }
        }
    };

    match result {
        Ok(response) => match response.payload {
            Some(payload) => {
                let metadata = &payload.record.metadata;
                let path = harvester.config.data_dir.join(record.path());

                if let Err(error) = write_metadata_to_file(path, metadata).await {
                    let message = format!("Failed to write metadata file: {}", error);
                    return mark_record_failed(harvester, record, &message).await;
                }

                match mark_record_available(harvester, record).await {
                    Ok(true) => RecordResult::Downloaded,
                    Ok(false) => RecordResult::FailedToMark,
                    Err(error) => {
                        error!(
                            "Record {} downloaded but could not be marked available: {}",
                            record.identifier, error
                        );
                        RecordResult::FailedToMark
                    }
                }
            }
            None => mark_record_failed(harvester, record, "OAI response missing payload").await,
        },
        Err(error) => {
            let message = format!("Failed to fetch OAI record: {}", error);
            mark_record_failed(harvester, record, &message).await
        }
    }
}

async fn mark_record_failed(
    harvester: &Harvester,
    record: &OaiRecordId,
    message: &str,
) -> RecordResult {
    let params = RecordFailureParams {
        endpoint: &harvester.config.endpoint,
        metadata_prefix: &harvester.config.metadata_prefix,
        identifier: &record.identifier,
        message,
    };

    match do_mark_download_failure_query(&harvester.pool, params).await {
        Ok(result) if result.rows_affected() == 1 => RecordResult::Failed,
        Ok(_) => {
            warn!(
                "Skipped transition pending->failed for {} (record is no longer pending)",
                record.identifier
            );
            RecordResult::FailedToMark
        }
        Err(error) => {
            error!(
                "Record {} failed and could not be marked failed (reason: {}; update error: {})",
                record.identifier, message, error
            );
            RecordResult::FailedToMark
        }
    }
}

async fn mark_record_available(
    harvester: &Harvester,
    record: &OaiRecordId,
) -> anyhow::Result<bool> {
    let params = RecordTransitionParams {
        endpoint: &harvester.config.endpoint,
        metadata_prefix: &harvester.config.metadata_prefix,
        identifier: &record.identifier,
    };

    let result = do_mark_download_success_query(&harvester.pool, params).await?;
    if result.rows_affected() == 0 {
        warn!(
            "Skipped transition pending->available for {} (record is no longer pending)",
            record.identifier
        );
        return Ok(false);
    }

    Ok(true)
}

async fn write_metadata_to_file(path: PathBuf, metadata: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    fs::write(&path, metadata).await?;
    Ok(())
}
