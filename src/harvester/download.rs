use std::path::PathBuf;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use oai_pmh::client::response::GetRecordResponse;
use oai_pmh::{Client, GetRecordArgs};
use tokio::fs;
use tokio::time::timeout;
use tracing::warn;

use crate::harvester::{BatchStats, CONCURRENT_DOWNLOADS};
use crate::oai::{HarvestEvent, OaiRecord, OaiRecordStatus};

use super::Harvester;

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let client = Client::new(&harvester.config.scope.endpoint)?;
    harvester
        .batched(OaiRecordStatus::Pending, "Downloaded", async |batch| {
            process_batch(&client, harvester, batch).await
        })
        .await
}

async fn process_batch(
    client: &Client,
    harvester: &Harvester,
    records: &[OaiRecord],
) -> BatchStats {
    let results: Vec<_> = stream::iter(records)
        .map(|record| process_record(client, harvester, record))
        .buffer_unordered(CONCURRENT_DOWNLOADS)
        .collect()
        .await;

    BatchStats::from_results(results)
}

async fn process_record(
    client: &Client,
    harvester: &Harvester,
    record: &OaiRecord,
) -> anyhow::Result<bool> {
    let outcome = fetch_and_write(client, harvester, record).await;
    let event = match &outcome {
        Ok(()) => HarvestEvent::DownloadSucceeded,
        Err(message) => HarvestEvent::DownloadFailed { message },
    };
    harvester.update(record, &event).await
}

async fn fetch_and_write(
    client: &Client,
    harvester: &Harvester,
    record: &OaiRecord,
) -> Result<(), String> {
    let response = fetch_with_retries(client, harvester, record).await?;

    let payload = response
        .payload
        .ok_or_else(|| "OAI response missing payload".to_string())?;

    let path = harvester.config.data_dir.join(record.path());
    write_metadata_to_file(path, &payload.record.metadata)
        .await
        .map_err(|e| format!("Failed to write metadata file: {}", e))
}

async fn fetch_with_retries(
    client: &Client,
    harvester: &Harvester,
    record: &OaiRecord,
) -> Result<GetRecordResponse, String> {
    let duration = Duration::from_secs(harvester.config.oai_timeout);
    let max_retries = harvester.config.oai_retries;
    let mut attempts = 0u32;

    loop {
        let args = GetRecordArgs::new(&record.identifier, &harvester.config.scope.metadata_prefix);
        match timeout(duration, client.get_record(args)).await {
            Ok(Ok(response)) => return Ok(response),
            Ok(Err(error)) => return Err(format!("Failed to fetch OAI record: {}", error)),
            Err(_) if attempts < max_retries => {
                attempts += 1;
                warn!(
                    "OAI get_record timed out for {}, retry {}/{}",
                    record.identifier, attempts, max_retries
                );
                tokio::time::sleep(Duration::from_millis(500 * 2u64.pow(attempts - 1))).await;
            }
            Err(_) => {
                return Err(format!(
                    "OAI get_record timed out after {}s",
                    harvester.config.oai_timeout
                ));
            }
        }
    }
}

async fn write_metadata_to_file(path: PathBuf, metadata: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    fs::write(&path, metadata).await?;
    Ok(())
}
