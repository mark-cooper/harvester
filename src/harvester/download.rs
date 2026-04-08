use std::path::PathBuf;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use oai_pmh::{Client, GetRecordArgs};
use tokio::fs;
use tokio::time::timeout;
use tracing::warn;

use crate::harvester::{BatchStats, CONCURRENT_DOWNLOADS};
use crate::oai::{HarvestEvent, OaiRecordId, OaiRecordStatus};

use super::Harvester;

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let client = Client::new(&harvester.config.endpoint)?;
    harvester
        .batched(OaiRecordStatus::Pending, "Downloaded", async |batch| {
            process_batch(&client, harvester, batch).await
        })
        .await
}

async fn process_batch(
    client: &Client,
    harvester: &Harvester,
    records: &[OaiRecordId],
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
    record: &OaiRecordId,
) -> anyhow::Result<bool> {
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
                    return harvester
                        .update(record, &HarvestEvent::DownloadFailed { message: &message })
                        .await;
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
                    harvester
                        .update(
                            record,
                            &HarvestEvent::DownloadFailed {
                                message: format!("Failed to write metadata file: {}", error)
                                    .as_str(),
                            },
                        )
                        .await
                } else {
                    harvester
                        .update(record, &HarvestEvent::DownloadSucceeded)
                        .await
                }
            }
            None => {
                harvester
                    .update(
                        record,
                        &HarvestEvent::DownloadFailed {
                            message: "OAI response missing payload",
                        },
                    )
                    .await
            }
        },
        Err(error) => {
            harvester
                .update(
                    record,
                    &HarvestEvent::DownloadFailed {
                        message: format!("Failed to fetch OAI record: {}", error).as_str(),
                    },
                )
                .await
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
