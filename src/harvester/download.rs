use std::path::PathBuf;

use futures::stream::{self, StreamExt};
use oai_pmh::{Client, GetRecordArgs};
use tokio::fs;

use crate::db::{
    FetchRecordsParams, UpdateStatusParams, do_update_status_query, fetch_records_by_status,
};
use crate::harvester::oai::{OaiRecordId, OaiRecordStatus};

use super::Harvester;

const BATCH_SIZE: usize = 100;
const CONCURRENT_DOWNLOADS: usize = 10;

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let client = Client::new(&harvester.config.endpoint)?;
    let mut last_identifier: Option<String> = None;
    let mut total_downloaded = 0usize;

    loop {
        let params = FetchRecordsParams {
            endpoint: &harvester.config.endpoint,
            metadata_prefix: &harvester.config.metadata_prefix,
            status: OaiRecordStatus::PENDING.as_str(),
            last_identifier: last_identifier.as_deref(),
        };
        let batch = fetch_records_by_status(&harvester.pool, params).await?;
        if batch.is_empty() {
            break;
        }

        last_identifier = batch.last().map(|r| r.identifier.clone());
        total_downloaded += process_batch(&client, harvester, &batch).await?;

        if batch.len() < BATCH_SIZE {
            break;
        }
    }

    println!("Downloaded {} records", total_downloaded);
    Ok(())
}

async fn process_batch(
    client: &Client,
    harvester: &Harvester,
    records: &[OaiRecordId],
) -> anyhow::Result<usize> {
    let results: Vec<_> = stream::iter(records)
        .map(|record| download_record(client, harvester, record))
        .buffer_unordered(CONCURRENT_DOWNLOADS)
        .collect()
        .await;

    let success_count = results.iter().filter(|r| r.is_ok()).count();
    Ok(success_count)
}

async fn download_record(
    client: &Client,
    harvester: &Harvester,
    record: &OaiRecordId,
) -> anyhow::Result<()> {
    let args = GetRecordArgs::new(&record.identifier, &harvester.config.metadata_prefix);

    let mut params = UpdateStatusParams {
        endpoint: &harvester.config.endpoint,
        metadata_prefix: &harvester.config.metadata_prefix,
        identifier: &record.identifier,
        status: OaiRecordStatus::AVAILABLE.as_str(),
        message: "",
    };

    match client.get_record(args).await {
        Ok(response) => {
            if let Some(payload) = response.payload {
                let metadata = &payload.record.metadata;
                let path = PathBuf::from(harvester.config.data_dir.clone()).join(record.path());
                write_metadata_to_file(path, metadata).await?;
                do_update_status_query(&harvester.pool, params).await?;
            }
        }
        Err(e) => {
            let message = e.to_string();
            params.status = OaiRecordStatus::FAILED.as_str();
            params.message = &message;
            do_update_status_query(&harvester.pool, params).await?;
        }
    }

    Ok(())
}

async fn write_metadata_to_file(path: PathBuf, metadata: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    fs::write(&path, metadata).await?;
    Ok(())
}
