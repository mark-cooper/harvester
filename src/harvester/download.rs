use std::path::PathBuf;

use futures::stream::{self, StreamExt};
use oai_pmh::{Client, GetRecordArgs};
use tokio::fs;

use crate::harvester::oai::OaiRecord;

use super::Harvester;

const BATCH_SIZE: usize = 100;
const CONCURRENT_DOWNLOADS: usize = 10;

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let client = Client::new(&harvester.config.endpoint)?;
    let mut last_identifier: Option<String> = None;
    let mut total_downloaded = 0usize;

    loop {
        let batch = fetch_pending_records(harvester, last_identifier.as_deref()).await?;
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

async fn fetch_pending_records(
    harvester: &Harvester,
    last_identifier: Option<&str>,
) -> anyhow::Result<Vec<OaiRecord>> {
    Ok(match last_identifier {
        Some(last_id) => {
            sqlx::query_as!(
                OaiRecord,
                r#"
                SELECT endpoint, metadata_prefix, identifier, fingerprint AS "fingerprint!", status
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND identifier > $3
                  AND status = 'pending'
                ORDER BY identifier
                LIMIT 100
                "#,
                &harvester.config.endpoint,
                &harvester.config.metadata_prefix,
                last_id
            )
            .fetch_all(&harvester.pool)
            .await?
        }
        None => {
            sqlx::query_as!(
                OaiRecord,
                r#"
                SELECT endpoint, metadata_prefix, identifier, fingerprint AS "fingerprint!", status
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND status = 'pending'
                ORDER BY identifier
                LIMIT 100
                "#,
                &harvester.config.endpoint,
                &harvester.config.metadata_prefix,
            )
            .fetch_all(&harvester.pool)
            .await?
        }
    })
}

async fn process_batch(
    client: &Client,
    harvester: &Harvester,
    records: &[OaiRecord],
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
    record: &OaiRecord,
) -> anyhow::Result<()> {
    let args = GetRecordArgs::new(&record.identifier, &harvester.config.metadata_prefix);

    match client.get_record(args).await {
        Ok(response) => {
            if let Some(payload) = response.payload {
                let metadata = &payload.record.metadata;
                write_metadata_to_file(&record.path(), metadata).await?;
                update_record_available(harvester, &record.identifier).await?;
            }
        }
        Err(e) => {
            update_record_failed(harvester, &record.identifier, &e.to_string()).await?;
        }
    }

    Ok(())
}

async fn write_metadata_to_file(path: &PathBuf, metadata: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    fs::write(&path, metadata).await?;
    Ok(())
}

async fn update_record_available(harvester: &Harvester, identifier: &str) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        UPDATE oai_records
        SET status = 'available', last_checked_at = NOW()
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
        &harvester.config.endpoint,
        &harvester.config.metadata_prefix,
        identifier
    )
    .execute(&harvester.pool)
    .await?;
    Ok(())
}

async fn update_record_failed(
    harvester: &Harvester,
    identifier: &str,
    message: &str,
) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        UPDATE oai_records
        SET status = 'failed', message = $4, last_checked_at = NOW()
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
        &harvester.config.endpoint,
        &harvester.config.metadata_prefix,
        identifier,
        message
    )
    .execute(&harvester.pool)
    .await?;
    Ok(())
}
