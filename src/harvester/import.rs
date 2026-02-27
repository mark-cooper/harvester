use std::time::Duration;

use tokio::time::timeout;
use tracing::info;

use crate::oai::{OaiIndexStatus, OaiRecordImport, OaiRecordStatus};

use super::Harvester;

use oai_pmh::{Client, ListIdentifiersArgs};

const BATCH_SIZE: usize = 100;

#[derive(Default)]
struct ImportStats {
    processed: usize,
    imported: usize,
    deleted: usize,
}

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let duration = Duration::from_secs(harvester.config.oai_timeout);
    let client = Client::new(&harvester.config.endpoint)?;

    timeout(duration, client.identify()).await.map_err(|_| {
        anyhow::anyhow!(
            "OAI identify timed out after {}s",
            harvester.config.oai_timeout
        )
    })??;

    let args = ListIdentifiersArgs::new(&harvester.config.metadata_prefix);
    let mut stream = timeout(duration, client.list_identifiers(args))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "OAI list_identifiers timed out after {}s",
                harvester.config.oai_timeout
            )
        })??;

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut total = ImportStats::default();

    while let Some(response) = timeout(duration, stream.try_next()).await.map_err(|_| {
        anyhow::anyhow!(
            "OAI list_identifiers page fetch timed out after {}s",
            harvester.config.oai_timeout
        )
    })?? {
        if let Some(e) = response.error {
            return Err(anyhow::anyhow!("OAI-PMH error: {:?}", e));
        }

        if let Some(payload) = response.payload {
            for header in payload.header {
                batch.push(OaiRecordImport::from(header));

                if batch.len() >= BATCH_SIZE {
                    let stats = batch_upsert_records(harvester, &batch).await?;
                    total.processed += stats.processed;
                    total.imported += stats.imported;
                    total.deleted += stats.deleted;
                    batch.clear();
                }
            }
        }
    }

    if !batch.is_empty() {
        let stats = batch_upsert_records(harvester, &batch).await?;
        total.processed += stats.processed;
        total.imported += stats.imported;
        total.deleted += stats.deleted;
    }

    info!(
        "Imported {} records (active: {}, deleted: {})",
        total.processed, total.imported, total.deleted
    );
    Ok(())
}

async fn batch_upsert_records(
    harvester: &Harvester,
    records: &[OaiRecordImport],
) -> anyhow::Result<ImportStats> {
    if records.is_empty() {
        return Ok(ImportStats::default());
    }

    let identifiers: Vec<_> = records.iter().map(|r| r.identifier.as_str()).collect();
    let datestamps: Vec<_> = records.iter().map(|r| r.datestamp.as_str()).collect();
    let statuses: Vec<_> = records.iter().map(|r| r.status.as_str()).collect();
    let batch_len = records.len() as i32;

    let statuses = sqlx::query_scalar::<_, String>(
        r#"
        INSERT INTO oai_records (
            endpoint, metadata_prefix, identifier, datestamp, status, message, last_checked_at
        )
        SELECT * FROM UNNEST(
            ARRAY_FILL($1::text, ARRAY[$6]),
            ARRAY_FILL($2::text, ARRAY[$6]),
            $3::text[],
            $4::text[],
            $5::text[],
            ARRAY_FILL(''::text, ARRAY[$6]),
            ARRAY_FILL(NOW(), ARRAY[$6])
        )
        ON CONFLICT (endpoint, metadata_prefix, identifier) DO UPDATE SET
            datestamp = EXCLUDED.datestamp,
            status = EXCLUDED.status,
            message = '',
            index_status = CASE
                WHEN EXCLUDED.status = $8 THEN $9
                ELSE oai_records.index_status
            END,
            index_message = CASE
                WHEN EXCLUDED.status = $8 THEN ''
                ELSE oai_records.index_message
            END,
            purged_at = CASE
                WHEN EXCLUDED.status = $8 THEN NULL
                ELSE oai_records.purged_at
            END,
            index_last_checked_at = CASE
                WHEN EXCLUDED.status = $8 THEN NULL
                ELSE oai_records.index_last_checked_at
            END,
            version = oai_records.version + 1,
            last_checked_at = EXCLUDED.last_checked_at
        WHERE oai_records.status != $7
        AND oai_records.datestamp != EXCLUDED.datestamp
        RETURNING status
        "#,
    )
    .bind(&harvester.config.endpoint)
    .bind(&harvester.config.metadata_prefix)
    .bind(&identifiers)
    .bind(&datestamps)
    .bind(&statuses)
    .bind(batch_len)
    .bind(OaiRecordStatus::Failed.as_str())
    .bind(OaiRecordStatus::Deleted.as_str())
    .bind(OaiIndexStatus::Pending.as_str())
    .fetch_all(&harvester.pool)
    .await?;

    let deleted = statuses
        .iter()
        .filter(|status| status.as_str() == OaiRecordStatus::Deleted.as_str())
        .count();
    let processed = statuses.len();
    let imported = processed.saturating_sub(deleted);

    Ok(ImportStats {
        processed,
        imported,
        deleted,
    })
}
