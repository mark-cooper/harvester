use crate::harvester::oai::{OaiConfig, OaiRecord};

use super::Harvester;

use oai_pmh::{Client, ListIdentifiersArgs};
use sqlx::PgPool;
use tokio::sync::mpsc;

const BATCH_SIZE: usize = 100;

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<Vec<OaiRecord>>(4);

    // Spawn blocking task for sync OAI-PMH fetching
    let fetch_handle = tokio::task::spawn_blocking({
        let config = harvester.config().clone();
        move || fetch_and_stream_records(&config, tx)
    });

    let mut total_processed = 0;
    while let Some(batch) = rx.recv().await {
        let count = batch_upsert_records(harvester.pool(), &batch).await?;
        total_processed += count;
    }

    fetch_handle.await??;

    println!("Processed {} records", total_processed);
    Ok(())
}

fn fetch_and_stream_records(
    config: &OaiConfig,
    tx: mpsc::Sender<Vec<OaiRecord>>,
) -> anyhow::Result<()> {
    let client = Client::new(config.endpoint.as_str())?;
    client.identify()?;

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let args = ListIdentifiersArgs::new(config.metadata_prefix.as_str());

    for response in client.list_identifiers(args)? {
        match response {
            Ok(response) => {
                if let Some(e) = response.error {
                    return Err(anyhow::anyhow!("OAI-PMH error: {:?}", e));
                }
                if let Some(payload) = response.payload {
                    for header in payload.header {
                        batch.push(OaiRecord::from_header(header, config));

                        if batch.len() >= BATCH_SIZE {
                            tx.blocking_send(std::mem::take(&mut batch))?;
                            batch = Vec::with_capacity(BATCH_SIZE);
                        }
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    // Send remaining records
    if !batch.is_empty() {
        tx.blocking_send(batch)?;
    }

    Ok(())
}

async fn batch_upsert_records(pool: &PgPool, records: &[OaiRecord]) -> anyhow::Result<usize> {
    if records.is_empty() {
        return Ok(0);
    }

    let endpoints: Vec<_> = records.iter().map(|r| r.endpoint.as_str()).collect();
    let prefixes: Vec<_> = records.iter().map(|r| r.metadata_prefix.as_str()).collect();
    let identifiers: Vec<_> = records.iter().map(|r| r.identifier.as_str()).collect();
    let datestamps: Vec<_> = records.iter().map(|r| r.datestamp.as_str()).collect();
    let statuses: Vec<_> = records.iter().map(|r| r.status.as_str()).collect();

    let result = sqlx::query(
        "INSERT INTO oai_records (
            endpoint, metadata_prefix, identifier, datestamp, status, message, last_checked_at
        )
        SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
            ARRAY_FILL(''::text, ARRAY[CARDINALITY($1)]),
            ARRAY_FILL(NOW(), ARRAY[CARDINALITY($1)]))
        ON CONFLICT (endpoint, metadata_prefix, identifier) DO UPDATE SET
            datestamp = EXCLUDED.datestamp,
            status = EXCLUDED.status,
            message = '',
            last_checked_at = EXCLUDED.last_checked_at
        WHERE oai_records.status != 'failed'
            AND oai_records.datestamp != EXCLUDED.datestamp",
    )
    .bind(&endpoints)
    .bind(&prefixes)
    .bind(&identifiers)
    .bind(&datestamps)
    .bind(&statuses)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() as usize)
}
