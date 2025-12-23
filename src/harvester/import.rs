use crate::harvester::oai::OaiRecordImport;

use super::Harvester;

use oai_pmh::{Client, ListIdentifiersArgs};

const BATCH_SIZE: usize = 100;

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let client = Client::new(&harvester.config.endpoint)?;
    client.identify().await?;

    let args = ListIdentifiersArgs::new(&harvester.config.metadata_prefix);
    let mut stream = client.list_identifiers(args).await?;

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut total_processed = 0;

    while let Some(result) = stream.next().await {
        let response = result?;
        if let Some(e) = response.error {
            return Err(anyhow::anyhow!("OAI-PMH error: {:?}", e));
        }

        if let Some(payload) = response.payload {
            for header in payload.header {
                batch.push(OaiRecordImport::from(header));

                if batch.len() >= BATCH_SIZE {
                    total_processed += batch_upsert_records(harvester, &batch).await?;
                    batch.clear();
                }
            }
        }
    }

    if !batch.is_empty() {
        total_processed += batch_upsert_records(harvester, &batch).await?;
    }

    println!("Processed {} records", total_processed);
    Ok(())
}

async fn batch_upsert_records(
    harvester: &Harvester,
    records: &[OaiRecordImport],
) -> anyhow::Result<usize> {
    if records.is_empty() {
        return Ok(0);
    }

    let identifiers: Vec<_> = records.iter().map(|r| r.identifier.as_str()).collect();
    let datestamps: Vec<_> = records.iter().map(|r| r.datestamp.as_str()).collect();
    let statuses: Vec<_> = records.iter().map(|r| r.status.as_str()).collect();
    let batch_len = records.len() as i32;

    let result = sqlx::query(
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
            version = oai_records.version + 1,
            last_checked_at = EXCLUDED.last_checked_at
        WHERE oai_records.status != 'failed'
        AND oai_records.datestamp != EXCLUDED.datestamp
        "#,
    )
    .bind(&harvester.config.endpoint)
    .bind(&harvester.config.metadata_prefix)
    .bind(&identifiers)
    .bind(&datestamps)
    .bind(&statuses)
    .bind(batch_len)
    .execute(&harvester.pool)
    .await?;

    Ok(result.rows_affected() as usize)
}
