use std::time::Duration;

use tracing::info;

use crate::{
    db::harvester::{ImportParams, ImportStats, batch_upsert_records},
    oai::OaiRecordImport,
};

use super::Harvester;

use oai_pmh::{Client, ListIdentifiersArgs};

const BATCH_SIZE: usize = 100;

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let total = process(harvester).await?;
    info!(
        "Imported {} records (active: {}, deleted: {})",
        total.processed, total.imported, total.deleted
    );
    Ok(())
}

async fn process(harvester: &Harvester) -> anyhow::Result<ImportStats> {
    let duration = Duration::from_secs(harvester.config.oai_timeout);
    let client = Client::new(&harvester.config.endpoint)?;

    oai_timeout("identify", duration, client.identify()).await??;

    let args = ListIdentifiersArgs::new(&harvester.config.metadata_prefix);
    let mut stream =
        oai_timeout("list_identifiers", duration, client.list_identifiers(args)).await??;

    let params = ImportParams {
        endpoint: &harvester.config.endpoint,
        metadata_prefix: &harvester.config.metadata_prefix,
    };
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut total = ImportStats::default();

    while let Some(response) =
        oai_timeout("list_identifiers page fetch", duration, stream.try_next()).await??
    {
        if harvester.is_shutdown() {
            break;
        }
        if let Some(e) = response.error {
            return Err(anyhow::anyhow!("OAI-PMH error: {:?}", e));
        }

        if let Some(payload) = response.payload {
            for header in payload.header {
                batch.push(OaiRecordImport::from(header));
                if batch.len() >= BATCH_SIZE {
                    let stats = batch_upsert_records(&harvester.pool, params, &batch).await?;
                    total.accumulate(&stats);
                    batch.clear();
                }
            }
        }
    }

    if !batch.is_empty() {
        let stats = batch_upsert_records(&harvester.pool, params, &batch).await?;
        total.accumulate(&stats);
    }

    Ok(total)
}

async fn oai_timeout<T>(
    label: &str,
    duration: Duration,
    future: impl Future<Output = T>,
) -> anyhow::Result<T> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| anyhow::anyhow!("OAI {} timed out after {}s", label, duration.as_secs()))
}
