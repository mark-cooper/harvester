use std::time::Duration;

use tokio::time::timeout;
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

    let params = ImportParams {
        endpoint: &harvester.config.endpoint,
        metadata_prefix: &harvester.config.metadata_prefix,
    };
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut total = ImportStats::default();

    while let Some(response) = timeout(duration, stream.try_next()).await.map_err(|_| {
        anyhow::anyhow!(
            "OAI list_identifiers page fetch timed out after {}s",
            harvester.config.oai_timeout
        )
    })?? {
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
