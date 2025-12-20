use crate::harvester::oai::{OaiConfig, OaiRecord};

use super::Harvester;

use oai_pmh::{Client, ListIdentifiersArgs};
use sqlx::PgPool;

pub(super) async fn run(harvester: &Harvester) -> anyhow::Result<()> {
    let config = harvester.config().clone();
    let pool = harvester.pool().clone();

    // OAI-PMH client is sync, so run in spawn_blocking and collect records
    let records = tokio::task::spawn_blocking(move || fetch_records(&config)).await??;
    process_records(&pool, records).await?;

    Ok(())
}

fn fetch_records(config: &OaiConfig) -> anyhow::Result<Vec<OaiRecord>> {
    let client = Client::new(config.endpoint.as_str())?;
    client.identify()?;

    let mut records = Vec::new();
    let args = ListIdentifiersArgs::new(config.metadata_prefix.as_str());

    for response in client.list_identifiers(args)? {
        match response {
            Ok(response) => {
                if let Some(e) = response.error {
                    return Err(anyhow::anyhow!("OAI-PMH request error: {:?}", e));
                }

                if let Some(payload) = response.payload {
                    for header in payload.header {
                        records.push(OaiRecord::from_header(header, config));
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    println!("Fetched {} records", records.len());
    Ok(records)
}

async fn process_records(pool: &PgPool, records: Vec<OaiRecord>) -> anyhow::Result<()> {
    // Placeholder: verify DB connection works
    let row: (i32,) = sqlx::query_as("SELECT 1").fetch_one(pool).await?;
    println!("DB check: {}", row.0);

    for record in &records {
        println!("{:?}", record);
    }

    // TODO: batch upsert
    // - Collect into batches of 100
    // - Check exists / status / datestamp
    // - INSERT or UPDATE as needed

    println!("Processed {} records", records.len());
    Ok(())
}
