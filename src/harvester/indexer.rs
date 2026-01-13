use crate::{Indexer, harvester::oai::OaiRecordId};

const BATCH_SIZE: usize = 100;

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    repository: String,
    oai_endpoint: String,
    oai_repository: String,
    solr_url: String,
}

impl IndexerConfig {
    // TODO: constructor supports validation later if necessary
    // (for example: parse endpoint/url as uri etc.)
    pub fn new(
        repository: String,
        oai_endpoint: String,
        oai_repository: String,
        solr_url: String,
    ) -> Self {
        Self {
            repository,
            oai_endpoint,
            oai_repository,
            solr_url,
        }
    }
}

pub async fn run(indexer: &Indexer) -> anyhow::Result<()> {
    let mut last_identifier: Option<String> = None;
    let mut total_indexed = 0usize;
    let mut total_deleted = 0usize;

    loop {
        let batch = fetch_records_to_index(indexer, last_identifier.as_deref()).await?;
        if batch.is_empty() {
            break;
        }

        last_identifier = batch.last().map(|r| r.identifier.clone());
        total_indexed += index_batch(indexer, &batch).await?;

        if batch.len() < BATCH_SIZE {
            break;
        }
    }

    loop {
        let batch = fetch_records_to_delete(indexer, last_identifier.as_deref()).await?;
        if batch.is_empty() {
            break;
        }

        last_identifier = batch.last().map(|r| r.identifier.clone());
        total_deleted += delete_batch(indexer, &batch).await?;

        if batch.len() < BATCH_SIZE {
            break;
        }
    }

    println!("Indexed records: {}", total_indexed);
    println!("Deleted records: {}", total_deleted);
    Ok(())
}

async fn fetch_records_to_index(
    indexer: &Indexer,
    last_identifier: Option<&str>,
) -> anyhow::Result<Vec<OaiRecordId>> {
    Ok(match last_identifier {
        Some(last_id) => {
            sqlx::query_as!(
                OaiRecordId,
                r#"
                SELECT identifier, fingerprint AS "fingerprint!"
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = 'oai_ead'
                  AND metadata->'repository' ? $2
                  AND identifier > $3
                  AND status = 'available'
                ORDER BY identifier
                LIMIT 100
                "#,
                &indexer.config.oai_endpoint,
                &indexer.config.oai_repository,
                last_id
            )
            .fetch_all(&indexer.pool)
            .await?
        }
        None => {
            sqlx::query_as!(
                OaiRecordId,
                r#"
                SELECT identifier, fingerprint AS "fingerprint!"
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = 'oai_ead'
                  AND metadata->'repository' ? $2
                  AND status = 'available'
                ORDER BY identifier
                LIMIT 100
                "#,
                &indexer.config.oai_endpoint,
                &indexer.config.oai_repository,
            )
            .fetch_all(&indexer.pool)
            .await?
        }
    })
}

async fn fetch_records_to_delete(
    indexer: &Indexer,
    last_identifier: Option<&str>,
) -> anyhow::Result<Vec<OaiRecordId>> {
    Ok(match last_identifier {
        Some(last_id) => {
            sqlx::query_as!(
                OaiRecordId,
                r#"
                SELECT identifier, fingerprint AS "fingerprint!"
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = 'oai_ead'
                  AND metadata->'repository' ? $2
                  AND identifier > $3
                  AND status = 'deleted'
                ORDER BY identifier
                LIMIT 100
                "#,
                &indexer.config.oai_endpoint,
                &indexer.config.oai_repository,
                last_id
            )
            .fetch_all(&indexer.pool)
            .await?
        }
        None => {
            sqlx::query_as!(
                OaiRecordId,
                r#"
                SELECT identifier, fingerprint AS "fingerprint!"
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = 'oai_ead'
                  AND metadata->'repository' ? $2
                  AND status = 'deleted'
                ORDER BY identifier
                LIMIT 100
                "#,
                &indexer.config.oai_endpoint,
                &indexer.config.oai_repository,
            )
            .fetch_all(&indexer.pool)
            .await?
        }
    })
}

async fn index_batch(_: &Indexer, batch: &[OaiRecordId]) -> anyhow::Result<usize> {
    let mut indexed = 0usize;

    for record in batch {
        println!("Indexing: {}", record.identifier);

        indexed += 1;
    }

    Ok(indexed)
}

async fn delete_batch(_: &Indexer, batch: &[OaiRecordId]) -> anyhow::Result<usize> {
    let mut indexed = 0usize;

    for record in batch {
        println!("Deleting: {}", record.identifier);

        indexed += 1;
    }

    Ok(indexed)
}
