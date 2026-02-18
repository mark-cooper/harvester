pub mod harvester;
pub mod indexer;
pub mod summarizer;

pub use harvester::*;
pub use indexer::*;

use sqlx::postgres::PgPoolOptions;
use sqlx::{Error, PgPool};

use crate::OaiRecordId;

pub struct FetchRecordsParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub status: &'a str,
    pub last_identifier: Option<&'a str>,
}

pub async fn create_pool(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    sqlx::migrate!().run(&pool).await?;

    Ok(pool)
}

pub async fn fetch_records_by_status(
    pool: &PgPool,
    params: FetchRecordsParams<'_>,
) -> Result<Vec<OaiRecordId>, Error> {
    match params.last_identifier {
        Some(last_id) => {
            sqlx::query_as!(
                OaiRecordId,
                r#"
                SELECT identifier, fingerprint AS "fingerprint!"
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND identifier > $3
                  AND status = $4
                ORDER BY identifier
                LIMIT 100
                "#,
                params.endpoint,
                params.metadata_prefix,
                last_id,
                params.status
            )
            .fetch_all(pool)
            .await
        }
        None => {
            sqlx::query_as!(
                OaiRecordId,
                r#"
                SELECT identifier, fingerprint AS "fingerprint!"
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND status = $3
                ORDER BY identifier
                LIMIT 100
                "#,
                params.endpoint,
                params.metadata_prefix,
                params.status
            )
            .fetch_all(pool)
            .await
        }
    }
}
