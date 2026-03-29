mod harvester;
mod indexer;
mod summarizer;

pub(crate) use harvester::{ImportParams, ImportStats, batch_upsert_records};
pub use harvester::{
    RecordTransitionParams, RetryHarvestParams, apply_harvest_event, apply_harvest_retry,
};
pub use indexer::{
    FetchIndexCandidatesParams, UpdateIndexStatusParams, apply_index_event,
    fetch_failed_records_for_indexing, fetch_failed_records_for_purging,
    fetch_pending_records_for_indexing, fetch_pending_records_for_purging,
};
pub use indexer::{ReindexStateParams, apply_reindex};

use sqlx::postgres::PgPoolOptions;
use sqlx::{Error, PgPool};

use crate::{OaiRecordId, oai::OaiRecordStatus};

pub(crate) struct FetchRecordsParams<'a> {
    pub(crate) endpoint: &'a str,
    pub(crate) metadata_prefix: &'a str,
    pub(crate) status: OaiRecordStatus,
    pub(crate) last_identifier: Option<&'a str>,
}

pub async fn create_pool(database_url: &str, max_connections: u32) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await?;

    sqlx::migrate!().run(&pool).await?;

    Ok(pool)
}

pub(crate) async fn fetch_records_by_status(
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
                params.status.as_str()
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
                params.status.as_str()
            )
            .fetch_all(pool)
            .await
        }
    }
}
