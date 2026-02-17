use sqlx::postgres::PgQueryResult;
use sqlx::{Error, PgPool};

use crate::oai::OaiRecordId;

pub struct FetchRecordsParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub status: &'a str,
    pub last_identifier: Option<&'a str>,
}

pub struct UpdateStatusParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub identifier: &'a str,
    pub status: &'a str,
    pub message: &'a str,
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

pub async fn do_update_status_query(
    pool: &PgPool,
    params: UpdateStatusParams<'_>,
) -> Result<PgQueryResult, Error> {
    sqlx::query!(
        r#"
        UPDATE oai_records
        SET status = $4, message = $5, last_checked_at = NOW()
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
        params.endpoint,
        params.metadata_prefix,
        params.identifier,
        params.status,
        params.message
    )
    .execute(pool)
    .await
}
