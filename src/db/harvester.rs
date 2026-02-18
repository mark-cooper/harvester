use sqlx::postgres::PgQueryResult;
use sqlx::{Error, PgPool};

use crate::oai::{OaiIndexStatus, OaiRecordStatus};

pub struct RecordTransitionParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub identifier: &'a str,
}

pub struct RecordFailureParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub identifier: &'a str,
    pub message: &'a str,
}

pub struct RecordParsedParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub identifier: &'a str,
    pub metadata: serde_json::Value,
}

/// Transition: `pending -> available`.
pub async fn do_mark_download_success_query(
    pool: &PgPool,
    params: RecordTransitionParams<'_>,
) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        UPDATE oai_records
        SET status = $4, message = '', last_checked_at = NOW()
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND identifier = $3
          AND status = $5
        "#,
    )
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(params.identifier)
    .bind(OaiRecordStatus::Available.as_str())
    .bind(OaiRecordStatus::Pending.as_str())
    .execute(pool)
    .await
}

/// Transition: `pending -> failed`.
pub async fn do_mark_download_failure_query(
    pool: &PgPool,
    params: RecordFailureParams<'_>,
) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        UPDATE oai_records
        SET status = $4, message = $5, last_checked_at = NOW()
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND identifier = $3
          AND status = $6
        "#,
    )
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(params.identifier)
    .bind(OaiRecordStatus::Failed.as_str())
    .bind(params.message)
    .bind(OaiRecordStatus::Pending.as_str())
    .execute(pool)
    .await
}

/// Transition: `available -> failed`.
pub async fn do_mark_metadata_failure_query(
    pool: &PgPool,
    params: RecordFailureParams<'_>,
) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        UPDATE oai_records
        SET status = $4, message = $5, last_checked_at = NOW()
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND identifier = $3
          AND status = $6
        "#,
    )
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(params.identifier)
    .bind(OaiRecordStatus::Failed.as_str())
    .bind(params.message)
    .bind(OaiRecordStatus::Available.as_str())
    .execute(pool)
    .await
}

/// Transition: `available -> parsed` and `index_status -> pending`.
pub async fn do_mark_metadata_success_query(
    pool: &PgPool,
    params: RecordParsedParams<'_>,
) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        UPDATE oai_records
        SET status = $4,
            metadata = $5,
            message = '',
            index_status = $6,
            index_message = '',
            index_attempts = 0,
            indexed_at = NULL,
            purged_at = NULL,
            index_last_checked_at = NULL,
            last_checked_at = NOW()
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND identifier = $3
          AND status = $7
        "#,
    )
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(params.identifier)
    .bind(OaiRecordStatus::Parsed.as_str())
    .bind(params.metadata)
    .bind(OaiIndexStatus::Pending.as_str())
    .bind(OaiRecordStatus::Available.as_str())
    .execute(pool)
    .await
}
