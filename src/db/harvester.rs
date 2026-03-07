use sqlx::postgres::PgQueryResult;
use sqlx::{Error, PgPool};

use crate::oai::{HarvestEvent, HarvestTransition, OaiIndexStatus};

pub struct RecordTransitionParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub identifier: &'a str,
}

pub struct RetryHarvestParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
}

/// Apply a harvest event to a single record.
/// Uses a static SQL query per event variant (no dynamic SQL construction).
pub async fn apply_harvest_event(
    pool: &PgPool,
    params: RecordTransitionParams<'_>,
    event: &HarvestEvent<'_>,
) -> Result<PgQueryResult, Error> {
    let HarvestTransition { from, to } = event.transition();

    match event {
        HarvestEvent::DownloadSucceeded => {
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
            .bind(to.as_str())
            .bind(from.as_str())
            .execute(pool)
            .await
        }
        HarvestEvent::DownloadFailed { message } | HarvestEvent::MetadataFailed { message } => {
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
            .bind(to.as_str())
            .bind(message)
            .bind(from.as_str())
            .execute(pool)
            .await
        }
        HarvestEvent::MetadataExtracted { metadata } => {
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
            .bind(to.as_str())
            .bind(metadata)
            .bind(OaiIndexStatus::Pending.as_str())
            .bind(from.as_str())
            .execute(pool)
            .await
        }
        HarvestEvent::HarvestRetryRequested => {
            return Err(sqlx::Error::Protocol(
                "HarvestRetryRequested is a batch operation; use apply_harvest_retry()".into(),
            ));
        }
    }
}

/// Batch retry: reset all failed harvest records to pending.
pub async fn apply_harvest_retry(
    pool: &PgPool,
    params: RetryHarvestParams<'_>,
) -> Result<PgQueryResult, Error> {
    let HarvestTransition { from, to } = HarvestEvent::HarvestRetryRequested.transition();
    sqlx::query(
        r#"
        UPDATE oai_records
        SET status = $3, message = '', last_checked_at = NOW()
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND status = $4
        "#,
    )
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(to.as_str())
    .bind(from.as_str())
    .execute(pool)
    .await
}
