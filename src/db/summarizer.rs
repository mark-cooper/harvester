#![allow(dead_code)] // Summarizer integration is still WIP.

use sqlx::{Error, PgPool};

use crate::oai::{OaiRecordId, OaiRecordStatus};

pub(crate) async fn fetch_record_for_summary(
    pool: &PgPool,
    endpoint: &str,
    metadata_prefix: &str,
    identifier: &str,
) -> Result<Option<OaiRecordId>, Error> {
    sqlx::query_as::<_, OaiRecordId>(
        r#"
        SELECT identifier, fingerprint, status
        FROM oai_records
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND identifier = $3
          AND status = $4
        LIMIT 1
        "#,
    )
    .bind(endpoint)
    .bind(metadata_prefix)
    .bind(identifier)
    .bind(OaiRecordStatus::Parsed.as_str())
    .fetch_optional(pool)
    .await
}
