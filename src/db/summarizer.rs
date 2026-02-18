use sqlx::{Error, PgPool};

use crate::oai::{OaiRecordId, OaiRecordStatus};

pub async fn fetch_record_for_summary(
    pool: &PgPool,
    endpoint: &str,
    metadata_prefix: &str,
    identifier: &str,
) -> Result<Option<OaiRecordId>, Error> {
    sqlx::query_as!(
        OaiRecordId,
        r#"
        SELECT identifier, fingerprint AS "fingerprint!"
        FROM oai_records
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND identifier = $3
          AND status = $4
        LIMIT 1
        "#,
        endpoint,
        metadata_prefix,
        identifier,
        OaiRecordStatus::Parsed.as_str()
    )
    .fetch_optional(pool)
    .await
}
