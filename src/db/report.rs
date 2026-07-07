use sqlx::{Error, PgPool};

use crate::oai::{OaiIndexStatus, OaiRecordStatus, OaiScope};

/// Records still `indexed` in the target index whose latest harvest `failed`:
/// the index serves stale content, and the record is invisible to the
/// indexer's fetch (which only selects parsed/deleted).
pub async fn stale_in_index(pool: &PgPool, scope: &OaiScope) -> Result<Vec<String>, Error> {
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT r.identifier
        FROM indexer_records i
        JOIN oai_records r ON r.id = i.record_id
        WHERE r.endpoint = $1
          AND r.metadata_prefix = $2
          AND i.status = $3
          AND r.status = $4
        ORDER BY r.identifier
        "#,
    )
    .bind(&scope.endpoint)
    .bind(&scope.metadata_prefix)
    .bind(OaiIndexStatus::Indexed.as_str())
    .bind(OaiRecordStatus::Failed.as_str())
    .fetch_all(pool)
    .await
}

/// Records that have stopped appearing in the OAI feed: `last_seen_at` older
/// than `days`. Deleted records are excluded (they are expected to drop out of
/// feeds); records with `last_seen_at` still NULL (never harvested since the
/// column was introduced) are excluded to avoid false positives.
pub async fn not_seen_since(
    pool: &PgPool,
    scope: &OaiScope,
    days: i64,
) -> Result<Vec<String>, Error> {
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT identifier
        FROM oai_records
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND status != $3
          AND last_seen_at IS NOT NULL
          AND last_seen_at < NOW() - ($4 * INTERVAL '1 day')
        ORDER BY identifier
        "#,
    )
    .bind(&scope.endpoint)
    .bind(&scope.metadata_prefix)
    .bind(OaiRecordStatus::Deleted.as_str())
    .bind(days)
    .fetch_all(pool)
    .await
}
