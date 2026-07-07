use sqlx::postgres::PgQueryResult;
use sqlx::{Error, PgPool};

use crate::{
    IndexSelectionMode,
    oai::{IndexEvent, OaiIndexStatus, OaiRecord, OaiRecordStatus, OaiScope},
};

pub struct FetchIndexCandidatesParams<'a> {
    pub scope: &'a OaiScope,
    pub source_repository: &'a str,
    pub selection_mode: IndexSelectionMode,
    pub max_attempts: Option<i32>,
    pub message_filter: Option<&'a str>,
    pub last_identifier: Option<&'a str>,
}

pub async fn fetch(
    pool: &PgPool,
    params: FetchIndexCandidatesParams<'_>,
) -> Result<Vec<OaiRecord>, Error> {
    match params.selection_mode {
        // Pending rows are always eligible; failed rows only while under the
        // attempts budget (a NULL budget means unlimited retries).
        IndexSelectionMode::Standard => {
            sqlx::query_as::<_, OaiRecord>(
                r#"
                SELECT r.id, r.identifier, r.fingerprint, r.status
                FROM oai_records r
                JOIN indexer_records i ON i.record_id = r.id
                WHERE r.endpoint = $1
                  AND r.metadata_prefix = $2
                  AND (
                        (r.status = $3 AND (i.status = $4 OR (i.status = $5 AND ($9::INT IS NULL OR i.attempts < $9))))
                     OR (r.status = $6 AND (i.status = $4 OR (i.status = $7 AND ($9::INT IS NULL OR i.attempts < $9))))
                  )
                  AND r.metadata->'repository' ? $8
                  AND ($10::TEXT IS NULL OR r.identifier > $10)
                  AND ($11::TEXT IS NULL OR i.message ILIKE ('%' || $11 || '%'))
                ORDER BY r.identifier
                LIMIT 100
                "#,
            )
            .bind(&params.scope.endpoint)
            .bind(&params.scope.metadata_prefix)
            .bind(OaiRecordStatus::Parsed.as_str())
            .bind(OaiIndexStatus::Pending.as_str())
            .bind(OaiIndexStatus::IndexFailed.as_str())
            .bind(OaiRecordStatus::Deleted.as_str())
            .bind(OaiIndexStatus::PurgeFailed.as_str())
            .bind(params.source_repository)
            .bind(params.max_attempts)
            .bind(params.last_identifier)
            .bind(params.message_filter)
            .fetch_all(pool)
            .await
        }

        // Failed rows only, with the budget and message filter applied
        // uniformly (the --retry escape hatch).
        IndexSelectionMode::FailedOnly => {
            sqlx::query_as::<_, OaiRecord>(
                r#"
                SELECT r.id, r.identifier, r.fingerprint, r.status
                FROM oai_records r
                JOIN indexer_records i ON i.record_id = r.id
                WHERE r.endpoint = $1
                  AND r.metadata_prefix = $2
                  AND (
                        (r.status = $3 AND i.status = $4)
                     OR (r.status = $5 AND i.status = $6)
                  )
                  AND r.metadata->'repository' ? $7
                  AND ($8::INT IS NULL OR i.attempts < $8)
                  AND ($9::TEXT IS NULL OR r.identifier > $9)
                  AND ($10::TEXT IS NULL OR i.message ILIKE ('%' || $10 || '%'))
                ORDER BY r.identifier
                LIMIT 100
                "#,
            )
            .bind(&params.scope.endpoint)
            .bind(&params.scope.metadata_prefix)
            .bind(OaiRecordStatus::Parsed.as_str())
            .bind(OaiIndexStatus::IndexFailed.as_str())
            .bind(OaiRecordStatus::Deleted.as_str())
            .bind(OaiIndexStatus::PurgeFailed.as_str())
            .bind(params.source_repository)
            .bind(params.max_attempts)
            .bind(params.last_identifier)
            .bind(params.message_filter)
            .fetch_all(pool)
            .await
        }
    }
}

/// Batch reindex: reset the index lifecycle to pending for all parsed/deleted
/// records in the given source repository. Wildcard transition: any -> pending.
pub async fn reindex(
    pool: &PgPool,
    scope: &OaiScope,
    source_repository: &str,
) -> Result<PgQueryResult, Error> {
    let eligible = [
        OaiRecordStatus::Parsed.as_str(),
        OaiRecordStatus::Deleted.as_str(),
    ];

    sqlx::query(
        r#"
        UPDATE indexer_records i
        SET status = $3,
            message = '',
            attempts = 0,
            indexed_at = NULL,
            purged_at = NULL,
            last_checked_at = NULL
        FROM oai_records r
        WHERE i.record_id = r.id
          AND r.endpoint = $1
          AND r.metadata_prefix = $2
          AND r.status = ANY($4::text[])
          AND r.metadata->'repository' ? $5
        "#,
    )
    .bind(&scope.endpoint)
    .bind(&scope.metadata_prefix)
    .bind(OaiIndexStatus::Pending.as_str())
    .bind(&eligible[..])
    .bind(source_repository)
    .execute(pool)
    .await
}

pub async fn repository_exists(pool: &PgPool, repository: &str) -> Result<bool, Error> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM oai_records
            WHERE metadata->'repository' ? $1
        )
        "#,
    )
    .bind(repository)
    .fetch_one(pool)
    .await
}

/// Apply an index event for a single record, keyed by `oai_records.id`. Each
/// arm encodes its `required_status` (record-status guard), accepted
/// predecessor index statuses, and target index status directly. Returns the
/// number of affected `indexer_records` rows (0 or 1).
pub async fn transition(
    pool: &PgPool,
    record_id: i64,
    event: &IndexEvent<'_>,
) -> Result<u64, Error> {
    // Index transitions: pending|index_failed -> indexed|index_failed (for parsed records),
    //                    pending|purge_failed -> purged|purge_failed   (for deleted records).
    let (required_status, from_a, from_b) = match event {
        IndexEvent::IndexSucceeded | IndexEvent::IndexFailed { .. } => (
            OaiRecordStatus::Parsed,
            OaiIndexStatus::Pending,
            OaiIndexStatus::IndexFailed,
        ),
        IndexEvent::PurgeSucceeded | IndexEvent::PurgeFailed { .. } => (
            OaiRecordStatus::Deleted,
            OaiIndexStatus::Pending,
            OaiIndexStatus::PurgeFailed,
        ),
    };

    let result = match event {
        IndexEvent::IndexSucceeded => {
            sqlx::query(
                r#"
            UPDATE indexer_records
            SET status = $2,
                message = '',
                attempts = 0,
                indexed_at = NOW(),
                purged_at = NULL,
                last_checked_at = NOW()
            WHERE record_id = $1
              AND (status = $4 OR status = $5)
              AND EXISTS (
                  SELECT 1 FROM oai_records r WHERE r.id = $1 AND r.status = $3
              )
            "#,
            )
            .bind(record_id)
            .bind(OaiIndexStatus::Indexed.as_str())
            .bind(required_status.as_str())
            .bind(from_a.as_str())
            .bind(from_b.as_str())
            .execute(pool)
            .await
        }

        IndexEvent::IndexFailed { message } | IndexEvent::PurgeFailed { message } => {
            let to = match event {
                IndexEvent::IndexFailed { .. } => OaiIndexStatus::IndexFailed,
                IndexEvent::PurgeFailed { .. } => OaiIndexStatus::PurgeFailed,
                _ => unreachable!(),
            };
            sqlx::query(
                r#"
                UPDATE indexer_records
                SET status = $2,
                    message = $3,
                    attempts = attempts + 1,
                    last_checked_at = NOW()
                WHERE record_id = $1
                  AND (status = $5 OR status = $6)
                  AND EXISTS (
                      SELECT 1 FROM oai_records r WHERE r.id = $1 AND r.status = $4
                  )
                "#,
            )
            .bind(record_id)
            .bind(to.as_str())
            .bind(message)
            .bind(required_status.as_str())
            .bind(from_a.as_str())
            .bind(from_b.as_str())
            .execute(pool)
            .await
        }

        IndexEvent::PurgeSucceeded => {
            sqlx::query(
                r#"
            UPDATE indexer_records
            SET status = $2,
                message = '',
                attempts = 0,
                indexed_at = NULL,
                purged_at = NOW(),
                last_checked_at = NOW()
            WHERE record_id = $1
              AND (status = $4 OR status = $5)
              AND EXISTS (
                  SELECT 1 FROM oai_records r WHERE r.id = $1 AND r.status = $3
              )
            "#,
            )
            .bind(record_id)
            .bind(OaiIndexStatus::Purged.as_str())
            .bind(required_status.as_str())
            .bind(from_a.as_str())
            .bind(from_b.as_str())
            .execute(pool)
            .await
        }
    };

    result.map(|r| r.rows_affected())
}
