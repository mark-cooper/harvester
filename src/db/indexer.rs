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
    let (parsed_index_status, deleted_index_status) = match params.selection_mode {
        IndexSelectionMode::PendingOnly => (OaiIndexStatus::Pending, OaiIndexStatus::Pending),
        IndexSelectionMode::FailedOnly => {
            (OaiIndexStatus::IndexFailed, OaiIndexStatus::PurgeFailed)
        }
    };

    sqlx::query_as::<_, OaiRecord>(
        r#"
        SELECT identifier, fingerprint, status
        FROM oai_records
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND (
                (status = $3 AND index_status = $4)
             OR (status = $5 AND index_status = $6)
          )
          AND metadata->'repository' ? $7
          AND ($8::TEXT IS NULL OR identifier > $8)
          AND ($9::INT IS NULL OR index_attempts < $9)
          AND ($10::TEXT IS NULL OR index_message ILIKE ('%' || $10 || '%'))
        ORDER BY identifier
        LIMIT 100
        "#,
    )
    .bind(&params.scope.endpoint)
    .bind(&params.scope.metadata_prefix)
    .bind(OaiRecordStatus::Parsed.as_str())
    .bind(parsed_index_status.as_str())
    .bind(OaiRecordStatus::Deleted.as_str())
    .bind(deleted_index_status.as_str())
    .bind(params.source_repository)
    .bind(params.last_identifier)
    .bind(params.max_attempts)
    .bind(params.message_filter)
    .fetch_all(pool)
    .await
}

/// Batch reindex: reset index_status to pending for all parsed/deleted records
/// in the given source repository. Wildcard transition: any index_status -> pending.
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
        UPDATE oai_records
        SET index_status = $3,
            index_message = '',
            index_attempts = 0,
            indexed_at = NULL,
            purged_at = NULL,
            index_last_checked_at = NULL
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND status = ANY($4::text[])
          AND metadata->'repository' ? $5
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

/// Apply an index event for a single record. Each arm encodes its
/// `required_status` (record-status guard), accepted predecessor index
/// statuses, and target index status directly.
pub async fn transition(
    pool: &PgPool,
    scope: &OaiScope,
    identifier: &str,
    event: &IndexEvent<'_>,
) -> Result<PgQueryResult, Error> {
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

    match event {
        IndexEvent::IndexSucceeded => {
            sqlx::query(
                r#"
            UPDATE oai_records
            SET index_status = $4,
                index_message = '',
                indexed_at = NOW(),
                purged_at = NULL,
                index_last_checked_at = NOW()
            WHERE endpoint = $1
              AND metadata_prefix = $2
              AND identifier = $3
              AND status = $5
              AND (index_status = $6 OR index_status = $7)
            "#,
            )
            .bind(&scope.endpoint)
            .bind(&scope.metadata_prefix)
            .bind(identifier)
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
                UPDATE oai_records
                SET index_status = $4,
                    index_message = $5,
                    index_attempts = index_attempts + 1,
                    index_last_checked_at = NOW()
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND identifier = $3
                  AND status = $6
                  AND (index_status = $7 OR index_status = $8)
                "#,
            )
            .bind(&scope.endpoint)
            .bind(&scope.metadata_prefix)
            .bind(identifier)
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
            UPDATE oai_records
            SET index_status = $4,
                index_message = '',
                indexed_at = NULL,
                purged_at = NOW(),
                index_last_checked_at = NOW()
            WHERE endpoint = $1
              AND metadata_prefix = $2
              AND identifier = $3
              AND status = $5
              AND (index_status = $6 OR index_status = $7)
            "#,
            )
            .bind(&scope.endpoint)
            .bind(&scope.metadata_prefix)
            .bind(identifier)
            .bind(OaiIndexStatus::Purged.as_str())
            .bind(required_status.as_str())
            .bind(from_a.as_str())
            .bind(from_b.as_str())
            .execute(pool)
            .await
        }
    }
}
