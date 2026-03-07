use sqlx::postgres::PgQueryResult;
use sqlx::{Error, PgPool};

use crate::oai::{IndexEvent, IndexTransition, OaiIndexStatus, OaiRecordId, OaiRecordStatus};

pub struct FetchIndexCandidatesParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub oai_repository: &'a str,
    pub max_attempts: Option<i32>,
    pub message_filter: Option<&'a str>,
    pub last_identifier: Option<&'a str>,
}

pub struct ReindexStateParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub oai_repository: &'a str,
}

pub struct UpdateIndexStatusParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub identifier: &'a str,
}

pub async fn fetch_pending_records_for_indexing(
    pool: &PgPool,
    params: FetchIndexCandidatesParams<'_>,
) -> Result<Vec<OaiRecordId>, Error> {
    match params.last_identifier {
        Some(last_id) => {
            sqlx::query_as::<_, OaiRecordId>(
                r#"
                SELECT identifier, fingerprint
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND status = $3
                  AND index_status = $4
                  AND metadata->'repository' ? $5
                  AND identifier > $6
                ORDER BY identifier
                LIMIT 100
                "#,
            )
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(OaiRecordStatus::Parsed.as_str())
            .bind(OaiIndexStatus::Pending.as_str())
            .bind(params.oai_repository)
            .bind(last_id)
            .fetch_all(pool)
            .await
        }
        None => {
            sqlx::query_as::<_, OaiRecordId>(
                r#"
                SELECT identifier, fingerprint
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND status = $3
                  AND index_status = $4
                  AND metadata->'repository' ? $5
                ORDER BY identifier
                LIMIT 100
                "#,
            )
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(OaiRecordStatus::Parsed.as_str())
            .bind(OaiIndexStatus::Pending.as_str())
            .bind(params.oai_repository)
            .fetch_all(pool)
            .await
        }
    }
}

pub async fn fetch_pending_records_for_purging(
    pool: &PgPool,
    params: FetchIndexCandidatesParams<'_>,
) -> Result<Vec<OaiRecordId>, Error> {
    match params.last_identifier {
        Some(last_id) => {
            sqlx::query_as::<_, OaiRecordId>(
                r#"
                SELECT identifier, fingerprint
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND status = $3
                  AND index_status = $4
                  AND metadata->'repository' ? $5
                  AND identifier > $6
                ORDER BY identifier
                LIMIT 100
                "#,
            )
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(OaiRecordStatus::Deleted.as_str())
            .bind(OaiIndexStatus::Pending.as_str())
            .bind(params.oai_repository)
            .bind(last_id)
            .fetch_all(pool)
            .await
        }
        None => {
            sqlx::query_as::<_, OaiRecordId>(
                r#"
                SELECT identifier, fingerprint
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND status = $3
                  AND index_status = $4
                  AND metadata->'repository' ? $5
                ORDER BY identifier
                LIMIT 100
                "#,
            )
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(OaiRecordStatus::Deleted.as_str())
            .bind(OaiIndexStatus::Pending.as_str())
            .bind(params.oai_repository)
            .fetch_all(pool)
            .await
        }
    }
}

pub async fn fetch_failed_records_for_indexing(
    pool: &PgPool,
    params: FetchIndexCandidatesParams<'_>,
) -> Result<Vec<OaiRecordId>, Error> {
    match params.last_identifier {
        Some(last_id) => {
            sqlx::query_as::<_, OaiRecordId>(
                r#"
                SELECT identifier, fingerprint
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND status = $3
                  AND index_status = $4
                  AND metadata->'repository' ? $5
                  AND identifier > $6
                  AND ($7::INT IS NULL OR index_attempts < $7)
                  AND ($8::TEXT IS NULL OR index_message ILIKE ('%' || $8 || '%'))
                ORDER BY identifier
                LIMIT 100
                "#,
            )
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(OaiRecordStatus::Parsed.as_str())
            .bind(OaiIndexStatus::IndexFailed.as_str())
            .bind(params.oai_repository)
            .bind(last_id)
            .bind(params.max_attempts)
            .bind(params.message_filter)
            .fetch_all(pool)
            .await
        }
        None => {
            sqlx::query_as::<_, OaiRecordId>(
                r#"
                SELECT identifier, fingerprint
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND status = $3
                  AND index_status = $4
                  AND metadata->'repository' ? $5
                  AND ($6::INT IS NULL OR index_attempts < $6)
                  AND ($7::TEXT IS NULL OR index_message ILIKE ('%' || $7 || '%'))
                ORDER BY identifier
                LIMIT 100
                "#,
            )
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(OaiRecordStatus::Parsed.as_str())
            .bind(OaiIndexStatus::IndexFailed.as_str())
            .bind(params.oai_repository)
            .bind(params.max_attempts)
            .bind(params.message_filter)
            .fetch_all(pool)
            .await
        }
    }
}

pub async fn fetch_failed_records_for_purging(
    pool: &PgPool,
    params: FetchIndexCandidatesParams<'_>,
) -> Result<Vec<OaiRecordId>, Error> {
    match params.last_identifier {
        Some(last_id) => {
            sqlx::query_as::<_, OaiRecordId>(
                r#"
                SELECT identifier, fingerprint
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND status = $3
                  AND index_status = $4
                  AND metadata->'repository' ? $5
                  AND identifier > $6
                  AND ($7::INT IS NULL OR index_attempts < $7)
                  AND ($8::TEXT IS NULL OR index_message ILIKE ('%' || $8 || '%'))
                ORDER BY identifier
                LIMIT 100
                "#,
            )
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(OaiRecordStatus::Deleted.as_str())
            .bind(OaiIndexStatus::PurgeFailed.as_str())
            .bind(params.oai_repository)
            .bind(last_id)
            .bind(params.max_attempts)
            .bind(params.message_filter)
            .fetch_all(pool)
            .await
        }
        None => {
            sqlx::query_as::<_, OaiRecordId>(
                r#"
                SELECT identifier, fingerprint
                FROM oai_records
                WHERE endpoint = $1
                  AND metadata_prefix = $2
                  AND status = $3
                  AND index_status = $4
                  AND metadata->'repository' ? $5
                  AND ($6::INT IS NULL OR index_attempts < $6)
                  AND ($7::TEXT IS NULL OR index_message ILIKE ('%' || $7 || '%'))
                ORDER BY identifier
                LIMIT 100
                "#,
            )
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(OaiRecordStatus::Deleted.as_str())
            .bind(OaiIndexStatus::PurgeFailed.as_str())
            .bind(params.oai_repository)
            .bind(params.max_attempts)
            .bind(params.message_filter)
            .fetch_all(pool)
            .await
        }
    }
}

/// Apply an index event to a single record.
/// Uses a static SQL query per event variant.
pub async fn apply_index_event(
    pool: &PgPool,
    params: UpdateIndexStatusParams<'_>,
    event: &IndexEvent<'_>,
) -> Result<PgQueryResult, Error> {
    let IndexTransition::SingleRecord {
        required_status,
        from: (from_a, from_b),
        to,
    } = event.transition()
    else {
        return Err(sqlx::Error::Protocol(
            "ReindexRequested is a batch operation; use apply_reindex()".into(),
        ));
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
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(params.identifier)
            .bind(to.as_str())
            .bind(required_status.as_str())
            .bind(from_a.as_str())
            .bind(from_b.as_str())
            .execute(pool)
            .await
        }
        IndexEvent::IndexFailed { message } => {
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
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(params.identifier)
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
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(params.identifier)
            .bind(to.as_str())
            .bind(required_status.as_str())
            .bind(from_a.as_str())
            .bind(from_b.as_str())
            .execute(pool)
            .await
        }
        IndexEvent::PurgeFailed { message } => {
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
            .bind(params.endpoint)
            .bind(params.metadata_prefix)
            .bind(params.identifier)
            .bind(to.as_str())
            .bind(message)
            .bind(required_status.as_str())
            .bind(from_a.as_str())
            .bind(from_b.as_str())
            .execute(pool)
            .await
        }
        IndexEvent::ReindexRequested => {
            unreachable!("handled by let-else above")
        }
    }
}

/// Batch reindex: reset index_status to pending for all parsed/deleted records.
pub async fn apply_reindex(
    pool: &PgPool,
    params: ReindexStateParams<'_>,
) -> Result<PgQueryResult, Error> {
    let IndexTransition::BatchReset {
        eligible_record_statuses,
        to,
    } = IndexEvent::ReindexRequested.transition()
    else {
        unreachable!("ReindexRequested always produces BatchReset")
    };

    let eligible: Vec<&str> = eligible_record_statuses.iter().map(|s| s.as_str()).collect();

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
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(to.as_str())
    .bind(&eligible)
    .bind(params.oai_repository)
    .execute(pool)
    .await
}
