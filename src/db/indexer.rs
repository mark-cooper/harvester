use sqlx::postgres::PgQueryResult;
use sqlx::{Error, PgPool};

use crate::oai::{OaiIndexStatus, OaiRecordId, OaiRecordStatus};

pub struct FetchIndexCandidatesParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub oai_repository: &'a str,
    pub max_attempts: Option<i32>,
    pub message_filter: Option<&'a str>,
    pub last_identifier: Option<&'a str>,
}

pub struct UpdateIndexFailureParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub identifier: &'a str,
    pub message: &'a str,
}

pub struct UpdateIndexStatusParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub identifier: &'a str,
}

pub struct ReindexStateParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub oai_repository: &'a str,
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

pub async fn do_mark_index_success_query(
    pool: &PgPool,
    params: UpdateIndexStatusParams<'_>,
) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        UPDATE oai_records
        SET index_status = $4,
            index_message = '',
            indexed_at = NOW(),
            purged_at = NULL,
            index_last_checked_at = NOW()
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
    )
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(params.identifier)
    .bind(OaiIndexStatus::Indexed.as_str())
    .execute(pool)
    .await
}

pub async fn do_mark_index_failure_query(
    pool: &PgPool,
    params: UpdateIndexFailureParams<'_>,
) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        UPDATE oai_records
        SET index_status = $4,
            index_message = $5,
            index_attempts = index_attempts + 1,
            index_last_checked_at = NOW()
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
    )
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(params.identifier)
    .bind(OaiIndexStatus::IndexFailed.as_str())
    .bind(params.message)
    .execute(pool)
    .await
}

pub async fn do_mark_purge_success_query(
    pool: &PgPool,
    params: UpdateIndexStatusParams<'_>,
) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        UPDATE oai_records
        SET index_status = $4,
            index_message = '',
            indexed_at = NULL,
            purged_at = NOW(),
            index_last_checked_at = NOW()
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
    )
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(params.identifier)
    .bind(OaiIndexStatus::Purged.as_str())
    .execute(pool)
    .await
}

pub async fn do_mark_purge_failure_query(
    pool: &PgPool,
    params: UpdateIndexFailureParams<'_>,
) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        UPDATE oai_records
        SET index_status = $4,
            index_message = $5,
            index_attempts = index_attempts + 1,
            index_last_checked_at = NOW()
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
    )
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(params.identifier)
    .bind(OaiIndexStatus::PurgeFailed.as_str())
    .bind(params.message)
    .execute(pool)
    .await
}

pub async fn do_reindex_state_query(
    pool: &PgPool,
    params: ReindexStateParams<'_>,
) -> Result<PgQueryResult, Error> {
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
          AND (status = $4 OR status = $5)
          AND metadata->'repository' ? $6
        "#,
    )
    .bind(params.endpoint)
    .bind(params.metadata_prefix)
    .bind(OaiIndexStatus::Pending.as_str())
    .bind(OaiRecordStatus::Parsed.as_str())
    .bind(OaiRecordStatus::Deleted.as_str())
    .bind(params.oai_repository)
    .execute(pool)
    .await
}
