use sqlx::postgres::PgQueryResult;
use sqlx::{Error, PgPool};

use crate::oai::{
    HarvestEvent, OaiIndexStatus, OaiRecordId, OaiRecordImport, OaiRecordStatus, RecordKey,
    RepositoryKey,
};

#[derive(Default)]
pub(crate) struct ImportStats {
    pub(crate) processed: usize,
    pub(crate) imported: usize,
    pub(crate) deleted: usize,
}

impl ImportStats {
    pub(crate) fn accumulate(&mut self, other: &Self) {
        self.processed += other.processed;
        self.imported += other.imported;
        self.deleted += other.deleted;
    }
}

pub(crate) async fn batch_upsert_records(
    pool: &PgPool,
    repo: &RepositoryKey,
    records: &[OaiRecordImport],
) -> anyhow::Result<ImportStats> {
    if records.is_empty() {
        return Ok(ImportStats::default());
    }

    let identifiers: Vec<_> = records.iter().map(|r| r.identifier.as_str()).collect();
    let datestamps: Vec<_> = records.iter().map(|r| r.datestamp.as_str()).collect();
    let statuses: Vec<_> = records.iter().map(|r| r.status.as_str()).collect();
    let batch_len = records.len() as i32;

    let statuses = sqlx::query_scalar::<_, String>(
        r#"
        INSERT INTO oai_records (
            endpoint, metadata_prefix, identifier, datestamp, status, message, last_checked_at
        )
        SELECT * FROM UNNEST(
            ARRAY_FILL($1::text, ARRAY[$6]),
            ARRAY_FILL($2::text, ARRAY[$6]),
            $3::text[],
            $4::text[],
            $5::text[],
            ARRAY_FILL(''::text, ARRAY[$6]),
            ARRAY_FILL(NOW(), ARRAY[$6])
        )
        ON CONFLICT (endpoint, metadata_prefix, identifier) DO UPDATE SET
            datestamp = EXCLUDED.datestamp,
            status = EXCLUDED.status,
            message = '',
            index_status = CASE
                WHEN EXCLUDED.status = $8 THEN $9
                ELSE oai_records.index_status
            END,
            index_message = CASE
                WHEN EXCLUDED.status = $8 THEN ''
                ELSE oai_records.index_message
            END,
            purged_at = CASE
                WHEN EXCLUDED.status = $8 THEN NULL
                ELSE oai_records.purged_at
            END,
            index_last_checked_at = CASE
                WHEN EXCLUDED.status = $8 THEN NULL
                ELSE oai_records.index_last_checked_at
            END,
            version = oai_records.version + 1,
            last_checked_at = EXCLUDED.last_checked_at
        WHERE oai_records.status != $7
        AND oai_records.datestamp != EXCLUDED.datestamp
        RETURNING status
        "#,
    )
    .bind(&repo.endpoint)
    .bind(&repo.metadata_prefix)
    .bind(&identifiers)
    .bind(&datestamps)
    .bind(&statuses)
    .bind(batch_len)
    .bind(OaiRecordStatus::Failed.as_str())
    .bind(OaiRecordStatus::Deleted.as_str())
    .bind(OaiIndexStatus::Pending.as_str())
    .fetch_all(pool)
    .await?;

    let deleted = statuses
        .iter()
        .filter(|status| status.as_str() == OaiRecordStatus::Deleted.as_str())
        .count();
    let processed = statuses.len();
    let imported = processed.saturating_sub(deleted);

    Ok(ImportStats {
        processed,
        imported,
        deleted,
    })
}

pub(crate) async fn fetch(
    pool: &PgPool,
    repo: &RepositoryKey,
    status: OaiRecordStatus,
    last_identifier: Option<&str>,
) -> Result<Vec<OaiRecordId>, Error> {
    sqlx::query_as::<_, OaiRecordId>(
        r#"
        SELECT identifier, fingerprint, status
        FROM oai_records
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND status = $3
          AND ($4::TEXT IS NULL OR identifier > $4)
        ORDER BY identifier
        LIMIT 100
        "#,
    )
    .bind(&repo.endpoint)
    .bind(&repo.metadata_prefix)
    .bind(status.as_str())
    .bind(last_identifier)
    .fetch_all(pool)
    .await
}

/// Batch retry: reset all failed harvest records to pending.
///
/// Transition: `failed -> pending`.
pub async fn retry(pool: &PgPool, repo: &RepositoryKey) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        UPDATE oai_records
        SET status = $3, message = '', last_checked_at = NOW()
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND status = $4
        "#,
    )
    .bind(&repo.endpoint)
    .bind(&repo.metadata_prefix)
    .bind(OaiRecordStatus::Pending.as_str())
    .bind(OaiRecordStatus::Failed.as_str())
    .execute(pool)
    .await
}

/// Apply a harvest event for a single record.
pub async fn transition(
    pool: &PgPool,
    key: RecordKey<'_>,
    event: &HarvestEvent<'_>,
) -> Result<PgQueryResult, Error> {
    match event {
        // pending -> available
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
            .bind(&key.repo.endpoint)
            .bind(&key.repo.metadata_prefix)
            .bind(key.identifier)
            .bind(OaiRecordStatus::Available.as_str())
            .bind(OaiRecordStatus::Pending.as_str())
            .execute(pool)
            .await
        }

        // pending -> failed (download) or available -> failed (metadata)
        HarvestEvent::DownloadFailed { message } | HarvestEvent::MetadataFailed { message } => {
            let from = match event {
                HarvestEvent::DownloadFailed { .. } => OaiRecordStatus::Pending,
                HarvestEvent::MetadataFailed { .. } => OaiRecordStatus::Available,
                _ => unreachable!(),
            };
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
            .bind(&key.repo.endpoint)
            .bind(&key.repo.metadata_prefix)
            .bind(key.identifier)
            .bind(OaiRecordStatus::Failed.as_str())
            .bind(message)
            .bind(from.as_str())
            .execute(pool)
            .await
        }

        // available -> parsed (also resets index lifecycle to pending)
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
            .bind(&key.repo.endpoint)
            .bind(&key.repo.metadata_prefix)
            .bind(key.identifier)
            .bind(OaiRecordStatus::Parsed.as_str())
            .bind(metadata)
            .bind(OaiIndexStatus::Pending.as_str())
            .bind(OaiRecordStatus::Available.as_str())
            .execute(pool)
            .await
        }
    }
}
