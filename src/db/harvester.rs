use sqlx::postgres::PgQueryResult;
use sqlx::{Error, PgPool, Postgres, Transaction};

use crate::oai::{HarvestEvent, OaiHeader, OaiIndexStatus, OaiRecord, OaiRecordStatus, OaiScope};

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
    scope: &OaiScope,
    records: &[OaiHeader],
) -> anyhow::Result<ImportStats> {
    if records.is_empty() {
        return Ok(ImportStats::default());
    }

    let identifiers: Vec<_> = records.iter().map(|r| r.identifier.as_str()).collect();
    let datestamps: Vec<_> = records.iter().map(|r| r.datestamp.as_str()).collect();
    let statuses: Vec<_> = records.iter().map(|r| r.status.as_str()).collect();
    let batch_len = records.len() as i32;

    let mut tx = pool.begin().await?;

    let rows = sqlx::query_as::<_, (i64, String)>(
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
            version = oai_records.version + 1,
            last_checked_at = EXCLUDED.last_checked_at
        WHERE oai_records.status != $7
        AND oai_records.datestamp != EXCLUDED.datestamp
        RETURNING id, status
        "#,
    )
    .bind(&scope.endpoint)
    .bind(&scope.metadata_prefix)
    .bind(&identifiers)
    .bind(&datestamps)
    .bind(&statuses)
    .bind(batch_len)
    .bind(OaiRecordStatus::Failed.as_str())
    .fetch_all(&mut *tx)
    .await?;

    let deleted_ids: Vec<i64> = rows
        .iter()
        .filter(|(_, status)| status.as_str() == OaiRecordStatus::Deleted.as_str())
        .map(|(id, _)| *id)
        .collect();

    if !deleted_ids.is_empty() {
        requeue_deleted_for_purge(&mut tx, &deleted_ids).await?;
    }

    tx.commit().await?;

    let deleted = deleted_ids.len();
    let processed = rows.len();
    let imported = processed.saturating_sub(deleted);

    Ok(ImportStats {
        processed,
        imported,
        deleted,
    })
}

/// Requeue deleted records for purge: `* -> pending`. A partial reset —
/// `attempts` and `indexed_at` are preserved on existing rows (the record is
/// still in the index, awaiting purge).
async fn requeue_deleted_for_purge(
    tx: &mut Transaction<'_, Postgres>,
    record_ids: &[i64],
) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        INSERT INTO indexer_records (record_id, status)
        SELECT UNNEST($1::bigint[]), $2
        ON CONFLICT (record_id) DO UPDATE SET
            status = $2,
            message = '',
            purged_at = NULL,
            last_checked_at = NULL
        "#,
    )
    .bind(record_ids)
    .bind(OaiIndexStatus::Pending.as_str())
    .execute(&mut **tx)
    .await
}

pub(crate) async fn fetch(
    pool: &PgPool,
    scope: &OaiScope,
    status: OaiRecordStatus,
    last_identifier: Option<&str>,
) -> Result<Vec<OaiRecord>, Error> {
    sqlx::query_as::<_, OaiRecord>(
        r#"
        SELECT id, identifier, fingerprint, status
        FROM oai_records
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND status = $3
          AND ($4::TEXT IS NULL OR identifier > $4)
        ORDER BY identifier
        LIMIT 100
        "#,
    )
    .bind(&scope.endpoint)
    .bind(&scope.metadata_prefix)
    .bind(status.as_str())
    .bind(last_identifier)
    .fetch_all(pool)
    .await
}

/// Batch retry: reset all failed harvest records to pending.
///
/// Transition: `failed -> pending`.
pub async fn retry(pool: &PgPool, scope: &OaiScope) -> Result<PgQueryResult, Error> {
    sqlx::query(
        r#"
        UPDATE oai_records
        SET status = $3, message = '', last_checked_at = NOW()
        WHERE endpoint = $1
          AND metadata_prefix = $2
          AND status = $4
        "#,
    )
    .bind(&scope.endpoint)
    .bind(&scope.metadata_prefix)
    .bind(OaiRecordStatus::Pending.as_str())
    .bind(OaiRecordStatus::Failed.as_str())
    .execute(pool)
    .await
}

/// Apply a harvest event for a single record. Returns the number of affected
/// `oai_records` rows (0 or 1).
pub async fn transition(
    pool: &PgPool,
    scope: &OaiScope,
    identifier: &str,
    event: &HarvestEvent<'_>,
) -> Result<u64, Error> {
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
            .bind(&scope.endpoint)
            .bind(&scope.metadata_prefix)
            .bind(identifier)
            .bind(OaiRecordStatus::Available.as_str())
            .bind(OaiRecordStatus::Pending.as_str())
            .execute(pool)
            .await
            .map(|result| result.rows_affected())
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
            .bind(&scope.endpoint)
            .bind(&scope.metadata_prefix)
            .bind(identifier)
            .bind(OaiRecordStatus::Failed.as_str())
            .bind(message)
            .bind(from.as_str())
            .execute(pool)
            .await
            .map(|result| result.rows_affected())
        }

        // available -> parsed (also requeues the record for indexing)
        HarvestEvent::MetadataExtracted { metadata } => {
            let mut tx = pool.begin().await?;

            let record_id = sqlx::query_scalar::<_, i64>(
                r#"
            UPDATE oai_records
            SET status = $4,
                metadata = $5,
                message = '',
                last_checked_at = NOW()
            WHERE endpoint = $1
              AND metadata_prefix = $2
              AND identifier = $3
              AND status = $6
            RETURNING id
            "#,
            )
            .bind(&scope.endpoint)
            .bind(&scope.metadata_prefix)
            .bind(identifier)
            .bind(OaiRecordStatus::Parsed.as_str())
            .bind(metadata)
            .bind(OaiRecordStatus::Available.as_str())
            .fetch_optional(&mut *tx)
            .await?;

            // Requeue for indexing: `* -> pending`. A full reset — a fresh
            // parse invalidates any previous index state.
            if let Some(record_id) = record_id {
                sqlx::query(
                    r#"
                    INSERT INTO indexer_records (record_id, status)
                    VALUES ($1, $2)
                    ON CONFLICT (record_id) DO UPDATE SET
                        status = $2,
                        message = '',
                        attempts = 0,
                        indexed_at = NULL,
                        purged_at = NULL,
                        last_checked_at = NULL
                    "#,
                )
                .bind(record_id)
                .bind(OaiIndexStatus::Pending.as_str())
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            Ok(record_id.is_some() as u64)
        }
    }
}
