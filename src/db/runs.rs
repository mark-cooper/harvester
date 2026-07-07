use sqlx::{Error, PgPool};

use crate::oai::OaiScope;

pub const KIND_HARVEST: &str = "harvest";
pub const KIND_INDEX: &str = "index";

pub const OUTCOME_COMPLETED: &str = "completed";
pub const OUTCOME_FAILED: &str = "failed";

/// Aggregate counters recorded on a finished run.
#[derive(Default)]
pub struct RunStats {
    pub processed: usize,
    pub imported: usize,
    pub deleted: usize,
    pub failed: usize,
}

/// Open a run row (outcome `running`). Returns the run id for `finish`.
pub async fn start(
    pool: &PgPool,
    kind: &str,
    scope: &OaiScope,
    source_repository: &str,
) -> Result<i64, Error> {
    sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO runs (kind, endpoint, metadata_prefix, source_repository)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        "#,
    )
    .bind(kind)
    .bind(&scope.endpoint)
    .bind(&scope.metadata_prefix)
    .bind(source_repository)
    .fetch_one(pool)
    .await
}

pub async fn finish(
    pool: &PgPool,
    run_id: i64,
    outcome: &str,
    stats: &RunStats,
    error_sample: &str,
) -> Result<(), Error> {
    sqlx::query(
        r#"
        UPDATE runs
        SET finished_at = NOW(),
            outcome = $2,
            processed = $3,
            imported = $4,
            deleted = $5,
            failed = $6,
            error_sample = $7
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .bind(outcome)
    .bind(stats.processed as i32)
    .bind(stats.imported as i32)
    .bind(stats.deleted as i32)
    .bind(stats.failed as i32)
    .bind(error_sample)
    .execute(pool)
    .await?;

    Ok(())
}
