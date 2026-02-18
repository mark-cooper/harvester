use sqlx::postgres::PgQueryResult;
use sqlx::{Error, PgPool};

pub struct UpdateStatusParams<'a> {
    pub endpoint: &'a str,
    pub metadata_prefix: &'a str,
    pub identifier: &'a str,
    pub status: &'a str,
    pub message: &'a str,
}

pub async fn do_update_status_query(
    pool: &PgPool,
    params: UpdateStatusParams<'_>,
) -> Result<PgQueryResult, Error> {
    sqlx::query!(
        r#"
        UPDATE oai_records
        SET status = $4, message = $5, last_checked_at = NOW()
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
        params.endpoint,
        params.metadata_prefix,
        params.identifier,
        params.status,
        params.message
    )
    .execute(pool)
    .await
}
