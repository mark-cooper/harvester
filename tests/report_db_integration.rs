mod support;

use harvester::db::report::{not_seen_since, stale_in_index};
use support::{
    DEFAULT_DATESTAMP, acquire_test_lock, fetch_record_id, insert_record, insert_record_with_index,
    metadata, scope, setup_test_pool,
};

const ENDPOINT: &str = "https://report.example.org/oai";
const REPOSITORY: &str = "Report Repository";

#[tokio::test]
async fn stale_in_index_reports_indexed_records_whose_harvest_failed() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    // Indexed in Solr, but the latest harvest failed: stale.
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "stale-record",
        DEFAULT_DATESTAMP,
        "failed",
        "indexed",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;
    // Healthy indexed record: not stale.
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "healthy-record",
        DEFAULT_DATESTAMP,
        "parsed",
        "indexed",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;
    // Failed harvest with no index presence: not stale.
    insert_record(&pool, ENDPOINT, "failed-only", DEFAULT_DATESTAMP, "failed").await?;

    let stale = stale_in_index(&pool, &scope(ENDPOINT)).await?;
    assert_eq!(stale, vec!["stale-record".to_string()]);

    Ok(())
}

#[tokio::test]
async fn not_seen_since_reports_records_absent_from_the_feed() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record(&pool, ENDPOINT, "vanished", DEFAULT_DATESTAMP, "parsed").await?;
    insert_record(&pool, ENDPOINT, "current", DEFAULT_DATESTAMP, "parsed").await?;
    insert_record(
        &pool,
        ENDPOINT,
        "never-tracked",
        DEFAULT_DATESTAMP,
        "parsed",
    )
    .await?;
    // Deleted records are expected to drop out of feeds: excluded.
    insert_record(&pool, ENDPOINT, "was-deleted", DEFAULT_DATESTAMP, "deleted").await?;

    for (identifier, days_ago) in [("vanished", 10), ("current", 0), ("was-deleted", 10)] {
        let id = fetch_record_id(&pool, ENDPOINT, identifier).await?;
        sqlx::query(
            "UPDATE oai_records SET last_seen_at = NOW() - ($2 * INTERVAL '1 day') WHERE id = $1",
        )
        .bind(id)
        .bind(days_ago)
        .execute(&pool)
        .await?;
    }

    let missing = not_seen_since(&pool, &scope(ENDPOINT), 7).await?;
    assert_eq!(missing, vec!["vanished".to_string()]);

    let missing = not_seen_since(&pool, &scope(ENDPOINT), 30).await?;
    assert!(missing.is_empty());

    Ok(())
}
