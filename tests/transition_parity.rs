mod support;

use harvester::{
    db::{harvester as harvest_db, indexer as index_db},
    oai::{HarvestEvent, IndexEvent},
};
use support::{
    DEFAULT_DATESTAMP, acquire_test_lock, fetch_record_snapshot, insert_record,
    insert_record_with_index, metadata, scope, setup_test_pool,
};

const ENDPOINT: &str = "https://parity.example.org/oai";
const REPOSITORY: &str = "Parity Repository";

// ---------------------------------------------------------------------------
// Legal harvest transitions via Rust event functions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn harvest_events_produce_legal_transitions() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    // DownloadSucceeded: pending -> available
    insert_record(&pool, ENDPOINT, "dl-ok", DEFAULT_DATESTAMP, "pending").await?;
    harvest_db::transition(
        &pool,
        &scope(ENDPOINT),
        "dl-ok",
        &HarvestEvent::DownloadSucceeded,
    )
    .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "dl-ok").await?;
    assert_eq!(snap.status, "available");

    // DownloadFailed: pending -> failed
    insert_record(&pool, ENDPOINT, "dl-fail", DEFAULT_DATESTAMP, "pending").await?;
    harvest_db::transition(
        &pool,
        &scope(ENDPOINT),
        "dl-fail",
        &HarvestEvent::DownloadFailed { message: "timeout" },
    )
    .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "dl-fail").await?;
    assert_eq!(snap.status, "failed");
    assert_eq!(snap.message, "timeout");

    // MetadataExtracted: available -> parsed
    insert_record(&pool, ENDPOINT, "meta-ok", DEFAULT_DATESTAMP, "available").await?;
    harvest_db::transition(
        &pool,
        &scope(ENDPOINT),
        "meta-ok",
        &HarvestEvent::MetadataExtracted {
            metadata: metadata(REPOSITORY),
        },
    )
    .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "meta-ok").await?;
    assert_eq!(snap.status, "parsed");

    // MetadataFailed: available -> failed
    insert_record(&pool, ENDPOINT, "meta-fail", DEFAULT_DATESTAMP, "available").await?;
    harvest_db::transition(
        &pool,
        &scope(ENDPOINT),
        "meta-fail",
        &HarvestEvent::MetadataFailed { message: "bad xml" },
    )
    .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "meta-fail").await?;
    assert_eq!(snap.status, "failed");
    assert_eq!(snap.message, "bad xml");

    // HarvestRetry: failed -> pending (batch)
    insert_record(&pool, ENDPOINT, "retry-me", DEFAULT_DATESTAMP, "failed").await?;
    harvest_db::retry(&pool, &scope(ENDPOINT)).await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "retry-me").await?;
    assert_eq!(snap.status, "pending");

    Ok(())
}

// ---------------------------------------------------------------------------
// Legal index transitions via Rust event functions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn index_events_produce_legal_transitions() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    // IndexSucceeded: pending -> indexed
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "idx-ok",
        DEFAULT_DATESTAMP,
        "parsed",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;
    index_db::transition(
        &pool,
        &scope(ENDPOINT),
        "idx-ok",
        &IndexEvent::IndexSucceeded,
    )
    .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "idx-ok").await?;
    assert_eq!(snap.index_status, "indexed");

    // IndexFailed: pending -> index_failed
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "idx-fail",
        DEFAULT_DATESTAMP,
        "parsed",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;
    index_db::transition(
        &pool,
        &scope(ENDPOINT),
        "idx-fail",
        &IndexEvent::IndexFailed {
            message: "traject error",
        },
    )
    .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "idx-fail").await?;
    assert_eq!(snap.index_status, "index_failed");

    // IndexSucceeded from index_failed: index_failed -> indexed
    index_db::transition(
        &pool,
        &scope(ENDPOINT),
        "idx-fail",
        &IndexEvent::IndexSucceeded,
    )
    .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "idx-fail").await?;
    assert_eq!(snap.index_status, "indexed");

    // PurgeSucceeded: pending -> purged
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "purge-ok",
        DEFAULT_DATESTAMP,
        "deleted",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;
    index_db::transition(
        &pool,
        &scope(ENDPOINT),
        "purge-ok",
        &IndexEvent::PurgeSucceeded,
    )
    .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "purge-ok").await?;
    assert_eq!(snap.index_status, "purged");

    // PurgeFailed: pending -> purge_failed
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "purge-fail",
        DEFAULT_DATESTAMP,
        "deleted",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;
    index_db::transition(
        &pool,
        &scope(ENDPOINT),
        "purge-fail",
        &IndexEvent::PurgeFailed {
            message: "solr down",
        },
    )
    .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "purge-fail").await?;
    assert_eq!(snap.index_status, "purge_failed");

    // PurgeSucceeded from purge_failed: purge_failed -> purged
    index_db::transition(
        &pool,
        &scope(ENDPOINT),
        "purge-fail",
        &IndexEvent::PurgeSucceeded,
    )
    .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "purge-fail").await?;
    assert_eq!(snap.index_status, "purged");

    Ok(())
}

// ---------------------------------------------------------------------------
// Wildcard resets allowed by triggers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wildcard_resets_are_allowed_by_triggers() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    // Status wildcard: parsed -> pending (import re-harvest)
    insert_record(
        &pool,
        ENDPOINT,
        "parsed-to-pending",
        DEFAULT_DATESTAMP,
        "parsed",
    )
    .await?;
    sqlx::query("UPDATE oai_records SET status = 'pending' WHERE identifier = $1")
        .bind("parsed-to-pending")
        .execute(&pool)
        .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "parsed-to-pending").await?;
    assert_eq!(snap.status, "pending");

    // Status wildcard: available -> deleted (import marks deletion)
    insert_record(
        &pool,
        ENDPOINT,
        "available-to-deleted",
        DEFAULT_DATESTAMP,
        "available",
    )
    .await?;
    sqlx::query("UPDATE oai_records SET status = 'deleted' WHERE identifier = $1")
        .bind("available-to-deleted")
        .execute(&pool)
        .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "available-to-deleted").await?;
    assert_eq!(snap.status, "deleted");

    // Index wildcard: indexed -> pending (reindex batch reset)
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "indexed-to-pending",
        DEFAULT_DATESTAMP,
        "parsed",
        "indexed",
        "",
        1,
        metadata(REPOSITORY),
    )
    .await?;
    index_db::reindex(&pool, &scope(ENDPOINT), REPOSITORY).await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "indexed-to-pending").await?;
    assert_eq!(snap.index_status, "pending");

    // Index wildcard: purged -> pending (metadata success resets index lifecycle)
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "purged-to-pending",
        DEFAULT_DATESTAMP,
        "deleted",
        "purged",
        "",
        1,
        metadata(REPOSITORY),
    )
    .await?;
    sqlx::query("UPDATE oai_records SET index_status = 'pending' WHERE identifier = $1")
        .bind("purged-to-pending")
        .execute(&pool)
        .await?;
    let snap = fetch_record_snapshot(&pool, ENDPOINT, "purged-to-pending").await?;
    assert_eq!(snap.index_status, "pending");

    Ok(())
}

// ---------------------------------------------------------------------------
// Illegal status transitions rejected by trigger
// ---------------------------------------------------------------------------

#[tokio::test]
async fn illegal_status_transitions_are_rejected() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    // pending -> parsed (skips available)
    insert_record(
        &pool,
        ENDPOINT,
        "skip-available",
        DEFAULT_DATESTAMP,
        "pending",
    )
    .await?;
    let result = sqlx::query("UPDATE oai_records SET status = 'parsed' WHERE identifier = $1")
        .bind("skip-available")
        .execute(&pool)
        .await;
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("illegal status transition"),
        "Expected trigger error, got: {err}"
    );

    // parsed -> available (invalid backward step)
    insert_record(&pool, ENDPOINT, "parsed-back", DEFAULT_DATESTAMP, "parsed").await?;
    let result = sqlx::query("UPDATE oai_records SET status = 'available' WHERE identifier = $1")
        .bind("parsed-back")
        .execute(&pool)
        .await;
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("illegal status transition"),
        "Expected trigger error, got: {err}"
    );

    // failed -> available (must go through pending first)
    insert_record(
        &pool,
        ENDPOINT,
        "failed-to-avail",
        DEFAULT_DATESTAMP,
        "failed",
    )
    .await?;
    let result = sqlx::query("UPDATE oai_records SET status = 'available' WHERE identifier = $1")
        .bind("failed-to-avail")
        .execute(&pool)
        .await;
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("illegal status transition"),
        "Expected trigger error, got: {err}"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Illegal index_status transitions rejected by trigger
// ---------------------------------------------------------------------------

#[tokio::test]
async fn illegal_index_status_transitions_are_rejected() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    // indexed -> index_failed (not a valid transition)
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "idx-to-fail",
        DEFAULT_DATESTAMP,
        "parsed",
        "indexed",
        "",
        1,
        metadata(REPOSITORY),
    )
    .await?;
    let result =
        sqlx::query("UPDATE oai_records SET index_status = 'index_failed' WHERE identifier = $1")
            .bind("idx-to-fail")
            .execute(&pool)
            .await;
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("illegal index_status transition"),
        "Expected trigger error, got: {err}"
    );

    // purged -> indexed (not a valid transition)
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "purged-to-idx",
        DEFAULT_DATESTAMP,
        "deleted",
        "purged",
        "",
        1,
        metadata(REPOSITORY),
    )
    .await?;
    let result =
        sqlx::query("UPDATE oai_records SET index_status = 'indexed' WHERE identifier = $1")
            .bind("purged-to-idx")
            .execute(&pool)
            .await;
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("illegal index_status transition"),
        "Expected trigger error, got: {err}"
    );

    Ok(())
}
