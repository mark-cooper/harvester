mod support;

use harvester::{
    IndexSelectionMode, OaiRecord,
    db::indexer::{FetchIndexCandidatesParams, fetch, reindex, transition},
    oai::{IndexEvent, OaiRecordStatus},
};
use support::{
    DEFAULT_DATESTAMP, acquire_test_lock, fetch_record_snapshot, insert_record_with_index,
    metadata, scope, setup_test_pool,
};

const ENDPOINT: &str = "https://example.org/oai";
const REPOSITORY: &str = "Integration Repository";

fn records_with_status(records: Vec<OaiRecord>) -> Vec<(String, OaiRecordStatus)> {
    records
        .into_iter()
        .map(|record| (record.identifier, record.status))
        .collect()
}

#[tokio::test]
async fn pending_candidate_queries_only_select_pending_records() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "a-pending-parsed",
        DEFAULT_DATESTAMP,
        "parsed",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "b-failed-parsed",
        DEFAULT_DATESTAMP,
        "parsed",
        "index_failed",
        "boom",
        1,
        metadata(REPOSITORY),
    )
    .await?;
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "c-pending-deleted",
        DEFAULT_DATESTAMP,
        "deleted",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "d-wrong-repository",
        DEFAULT_DATESTAMP,
        "parsed",
        "pending",
        "",
        0,
        metadata("Other Repository"),
    )
    .await?;

    let s = scope(ENDPOINT);
    let params = FetchIndexCandidatesParams {
        scope: &s,
        source_repository: REPOSITORY,
        selection_mode: IndexSelectionMode::PendingOnly,
        max_attempts: None,
        message_filter: None,
        last_identifier: None,
    };

    let pending = fetch(&pool, params).await?;
    assert_eq!(
        records_with_status(pending),
        vec![
            ("a-pending-parsed".to_string(), OaiRecordStatus::Parsed),
            ("c-pending-deleted".to_string(), OaiRecordStatus::Deleted),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn failed_candidate_queries_apply_attempt_and_message_filters() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "a-timeout-low-attempts",
        DEFAULT_DATESTAMP,
        "parsed",
        "index_failed",
        "timed out waiting for traject",
        1,
        metadata(REPOSITORY),
    )
    .await?;
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "b-timeout-maxed",
        DEFAULT_DATESTAMP,
        "parsed",
        "index_failed",
        "timed out waiting for traject",
        5,
        metadata(REPOSITORY),
    )
    .await?;
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "c-other-message",
        DEFAULT_DATESTAMP,
        "parsed",
        "index_failed",
        "connection reset",
        1,
        metadata(REPOSITORY),
    )
    .await?;
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "d-purge-timeout",
        DEFAULT_DATESTAMP,
        "deleted",
        "purge_failed",
        "timed out calling solr",
        2,
        metadata(REPOSITORY),
    )
    .await?;

    let s = scope(ENDPOINT);
    let params = FetchIndexCandidatesParams {
        scope: &s,
        source_repository: REPOSITORY,
        selection_mode: IndexSelectionMode::FailedOnly,
        max_attempts: Some(5),
        message_filter: Some("timed out"),
        last_identifier: None,
    };
    let failed = fetch(&pool, params).await?;
    assert_eq!(
        records_with_status(failed),
        vec![
            (
                "a-timeout-low-attempts".to_string(),
                OaiRecordStatus::Parsed
            ),
            ("d-purge-timeout".to_string(), OaiRecordStatus::Deleted),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn transition_updates_set_expected_index_lifecycle_fields() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "index-transition",
        DEFAULT_DATESTAMP,
        "parsed",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "purge-transition",
        DEFAULT_DATESTAMP,
        "deleted",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;

    transition(
        &pool,
        &scope(ENDPOINT),
        "index-transition",
        &IndexEvent::IndexFailed {
            message: "traject failed",
        },
    )
    .await?;
    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "index-transition").await?;
    assert_eq!(snapshot.index_status, "index_failed");
    assert_eq!(snapshot.index_attempts, 1);
    assert_eq!(snapshot.index_message, "traject failed");
    assert!(!snapshot.indexed_at_set);

    transition(
        &pool,
        &scope(ENDPOINT),
        "index-transition",
        &IndexEvent::IndexSucceeded,
    )
    .await?;
    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "index-transition").await?;
    assert_eq!(snapshot.index_status, "indexed");
    assert_eq!(snapshot.index_message, "");
    assert!(snapshot.indexed_at_set);
    assert!(!snapshot.purged_at_set);

    transition(
        &pool,
        &scope(ENDPOINT),
        "purge-transition",
        &IndexEvent::PurgeFailed {
            message: "solr failed",
        },
    )
    .await?;
    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "purge-transition").await?;
    assert_eq!(snapshot.index_status, "purge_failed");
    assert_eq!(snapshot.index_attempts, 1);
    assert_eq!(snapshot.index_message, "solr failed");

    transition(
        &pool,
        &scope(ENDPOINT),
        "purge-transition",
        &IndexEvent::PurgeSucceeded,
    )
    .await?;
    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "purge-transition").await?;
    assert_eq!(snapshot.index_status, "purged");
    assert_eq!(snapshot.index_message, "");
    assert!(snapshot.purged_at_set);
    assert!(!snapshot.indexed_at_set);

    Ok(())
}

#[tokio::test]
async fn reindex_requeues_only_matching_repository_records() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "parsed-indexed",
        DEFAULT_DATESTAMP,
        "parsed",
        "indexed",
        "",
        4,
        metadata(REPOSITORY),
    )
    .await?;
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "deleted-purged",
        DEFAULT_DATESTAMP,
        "deleted",
        "purged",
        "",
        2,
        metadata(REPOSITORY),
    )
    .await?;
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "parsed-other-repository",
        DEFAULT_DATESTAMP,
        "parsed",
        "indexed",
        "should stay indexed",
        3,
        metadata("Other Repository"),
    )
    .await?;

    let result = reindex(&pool, &scope(ENDPOINT), REPOSITORY).await?;
    assert_eq!(result.rows_affected(), 2);

    let parsed = fetch_record_snapshot(&pool, ENDPOINT, "parsed-indexed").await?;
    assert_eq!(parsed.status, "parsed");
    assert_eq!(parsed.index_status, "pending");
    assert_eq!(parsed.index_attempts, 0);
    assert_eq!(parsed.index_message, "");
    assert!(!parsed.indexed_at_set);

    let deleted = fetch_record_snapshot(&pool, ENDPOINT, "deleted-purged").await?;
    assert_eq!(deleted.status, "deleted");
    assert_eq!(deleted.index_status, "pending");
    assert_eq!(deleted.index_attempts, 0);
    assert_eq!(deleted.index_message, "");
    assert!(!deleted.purged_at_set);

    let other_repository =
        fetch_record_snapshot(&pool, ENDPOINT, "parsed-other-repository").await?;
    assert_eq!(other_repository.status, "parsed");
    assert_eq!(other_repository.index_status, "indexed");
    assert_eq!(other_repository.index_attempts, 3);
    assert_eq!(other_repository.index_message, "should stay indexed");

    Ok(())
}
