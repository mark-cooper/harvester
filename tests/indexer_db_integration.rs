mod support;

use harvester::{
    OaiRecordId,
    db::{
        FetchIndexCandidatesParams, ResetIndexStateParams, UpdateIndexFailureParams,
        UpdateIndexStatusParams, do_mark_index_failure_query, do_mark_index_success_query,
        do_mark_purge_failure_query, do_mark_purge_success_query, do_reset_index_state_query,
        fetch_failed_records_for_indexing, fetch_failed_records_for_purging,
        fetch_pending_records_for_indexing, fetch_pending_records_for_purging,
    },
};
use support::{
    DEFAULT_DATESTAMP, METADATA_PREFIX, acquire_test_lock, fetch_record_snapshot,
    insert_record_with_index, setup_test_pool,
};

const ENDPOINT: &str = "https://example.org/oai";
const REPOSITORY: &str = "Integration Repository";

fn metadata(repository: &str) -> serde_json::Value {
    serde_json::json!({ "repository": [repository] })
}

fn identifiers(records: Vec<OaiRecordId>) -> Vec<String> {
    records
        .into_iter()
        .map(|record| record.identifier)
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

    let params = FetchIndexCandidatesParams {
        endpoint: ENDPOINT,
        metadata_prefix: METADATA_PREFIX,
        oai_repository: REPOSITORY,
        max_attempts: None,
        message_filter: None,
        last_identifier: None,
    };

    let to_index = fetch_pending_records_for_indexing(&pool, params).await?;
    assert_eq!(identifiers(to_index), vec!["a-pending-parsed".to_string()]);

    let params = FetchIndexCandidatesParams {
        endpoint: ENDPOINT,
        metadata_prefix: METADATA_PREFIX,
        oai_repository: REPOSITORY,
        max_attempts: None,
        message_filter: None,
        last_identifier: None,
    };
    let to_purge = fetch_pending_records_for_purging(&pool, params).await?;
    assert_eq!(identifiers(to_purge), vec!["c-pending-deleted".to_string()]);

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

    let params = FetchIndexCandidatesParams {
        endpoint: ENDPOINT,
        metadata_prefix: METADATA_PREFIX,
        oai_repository: REPOSITORY,
        max_attempts: Some(5),
        message_filter: Some("timed out"),
        last_identifier: None,
    };
    let failed_index = fetch_failed_records_for_indexing(&pool, params).await?;
    assert_eq!(
        identifiers(failed_index),
        vec!["a-timeout-low-attempts".to_string()]
    );

    let params = FetchIndexCandidatesParams {
        endpoint: ENDPOINT,
        metadata_prefix: METADATA_PREFIX,
        oai_repository: REPOSITORY,
        max_attempts: Some(3),
        message_filter: Some("timed out"),
        last_identifier: None,
    };
    let failed_purge = fetch_failed_records_for_purging(&pool, params).await?;
    assert_eq!(
        identifiers(failed_purge),
        vec!["d-purge-timeout".to_string()]
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

    do_mark_index_failure_query(
        &pool,
        UpdateIndexFailureParams {
            endpoint: ENDPOINT,
            metadata_prefix: METADATA_PREFIX,
            identifier: "index-transition",
            message: "traject failed",
        },
    )
    .await?;
    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "index-transition").await?;
    assert_eq!(snapshot.index_status, "index_failed");
    assert_eq!(snapshot.index_attempts, 1);
    assert_eq!(snapshot.index_message, "traject failed");
    assert!(!snapshot.indexed_at_set);

    do_mark_index_success_query(
        &pool,
        UpdateIndexStatusParams {
            endpoint: ENDPOINT,
            metadata_prefix: METADATA_PREFIX,
            identifier: "index-transition",
        },
    )
    .await?;
    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "index-transition").await?;
    assert_eq!(snapshot.index_status, "indexed");
    assert_eq!(snapshot.index_message, "");
    assert!(snapshot.indexed_at_set);
    assert!(!snapshot.purged_at_set);

    do_mark_purge_failure_query(
        &pool,
        UpdateIndexFailureParams {
            endpoint: ENDPOINT,
            metadata_prefix: METADATA_PREFIX,
            identifier: "purge-transition",
            message: "solr failed",
        },
    )
    .await?;
    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "purge-transition").await?;
    assert_eq!(snapshot.index_status, "purge_failed");
    assert_eq!(snapshot.index_attempts, 1);
    assert_eq!(snapshot.index_message, "solr failed");

    do_mark_purge_success_query(
        &pool,
        UpdateIndexStatusParams {
            endpoint: ENDPOINT,
            metadata_prefix: METADATA_PREFIX,
            identifier: "purge-transition",
        },
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
async fn reset_requeues_all_parsed_and_deleted_records() -> anyhow::Result<()> {
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

    let result = do_reset_index_state_query(
        &pool,
        ResetIndexStateParams {
            endpoint: ENDPOINT,
            metadata_prefix: METADATA_PREFIX,
        },
    )
    .await?;
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

    Ok(())
}
