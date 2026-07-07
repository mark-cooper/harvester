mod support;

use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
    sync::atomic::AtomicBool,
};

use sqlx::PgPool;

use harvester::{
    ARCLIGHT_METADATA_PREFIX, ArcLightIndexer, ArcLightIndexerConfig, IndexRunOptions, IndexRunner,
    IndexRunnerConfig,
};
use support::{
    DEFAULT_DATESTAMP, acquire_test_lock, create_temp_dir, create_temp_file, create_traject_shim,
    fetch_fingerprint, fetch_latest_run, fetch_record_snapshot, insert_record_with_index, metadata,
    setup_test_pool, start_mock_solr_server,
};

const ENDPOINT: &str = "https://indexer.example.org/oai";
const REPOSITORY: &str = "Integration Repository";
const REPOSITORY_ID: &str = "integration-repo";

struct EnvVarGuard {
    key: String,
    original: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &str, value: String) -> Self {
        let original = env::var(key).ok();
        // Tests use a global mutex lock, so mutating process environment is synchronized.
        unsafe { env::set_var(key, value) };
        Self {
            key: key.to_string(),
            original,
        }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(value) = &self.original {
            // Tests use a global mutex lock, so mutating process environment is synchronized.
            unsafe { env::set_var(&self.key, value) };
        } else {
            // Tests use a global mutex lock, so mutating process environment is synchronized.
            unsafe { env::remove_var(&self.key) };
        }
    }
}

fn prepend_path(dir: &Path) -> EnvVarGuard {
    let path = env::var("PATH").unwrap_or_default();
    EnvVarGuard::set("PATH", format!("{}:{}", dir.display(), path))
}

fn build_config(
    configuration: PathBuf,
    data_dir: PathBuf,
    repository_file: PathBuf,
    solr_url: String,
) -> ArcLightIndexerConfig {
    ArcLightIndexerConfig {
        configuration,
        dir: data_dir,
        repository: REPOSITORY_ID.to_string(),
        repository_file,
        record_timeout_seconds: 5,
        solr_url,
        solr_commit_within_ms: 1000,
    }
}

fn build_runner(
    indexer: ArcLightIndexer,
    pool: PgPool,
    run_options: IndexRunOptions,
    preview: bool,
) -> IndexRunner<ArcLightIndexer> {
    let config = IndexRunnerConfig {
        scope: harvester::OaiScope::new(ENDPOINT, ARCLIGHT_METADATA_PREFIX),
        source_repository: REPOSITORY.to_string(),
        run_options,
        preview,
    };

    IndexRunner::new(indexer, config, pool, Arc::new(AtomicBool::new(false)))
}

#[tokio::test]
async fn index_success_marks_record_indexed() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "index-success",
        DEFAULT_DATESTAMP,
        "parsed",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;

    let configuration = create_temp_file("index-success-config")?;
    let data_dir = create_temp_dir("index-success-data")?;
    let repository_file = create_temp_file("index-success-repo-file")?;
    let shim = create_traject_shim("index-success-traject")?;
    let _path_guard = prepend_path(shim.parent().unwrap());
    let _mode_guard = EnvVarGuard::set("TRAJECT_SHIM_MODE", "success".to_string());
    let solr = start_mock_solr_server(200, r#"{"responseHeader":{"status":0}}"#).await?;

    let config = build_config(
        configuration,
        data_dir,
        repository_file,
        solr.solr_url.clone(),
    );
    let indexer = ArcLightIndexer::new(config);
    let runner = build_runner(
        indexer,
        pool.clone(),
        IndexRunOptions::standard(Some(5)),
        false,
    );
    runner.run().await?;

    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "index-success").await?;
    assert_eq!(snapshot.status, "parsed");
    assert_eq!(snapshot.index_status.as_deref(), Some("indexed"));
    assert_eq!(snapshot.index_attempts, Some(0));
    assert_eq!(snapshot.index_message.as_deref(), Some(""));
    assert!(snapshot.indexed_at_set);
    assert!(!snapshot.purged_at_set);

    Ok(())
}

#[tokio::test]
async fn index_failure_marks_record_index_failed() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "index-failure",
        DEFAULT_DATESTAMP,
        "parsed",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;

    let configuration = create_temp_file("index-failure-config")?;
    let data_dir = create_temp_dir("index-failure-data")?;
    let repository_file = create_temp_file("index-failure-repo-file")?;
    let shim = create_traject_shim("index-failure-traject")?;
    let _path_guard = prepend_path(shim.parent().unwrap());
    let _mode_guard = EnvVarGuard::set("TRAJECT_SHIM_MODE", "fail".to_string());
    let _message_guard =
        EnvVarGuard::set("TRAJECT_SHIM_MESSAGE", "shim traject failure".to_string());
    let solr = start_mock_solr_server(200, r#"{"responseHeader":{"status":0}}"#).await?;

    let config = build_config(
        configuration,
        data_dir,
        repository_file,
        solr.solr_url.clone(),
    );
    let indexer = ArcLightIndexer::new(config);
    let runner = build_runner(
        indexer,
        pool.clone(),
        IndexRunOptions::standard(Some(5)),
        false,
    );
    let result = runner.run().await;
    assert!(result.is_err());

    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "index-failure").await?;
    assert_eq!(snapshot.status, "parsed");
    assert_eq!(snapshot.index_status.as_deref(), Some("index_failed"));
    assert_eq!(snapshot.index_attempts, Some(1));
    assert!(
        snapshot
            .index_message
            .as_deref()
            .unwrap_or_default()
            .contains("shim traject failure")
    );
    assert!(!snapshot.indexed_at_set);

    Ok(())
}

#[tokio::test]
async fn index_batch_mixed_results_continue_processing_remaining_records() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "index-mixed-success",
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
        "index-mixed-failure",
        DEFAULT_DATESTAMP,
        "parsed",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;

    let failure_fingerprint = fetch_fingerprint(&pool, ENDPOINT, "index-mixed-failure").await?;
    let configuration = create_temp_file("index-mixed-config")?;
    let data_dir = create_temp_dir("index-mixed-data")?;
    let repository_file = create_temp_file("index-mixed-repo-file")?;
    let shim = create_traject_shim("index-mixed-traject")?;
    let _path_guard = prepend_path(shim.parent().unwrap());
    let _mode_guard = EnvVarGuard::set("TRAJECT_SHIM_MODE", "fail_on_id".to_string());
    let _failure_guard = EnvVarGuard::set("TRAJECT_SHIM_FAIL_ID", failure_fingerprint);
    let _message_guard =
        EnvVarGuard::set("TRAJECT_SHIM_MESSAGE", "shim targeted failure".to_string());
    let solr = start_mock_solr_server(200, r#"{"responseHeader":{"status":0}}"#).await?;

    let config = build_config(
        configuration,
        data_dir,
        repository_file,
        solr.solr_url.clone(),
    );
    let indexer = ArcLightIndexer::new(config);
    let runner = build_runner(
        indexer,
        pool.clone(),
        IndexRunOptions::standard(Some(5)),
        false,
    );
    let result = runner.run().await;
    assert!(result.is_err());
    let message = result.unwrap_err().to_string();
    assert!(message.contains("1 failed record(s)"));

    let success = fetch_record_snapshot(&pool, ENDPOINT, "index-mixed-success").await?;
    assert_eq!(success.index_status.as_deref(), Some("indexed"));
    assert_eq!(success.index_attempts, Some(0));
    assert_eq!(success.index_message.as_deref(), Some(""));
    assert!(success.indexed_at_set);

    let failed = fetch_record_snapshot(&pool, ENDPOINT, "index-mixed-failure").await?;
    assert_eq!(failed.index_status.as_deref(), Some("index_failed"));
    assert_eq!(failed.index_attempts, Some(1));
    assert!(
        failed
            .index_message
            .as_deref()
            .unwrap_or_default()
            .contains("shim targeted failure")
    );
    assert!(!failed.indexed_at_set);

    Ok(())
}

#[tokio::test]
async fn delete_success_marks_record_purged() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "delete-success",
        DEFAULT_DATESTAMP,
        "deleted",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;

    let solr = start_mock_solr_server(200, r#"{"responseHeader":{"status":0}}"#).await?;
    let configuration = create_temp_file("delete-success-config")?;
    let data_dir = create_temp_dir("delete-success-data")?;
    let repository_file = create_temp_file("delete-success-repo-file")?;

    let config = build_config(
        configuration,
        data_dir,
        repository_file,
        solr.solr_url.clone(),
    );
    let indexer = ArcLightIndexer::new(config);
    let runner = build_runner(
        indexer,
        pool.clone(),
        IndexRunOptions::standard(Some(5)),
        false,
    );
    runner.run().await?;

    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "delete-success").await?;
    assert_eq!(snapshot.status, "deleted");
    assert_eq!(snapshot.index_status.as_deref(), Some("purged"));
    assert_eq!(snapshot.index_message.as_deref(), Some(""));
    assert!(snapshot.purged_at_set);

    let run = fetch_latest_run(&pool, ENDPOINT).await?;
    assert_eq!(run.kind, "index");
    assert_eq!(run.outcome, "completed");
    assert_eq!(run.deleted, 1);
    assert_eq!(run.failed, 0);
    assert_eq!(run.error_sample, "");
    assert!(run.finished_at_set);

    Ok(())
}

#[tokio::test]
async fn preflight_aborts_run_when_solr_is_unreachable() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "preflight-unreachable",
        DEFAULT_DATESTAMP,
        "deleted",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;

    let configuration = create_temp_file("preflight-config")?;
    let data_dir = create_temp_dir("preflight-data")?;
    let repository_file = create_temp_file("preflight-repo-file")?;

    let config = build_config(
        configuration,
        data_dir,
        repository_file,
        // Unroutable: connection is refused, so preflight fails fast.
        "http://127.0.0.1:9/solr/arclight".to_string(),
    );
    let indexer = ArcLightIndexer::new(config);
    let runner = build_runner(
        indexer,
        pool.clone(),
        IndexRunOptions::standard(Some(5)),
        false,
    );
    let result = runner.run().await;
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("preflight failed"),
        "expected preflight error, got: {message}"
    );

    // No attempt was consumed.
    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "preflight-unreachable").await?;
    assert_eq!(snapshot.index_status.as_deref(), Some("pending"));
    assert_eq!(snapshot.index_attempts, Some(0));

    let run = fetch_latest_run(&pool, ENDPOINT).await?;
    assert_eq!(run.outcome, "failed");
    assert!(run.error_sample.contains("preflight failed"));

    Ok(())
}

#[tokio::test]
async fn circuit_breaker_aborts_run_after_mass_failure() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    // 150 purge candidates against a Solr that errors on every call: the
    // first full batch (100) fails, trips the breaker, and the remaining 50
    // keep their pending state with no attempt consumed.
    for n in 0..150 {
        insert_record_with_index(
            &pool,
            ENDPOINT,
            &format!("breaker-{n:03}"),
            DEFAULT_DATESTAMP,
            "deleted",
            "pending",
            "",
            0,
            metadata(REPOSITORY),
        )
        .await?;
    }

    let solr = start_mock_solr_server(500, r#"{"error":"boom"}"#).await?;
    let configuration = create_temp_file("breaker-config")?;
    let data_dir = create_temp_dir("breaker-data")?;
    let repository_file = create_temp_file("breaker-repo-file")?;

    let config = build_config(
        configuration,
        data_dir,
        repository_file,
        solr.solr_url.clone(),
    );
    let indexer = ArcLightIndexer::new(config);
    let runner = build_runner(
        indexer,
        pool.clone(),
        IndexRunOptions::standard(Some(5)),
        false,
    );
    let result = runner.run().await;
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("circuit breaker"),
        "expected circuit breaker error, got: {message}"
    );

    let failed = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM indexer_records WHERE status = 'purge_failed'",
    )
    .fetch_one(&pool)
    .await?;
    let pending = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM indexer_records WHERE status = 'pending'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(failed, 100, "exactly one full batch consumed attempts");
    assert_eq!(pending, 50, "records after the trip were not attempted");

    let run = fetch_latest_run(&pool, ENDPOINT).await?;
    assert_eq!(run.outcome, "failed");
    assert_eq!(run.failed, 100);
    assert!(!run.error_sample.is_empty());

    Ok(())
}

#[tokio::test]
async fn delete_failure_marks_record_purge_failed() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "delete-failure",
        DEFAULT_DATESTAMP,
        "deleted",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;

    let solr = start_mock_solr_server(500, r#"{"error":"boom"}"#).await?;
    let configuration = create_temp_file("delete-failure-config")?;
    let data_dir = create_temp_dir("delete-failure-data")?;
    let repository_file = create_temp_file("delete-failure-repo-file")?;

    let config = build_config(
        configuration,
        data_dir,
        repository_file,
        solr.solr_url.clone(),
    );
    let indexer = ArcLightIndexer::new(config);
    let runner = build_runner(
        indexer,
        pool.clone(),
        IndexRunOptions::standard(Some(5)),
        false,
    );
    let result = runner.run().await;
    assert!(result.is_err());

    let snapshot = fetch_record_snapshot(&pool, ENDPOINT, "delete-failure").await?;
    assert_eq!(snapshot.status, "deleted");
    assert_eq!(snapshot.index_status.as_deref(), Some("purge_failed"));
    assert_eq!(snapshot.index_attempts, Some(1));
    assert!(
        snapshot
            .index_message
            .as_deref()
            .unwrap_or_default()
            .contains("500")
    );
    assert!(!snapshot.purged_at_set);

    Ok(())
}

#[tokio::test]
async fn delete_batch_failures_continue_processing_remaining_records() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "delete-batch-fail-a",
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
        "delete-batch-fail-b",
        DEFAULT_DATESTAMP,
        "deleted",
        "pending",
        "",
        0,
        metadata(REPOSITORY),
    )
    .await?;

    let solr = start_mock_solr_server(500, r#"{"error":"boom"}"#).await?;
    let configuration = create_temp_file("delete-batch-fail-config")?;
    let data_dir = create_temp_dir("delete-batch-fail-data")?;
    let repository_file = create_temp_file("delete-batch-fail-repo-file")?;

    let config = build_config(
        configuration,
        data_dir,
        repository_file,
        solr.solr_url.clone(),
    );
    let indexer = ArcLightIndexer::new(config);
    let runner = build_runner(
        indexer,
        pool.clone(),
        IndexRunOptions::standard(Some(5)),
        false,
    );
    let result = runner.run().await;
    assert!(result.is_err());
    let message = result.unwrap_err().to_string();
    assert!(message.contains("2 failed record(s)"));

    let first = fetch_record_snapshot(&pool, ENDPOINT, "delete-batch-fail-a").await?;
    assert_eq!(first.index_status.as_deref(), Some("purge_failed"));
    assert_eq!(first.index_attempts, Some(1));
    assert!(
        first
            .index_message
            .as_deref()
            .unwrap_or_default()
            .contains("500")
    );
    assert!(!first.purged_at_set);

    let second = fetch_record_snapshot(&pool, ENDPOINT, "delete-batch-fail-b").await?;
    assert_eq!(second.index_status.as_deref(), Some("purge_failed"));
    assert_eq!(second.index_attempts, Some(1));
    assert!(
        second
            .index_message
            .as_deref()
            .unwrap_or_default()
            .contains("500")
    );
    assert!(!second.purged_at_set);

    Ok(())
}

#[tokio::test]
async fn preview_mode_has_no_side_effects() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;

    insert_record_with_index(
        &pool,
        ENDPOINT,
        "preview-index",
        DEFAULT_DATESTAMP,
        "parsed",
        "pending",
        "queued",
        0,
        metadata(REPOSITORY),
    )
    .await?;
    insert_record_with_index(
        &pool,
        ENDPOINT,
        "preview-delete",
        DEFAULT_DATESTAMP,
        "deleted",
        "pending",
        "queued",
        0,
        metadata(REPOSITORY),
    )
    .await?;

    let configuration = create_temp_file("preview-config")?;
    let data_dir = create_temp_dir("preview-data")?;
    let repository_file = create_temp_file("preview-repo-file")?;

    let config = build_config(
        configuration,
        data_dir,
        repository_file,
        "http://127.0.0.1:65535/solr/arclight".to_string(),
    );
    let indexer = ArcLightIndexer::new(config);
    let runner = build_runner(
        indexer,
        pool.clone(),
        IndexRunOptions::standard(Some(5)),
        true,
    );
    runner.run().await?;

    let indexed = fetch_record_snapshot(&pool, ENDPOINT, "preview-index").await?;
    assert_eq!(indexed.index_status.as_deref(), Some("pending"));
    assert_eq!(indexed.index_message.as_deref(), Some("queued"));
    assert_eq!(indexed.index_attempts, Some(0));
    assert!(!indexed.indexed_at_set);

    let deleted = fetch_record_snapshot(&pool, ENDPOINT, "preview-delete").await?;
    assert_eq!(deleted.index_status.as_deref(), Some("pending"));
    assert_eq!(deleted.index_message.as_deref(), Some("queued"));
    assert_eq!(deleted.index_attempts, Some(0));
    assert!(!deleted.purged_at_set);

    Ok(())
}
