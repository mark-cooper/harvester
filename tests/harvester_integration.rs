mod support;

use std::{collections::HashMap, fs};

use harvester::OaiRecordId;
use support::{
    DEFAULT_DATESTAMP, EAD_XML, GetRecordSpec, MockOaiConfig, acquire_test_lock,
    count_records_for_identifier, create_rules_file, create_temp_dir, create_temp_file,
    fetch_fingerprint, fetch_record_snapshot, header_spec, insert_record, run_harvest,
    setup_test_pool, start_mock_oai_server,
};

#[tokio::test]
async fn run_without_rules_leaves_records_available() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;
    let data_dir = create_temp_dir("download-success")?;
    let identifier = "record-success";

    let mut records = HashMap::new();
    records.insert(
        identifier.to_string(),
        GetRecordSpec::Payload(EAD_XML.to_string()),
    );
    let server = start_mock_oai_server(MockOaiConfig {
        headers: vec![header_spec(identifier, DEFAULT_DATESTAMP, None)],
        records,
    })
    .await?;

    run_harvest(&pool, &server.endpoint, data_dir.clone(), None).await?;

    let snapshot = fetch_record_snapshot(&pool, &server.endpoint, identifier).await?;
    assert_eq!(snapshot.status, "available");
    assert!(snapshot.message.is_empty());
    assert_eq!(snapshot.metadata, serde_json::json!({}));

    let fingerprint = fetch_fingerprint(&pool, &server.endpoint, identifier).await?;
    let record = OaiRecordId {
        identifier: identifier.to_string(),
        fingerprint,
    };
    assert!(data_dir.join(record.path()).is_file());
    Ok(())
}

#[tokio::test]
async fn import_marks_deleted_headers_as_deleted() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;
    let data_dir = create_temp_dir("import-deleted")?;
    let identifier = "record-deleted";

    let server = start_mock_oai_server(MockOaiConfig {
        headers: vec![header_spec(identifier, DEFAULT_DATESTAMP, Some("deleted"))],
        records: HashMap::new(),
    })
    .await?;

    run_harvest(&pool, &server.endpoint, data_dir, None).await?;

    let snapshot = fetch_record_snapshot(&pool, &server.endpoint, identifier).await?;
    assert_eq!(snapshot.status, "deleted");
    assert!(snapshot.message.is_empty());
    Ok(())
}

#[tokio::test]
async fn import_is_idempotent_for_same_data() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;
    let data_dir = create_temp_dir("import-idempotent")?;
    let identifier = "record-idempotent";

    let mut records = HashMap::new();
    records.insert(
        identifier.to_string(),
        GetRecordSpec::Payload(EAD_XML.to_string()),
    );
    let server = start_mock_oai_server(MockOaiConfig {
        headers: vec![header_spec(identifier, DEFAULT_DATESTAMP, None)],
        records,
    })
    .await?;

    run_harvest(&pool, &server.endpoint, data_dir.clone(), None).await?;
    run_harvest(&pool, &server.endpoint, data_dir, None).await?;

    let count = count_records_for_identifier(&pool, &server.endpoint, identifier).await?;
    assert_eq!(count, 1);

    let snapshot = fetch_record_snapshot(&pool, &server.endpoint, identifier).await?;
    assert_eq!(snapshot.status, "available");
    assert_eq!(snapshot.version, 1);
    Ok(())
}

#[tokio::test]
async fn import_does_not_reset_failed_record() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;
    let data_dir = create_temp_dir("import-preserve-failed")?;
    let identifier = "record-failed-sticky";
    let original_datestamp = "2026-02-01";
    let newer_datestamp = "2026-02-10";

    let mut records = HashMap::new();
    records.insert(
        identifier.to_string(),
        GetRecordSpec::Payload(EAD_XML.to_string()),
    );
    let server = start_mock_oai_server(MockOaiConfig {
        headers: vec![header_spec(identifier, newer_datestamp, None)],
        records,
    })
    .await?;

    insert_record(
        &pool,
        &server.endpoint,
        identifier,
        original_datestamp,
        "failed",
    )
    .await?;

    run_harvest(&pool, &server.endpoint, data_dir, None).await?;

    let count = count_records_for_identifier(&pool, &server.endpoint, identifier).await?;
    assert_eq!(count, 1);

    let snapshot = fetch_record_snapshot(&pool, &server.endpoint, identifier).await?;
    assert_eq!(snapshot.status, "failed");
    assert_eq!(snapshot.datestamp, original_datestamp);
    assert_eq!(snapshot.version, 1);
    Ok(())
}

#[tokio::test]
async fn download_marks_failed_when_payload_is_missing() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;
    let data_dir = create_temp_dir("download-missing-payload")?;
    let identifier = "record-missing-payload";

    let mut records = HashMap::new();
    records.insert(identifier.to_string(), GetRecordSpec::MissingPayload);
    let server = start_mock_oai_server(MockOaiConfig {
        headers: vec![header_spec(identifier, DEFAULT_DATESTAMP, None)],
        records,
    })
    .await?;

    run_harvest(&pool, &server.endpoint, data_dir, None).await?;

    let snapshot = fetch_record_snapshot(&pool, &server.endpoint, identifier).await?;
    assert_eq!(snapshot.status, "failed");
    assert!(snapshot.message.contains("missing payload"));
    Ok(())
}

#[tokio::test]
async fn download_marks_failed_when_write_to_disk_fails() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;
    let data_dir_file = create_temp_file("download-write-failure")?;
    let identifier = "record-write-failure";

    let mut records = HashMap::new();
    records.insert(
        identifier.to_string(),
        GetRecordSpec::Payload(EAD_XML.to_string()),
    );
    let server = start_mock_oai_server(MockOaiConfig {
        headers: vec![header_spec(identifier, DEFAULT_DATESTAMP, None)],
        records,
    })
    .await?;

    run_harvest(&pool, &server.endpoint, data_dir_file, None).await?;

    let snapshot = fetch_record_snapshot(&pool, &server.endpoint, identifier).await?;
    assert_eq!(snapshot.status, "failed");
    assert!(snapshot.message.contains("Failed to write metadata file"));
    Ok(())
}

#[tokio::test]
async fn download_batch_handles_mixed_record_outcomes() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;
    let data_dir = create_temp_dir("download-mixed-batch")?;
    let success_id = "record-success-batch";
    let missing_payload_id = "record-missing-payload-batch";
    let missing_record_id = "record-missing-batch";

    let mut records = HashMap::new();
    records.insert(
        success_id.to_string(),
        GetRecordSpec::Payload(EAD_XML.to_string()),
    );
    records.insert(
        missing_payload_id.to_string(),
        GetRecordSpec::MissingPayload,
    );

    let server = start_mock_oai_server(MockOaiConfig {
        headers: vec![
            header_spec(success_id, DEFAULT_DATESTAMP, None),
            header_spec(missing_payload_id, DEFAULT_DATESTAMP, None),
            header_spec(missing_record_id, DEFAULT_DATESTAMP, None),
        ],
        records,
    })
    .await?;

    run_harvest(&pool, &server.endpoint, data_dir, None).await?;

    let success = fetch_record_snapshot(&pool, &server.endpoint, success_id).await?;
    assert_eq!(success.status, "available");

    let missing_payload =
        fetch_record_snapshot(&pool, &server.endpoint, missing_payload_id).await?;
    assert_eq!(missing_payload.status, "failed");
    assert!(missing_payload.message.contains("missing payload"));

    let missing_record = fetch_record_snapshot(&pool, &server.endpoint, missing_record_id).await?;
    assert_eq!(missing_record.status, "failed");
    assert!(!missing_record.message.is_empty());
    Ok(())
}

#[tokio::test]
async fn metadata_missing_file_marks_failed_and_continues() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;
    let data_dir = create_temp_dir("metadata-continue")?;
    let rules_path = create_rules_file("metadata-rules")?;
    let missing_identifier = "a-missing-file";
    let present_identifier = "b-existing-file";

    let server = start_mock_oai_server(MockOaiConfig {
        headers: vec![header_spec(
            "placeholder-deleted",
            DEFAULT_DATESTAMP,
            Some("deleted"),
        )],
        records: HashMap::new(),
    })
    .await?;

    insert_record(
        &pool,
        &server.endpoint,
        missing_identifier,
        DEFAULT_DATESTAMP,
        "available",
    )
    .await?;
    insert_record(
        &pool,
        &server.endpoint,
        present_identifier,
        DEFAULT_DATESTAMP,
        "available",
    )
    .await?;

    let present_fingerprint =
        fetch_fingerprint(&pool, &server.endpoint, present_identifier).await?;
    let present_record = OaiRecordId {
        identifier: present_identifier.to_string(),
        fingerprint: present_fingerprint,
    };
    let present_path = data_dir.join(present_record.path());
    if let Some(parent) = present_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(present_path, EAD_XML)?;

    run_harvest(&pool, &server.endpoint, data_dir, Some(rules_path)).await?;

    let missing = fetch_record_snapshot(&pool, &server.endpoint, missing_identifier).await?;
    assert_eq!(missing.status, "failed");
    assert!(missing.message.contains("Unable to open metadata file"));

    let present = fetch_record_snapshot(&pool, &server.endpoint, present_identifier).await?;
    assert_eq!(present.status, "parsed");
    Ok(())
}

#[tokio::test]
async fn metadata_invalid_xml_marks_failed() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;
    let data_dir = create_temp_dir("metadata-invalid-xml")?;
    let rules_path = create_rules_file("metadata-rules-invalid")?;
    let identifier = "record-invalid-xml";

    let server = start_mock_oai_server(MockOaiConfig {
        headers: vec![header_spec(
            "placeholder-deleted",
            DEFAULT_DATESTAMP,
            Some("deleted"),
        )],
        records: HashMap::new(),
    })
    .await?;

    insert_record(
        &pool,
        &server.endpoint,
        identifier,
        DEFAULT_DATESTAMP,
        "available",
    )
    .await?;

    let fingerprint = fetch_fingerprint(&pool, &server.endpoint, identifier).await?;
    let record = OaiRecordId {
        identifier: identifier.to_string(),
        fingerprint,
    };
    let path = data_dir.join(record.path());
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, "<ead><archdesc><did>")?;

    run_harvest(&pool, &server.endpoint, data_dir, Some(rules_path)).await?;

    let snapshot = fetch_record_snapshot(&pool, &server.endpoint, identifier).await?;
    assert_eq!(snapshot.status, "failed");
    assert!(
        snapshot.message.contains("XML") || snapshot.message.contains("Required field"),
        "unexpected metadata failure message: {}",
        snapshot.message
    );
    Ok(())
}

#[tokio::test]
async fn full_pipeline_with_rules_parses_metadata() -> anyhow::Result<()> {
    let _guard = acquire_test_lock().await;
    let pool = setup_test_pool().await?;
    let data_dir = create_temp_dir("full-pipeline-rules")?;
    let rules_path = create_rules_file("full-pipeline-rules")?;
    let identifier = "record-full-pipeline";

    let mut records = HashMap::new();
    records.insert(
        identifier.to_string(),
        GetRecordSpec::Payload(EAD_XML.to_string()),
    );
    let server = start_mock_oai_server(MockOaiConfig {
        headers: vec![header_spec(identifier, DEFAULT_DATESTAMP, None)],
        records,
    })
    .await?;

    run_harvest(&pool, &server.endpoint, data_dir, Some(rules_path)).await?;

    let snapshot = fetch_record_snapshot(&pool, &server.endpoint, identifier).await?;
    assert_eq!(snapshot.status, "parsed");
    assert_eq!(
        snapshot.metadata["title"],
        serde_json::json!(["Integration Title"])
    );
    assert_eq!(
        snapshot.metadata["repository"],
        serde_json::json!(["Integration Repository"])
    );
    Ok(())
}
