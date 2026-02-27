#![allow(dead_code)]

use std::{
    collections::HashMap,
    env, fs,
    path::PathBuf,
    sync::{
        Arc, Once, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::Context;
use harvester::{ARCLIGHT_METADATA_PREFIX, Harvester, OaiConfig};
use sqlx::{
    PgPool, Row,
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, MutexGuard},
    task::JoinHandle,
};

pub const METADATA_PREFIX: &str = ARCLIGHT_METADATA_PREFIX;
pub const DEFAULT_DATESTAMP: &str = "2026-02-07";
pub const EAD_XML: &str = r#"<ead xmlns="urn:isbn:1-931666-22-9"><archdesc><did><repository><corpname>Integration Repository</corpname></repository><unittitle>Integration Title</unittitle><unitid>ID-INT-001</unitid></did></archdesc></ead>"#;
const RULES_CSV: &str =
    "title,unittitle,required\nunit_id,unitid,required\nrepository,repository/corpname,required\n";

static MIGRATOR: Migrator = sqlx::migrate!();
static TEST_COUNTER: AtomicUsize = AtomicUsize::new(1);

pub struct RecordSnapshot {
    pub status: String,
    pub message: String,
    pub datestamp: String,
    pub version: i32,
    pub metadata: serde_json::Value,
    pub index_status: String,
    pub index_message: String,
    pub index_attempts: i32,
    pub indexed_at_set: bool,
    pub purged_at_set: bool,
}

#[derive(Clone)]
pub struct HeaderSpec {
    identifier: String,
    datestamp: String,
    status: Option<String>,
}

#[derive(Clone)]
pub enum GetRecordSpec {
    Payload(String),
    MissingPayload,
}

#[derive(Clone)]
pub struct MockOaiConfig {
    pub headers: Vec<HeaderSpec>,
    pub records: HashMap<String, GetRecordSpec>,
}

pub struct MockOaiServer {
    pub endpoint: String,
    handle: JoinHandle<()>,
}

pub struct MockSolrServer {
    pub solr_url: String,
    handle: JoinHandle<()>,
}

impl Drop for MockSolrServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Drop for MockOaiServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub fn header_spec(identifier: &str, datestamp: &str, status: Option<&str>) -> HeaderSpec {
    HeaderSpec {
        identifier: identifier.to_string(),
        datestamp: datestamp.to_string(),
        status: status.map(ToString::to_string),
    }
}

pub async fn acquire_test_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

pub async fn setup_test_pool() -> anyhow::Result<PgPool> {
    load_test_env();
    let database_url =
        env::var("DATABASE_URL").context("DATABASE_URL was not found; expected .env.test")?;

    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
    {
        Ok(pool) => pool,
        Err(error) if error.to_string().contains("does not exist") => {
            ensure_test_database_exists(&database_url).await?;
            PgPoolOptions::new()
                .max_connections(5)
                .connect(&database_url)
                .await?
        }
        Err(error) => return Err(error.into()),
    };

    reset_test_database(&pool).await?;
    Ok(pool)
}

pub async fn run_harvest(
    pool: &PgPool,
    endpoint: &str,
    data_dir: PathBuf,
    rules: Option<PathBuf>,
) -> anyhow::Result<()> {
    let config = OaiConfig {
        data_dir,
        endpoint: endpoint.to_string(),
        metadata_prefix: METADATA_PREFIX.to_string(),
        oai_timeout: 10,
        oai_retries: 0,
    };
    let harvester = Harvester::new(config, pool.clone());
    harvester.run(rules).await
}

pub async fn insert_record(
    pool: &PgPool,
    endpoint: &str,
    identifier: &str,
    datestamp: &str,
    status: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO oai_records (endpoint, metadata_prefix, identifier, datestamp, status)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(endpoint)
    .bind(METADATA_PREFIX)
    .bind(identifier)
    .bind(datestamp)
    .bind(status)
    .execute(pool)
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_record_with_index(
    pool: &PgPool,
    endpoint: &str,
    identifier: &str,
    datestamp: &str,
    status: &str,
    index_status: &str,
    index_message: &str,
    index_attempts: i32,
    metadata: serde_json::Value,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO oai_records (
            endpoint, metadata_prefix, identifier, datestamp, status, metadata,
            index_status, index_message, index_attempts
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(endpoint)
    .bind(METADATA_PREFIX)
    .bind(identifier)
    .bind(datestamp)
    .bind(status)
    .bind(metadata)
    .bind(index_status)
    .bind(index_message)
    .bind(index_attempts)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn fetch_fingerprint(
    pool: &PgPool,
    endpoint: &str,
    identifier: &str,
) -> anyhow::Result<String> {
    let row = sqlx::query(
        r#"
        SELECT fingerprint
        FROM oai_records
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
    )
    .bind(endpoint)
    .bind(METADATA_PREFIX)
    .bind(identifier)
    .fetch_one(pool)
    .await?;

    Ok(row.try_get("fingerprint")?)
}

pub async fn fetch_record_snapshot(
    pool: &PgPool,
    endpoint: &str,
    identifier: &str,
) -> anyhow::Result<RecordSnapshot> {
    let row = sqlx::query(
        r#"
        SELECT status, message, datestamp, version, metadata,
               index_status, index_message, index_attempts,
               indexed_at IS NOT NULL AS indexed_at_set,
               purged_at IS NOT NULL AS purged_at_set
        FROM oai_records
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
    )
    .bind(endpoint)
    .bind(METADATA_PREFIX)
    .bind(identifier)
    .fetch_one(pool)
    .await?;

    Ok(RecordSnapshot {
        status: row.try_get("status")?,
        message: row.try_get("message")?,
        datestamp: row.try_get("datestamp")?,
        version: row.try_get("version")?,
        metadata: row.try_get("metadata")?,
        index_status: row.try_get("index_status")?,
        index_message: row.try_get("index_message")?,
        index_attempts: row.try_get("index_attempts")?,
        indexed_at_set: row.try_get("indexed_at_set")?,
        purged_at_set: row.try_get("purged_at_set")?,
    })
}

pub async fn count_records_for_identifier(
    pool: &PgPool,
    endpoint: &str,
    identifier: &str,
) -> anyhow::Result<i64> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*) AS count
        FROM oai_records
        WHERE endpoint = $1 AND metadata_prefix = $2 AND identifier = $3
        "#,
    )
    .bind(endpoint)
    .bind(METADATA_PREFIX)
    .bind(identifier)
    .fetch_one(pool)
    .await?;

    Ok(row.try_get("count")?)
}

pub fn create_rules_file(name: &str) -> anyhow::Result<PathBuf> {
    let path = unique_path(name).with_extension("csv");
    fs::write(&path, RULES_CSV)?;
    Ok(path)
}

pub fn create_temp_dir(name: &str) -> anyhow::Result<PathBuf> {
    let path = unique_path(name);
    fs::create_dir_all(&path)?;
    Ok(path)
}

pub fn create_temp_file(name: &str) -> anyhow::Result<PathBuf> {
    let path = unique_path(name).with_extension("tmp");
    fs::write(&path, b"x")?;
    Ok(path)
}

pub fn create_traject_shim(name: &str) -> anyhow::Result<PathBuf> {
    let dir = unique_path(name);
    fs::create_dir_all(&dir)?;
    let path = dir.join("traject");
    fs::write(
        &path,
        r#"#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--version" ]]; then
  echo "traject-shim 0.1"
  exit 0
fi

mode="${TRAJECT_SHIM_MODE:-success}"
case "$mode" in
  success)
    exit 0
    ;;
  fail)
    echo "${TRAJECT_SHIM_MESSAGE:-shim failure}" >&2
    exit 1
    ;;
  fail_on_id)
    target_id=""
    for arg in "$@"; do
      if [[ "$arg" == id=* ]]; then
        target_id="${arg#id=}"
      fi
    done
    if [[ -n "${TRAJECT_SHIM_FAIL_ID:-}" && "$target_id" == "${TRAJECT_SHIM_FAIL_ID}" ]]; then
      echo "${TRAJECT_SHIM_MESSAGE:-shim failure}" >&2
      exit 1
    fi
    exit 0
    ;;
  sleep)
    sleep "${TRAJECT_SHIM_SLEEP_SECONDS:-1}"
    exit 0
    ;;
  *)
    echo "unknown traject shim mode: ${mode}" >&2
    exit 2
    ;;
esac
"#,
    )?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
    }

    Ok(path)
}

pub async fn start_mock_solr_server(
    status_code: u16,
    body: &str,
) -> anyhow::Result<MockSolrServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let solr_url = format!("http://{}/solr/arclight", address);
    let body = body.to_string();

    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(value) => value,
                Err(_) => break,
            };
            let body = body.clone();
            tokio::spawn(async move {
                if let Err(error) = handle_solr_connection(&mut socket, status_code, &body).await {
                    eprintln!("mock Solr request handling failed: {}", error);
                }
            });
        }
    });

    Ok(MockSolrServer { solr_url, handle })
}

pub async fn start_mock_oai_server(config: MockOaiConfig) -> anyhow::Result<MockOaiServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let endpoint = format!("http://{}", address);
    let endpoint_for_task = endpoint.clone();
    let shared_config = Arc::new(config);

    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(value) => value,
                Err(_) => break,
            };
            let endpoint = endpoint_for_task.clone();
            let config = shared_config.clone();
            tokio::spawn(async move {
                if let Err(error) = handle_connection(&mut socket, &endpoint, &config).await {
                    eprintln!("mock OAI server request handling failed: {}", error);
                }
            });
        }
    });

    Ok(MockOaiServer { endpoint, handle })
}

fn load_test_env() {
    static LOAD_ENV: Once = Once::new();
    LOAD_ENV.call_once(|| {
        let _ = dotenvy::from_filename_override(".env.test");
    });
}

async fn ensure_test_database_exists(database_url: &str) -> anyhow::Result<()> {
    let connect_options: PgConnectOptions = database_url.parse()?;
    let database_name = connect_options
        .get_database()
        .context("DATABASE_URL is missing a database name")?
        .to_string();
    let admin_options = connect_options.database("postgres");
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin_options)
        .await?;

    let create_database_query =
        format!("CREATE DATABASE \"{}\"", database_name.replace('"', "\"\""));

    match sqlx::query(&create_database_query)
        .execute(&admin_pool)
        .await
    {
        Ok(_) => Ok(()),
        Err(error) => {
            if let Some(database_error) = error.as_database_error()
                && database_error.code().as_deref() == Some("42P04")
            {
                return Ok(());
            }
            Err(error.into())
        }
    }
}

async fn reset_test_database(pool: &PgPool) -> anyhow::Result<()> {
    MIGRATOR.undo(pool, 0).await?;
    MIGRATOR.run(pool).await?;
    Ok(())
}

fn unique_path(name: &str) -> PathBuf {
    let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!("harvester-{name}-{id}"))
}

async fn handle_connection(
    socket: &mut TcpStream,
    endpoint: &str,
    config: &MockOaiConfig,
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 8192];
    let mut total = 0usize;

    loop {
        let bytes_read = socket.read(&mut buf[total..]).await?;
        if bytes_read == 0 {
            return Ok(());
        }
        total += bytes_read;
        if buf[..total].windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if total == buf.len() {
            break;
        }
    }

    let request = String::from_utf8_lossy(&buf[..total]);
    let request_line = request.lines().next().unwrap_or_default();
    let path = request_line.split_whitespace().nth(1).unwrap_or("/");
    let params = parse_query_params(path);
    let body = build_oai_response(endpoint, config, &params);
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/xml; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

async fn handle_solr_connection(
    socket: &mut TcpStream,
    status_code: u16,
    body: &str,
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 8192];
    let mut total = 0usize;

    loop {
        let bytes_read = socket.read(&mut buf[total..]).await?;
        if bytes_read == 0 {
            return Ok(());
        }
        total += bytes_read;
        if buf[..total].windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if total == buf.len() {
            break;
        }
    }

    let status_text = if status_code == 200 { "OK" } else { "ERROR" };
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status_code,
        status_text,
        body.len(),
        body
    );

    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

fn parse_query_params(path: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let query = path.split_once('?').map(|(_, query)| query).unwrap_or("");
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        params.insert(key.to_string(), value.to_string());
    }
    params
}

fn build_oai_response(
    endpoint: &str,
    config: &MockOaiConfig,
    params: &HashMap<String, String>,
) -> String {
    match params.get("verb").map(|value| value.as_str()) {
        Some("Identify") => identify_response(endpoint),
        Some("ListIdentifiers") => list_identifiers_response(endpoint, params, &config.headers),
        Some("GetRecord") => get_record_response(endpoint, params, config),
        _ => error_response(endpoint, params, "badVerb", "Unknown or missing verb"),
    }
}

fn identify_response(endpoint: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-02-07T00:00:00Z</responseDate>
  <request verb="Identify">{endpoint}</request>
  <Identify>
    <repositoryName>Integration Test Repository</repositoryName>
    <baseURL>{endpoint}</baseURL>
    <protocolVersion>2.0</protocolVersion>
    <adminEmail>integration@example.com</adminEmail>
    <earliestDatestamp>2026-01-01</earliestDatestamp>
    <deletedRecord>persistent</deletedRecord>
    <granularity>YYYY-MM-DD</granularity>
  </Identify>
</OAI-PMH>"#
    )
}

fn list_identifiers_response(
    endpoint: &str,
    params: &HashMap<String, String>,
    headers: &[HeaderSpec],
) -> String {
    let verb = params
        .get("verb")
        .map(String::as_str)
        .unwrap_or("ListIdentifiers");
    let metadata_prefix = params
        .get("metadataPrefix")
        .map(String::as_str)
        .unwrap_or(METADATA_PREFIX);
    let header_xml = headers
        .iter()
        .map(|header| match header.status.as_deref() {
            Some(status) => format!(
                "<header status=\"{status}\"><identifier>{}</identifier><datestamp>{}</datestamp></header>",
                header.identifier, header.datestamp
            ),
            None => format!(
                "<header><identifier>{}</identifier><datestamp>{}</datestamp></header>",
                header.identifier, header.datestamp
            ),
        })
        .collect::<Vec<_>>()
        .join("");

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-02-07T00:00:00Z</responseDate>
  <request verb="{verb}" metadataPrefix="{metadata_prefix}">{endpoint}</request>
  <ListIdentifiers>{header_xml}</ListIdentifiers>
</OAI-PMH>"#
    )
}

fn get_record_response(
    endpoint: &str,
    params: &HashMap<String, String>,
    config: &MockOaiConfig,
) -> String {
    let identifier = params
        .get("identifier")
        .map(String::as_str)
        .unwrap_or_default();
    let metadata_prefix = params
        .get("metadataPrefix")
        .map(String::as_str)
        .unwrap_or(METADATA_PREFIX);
    let datestamp = config
        .headers
        .iter()
        .find(|header| header.identifier == identifier)
        .map(|header| header.datestamp.as_str())
        .unwrap_or(DEFAULT_DATESTAMP);

    match config.records.get(identifier) {
        Some(GetRecordSpec::Payload(metadata)) => format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-02-07T00:00:00Z</responseDate>
  <request verb="GetRecord" metadataPrefix="{metadata_prefix}" identifier="{identifier}">{endpoint}</request>
  <GetRecord>
    <record>
      <header>
        <identifier>{identifier}</identifier>
        <datestamp>{datestamp}</datestamp>
      </header>
      <metadata>{metadata}</metadata>
    </record>
  </GetRecord>
</OAI-PMH>"#
        ),
        Some(GetRecordSpec::MissingPayload) => format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-02-07T00:00:00Z</responseDate>
  <request verb="GetRecord" metadataPrefix="{metadata_prefix}" identifier="{identifier}">{endpoint}</request>
</OAI-PMH>"#
        ),
        None => error_response(endpoint, params, "idDoesNotExist", "Unknown identifier"),
    }
}

fn error_response(
    endpoint: &str,
    params: &HashMap<String, String>,
    code: &str,
    message: &str,
) -> String {
    let verb = params.get("verb").map(String::as_str).unwrap_or("Unknown");
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-02-07T00:00:00Z</responseDate>
  <request verb="{verb}">{endpoint}</request>
  <error code="{code}">{message}</error>
</OAI-PMH>"#
    )
}
