use std::{
    path::PathBuf,
    process::{Output, Stdio},
    time::Duration,
};

use clap::Args;
use futures::future::BoxFuture;
use reqwest::Client;
use sqlx::PgPool;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

use crate::{
    OaiRecordId,
    db::{
        FetchIndexCandidatesParams, UpdateIndexFailureParams, UpdateIndexStatusParams,
        do_mark_index_failure_query, do_mark_index_success_query, do_mark_purge_failure_query,
        do_mark_purge_success_query, fetch_failed_records_for_indexing,
        fetch_failed_records_for_purging, fetch_pending_records_for_indexing,
        fetch_pending_records_for_purging,
    },
    indexer::{self, Indexer, truncate_middle},
};

#[derive(Debug, Args)]
pub struct ArcLightArgs {
    /// Target repository id
    pub repository: String,

    /// Source OAI endpoint url
    pub oai_endpoint: String,

    /// Source OAI repository name
    pub oai_repository: String,

    /// Traject configuration file path
    #[arg(short, long, default_value = "traject/ead2_config.rb")]
    pub configuration: PathBuf,

    /// EAD base directory
    #[arg(short, long, default_value = "data")]
    pub dir: PathBuf,

    /// Preview mode (show matching records, do not index or delete)
    #[arg(short, long, default_value_t = false)]
    pub preview: bool,

    /// Repositories yaml file
    #[arg(short, long, default_value = "config/repositories.yml")]
    pub repository_file: PathBuf,

    /// Solr url
    #[arg(short, long, default_value = "http://127.0.0.1:8983/solr/arclight")]
    pub solr_url: String,

    /// Per-record timeout for traject and Solr operations
    #[arg(long, default_value_t = 300)]
    pub record_timeout_seconds: u64,

    /// Solr commit-within window for delete operations (interval commit strategy)
    #[arg(long, default_value_t = 10000)]
    pub solr_commit_within_ms: u64,
}

#[derive(Debug, Clone, Copy)]
enum IndexSelectionMode {
    FailedOnly,
    PendingOnly,
}

#[derive(Debug, Clone)]
pub struct ArcLightRunOptions {
    selection_mode: IndexSelectionMode,
    message_filter: Option<String>,
    max_attempts: Option<i32>,
}

impl ArcLightRunOptions {
    pub fn failed_only(message_filter: Option<String>, max_attempts: Option<i32>) -> Self {
        Self {
            selection_mode: IndexSelectionMode::FailedOnly,
            message_filter,
            max_attempts,
        }
    }

    pub fn pending_only() -> Self {
        Self {
            selection_mode: IndexSelectionMode::PendingOnly,
            message_filter: None,
            max_attempts: None,
        }
    }
}

pub struct ArcLightIndexer {
    config: ArcLightIndexerConfig,
    client: Client,
    pool: PgPool,
}

impl ArcLightIndexer {
    pub fn new(config: ArcLightIndexerConfig, pool: PgPool) -> Self {
        Self {
            config,
            client: Client::new(),
            pool,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        indexer::run(self).await
    }

    fn solr_update_url(&self) -> String {
        format!("{}/update", self.config.solr_url.trim_end_matches('/'))
    }

    fn timeout_duration(&self) -> Duration {
        Duration::from_secs(self.config.record_timeout_seconds)
    }

    async fn mark_index_failure(&self, record: &OaiRecordId, message: &str) -> anyhow::Result<()> {
        let params = UpdateIndexFailureParams {
            endpoint: &self.config.oai_endpoint,
            metadata_prefix: &self.config.metadata_prefix,
            identifier: &record.identifier,
            message,
        };
        do_mark_index_failure_query(&self.pool, params).await?;
        Ok(())
    }

    async fn mark_purge_failure(&self, record: &OaiRecordId, message: &str) -> anyhow::Result<()> {
        let params = UpdateIndexFailureParams {
            endpoint: &self.config.oai_endpoint,
            metadata_prefix: &self.config.metadata_prefix,
            identifier: &record.identifier,
            message,
        };
        do_mark_purge_failure_query(&self.pool, params).await?;
        Ok(())
    }

    async fn run_traject(&self, record: &OaiRecordId) -> anyhow::Result<Output> {
        let path = self.config.dir.join(record.path());

        let mut child = Command::new("traject")
            .arg("-i")
            .arg("xml")
            .arg("-c")
            .arg(&self.config.configuration)
            .arg("-s")
            .arg(format!("repository={}", &self.config.repository))
            .arg("-s")
            .arg(format!("id={}", &record.fingerprint))
            .arg("-u")
            .arg(&self.config.solr_url)
            .arg(path)
            .env("REPOSITORY_FILE", &self.config.repository_file)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to capture traject stdout"))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to capture traject stderr"))?;

        let stdout_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            stdout.read_to_end(&mut buf).await?;
            Ok::<Vec<u8>, std::io::Error>(buf)
        });
        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            stderr.read_to_end(&mut buf).await?;
            Ok::<Vec<u8>, std::io::Error>(buf)
        });

        let status = match timeout(self.timeout_duration(), child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                let _ = stdout_task.await;
                let _ = stderr_task.await;
                return Err(error.into());
            }
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                let _ = stdout_task.await;
                let _ = stderr_task.await;
                anyhow::bail!(
                    "traject timed out after {}s",
                    self.config.record_timeout_seconds
                );
            }
        };

        let stdout = stdout_task
            .await
            .map_err(|err| anyhow::anyhow!("failed to collect traject stdout output: {}", err))??;
        let stderr = stderr_task
            .await
            .map_err(|err| anyhow::anyhow!("failed to collect traject stderr output: {}", err))??;

        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }
}

impl Indexer for ArcLightIndexer {
    fn fetch_records_to_index<'a>(
        &'a self,
        last_identifier: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>> {
        Box::pin(async move {
            let params = FetchIndexCandidatesParams {
                endpoint: &self.config.oai_endpoint,
                metadata_prefix: &self.config.metadata_prefix,
                oai_repository: &self.config.oai_repository,
                max_attempts: self.config.max_attempts,
                message_filter: self.config.message_filter.as_deref(),
                last_identifier,
            };
            Ok(match self.config.selection_mode {
                IndexSelectionMode::FailedOnly => {
                    fetch_failed_records_for_indexing(&self.pool, params).await?
                }
                IndexSelectionMode::PendingOnly => {
                    fetch_pending_records_for_indexing(&self.pool, params).await?
                }
            })
        })
    }

    fn fetch_records_to_purge<'a>(
        &'a self,
        last_identifier: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>> {
        Box::pin(async move {
            let params = FetchIndexCandidatesParams {
                endpoint: &self.config.oai_endpoint,
                metadata_prefix: &self.config.metadata_prefix,
                oai_repository: &self.config.oai_repository,
                max_attempts: self.config.max_attempts,
                message_filter: self.config.message_filter.as_deref(),
                last_identifier,
            };
            Ok(match self.config.selection_mode {
                IndexSelectionMode::FailedOnly => {
                    fetch_failed_records_for_purging(&self.pool, params).await?
                }
                IndexSelectionMode::PendingOnly => {
                    fetch_pending_records_for_purging(&self.pool, params).await?
                }
            })
        })
    }

    fn index_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if self.config.preview {
                println!("Would index record: {}", record.identifier);
                return Ok(());
            }

            let output = match self.run_traject(record).await {
                Ok(output) => output,
                Err(error) => {
                    let message = truncate_middle(&error.to_string(), 200, 200);
                    self.mark_index_failure(record, &message).await?;
                    anyhow::bail!("{}", message);
                }
            };

            if output.status.success() {
                let params = UpdateIndexStatusParams {
                    endpoint: &self.config.oai_endpoint,
                    metadata_prefix: &self.config.metadata_prefix,
                    identifier: &record.identifier,
                };
                do_mark_index_success_query(&self.pool, params).await?;
                Ok(())
            } else {
                let stderr = truncate_middle(&String::from_utf8_lossy(&output.stderr), 200, 200);
                self.mark_index_failure(record, &stderr).await?;
                anyhow::bail!("traject failed: {}", stderr);
            }
        })
    }

    fn delete_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if self.config.preview {
                println!("Would delete record: {}", record.identifier);
                return Ok(());
            }

            let payload = serde_json::json!({
                "delete": {
                    "query": format!("_root_:{}", record.fingerprint),
                    "commitWithin": self.config.solr_commit_within_ms
                }
            });
            let payload = payload.to_string();

            let response = match timeout(
                self.timeout_duration(),
                self.client
                    .post(self.solr_update_url())
                    .header("Content-Type", "application/json")
                    .body(payload)
                    .send(),
            )
            .await
            {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    let message = truncate_middle(
                        &format!("failed to call Solr delete API: {}", error),
                        200,
                        200,
                    );
                    self.mark_purge_failure(record, &message).await?;
                    anyhow::bail!("{}", message);
                }
                Err(_) => {
                    let message = format!(
                        "Solr delete timed out after {}s",
                        self.config.record_timeout_seconds
                    );
                    self.mark_purge_failure(record, &message).await?;
                    anyhow::bail!("{}", message);
                }
            };

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                let body = truncate_middle(&body, 200, 200);
                let message = format!("Solr delete API returned {}: {}", status, body);
                self.mark_purge_failure(record, &message).await?;
                anyhow::bail!("{}", message);
            }

            let params = UpdateIndexStatusParams {
                endpoint: &self.config.oai_endpoint,
                metadata_prefix: &self.config.metadata_prefix,
                identifier: &record.identifier,
            };
            do_mark_purge_success_query(&self.pool, params).await?;
            Ok(())
        })
    }
}

#[derive(Debug, Clone)]
pub struct ArcLightIndexerConfig {
    configuration: PathBuf,
    dir: PathBuf,
    repository: String,
    oai_endpoint: String,
    oai_repository: String,
    preview: bool,
    repository_file: PathBuf,
    selection_mode: IndexSelectionMode,
    message_filter: Option<String>,
    max_attempts: Option<i32>,
    record_timeout_seconds: u64,
    solr_url: String,
    solr_commit_within_ms: u64,
    metadata_prefix: String,
}

#[derive(Debug, Clone)]
pub struct ArcLightIndexerConfigInput {
    pub configuration: PathBuf,
    pub dir: PathBuf,
    pub repository: String,
    pub oai_endpoint: String,
    pub oai_repository: String,
    pub preview: bool,
    pub repository_file: PathBuf,
    pub record_timeout_seconds: u64,
    pub solr_url: String,
    pub solr_commit_within_ms: u64,
    pub run_options: ArcLightRunOptions,
}

impl ArcLightIndexerConfig {
    pub fn new(input: ArcLightIndexerConfigInput) -> Self {
        Self {
            configuration: input.configuration,
            dir: input.dir,
            repository: input.repository,
            oai_endpoint: input.oai_endpoint,
            oai_repository: input.oai_repository,
            preview: input.preview,
            repository_file: input.repository_file,
            selection_mode: input.run_options.selection_mode,
            message_filter: input.run_options.message_filter,
            max_attempts: input.run_options.max_attempts,
            record_timeout_seconds: input.record_timeout_seconds,
            solr_url: input.solr_url,
            solr_commit_within_ms: input.solr_commit_within_ms,
            metadata_prefix: "oai_ead".to_string(),
        }
    }
}
