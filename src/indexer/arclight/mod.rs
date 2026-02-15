pub mod cli;
pub mod config;

use std::{
    process::{Output, Stdio},
    time::Duration,
};

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
    indexer::{self, IndexSelectionMode, Indexer, truncate_middle},
};

use config::ArcLightIndexerConfig;

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
