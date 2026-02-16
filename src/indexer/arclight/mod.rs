pub mod cli;
pub mod config;

use std::{
    process::{Output, Stdio},
    time::Duration,
};

use futures::future::BoxFuture;
use reqwest::Client;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

use crate::{OaiRecordId, indexer::Indexer};

use config::ArcLightIndexerConfig;

pub struct ArcLightIndexer {
    config: ArcLightIndexerConfig,
    client: Client,
}

impl ArcLightIndexer {
    pub fn new(config: ArcLightIndexerConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    fn solr_update_url(&self) -> String {
        format!("{}/update", self.config.solr_url.trim_end_matches('/'))
    }

    fn timeout_duration(&self) -> Duration {
        Duration::from_secs(self.config.record_timeout_seconds)
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
    fn index_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let output = self.run_traject(record).await?;

            if output.status.success() {
                Ok(())
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!("traject failed: {}", stderr);
            }
        })
    }

    fn delete_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
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
                    anyhow::bail!("failed to call Solr delete API: {}", error);
                }
                Err(_) => {
                    anyhow::bail!(
                        "Solr delete timed out after {}s",
                        self.config.record_timeout_seconds
                    );
                }
            };

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                anyhow::bail!("Solr delete API returned {}: {}", status, body);
            }

            Ok(())
        })
    }
}
