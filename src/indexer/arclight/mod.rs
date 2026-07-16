pub mod cli;
pub mod config;

use std::{
    io,
    process::{Output, Stdio},
    time::Duration,
};

use futures::future::BoxFuture;
use reqwest::Client;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

use crate::{OaiRecord, indexer::Indexer};

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

    async fn run_traject(&self, record: &OaiRecord) -> anyhow::Result<Output> {
        let path = self.config.dir.join(record.path());

        let mut child = Command::new("traject")
            .arg("-i")
            .arg("xml")
            .arg("-c")
            .arg(&self.config.configuration)
            .arg("-s")
            .arg(format!("repository={}", self.config.repository))
            .arg("-s")
            .arg(format!("id={}", record.fingerprint))
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
            Ok::<Vec<u8>, io::Error>(buf)
        });
        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            stderr.read_to_end(&mut buf).await?;
            Ok::<Vec<u8>, io::Error>(buf)
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

    /// Delete a root document and all its nested children from Solr.
    async fn solr_delete_by_root(&self, fingerprint: &str) -> anyhow::Result<()> {
        let commit_within =
            (!self.config.solr_no_commit).then_some(self.config.solr_commit_within_ms);
        let payload = delete_by_root_payload(fingerprint, commit_within);
        let url = self.solr_update_url();

        let response = match timeout(
            self.timeout_duration(),
            self.client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(payload.to_string())
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
    }

    fn solr_update_url(&self) -> String {
        format!("{}/update", self.config.solr_url.trim_end_matches('/'))
    }

    fn timeout_duration(&self) -> Duration {
        Duration::from_secs(self.config.record_timeout_seconds)
    }
}

/// Build the Solr `/update` delete-by-query body for a root fingerprint.
/// `commit_within_ms` adds a `commitWithin` directive when set; `None`
/// (from `--no-commit`) omits it so visibility defers to Solr's autoCommit.
fn delete_by_root_payload(fingerprint: &str, commit_within_ms: Option<u64>) -> serde_json::Value {
    let mut delete = serde_json::json!({ "query": format!("_root_:{}", fingerprint) });
    if let Some(ms) = commit_within_ms {
        delete["commitWithin"] = serde_json::json!(ms);
    }
    serde_json::json!({ "delete": delete })
}

/// Drop Ruby Logger info/debug banner lines (`I, [timestamp #pid]  INFO -- : ...`)
/// so the first retained line of a traject failure is the exception itself,
/// not boilerplate that survives truncation at the expense of the real error.
fn strip_ruby_logger_noise(stderr: &str) -> String {
    let filtered: Vec<&str> = stderr
        .lines()
        .filter(|line| !line.starts_with("I, [") && !line.starts_with("D, ["))
        .collect();

    if filtered.is_empty() {
        stderr.to_string()
    } else {
        filtered.join("\n")
    }
}

impl Indexer for ArcLightIndexer {
    /// Solr reachability check. Any HTTP response counts as reachable — a
    /// Solr that answers with an error status still gets accurate per-record
    /// handling; only connection failures and timeouts abort the run.
    fn preflight<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let url = format!("{}/admin/ping", self.config.solr_url.trim_end_matches('/'));
            match timeout(self.timeout_duration(), self.client.get(&url).send()).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(error)) => {
                    anyhow::bail!("Solr is unreachable at {}: {}", self.config.solr_url, error)
                }
                Err(_) => anyhow::bail!(
                    "Solr ping timed out after {}s at {}",
                    self.config.record_timeout_seconds,
                    self.config.solr_url
                ),
            }
        })
    }

    fn index_record<'a>(&'a self, record: &'a OaiRecord) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            // Delete existing root + nested children first to avoid orphaned
            // child documents when Solr re-indexes a nested document block.
            // Awaited before the add below so the delete is ordered ahead of it.
            self.solr_delete_by_root(&record.fingerprint).await?;

            let output = self.run_traject(record).await?;

            if output.status.success() {
                Ok(())
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!("traject failed: {}", strip_ruby_logger_noise(&stderr));
            }
        })
    }

    fn delete_record<'a>(&'a self, record: &'a OaiRecord) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { self.solr_delete_by_root(&record.fingerprint).await })
    }
}

#[cfg(test)]
mod tests {
    use super::{delete_by_root_payload, strip_ruby_logger_noise};

    #[test]
    fn delete_payload_includes_commit_within_by_default() {
        let payload = delete_by_root_payload("abc123", Some(10_000));
        let delete = &payload["delete"];
        assert_eq!(delete["query"], "_root_:abc123");
        assert_eq!(delete["commitWithin"], 10_000);
    }

    #[test]
    fn delete_payload_omits_commit_within_when_no_commit() {
        let payload = delete_by_root_payload("abc123", None);
        let delete = &payload["delete"];
        assert_eq!(delete["query"], "_root_:abc123");
        assert!(delete.get("commitWithin").is_none());
    }

    #[test]
    fn drops_info_and_debug_banner_lines() {
        let stderr = "I, [2026-07-07T03:16:33 #15]  INFO -- : traject (3.8.3) executing\n\
                      D, [2026-07-07T03:16:33 #15] DEBUG -- : loading config\n\
                      /usr/local/lib/ruby/gems/foo.rb:1:in 'x': uninitialized constant Foo (NameError)\n\
                      \tfrom /usr/local/bundle/bin/traject:25:in '<main>'";
        let filtered = strip_ruby_logger_noise(stderr);
        assert!(filtered.starts_with("/usr/local/lib/ruby/gems/foo.rb"));
        assert!(filtered.contains("NameError"));
        assert!(!filtered.contains("INFO"));
    }

    #[test]
    fn keeps_warning_and_error_logger_lines() {
        let stderr = "I, [2026-07-07T03:16:33 #15]  INFO -- : executing\n\
                      E, [2026-07-07T03:16:34 #15] ERROR -- : could not index document";
        let filtered = strip_ruby_logger_noise(stderr);
        assert_eq!(
            filtered,
            "E, [2026-07-07T03:16:34 #15] ERROR -- : could not index document"
        );
    }

    #[test]
    fn falls_back_to_original_when_everything_is_noise() {
        let stderr = "I, [2026-07-07T03:16:33 #15]  INFO -- : only info here";
        assert_eq!(strip_ruby_logger_noise(stderr), stderr);
    }
}
