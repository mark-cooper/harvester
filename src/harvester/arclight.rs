use std::path::PathBuf;

use futures::future::BoxFuture;
use sqlx::PgPool;
use tokio::process::Command;

use crate::harvester::{
    indexer::{self, Indexer},
    oai::OaiRecordId,
};

pub struct ArcLightIndexer {
    config: ArcLightIndexerConfig,
    pool: PgPool,
}

impl ArcLightIndexer {
    pub fn new(config: ArcLightIndexerConfig, pool: PgPool) -> Self {
        Self { config, pool }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        indexer::run(self).await
    }
}

impl Indexer for ArcLightIndexer {
    fn fetch_records<'a>(
        &'a self,
        status: &'a str,
        last_identifier: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>> {
        Box::pin(async move {
            Ok(match last_identifier {
                Some(last_id) => {
                    sqlx::query_as!(
                        OaiRecordId,
                        r#"
                        SELECT identifier, fingerprint AS "fingerprint!"
                        FROM oai_records
                        WHERE endpoint = $1
                          AND metadata_prefix = 'oai_ead'
                          AND metadata->'repository' ? $2
                          AND identifier > $3
                          AND status = $4
                        ORDER BY identifier
                        LIMIT 100
                        "#,
                        &self.config.oai_endpoint,
                        &self.config.oai_repository,
                        last_id,
                        status
                    )
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as!(
                        OaiRecordId,
                        r#"
                        SELECT identifier, fingerprint AS "fingerprint!"
                        FROM oai_records
                        WHERE endpoint = $1
                          AND metadata_prefix = 'oai_ead'
                          AND metadata->'repository' ? $2
                          AND status = $3
                        ORDER BY identifier
                        LIMIT 100
                        "#,
                        &self.config.oai_endpoint,
                        &self.config.oai_repository,
                        status
                    )
                    .fetch_all(&self.pool)
                    .await?
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

            let path = self.config.dir.join(record.path());

            let output = Command::new("traject")
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
                .output()
                .await?;

            // TODO: status updates need to be moved out
            // see also: download:update_record_available etc.
            // the in common part: endpoint, identifier, status + message for fail
            if output.status.success() {
                sqlx::query!(
                    r#"
                    UPDATE oai_records
                    SET status = 'indexed', last_checked_at = NOW()
                    WHERE endpoint = $1 AND metadata_prefix = 'oai_ead' AND identifier = $2
                    "#,
                    &self.config.oai_endpoint,
                    record.identifier
                )
                .execute(&self.pool)
                .await?;
                Ok(())
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                sqlx::query!(
                    r#"
                    UPDATE oai_records
                    SET status = 'failed', message = $3, last_checked_at = NOW()
                    WHERE endpoint = $1 AND metadata_prefix = 'oai_ead' AND identifier = $2
                    "#,
                    &self.config.oai_endpoint,
                    record.identifier,
                    &stderr
                )
                .execute(&self.pool)
                .await?;
                anyhow::bail!("traject failed: {}", stderr)
            }
        })
    }

    fn delete_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            // TODO: implement Solr delete
            let _ = record;
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
    solr_url: String,
}

impl ArcLightIndexerConfig {
    // TODO: constructor supports validation later if necessary
    // (for example: parse endpoint/url as uri etc.)
    pub fn new(
        configuration: PathBuf,
        dir: PathBuf,
        repository: String,
        oai_endpoint: String,
        oai_repository: String,
        preview: bool,
        repository_file: PathBuf,
        solr_url: String,
    ) -> Self {
        Self {
            configuration,
            dir,
            repository,
            oai_endpoint,
            oai_repository,
            preview,
            repository_file,
            solr_url,
        }
    }
}
