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
                .arg(record.path())
                .output()
                .await?;

            if output.status.success() {
                // TODO: update db "indexed"
                Ok(())
            } else {
                // TODO: update db "failed" with message
                let stderr = String::from_utf8_lossy(&output.stderr);
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
    repository: String,
    oai_endpoint: String,
    oai_repository: String,
    preview: bool,
    solr_url: String,
}

impl ArcLightIndexerConfig {
    // TODO: constructor supports validation later if necessary
    // (for example: parse endpoint/url as uri etc.)
    pub fn new(
        configuration: PathBuf,
        repository: String,
        oai_endpoint: String,
        oai_repository: String,
        preview: bool,
        solr_url: String,
    ) -> Self {
        Self {
            configuration,
            repository,
            oai_endpoint,
            oai_repository,
            preview,
            solr_url,
        }
    }
}
