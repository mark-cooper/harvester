use std::path::PathBuf;

use futures::future::BoxFuture;
use sqlx::PgPool;
use tokio::process::Command;

use crate::{
    OaiRecordId,
    db::{UpdateStatusParams, do_update_status_query},
    harvester::oai::OaiRecordStatus,
    indexer::{self, Indexer, truncate_middle},
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
                          AND metadata_prefix = $2
                          AND status = $3
                          AND identifier > $4
                          AND metadata->'repository' ? $5
                        ORDER BY identifier
                        LIMIT 100
                        "#,
                        &self.config.oai_endpoint,
                        &self.config.metadata_prefix,
                        status,
                        last_id,
                        &self.config.oai_repository,
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
                          AND metadata_prefix = $2
                          AND status = $3
                          AND metadata->'repository' ? $4
                        ORDER BY identifier
                        LIMIT 100
                        "#,
                        &self.config.oai_endpoint,
                        &self.config.metadata_prefix,
                        status,
                        &self.config.oai_repository,
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

            let mut params = UpdateStatusParams {
                endpoint: &self.config.oai_endpoint,
                metadata_prefix: &self.config.metadata_prefix,
                identifier: &record.identifier,
                status: OaiRecordStatus::Indexed.as_str(),
                message: "",
            };

            if output.status.success() {
                do_update_status_query(&self.pool, params).await?;
                Ok(())
            } else {
                let stderr = truncate_middle(&String::from_utf8_lossy(&output.stderr), 200, 200);
                params.status = OaiRecordStatus::Failed.as_str();
                params.message = &stderr;
                do_update_status_query(&self.pool, params).await?;
                anyhow::bail!("traject failed: {}", stderr);
            }
        })
    }

    fn delete_record<'a>(&'a self, record: &'a OaiRecordId) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            // TODO: implement Solr delete
            // status "purged"
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
    metadata_prefix: String,
}

impl ArcLightIndexerConfig {
    // TODO: constructor supports validation later if necessary
    // (for example: parse endpoint/url as uri etc.)
    #[allow(clippy::too_many_arguments)]
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
            metadata_prefix: "oai_ead".to_string(),
        }
    }
}
