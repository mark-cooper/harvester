use std::path::PathBuf;

use clap::Args;
use futures::future::BoxFuture;
use sqlx::PgPool;
use tokio::process::Command;

use crate::{
    OaiRecordId,
    db::{
        FetchIndexCandidatesParams, UpdateIndexFailureParams, UpdateIndexStatusParams,
        do_mark_index_failure_query, do_mark_index_success_query, fetch_records_for_indexing,
        fetch_records_for_purging,
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
}

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
    fn fetch_records_to_index<'a>(
        &'a self,
        last_identifier: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Vec<OaiRecordId>>> {
        Box::pin(async move {
            let params = FetchIndexCandidatesParams {
                endpoint: &self.config.oai_endpoint,
                metadata_prefix: &self.config.metadata_prefix,
                oai_repository: &self.config.oai_repository,
                last_identifier,
            };
            Ok(fetch_records_for_indexing(&self.pool, params).await?)
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
                last_identifier,
            };
            Ok(fetch_records_for_purging(&self.pool, params).await?)
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
                let params = UpdateIndexFailureParams {
                    endpoint: &self.config.oai_endpoint,
                    metadata_prefix: &self.config.metadata_prefix,
                    identifier: &record.identifier,
                    message: &stderr,
                };
                do_mark_index_failure_query(&self.pool, params).await?;
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
