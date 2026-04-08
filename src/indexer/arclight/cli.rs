use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
};

use clap::Args;
use sqlx::{Pool, Postgres};
use tracing::info;

use super::{
    ArcLightIndexer,
    config::{ARCLIGHT_METADATA_PREFIX, build_config},
};
use crate::{
    db::{self, indexer::ReindexStateParams},
    indexer::{IndexRunOptions, IndexRunner, IndexRunnerConfig},
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
    #[arg(short, long, default_value = "data", env = "DATA_DIR")]
    pub dir: PathBuf,

    /// Preview mode (show matching records, do not index or delete)
    #[arg(short, long, default_value_t = false)]
    pub preview: bool,

    /// Retry failed indexing attempts
    #[arg(long, default_value_t = false, conflicts_with = "reindex")]
    pub retry: bool,

    /// Optional substring filter on failed index message
    #[arg(long, requires = "retry")]
    pub message_filter: Option<String>,

    /// Skip failed records at/above this attempt count
    #[arg(long, requires = "retry")]
    pub max_attempts: Option<i32>,

    /// Solr url
    #[arg(
        short,
        long,
        default_value = "http://127.0.0.1:8983/solr/arclight",
        env = "SOLR_URL"
    )]
    pub solr_url: String,

    /// Per-record timeout for traject and Solr operations
    #[arg(long, default_value_t = 300)]
    pub record_timeout_seconds: u64,

    /// Solr commit-within window for delete operations (interval commit strategy)
    #[arg(long, default_value_t = 10000)]
    pub solr_commit_within_ms: u64,

    /// Reset index state to pending before running (reindex all parsed/deleted records)
    #[arg(long, default_value_t = false, conflicts_with = "retry")]
    pub reindex: bool,
}

pub async fn index(
    cfg: ArcLightArgs,
    pool: Pool<Postgres>,
    shutdown: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    info!("Indexing records into {}", cfg.repository);

    if cfg.reindex {
        let params = ReindexStateParams {
            endpoint: &cfg.oai_endpoint,
            metadata_prefix: ARCLIGHT_METADATA_PREFIX,
            oai_repository: &cfg.oai_repository,
        };
        let result = db::indexer::reindex(&pool, params).await?;
        info!(
            "Requeued {} record(s) to pending index status",
            result.rows_affected()
        );
    }

    let run_options = if cfg.retry {
        IndexRunOptions::failed_only(cfg.message_filter.clone(), cfg.max_attempts)
    } else {
        IndexRunOptions::pending_only()
    };

    let oai_endpoint = cfg.oai_endpoint.clone();
    let oai_repository = cfg.oai_repository.clone();
    let preview = cfg.preview;

    let config = build_config(cfg)?;
    let indexer = ArcLightIndexer::new(config);

    let runner_config = IndexRunnerConfig {
        endpoint: oai_endpoint,
        metadata_prefix: ARCLIGHT_METADATA_PREFIX.to_string(),
        oai_repository,
        run_options,
        preview,
    };
    let runner = IndexRunner::new(indexer, runner_config, pool, shutdown);
    runner.run().await
}
