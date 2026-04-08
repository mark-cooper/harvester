use std::{
    path::{self, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};

use clap::Args;
use sqlx::{Pool, Postgres};
use tracing::info;

use super::{Harvester, perform};
use crate::{
    OaiConfig,
    db::{self, harvester::RetryHarvestParams},
    expand_path,
};

#[derive(Debug, Args)]
pub struct HarvesterArgs {
    /// OAI endpoint url
    pub endpoint: String,

    /// Base directory for downloads
    #[arg(short, long, default_value = "data", env = "DATA_DIR")]
    pub dir: PathBuf,

    /// OAI metadata prefix
    #[arg(short, long, env = "METADATA_PREFIX")]
    pub metadata_prefix: String,

    /// Reset failed records to pending before harvesting
    #[arg(long, default_value_t = false)]
    pub retry: bool,

    /// XML scanning rules file
    #[arg(short, long, env = "RULES_FILE")]
    pub rules: Option<PathBuf>,

    /// Timeout for individual OAI operations (seconds)
    #[arg(long, default_value_t = 120, env = "OAI_TIMEOUT")]
    pub oai_timeout: u64,

    /// Max retries for transient OAI failures (per record)
    #[arg(long, default_value_t = 0, env = "OAI_RETRIES")]
    pub oai_retries: u32,
}

pub async fn harvest(
    cfg: HarvesterArgs,
    pool: Pool<Postgres>,
    shutdown: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    info!("Harvesting records from {}", cfg.endpoint);

    if cfg.retry {
        let params = RetryHarvestParams {
            endpoint: &cfg.endpoint,
            metadata_prefix: &cfg.metadata_prefix,
        };
        let result = db::harvester::retry(&pool, params).await?;
        info!(
            "Reset {} failed record(s) to pending",
            result.rows_affected()
        );
    }

    let data_dir = path::absolute(expand_path(&cfg.dir))?;

    let config = OaiConfig {
        data_dir,
        endpoint: cfg.endpoint,
        metadata_prefix: cfg.metadata_prefix,
        oai_timeout: cfg.oai_timeout,
        oai_retries: cfg.oai_retries,
    };
    let harvester = Harvester::new(config, pool, shutdown.clone());
    perform(&harvester, cfg.rules.map(|p| expand_path(&p))).await
}
