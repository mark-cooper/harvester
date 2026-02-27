use std::path::PathBuf;

use clap::Args;

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
