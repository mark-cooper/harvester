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
}
