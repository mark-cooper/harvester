use std::path::PathBuf;

use clap::Args;

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

    /// Solr url
    #[arg(short, long, default_value = "http://127.0.0.1:8983/solr/arclight")]
    pub solr_url: String,

    /// Per-record timeout for traject and Solr operations
    #[arg(long, default_value_t = 300)]
    pub record_timeout_seconds: u64,

    /// Solr commit-within window for delete operations (interval commit strategy)
    #[arg(long, default_value_t = 10000)]
    pub solr_commit_within_ms: u64,

    /// Reset index state to pending before running (reindex all parsed/deleted records)
    #[arg(long, default_value_t = false)]
    pub reindex: bool,
}

#[derive(Debug, Args)]
pub struct ArcLightRetryArgs {
    #[command(flatten)]
    pub arclight: ArcLightArgs,

    /// Optional substring filter on failed index message
    #[arg(long)]
    pub message_filter: Option<String>,

    /// Skip failed records at/above this attempt count
    #[arg(long)]
    pub max_attempts: Option<i32>,
}
