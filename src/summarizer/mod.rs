#![allow(dead_code)] // While WIP

use std::path::PathBuf;

use clap::Args;
use sqlx::PgPool;

mod gemini;

#[derive(Debug, Args)]
pub struct SummarizerArgs {
    /// OAI endpoint url
    pub endpoint: String,

    /// OAI metadata prefix
    #[arg(short, long)]
    pub metadata_prefix: String,

    /// OAI identifier
    #[arg(short, long)]
    pub identifier: Option<String>,

    /// Base directory for downloads
    #[arg(short, long, default_value = "data")]
    pub dir: PathBuf,

    /// Max number of concurrent summarizer requests
    #[arg(short, long, default_value_t = 1)]
    pub concurrency: u8,

    /// Per-record timeout for summarizer api calls
    #[arg(short, long, default_value_t = 60)]
    pub timeout_seconds: u64,
}

pub struct SummarizerContext {
    pool: PgPool,
    endpoint: String,
    metadata_prefix: String,
    identifier: Option<String>,
    run_options: SummarizerRunOptions,
}

impl SummarizerContext {
    pub fn new(
        pool: PgPool,
        endpoint: String,
        metadata_prefix: String,
        identifier: Option<String>,
        run_options: SummarizerRunOptions,
    ) -> Self {
        Self {
            pool,
            endpoint,
            metadata_prefix,
            identifier,
            run_options,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SummarizerRunOptions {
    pub(crate) concurrency: u8,
    pub(crate) timeout_seconds: u64,
}

pub trait Summarizer {
    fn summarize(&self, content: String) -> anyhow::Result<()>;
}

// TODO:
// select {oairecordid} where {endpoint} and {metadata_prefix} and status {parsed}

pub async fn run<T: Summarizer>(_ctx: &SummarizerContext, summarizer: &T) -> anyhow::Result<()> {
    let content = "".to_string();
    summarizer.summarize(content)?;
    Ok(())
}
