#![allow(dead_code)] // While WIP

use clap::Args;

mod gemini;

#[derive(Debug, Args)]
pub struct SummarizerArgs {
    /// OAI endpoint url
    pub endpoint: String,

    /// OAI metadata prefix
    #[arg(short, long)]
    pub metadata_prefix: String,

    // OAI identifier
    #[arg(short, long)]
    pub identifier: Option<String>,
}

pub struct SummarizerContext {}

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
