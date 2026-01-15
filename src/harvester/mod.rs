mod arclight;
mod download;
mod import;
mod indexer;
mod metadata;
pub mod oai;
mod rules;

use std::path::PathBuf;

pub use arclight::{ArcLightIndexer, ArcLightIndexerConfig};
pub use oai::{OaiConfig, OaiRecordId};

use sqlx::PgPool;

pub struct Harvester {
    config: OaiConfig,
    pool: PgPool,
}

impl Harvester {
    pub fn new(config: OaiConfig, pool: PgPool) -> Self {
        Self { config, pool }
    }

    pub async fn download(&self) -> anyhow::Result<()> {
        download::run(&self).await
    }

    pub async fn import(&self) -> anyhow::Result<()> {
        import::run(&self).await
    }

    pub async fn metadata(&self, rules: PathBuf) -> anyhow::Result<()> {
        metadata::run(&self, rules).await
    }
}

/// Get head & tail of string for debugging shelled-out cmds
fn truncate_middle(s: &str, head: usize, tail: usize) -> String {
    let total = s.chars().count();
    if total <= head + tail {
        return s.to_string();
    }

    let head_end = s.char_indices().nth(head).unwrap().0;
    let tail_start = s.char_indices().nth(total - tail).unwrap().0;

    format!(
        "{}\n... <{} chars omitted> ...\n{}",
        &s[..head_end],
        total - head - tail,
        &s[tail_start..],
    )
}
