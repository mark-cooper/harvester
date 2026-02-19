pub mod db;
mod harvester;
mod indexer;
pub mod oai;
mod summarizer;
use std::path::{Path, PathBuf};

pub use harvester::{Harvester, HarvesterArgs};
pub use indexer::arclight::ArcLightIndexer;
pub use indexer::arclight::cli::{ArcLightArgs, ArcLightRetryArgs};
pub use indexer::arclight::config::{
    ARCLIGHT_METADATA_PREFIX, ArcLightIndexerConfig, build_config as build_arclight_config,
};
pub use indexer::{IndexRunOptions, IndexerContext, run as run_indexer};
pub use oai::{OaiConfig, OaiRecordId};

pub fn expand_path(path: &Path) -> PathBuf {
    PathBuf::from(shellexpand::tilde(&path.to_string_lossy()).as_ref())
}
