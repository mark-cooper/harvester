pub mod db;
mod harvester;
mod indexer;
use std::path::{Path, PathBuf};

pub use harvester::{Harvester, HarvesterArgs, OaiConfig, OaiRecordId};
pub use indexer::IndexRunOptions;
pub use indexer::arclight::ArcLightIndexer;
pub use indexer::arclight::cli::{ArcLightArgs, ArcLightReindexArgs, ArcLightRetryArgs};
pub use indexer::arclight::config::{
    ArcLightIndexerConfig, ArcLightIndexerConfigInput, build_config as build_arclight_config,
};

pub fn expand_path(path: &Path) -> PathBuf {
    PathBuf::from(shellexpand::tilde(&path.to_string_lossy()).as_ref())
}
