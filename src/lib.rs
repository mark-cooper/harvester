pub mod db;
mod harvester;
mod indexer;
use std::path::{Path, PathBuf};

pub use harvester::{
    ArcLightArgs, ArcLightIndexer, ArcLightIndexerConfig, Harvester, HarvesterArgs, OaiConfig,
    OaiRecordId,
};

pub fn expand_path(path: &Path) -> PathBuf {
    PathBuf::from(shellexpand::tilde(&path.to_string_lossy()).as_ref())
}
