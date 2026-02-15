pub mod db;
mod harvester;
mod indexer;
use std::path::{Path, PathBuf};

pub use harvester::{Harvester, HarvesterArgs, OaiConfig, OaiRecordId};
pub use indexer::IndexRunOptions;
pub use indexer::arclight::{
    ArcLightArgs, ArcLightIndexer, ArcLightIndexerConfig, ArcLightIndexerConfigInput,
    ArcLightReindexArgs, ArcLightRetryArgs,
};

pub fn expand_path(path: &Path) -> PathBuf {
    PathBuf::from(shellexpand::tilde(&path.to_string_lossy()).as_ref())
}
