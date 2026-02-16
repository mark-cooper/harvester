use std::path::{self, PathBuf};

use crate::{expand_path, indexer::ensure_traject_available};

use super::cli::ArcLightArgs;

pub const ARCLIGHT_METADATA_PREFIX: &str = "oai_ead";

#[derive(Debug, Clone)]
pub struct ArcLightIndexerConfig {
    pub(super) configuration: PathBuf,
    pub(super) dir: PathBuf,
    pub(super) repository: String,
    pub(super) repository_file: PathBuf,
    pub(super) record_timeout_seconds: u64,
    pub(super) solr_url: String,
    pub(super) solr_commit_within_ms: u64,
}

pub struct ArcLightIndexerConfigInput {
    pub configuration: PathBuf,
    pub dir: PathBuf,
    pub repository: String,
    pub repository_file: PathBuf,
    pub record_timeout_seconds: u64,
    pub solr_url: String,
    pub solr_commit_within_ms: u64,
}

impl ArcLightIndexerConfig {
    pub fn new(input: ArcLightIndexerConfigInput) -> Self {
        Self {
            configuration: input.configuration,
            dir: input.dir,
            repository: input.repository,
            repository_file: input.repository_file,
            record_timeout_seconds: input.record_timeout_seconds,
            solr_url: input.solr_url,
            solr_commit_within_ms: input.solr_commit_within_ms,
        }
    }
}

pub fn build_config(cfg: ArcLightArgs) -> anyhow::Result<ArcLightIndexerConfig> {
    ensure_traject_available()?;
    let (configuration, data_dir, repository_file) = resolve_paths(&cfg)?;

    Ok(ArcLightIndexerConfig::new(ArcLightIndexerConfigInput {
        configuration,
        dir: data_dir,
        repository: cfg.repository,
        repository_file,
        record_timeout_seconds: cfg.record_timeout_seconds,
        solr_url: cfg.solr_url,
        solr_commit_within_ms: cfg.solr_commit_within_ms,
    }))
}

fn resolve_paths(cfg: &ArcLightArgs) -> anyhow::Result<(PathBuf, PathBuf, PathBuf)> {
    let configuration = path::absolute(expand_path(&cfg.configuration))?;
    let data_dir = path::absolute(expand_path(&cfg.dir))?;
    let repository_file = path::absolute(expand_path(&cfg.repository_file))?;

    if !configuration.is_file() {
        anyhow::bail!("traject configuration was not found");
    }

    if !data_dir.is_dir() {
        anyhow::bail!("base directory was not found");
    }

    if !repository_file.is_file() {
        anyhow::bail!("repositories configuration was not found");
    }

    Ok((configuration, data_dir, repository_file))
}
