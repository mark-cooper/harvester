use std::{
    fs,
    path::{self, PathBuf},
    process::Command,
};

use crate::expand_path;

use super::cli::ArcLightArgs;

pub const ARCLIGHT_METADATA_PREFIX: &str = "oai_ead";

#[derive(Debug, Clone)]
pub struct ArcLightIndexerConfig {
    pub configuration: PathBuf,
    pub dir: PathBuf,
    pub repository: String,
    pub repository_file: PathBuf,
    pub record_timeout_seconds: u64,
    pub solr_url: String,
    pub solr_commit_within_ms: u64,
}

pub fn build_config(cfg: ArcLightArgs) -> anyhow::Result<ArcLightIndexerConfig> {
    ensure_traject_available()?;
    let (configuration, data_dir) = resolve_paths(&cfg)?;
    let repository_file = generate_repository_file(&cfg.repository, &cfg.oai_repository)?;

    Ok(ArcLightIndexerConfig {
        configuration,
        dir: data_dir,
        repository: cfg.repository,
        repository_file,
        record_timeout_seconds: cfg.record_timeout_seconds,
        solr_url: cfg.solr_url,
        solr_commit_within_ms: cfg.solr_commit_within_ms,
    })
}

fn ensure_traject_available() -> anyhow::Result<()> {
    let status = Command::new("traject").args(["--version"]).status()?;

    if !status.success() {
        anyhow::bail!("traject failed with exit code: {:?}", status.code());
    }

    Ok(())
}

fn resolve_paths(cfg: &ArcLightArgs) -> anyhow::Result<(PathBuf, PathBuf)> {
    let configuration = path::absolute(expand_path(&cfg.configuration))?;
    let data_dir = path::absolute(expand_path(&cfg.dir))?;

    if !configuration.is_file() {
        anyhow::bail!("traject configuration was not found");
    }

    if !data_dir.is_dir() {
        anyhow::bail!("base directory was not found");
    }

    Ok((configuration, data_dir))
}

fn generate_repository_file(repository: &str, name: &str) -> anyhow::Result<PathBuf> {
    let path = std::env::temp_dir().join("harvester-repositories.yml");
    let content = format!("{}:\n  name: \"{}\"\n", repository, name);
    fs::write(&path, content)?;
    Ok(path)
}
