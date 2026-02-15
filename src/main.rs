use std::{
    path::{self, PathBuf},
    process::Command,
};

use clap::{Args, Parser, Subcommand};
use harvester::{
    ArcLightArgs, ArcLightIndexer, ArcLightIndexerConfig, ArcLightIndexerConfigInput,
    ArcLightRunOptions, Harvester, HarvesterArgs, OaiConfig, db, expand_path,
};

/// OAI-PMH harvester
#[derive(Debug, Parser)]
#[command(name = "harvester")]
#[command(about = "OAI-PMH harvester", long_about = None)]
struct Cli {
    /// Database connection URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[command(subcommand)]
    command: Commands,
}

// TODO: identify etc.
#[derive(Debug, Subcommand)]
enum Commands {
    #[command(arg_required_else_help = true)]
    Harvest(HarvesterArgs),

    /// Index records into a target system
    #[command(subcommand)]
    Index(IndexCommands),
}

#[derive(Debug, Subcommand)]
enum IndexCommands {
    /// ArcLight index operations
    #[command(name = "arclight", subcommand)]
    ArcLight(ArcLightCommands),
}

#[derive(Debug, Subcommand)]
enum ArcLightCommands {
    /// Index ready records (pending index state)
    #[command(name = "run", arg_required_else_help = true)]
    Run(ArcLightArgs),

    /// Retry only failed index/purge records
    #[command(name = "retry", arg_required_else_help = true)]
    Retry(ArcLightRetryArgs),

    /// Requeue all parsed/deleted records for this OAI repository
    #[command(name = "reindex", arg_required_else_help = true)]
    Reindex(ArcLightReindexArgs),
}

#[derive(Debug, Args)]
struct ArcLightRetryArgs {
    #[command(flatten)]
    pub arclight: ArcLightArgs,

    /// Optional substring filter on failed index message
    #[arg(long)]
    pub message_filter: Option<String>,

    /// Skip failed records at/above this attempt count
    #[arg(long)]
    pub max_attempts: Option<i32>,
}

#[derive(Debug, Args)]
struct ArcLightReindexArgs {
    /// Source OAI endpoint url
    pub oai_endpoint: String,

    /// Source OAI repository name
    pub oai_repository: String,

    /// OAI metadata prefix
    #[arg(short, long, default_value = "oai_ead")]
    pub metadata_prefix: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env first, then .env.local can override
    let _ = dotenvy::from_filename_override(".env");
    let _ = dotenvy::from_filename_override(".env.local");

    let args = Cli::parse();
    let pool = db::create_pool(&args.database_url).await?;

    match args.command {
        Commands::Harvest(cfg) => {
            println!("Harvesting records from {}", cfg.endpoint);

            let data_dir = path::absolute(expand_path(&cfg.dir))?;

            let config = OaiConfig {
                data_dir,
                endpoint: cfg.endpoint,
                metadata_prefix: cfg.metadata_prefix,
            };
            let harvester = Harvester::new(config, pool);
            harvester.run(cfg.rules.map(|p| expand_path(&p))).await?;
        }
        Commands::Index(IndexCommands::ArcLight(command)) => match command {
            ArcLightCommands::Run(cfg) => {
                println!("Indexing records into {}", cfg.repository);
                let config = build_arclight_config(cfg, ArcLightRunOptions::pending_only())?;
                let indexer = ArcLightIndexer::new(config, pool.clone());

                indexer.run().await?;
            }
            ArcLightCommands::Retry(cfg) => {
                println!(
                    "Retrying failed index records into {}",
                    cfg.arclight.repository
                );
                let config = build_arclight_config(
                    cfg.arclight,
                    ArcLightRunOptions::failed_only(cfg.message_filter, cfg.max_attempts),
                )?;
                let indexer = ArcLightIndexer::new(config, pool.clone());

                indexer.run().await?;
            }
            ArcLightCommands::Reindex(cfg) => {
                let params = db::ReindexStateParams {
                    endpoint: &cfg.oai_endpoint,
                    metadata_prefix: &cfg.metadata_prefix,
                    oai_repository: &cfg.oai_repository,
                };
                let result = db::do_reindex_state_query(&pool, params).await?;
                println!(
                    "Requeued {} record(s) to pending index status",
                    result.rows_affected()
                );
            }
        },
    }

    Ok(())
}

fn ensure_traject_available() -> anyhow::Result<()> {
    let status = Command::new("traject").args(["--version"]).status()?;

    if !status.success() {
        anyhow::bail!("traject failed with exit code: {:?}", status.code());
    }

    Ok(())
}

fn resolve_arclight_paths(cfg: &ArcLightArgs) -> anyhow::Result<(PathBuf, PathBuf, PathBuf)> {
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

fn build_arclight_config(
    cfg: ArcLightArgs,
    run_options: ArcLightRunOptions,
) -> anyhow::Result<ArcLightIndexerConfig> {
    ensure_traject_available()?;
    let (configuration, data_dir, repository_file) = resolve_arclight_paths(&cfg)?;

    Ok(ArcLightIndexerConfig::new(ArcLightIndexerConfigInput {
        configuration,
        dir: data_dir,
        repository: cfg.repository,
        oai_endpoint: cfg.oai_endpoint,
        oai_repository: cfg.oai_repository,
        preview: cfg.preview,
        repository_file,
        record_timeout_seconds: cfg.record_timeout_seconds,
        solr_url: cfg.solr_url,
        solr_commit_within_ms: cfg.solr_commit_within_ms,
        run_options,
    }))
}
