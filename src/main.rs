use std::{
    path::{self},
    process::Command,
};

use clap::{Parser, Subcommand};
use harvester::{
    ArcLightArgs, ArcLightIndexer, ArcLightIndexerConfig, Harvester, HarvesterArgs, OaiConfig, db,
    expand_path,
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
    #[command(name = "arclight", arg_required_else_help = true)]
    ArcLight(ArcLightArgs),
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
        Commands::Index(IndexCommands::ArcLight(cfg)) => {
            let status = Command::new("traject").args(["--version"]).status()?;

            if !status.success() {
                anyhow::bail!("traject failed with exit code: {:?}", status.code());
            }

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

            println!("Indexing records into {}", cfg.repository);
            let config = ArcLightIndexerConfig::new(
                configuration,
                data_dir,
                cfg.repository,
                cfg.oai_endpoint,
                cfg.oai_repository,
                cfg.preview,
                repository_file,
                cfg.record_timeout_seconds,
                cfg.solr_url,
                cfg.solr_commit_within_ms,
            );
            let indexer = ArcLightIndexer::new(config, pool);

            indexer.run().await?;
        }
    }

    Ok(())
}
