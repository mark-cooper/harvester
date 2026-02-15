use std::path;

use clap::{Parser, Subcommand};
use harvester::{
    ArcLightArgs, ArcLightIndexer, ArcLightReindexArgs, ArcLightRetryArgs, Harvester,
    HarvesterArgs, IndexRunOptions, OaiConfig, build_arclight_config, db, expand_path,
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
                let config = build_arclight_config(cfg, IndexRunOptions::pending_only())?;
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
                    IndexRunOptions::failed_only(cfg.message_filter, cfg.max_attempts),
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
