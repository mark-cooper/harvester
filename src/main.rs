use std::env;

use clap::{Args, Parser, Subcommand, command};
use harvester::{Harvester, OaiConfig, db};

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
}

#[derive(Debug, Args)]
struct HarvesterArgs {
    /// OAI endpoint url
    endpoint: String,

    /// OAI metadata prefix
    #[arg(short, long)]
    metadata_prefix: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env first, then .env.local can override
    let _ = dotenvy::from_filename(".env");
    let _ = dotenvy::from_filename(".env.local");

    let args = Cli::parse();
    let cwd = env::current_dir()?; // TODO: support path arg?
    let pool = db::create_pool(&args.database_url).await?;

    match args.command {
        Commands::Harvest(cfg) => {
            println!("Harvesting records from {}", cfg.endpoint);
            let config = OaiConfig {
                data_dir: cwd.join("data"),
                endpoint: cfg.endpoint,
                metadata_prefix: cfg.metadata_prefix,
            };
            let harvester = Harvester::new(config, pool);

            harvester.import().await?;
            harvester.download().await?;
            // harvester.metadata(rules).await?;
            // harvester.summarize().await?;
        }
    }

    Ok(())
}
