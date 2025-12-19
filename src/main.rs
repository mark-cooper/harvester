use std::error::Error;

use clap::{Args, Parser, Subcommand, command};
use harvester::{Harvester, OaiConfig};

/// OAI-PMH harvester
#[derive(Debug, Parser)]
#[command(name = "harvester")]
#[command(about = "OAI-PMH harvester", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(arg_required_else_help = true)]
    Harvest(HarvesterArgs),
}

#[derive(Debug, Args)]
struct HarvesterArgs {
    endpoint: String,
    #[arg(short, long)]
    metadata_prefix: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::parse();
    // TODO: init db connection pool

    match args.command {
        Commands::Harvest(cfg) => {
            println!("Downloading records from {}", cfg.endpoint);
            let config = OaiConfig {
                endpoint: cfg.endpoint,
                metadata_prefix: cfg.metadata_prefix,
            };
            let harvester = Harvester::new(config);
            harvester.import()?;
            harvester.download()?;
            // harvester.characterize(rules)?;
        }
    }

    Ok(())
}
