use std::{env, fs::exists, path::PathBuf, process::Command};

use clap::{Args, Parser, Subcommand, command};
use harvester::{Harvester, Indexer, IndexerConfig, OaiConfig, db};

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

    #[command(arg_required_else_help = true)]
    Index(IndexerArgs),
}

#[derive(Debug, Args)]
struct HarvesterArgs {
    /// OAI endpoint url
    endpoint: String,

    /// OAI metadata prefix
    #[arg(short, long)]
    metadata_prefix: String,

    /// XML scanning rules file
    #[arg(short, long)]
    rules: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct IndexerArgs {
    /// Target repository id
    repository: String,

    /// Source OAI endpoint url
    oai_endpoint: String,

    /// Source OAI repository name
    oai_repository: String,

    /// Traject configuration file path
    #[arg(short, long, default_value = "traject/ead2_config.rb")]
    configuration: PathBuf,

    /// Solr url
    #[arg(short, long, default_value = "http://127.0.0.1/solr/arclight")]
    solr_url: String,

    /// Preview mode (show matching records, do not index)
    #[arg(short, long, default_value_t = false)]
    preview: bool,
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

            if let Some(rules) = cfg.rules {
                rules.try_exists().expect("rules file not found");
                harvester.metadata(rules).await?;
            }

            // harvester.summarize().await?;
        }
        Commands::Index(cfg) => {
            let status = Command::new("traject").args(["--version"]).status()?;

            if !status.success() {
                anyhow::bail!("traject failed with exit code: {:?}", status.code());
            }

            if !exists(cfg.configuration)? {
                anyhow::bail!("traject configuration was not found");
            }

            println!("Indexing records into {}", cfg.repository);
            let config = IndexerConfig::new(
                cfg.repository,
                cfg.oai_endpoint,
                cfg.oai_repository,
                cfg.solr_url,
            );
            let indexer = Indexer::new(config, pool);

            indexer.run().await?;
            todo!()
        }
    }

    Ok(())
}
