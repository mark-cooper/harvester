use std::{
    path::{self, PathBuf},
    process::Command,
};

use clap::{Args, Parser, Subcommand, command};
use harvester::{ArcLightIndexer, ArcLightIndexerConfig, Harvester, OaiConfig, db};

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

#[derive(Debug, Args)]
struct HarvesterArgs {
    /// OAI endpoint url
    endpoint: String,

    /// Base directory for downloads
    #[arg(short, long, default_value = "data")]
    dir: PathBuf,

    /// OAI metadata prefix
    #[arg(short, long)]
    metadata_prefix: String,

    /// XML scanning rules file
    #[arg(short, long)]
    rules: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ArcLightArgs {
    /// Target repository id
    repository: String,

    /// Source OAI endpoint url
    oai_endpoint: String,

    /// Source OAI repository name
    oai_repository: String,

    /// Traject configuration file path
    #[arg(short, long, default_value = "traject/ead2_config.rb")]
    configuration: PathBuf,

    /// EAD base directory
    #[arg(short, long, default_value = "data")]
    dir: PathBuf,

    /// Preview mode (show matching records, do not index or delete)
    #[arg(short, long, default_value_t = false)]
    preview: bool,

    /// Repositories yaml file
    #[arg(short, long, default_value = "config/repositories.yml")]
    repository_file: PathBuf,

    /// Solr url
    #[arg(short, long, default_value = "http://127.0.0.1:8983/solr/arclight")]
    solr_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env first, then .env.local can override
    let _ = dotenvy::from_filename(".env");
    let _ = dotenvy::from_filename(".env.local");

    let args = Cli::parse();
    let pool = db::create_pool(&args.database_url).await?;

    match args.command {
        Commands::Harvest(cfg) => {
            println!("Harvesting records from {}", cfg.endpoint);

            let data_dir = path::absolute(cfg.dir)?;

            let config = OaiConfig {
                data_dir,
                endpoint: cfg.endpoint,
                metadata_prefix: cfg.metadata_prefix,
            };
            let harvester = Harvester::new(config, pool);

            harvester.import().await?;
            harvester.download().await?;

            if let Some(rules) = cfg.rules {
                let rules = path::absolute(rules)?;
                if !rules.is_file() {
                    anyhow::bail!("rules file was not found");
                }
                harvester.metadata(rules).await?;
            }

            // harvester.summarize().await?;
        }
        Commands::Index(IndexCommands::ArcLight(cfg)) => {
            let status = Command::new("traject").args(["--version"]).status()?;

            if !status.success() {
                anyhow::bail!("traject failed with exit code: {:?}", status.code());
            }

            let configuration = path::absolute(cfg.configuration)?;
            let data_dir = path::absolute(cfg.dir)?;
            let repository_file = path::absolute(cfg.repository_file)?;

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
                cfg.solr_url,
            );
            let indexer = ArcLightIndexer::new(config, pool);

            indexer.run().await?;
        }
    }

    Ok(())
}
