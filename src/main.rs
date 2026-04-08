use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use clap::{Parser, Subcommand};
use harvester::{ArcLightArgs, HarvesterArgs, db};
use tracing::info;

/// OAI-PMH harvester
#[derive(Debug, Parser)]
#[command(name = "harvester")]
#[command(about = "OAI-PMH harvester", long_about = None)]
struct Cli {
    /// Database connection URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Maximum database connections
    #[arg(long, default_value_t = 10, env = "DB_MAX_CONNECTIONS")]
    db_max_connections: u32,

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
    #[command(name = "arclight")]
    ArcLight(ArcLightArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env first, then .env.local can override
    let _ = dotenvy::from_filename_override(".env");
    let _ = dotenvy::from_filename_override(".env.local");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Cli::parse();
    let pool = db::create_pool(&args.database_url, args.db_max_connections).await?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("Received interrupt, finishing current batch...");
        shutdown_clone.store(true, Ordering::Relaxed);
    });

    match args.command {
        Commands::Harvest(cfg) => {
            harvester::harvest(cfg, pool, shutdown).await?;
        }
        Commands::Index(IndexCommands::ArcLight(cfg)) => {
            harvester::index(cfg, pool, shutdown).await?;
        }
    }

    Ok(())
}
