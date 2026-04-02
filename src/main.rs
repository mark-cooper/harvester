use std::path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use clap::{Parser, Subcommand};
use harvester::{
    ARCLIGHT_METADATA_PREFIX, ArcLightArgs, ArcLightIndexer, Harvester, HarvesterArgs,
    IndexRunOptions, IndexerContext, OaiConfig, build_arclight_config,
    db::{self, ReindexStateParams, RetryHarvestParams},
    expand_path, run_indexer,
};
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
            info!("Harvesting records from {}", cfg.endpoint);

            if cfg.retry {
                let params = RetryHarvestParams {
                    endpoint: &cfg.endpoint,
                    metadata_prefix: &cfg.metadata_prefix,
                };
                let result = db::apply_retry(&pool, params).await?;
                info!(
                    "Reset {} failed record(s) to pending",
                    result.rows_affected()
                );
            }

            let data_dir = path::absolute(expand_path(&cfg.dir))?;

            let config = OaiConfig {
                data_dir,
                endpoint: cfg.endpoint,
                metadata_prefix: cfg.metadata_prefix,
                oai_timeout: cfg.oai_timeout,
                oai_retries: cfg.oai_retries,
            };
            let harvester = Harvester::new(config, pool, shutdown.clone());
            harvester.run(cfg.rules.map(|p| expand_path(&p))).await?;
        }
        Commands::Index(IndexCommands::ArcLight(cfg)) => {
            if cfg.reindex {
                let params = ReindexStateParams {
                    endpoint: &cfg.oai_endpoint,
                    metadata_prefix: ARCLIGHT_METADATA_PREFIX,
                    oai_repository: &cfg.oai_repository,
                };

                let result = db::apply_reindex(&pool, params).await?;
                info!(
                    "Requeued {} record(s) to pending index status",
                    result.rows_affected()
                );
            }

            let run_option = if cfg.retry {
                IndexRunOptions::failed_only(cfg.message_filter.clone(), cfg.max_attempts)
            } else {
                IndexRunOptions::pending_only()
            };

            info!("Indexing records into {}", cfg.repository);

            let ctx = IndexerContext::new(
                pool,
                cfg.oai_endpoint.clone(),
                ARCLIGHT_METADATA_PREFIX.to_string(),
                cfg.oai_repository.clone(),
                run_option,
                cfg.preview,
                shutdown.clone(),
            );

            let config = build_arclight_config(cfg)?;
            let indexer = ArcLightIndexer::new(config);

            run_indexer(&ctx, &indexer).await?;
        }
    }

    Ok(())
}
