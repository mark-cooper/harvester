use clap::Args;
use sqlx::{Pool, Postgres};
use tracing::{info, warn};

use crate::{db, oai::OaiScope};

#[derive(Debug, Args)]
pub struct ReportArgs {
    /// OAI endpoint url
    pub endpoint: String,

    /// OAI metadata prefix
    #[arg(short, long, env = "METADATA_PREFIX")]
    pub metadata_prefix: String,

    /// Also report records absent from the OAI feed for this many days
    #[arg(long)]
    pub not_seen_days: Option<i64>,
}

/// Print health reports for a scope: records serving stale content from the
/// index (indexed but harvest failed), and optionally records that have
/// dropped out of the OAI feed without a delete notice.
pub async fn report(cfg: ReportArgs, pool: Pool<Postgres>) -> anyhow::Result<()> {
    let scope = OaiScope::new(cfg.endpoint, cfg.metadata_prefix);

    let stale = db::report::stale_in_index(&pool, &scope).await?;
    if stale.is_empty() {
        info!("Stale in index (indexed, but latest harvest failed): none");
    } else {
        warn!(
            "Stale in index (indexed, but latest harvest failed): {}",
            stale.len()
        );
        for identifier in &stale {
            warn!("  {identifier}");
        }
    }

    if let Some(days) = cfg.not_seen_days {
        let missing = db::report::not_seen_since(&pool, &scope, days).await?;
        if missing.is_empty() {
            info!("Not seen in the OAI feed for {days}+ days: none");
        } else {
            warn!("Not seen in the OAI feed for {days}+ days: {}", missing.len());
            for identifier in &missing {
                warn!("  {identifier}");
            }
        }
    }

    Ok(())
}
