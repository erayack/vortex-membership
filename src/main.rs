use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;
use vortex_membership::AppError;
use vortex_membership::config::{load_node_config, load_scenario};
use vortex_membership::harness::run_scenario;

#[derive(Debug, Parser)]
#[command(name = "vortex-membership")]
#[command(about = "SWIM-based membership and rendezvous ownership service")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Debug, Subcommand)]
enum Mode {
    /// Run a single membership node.
    Node {
        #[arg(long, default_value = "config/node.toml")]
        config: PathBuf,
    },
    /// Run the local multi-node lab harness.
    Lab {
        #[arg(long, default_value = "config/lab.toml")]
        scenario: PathBuf,
    },
}

fn init_tracing() -> Result<(), AppError> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()
        .map_err(|_| AppError::TracingInit)
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    init_tracing()?;
    let cli = Cli::parse();

    match cli.mode {
        Mode::Node { config } => {
            let app_config = load_node_config(&config)?;
            info!(
                path = %config.display(),
                quarantine_ms = app_config.swim.quarantine_ms,
                "starting node mode bootstrap"
            );
        }
        Mode::Lab { scenario } => {
            let scenario_config = load_scenario(&scenario)?;
            let report = run_scenario(scenario_config.clone()).await?;
            info!(
                path = %scenario.display(),
                quarantine_ms = scenario_config.swim.quarantine_ms,
                detection_p50_ms = report.detection_p50_ms,
                detection_p95_ms = report.detection_p95_ms,
                false_suspicions = report.false_suspicions,
                convergence_ms = report.convergence_ms,
                owner_churn_per_min = report.owner_churn_per_min,
                "completed lab scenario"
            );
        }
    }

    Ok(())
}
