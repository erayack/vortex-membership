use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;
use vortex_membership::AppError;
use vortex_membership::config::{ScenarioConfig, load_node_config, load_scenario};
use vortex_membership::dissemination::Disseminator;
use vortex_membership::failure_detector::FailureDetector;
use vortex_membership::harness::run_scenario;
use vortex_membership::node::NodeRuntime;
use vortex_membership::ownership::OwnershipResolver;
use vortex_membership::report::write_run_report;
use vortex_membership::state::MembershipStore;
use vortex_membership::transport::UdpTransport;

#[derive(Debug, Parser)]
#[command(name = "vortex-membership")]
#[command(about = "SWIM-based membership and rendezvous ownership service")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a single membership node.
    Node {
        #[arg(long)]
        config: std::path::PathBuf,
    },
    /// Run the local multi-node lab harness.
    Lab {
        #[arg(long)]
        scenario: std::path::PathBuf,
        #[arg(long)]
        out: std::path::PathBuf,
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

    match cli.command {
        Command::Node { config } => run_node_mode(&config).await?,
        Command::Lab { scenario, out } => run_lab_mode(&scenario, &out).await?,
    }

    Ok(())
}

async fn run_node_mode(config_path: &std::path::Path) -> Result<(), AppError> {
    let app_config = load_node_config(config_path)?;
    let transport = UdpTransport::bind(app_config.node.bind_addr)
        .await
        .map_err(|error| AppError::Node(vortex_membership::node::NodeError::Transport(error)))?;

    let runtime = NodeRuntime::new(
        app_config.node.node_id.clone(),
        app_config.swim.anti_entropy_interval_ms,
        transport,
        MembershipStore::new(
            app_config.node.node_id.clone(),
            app_config.node.bind_addr,
            app_config.swim.quarantine_ms,
        ),
        FailureDetector::new(
            app_config.node.node_id.clone(),
            app_config.swim.probe_interval_ms,
            app_config.swim.ack_timeout_ms,
            app_config.swim.indirect_ping_count,
            app_config.swim.suspect_timeout_ms,
        ),
        Disseminator::with_retransmit_multiplier(1, app_config.swim.retransmit_multiplier),
        OwnershipResolver::default(),
    );

    info!(
        path = %config_path.display(),
        node_id = %app_config.node.node_id,
        bound_addr = %app_config.node.bind_addr,
        quarantine_ms = app_config.swim.quarantine_ms,
        "starting node mode runtime"
    );

    runtime.run().await?;
    Ok(())
}

async fn run_lab_mode(
    scenario_path: &std::path::Path,
    out_path: &std::path::Path,
) -> Result<(), AppError> {
    let scenario_config = load_scenario(scenario_path)?;
    validate_lab_output_paths(&scenario_config, out_path)?;
    let quarantine_ms = scenario_config.swim.quarantine_ms;
    let report = run_scenario(scenario_config).await?;
    write_run_report(out_path, &report)?;

    info!(
        path = %scenario_path.display(),
        out = %out_path.display(),
        quarantine_ms,
        detection_p50_ms = report.detection_p50_ms,
        detection_p95_ms = report.detection_p95_ms,
        false_suspicions = report.false_suspicions,
        convergence_ms = report.convergence_ms,
        owner_churn_per_min = report.owner_churn_per_min,
        "completed lab scenario"
    );
    Ok(())
}

fn validate_lab_output_paths(
    scenario_config: &ScenarioConfig,
    out_path: &std::path::Path,
) -> Result<(), AppError> {
    let artifact_path = scenario_config
        .harness
        .artifact_path
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("artifacts/latest-run.json"));

    if normalize_for_compare(&artifact_path) == normalize_for_compare(out_path) {
        return Err(AppError::InvalidLabOutput {
            message: format!(
                "`--out` path must differ from harness artifact path: {}",
                out_path.display()
            ),
        });
    }

    Ok(())
}

fn normalize_for_compare(path: &std::path::Path) -> std::path::PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use vortex_membership::AppError;
    use vortex_membership::config::ScenarioConfig;

    use super::{Cli, Command, validate_lab_output_paths};

    #[test]
    fn cli_parses_node_mode_with_required_config() {
        let result = Cli::try_parse_from(["vortex-membership", "node", "--config", "node.toml"]);
        assert!(result.is_ok());
        let cli = match result {
            Ok(cli) => cli,
            Err(error) => panic!("cli should parse node subcommand: {error}"),
        };
        match cli.command {
            Command::Node { config } => assert_eq!(config, std::path::PathBuf::from("node.toml")),
            Command::Lab { .. } => panic!("expected node command"),
        }
    }

    #[test]
    fn cli_parses_lab_mode_with_required_out() {
        let result = Cli::try_parse_from([
            "vortex-membership",
            "lab",
            "--scenario",
            "lab.toml",
            "--out",
            "report.json",
        ]);
        assert!(result.is_ok());
        let cli = match result {
            Ok(cli) => cli,
            Err(error) => panic!("cli should parse lab subcommand: {error}"),
        };
        match cli.command {
            Command::Lab { scenario, out } => {
                assert_eq!(scenario, std::path::PathBuf::from("lab.toml"));
                assert_eq!(out, std::path::PathBuf::from("report.json"));
            }
            Command::Node { .. } => panic!("expected lab command"),
        }
    }

    #[test]
    fn cli_rejects_lab_mode_without_out() {
        let result = Cli::try_parse_from(["vortex-membership", "lab", "--scenario", "lab.toml"]);
        assert!(result.is_err());
    }

    #[test]
    fn validate_lab_output_paths_rejects_conflict_with_artifact_path() {
        let mut scenario = ScenarioConfig::default();
        scenario.harness.artifact_path = Some(std::path::PathBuf::from("report.json"));

        let result = validate_lab_output_paths(&scenario, std::path::Path::new("report.json"));
        assert!(matches!(result, Err(AppError::InvalidLabOutput { .. })));
    }

    #[test]
    fn validate_lab_output_paths_allows_distinct_paths() {
        let mut scenario = ScenarioConfig::default();
        scenario.harness.artifact_path = Some(std::path::PathBuf::from("artifact.json"));

        let result = validate_lab_output_paths(&scenario, std::path::Path::new("report.json"));
        assert!(result.is_ok());
    }
}
