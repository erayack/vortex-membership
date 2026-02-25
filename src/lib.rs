pub mod anti_entropy;
pub mod config;
pub mod dissemination;
pub mod failure_detector;
pub mod harness;
pub mod node;
pub mod ownership;
pub mod protocol;
pub mod report;
pub mod state;
pub mod transport;
pub mod types;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("failed to initialize tracing")]
    TracingInit,
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("invalid lab output configuration: {message}")]
    InvalidLabOutput { message: String },
    #[error("node runtime error: {0}")]
    Node(#[from] node::NodeError),
    #[error("harness error: {0}")]
    Harness(#[from] harness::HarnessError),
    #[error("report error: {0}")]
    Report(#[from] report::ReportError),
}
