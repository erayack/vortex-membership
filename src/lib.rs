pub mod anti_entropy;
pub mod config;
pub mod dissemination;
pub mod failure_detector;
pub mod node;
pub mod ownership;
pub mod protocol;
pub mod state;
pub mod transport;
pub mod types;
pub mod harness {}
pub mod report {}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("failed to initialize tracing")]
    TracingInit,
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),
}
