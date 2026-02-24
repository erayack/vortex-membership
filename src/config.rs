use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const DEFAULT_PROBE_INTERVAL_MS: u64 = 1_000;
const DEFAULT_ACK_TIMEOUT_MS: u64 = 300;
const DEFAULT_INDIRECT_PING_COUNT: usize = 3;
const DEFAULT_SUSPECT_TIMEOUT_MS: u64 = 3_000;
const DEFAULT_QUARANTINE_MS: u64 = 60_000;
const DEFAULT_ANTI_ENTROPY_INTERVAL_MS: u64 = 5_000;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub swim: SwimConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ScenarioConfig {
    pub harness: HarnessConfig,
    pub swim: SwimConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct HarnessConfig {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct SwimConfig {
    pub probe_interval_ms: u64,
    pub ack_timeout_ms: u64,
    pub indirect_ping_count: usize,
    pub suspect_timeout_ms: u64,
    pub quarantine_ms: u64,
    pub anti_entropy_interval_ms: u64,
}

impl Default for SwimConfig {
    fn default() -> Self {
        Self {
            probe_interval_ms: DEFAULT_PROBE_INTERVAL_MS,
            ack_timeout_ms: DEFAULT_ACK_TIMEOUT_MS,
            indirect_ping_count: DEFAULT_INDIRECT_PING_COUNT,
            suspect_timeout_ms: DEFAULT_SUSPECT_TIMEOUT_MS,
            quarantine_ms: DEFAULT_QUARANTINE_MS,
            anti_entropy_interval_ms: DEFAULT_ANTI_ENTROPY_INTERVAL_MS,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config at {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse config at {path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid config at {path}: {message}")]
    Invalid { path: PathBuf, message: String },
}

/// Loads node runtime configuration from a TOML file.
///
/// # Errors
///
/// Returns [`ConfigError`] when the file cannot be read, parsed, or validated.
pub fn load_node_config(path: &Path) -> Result<AppConfig, ConfigError> {
    let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;

    let config = toml::from_str::<AppConfig>(&raw).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })?;

    validate_swim(path, &config.swim)?;
    Ok(config)
}

/// Loads lab scenario configuration from a TOML file.
///
/// # Errors
///
/// Returns [`ConfigError`] when the file cannot be read, parsed, or validated.
pub fn load_scenario(path: &Path) -> Result<ScenarioConfig, ConfigError> {
    let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;

    let config = toml::from_str::<ScenarioConfig>(&raw).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })?;

    validate_swim(path, &config.swim)?;
    Ok(config)
}

fn validate_swim(path: &Path, swim: &SwimConfig) -> Result<(), ConfigError> {
    if swim.probe_interval_ms == 0 {
        return Err(invalid(path, "`probe_interval_ms` must be > 0"));
    }
    if swim.ack_timeout_ms == 0 {
        return Err(invalid(path, "`ack_timeout_ms` must be > 0"));
    }
    if swim.indirect_ping_count == 0 {
        return Err(invalid(path, "`indirect_ping_count` must be > 0"));
    }
    if swim.suspect_timeout_ms == 0 {
        return Err(invalid(path, "`suspect_timeout_ms` must be > 0"));
    }
    if swim.quarantine_ms == 0 {
        return Err(invalid(path, "`quarantine_ms` must be > 0"));
    }
    if swim.anti_entropy_interval_ms == 0 {
        return Err(invalid(path, "`anti_entropy_interval_ms` must be > 0"));
    }
    if swim.suspect_timeout_ms < swim.ack_timeout_ms {
        return Err(invalid(
            path,
            "`suspect_timeout_ms` must be >= `ack_timeout_ms`",
        ));
    }

    Ok(())
}

fn invalid(path: &Path, message: &'static str) -> ConfigError {
    ConfigError::Invalid {
        path: path.to_path_buf(),
        message: message.to_owned(),
    }
}
