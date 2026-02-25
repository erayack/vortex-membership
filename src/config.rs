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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct HarnessConfig {
    pub node_count: usize,
    pub base_port: u16,
    pub runtime_ms: u64,
    pub key_count: usize,
    pub random_seed: u64,
    pub artifact_path: Option<PathBuf>,
    pub events: Vec<ScheduledFault>,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            node_count: 5,
            base_port: 15_000,
            runtime_ms: 20_000,
            key_count: 128,
            random_seed: 42,
            artifact_path: None,
            events: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduledFault {
    pub at_ms: u64,
    #[serde(flatten)]
    pub action: FaultAction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FaultAction {
    Kill {
        node: usize,
    },
    Delay {
        from: usize,
        to: usize,
        delay_ms: u64,
    },
    Loss {
        from: usize,
        to: usize,
        loss_rate: f64,
    },
    Partition {
        left: Vec<usize>,
        right: Vec<usize>,
    },
    Heal,
}

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
    validate_harness(path, &config.harness)?;
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

fn validate_harness(path: &Path, harness: &HarnessConfig) -> Result<(), ConfigError> {
    if harness.node_count < 2 {
        return Err(invalid(path, "`node_count` must be >= 2"));
    }
    if harness.runtime_ms == 0 {
        return Err(invalid(path, "`runtime_ms` must be > 0"));
    }
    if harness.key_count == 0 {
        return Err(invalid(path, "`key_count` must be > 0"));
    }

    let max_nodes = usize::from(u16::MAX.saturating_sub(harness.base_port)) + 1;
    if harness.node_count > max_nodes {
        return Err(invalid(
            path,
            "`node_count` is too large for `base_port` range",
        ));
    }

    for fault in &harness.events {
        if fault.at_ms > harness.runtime_ms {
            return Err(invalid(path, "fault schedule exceeds `runtime_ms`"));
        }
        match &fault.action {
            FaultAction::Kill { node } => {
                if *node >= harness.node_count {
                    return Err(invalid(path, "kill fault references unknown node"));
                }
            }
            FaultAction::Delay { from, to, .. } => {
                if *from >= harness.node_count || *to >= harness.node_count {
                    return Err(invalid(path, "delay fault references unknown node"));
                }
            }
            FaultAction::Loss {
                from,
                to,
                loss_rate,
            } => {
                if *from >= harness.node_count || *to >= harness.node_count {
                    return Err(invalid(path, "loss fault references unknown node"));
                }
                if !loss_rate.is_finite() || !(0.0..=1.0).contains(loss_rate) {
                    return Err(invalid(path, "`loss_rate` must be in [0.0, 1.0]"));
                }
            }
            FaultAction::Partition { left, right } => {
                let left_ok = left.iter().all(|node| *node < harness.node_count);
                let right_ok = right.iter().all(|node| *node < harness.node_count);
                if !left_ok || !right_ok {
                    return Err(invalid(path, "partition fault references unknown node"));
                }
            }
            FaultAction::Heal => {}
        }
    }

    Ok(())
}

fn invalid(path: &Path, message: &'static str) -> ConfigError {
    ConfigError::Invalid {
        path: path.to_path_buf(),
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{ConfigError, FaultAction, HarnessConfig, ScheduledFault, validate_harness};

    #[test]
    fn validate_harness_enforces_high_port_node_count_boundary() {
        let path = Path::new("scenario.toml");
        let mut harness = HarnessConfig {
            base_port: u16::MAX - 1,
            node_count: 2,
            ..HarnessConfig::default()
        };
        assert!(validate_harness(path, &harness).is_ok());

        harness.node_count = 3;
        assert!(matches!(
            validate_harness(path, &harness),
            Err(ConfigError::Invalid { message, .. })
                if message == "`node_count` is too large for `base_port` range"
        ));

        let overflow_edge = HarnessConfig {
            base_port: u16::MAX,
            node_count: 2,
            ..HarnessConfig::default()
        };
        assert!(matches!(
            validate_harness(path, &overflow_edge),
            Err(ConfigError::Invalid { message, .. })
                if message == "`node_count` is too large for `base_port` range"
        ));
    }

    #[test]
    fn validate_harness_rejects_unknown_fault_node() {
        let harness = HarnessConfig {
            node_count: 2,
            events: vec![ScheduledFault {
                at_ms: 1,
                action: FaultAction::Delay {
                    from: 2,
                    to: 1,
                    delay_ms: 5,
                },
            }],
            ..HarnessConfig::default()
        };

        assert!(validate_harness(Path::new("scenario.toml"), &harness).is_err());
    }
}
