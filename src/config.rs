use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

const DEFAULT_PROBE_INTERVAL_MS: u64 = 1_000;
const DEFAULT_ACK_TIMEOUT_MS: u64 = 300;
const DEFAULT_INDIRECT_PING_COUNT: usize = 3;
const DEFAULT_SUSPECT_TIMEOUT_MS: u64 = 3_000;
const DEFAULT_QUARANTINE_MS: u64 = 60_000;
const DEFAULT_ANTI_ENTROPY_INTERVAL_MS: u64 = 5_000;
const DEFAULT_RETRANSMIT_MULTIPLIER: f64 = 4.0;
const DEFAULT_OBSERVER_BUFFER: usize = 1_024;
const DEFAULT_OWNERSHIP_STABILITY_WINDOW_MS: u64 = 3_000;
const DEFAULT_OWNERSHIP_MAX_TRACKED_KEYS: usize = 16_384;
const DEFAULT_OWNERSHIP_FAST_CUTOVER_ON_TERMINAL: bool = true;
const DEFAULT_LIFEGUARD_ENABLED: bool = false;
const DEFAULT_LIFEGUARD_MAX_MULTIPLIER: f64 = 2.0;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub node: NodeConfig,
    pub swim: SwimConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub bind_addr: SocketAddr,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId::from("node-0"),
            bind_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 15_000)),
        }
    }
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
    pub retransmit_multiplier: f64,
    pub observer_buffer: usize,
    pub ownership_stability_window_ms: u64,
    pub ownership_max_tracked_keys: usize,
    pub ownership_fast_cutover_on_terminal: bool,
    pub lifeguard_enabled: bool,
    pub lifeguard_max_multiplier: f64,
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
            retransmit_multiplier: DEFAULT_RETRANSMIT_MULTIPLIER,
            observer_buffer: DEFAULT_OBSERVER_BUFFER,
            ownership_stability_window_ms: DEFAULT_OWNERSHIP_STABILITY_WINDOW_MS,
            ownership_max_tracked_keys: DEFAULT_OWNERSHIP_MAX_TRACKED_KEYS,
            ownership_fast_cutover_on_terminal: DEFAULT_OWNERSHIP_FAST_CUTOVER_ON_TERMINAL,
            lifeguard_enabled: DEFAULT_LIFEGUARD_ENABLED,
            lifeguard_max_multiplier: DEFAULT_LIFEGUARD_MAX_MULTIPLIER,
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

    validate_node(path, &config.node)?;
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

fn validate_node(path: &Path, node: &NodeConfig) -> Result<(), ConfigError> {
    if node.node_id.as_str().trim().is_empty() {
        return Err(invalid(path, "`node.node_id` must not be empty"));
    }
    if node.bind_addr.port() == 0 {
        return Err(invalid(path, "`node.bind_addr` must use a non-zero port"));
    }

    Ok(())
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
    if !swim.retransmit_multiplier.is_finite() || swim.retransmit_multiplier <= 0.0 {
        return Err(invalid(
            path,
            "`retransmit_multiplier` must be a finite number > 0",
        ));
    }
    if swim.observer_buffer == 0 {
        return Err(invalid(path, "`observer_buffer` must be > 0"));
    }
    if swim.ownership_stability_window_ms == 0 {
        return Err(invalid(path, "`ownership_stability_window_ms` must be > 0"));
    }
    if swim.ownership_max_tracked_keys == 0 {
        return Err(invalid(path, "`ownership_max_tracked_keys` must be > 0"));
    }
    if !swim.lifeguard_max_multiplier.is_finite()
        || swim.lifeguard_max_multiplier < 1.0
        || swim.lifeguard_max_multiplier > 10.0
    {
        return Err(invalid(
            path,
            "`lifeguard_max_multiplier` must be a finite number in [1.0, 10.0]",
        ));
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
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        ConfigError, FaultAction, HarnessConfig, ScheduledFault, SwimConfig, load_node_config,
        validate_harness, validate_swim,
    };

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

    #[test]
    fn load_node_config_rejects_blank_node_id() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        let config_path =
            std::env::temp_dir().join(format!("vortex-membership-node-config-{unique}.toml"));

        let payload = r#"
[node]
node_id = "  "
bind_addr = "127.0.0.1:15000"

[swim]
probe_interval_ms = 1000
ack_timeout_ms = 300
indirect_ping_count = 3
suspect_timeout_ms = 3000
quarantine_ms = 60000
anti_entropy_interval_ms = 5000
"#;
        let write_result = fs::write(&config_path, payload);
        assert!(write_result.is_ok());

        assert!(matches!(
            load_node_config(&config_path),
            Err(ConfigError::Invalid { message, .. })
                if message == "`node.node_id` must not be empty"
        ));
    }

    #[test]
    fn validate_swim_rejects_non_positive_retransmit_multiplier() {
        let path = Path::new("scenario.toml");
        let swim = SwimConfig {
            retransmit_multiplier: 0.0,
            ..SwimConfig::default()
        };

        assert!(matches!(
            validate_swim(path, &swim),
            Err(ConfigError::Invalid { message, .. })
                if message == "`retransmit_multiplier` must be a finite number > 0"
        ));
    }

    #[test]
    fn validate_swim_rejects_zero_observer_buffer() {
        let path = Path::new("scenario.toml");
        let swim = SwimConfig {
            observer_buffer: 0,
            ..SwimConfig::default()
        };

        assert!(matches!(
            validate_swim(path, &swim),
            Err(ConfigError::Invalid { message, .. }) if message == "`observer_buffer` must be > 0"
        ));
    }

    #[test]
    fn validate_swim_rejects_zero_ownership_stability_window() {
        let path = Path::new("scenario.toml");
        let swim = SwimConfig {
            ownership_stability_window_ms: 0,
            ..SwimConfig::default()
        };

        assert!(matches!(
            validate_swim(path, &swim),
            Err(ConfigError::Invalid { message, .. })
                if message == "`ownership_stability_window_ms` must be > 0"
        ));
    }

    #[test]
    fn validate_swim_rejects_zero_ownership_max_tracked_keys() {
        let path = Path::new("scenario.toml");
        let swim = SwimConfig {
            ownership_max_tracked_keys: 0,
            ..SwimConfig::default()
        };

        assert!(matches!(
            validate_swim(path, &swim),
            Err(ConfigError::Invalid { message, .. })
                if message == "`ownership_max_tracked_keys` must be > 0"
        ));
    }

    #[test]
    fn validate_swim_rejects_out_of_range_lifeguard_max_multiplier() {
        let path = Path::new("scenario.toml");
        let too_low = SwimConfig {
            lifeguard_max_multiplier: 0.5,
            ..SwimConfig::default()
        };
        let too_high = SwimConfig {
            lifeguard_max_multiplier: 10.1,
            ..SwimConfig::default()
        };
        let non_finite = SwimConfig {
            lifeguard_max_multiplier: f64::INFINITY,
            ..SwimConfig::default()
        };

        for swim in [too_low, too_high, non_finite] {
            assert!(matches!(
                validate_swim(path, &swim),
                Err(ConfigError::Invalid { message, .. })
                    if message == "`lifeguard_max_multiplier` must be a finite number in [1.0, 10.0]"
            ));
        }
    }
}
