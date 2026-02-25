use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::ScenarioConfig;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunReport {
    pub detection_p50_ms: u64,
    pub detection_p95_ms: u64,
    pub false_suspicions: u64,
    pub convergence_ms: u64,
    pub owner_churn_per_min: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunArtifact {
    pub scenario: ScenarioConfig,
    pub report: RunReport,
    pub false_suspicion_rate: f64,
    pub detection_samples_ms: Vec<u64>,
}

impl RunArtifact {
    #[must_use]
    pub fn artifact_path_or_default(&self, configured: Option<PathBuf>) -> PathBuf {
        configured.unwrap_or_else(|| PathBuf::from("artifacts/latest-run.json"))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReportError {
    #[error("failed to create output parent directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to serialize JSON output: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("failed to write JSON output at {path}: {source}")]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
}

/// Writes a pretty-printed run artifact JSON file to disk.
///
/// # Errors
///
/// Returns [`ReportError`] if parent directory creation, serialization, or file
/// writing fails.
pub fn write_run_artifact(path: &Path, artifact: &RunArtifact) -> Result<(), ReportError> {
    write_json_file(path, artifact)
}

/// Writes a pretty-printed run report JSON file to disk.
///
/// # Errors
///
/// Returns [`ReportError`] if parent directory creation, serialization, or file
/// writing fails.
pub fn write_run_report(path: &Path, report: &RunReport) -> Result<(), ReportError> {
    write_json_file(path, report)
}

fn write_json_file<T: Serialize>(path: &Path, payload: &T) -> Result<(), ReportError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ReportError::CreateDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let serialized = serde_json::to_string_pretty(payload)?;
    fs::write(path, serialized).map_err(|source| ReportError::Write {
        path: path.to_path_buf(),
        source,
    })
}

#[must_use]
pub fn percentile(samples: &[u64], pct: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();

    let bounded = pct.clamp(0.0, 1.0);
    let last_idx = sorted.len() - 1;
    let denominator = u32::try_from(last_idx)
        .map_or(u32::MAX, |value| value)
        .max(1);

    for (idx, _) in sorted.iter().enumerate().take(last_idx + 1) {
        let numerator = u32::try_from(idx).map_or(u32::MAX, |value| value);
        if f64::from(numerator) / f64::from(denominator) >= bounded {
            return sorted[idx];
        }
    }

    sorted[last_idx]
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{RunReport, percentile, write_run_report};

    #[test]
    fn percentile_returns_zero_for_empty() {
        assert_eq!(percentile(&[], 0.5), 0);
    }

    #[test]
    fn percentile_selects_rounded_rank() {
        let samples = vec![100, 20, 40, 60, 80];
        assert_eq!(percentile(&samples, 0.5), 60);
        assert_eq!(percentile(&samples, 0.95), 100);
    }

    #[test]
    fn write_run_report_writes_pretty_json_and_creates_parent_dirs() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        let root = std::env::temp_dir().join(format!("vortex-membership-report-{unique}"));
        let out = root.join("nested").join("run-report.json");

        let report = RunReport {
            detection_p50_ms: 10,
            detection_p95_ms: 20,
            false_suspicions: 1,
            convergence_ms: 30,
            owner_churn_per_min: 2.5,
        };
        let write_result = write_run_report(&out, &report);
        assert!(write_result.is_ok());

        let read_result = std::fs::read_to_string(&out);
        assert!(read_result.is_ok());
        let payload = match read_result {
            Ok(payload) => payload,
            Err(error) => panic!("report file should exist: {error}"),
        };
        assert!(payload.contains("\n  \"detection_p50_ms\": 10"));
    }
}
