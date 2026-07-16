// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Observability configuration structures.

use crate::config::{
    FlatConfig,
    keys::{observe_log, observe_metrics},
};
use crate::error::{CommonError, CommonErrorKind};
use serde::{Deserialize, Serialize};

/// Observability configuration loaded from flat `observe.*` file keys.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Logging configuration.
    pub log: LogConfig,
    /// Metrics configuration.
    pub metrics: MetricsConfig,
    /// Resource attributes.
    pub resource: ResourceConfig,
}

impl ObservabilityConfig {
    /// Parse the shared flat `observe.*` configuration model.
    pub fn from_flat(flat: &FlatConfig) -> Result<Self, CommonError> {
        let config = Self {
            log: LogConfig {
                format: required_str(flat, observe_log::FORMAT)?,
                output: required_str(flat, observe_log::OUTPUT)?,
                level: required_str(flat, observe_log::LEVEL)?,
            },
            metrics: MetricsConfig {
                prometheus: PrometheusConfig {
                    bind: required_str(flat, observe_metrics::PROMETHEUS_BIND)?,
                    path: required_str(flat, observe_metrics::PROMETHEUS_PATH)?,
                },
            },
            resource: ResourceConfig::default(),
        };

        validate_log_config(&config.log)?;
        validate_metrics_config(&config.metrics)?;
        Ok(config)
    }
}

/// Logging configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogConfig {
    /// Format: "compact" or "json".
    pub format: String,
    /// Output stream: "stderr" or "stdout".
    pub output: String,
    /// EnvFilter directive string.
    pub level: String,
}

/// Metrics configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Prometheus configuration.
    pub prometheus: PrometheusConfig,
}

/// Prometheus configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrometheusConfig {
    /// Bind address (e.g., "127.0.0.1:18081").
    pub bind: String,
    /// Metrics path (e.g., "/metrics").
    pub path: String,
}

/// Resource attributes configuration.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResourceConfig {
    /// Service name.
    pub service_name: Option<String>,
    /// Service version.
    pub service_version: Option<String>,
    /// Environment (e.g., "production", "staging", "development").
    pub environment: Option<String>,
    /// Instance ID.
    pub instance_id: Option<String>,
    /// Node name.
    pub node_name: Option<String>,
    /// Cluster name.
    pub cluster: Option<String>,
}

/// Service information for observability initialization.
#[derive(Clone, Debug)]
pub struct ServiceInfo {
    /// Service name.
    pub name: String,
    /// Service version.
    pub version: String,
    /// Environment.
    pub environment: String,
    /// Instance ID.
    pub instance_id: String,
    /// Node name (optional).
    pub node_name: Option<String>,
}

fn required_str(flat: &FlatConfig, key: &'static str) -> Result<String, CommonError> {
    let value = flat
        .get_str(key)
        .ok_or_else(|| invalid_config(key, "must be present and be a string"))?;
    if value.trim().is_empty() {
        return Err(invalid_config(key, "must not be empty"));
    }
    Ok(value)
}

fn validate_log_config(config: &LogConfig) -> Result<(), CommonError> {
    match config.format.as_str() {
        "compact" | "json" => {}
        _ => return Err(invalid_config(observe_log::FORMAT, "must be compact or json")),
    }
    match config.output.as_str() {
        "stderr" | "stdout" => {}
        _ => return Err(invalid_config(observe_log::OUTPUT, "must be stderr or stdout")),
    }
    Ok(())
}

fn validate_metrics_config(config: &MetricsConfig) -> Result<(), CommonError> {
    config.prometheus.bind.parse::<std::net::SocketAddr>().map_err(|err| {
        CommonError::new(
            CommonErrorKind::InvalidArgument,
            format!("Invalid {}: {err}", observe_metrics::PROMETHEUS_BIND),
        )
    })?;
    if !config.prometheus.path.starts_with('/') {
        return Err(invalid_config(observe_metrics::PROMETHEUS_PATH, "must start with /"));
    }
    Ok(())
}

fn invalid_config(key: &'static str, detail: impl Into<String>) -> CommonError {
    CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {}", detail.into()))
}

#[cfg(test)]
mod tests {
    use crate::config::FlatConfig;

    use super::*;

    const TEST_LEVEL: &str =
        "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn";

    #[test]
    fn minimal_observe_config_requires_file_values() {
        let flat = FlatConfig::new();

        let err = ObservabilityConfig::from_flat(&flat).expect_err("missing observe.* file values must fail");

        assert!(err.message.contains("observe.log.format"), "{err:?}");
    }

    #[test]
    fn minimal_observe_config_parses_file_values_only() {
        let config = ObservabilityConfig::from_flat(&minimal_observe_flat()).unwrap();

        assert_eq!(config.log.format, "compact");
        assert_eq!(config.log.output, "stderr");
        assert_eq!(config.log.level, TEST_LEVEL);
        assert_eq!(config.metrics.prometheus.bind, "127.0.0.1:18081");
        assert_eq!(config.metrics.prometheus.path, "/metrics");
    }

    #[test]
    fn invalid_values_are_rejected() {
        let cases = [
            ("observe.log.format", "pretty"),
            ("observe.log.output", "file"),
            ("observe.metrics.prometheus.bind", "127.0.0.1:70000"),
            ("observe.metrics.prometheus.path", "metrics"),
        ];

        for (key, value) in cases {
            let mut flat = minimal_observe_flat();
            flat.set(key, value);

            let err = ObservabilityConfig::from_flat(&flat).expect_err("invalid observe value must fail");
            assert!(err.message.contains(key), "{key}: {err:?}");
        }
    }

    fn minimal_observe_flat() -> FlatConfig {
        let mut flat = FlatConfig::new();
        flat.set(observe_log::FORMAT, "compact");
        flat.set(observe_log::OUTPUT, "stderr");
        flat.set(observe_log::LEVEL, TEST_LEVEL);
        flat.set(observe_metrics::PROMETHEUS_BIND, "127.0.0.1:18081");
        flat.set(observe_metrics::PROMETHEUS_PATH, "/metrics");
        flat
    }
}
