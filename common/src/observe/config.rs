// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Observability configuration structures.

use serde::{Deserialize, Serialize};

/// Observability configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Logging configuration.
    pub logging: LoggingConfig,
    /// Tracing configuration.
    pub tracing: TracingConfig,
    /// Metrics configuration.
    pub metrics: MetricsConfig,
    /// Resource attributes.
    pub resource: ResourceConfig,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            logging: LoggingConfig::default(),
            tracing: TracingConfig::default(),
            metrics: MetricsConfig::default(),
            resource: ResourceConfig::default(),
        }
    }
}

/// Logging configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error).
    pub level: String,
    /// Format: "json" or "pretty".
    pub format: String,
    /// Target filters (e.g., "vecton=debug,transport=info").
    pub targets: Option<String>,
    /// Output to stdout (default true).
    pub stdout: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "json".to_string(),
            targets: None,
            stdout: true,
        }
    }
}

/// Tracing configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Enable tracing.
    pub enabled: bool,
    /// Sampling configuration.
    pub sampling: SamplingConfig,
    /// OTLP configuration.
    pub otlp: OtlpConfig,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sampling: SamplingConfig::default(),
            otlp: OtlpConfig::default(),
        }
    }
}

/// Sampling configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SamplingConfig {
    /// Sampling ratio (0.0 to 1.0).
    pub ratio: f64,
    /// Use parent-based sampling.
    pub parent_based: bool,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            ratio: 1.0,
            parent_based: true,
        }
    }
}

/// OTLP configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtlpConfig {
    /// Enable OTLP export.
    pub enabled: bool,
    /// OTLP endpoint URL.
    pub endpoint: String,
    /// Protocol: "grpc" or "http".
    pub protocol: String,
    /// Additional headers (key=value pairs, comma-separated).
    pub headers: Option<String>,
    /// Timeout in milliseconds.
    pub timeout_ms: u64,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://localhost:4317".to_string(),
            protocol: "grpc".to_string(),
            headers: None,
            timeout_ms: 10000,
        }
    }
}

/// Metrics configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable metrics.
    pub enabled: bool,
    /// Prometheus configuration.
    pub prometheus: PrometheusConfig,
    /// OTLP metrics configuration.
    pub otlp: OtlpMetricsConfig,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prometheus: PrometheusConfig::default(),
            otlp: OtlpMetricsConfig::default(),
        }
    }
}

/// Prometheus configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrometheusConfig {
    /// Enable Prometheus exporter.
    pub enabled: bool,
    /// Bind address (e.g., "0.0.0.0:9090").
    pub bind: String,
    /// Metrics path (e.g., "/metrics").
    pub path: String,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: "0.0.0.0:9090".to_string(),
            path: "/metrics".to_string(),
        }
    }
}

/// OTLP metrics configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtlpMetricsConfig {
    /// Enable OTLP metrics export.
    pub enabled: bool,
    /// OTLP endpoint URL.
    pub endpoint: String,
    /// Protocol: "grpc" or "http".
    pub protocol: String,
    /// Export interval in milliseconds.
    pub interval_ms: u64,
}

impl Default for OtlpMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://localhost:4317".to_string(),
            protocol: "grpc".to_string(),
            interval_ms: 60000,
        }
    }
}

/// Resource attributes configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            service_name: None,
            service_version: None,
            environment: None,
            instance_id: None,
            node_name: None,
            cluster: None,
        }
    }
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
