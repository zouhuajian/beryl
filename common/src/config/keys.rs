// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Shared configuration key constants.
//!
//! `common` owns generic config loading plus shared primitives such as
//! observability. Module-specific keys, defaults, and validation belong to the
//! owning module's typed config.

/// Observability logging configuration keys.
pub mod observe_logging {
    /// Log level: "trace", "debug", "info", "warn", or "error".
    pub const LEVEL: &str = "observe.logging.level";
    /// Log format: "json" or "pretty".
    pub const FORMAT: &str = "observe.logging.format";
    /// Target filters (e.g., "metadata=debug,worker=info", optional).
    pub const TARGETS: &str = "observe.logging.targets";
    /// Output logs to stdout.
    pub const STDOUT: &str = "observe.logging.stdout";
}

/// Observability tracing configuration keys.
pub mod observe_tracing {
    /// Enable distributed tracing.
    pub const ENABLED: &str = "observe.tracing.enabled";
    /// Sampling ratio for traces (0.0-1.0, e.g., "1.0" = 100%).
    pub const SAMPLING_RATIO: &str = "observe.tracing.sampling.ratio";
    /// Use parent-based sampling (respect parent trace decision).
    pub const SAMPLING_PARENT_BASED: &str = "observe.tracing.sampling.parent_based";
    /// Enable OTLP trace export.
    pub const OTLP_ENABLED: &str = "observe.tracing.otlp.enabled";
    /// OTLP endpoint URL.
    pub const OTLP_ENDPOINT: &str = "observe.tracing.otlp.endpoint";
    /// OTLP protocol: "grpc" or "http".
    pub const OTLP_PROTOCOL: &str = "observe.tracing.otlp.protocol";
    /// OTLP export timeout in milliseconds.
    pub const OTLP_TIMEOUT_MS: &str = "observe.tracing.otlp.timeout_ms";
}

/// Observability metrics configuration keys.
pub mod observe_metrics {
    /// Enable metrics collection.
    pub const ENABLED: &str = "observe.metrics.enabled";
    /// Enable Prometheus metrics exporter.
    pub const PROMETHEUS_ENABLED: &str = "observe.metrics.prometheus.enabled";
    /// Prometheus bind address (format: "host:port").
    pub const PROMETHEUS_BIND: &str = "observe.metrics.prometheus.bind";
    /// HTTP path for Prometheus metrics.
    pub const PROMETHEUS_PATH: &str = "observe.metrics.prometheus.path";
    /// Enable OTLP metrics export.
    pub const OTLP_ENABLED: &str = "observe.metrics.otlp.enabled";
    /// OTLP metrics endpoint URL.
    pub const OTLP_ENDPOINT: &str = "observe.metrics.otlp.endpoint";
    /// OTLP metrics protocol: "grpc" or "http".
    pub const OTLP_PROTOCOL: &str = "observe.metrics.otlp.protocol";
    /// OTLP metrics export interval in milliseconds.
    pub const OTLP_INTERVAL_MS: &str = "observe.metrics.otlp.interval_ms";
}
