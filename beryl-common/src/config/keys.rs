// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared configuration key constants.
//!
//! `common` owns generic config loading plus shared primitives such as
//! observability. Module-specific keys, defaults, and validation belong to the
//! owning module's typed config.

/// Observability logging configuration keys.
pub mod observe_log {
    /// EnvFilter directive string.
    pub const LEVEL: &str = "observe.log.level";
    /// Log format: "compact" or "json".
    pub const FORMAT: &str = "observe.log.format";
    /// Log output stream: "stderr" or "stdout".
    pub const OUTPUT: &str = "observe.log.output";
}

/// Observability metrics configuration keys.
pub mod observe_metrics {
    /// Prometheus bind address (format: "host:port").
    pub const PROMETHEUS_BIND: &str = "observe.metrics.prometheus.bind";
    /// HTTP path for Prometheus metrics.
    pub const PROMETHEUS_PATH: &str = "observe.metrics.prometheus.path";
}
