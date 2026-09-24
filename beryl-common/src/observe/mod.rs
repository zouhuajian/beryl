// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Unified observability infrastructure for Beryl.
//!
//! This module provides:
//! - Logging with compact or flat JSON output
//! - Tracing subscriber setup
//! - Metrics recorder setup and Prometheus export
//! - Incoming W3C traceparent context

pub mod config;
pub mod propagation;
mod tracing;

use metrics_exporter_prometheus::PrometheusHandle;

pub use config::{ObservabilityConfig, ServiceInfo};

/// Initialize observability infrastructure.
///
/// Returns the metrics renderer for the process HTTP endpoint.
/// This function should be called once at application startup. Subsequent calls
/// will return an error if already initialized.
pub fn init_observability(
    config: &ObservabilityConfig,
    service_info: ServiceInfo,
) -> Result<PrometheusHandle, Box<dyn std::error::Error>> {
    tracing::init_tracing_subscriber(config)?;
    let handle = metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;

    ::tracing::info!(
        event = "observability_initialized",
        service_name = %service_info.name,
        service_version = %service_info.version,
        environment = %service_info.environment,
        "Observability initialized"
    );

    Ok(handle)
}
