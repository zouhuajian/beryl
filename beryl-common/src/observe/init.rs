// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Observability initialization.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::observe::config::{ObservabilityConfig, ServiceInfo};

static INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Guard that keeps process observability resources alive.
pub struct ObservabilityGuard {
    prometheus_handle: metrics_exporter_prometheus::PrometheusHandle,
}

impl ObservabilityGuard {
    /// Return the renderer used by the process-owned HTTP server.
    pub fn prometheus_handle(&self) -> metrics_exporter_prometheus::PrometheusHandle {
        self.prometheus_handle.clone()
    }
}

/// Initialize observability infrastructure.
///
/// This function should be called once at application startup. Subsequent calls
/// will return an error if already initialized.
pub fn init_observability(
    config: &ObservabilityConfig,
    service_info: ServiceInfo,
) -> Result<ObservabilityGuard, Box<dyn std::error::Error>> {
    if INITIALIZED.swap(true, Ordering::SeqCst) {
        return Err("Observability already initialized".into());
    }

    match init_observability_once(config, service_info) {
        Ok(guard) => Ok(guard),
        Err(err) => {
            INITIALIZED.store(false, Ordering::SeqCst);
            Err(err)
        }
    }
}

fn init_observability_once(
    config: &ObservabilityConfig,
    service_info: ServiceInfo,
) -> Result<ObservabilityGuard, Box<dyn std::error::Error>> {
    crate::observe::tracing::init_tracing_subscriber(config)?;
    let handle = metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;

    tracing::info!(
        event = "observability_initialized",
        service_name = %service_info.name,
        service_version = %service_info.version,
        environment = %service_info.environment,
        "Observability initialized"
    );

    Ok(ObservabilityGuard {
        prometheus_handle: handle,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::config::{LogConfig, ResourceConfig};

    #[test]
    fn failed_initialization_resets_process_guard() {
        INITIALIZED.store(false, Ordering::SeqCst);

        let config = ObservabilityConfig {
            log: LogConfig {
                format: "invalid".to_string(),
                output: "stderr".to_string(),
                level: "warn".to_string(),
            },
            resource: ResourceConfig::default(),
        };
        assert!(init_observability(&config, test_service_info()).is_err());
        assert!(!INITIALIZED.load(Ordering::SeqCst));
    }

    fn test_service_info() -> ServiceInfo {
        ServiceInfo {
            name: "test-service".to_string(),
            version: "0.0.0".to_string(),
            environment: "test".to_string(),
            instance_id: "test-instance".to_string(),
            node_name: None,
        }
    }
}
