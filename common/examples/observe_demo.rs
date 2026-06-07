// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Observability demo example.
//!
//! This example demonstrates:
//! - Initializing observability with production-style flat JSON logging
//! - Prometheus metrics endpoint
//! - Tracing spans
//! - Mock transport and UFS operations

use common::observe::config::{
    LogConfig, MetricsConfig, ObservabilityConfig, PrometheusConfig, ResourceConfig, ServiceInfo,
};
use common::observe::init_observability;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ObservabilityConfig {
        log: LogConfig {
            level: "info,vecton=info,common=info,tonic=warn,tower=warn,h2=warn".to_string(),
            format: "json".to_string(),
            output: "stdout".to_string(),
        },
        metrics: MetricsConfig {
            prometheus: PrometheusConfig {
                bind: "0.0.0.0:9090".to_string(),
                path: "/metrics".to_string(),
            },
        },
        resource: ResourceConfig {
            service_name: Some("observability-demo".to_string()),
            service_version: Some("0.1.0".to_string()),
            environment: Some("development".to_string()),
            instance_id: Some("demo-1".to_string()),
            node_name: Some("demo-node".to_string()),
            ..Default::default()
        },
    };

    let service_info = ServiceInfo {
        name: "observability-demo".to_string(),
        version: "0.1.0".to_string(),
        environment: "development".to_string(),
        instance_id: "demo-1".to_string(),
        node_name: Some("demo-node".to_string()),
    };

    // Initialize observability
    let _guard = init_observability(&config, service_info)?;

    tracing::info!("Observability demo started");
    tracing::info!("Prometheus metrics available at http://localhost:9090/metrics");

    // Simulate some operations with tracing spans
    for i in 0..5 {
        let span = tracing::info_span!("demo.operation", iteration = i);
        let _guard = span.enter();

        tracing::info!("Performing operation {}", i);

        // Simulate some work
        sleep(Duration::from_millis(100)).await;

        // Record some metrics (these would normally come from RPC and UFS adapters)
        metrics::counter!("demo_operations_total", "status" => "ok").increment(1);
        metrics::histogram!("demo_operation_latency_ms").record(100.0);

        tracing::info!("Operation {} completed", i);
    }

    tracing::info!("Demo completed. Check http://localhost:9090/metrics for Prometheus metrics");
    tracing::info!("Press Ctrl+C to exit");

    // Keep the program running so metrics endpoint stays available
    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
