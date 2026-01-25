// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Observability demo example.
//!
//! This example demonstrates:
//! - Initializing observability with stdout JSON logging
//! - Prometheus metrics endpoint
//! - Tracing spans
//! - Mock transport and UFS operations

use common::observe::{ObservabilityConfig, ServiceInfo, init_observability};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure observability
    let config = ObservabilityConfig {
        logging: common::observe::config::LoggingConfig {
            level: "info".to_string(),
            format: "json".to_string(),
            targets: None,
            stdout: true,
        },
        tracing: common::observe::config::TracingConfig {
            enabled: true,
            sampling: common::observe::config::SamplingConfig {
                ratio: 1.0,
                parent_based: true,
            },
            otlp: common::observe::config::OtlpConfig {
                enabled: false, // Set to true if you have an OTLP collector running
                endpoint: "http://localhost:4317".to_string(),
                protocol: "grpc".to_string(),
                headers: None,
                timeout_ms: 10000,
            },
        },
        metrics: common::observe::config::MetricsConfig {
            enabled: true,
            prometheus: common::observe::config::PrometheusConfig {
                enabled: true,
                bind: "0.0.0.0:9090".to_string(),
                path: "/metrics".to_string(),
            },
            otlp: common::observe::config::OtlpMetricsConfig {
                enabled: false, // Set to true if you have an OTLP collector running
                endpoint: "http://localhost:4317".to_string(),
                protocol: "grpc".to_string(),
                interval_ms: 60000,
            },
        },
        resource: common::observe::config::ResourceConfig {
            service_name: Some("observability-demo".to_string()),
            service_version: Some("0.1.0".to_string()),
            environment: Some("development".to_string()),
            instance_id: Some("demo-1".to_string()),
            node_name: Some("demo-node".to_string()),
            cluster: None,
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

        // Record some metrics (these would normally come from transport/ufs)
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
