// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tracing setup and configuration.

use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{self},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

/// Initialize tracing subscriber with JSON output to stdout.
pub fn init_tracing_subscriber(
    level: &str,
    format: &str,
    targets: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let filter = if let Some(targets) = targets {
        EnvFilter::try_new(targets)?
    } else {
        EnvFilter::try_new(level)?
    };

    let fmt_layer = if format == "json" {
        fmt::layer().json().with_writer(std::io::stdout).boxed()
    } else {
        fmt::layer().pretty().with_writer(std::io::stdout).boxed()
    };

    Registry::default().with(filter).with(fmt_layer).try_init()?;

    Ok(())
}

/// Add OpenTelemetry layer to existing subscriber.
#[cfg(feature = "otel")]
pub fn add_otel_layer_to_subscriber(
    provider: opentelemetry_sdk::trace::TracerProvider,
) -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let otel_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("vecton"));

    tracing_subscriber::registry().with(otel_layer).try_init()?;

    Ok(())
}

#[cfg(feature = "otel")]
pub mod otel {
    use opentelemetry::trace::TraceError;
    use opentelemetry_sdk::{
        Resource,
        trace::{self, TracerProvider},
    };
    use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};
    use std::time::Duration;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    /// Initialize OpenTelemetry tracing with OTLP exporter.
    pub fn init_otel_tracing(
        endpoint: &str,
        _protocol: &str,
        _headers: Option<&str>,
        timeout_ms: u64,
        sampling_ratio: f64,
        parent_based: bool,
        resource: &crate::observe::config::ResourceConfig,
        service_info: &crate::observe::config::ServiceInfo,
    ) -> Result<TracerProvider, TraceError> {
        use opentelemetry_otlp::WithExportConfig;

        let mut resource_builder = Resource::default();

        // Set service name from service_info or config
        let service_name = resource.service_name.as_ref().unwrap_or(&service_info.name);
        resource_builder = resource_builder.with_attributes(vec![SERVICE_NAME.string(service_name.clone())]);

        if let Some(version) = resource
            .service_version
            .as_ref()
            .or_else(|| Some(&service_info.version))
        {
            resource_builder = resource_builder.with_attributes(vec![SERVICE_VERSION.string(version.clone())]);
        }

        if let Some(env) = &resource.environment {
            resource_builder = resource_builder.with_attributes(vec![opentelemetry::KeyValue::new(
                "deployment.environment",
                env.clone(),
            )]);
        }

        if let Some(instance_id) = &resource.instance_id {
            resource_builder = resource_builder.with_attributes(vec![opentelemetry::KeyValue::new(
                "service.instance.id",
                instance_id.clone(),
            )]);
        }

        let exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_millis(timeout_ms));

        let provider = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(exporter)
            .with_trace_config(
                trace::config()
                    .with_resource(resource_builder)
                    .with_sampler(if parent_based {
                        trace::Sampler::ParentBased(Box::new(trace::Sampler::TraceIdRatioBased(sampling_ratio)))
                    } else {
                        trace::Sampler::TraceIdRatioBased(sampling_ratio)
                    }),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;

        Ok(provider)
    }

    /// Add OpenTelemetry layer to existing subscriber.
    pub fn add_otel_layer(provider: TracerProvider) -> Result<(), Box<dyn std::error::Error>> {
        let otel_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("vecton"));

        tracing_subscriber::registry().with(otel_layer).try_init()?;

        Ok(())
    }
}
