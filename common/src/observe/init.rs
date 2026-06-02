// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Observability initialization.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::observe::config::{ObservabilityConfig, ServiceInfo};

static INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Guard that ensures observability is properly shut down on drop.
pub struct ObservabilityGuard {
    #[cfg(feature = "otel")]
    tracer_provider: Option<opentelemetry_sdk::trace::TracerProvider>,
    #[cfg(feature = "otel")]
    meter_provider: Option<opentelemetry_sdk::metrics::MeterProvider>,
    prometheus_handle: Option<metrics_exporter_prometheus::PrometheusHandle>,
}

impl Drop for ObservabilityGuard {
    fn drop(&mut self) {
        // Flush OTLP exporters
        #[cfg(feature = "otel")]
        {
            if let Some(provider) = self.tracer_provider.take() {
                opentelemetry_sdk::trace::TracerProvider::shutdown(&provider);
            }
            if let Some(provider) = self.meter_provider.take() {
                opentelemetry_sdk::metrics::MeterProvider::shutdown(&provider);
            }
        }
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
    // Check if already initialized
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
    // Initialize logging (stdout JSON)
    if config.logging.stdout {
        crate::observe::tracing::init_tracing_subscriber(
            &config.logging.level,
            &config.logging.format,
            config.logging.targets.as_deref(),
        )?;
    }

    let mut guard = ObservabilityGuard {
        #[cfg(feature = "otel")]
        tracer_provider: None,
        #[cfg(feature = "otel")]
        meter_provider: None,
        prometheus_handle: None,
    };

    // Initialize tracing with OTLP if enabled
    #[cfg(feature = "otel")]
    if config.tracing.enabled && config.tracing.otlp.enabled {
        match crate::observe::tracing::otel::init_otel_tracing(
            &config.tracing.otlp.endpoint,
            &config.tracing.otlp.protocol,
            config.tracing.otlp.headers.as_deref(),
            config.tracing.otlp.timeout_ms,
            config.tracing.sampling.ratio,
            config.tracing.sampling.parent_based,
            &config.resource,
            &service_info,
        ) {
            Ok(provider) => {
                // Add OTLP layer to existing subscriber
                if let Err(e) = crate::observe::tracing::otel::add_otel_layer(provider.clone()) {
                    tracing::warn!(
                        error = %e,
                        "Failed to add OTLP layer to subscriber, continuing without it"
                    );
                } else {
                    guard.tracer_provider = Some(provider);
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to initialize OTLP tracing, continuing without it"
                );
            }
        }
    }

    // Initialize metrics
    if config.metrics.enabled {
        // Initialize Prometheus exporter if enabled
        if config.metrics.prometheus.enabled {
            let handle = init_prometheus_metrics(&config.metrics.prometheus.bind, &config.metrics.prometheus.path)?;
            guard.prometheus_handle = Some(handle);
        }

        // Initialize OTLP metrics if enabled
        #[cfg(feature = "otel")]
        if config.metrics.otlp.enabled {
            match init_otlp_metrics(
                &config.metrics.otlp.endpoint,
                &config.metrics.otlp.protocol,
                Duration::from_millis(config.metrics.otlp.interval_ms),
                &config.resource,
                &service_info,
            ) {
                Ok(provider) => {
                    guard.meter_provider = Some(provider);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to initialize OTLP metrics, continuing without it"
                    );
                }
            }
        }
    }

    tracing::info!(
        service_name = %service_info.name,
        service_version = %service_info.version,
        environment = %service_info.environment,
        "Observability initialized"
    );

    Ok(guard)
}

fn bind_prometheus_listener(bind: &str) -> Result<(SocketAddr, tokio::net::TcpListener), Box<dyn std::error::Error>> {
    let bind_addr: SocketAddr = bind.parse()?;
    let listener = std::net::TcpListener::bind(bind_addr)?;
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let local_addr = listener.local_addr()?;
    Ok((local_addr, listener))
}

/// Initialize Prometheus metrics exporter with HTTP endpoint.
fn init_prometheus_metrics(
    bind: &str,
    path: &str,
) -> Result<metrics_exporter_prometheus::PrometheusHandle, Box<dyn std::error::Error>> {
    use metrics_exporter_prometheus::PrometheusBuilder;

    let (bind_addr, listener) = bind_prometheus_listener(bind)?;
    let handle = PrometheusBuilder::new().install_recorder()?;

    // Start HTTP server for /metrics endpoint
    let path = path.to_string();
    let path_clone = path.clone();
    let handle_clone = handle.clone();

    tokio::spawn(async move {
        use http::Response;
        use http_body_util::Full;
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let io = TokioIo::new(stream);
                    let path = path_clone.clone();
                    let handle = handle_clone.clone();

                    tokio::task::spawn(async move {
                        let service = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                            let path = path.clone();
                            let handle = handle.clone();
                            async move {
                                if req.uri().path() == path {
                                    let body = handle.render();
                                    Ok::<_, hyper::Error>(
                                        Response::builder()
                                            .status(200)
                                            .header("Content-Type", "text/plain; version=0.0.4")
                                            .body(Full::new(hyper::body::Bytes::from(body)))
                                            .unwrap(),
                                    )
                                } else {
                                    Ok(Response::builder()
                                        .status(404)
                                        .body(Full::new(hyper::body::Bytes::from("Not Found")))
                                        .unwrap())
                                }
                            }
                        });

                        let _ = http1::Builder::new().serve_connection(io, service).await;
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Error accepting connection on Prometheus endpoint");
                }
            }
        }
    });

    tracing::info!(bind = %bind_addr, path = %path, "Prometheus metrics endpoint started");

    Ok(handle)
}

#[cfg(feature = "otel")]
fn init_otlp_metrics(
    endpoint: &str,
    _protocol: &str,
    interval: Duration,
    resource: &crate::observe::config::ResourceConfig,
    service_info: &ServiceInfo,
) -> Result<opentelemetry_sdk::metrics::MeterProvider, Box<dyn std::error::Error>> {
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::MeterProvider;
    use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};

    let mut resource_builder = Resource::default();

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
        resource_builder = resource_builder.with_attributes(vec![KeyValue::new("deployment.environment", env.clone())]);
    }

    let exporter = opentelemetry_otlp::new_exporter().tonic().with_endpoint(endpoint);

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_interval(interval)
        .build();

    let provider = MeterProvider::builder()
        .with_resource(resource_builder)
        .with_reader(reader)
        .build();

    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::config::{
        LoggingConfig, MetricsConfig, OtlpConfig, OtlpMetricsConfig, PrometheusConfig, ResourceConfig, TracingConfig,
    };

    #[test]
    fn prometheus_bind_conflict_returns_error_and_resets_initialized() {
        INITIALIZED.store(false, Ordering::SeqCst);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("test listener must bind");
        let bind = listener.local_addr().expect("test listener local addr").to_string();

        let config = ObservabilityConfig {
            logging: LoggingConfig {
                stdout: false,
                ..LoggingConfig::default()
            },
            tracing: TracingConfig {
                enabled: false,
                sampling: Default::default(),
                otlp: OtlpConfig::default(),
            },
            metrics: MetricsConfig {
                enabled: true,
                prometheus: PrometheusConfig {
                    enabled: true,
                    bind,
                    path: "/metrics".to_string(),
                },
                otlp: OtlpMetricsConfig::default(),
            },
            resource: ResourceConfig::default(),
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("test runtime");
        let err = match runtime.block_on(async { init_observability(&config, test_service_info()) }) {
            Ok(_) => panic!("bind conflict must fail init"),
            Err(err) => err,
        };

        let err = err
            .downcast_ref::<std::io::Error>()
            .expect("bind conflict should return io error");
        assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
        assert!(!INITIALIZED.load(Ordering::SeqCst));
    }

    #[test]
    fn prometheus_listener_bind_succeeds_before_accept_task_starts() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("test runtime");

        let (addr, listener) = runtime
            .block_on(async { bind_prometheus_listener("127.0.0.1:0") })
            .expect("ephemeral Prometheus listener bind");

        assert_ne!(addr.port(), 0);
        let stream = std::net::TcpStream::connect(addr).expect("listener should accept TCP connect after bind");
        drop(stream);
        drop(listener);
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
