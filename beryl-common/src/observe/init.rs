// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Observability initialization.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::observe::config::{ObservabilityConfig, ServiceInfo};

static INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Guard that keeps process observability resources alive.
pub struct ObservabilityGuard {
    _prometheus_handle: Option<metrics_exporter_prometheus::PrometheusHandle>,
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
    let handle = init_prometheus_metrics(&config.metrics.prometheus.bind, &config.metrics.prometheus.path)?;

    tracing::info!(
        event = "observability_initialized",
        service_name = %service_info.name,
        service_version = %service_info.version,
        environment = %service_info.environment,
        "Observability initialized"
    );

    Ok(ObservabilityGuard {
        _prometheus_handle: Some(handle),
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::config::{LogConfig, MetricsConfig, PrometheusConfig, ResourceConfig};

    #[test]
    fn prometheus_bind_conflict_returns_error_and_resets_initialized() {
        INITIALIZED.store(false, Ordering::SeqCst);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("test listener must bind");
        let bind = listener.local_addr().expect("test listener local addr").to_string();

        let config = ObservabilityConfig {
            log: LogConfig {
                format: "compact".to_string(),
                output: "stderr".to_string(),
                level: "warn".to_string(),
            },
            metrics: MetricsConfig {
                prometheus: PrometheusConfig {
                    bind,
                    path: "/metrics".to_string(),
                },
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
