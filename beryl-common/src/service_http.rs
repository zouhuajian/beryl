// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared HTTP serving mechanics for Beryl processes.

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use http::{Response, StatusCode};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Owned HTTP listener and connection lifecycle for one Beryl process.
///
/// Graceful shutdown stops accepting new connections and waits for accepted
/// requests. Dropping the handle cancels and aborts all remaining HTTP work,
/// which prevents connection tasks from outliving their process owner.
pub struct ServiceHttpHandle {
    #[cfg(test)]
    local_addr: SocketAddr,
    shutdown: CancellationToken,
    force: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl ServiceHttpHandle {
    /// Drains accepted requests until `deadline`, then aborts and awaits them.
    ///
    /// Returns `true` when the deadline required forced cancellation. Task
    /// panics remain errors; cancellation initiated here is expected.
    pub async fn shutdown_until(mut self, deadline: Instant) -> Result<bool, tokio::task::JoinError> {
        self.shutdown.cancel();
        let Some(mut task) = self.task.take() else {
            return Ok(false);
        };
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(result) => {
                result?;
                Ok(false)
            }
            Err(_) => {
                self.force.cancel();
                task.await?;
                Ok(true)
            }
        }
    }
}

impl Drop for ServiceHttpHandle {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.force.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Starts the process HTTP endpoint after synchronously binding its socket.
///
/// The caller owns readiness semantics through `is_ready`; this module only
/// provides the shared transport and the fixed `/metrics`, `/health`, and
/// `/ready` routes.
pub fn spawn_service_http(
    bind: SocketAddr,
    metrics: PrometheusHandle,
    is_ready: Arc<dyn Fn() -> bool + Send + Sync>,
) -> io::Result<ServiceHttpHandle> {
    let listener = std::net::TcpListener::bind(bind)?;
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;
    let local_addr = listener.local_addr()?;

    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let force = CancellationToken::new();
    let task_force = force.clone();
    tracing::info!(address = %local_addr, "HTTP service started");
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = task_shutdown.cancelled() => break,
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = completed {
                        tracing::debug!(%error, "HTTP connection task terminated unexpectedly");
                    }
                }
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        let metrics = metrics.clone();
                        let is_ready = Arc::clone(&is_ready);
                        connections.spawn(async move {
                            let service = service_fn(move |request| {
                                handle_request(request, metrics.clone(), Arc::clone(&is_ready))
                            });
                            if let Err(error) = http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service)
                                .await
                            {
                                tracing::debug!(%error, "HTTP connection closed with an error");
                            }
                        });
                    }
                    Err(error) => tracing::warn!(%error, "HTTP accept failed"),
                },
            }
        }
        while !connections.is_empty() {
            tokio::select! {
                biased;
                _ = task_force.cancelled() => {
                    connections.abort_all();
                    while let Some(completed) = connections.join_next().await {
                        if let Err(error) = completed
                            && !error.is_cancelled()
                        {
                            tracing::debug!(%error, "HTTP connection task terminated unexpectedly");
                        }
                    }
                }
                completed = connections.join_next() => {
                    if let Some(Err(error)) = completed {
                        tracing::debug!(%error, "HTTP connection task terminated unexpectedly");
                    }
                }
            }
        }
    });
    Ok(ServiceHttpHandle {
        #[cfg(test)]
        local_addr,
        shutdown,
        force,
        task: Some(task),
    })
}

async fn handle_request<B>(
    request: Request<B>,
    metrics: PrometheusHandle,
    is_ready: Arc<dyn Fn() -> bool + Send + Sync>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/metrics") => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; version=0.0.4")
            .body(Full::new(Bytes::from(metrics.render())))
            .expect("static metrics response is valid"),
        (&Method::GET, "/health") => text_response(StatusCode::OK, "ok"),
        (&Method::GET, "/ready") if is_ready() => text_response(StatusCode::OK, "ready"),
        (&Method::GET, "/ready") => text_response(StatusCode::SERVICE_UNAVAILABLE, "not ready"),
        _ => text_response(StatusCode::NOT_FOUND, "not found"),
    };
    Ok(response)
}

fn text_response(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .expect("static text response is valid")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use metrics_exporter_prometheus::PrometheusBuilder;
    use tokio::net::TcpStream;

    use super::*;

    #[tokio::test]
    async fn routes_report_liveness_and_dynamic_readiness() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let metrics = recorder.handle();
        let ready = Arc::new(AtomicBool::new(false));
        let ready_state = Arc::clone(&ready);
        let is_ready: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || ready_state.load(Ordering::Relaxed));

        let health = handle_request(
            Request::get("/health").body(()).unwrap(),
            metrics.clone(),
            Arc::clone(&is_ready),
        )
        .await
        .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let metrics_response = handle_request(
            Request::get("/metrics").body(()).unwrap(),
            metrics.clone(),
            Arc::clone(&is_ready),
        )
        .await
        .unwrap();
        assert_eq!(metrics_response.status(), StatusCode::OK);
        assert_eq!(metrics_response.headers()["Content-Type"], "text/plain; version=0.0.4");

        let unavailable = handle_request(
            Request::get("/ready").body(()).unwrap(),
            metrics.clone(),
            Arc::clone(&is_ready),
        )
        .await
        .unwrap();
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);

        ready.store(true, Ordering::Relaxed);
        let available = handle_request(
            Request::get("/ready").body(()).unwrap(),
            metrics.clone(),
            Arc::clone(&is_ready),
        )
        .await
        .unwrap();
        assert_eq!(available.status(), StatusCode::OK);

        let missing = handle_request(Request::get("/unknown").body(()).unwrap(), metrics, is_ready)
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn shutdown_closes_listener_and_awaits_connection_tasks() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let server = spawn_service_http("127.0.0.1:0".parse().unwrap(), recorder.handle(), Arc::new(|| true)).unwrap();
        let address = server.local_addr;
        let connection = TcpStream::connect(address).await.unwrap();
        drop(connection);

        let forced = tokio::time::timeout(
            Duration::from_secs(2),
            server.shutdown_until(Instant::now() + Duration::from_secs(1)),
        )
        .await
        .expect("HTTP shutdown must be bounded")
        .unwrap();

        assert!(!forced);
        assert!(TcpStream::connect(address).await.is_err());
    }
}
