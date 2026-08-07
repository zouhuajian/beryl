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

/// Starts the process HTTP endpoint after synchronously binding its socket.
///
/// The caller owns readiness semantics through `is_ready`; this module only
/// provides the shared transport and the fixed `/metrics`, `/health`, and
/// `/ready` routes.
pub fn spawn_service_http(
    bind: SocketAddr,
    metrics: PrometheusHandle,
    is_ready: Arc<dyn Fn() -> bool + Send + Sync>,
) -> io::Result<tokio::task::JoinHandle<()>> {
    let listener = std::net::TcpListener::bind(bind)?;
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;
    let local_addr = listener.local_addr()?;

    tracing::info!(address = %local_addr, "HTTP service started");
    Ok(tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let metrics = metrics.clone();
                    let is_ready = Arc::clone(&is_ready);
                    tokio::spawn(async move {
                        let service =
                            service_fn(move |request| handle_request(request, metrics.clone(), Arc::clone(&is_ready)));
                        if let Err(error) = http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await
                        {
                            tracing::debug!(%error, "HTTP connection closed with an error");
                        }
                    });
                }
                Err(error) => tracing::warn!(%error, "HTTP accept failed"),
            }
        }
    }))
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

    use metrics_exporter_prometheus::PrometheusBuilder;

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
}
