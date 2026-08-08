// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Process-owned gRPC listener and accepted-connection lifecycle.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;

use http::{Request, Response};
use hyper::body::Incoming;
use hyper::rt::Executor;
use hyper::server::conn::http2;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::body::Body;
use tonic::service::Routes;
use tower::Service;
use tower::limit::ConcurrencyLimit;

/// Listener IO or task failure returned to the process lifecycle owner.
#[derive(Debug, thiserror::Error)]
pub enum GrpcServerError {
    #[error("gRPC listener IO failed: {0}")]
    Io(#[from] io::Error),
    #[error("gRPC listener task failed: {0}")]
    Task(#[from] JoinError),
}

/// Hyper executor that keeps every accepted HTTP/2 request owned by the server.
#[derive(Clone)]
struct TrackedRequestExecutor {
    tasks: TaskTracker,
    force: CancellationToken,
}

impl<F> Executor<F> for TrackedRequestExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, request: F) {
        let force = self.force.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                biased;
                _ = force.cancelled() => {}
                _ = request => {}
            }
        });
    }
}

/// Owned gRPC listener and every connection accepted during one process run.
///
/// Graceful shutdown stops admission and lets active requests finish. The
/// deadline path aborts every retained connection task and awaits all of them,
/// so tonic handlers cannot detach from the process lifecycle owner.
pub struct GrpcServerHandle {
    local_addr: SocketAddr,
    shutdown: CancellationToken,
    force: CancellationToken,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl GrpcServerHandle {
    /// Returns the socket selected by the operating system after binding.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Waits for an unexpected listener task exit without relinquishing ownership when cancelled.
    pub async fn wait(&mut self) -> Result<(), GrpcServerError> {
        let result = self.task.as_mut().expect("gRPC listener task is owned").await;
        self.task.take();
        result??;
        Ok(())
    }

    /// Drains accepted connections until `deadline`, then aborts and awaits them.
    ///
    /// Returns `true` when forced connection cancellation was required.
    pub async fn shutdown_until(mut self, deadline: Instant) -> Result<bool, GrpcServerError> {
        self.shutdown.cancel();
        let Some(mut task) = self.task.take() else {
            return Ok(false);
        };
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(result) => {
                result??;
                Ok(false)
            }
            Err(_) => {
                self.force.cancel();
                task.await??;
                Ok(true)
            }
        }
    }
}

impl Drop for GrpcServerHandle {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.force.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Binds and starts one HTTP/2 gRPC listener with tracked connections.
///
/// `max_inflight_per_connection` preserves Worker request admission bounds;
/// Metadata passes `None` to keep its existing unbounded-per-connection policy.
pub fn spawn_grpc_server(
    bind: SocketAddr,
    routes: Routes,
    max_inflight_per_connection: Option<usize>,
) -> io::Result<GrpcServerHandle> {
    let listener = std::net::TcpListener::bind(bind)?;
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;
    let local_addr = listener.local_addr()?;
    let routes = routes.prepare();
    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let force = CancellationToken::new();
    let task_force = force.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        let request_tasks = TaskTracker::new();
        let request_executor = TrackedRequestExecutor {
            tasks: request_tasks.clone(),
            force: task_force.clone(),
        };
        loop {
            tokio::select! {
                biased;
                _ = task_shutdown.cancelled() => break,
                completed = connections.join_next(), if !connections.is_empty() => {
                    log_connection_result(completed);
                }
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        if let Err(error) = stream.set_nodelay(true) {
                            tracing::warn!(%error, "Failed to enable TCP_NODELAY for gRPC connection");
                        }
                        let routes = routes.clone();
                        let connection_shutdown = task_shutdown.child_token();
                        let request_executor = request_executor.clone();
                        connections.spawn(async move {
                            match max_inflight_per_connection {
                                Some(limit) => {
                                    serve_connection(
                                        stream,
                                        ConcurrencyLimit::new(routes, limit),
                                        connection_shutdown,
                                        request_executor,
                                    )
                                    .await;
                                }
                                None => serve_connection(stream, routes, connection_shutdown, request_executor).await,
                            }
                        });
                    }
                    Err(error) if is_transient_accept_error(&error) => {
                        tracing::debug!(%error, "Transient gRPC accept failure");
                    }
                    Err(error) => {
                        task_force.cancel();
                        abort_connections(&mut connections).await;
                        request_tasks.close();
                        request_tasks.wait().await;
                        return Err(error);
                    }
                },
            }
        }

        while !connections.is_empty() {
            tokio::select! {
                biased;
                _ = task_force.cancelled() => {
                    abort_connections(&mut connections).await;
                }
                completed = connections.join_next() => log_connection_result(completed),
            }
        }
        request_tasks.close();
        request_tasks.wait().await;
        Ok(())
    });
    Ok(GrpcServerHandle {
        local_addr,
        shutdown,
        force,
        task: Some(task),
    })
}

/// Owns one Hyper HTTP/2 connection until completion or graceful cancellation.
async fn serve_connection<S>(
    stream: TcpStream,
    service: S,
    shutdown: CancellationToken,
    request_executor: TrackedRequestExecutor,
) where
    S: Service<Request<Incoming>, Response = Response<Body>, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    let builder = http2::Builder::new(request_executor);
    let connection = builder.serve_connection(TokioIo::new(stream), TowerToHyperService::new(service));
    tokio::pin!(connection);
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            if let Err(error) = connection.await {
                tracing::debug!(%error, "gRPC connection closed with an error");
            }
        }
        result = &mut connection => {
            if let Err(error) = result {
                tracing::debug!(%error, "gRPC connection closed with an error");
            }
        }
    }
}

/// Returns whether a socket error is safe to retry without replacing the bound listener.
fn is_transient_accept_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
    )
}

/// Aborts and reaps every accepted connection task before the server owner returns.
async fn abort_connections(connections: &mut JoinSet<()>) {
    connections.abort_all();
    while let Some(completed) = connections.join_next().await {
        log_connection_result(Some(completed));
    }
}

/// Reports connection task failures while treating owner-initiated cancellation as expected.
fn log_connection_result(completed: Option<Result<(), tokio::task::JoinError>>) {
    if let Some(Err(error)) = completed
        && !error.is_cancelled()
    {
        tracing::warn!(%error, "gRPC connection task terminated unexpectedly");
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn accept_error_policy_only_retries_transient_socket_failures() {
        for kind in [
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::Interrupted,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::TimedOut,
        ] {
            assert!(is_transient_accept_error(&io::Error::from(kind)));
        }
        for kind in [io::ErrorKind::PermissionDenied, io::ErrorKind::OutOfMemory] {
            assert!(!is_transient_accept_error(&io::Error::from(kind)));
        }
    }

    #[tokio::test]
    async fn forced_executor_cancels_and_awaits_active_request() {
        let tasks = TaskTracker::new();
        let force = CancellationToken::new();
        let executor = TrackedRequestExecutor {
            tasks: tasks.clone(),
            force: force.clone(),
        };
        executor.execute(std::future::pending::<()>());
        while tasks.is_empty() {
            tokio::task::yield_now().await;
        }

        force.cancel();
        tasks.close();
        tokio::time::timeout(Duration::from_secs(1), tasks.wait())
            .await
            .expect("forced request task must be owned and awaited");

        assert!(tasks.is_empty());
    }
}
