// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Process-owned gRPC listener and accepted-connection lifecycle.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::header::{HEADER_PRE_HANDLER_REJECTION, PRE_HANDLER_REJECTION_RPC_CONCURRENCY};
use http::{Request, Response};
use hyper::body::Incoming;
use hyper::rt::Executor;
use hyper::server::conn::http2;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::body::Body;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::service::Routes;
use tonic::{Code, Status};
use tower::Service;
use tower::limit::ConcurrencyLimit;

/// Largest request concurrency supported by the underlying Tokio semaphore.
pub const MAX_GRPC_CONCURRENT_REQUESTS: usize = Semaphore::MAX_PERMITS;

/// Listener IO or task failure returned to the process lifecycle owner.
#[derive(Debug, thiserror::Error)]
pub enum GrpcServerError {
    #[error("gRPC listener IO failed: {0}")]
    Io(#[from] io::Error),
    #[error("gRPC listener task failed: {0}")]
    Task(#[from] JoinError),
}

/// Request class used to reserve server concurrency for control traffic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcRequestClass {
    /// Filesystem or other public work that must not consume control reserve.
    Regular,
    /// Health and internal coordination work allowed to use reserved capacity.
    Control,
}

impl RpcRequestClass {
    fn label(self) -> &'static str {
        match self {
            Self::Regular => "regular",
            Self::Control => "control",
        }
    }
}

/// Immediate request-concurrency bounds for one gRPC server.
#[derive(Clone, Copy)]
pub struct GrpcRequestConcurrencyConfig {
    /// Low-cardinality server label used by request-concurrency metrics.
    pub server_name: &'static str,
    /// Maximum active requests across all accepted connections.
    pub max_concurrent_requests: usize,
    /// Maximum active requests sharing one accepted HTTP/2 connection.
    pub max_concurrent_requests_per_connection: usize,
    /// Portion of the server-wide maximum unavailable to regular requests.
    pub reserved_control_requests: usize,
    /// Classifies a raw gRPC path before routing or protobuf decoding.
    pub classify: fn(&str) -> RpcRequestClass,
}

/// Server-wide request-concurrency state shared by every accepted connection.
///
/// The regular semaphore is smaller than the total semaphore by the configured
/// reserve, so regular traffic cannot consume capacity kept for control work.
#[derive(Clone)]
struct GrpcRequestConcurrencyState {
    total: Arc<Semaphore>,
    regular: Arc<Semaphore>,
    server_name: &'static str,
    classify: fn(&str) -> RpcRequestClass,
}

impl GrpcRequestConcurrencyState {
    /// Validates the capacity relationships before allocating shared semaphores.
    fn new(config: GrpcRequestConcurrencyConfig) -> io::Result<Self> {
        validate_grpc_request_concurrency(config)?;
        let state = Self {
            total: Arc::new(Semaphore::new(config.max_concurrent_requests)),
            regular: Arc::new(Semaphore::new(
                config.max_concurrent_requests - config.reserved_control_requests,
            )),
            server_name: config.server_name,
            classify: config.classify,
        };
        for class in [RpcRequestClass::Regular, RpcRequestClass::Control] {
            metrics::gauge!(
                "grpc_server_requests_inflight",
                "server" => state.server_name,
                "traffic_class" => class.label()
            )
            .set(0.0);
        }
        Ok(state)
    }

    /// Builds a machine-readable gRPC overload response without calling a route.
    fn reject(&self, class: RpcRequestClass, limit_scope: &'static str) -> Response<Body> {
        metrics::counter!(
            "grpc_server_concurrency_rejections_total",
            "server" => self.server_name,
            "traffic_class" => class.label(),
            "limit_scope" => limit_scope
        )
        .increment(1);
        let mut metadata = MetadataMap::new();
        metadata.insert(
            HEADER_PRE_HANDLER_REJECTION,
            MetadataValue::from_static(PRE_HANDLER_REJECTION_RPC_CONCURRENCY),
        );
        Status::with_metadata(Code::ResourceExhausted, "gRPC request concurrency exhausted", metadata).into_http()
    }
}

/// Rejects configurations that cannot preserve both connection and reserve invariants.
fn validate_grpc_request_concurrency(config: GrpcRequestConcurrencyConfig) -> io::Result<()> {
    let invalid = |detail| io::Error::new(io::ErrorKind::InvalidInput, detail);
    if config.server_name.is_empty() {
        return Err(invalid("gRPC request-concurrency server name must not be empty"));
    }
    if config.max_concurrent_requests == 0 {
        return Err(invalid("gRPC maximum concurrent requests must be greater than zero"));
    }
    if config.max_concurrent_requests > MAX_GRPC_CONCURRENT_REQUESTS {
        return Err(invalid(
            "gRPC maximum concurrent requests exceeds the runtime semaphore maximum",
        ));
    }
    if config.max_concurrent_requests_per_connection == 0 {
        return Err(invalid(
            "gRPC maximum concurrent requests per connection must be greater than zero",
        ));
    }
    if config.max_concurrent_requests_per_connection > config.max_concurrent_requests {
        return Err(invalid(
            "gRPC maximum concurrent requests per connection must not exceed the server maximum",
        ));
    }
    if config.reserved_control_requests >= config.max_concurrent_requests {
        return Err(invalid(
            "gRPC reserved control requests must be smaller than the server maximum",
        ));
    }
    Ok(())
}

/// Owns every permit for one active request until completion or cancellation.
///
/// Dropping the handler future drops this guard, releasing connection, class,
/// and total capacity together while decrementing the active-request gauge.
struct ActiveRequestPermit {
    _connection: OwnedSemaphorePermit,
    _regular: Option<OwnedSemaphorePermit>,
    _total: OwnedSemaphorePermit,
    server_name: &'static str,
    class: RpcRequestClass,
}

impl ActiveRequestPermit {
    /// Records one active request after all required permits are acquired.
    fn new(
        connection: OwnedSemaphorePermit,
        regular: Option<OwnedSemaphorePermit>,
        total: OwnedSemaphorePermit,
        server_name: &'static str,
        class: RpcRequestClass,
    ) -> Self {
        metrics::gauge!(
            "grpc_server_requests_inflight",
            "server" => server_name,
            "traffic_class" => class.label()
        )
        .increment(1.0);
        Self {
            _connection: connection,
            _regular: regular,
            _total: total,
            server_name,
            class,
        }
    }
}

impl Drop for ActiveRequestPermit {
    fn drop(&mut self) {
        metrics::gauge!(
            "grpc_server_requests_inflight",
            "server" => self.server_name,
            "traffic_class" => self.class.label()
        )
        .decrement(1.0);
    }
}

/// Immediate concurrency-limit layer for one accepted HTTP/2 connection.
///
/// Hyper clones this service per stream. The connection semaphore remains
/// shared across those clones, while `state` shares server-wide capacity with
/// every other connection.
#[derive(Clone)]
struct GrpcConcurrencyLimitService<S> {
    inner: S,
    state: GrpcRequestConcurrencyState,
    connection: Arc<Semaphore>,
}

impl<S> GrpcConcurrencyLimitService<S> {
    /// Creates one connection-local concurrency boundary around the route service.
    fn new(inner: S, state: GrpcRequestConcurrencyState, max_concurrent_requests: usize) -> Self {
        Self {
            inner,
            state,
            connection: Arc::new(Semaphore::new(max_concurrent_requests)),
        }
    }
}

impl<S, B> Service<Request<B>> for GrpcConcurrencyLimitService<S>
where
    S: Service<Request<B>, Response = Response<Body>, Error = Infallible> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        // Concurrency checks belong in `call`: waiting in `poll_ready` would queue excess
        // streams instead of returning an immediate ResourceExhausted response.
        let class = (self.state.classify)(request.uri().path());
        let connection = match Arc::clone(&self.connection).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let response = self.state.reject(class, "connection");
                return Box::pin(async move { Ok(response) });
            }
        };
        let regular = match class {
            RpcRequestClass::Regular => match Arc::clone(&self.state.regular).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    let response = self.state.reject(class, "global");
                    return Box::pin(async move { Ok(response) });
                }
            },
            RpcRequestClass::Control => None,
        };
        let total = match Arc::clone(&self.state.total).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let response = self.state.reject(class, "global");
                return Box::pin(async move { Ok(response) });
            }
        };
        let permit = ActiveRequestPermit::new(connection, regular, total, self.state.server_name, class);
        let response = self.inner.call(request);
        Box::pin(async move {
            let result = response.await;
            drop(permit);
            result
        })
    }
}

/// Request-concurrency policy selected by the process that owns the listener.
#[derive(Clone)]
enum RequestConcurrencyPolicy {
    /// Applies no request-concurrency limit at the connection service layer.
    Unbounded,
    /// Uses Tower readiness to backpressure excess requests on each connection.
    BackpressurePerConnection { max_inflight: usize },
    /// Rejects excess requests immediately at connection or server-wide bounds.
    RejectExcess {
        state: GrpcRequestConcurrencyState,
        max_inflight_per_connection: usize,
    },
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
/// Graceful shutdown stops accepting requests and lets active requests finish. The
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
/// `max_inflight_per_connection` preserves Worker request-concurrency bounds;
/// callers that need immediate server-wide rejection use
/// [`spawn_grpc_server_with_concurrency_limits`].
pub fn spawn_grpc_server(
    bind: SocketAddr,
    routes: Routes,
    max_inflight_per_connection: Option<usize>,
) -> io::Result<GrpcServerHandle> {
    let request_policy = match max_inflight_per_connection {
        Some(max_inflight) => RequestConcurrencyPolicy::BackpressurePerConnection { max_inflight },
        None => RequestConcurrencyPolicy::Unbounded,
    };
    spawn_grpc_server_with_request_policy(bind, routes, request_policy)
}

/// Binds a gRPC listener that rejects excess requests before protobuf decoding.
pub fn spawn_grpc_server_with_concurrency_limits(
    bind: SocketAddr,
    routes: Routes,
    config: GrpcRequestConcurrencyConfig,
) -> io::Result<GrpcServerHandle> {
    let state = GrpcRequestConcurrencyState::new(config)?;
    spawn_grpc_server_with_request_policy(
        bind,
        routes,
        RequestConcurrencyPolicy::RejectExcess {
            state,
            max_inflight_per_connection: config.max_concurrent_requests_per_connection,
        },
    )
}

/// Starts the shared listener lifecycle with the selected per-connection service.
fn spawn_grpc_server_with_request_policy(
    bind: SocketAddr,
    routes: Routes,
    request_policy: RequestConcurrencyPolicy,
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
                        let request_policy = request_policy.clone();
                        let connection_shutdown = task_shutdown.child_token();
                        let request_executor = request_executor.clone();
                        connections.spawn(async move {
                            match request_policy {
                                RequestConcurrencyPolicy::BackpressurePerConnection { max_inflight } => {
                                    serve_connection(
                                        stream,
                                        ConcurrencyLimit::new(routes, max_inflight),
                                        connection_shutdown,
                                        request_executor,
                                    )
                                    .await;
                                }
                                RequestConcurrencyPolicy::Unbounded => {
                                    serve_connection(stream, routes, connection_shutdown, request_executor).await;
                                }
                                RequestConcurrencyPolicy::RejectExcess {
                                    state,
                                    max_inflight_per_connection,
                                } => {
                                    serve_connection(
                                        stream,
                                        GrpcConcurrencyLimitService::new(routes, state, max_inflight_per_connection),
                                        connection_shutdown,
                                        request_executor,
                                    )
                                    .await;
                                }
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
    use std::future::Pending;
    use std::time::Duration;

    use futures::FutureExt;

    use super::*;

    #[derive(Clone)]
    struct PendingService;

    impl<B> Service<Request<B>> for PendingService {
        type Response = Response<Body>;
        type Error = Infallible;
        type Future = Pending<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<B>) -> Self::Future {
            std::future::pending()
        }
    }

    fn classify_test_request(path: &str) -> RpcRequestClass {
        if path.starts_with("/control.") {
            RpcRequestClass::Control
        } else {
            RpcRequestClass::Regular
        }
    }

    fn concurrency_config(
        max_concurrent_requests: usize,
        max_concurrent_requests_per_connection: usize,
        reserved_control_requests: usize,
    ) -> GrpcRequestConcurrencyConfig {
        GrpcRequestConcurrencyConfig {
            server_name: "test",
            max_concurrent_requests,
            max_concurrent_requests_per_connection,
            reserved_control_requests,
            classify: classify_test_request,
        }
    }

    fn request(path: &'static str) -> Request<()> {
        Request::builder().uri(path).body(()).unwrap()
    }

    fn assert_concurrency_rejection(response: &Response<Body>) {
        let status = Status::from_header_map(response.headers()).expect("response must contain a gRPC status");
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            status
                .metadata()
                .get(HEADER_PRE_HANDLER_REJECTION)
                .expect("pre-handler rejection marker")
                .to_str()
                .unwrap(),
            PRE_HANDLER_REJECTION_RPC_CONCURRENCY
        );
    }

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

    #[tokio::test]
    async fn rejects_one_request_beyond_the_exact_global_limit() {
        let state = GrpcRequestConcurrencyState::new(concurrency_config(2, 2, 0)).unwrap();
        let mut first_connection = GrpcConcurrencyLimitService::new(PendingService, state.clone(), 2);
        let mut second_connection = GrpcConcurrencyLimitService::new(PendingService, state, 2);
        let first = first_connection.call(request("/regular.Service/First"));
        let second = first_connection.call(request("/regular.Service/Second"));

        let rejected = second_connection.call(request("/regular.Service/Third")).await.unwrap();

        assert_concurrency_rejection(&rejected);
        drop((first, second));
    }

    #[tokio::test]
    async fn per_connection_limit_does_not_consume_other_connection_capacity() {
        let state = GrpcRequestConcurrencyState::new(concurrency_config(2, 1, 0)).unwrap();
        let mut first_connection = GrpcConcurrencyLimitService::new(PendingService, state.clone(), 1);
        let mut second_connection = GrpcConcurrencyLimitService::new(PendingService, state, 1);
        let active = first_connection.call(request("/regular.Service/First"));

        let rejected = first_connection.call(request("/regular.Service/Second")).await.unwrap();
        let accepted_elsewhere = second_connection.call(request("/regular.Service/Third"));

        assert_concurrency_rejection(&rejected);
        assert!(accepted_elsewhere.now_or_never().is_none());
        drop(active);
    }

    #[tokio::test]
    async fn control_reserve_survives_regular_request_saturation() {
        let state = GrpcRequestConcurrencyState::new(concurrency_config(2, 2, 1)).unwrap();
        let mut first_connection = GrpcConcurrencyLimitService::new(PendingService, state.clone(), 2);
        let mut second_connection = GrpcConcurrencyLimitService::new(PendingService, state, 2);
        let regular = first_connection.call(request("/regular.Service/First"));

        let regular_rejected = first_connection.call(request("/regular.Service/Second")).await.unwrap();
        let control = first_connection.call(request("/control.Service/First"));
        let control_rejected = second_connection
            .call(request("/control.Service/Second"))
            .await
            .unwrap();

        assert_concurrency_rejection(&regular_rejected);
        assert_concurrency_rejection(&control_rejected);
        drop((regular, control));
    }

    #[tokio::test]
    async fn dropping_request_future_releases_all_concurrency_permits() {
        let state = GrpcRequestConcurrencyState::new(concurrency_config(1, 1, 0)).unwrap();
        let mut service = GrpcConcurrencyLimitService::new(PendingService, state, 1);
        let active = service.call(request("/regular.Service/First"));
        let rejected = service.call(request("/regular.Service/Second")).await.unwrap();
        assert_concurrency_rejection(&rejected);

        drop(active);

        assert!(service.call(request("/regular.Service/Third")).now_or_never().is_none());
    }

    #[test]
    fn invalid_concurrency_limits_are_rejected() {
        assert!(GrpcRequestConcurrencyState::new(concurrency_config(0, 1, 0)).is_err());
        assert!(GrpcRequestConcurrencyState::new(concurrency_config(2, 0, 0)).is_err());
        assert!(GrpcRequestConcurrencyState::new(concurrency_config(2, 3, 0)).is_err());
        assert!(GrpcRequestConcurrencyState::new(concurrency_config(2, 2, 2)).is_err());
        assert!(GrpcRequestConcurrencyState::new(concurrency_config(MAX_GRPC_CONCURRENT_REQUESTS, 1, 0)).is_ok());
        assert!(GrpcRequestConcurrencyState::new(concurrency_config(MAX_GRPC_CONCURRENT_REQUESTS + 1, 1, 0)).is_err());
    }
}
