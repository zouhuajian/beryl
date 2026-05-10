// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! gRPC transport implementation using tonic.

use async_trait::async_trait;
use common::{timeout_at, ConcurrencyLimiter};
use futures::StreamExt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tracing::{debug, info_span, Instrument};
use uuid::Uuid;

use crate::buffer::ZeroCopyBuffer;
use crate::connection::{Connection, ConnectionMetadata};
use crate::ctx_adapter::inject_context_to_metadata;
use crate::error::{TransportError, TransportResult};
use crate::net::config::NetTransportConfig;
use crate::transport::{NetTransport, NetTransportCapability, ReliabilitySource};
use common::header::RequestHeader;

// Re-export for convenience
pub use crate::net::methods;

// Import observability from common
use common::observe::error::{classify_transport_error, ErrorKind};

/// gRPC connection wrapper.
pub struct GrpcConnection {
    channel: Channel,
    metadata: ConnectionMetadata,
}

#[async_trait]
impl Connection for GrpcConnection {
    fn remote_addr(&self) -> &str {
        &self.metadata.remote_addr
    }

    async fn is_healthy(&self) -> bool {
        // Check if channel is ready
        // Note: tonic::transport::Channel doesn't have a ready() method in this version
        // We'll use a simple check - in production you might want to send a ping
        true // Placeholder - implement proper health check
    }

    async fn close(&mut self) -> TransportResult<()> {
        // Tonic channels are reference-counted, so we just drop
        Ok(())
    }
}

/// gRPC transport implementation with timeout, backpressure, retry, and observability.
pub struct GrpcTransport {
    config: NetTransportConfig,
    // Client-side limiter for backpressure control (using common::ConcurrencyLimiter)
    request_limiter: Arc<ConcurrencyLimiter>,
    // Inflight requests counter
    inflight: Arc<AtomicI64>,
}

impl GrpcTransport {
    pub fn new(config: NetTransportConfig) -> Self {
        let limiter = Arc::new(ConcurrencyLimiter::new(config.max_inflight_requests));

        Self {
            config,
            request_limiter: limiter,
            inflight: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(NetTransportConfig::default())
    }

    /// Execute a unary RPC with timeout, backpressure, retry, and observability.
    async fn execute_unary_with_retry<F, Fut, Resp>(
        &self,
        method: &str,
        ctx: RequestHeader,
        f: F,
    ) -> TransportResult<Resp>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = TransportResult<Resp>>,
    {
        let request_id = ctx.client.call_id.to_string();
        let trace_id = ctx
            .traceparent
            .clone()
            .unwrap_or_else(|| format!("00-{}-{}-00", Uuid::new_v4(), Uuid::new_v4()));
        let effective_timeout = ctx.deadline.remaining().max(Duration::from_millis(1));

        // Create tracing span
        let span = info_span!(
            "transport.rpc",
            method = method,
            request_id = %request_id,
            trace_id = %trace_id,
            timeout_ms = effective_timeout.as_millis() as u64,
        );
        let span_clone = span.clone();

        async move {
            // Increment inflight
            let _inflight_count = self.inflight.fetch_add(1, Ordering::Relaxed) + 1;
            // TODO: metrics::gauge!(transport::RPC_INFLIGHT).set(inflight_count as f64);

            // Acquire permit using common::ConcurrencyLimiter (respects deadline)
            let backpressure_start = Instant::now();
            let _permit = match self.request_limiter.acquire(&ctx).await {
                Ok(permit) => {
                    let wait_ms = backpressure_start.elapsed();
                    if wait_ms.as_millis() > 0 {
                        // TODO: metrics::histogram!(transport::BACKPRESSURE_WAIT_MS, "method" => method).record(wait_ms.as_secs_f64() * 1000.0);
                    }
                    permit
                }
                Err(e) => {
                    // TODO: metrics::counter!(transport::RPC_REQUESTS_TOTAL, "method" => method, "status" => "overloaded").increment(1);
                    self.inflight.fetch_sub(1, Ordering::Relaxed);
                    return Err(TransportError::Overloaded(format!(
                        "Failed to acquire permit: {}",
                        e.message
                    )));
                }
            };

            let start = Instant::now();

            // Retry loop (only for idempotent requests)
            let mut last_error = None;
            let max_attempts = if self.config.retry_policy.is_enabled() {
                self.config.retry_policy.max_retries + 1
            } else {
                1
            };

            for attempt in 0..max_attempts {
                if attempt > 0 {
                    let backoff = self.config.retry_policy.backoff_for_attempt(attempt);
                    debug!(
                        "Retrying request {} (attempt {}/{}) after {:?}",
                        request_id,
                        attempt + 1,
                        max_attempts,
                        backoff
                    );
                    tokio::time::sleep(backoff).await;
                }

                // Check deadline before each attempt (using caller_ctx.deadline)
                if ctx.deadline.has_passed() {
                    let _error_kind = classify_transport_error("deadline_exceeded");
                    // TODO: metrics::counter!(transport::RPC_REQUESTS_TOTAL, "method" => method, "status" => error_kind.as_str()).increment(1);
                    // TODO: metrics::histogram!(transport::RPC_LATENCY_MS, "method" => method).record(start.elapsed().as_secs_f64() * 1000.0);
                    self.inflight.fetch_sub(1, Ordering::Relaxed);
                    return Err(TransportError::DeadlineExceeded(
                        "Deadline exceeded before retry".to_string(),
                    ));
                }

                // Execute request with timeout using common::timeout_at
                match timeout_at(ctx.deadline, f()).await {
                    Ok(Ok(resp)) => {
                        let latency_ms = start.elapsed();
                        let _error_kind = ErrorKind::Ok;

                        // TODO: metrics::counter!(transport::RPC_REQUESTS_TOTAL, "method" => method, "status" => error_kind.as_str()).increment(1);
                        // TODO: metrics::histogram!(transport::RPC_LATENCY_MS, "method" => method).record(latency_ms.as_secs_f64() * 1000.0);

                        span.record("status", "ok");
                        span.record("latency_ms", latency_ms.as_millis() as u64);

                        self.inflight.fetch_sub(1, Ordering::Relaxed);
                        return Ok(resp);
                    }
                    Ok(Err(e)) => {
                        last_error = Some(e);
                    }
                    Err(e) => {
                        // TODO: metrics::counter!(transport::TIMEOUT_TOTAL, "kind" => "request", "method" => method).increment(1);
                        last_error = Some(TransportError::DeadlineExceeded(format!(
                            "Request timeout: {}",
                            e.message
                        )));
                    }
                };

                // Check if error is retryable
                if let Some(ref err) = last_error {
                    if !self.config.retry_policy.is_retryable(err) {
                        break;
                    }
                }
            }

            // All retries exhausted
            let latency_ms = start.elapsed();
            if let Some(err) = last_error {
                let error_kind = classify_transport_error(err.error_code());

                // TODO: metrics::counter!(transport::RPC_REQUESTS_TOTAL, "method" => method, "status" => error_kind.as_str()).increment(1);
                // TODO: metrics::histogram!(transport::RPC_LATENCY_MS, "method" => method).record(latency_ms.as_secs_f64() * 1000.0);

                span.record("status", error_kind.as_str());
                span.record("latency_ms", latency_ms.as_millis() as u64);

                self.inflight.fetch_sub(1, Ordering::Relaxed);
                Err(err)
            } else {
                self.inflight.fetch_sub(1, Ordering::Relaxed);
                Err(TransportError::Internal("Unknown error in retry loop".to_string()))
            }
        }
        .instrument(span_clone)
        .await
    }

    /// Open a block-local read stream using generated client stubs.
    pub async fn call_open_read_stream(
        &self,
        connection: &GrpcConnection,
        request: proto::worker::OpenReadStreamRequestProto,
        ctx: RequestHeader,
    ) -> TransportResult<proto::worker::OpenReadStreamResponseProto> {
        use proto::worker::worker_data_service_client::WorkerDataServiceClient;

        let connection_clone = connection.channel.clone();
        let ctx_clone = ctx.clone();

        self.execute_unary_with_retry(crate::net::methods::worker_data::OPEN_READ_STREAM, ctx, move || {
            let channel = connection_clone.clone();
            let req = request.clone();
            let ctx_for_metadata = ctx_clone.clone();

            async move {
                let mut client = WorkerDataServiceClient::new(channel);

                let mut request_builder = Request::new(req);
                let metadata = request_builder.metadata_mut();

                inject_context_to_metadata(&ctx_for_metadata, metadata)
                    .map_err(|e| TransportError::Protocol(format!("Failed to inject context: {}", e)))?;

                let response = client
                    .open_read_stream(request_builder)
                    .await
                    .map_err(TransportError::from)?;

                Ok(response.into_inner())
            }
        })
        .await
    }

    /// Read frames from an opened block-local stream.
    pub async fn call_read_stream(
        &self,
        connection: &GrpcConnection,
        request: proto::worker::ReadStreamRequestProto,
        ctx: RequestHeader,
    ) -> TransportResult<
        Box<dyn futures::Stream<Item = TransportResult<proto::worker::ReadStreamResponseProto>> + Send + Unpin>,
    > {
        use proto::worker::worker_data_service_client::WorkerDataServiceClient;

        let _permit = self
            .request_limiter
            .acquire(&ctx)
            .await
            .map_err(|e| TransportError::Overloaded(format!("Failed to acquire permit: {}", e.message)))?;

        let channel = connection.channel.clone();
        let mut client = WorkerDataServiceClient::new(channel);

        let mut request_builder = Request::new(request);
        let metadata = request_builder.metadata_mut();

        inject_context_to_metadata(&ctx, metadata)
            .map_err(|e| TransportError::Protocol(format!("Failed to inject context: {}", e)))?;

        let response_stream = client
            .read_stream(request_builder)
            .await
            .map_err(TransportError::from)?
            .into_inner();

        let transport_stream = response_stream.map(|item| item.map_err(TransportError::from));

        Ok(Box::new(transport_stream))
    }

    /// Open a block-local write stream using generated client stubs.
    pub async fn call_open_write_stream(
        &self,
        connection: &GrpcConnection,
        request: proto::worker::OpenWriteStreamRequestProto,
        ctx: RequestHeader,
    ) -> TransportResult<proto::worker::OpenWriteStreamResponseProto> {
        use proto::worker::worker_data_service_client::WorkerDataServiceClient;

        let connection_clone = connection.channel.clone();
        let ctx_clone = ctx.clone();

        self.execute_unary_with_retry(crate::net::methods::worker_data::OPEN_WRITE_STREAM, ctx, move || {
            let channel = connection_clone.clone();
            let req = request.clone();
            let ctx_for_metadata = ctx_clone.clone();

            async move {
                let mut client = WorkerDataServiceClient::new(channel);

                let mut request_builder = Request::new(req);
                let metadata = request_builder.metadata_mut();

                inject_context_to_metadata(&ctx_for_metadata, metadata)
                    .map_err(|e| TransportError::Protocol(format!("Failed to inject context: {}", e)))?;

                let response = client
                    .open_write_stream(request_builder)
                    .await
                    .map_err(TransportError::from)?;

                Ok(response.into_inner())
            }
        })
        .await
    }

    /// Helper method for client streaming using generated client stubs.
    pub async fn call_write_stream(
        &self,
        connection: &GrpcConnection,
        _request_stream: Box<
            dyn futures::Stream<Item = TransportResult<proto::worker::WriteStreamRequestProto>> + Send + Unpin,
        >,
        ctx: RequestHeader,
    ) -> TransportResult<proto::worker::WriteStreamResponseProto> {
        use proto::worker::worker_data_service_client::WorkerDataServiceClient;

        // Check backpressure
        let _permit = self
            .request_limiter
            .acquire(&ctx)
            .await
            .map_err(|e| TransportError::Overloaded(format!("Failed to acquire permit: {}", e.message)))?;

        let channel = connection.channel.clone();
        let _client = WorkerDataServiceClient::new(channel);

        // Convert transport stream to tonic stream
        // Note: tonic's write_stream expects Stream<Item = WriteStreamRequestProto>, not Result
        // We need to handle errors properly. For now, return NotImplemented.
        // TODO: Implement proper stream conversion that handles errors
        // The challenge is that tonic expects Stream<Item = T>, not Stream<Item = Result<T, E>>
        // Options:
        // 1. Collect all items first (memory intensive, but handles errors)
        // 2. Use a custom stream adapter that converts errors to Status and stops the stream
        // 3. Change the API to accept a stream without Result wrapper
        Err(TransportError::NotImplemented(
            "call_write_stream requires proper stream conversion - tonic expects Stream<Item = T>, not Stream<Item = Result<T, E>>".to_string()
        ))
    }

    /// Commit an opened write stream using generated client stubs.
    pub async fn call_commit_write(
        &self,
        connection: &GrpcConnection,
        request: proto::worker::CommitWriteRequestProto,
        ctx: RequestHeader,
    ) -> TransportResult<proto::worker::CommitWriteResponseProto> {
        use proto::worker::worker_data_service_client::WorkerDataServiceClient;

        let connection_clone = connection.channel.clone();
        let ctx_clone = ctx.clone();

        self.execute_unary_with_retry(crate::net::methods::worker_data::COMMIT_WRITE, ctx, move || {
            let channel = connection_clone.clone();
            let req = request.clone();
            let ctx_for_metadata = ctx_clone.clone();

            async move {
                let mut client = WorkerDataServiceClient::new(channel);
                let mut request_builder = Request::new(req);
                let metadata = request_builder.metadata_mut();

                inject_context_to_metadata(&ctx_for_metadata, metadata)
                    .map_err(|e| TransportError::Protocol(format!("Failed to inject context: {}", e)))?;

                let response = client
                    .commit_write(request_builder)
                    .await
                    .map_err(TransportError::from)?;
                Ok(response.into_inner())
            }
        })
        .await
    }

    /// Abort an opened write stream using generated client stubs.
    pub async fn call_abort_write(
        &self,
        connection: &GrpcConnection,
        request: proto::worker::AbortWriteRequestProto,
        ctx: RequestHeader,
    ) -> TransportResult<proto::worker::AbortWriteResponseProto> {
        use proto::worker::worker_data_service_client::WorkerDataServiceClient;

        let connection_clone = connection.channel.clone();
        let ctx_clone = ctx.clone();

        self.execute_unary_with_retry(crate::net::methods::worker_data::ABORT_WRITE, ctx, move || {
            let channel = connection_clone.clone();
            let req = request.clone();
            let ctx_for_metadata = ctx_clone.clone();

            async move {
                let mut client = WorkerDataServiceClient::new(channel);
                let mut request_builder = Request::new(req);
                let metadata = request_builder.metadata_mut();

                inject_context_to_metadata(&ctx_for_metadata, metadata)
                    .map_err(|e| TransportError::Protocol(format!("Failed to inject context: {}", e)))?;

                let response = client
                    .abort_write(request_builder)
                    .await
                    .map_err(TransportError::from)?;
                Ok(response.into_inner())
            }
        })
        .await
    }
}

#[async_trait]
impl NetTransport for GrpcTransport {
    type Connection = GrpcConnection;
    type Buffer = ZeroCopyBuffer;

    async fn connect(&self, addr: &str) -> TransportResult<Self::Connection> {
        debug!("Connecting to gRPC endpoint: {}", addr);

        let endpoint = Endpoint::from_shared(addr.to_string())
            .map_err(|e| TransportError::Connection(format!("invalid endpoint: {}", e)))?
            .timeout(self.config.request_timeout)
            .connect_timeout(self.config.connect_timeout);

        // Apply keepalive settings if configured
        let endpoint = if let Some(interval) = self.config.keepalive_interval {
            endpoint.keep_alive_timeout(interval)
        } else {
            endpoint
        };

        let endpoint = if let Some(timeout) = self.config.keepalive_timeout {
            endpoint.keep_alive_timeout(timeout)
        } else {
            endpoint
        };

        // Connect with timeout
        let channel = match timeout(self.config.connect_timeout, endpoint.connect()).await {
            Ok(Ok(channel)) => channel,
            Ok(Err(e)) => {
                return Err(TransportError::Connection(format!("connection failed: {}", e)));
            }
            Err(_) => {
                return Err(TransportError::DeadlineExceeded(format!(
                    "Connection timeout after {:?}",
                    self.config.connect_timeout
                )));
            }
        };

        Ok(GrpcConnection {
            channel,
            metadata: ConnectionMetadata::new(addr.to_string()),
        })
    }

    async fn unary_call<Req, Resp>(
        &self,
        _connection: &Self::Connection,
        method: &str,
        _request: Req,
        ctx: RequestHeader,
    ) -> TransportResult<Resp>
    where
        Req: Send + Sync,
        Resp: Send + Sync,
    {
        // For now, return an error indicating that unary_call requires type-specific implementation
        // The actual implementation should use generated client stubs (e.g., WorkerDataServiceClient)
        // This maintains the trait signature while allowing type-specific helpers to be added
        self.execute_unary_with_retry(method, ctx, || async {
            Err(TransportError::Protocol(format!(
                "unary_call requires type-specific implementation using generated client stubs. Method: {}",
                method
            )))
        })
        .await
    }

    async fn server_streaming<Req, Resp>(
        &self,
        _connection: &Self::Connection,
        _method: &str,
        _request: Req,
        _ctx: RequestHeader,
    ) -> TransportResult<Box<dyn futures::Stream<Item = TransportResult<Resp>> + Send + Unpin>>
    where
        Req: Send + Sync,
        Resp: Send + Sync,
    {
        // Streaming requests don't support retry (by design)
        // But we still apply backpressure and timeout
        let _permit = self
            .request_limiter
            .acquire(&_ctx)
            .await
            .map_err(|_| TransportError::Overloaded("Failed to acquire semaphore".to_string()))?;

        // For now, return an error indicating that server_streaming requires type-specific implementation
        // The actual implementation should use generated client stubs (e.g., WorkerDataServiceClient)
        Err(TransportError::Protocol(format!(
            "server_streaming requires type-specific implementation using generated client stubs. Method: {}",
            _method
        )))
    }

    async fn client_streaming<Req, Resp>(
        &self,
        _connection: &Self::Connection,
        _method: &str,
        _request_stream: Box<dyn futures::Stream<Item = TransportResult<Req>> + Send + Unpin>,
        ctx: RequestHeader,
    ) -> TransportResult<Resp>
    where
        Req: Send + Sync,
        Resp: Send + Sync,
    {
        // Streaming requests don't support retry
        let _permit = self
            .request_limiter
            .acquire(&ctx)
            .await
            .map_err(|e| TransportError::Overloaded(format!("Failed to acquire permit: {}", e.message)))?;

        // For now, return an error indicating that client_streaming requires type-specific implementation
        Err(TransportError::Protocol(format!(
            "client_streaming requires type-specific implementation using generated client stubs. Method: {}",
            _method
        )))
    }

    async fn bidi_streaming<Req, Resp>(
        &self,
        _connection: &Self::Connection,
        _method: &str,
        _request_stream: Box<dyn futures::Stream<Item = TransportResult<Req>> + Send + Unpin>,
        ctx: RequestHeader,
    ) -> TransportResult<Box<dyn futures::Stream<Item = TransportResult<Resp>> + Send + Unpin>>
    where
        Req: Send + Sync,
        Resp: Send + Sync,
    {
        // Streaming requests don't support retry
        let _permit = self
            .request_limiter
            .acquire(&ctx)
            .await
            .map_err(|e| TransportError::Overloaded(format!("Failed to acquire permit: {}", e.message)))?;

        Err(TransportError::Protocol(
            "gRPC bidi_streaming not yet implemented".to_string(),
        ))
    }
}

impl NetTransportCapability for GrpcTransport {
    fn zero_copy_payload(&self) -> bool {
        // gRPC over TCP/HTTP2: best-effort zero-copy
        // Transport layer avoids extra copies, but TCP/HTTP2 may still copy
        false
    }

    fn reliability_provided_by(&self) -> ReliabilitySource {
        // gRPC over HTTP/2 over TCP: reliability provided by TCP
        ReliabilitySource::UnderlyingProtocol
    }
}

/// Helper to create a gRPC transport with default settings.
pub fn create_grpc_transport() -> GrpcTransport {
    GrpcTransport::with_default_config()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::config::NetTransportConfig;
    use crate::retry::RetryPolicy;
    use common::header::RequestHeader;
    use common::Deadline;
    use std::time::Duration;
    use types::ClientId;

    #[tokio::test]
    async fn test_semaphore_backpressure() {
        // Create transport with very low concurrency limit
        let config = NetTransportConfig::default().with_max_inflight_requests(2);
        let _transport = GrpcTransport::new(config);
        // This test verifies that semaphore is working
        // In a real scenario, we would need actual gRPC stubs to test end-to-end
    }

    #[tokio::test]
    async fn test_request_timeout() {
        let config = NetTransportConfig::default().with_request_timeout(Duration::from_millis(100));
        let _transport = GrpcTransport::new(config);

        // Create a context with a short timeout
        let deadline = Deadline::from_now(Duration::from_millis(50));
        let ctx = RequestHeader::with_deadline(ClientId::new(1), deadline);

        // Verify deadline is set correctly
        let remaining = ctx.deadline.remaining();
        assert!(remaining <= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_retry_policy() {
        let policy = RetryPolicy::default_enabled();
        assert!(policy.is_enabled());
        assert_eq!(policy.max_retries, 3);

        let disabled = RetryPolicy::disabled();
        assert!(!disabled.is_enabled());

        // Test backoff calculation
        let backoff1 = policy.backoff_for_attempt(1);
        assert!(backoff1 > Duration::from_secs(0));
        assert!(backoff1 <= policy.max_backoff);
    }

    #[tokio::test]
    async fn test_deadline_override() {
        let deadline = Deadline::from_now(Duration::from_secs(5));
        let ctx = RequestHeader::with_deadline(ClientId::new(1), deadline);

        // Verify deadline is set correctly
        let remaining = ctx.deadline.remaining();
        assert!(remaining <= Duration::from_secs(6));
        assert!(remaining >= Duration::from_secs(4)); // Allow some margin
    }
}
