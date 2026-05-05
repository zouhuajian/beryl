// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker RPC client.

use crate::canonical::{validate_data_header_or_action, ClientAction, RetryOutcome};
use crate::error::{ClientError, ClientResult};
use crate::modes::ReadMode;
use bytes::Bytes;
use common::error::canonical::RefreshReason;
use common::header::RequestHeader;
use futures::StreamExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
use transport::net::{NetTransportConfig, NetTransportKind};
use transport::NetTransport;
use transport::{GrpcConnection, GrpcTransport, TransportError, TransportResult};
use types::chunk::{ByteRange, ChunkRef};
use types::ids::{DataHandleId, WorkerId};

/// Worker endpoint information from metadata.
#[derive(Clone, Debug)]
pub struct WorkerEndpointInfo {
    /// Worker ID.
    pub worker_id: WorkerId,
    /// Network endpoint (host:port).
    pub endpoint: String,
    /// Network transport kind (0=unspecified/grpc, 1=grpc, 2=quic, 3=rdma).
    pub net_transport_kind: i32,
    /// Worker epoch/boot_id.
    pub worker_epoch: u64,
}

impl WorkerEndpointInfo {
    /// Convert proto NetTransportKind to transport::net::NetTransportKind.
    pub fn net_transport_kind_to_transport_kind(kind: i32) -> NetTransportKind {
        match kind {
            1 => NetTransportKind::Grpc,
            2 => NetTransportKind::Quic,
            3 => NetTransportKind::Rdma,
            _ => NetTransportKind::Grpc, // Default to grpc
        }
    }

    /// Create from proto WorkerEndpointInfo.
    pub fn from_proto(proto: proto::common::WorkerEndpointInfoProto) -> Self {
        Self {
            worker_id: WorkerId::new(proto.worker_id),
            endpoint: proto.endpoint,
            net_transport_kind: proto.net_transport_kind,
            worker_epoch: proto.worker_epoch,
        }
    }
}

type MetadataRefreshFn = Box<
    dyn Fn(WorkerId) -> Pin<Box<dyn std::future::Future<Output = ClientResult<WorkerEndpointInfo>> + Send>>
        + Send
        + Sync,
>;

type FencingRefreshFn = Box<
    dyn Fn() -> Pin<Box<dyn std::future::Future<Output = ClientResult<proto::common::FencingTokenProto>> + Send>>
        + Send
        + Sync,
>;

/// Client transport enum (wraps different transport implementations).
enum ClientTransport {
    Grpc(Arc<GrpcTransport>),
}

/// Client connection enum (wraps different connection types).
#[derive(Clone)]
enum ClientConnection {
    Grpc(Arc<GrpcConnection>),
    #[cfg(test)]
    Mock(NetTransportKind),
}

/// Worker service client using transport abstraction.
pub struct WorkerClient {
    /// Transport for network communication (dynamic based on worker kind).
    transport: ClientTransport,
    /// Connection to worker (cached).
    connection: Option<ClientConnection>,
    /// Worker endpoint information.
    endpoint_info: WorkerEndpointInfo,
    /// Default transport config (for timeout/backpressure settings).
    default_config: NetTransportConfig,
}

impl WorkerClient {
    /// Create a new worker client from worker endpoint info.
    ///
    /// The transport kind is determined from endpoint_info.net_transport_kind,
    /// not from client configuration. This ensures client uses the protocol
    /// that the worker actually supports.
    pub async fn new(
        mut endpoint_info: WorkerEndpointInfo,
        default_config: Option<NetTransportConfig>,
    ) -> ClientResult<Self> {
        endpoint_info.endpoint = normalize_worker_endpoint(&endpoint_info.endpoint);
        // Use default config or create a reasonable default
        let default_config = default_config.unwrap_or_else(|| {
            NetTransportConfig::default()
                .with_connect_timeout(Duration::from_secs(5))
                .with_request_timeout(Duration::from_secs(30))
                .with_max_inflight_requests(100)
        });

        // Build transport based on worker's declared kind
        let transport_kind = WorkerEndpointInfo::net_transport_kind_to_transport_kind(endpoint_info.net_transport_kind);

        let net_config = NetTransportConfig::new(transport_kind)
            .with_connect_timeout(default_config.connect_timeout)
            .with_request_timeout(default_config.request_timeout)
            .with_max_inflight_requests(default_config.max_inflight_requests);

        // Build transport based on kind
        let transport = match transport_kind {
            NetTransportKind::Grpc => ClientTransport::Grpc(Arc::new(GrpcTransport::new(net_config))),
            other => {
                return Err(ClientError::Unimplemented(format!(
                    "Worker {} at {} requested unsupported transport {:?}",
                    endpoint_info.worker_id.as_u64(),
                    endpoint_info.endpoint,
                    other
                )));
            }
        };

        // Connect to worker
        let connection = Self::connect_transport(&transport, &endpoint_info.endpoint, &endpoint_info)
            .await
            .map_err(|e| ClientError::Worker(format!("Failed to connect via transport: {}", e)))?;

        Ok(Self {
            transport,
            connection: Some(connection),
            endpoint_info,
            default_config,
        })
    }

    /// Connect using the appropriate transport.
    async fn connect_transport(
        transport: &ClientTransport,
        endpoint: &str,
        info: &WorkerEndpointInfo,
    ) -> ClientResult<ClientConnection> {
        match transport {
            ClientTransport::Grpc(t) => {
                let conn = t
                    .connect(endpoint)
                    .await
                    .map_err(|err| Self::map_connection_error(info, endpoint, err))?;
                Ok(ClientConnection::Grpc(Arc::new(conn)))
            }
        }
    }

    /// Get or create connection to worker.
    async fn get_connection(&self) -> ClientResult<ClientConnection> {
        if let Some(conn) = &self.connection {
            Ok(conn.clone())
        } else {
            // Reconnect if connection is lost
            let connection =
                Self::connect_transport(&self.transport, &self.endpoint_info.endpoint, &self.endpoint_info)
                    .await
                    .map_err(|e| ClientError::Worker(format!("Failed to reconnect: {}", e)))?;
            Ok(connection)
        }
    }

    /// Update endpoint info (for metadata refresh scenarios).
    pub async fn update_endpoint_info(&mut self, mut new_info: WorkerEndpointInfo) -> ClientResult<()> {
        new_info.endpoint = normalize_worker_endpoint(&new_info.endpoint);
        // If transport kind changed, rebuild transport
        if new_info.net_transport_kind != self.endpoint_info.net_transport_kind {
            let transport_kind = WorkerEndpointInfo::net_transport_kind_to_transport_kind(new_info.net_transport_kind);
            let net_config = NetTransportConfig::new(transport_kind)
                .with_connect_timeout(self.default_config.connect_timeout)
                .with_request_timeout(self.default_config.request_timeout)
                .with_max_inflight_requests(self.default_config.max_inflight_requests);

            self.transport = match transport_kind {
                NetTransportKind::Grpc => ClientTransport::Grpc(Arc::new(GrpcTransport::new(net_config))),
                other => {
                    return Err(ClientError::Unimplemented(format!(
                        "Worker {} at {} requested unsupported transport {:?}",
                        new_info.worker_id.as_u64(),
                        new_info.endpoint,
                        other
                    )));
                }
            };
            // Clear old connection
            self.connection = None;
        }

        // If endpoint changed, reconnect
        if new_info.endpoint != self.endpoint_info.endpoint {
            self.connection = None;
        }

        self.endpoint_info = new_info;
        Ok(())
    }

    /// Read a chunk with automatic refresh on protocol mismatch or stale endpoint.
    #[expect(clippy::too_many_arguments, reason = "worker read API keeps wire fields explicit")]
    pub async fn read_chunk(
        &mut self,
        ctx: &RequestHeader,
        chunk: ChunkRef,
        offset_in_chunk: u32,
        len: u32,
        expected_version: Option<u64>,
        read_mode: ReadMode,
        refresh_fn: Option<MetadataRefreshFn>,
    ) -> ClientResult<(Bytes, u64)> {
        let request = proto::worker::ReadChunkRequestProto {
            chunk: Some(chunk_ref_to_proto(chunk)),
            offset_in_chunk,
            len,
            expected_version: expected_version.unwrap_or(0),
            read_mode: proto::common::ReadModeProto::from(read_mode) as i32,
        };

        // Try once, with refresh on failure
        let mut attempt = 0;
        loop {
            attempt += 1;

            // Get connection
            let connection = match self.get_connection().await {
                Ok(conn) => conn,
                Err(e) => {
                    // On connection failure, try refresh if available
                    if attempt == 1 && refresh_fn.is_some() {
                        if let Some(ref refresh) = refresh_fn {
                            if let Ok(new_info) = refresh(self.endpoint_info.worker_id).await {
                                self.handle_endpoint_refresh(new_info, ctx, "connection failure")
                                    .await?;
                                continue;
                            }
                        }
                    }
                    return Err(e);
                }
            };

            // RequestHeader is an alias for CallerContext, so we can use it directly
            let request_ctx: RequestHeader = ctx.clone();

            let result = self
                .call_with_transport("read_chunk", &request_ctx, &connection, |transport, conn| {
                    let ctx = request_ctx.clone();
                    async move { transport.call_read_chunk(conn.as_ref(), request, ctx).await }
                })
                .await;

            match result {
                Ok(response) => return self.parse_read_chunk_response("read_chunk", response, &request_ctx),
                Err(e) => {
                    if attempt == 1 && refresh_fn.is_some() && Self::should_refresh_on_error(&e) {
                        if let Some(ref refresh) = refresh_fn {
                            if let Ok(new_info) = refresh(self.endpoint_info.worker_id).await {
                                self.handle_endpoint_refresh(new_info, ctx, "protocol mismatch").await?;
                                continue;
                            }
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Handle endpoint refresh: update endpoint info and log/metrics.
    async fn handle_endpoint_refresh(
        &mut self,
        new_info: WorkerEndpointInfo,
        ctx: &RequestHeader,
        reason: &str,
    ) -> ClientResult<()> {
        let old_kind = self.endpoint_info.net_transport_kind;
        let old_endpoint = self.endpoint_info.endpoint.clone();
        let old_epoch = self.endpoint_info.worker_epoch;

        let new_kind = new_info.net_transport_kind;
        let new_endpoint = new_info.endpoint.clone();
        let new_epoch = new_info.worker_epoch;

        // Log warning with full context
        warn!(
            worker_id = self.endpoint_info.worker_id.as_u64(),
            old_kind = old_kind,
            old_endpoint = %old_endpoint,
            old_epoch = old_epoch,
            new_kind = new_kind,
            new_endpoint = %new_endpoint,
            new_epoch = new_epoch,
            request_id = ctx.client.call_id.to_string(),
            reason = reason,
            "endpoint stale / protocol mismatch refresh"
        );

        // Record counter metric for endpoint refresh
        metrics::counter!(
            "client_worker_endpoint_refresh_total",
            "worker_id" => self.endpoint_info.worker_id.as_u64().to_string(),
            "old_kind" => old_kind.to_string(),
            "new_kind" => new_kind.to_string(),
            "reason" => reason.to_string()
        )
        .increment(1);

        // Update endpoint info
        self.update_endpoint_info(new_info).await?;

        Ok(())
    }

    /// Read a file range with automatic refresh on protocol mismatch or stale endpoint.
    #[expect(clippy::too_many_arguments, reason = "worker read API keeps wire fields explicit")]
    pub async fn read_range(
        &mut self,
        ctx: &RequestHeader,
        data_handle_id: DataHandleId,
        range: ByteRange,
        prefer_worker_ids: Vec<WorkerId>,
        expected_version: Option<u64>,
        read_mode: ReadMode,
        refresh_fn: Option<MetadataRefreshFn>,
    ) -> ClientResult<(Vec<Bytes>, u64)> {
        let request = proto::worker::ReadRangeRequestProto {
            data_handle_id: data_handle_id.as_u64(),
            range: Some(byte_range_to_proto(range)),
            prefer_worker_ids: prefer_worker_ids.iter().map(|w| w.as_u64()).collect(),
            expected_version: expected_version.unwrap_or(0),
            read_mode: proto::common::ReadModeProto::from(read_mode) as i32,
        };

        // Try once, with refresh on failure
        let mut attempt = 0;
        loop {
            attempt += 1;

            // Get connection
            let connection = match self.get_connection().await {
                Ok(conn) => conn,
                Err(e) => {
                    // On connection failure, try refresh if available
                    if attempt == 1 && refresh_fn.is_some() {
                        if let Some(ref refresh) = refresh_fn {
                            if let Ok(new_info) = refresh(self.endpoint_info.worker_id).await {
                                self.handle_endpoint_refresh(new_info, ctx, "connection failure")
                                    .await?;
                                continue;
                            }
                        }
                    }
                    return Err(e);
                }
            };

            // RequestHeader is an alias for CallerContext, so we can use it directly
            let request_ctx: RequestHeader = ctx.clone();

            let stream_result = self
                .call_with_transport("read_range", &request_ctx, &connection, |transport, conn| {
                    let request = request.clone();
                    let ctx = request_ctx.clone();
                    async move { transport.call_read_range(conn.as_ref(), request, ctx).await }
                })
                .await;

            match stream_result {
                Ok(mut stream) => {
                    let mut chunks = Vec::new();
                    let mut version = 0;

                    while let Some(chunk_result) = stream.next().await {
                        let chunk = match chunk_result {
                            Ok(chunk) => chunk,
                            Err(err) => return Err(self.map_transport_error(err, "read_range", &request_ctx)),
                        };
                        let chunk_data = chunk
                            .data
                            .ok_or_else(|| self.missing_field_error("chunk.data", "read_range", &request_ctx))?;
                        chunks.push(chunk_data.data);
                        if chunk.current_version > 0 {
                            version = chunk.current_version;
                        }
                    }

                    return Ok((chunks, version));
                }
                Err(e) => {
                    if attempt == 1 && refresh_fn.is_some() && Self::should_refresh_on_error(&e) {
                        if let Some(ref refresh) = refresh_fn {
                            if let Ok(new_info) = refresh(self.endpoint_info.worker_id).await {
                                self.handle_endpoint_refresh(new_info, ctx, "protocol mismatch").await?;
                                continue;
                            }
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Write a chunk with automatic refresh on worker_epoch/fencing NEED_REFRESH.
    pub async fn write_chunk_with_refresh(
        &mut self,
        ctx: &RequestHeader,
        mut request: proto::worker::WriteChunkRequestProto,
        metadata_refresh: MetadataRefreshFn,
        fencing_refresh: Option<FencingRefreshFn>,
    ) -> ClientResult<RetryOutcome<proto::worker::WriteChunkResponseProto>> {
        if request.worker_epoch == 0 {
            request.worker_epoch = self.endpoint_info.worker_epoch;
        }

        let mut refreshes = 0;
        let mut retries = 0;
        let mut last_canonical_error = None;
        let mut attempt_request = request.clone();

        for _ in 0..2 {
            let connection = self.get_connection().await?;
            let request_ctx: RequestHeader = ctx.clone();

            let response = self
                .call_with_transport("write_chunk", &request_ctx, &connection, |transport, conn| {
                    let request = attempt_request.clone();
                    let ctx = request_ctx.clone();
                    async move { transport.call_write_chunk(conn.as_ref(), request, ctx).await }
                })
                .await?;

            match validate_data_header_or_action(response.header.as_ref()) {
                Ok(()) => {
                    return Ok(RetryOutcome {
                        result: response,
                        refreshes,
                        retries,
                        transport_retries: 0,
                        last_canonical_error,
                    });
                }
                Err(ClientAction::Refresh {
                    reason,
                    hint,
                    canonical,
                }) => {
                    last_canonical_error = Some(canonical.as_ref().clone());
                    if refreshes > 0 {
                        return Err(ClientError::from(ClientAction::Refresh {
                            reason,
                            hint,
                            canonical,
                        }));
                    }
                    refreshes = 1;

                    // Always fetch authoritative endpoint/epoch from metadata.
                    let meta_info = metadata_refresh(self.endpoint_info.worker_id).await?;
                    if let Some(epoch) = hint.worker_epoch {
                        if epoch != meta_info.worker_epoch {
                            warn!(
                                worker_id = self.endpoint_info.worker_id.as_u64(),
                                hinted_epoch = epoch,
                                meta_epoch = meta_info.worker_epoch,
                                "worker hint epoch differs from metadata; using metadata"
                            );
                        }
                    }
                    // Apply metadata result to request and cached endpoint.
                    self.handle_endpoint_refresh(meta_info.clone(), ctx, "metadata refresh")
                        .await?;
                    attempt_request.worker_epoch = meta_info.worker_epoch;

                    match reason {
                        RefreshReason::WorkerEpochMismatch => {
                            // nothing else; already refreshed metadata
                        }
                        RefreshReason::Fencing => {
                            if let Some(ref refresh_fn) = fencing_refresh {
                                let token = refresh_fn().await?;
                                attempt_request.token = Some(token);
                            } else {
                                return Err(ClientError::from(ClientAction::Refresh {
                                    reason,
                                    hint,
                                    canonical,
                                }));
                            }
                        }
                        _ => {
                            return Err(ClientError::from(ClientAction::Refresh {
                                reason,
                                hint,
                                canonical,
                            }))
                        }
                    }

                    if let Some(endpoint_hint) = hint.endpoint_hint {
                        let hint_info = WorkerEndpointInfo {
                            worker_id: WorkerId::new(endpoint_hint.worker_id),
                            endpoint: endpoint_hint.endpoint,
                            net_transport_kind: endpoint_hint.net_transport_kind,
                            worker_epoch: endpoint_hint.worker_epoch,
                        };
                        if hint_info.worker_epoch != meta_info.worker_epoch {
                            warn!(
                                worker_id = self.endpoint_info.worker_id.as_u64(),
                                hint_epoch = hint_info.worker_epoch,
                                meta_epoch = meta_info.worker_epoch,
                                "endpoint hint epoch differs from metadata; ignoring hint for persistence"
                            );
                        }
                    }

                    metrics::counter!(
                        "client_worker_refresh_total",
                        "reason" => format!("{:?}", reason),
                        "worker_id" => self.endpoint_info.worker_id.as_u64().to_string()
                    )
                    .increment(1);

                    continue;
                }
                Err(ClientAction::Retry { after_ms, canonical }) => {
                    last_canonical_error = Some(canonical.as_ref().clone());
                    if retries > 0 {
                        return Err(ClientError::from(ClientAction::Retry { after_ms, canonical }));
                    }
                    retries = 1;
                    if let Some(delay) = after_ms {
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    }
                    continue;
                }
                Err(action) => return Err(ClientError::from(action)),
            }
        }

        if let Some(canonical) = last_canonical_error {
            return Err(ClientError::from(ClientAction::Fail {
                canonical: Box::new(canonical),
            }));
        }
        Err(ClientError::Worker("write_chunk retry loop exhausted".to_string()))
    }

    #[allow(unreachable_patterns)]
    async fn call_with_transport<R, Fut, Call>(
        &self,
        rpc_name: &str,
        ctx: &RequestHeader,
        connection: &ClientConnection,
        call: Call,
    ) -> ClientResult<R>
    where
        Call: FnOnce(Arc<GrpcTransport>, Arc<GrpcConnection>) -> Fut,
        Fut: Future<Output = TransportResult<R>>,
    {
        match (&self.transport, connection) {
            (ClientTransport::Grpc(transport), ClientConnection::Grpc(conn)) => {
                call(Arc::clone(transport), Arc::clone(conn))
                    .await
                    .map_err(|err| self.map_transport_error(err, rpc_name, ctx))
            }
            _ => Err(self.mismatch_error(rpc_name, ctx, connection.kind())),
        }
    }

    fn map_transport_error(&self, err: TransportError, rpc_name: &str, ctx: &RequestHeader) -> ClientError {
        let err_desc = err.to_string();
        let call_id = ctx.client.call_id.to_string();
        let base_msg = format!(
            "rpc {} (call_id {}) to worker {} (endpoint {}) failed: {}",
            rpc_name,
            call_id,
            self.endpoint_info.worker_id.as_u64(),
            self.endpoint_info.endpoint,
            err_desc,
        );

        match err {
            TransportError::NotImplemented(_) | TransportError::NotSupported(_) => {
                ClientError::Unimplemented(format!("protocol mismatch: {}", base_msg))
            }
            _ => ClientError::Worker(base_msg),
        }
    }

    fn mismatch_error(&self, rpc_name: &str, ctx: &RequestHeader, connection_kind: NetTransportKind) -> ClientError {
        let call_id = ctx.client.call_id.to_string();
        ClientError::Worker(format!(
            "transport/connection mismatch for worker {} (endpoint {}) call {} (call_id {}): transport {:?} vs connection {:?}",
            self.endpoint_info.worker_id.as_u64(),
            self.endpoint_info.endpoint,
            rpc_name,
            call_id,
            self.transport.kind(),
            connection_kind,
        ))
    }

    fn parse_read_chunk_response(
        &self,
        rpc_name: &str,
        response: proto::worker::ReadChunkResponseProto,
        ctx: &RequestHeader,
    ) -> ClientResult<(Bytes, u64)> {
        let chunk_data = response
            .data
            .ok_or_else(|| self.missing_field_error("data", rpc_name, ctx))?;
        Ok((chunk_data.data, response.current_version))
    }

    fn missing_field_error(&self, field: &str, rpc_name: &str, ctx: &RequestHeader) -> ClientError {
        let call_id = ctx.client.call_id.to_string();
        ClientError::Worker(format!(
            "rpc {} (call_id {}) for worker {} (endpoint {}) missing field {}",
            rpc_name,
            call_id,
            self.endpoint_info.worker_id.as_u64(),
            self.endpoint_info.endpoint,
            field,
        ))
    }

    fn should_refresh_on_error(error: &ClientError) -> bool {
        match error {
            ClientError::Unimplemented(_) => true,
            ClientError::Worker(msg) => msg.contains("protocol mismatch") || msg.contains("transport mismatch"),
            _ => false,
        }
    }

    fn map_connection_error(info: &WorkerEndpointInfo, endpoint: &str, err: TransportError) -> ClientError {
        let err_desc = err.to_string();
        let base_msg = format!(
            "failed to connect to worker {} (endpoint {}): {}",
            info.worker_id.as_u64(),
            endpoint,
            err_desc,
        );
        match err {
            TransportError::NotImplemented(_) | TransportError::NotSupported(_) => {
                ClientError::Unimplemented(format!("protocol mismatch: {}", base_msg))
            }
            _ => ClientError::Worker(base_msg),
        }
    }

    /// Get worker endpoint info.
    pub fn endpoint_info(&self) -> &WorkerEndpointInfo {
        &self.endpoint_info
    }
}

fn normalize_worker_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{}", endpoint)
    }
}

impl ClientTransport {
    fn kind(&self) -> NetTransportKind {
        match self {
            ClientTransport::Grpc(_) => NetTransportKind::Grpc,
        }
    }
}

impl ClientConnection {
    fn kind(&self) -> NetTransportKind {
        match self {
            ClientConnection::Grpc(_) => NetTransportKind::Grpc,
            #[cfg(test)]
            ClientConnection::Mock(kind) => *kind,
        }
    }

    #[cfg(test)]
    fn mock(kind: NetTransportKind) -> Self {
        ClientConnection::Mock(kind)
    }
}

// Helper traits for conversions
trait DataHandleIdExt {
    fn as_u64(&self) -> u64;
}

impl DataHandleIdExt for DataHandleId {
    fn as_u64(&self) -> u64 {
        self.0
    }
}

trait WorkerIdExt {
    fn as_u64(&self) -> u64;
}

impl WorkerIdExt for WorkerId {
    fn as_u64(&self) -> u64 {
        self.0
    }
}

// Helper functions for conversions (avoid orphan rule issues)
fn chunk_ref_to_proto(chunk: ChunkRef) -> proto::common::ChunkIdProto {
    use proto::common::BlockIdProto;
    proto::common::ChunkIdProto {
        block: Some(BlockIdProto {
            data_handle_id: chunk.block_id.data_handle_id.as_u64(),
            block_index: chunk.block_id.index.as_u32(),
        }),
        chunk_index: chunk.chunk_idx,
    }
}

fn byte_range_to_proto(range: ByteRange) -> proto::common::ByteRangeProto {
    proto::common::ByteRangeProto {
        offset: range.offset,
        len: range.len,
    }
}

// Helper trait for BlockIndex
trait BlockIndexExt {
    fn as_u32(&self) -> u32;
}

impl BlockIndexExt for types::ids::BlockIndex {
    fn as_u32(&self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::net::NetTransportConfig;
    use transport::net::NetTransportKind;
    use types::ids::ClientId;

    fn build_test_client() -> WorkerClient {
        WorkerClient {
            transport: ClientTransport::Grpc(Arc::new(GrpcTransport::with_default_config())),
            connection: None,
            endpoint_info: WorkerEndpointInfo {
                worker_id: WorkerId::new(1),
                endpoint: "127.0.0.1:1234".to_string(),
                net_transport_kind: 1,
                worker_epoch: 0,
            },
            default_config: NetTransportConfig::default(),
        }
    }

    #[tokio::test]
    async fn call_with_transport_detects_mismatch() {
        let client = build_test_client();
        let ctx = RequestHeader::new(ClientId::new(1));
        let connection = ClientConnection::mock(NetTransportKind::Quic);

        let err = client
            .call_with_transport("read_chunk", &ctx, &connection, |_, _| async { Ok(()) })
            .await
            .unwrap_err();

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("transport/connection mismatch")));
    }

    #[tokio::test]
    async fn new_returns_unimplemented_for_quic() {
        let endpoint_info = WorkerEndpointInfo {
            worker_id: WorkerId::new(2),
            endpoint: "127.0.0.1:4321".to_string(),
            net_transport_kind: 2,
            worker_epoch: 1,
        };

        let result = WorkerClient::new(endpoint_info, None).await;

        assert!(matches!(result, Err(ClientError::Unimplemented(msg)) if msg.contains("unsupported transport")));
    }

    #[tokio::test]
    async fn parse_read_chunk_missing_data_errors() {
        let client = build_test_client();
        let ctx = RequestHeader::new(ClientId::new(3));

        let response = proto::worker::ReadChunkResponseProto {
            data: None,
            current_version: 0,
        };

        let err = client
            .parse_read_chunk_response("read_chunk", response, &ctx)
            .unwrap_err();

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("missing field data")));
    }
}
