// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker RPC client.

use crate::error::{ClientError, ClientResult};
use common::header::RequestHeader;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
use transport::net::{NetTransportConfig, NetTransportKind};
use transport::NetTransport;
use transport::{GrpcConnection, GrpcTransport, TransportError, TransportResult};
use types::ids::WorkerId;

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

trait WorkerIdExt {
    fn as_u64(&self) -> u64;
}

impl WorkerIdExt for WorkerId {
    fn as_u64(&self) -> u64 {
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
            .call_with_transport("open_read_stream", &ctx, &connection, |_, _| async { Ok(()) })
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
}
