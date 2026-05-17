// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker RPC client.

use std::sync::Arc;
use std::time::Duration;

use crate::error::{ClientError, ClientResult};
use tonic::transport as tonic_net;
use types::ids::WorkerId;

/// Minimal client-local worker net protocol selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ClientWorkerNetProtocol {
    #[default]
    Grpc,
    Quic,
    Rdma,
}

/// Minimal client-local worker net config kept only for connection setup.
#[derive(Clone, Debug)]
pub struct ClientWorkerNetConfig {
    /// Connection establishment timeout for direct worker gRPC channels.
    pub connect_timeout: Duration,
    /// Per-request timeout applied to the tonic endpoint.
    pub request_timeout: Duration,
    /// Reserved client-side concurrency budget for future direct worker calls.
    pub max_inflight_requests: usize,
}

impl Default for ClientWorkerNetConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_inflight_requests: 100,
        }
    }
}

/// Worker endpoint information from metadata.
#[derive(Clone, Debug)]
pub struct WorkerEndpointInfo {
    /// Worker ID.
    pub worker_id: WorkerId,
    /// Network endpoint (host:port).
    pub endpoint: String,
    /// Worker network protocol (0=unspecified/grpc, 1=grpc, 2=quic, 3=rdma).
    pub worker_net_protocol: i32,
    /// Worker epoch/boot_id.
    pub worker_epoch: u64,
}

impl WorkerEndpointInfo {
    /// Convert proto worker net kind to the client-local protocol enum.
    pub(crate) fn worker_net_protocol_to_protocol(kind: i32) -> ClientWorkerNetProtocol {
        match kind {
            1 => ClientWorkerNetProtocol::Grpc,
            2 => ClientWorkerNetProtocol::Quic,
            3 => ClientWorkerNetProtocol::Rdma,
            _ => ClientWorkerNetProtocol::Grpc,
        }
    }

    /// Create from proto WorkerEndpointInfo.
    pub fn from_proto(proto: proto::common::WorkerEndpointInfoProto) -> Self {
        Self {
            worker_id: WorkerId::new(proto.worker_id),
            endpoint: proto.endpoint,
            worker_net_protocol: proto.worker_net_protocol,
            worker_epoch: proto.worker_epoch,
        }
    }
}

#[derive(Clone)]
enum ClientConnection {
    Grpc(Arc<tonic_net::Channel>),
    #[cfg(test)]
    Mock(ClientWorkerNetProtocol),
}

/// Worker service client with only minimal connection handling.
pub struct WorkerClient {
    protocol: ClientWorkerNetProtocol,
    connection: Option<ClientConnection>,
    endpoint_info: WorkerEndpointInfo,
    default_config: ClientWorkerNetConfig,
}

impl WorkerClient {
    /// Create a new worker client from worker endpoint info.
    ///
    /// The protocol is determined from endpoint_info.worker_net_protocol, not
    /// from client configuration, so the client honors metadata authority.
    pub async fn new(
        mut endpoint_info: WorkerEndpointInfo,
        default_config: Option<ClientWorkerNetConfig>,
    ) -> ClientResult<Self> {
        endpoint_info.endpoint = normalize_worker_endpoint(&endpoint_info.endpoint);
        let default_config = default_config.unwrap_or_default();
        let protocol = WorkerEndpointInfo::worker_net_protocol_to_protocol(endpoint_info.worker_net_protocol);
        let connection = match protocol {
            ClientWorkerNetProtocol::Grpc => Some(ClientConnection::Grpc(Arc::new(
                connect_grpc(&endpoint_info, &default_config).await?,
            ))),
            ClientWorkerNetProtocol::Quic | ClientWorkerNetProtocol::Rdma => {
                return Err(unsupported_protocol_error(&endpoint_info, protocol));
            }
        };

        Ok(Self {
            protocol,
            connection,
            endpoint_info,
            default_config,
        })
    }

    /// Update endpoint info after a metadata refresh.
    pub async fn update_endpoint_info(&mut self, mut new_info: WorkerEndpointInfo) -> ClientResult<()> {
        new_info.endpoint = normalize_worker_endpoint(&new_info.endpoint);
        let new_protocol = WorkerEndpointInfo::worker_net_protocol_to_protocol(new_info.worker_net_protocol);
        match new_protocol {
            ClientWorkerNetProtocol::Grpc => {}
            ClientWorkerNetProtocol::Quic | ClientWorkerNetProtocol::Rdma => {
                return Err(unsupported_protocol_error(&new_info, new_protocol));
            }
        }

        if new_protocol != self.protocol || new_info.endpoint != self.endpoint_info.endpoint {
            self.connection = Some(ClientConnection::Grpc(Arc::new(
                connect_grpc(&new_info, &self.default_config).await?,
            )));
            self.protocol = new_protocol;
        }

        self.endpoint_info = new_info;
        Ok(())
    }

    /// Get worker endpoint info.
    pub fn endpoint_info(&self) -> &WorkerEndpointInfo {
        &self.endpoint_info
    }

    /// Returns true when a connection has been established.
    pub fn is_connected(&self) -> bool {
        self.connection.as_ref().map(ClientConnection::protocol).is_some()
    }

    #[cfg(test)]
    fn ensure_connection_protocol(&self, rpc_name: &str) -> ClientResult<()> {
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| ClientError::Worker(format!("worker {} has no active connection", self.worker_id_raw())))?;
        let connection_protocol = connection.protocol();
        if connection_protocol == self.protocol {
            Ok(())
        } else {
            Err(ClientError::Worker(format!(
                "protocol/connection mismatch for worker {} (endpoint {}) call {}: protocol {:?} vs connection {:?}",
                self.worker_id_raw(),
                self.endpoint_info.endpoint,
                rpc_name,
                self.protocol,
                connection_protocol,
            )))
        }
    }

    #[cfg(test)]
    fn worker_id_raw(&self) -> u64 {
        self.endpoint_info.worker_id.0
    }
}

impl ClientConnection {
    fn protocol(&self) -> ClientWorkerNetProtocol {
        match self {
            ClientConnection::Grpc(channel) => {
                let _ = Arc::strong_count(channel);
                ClientWorkerNetProtocol::Grpc
            }
            #[cfg(test)]
            ClientConnection::Mock(protocol) => *protocol,
        }
    }

    #[cfg(test)]
    fn mock(protocol: ClientWorkerNetProtocol) -> Self {
        ClientConnection::Mock(protocol)
    }
}

async fn connect_grpc(info: &WorkerEndpointInfo, config: &ClientWorkerNetConfig) -> ClientResult<tonic_net::Channel> {
    let endpoint = tonic_net::Endpoint::from_shared(info.endpoint.clone())
        .map_err(|err| ClientError::Worker(format!("invalid worker endpoint {}: {}", info.endpoint, err)))?
        .connect_timeout(config.connect_timeout)
        .timeout(config.request_timeout);

    endpoint.connect().await.map_err(|err| {
        ClientError::Worker(format!(
            "failed to connect to worker {} (endpoint {}): {}",
            info.worker_id.0, info.endpoint, err
        ))
    })
}

fn unsupported_protocol_error(info: &WorkerEndpointInfo, protocol: ClientWorkerNetProtocol) -> ClientError {
    ClientError::Unimplemented(format!(
        "Worker {} at {} requested unsupported worker net protocol {:?}",
        info.worker_id.0, info.endpoint, protocol
    ))
}

fn normalize_worker_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_test_client(connection_protocol: ClientWorkerNetProtocol) -> WorkerClient {
        WorkerClient {
            protocol: ClientWorkerNetProtocol::Grpc,
            connection: Some(ClientConnection::mock(connection_protocol)),
            endpoint_info: WorkerEndpointInfo {
                worker_id: WorkerId::new(1),
                endpoint: "127.0.0.1:1234".to_string(),
                worker_net_protocol: 1,
                worker_epoch: 0,
            },
            default_config: ClientWorkerNetConfig::default(),
        }
    }

    #[test]
    fn connection_protocol_mismatch_is_reported() {
        let client = build_test_client(ClientWorkerNetProtocol::Quic);

        let err = client.ensure_connection_protocol("open_read_stream").unwrap_err();

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("protocol/connection mismatch")));
    }

    #[tokio::test]
    async fn new_returns_unimplemented_for_quic() {
        let endpoint_info = WorkerEndpointInfo {
            worker_id: WorkerId::new(2),
            endpoint: "127.0.0.1:4321".to_string(),
            worker_net_protocol: 2,
            worker_epoch: 1,
        };

        let result = WorkerClient::new(endpoint_info, None).await;

        assert!(
            matches!(result, Err(ClientError::Unimplemented(msg)) if msg.contains("unsupported worker net protocol"))
        );
    }
}
