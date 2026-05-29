// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker peer client router.

use async_trait::async_trait;
use common::header::RequestHeader;
use types::ids::StreamId;

use crate::data::core::{
    AbortWriteRequest, AbortWriteResult, CommitWriteRequest, CommitWriteResult, ReadFrame, ReadOpenRequest,
    ReadOpenResult, SyncCommittedBlockRequest, SyncCommittedBlockResult, WorkerCoreResult, WriteFrame,
    WriteFrameResult, WriteOpenRequest, WriteOpenResult,
};
use crate::error::WorkerError;
use crate::net::config::WorkerPeerNetConfig;
use crate::net::endpoint::WorkerNetEndpoint;
use crate::net::peer::client::WorkerPeerClient;
use crate::net::peer::grpc::GrpcWorkerPeerClient;
use crate::net::peer::quic::QuicWorkerPeerClient;
use crate::net::peer::rdma::RdmaWorkerPeerClient;
use crate::net::protocol::WorkerNetProtocol;
use crate::net::selector::WorkerPeerSelector;

/// Worker-internal router over service-specific peer clients.
#[derive(Clone, Debug)]
pub struct WorkerPeerClientRouter {
    selector: WorkerPeerSelector,
    enabled_protocols: Vec<WorkerNetProtocol>,
    grpc: Option<GrpcWorkerPeerClient>,
    quic: Option<QuicWorkerPeerClient>,
    rdma: Option<RdmaWorkerPeerClient>,
}

impl WorkerPeerClientRouter {
    pub fn new(selector: WorkerPeerSelector) -> Self {
        Self {
            selector,
            enabled_protocols: vec![WorkerNetProtocol::Grpc],
            grpc: Some(GrpcWorkerPeerClient::new()),
            quic: None,
            rdma: None,
        }
    }

    pub fn from_config(config: &WorkerPeerNetConfig) -> Self {
        Self {
            selector: WorkerPeerSelector::new(config.selection_policy),
            enabled_protocols: config.enabled_protocols.clone(),
            grpc: Some(GrpcWorkerPeerClient::new()),
            quic: None,
            rdma: None,
        }
    }

    pub fn select_endpoint<'a>(&self, endpoints: &'a [WorkerNetEndpoint]) -> Option<&'a WorkerNetEndpoint> {
        self.selector.select_enabled(endpoints, &self.enabled_protocols)
    }

    fn ensure_enabled(&self, protocol: WorkerNetProtocol, operation: &str) -> WorkerCoreResult<()> {
        if self.enabled_protocols.contains(&protocol) {
            Ok(())
        } else {
            Err(WorkerError::Unimplemented(format!(
                "worker peer protocol {protocol} is disabled for {operation}"
            )))
        }
    }

    fn grpc(&self, operation: &str) -> WorkerCoreResult<&GrpcWorkerPeerClient> {
        self.grpc
            .as_ref()
            .ok_or_else(|| WorkerError::Unimplemented(format!("worker gRPC peer client {operation} is disabled")))
    }

    fn protocol_unimplemented(&self, protocol: WorkerNetProtocol, operation: &str) -> WorkerError {
        match protocol {
            WorkerNetProtocol::Grpc => {
                WorkerError::Unimplemented(format!("worker gRPC peer client {operation} is disabled"))
            }
            WorkerNetProtocol::Quic => {
                let _ = self.quic.as_ref();
                crate::net::peer::quic::unimplemented(operation)
            }
            WorkerNetProtocol::Rdma => {
                let _ = self.rdma.as_ref();
                crate::net::peer::rdma::unimplemented(operation)
            }
        }
    }
}

impl Default for WorkerPeerClientRouter {
    fn default() -> Self {
        Self::new(WorkerPeerSelector::default())
    }
}

#[async_trait]
impl WorkerPeerClient for WorkerPeerClientRouter {
    async fn open_read(
        &self,
        endpoint: &WorkerNetEndpoint,
        req: ReadOpenRequest,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<ReadOpenResult> {
        self.ensure_enabled(endpoint.protocol, "open_read")?;
        match endpoint.protocol {
            WorkerNetProtocol::Grpc => self.grpc("open_read")?.open_read(endpoint, req, ctx).await,
            other => Err(self.protocol_unimplemented(other, "open_read")),
        }
    }

    async fn read_stream(
        &self,
        endpoint: &WorkerNetEndpoint,
        stream_id: StreamId,
        max_bytes: u32,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<Vec<ReadFrame>> {
        self.ensure_enabled(endpoint.protocol, "read_stream")?;
        match endpoint.protocol {
            WorkerNetProtocol::Grpc => {
                self.grpc("read_stream")?
                    .read_stream(endpoint, stream_id, max_bytes, ctx)
                    .await
            }
            other => Err(self.protocol_unimplemented(other, "read_stream")),
        }
    }

    async fn open_write(
        &self,
        endpoint: &WorkerNetEndpoint,
        req: WriteOpenRequest,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<WriteOpenResult> {
        self.ensure_enabled(endpoint.protocol, "open_write")?;
        match endpoint.protocol {
            WorkerNetProtocol::Grpc => self.grpc("open_write")?.open_write(endpoint, req, ctx).await,
            other => Err(self.protocol_unimplemented(other, "open_write")),
        }
    }

    async fn write_stream(
        &self,
        endpoint: &WorkerNetEndpoint,
        frame: WriteFrame,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<WriteFrameResult> {
        self.ensure_enabled(endpoint.protocol, "write_stream")?;
        match endpoint.protocol {
            WorkerNetProtocol::Grpc => self.grpc("write_stream")?.write_stream(endpoint, frame, ctx).await,
            other => Err(self.protocol_unimplemented(other, "write_stream")),
        }
    }

    async fn commit_write(
        &self,
        endpoint: &WorkerNetEndpoint,
        req: CommitWriteRequest,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<CommitWriteResult> {
        self.ensure_enabled(endpoint.protocol, "commit_write")?;
        match endpoint.protocol {
            WorkerNetProtocol::Grpc => self.grpc("commit_write")?.commit_write(endpoint, req, ctx).await,
            other => Err(self.protocol_unimplemented(other, "commit_write")),
        }
    }

    async fn sync_committed_block(
        &self,
        endpoint: &WorkerNetEndpoint,
        req: SyncCommittedBlockRequest,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<SyncCommittedBlockResult> {
        self.ensure_enabled(endpoint.protocol, "sync_committed_block")?;
        match endpoint.protocol {
            WorkerNetProtocol::Grpc => {
                self.grpc("sync_committed_block")?
                    .sync_committed_block(endpoint, req, ctx)
                    .await
            }
            other => Err(self.protocol_unimplemented(other, "sync_committed_block")),
        }
    }

    async fn abort_write(
        &self,
        endpoint: &WorkerNetEndpoint,
        req: AbortWriteRequest,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<AbortWriteResult> {
        self.ensure_enabled(endpoint.protocol, "abort_write")?;
        match endpoint.protocol {
            WorkerNetProtocol::Grpc => self.grpc("abort_write")?.abort_write(endpoint, req, ctx).await,
            other => Err(self.protocol_unimplemented(other, "abort_write")),
        }
    }
}

#[cfg(test)]
mod tests {
    use common::header::RequestHeader;
    use types::chunk::ByteRange;
    use types::ids::{BlockId, ClientId, ShardGroupId};

    use super::*;
    use crate::net::capability::WorkerNetCapabilities;
    use crate::net::config::{PeerProtocolSelectionPolicy, WorkerPeerNetConfig};
    use crate::net::endpoint::WorkerEndpointRole;

    #[test]
    fn select_endpoint_ignores_disabled_protocols() {
        let router = WorkerPeerClientRouter::from_config(&WorkerPeerNetConfig {
            enabled_protocols: vec![WorkerNetProtocol::Rdma],
            selection_policy: PeerProtocolSelectionPolicy::PreferGrpc,
        });
        let endpoints = vec![
            endpoint(WorkerNetProtocol::Grpc, "127.0.0.1:9090"),
            endpoint(WorkerNetProtocol::Rdma, "127.0.0.1:9092"),
        ];

        let selected = router.select_endpoint(&endpoints).expect("enabled endpoint");

        assert_eq!(selected.protocol, WorkerNetProtocol::Rdma);
    }

    #[tokio::test]
    async fn dispatch_rejects_disabled_protocol_before_peer_client() {
        let router = WorkerPeerClientRouter::from_config(&WorkerPeerNetConfig {
            enabled_protocols: Vec::new(),
            selection_policy: PeerProtocolSelectionPolicy::PreferGrpc,
        });

        let error = router
            .open_read(
                &endpoint(WorkerNetProtocol::Grpc, "127.0.0.1:1"),
                ReadOpenRequest {
                    group_id: ShardGroupId::new(3),
                    block_id: BlockId::from_u64_u32(7, 0),
                    worker_run_id: "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
                    byte_range: ByteRange { offset: 0, len: 1024 },
                    block_stamp: 1,
                    block_format_id: types::BlockFormatId::FULL_EFFECTIVE,
                    block_size: 4096,
                    chunk_size: 1024,
                    effective_block_len: 4096,
                    frame_size: 1024,
                },
                RequestHeader::new(ClientId::new(42)),
            )
            .await
            .unwrap_err();

        let WorkerError::Unimplemented(message) = error else {
            panic!("expected disabled protocol to return unimplemented");
        };
        assert!(message.contains("grpc"));
        assert!(message.contains("disabled"));
    }

    fn endpoint(protocol: WorkerNetProtocol, endpoint: &str) -> WorkerNetEndpoint {
        WorkerNetEndpoint {
            protocol,
            endpoint: endpoint.to_string(),
            role: WorkerEndpointRole::PeerData,
            priority: 0,
            capabilities: WorkerNetCapabilities::default(),
            worker_epoch: 1,
        }
    }
}
