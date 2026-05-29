// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! gRPC worker peer client.

use async_trait::async_trait;
use common::header::RequestHeader;
use types::ids::StreamId;

use crate::data::core::{
    AbortWriteRequest, AbortWriteResult, CommitWriteRequest, CommitWriteResult, ReadFrame, ReadOpenRequest,
    ReadOpenResult, SyncCommittedBlockRequest, SyncCommittedBlockResult, WorkerCoreResult, WriteFrame,
    WriteFrameResult, WriteOpenRequest, WriteOpenResult,
};
use crate::error::WorkerError;
use crate::net::endpoint::WorkerNetEndpoint;
use crate::net::peer::client::WorkerPeerClient;

/// gRPC implementation of the worker-internal peer client.
#[derive(Clone, Debug, Default)]
pub struct GrpcWorkerPeerClient;

impl GrpcWorkerPeerClient {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl WorkerPeerClient for GrpcWorkerPeerClient {
    async fn open_read(
        &self,
        _endpoint: &WorkerNetEndpoint,
        _req: ReadOpenRequest,
        _ctx: RequestHeader,
    ) -> WorkerCoreResult<ReadOpenResult> {
        Err(peer_grpc_unimplemented("open_read"))
    }

    async fn read_stream(
        &self,
        _endpoint: &WorkerNetEndpoint,
        _stream_id: StreamId,
        _max_bytes: u32,
        _ctx: RequestHeader,
    ) -> WorkerCoreResult<Vec<ReadFrame>> {
        Err(peer_grpc_unimplemented("read_stream"))
    }

    async fn open_write(
        &self,
        _endpoint: &WorkerNetEndpoint,
        _req: WriteOpenRequest,
        _ctx: RequestHeader,
    ) -> WorkerCoreResult<WriteOpenResult> {
        Err(peer_grpc_unimplemented("open_write"))
    }

    async fn write_stream(
        &self,
        _endpoint: &WorkerNetEndpoint,
        _frame: WriteFrame,
        _ctx: RequestHeader,
    ) -> WorkerCoreResult<WriteFrameResult> {
        Err(peer_grpc_unimplemented("write_stream"))
    }

    async fn commit_write(
        &self,
        _endpoint: &WorkerNetEndpoint,
        _req: CommitWriteRequest,
        _ctx: RequestHeader,
    ) -> WorkerCoreResult<CommitWriteResult> {
        Err(peer_grpc_unimplemented("commit_write"))
    }

    async fn sync_committed_block(
        &self,
        _endpoint: &WorkerNetEndpoint,
        _req: SyncCommittedBlockRequest,
        _ctx: RequestHeader,
    ) -> WorkerCoreResult<SyncCommittedBlockResult> {
        Err(peer_grpc_unimplemented("sync_committed_block"))
    }

    async fn abort_write(
        &self,
        _endpoint: &WorkerNetEndpoint,
        _req: AbortWriteRequest,
        _ctx: RequestHeader,
    ) -> WorkerCoreResult<AbortWriteResult> {
        Err(peer_grpc_unimplemented("abort_write"))
    }
}

fn peer_grpc_unimplemented(operation: &str) -> WorkerError {
    WorkerError::Unimplemented(format!(
        "worker peer gRPC client {operation} is not enabled/implemented yet; structured remote error mapping must be implemented before use"
    ))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use common::header::RequestHeader;
    use types::chunk::ByteRange;
    use types::ids::{BlockId, ClientId, ShardGroupId, StreamId};
    use types::lease::FencingToken;

    use super::*;
    use crate::data::core::{
        AbortWriteRequest, CommitWriteRequest, SyncCommittedBlockRequest, WriteFrame, WriteOpenRequest,
    };
    use crate::net::capability::WorkerNetCapabilities;
    use crate::net::endpoint::WorkerEndpointRole;
    use crate::net::protocol::WorkerNetProtocol;
    use crate::store::block::ChecksumKind;

    #[tokio::test]
    async fn grpc_peer_client_methods_are_explicitly_unimplemented() {
        let client = GrpcWorkerPeerClient::new();
        let endpoint = peer_endpoint();
        let header = RequestHeader::new(ClientId::new(42));
        let block_id = BlockId::from_u64_u32(7, 0);
        let token = FencingToken::new(block_id, ClientId::new(42), 1);

        assert_peer_grpc_unimplemented(
            client
                .open_read(
                    &endpoint,
                    ReadOpenRequest {
                        group_id: ShardGroupId::new(3),
                        block_id,
                        worker_run_id: test_worker_run_id(),
                        byte_range: ByteRange { offset: 0, len: 1024 },
                        block_stamp: 1,
                        block_format_id: types::BlockFormatId::FULL_EFFECTIVE,
                        block_size: 4096,
                        chunk_size: 1024,
                        effective_block_len: 4096,
                        frame_size: 1024,
                    },
                    header.clone(),
                )
                .await
                .unwrap_err(),
        );
        assert_peer_grpc_unimplemented(
            client
                .read_stream(&endpoint, StreamId::new(11), 1024, header.clone())
                .await
                .unwrap_err(),
        );
        assert_peer_grpc_unimplemented(
            client
                .open_write(
                    &endpoint,
                    WriteOpenRequest {
                        group_id: ShardGroupId::new(3),
                        block_id,
                        worker_run_id: test_worker_run_id(),
                        token,
                        block_stamp: 1,
                        frame_size: 1024,
                        block_size: 4096,
                        block_format_id: types::BlockFormatId::FULL_EFFECTIVE,
                        chunk_size: 1024,
                        effective_block_len: 4096,
                        checksum_kind: ChecksumKind::None,
                    },
                    header.clone(),
                )
                .await
                .unwrap_err(),
        );
        assert_peer_grpc_unimplemented(
            client
                .write_stream(
                    &endpoint,
                    WriteFrame {
                        stream_id: StreamId::new(11),
                        seq: 1,
                        offset_in_block: 0,
                        data: Bytes::from_static(b"data"),
                        checksum32: 0,
                    },
                    header.clone(),
                )
                .await
                .unwrap_err(),
        );
        assert_peer_grpc_unimplemented(
            client
                .commit_write(
                    &endpoint,
                    CommitWriteRequest {
                        stream_id: StreamId::new(11),
                        group_id: ShardGroupId::new(3),
                        block_id,
                        worker_run_id: test_worker_run_id(),
                        token,
                        commit_seq: 1,
                        effective_block_len: 4,
                        block_stamp: 1,
                        block_format_id: types::BlockFormatId::FULL_EFFECTIVE,
                        block_size: 4096,
                        chunk_size: 1024,
                        require_sync: false,
                    },
                    header.clone(),
                )
                .await
                .unwrap_err(),
        );
        assert_peer_grpc_unimplemented(
            client
                .sync_committed_block(
                    &endpoint,
                    SyncCommittedBlockRequest {
                        group_id: ShardGroupId::new(3),
                        block_id,
                        worker_run_id: test_worker_run_id(),
                        block_stamp: 1,
                        expected_block_len: 4,
                        block_format_id: types::BlockFormatId::FULL_EFFECTIVE,
                        block_size: 4096,
                        chunk_size: 1024,
                    },
                    header.clone(),
                )
                .await
                .unwrap_err(),
        );
        assert_peer_grpc_unimplemented(
            client
                .abort_write(
                    &endpoint,
                    AbortWriteRequest {
                        stream_id: StreamId::new(11),
                        group_id: ShardGroupId::new(3),
                        block_id,
                        token,
                    },
                    header,
                )
                .await
                .unwrap_err(),
        );
    }

    fn peer_endpoint() -> WorkerNetEndpoint {
        WorkerNetEndpoint {
            protocol: WorkerNetProtocol::Grpc,
            endpoint: "127.0.0.1:1".to_string(),
            role: WorkerEndpointRole::PeerData,
            priority: 0,
            capabilities: WorkerNetCapabilities::default(),
            worker_epoch: 1,
        }
    }

    fn test_worker_run_id() -> types::WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
    }

    fn assert_peer_grpc_unimplemented(error: WorkerError) {
        let WorkerError::Unimplemented(message) = error else {
            panic!("expected peer gRPC unimplemented error");
        };
        assert!(message.contains("worker peer gRPC client"));
        assert!(message.contains("not enabled/implemented yet"));
        assert!(message.contains("structured remote error mapping"));
    }
}
