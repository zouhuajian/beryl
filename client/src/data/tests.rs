// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use types::{BlockId, BlockIndex, ClientId, DataHandleId, GroupName, WorkerEndpointInfo, WorkerId, WorkerNetProtocol};

use super::{
    WorkerBlockSyncResult, WorkerBlockWriteHandle, WorkerCommitResult, WorkerDataClient, WorkerDataPlane,
    WorkerReadResult, WorkerWriteTarget,
};
use crate::error::{ClientError, ClientResult};
use crate::planner::PlannedBlockRead;
use crate::runtime::{AttemptContext, OperationContext, OperationIdentity, OperationKind};

#[tokio::test]
async fn data_plane_rejects_zero_block_stamp_before_worker_io() {
    let data_plane = WorkerDataPlane::with_client(Arc::new(NoWorkerIo));
    let block_read = planned_block_read(0);

    let err = data_plane
        .read_block_ranges(data_attempt_context(), test_group_name(), &[block_read])
        .await
        .expect_err("zero stamp must fail before worker IO");

    assert!(matches!(err, ClientError::InvalidLayout(msg) if msg.contains("block_stamp")));
}

struct NoWorkerIo;

#[async_trait]
impl WorkerDataClient for NoWorkerIo {
    async fn read_block_range(
        &self,
        _attempt: AttemptContext,
        _group_name: GroupName,
        _block_read: &PlannedBlockRead,
    ) -> ClientResult<WorkerReadResult> {
        panic!("worker read must not run")
    }

    async fn open_block_write(
        &self,
        _attempt: AttemptContext,
        _target: WorkerWriteTarget,
    ) -> ClientResult<WorkerBlockWriteHandle> {
        panic!("worker open write must not run")
    }

    async fn write_block_bytes(
        &self,
        _handle: &WorkerBlockWriteHandle,
        _data: Bytes,
    ) -> ClientResult<proto::worker::WriteStreamResponseProto> {
        panic!("worker write must not run")
    }

    async fn commit_block_write(
        &self,
        _attempt: AttemptContext,
        _handle: &WorkerBlockWriteHandle,
        _effective_len: u64,
        _commit_seq: u64,
        _require_sync: bool,
    ) -> ClientResult<WorkerCommitResult> {
        panic!("worker commit must not run")
    }

    async fn sync_committed_block(
        &self,
        _attempt: AttemptContext,
        _handle: &WorkerBlockWriteHandle,
        _expected_len: u64,
    ) -> ClientResult<WorkerBlockSyncResult> {
        panic!("worker sync must not run")
    }

    async fn abort_block_write(&self, _attempt: AttemptContext, _handle: &WorkerBlockWriteHandle) -> ClientResult<()> {
        panic!("worker abort must not run")
    }
}

fn data_attempt_context() -> AttemptContext {
    let operation = OperationContext::new(
        ClientId::new(7),
        OperationKind::WorkerReadData,
        "OpenReadStream",
        OperationIdentity::path("/alpha"),
    )
    .expect("operation context");
    AttemptContext::for_data(&operation, 0)
}

fn worker_endpoint() -> WorkerEndpointInfo {
    WorkerEndpointInfo {
        worker_id: WorkerId::new(1),
        endpoint: "127.0.0.1:19101".to_string(),
        worker_net_protocol: WorkerNetProtocol::Grpc,
        worker_run_id: "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("valid test WorkerRunId"),
    }
}

fn planned_block_read(block_stamp: u64) -> PlannedBlockRead {
    PlannedBlockRead {
        file_offset: 0,
        len: 4,
        end_file_offset: 4,
        block_id: BlockId::new(DataHandleId::new(202), BlockIndex::new(0)),
        block_offset: 0,
        workers: vec![worker_endpoint()],
        block_stamp,
        block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
        block_size: 4096,
        chunk_size: 4096,
        effective_len: 5,
    }
}

fn test_group_name() -> GroupName {
    GroupName::parse("root").unwrap()
}
