// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker data-plane orchestration owned by the client crate.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream;

use super::channel_pool::GrpcWorkerChannelPool;
use super::protocol::{
    build_abort_write_request, build_commit_write_request, build_open_read_stream_request,
    build_open_write_stream_request, build_sync_committed_block_request, build_tonic_request,
    build_write_stream_requests, invalid_worker_header, is_transient_worker_transport_status,
    parse_commit_write_response, parse_open_write_stream_response, parse_sync_committed_block_response,
    parse_worker_control_header, read_stream_to_bytes, validate_abort_write_response,
    validate_open_read_stream_response, validate_worker_read_result, validate_write_stream_response,
};
use super::{
    WorkerBlockSyncResult, WorkerBlockWriteHandle, WorkerCommitResult, WorkerDataClient, WorkerReadResult,
    WorkerWriteTarget,
};
use crate::cache::CacheInvalidationReason;
use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult};
use crate::metrics::{ClientMetrics, NoopClientMetrics};
use crate::planner::read_planner::PlannedBlockRead;
use crate::runtime::AttemptContext;
use types::{GroupName, WriteTarget};

#[derive(Debug)]
struct GrpcWorkerDataClient {
    channel_pool: GrpcWorkerChannelPool,
}

impl GrpcWorkerDataClient {
    fn new() -> Self {
        Self {
            channel_pool: GrpcWorkerChannelPool::new(true, 1, Arc::new(NoopClientMetrics)),
        }
    }

    fn from_config(config: &ClientConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self {
            channel_pool: GrpcWorkerChannelPool::from_config(config, metrics),
        }
    }
}

#[async_trait]
impl WorkerDataClient for GrpcWorkerDataClient {
    async fn read_block_range(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        block_read: &PlannedBlockRead,
    ) -> ClientResult<WorkerReadResult> {
        let mut last_transport_error = None;
        for worker in &block_read.workers {
            let mut client = self
                .channel_pool
                .worker_data_service_client(worker, "OpenReadStream")
                .await?;
            let request = build_open_read_stream_request(&attempt, &group_name, block_read, worker)?;
            let open_response = match client.open_read_stream(build_tonic_request(&attempt, request)).await {
                Ok(response) => response.into_inner(),
                Err(status) if is_transient_worker_transport_status(&status) => {
                    self.channel_pool
                        .invalidate_worker_channel(worker, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(ClientError::from(status));
                    continue;
                }
                Err(status) => return Err(ClientError::from(status)),
            };
            if let Err(err) = parse_worker_control_header(&attempt, open_response.header.as_ref()) {
                self.channel_pool.invalidate_on_worker_run_mismatch(worker, &err);
                return Err(err);
            }
            validate_open_read_stream_response(block_read, &open_response)?;
            let stream_id = open_response
                .stream_id
                .ok_or_else(|| invalid_worker_header("worker OK response missing stream_id"))?;
            if stream_id.high == 0 && stream_id.low == 0 {
                return Err(invalid_worker_header(
                    "worker OK response invalid stream_id: zero value",
                ));
            }
            let stream_request = proto::worker::ReadStreamRequestProto {
                stream_id: Some(stream_id),
                max_bytes: open_response.frame_size.max(1),
            };
            let mut stream = client
                .read_stream(build_tonic_request(&attempt, stream_request))
                .await
                .map_err(ClientError::from)?
                .into_inner();
            let bytes = read_stream_to_bytes(&mut stream, block_read).await?;
            return Ok(WorkerReadResult {
                bytes,
                block_stamp: open_response.block_stamp,
                committed_length: open_response.committed_length,
            });
        }
        Err(last_transport_error
            .unwrap_or_else(|| ClientError::Worker("worker read has no reachable worker candidates".to_string())))
    }

    async fn open_block_write(
        &self,
        attempt: AttemptContext,
        target: WorkerWriteTarget,
    ) -> ClientResult<WorkerBlockWriteHandle> {
        let mut last_transport_error = None;
        for worker in &target.target.worker_endpoints {
            let mut client = self
                .channel_pool
                .worker_data_service_client(worker, "OpenWriteStream")
                .await?;
            let request = build_open_write_stream_request(&attempt, &target, worker)?;
            let response = match client.open_write_stream(build_tonic_request(&attempt, request)).await {
                Ok(response) => response.into_inner(),
                Err(status) if is_transient_worker_transport_status(&status) => {
                    self.channel_pool
                        .invalidate_worker_channel(worker, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(ClientError::UnknownOutcome(format!(
                        "worker OpenWriteStream outcome is unknown after transport status {}: {}",
                        status.code(),
                        status.message()
                    )));
                    continue;
                }
                Err(status) => return Err(ClientError::from(status)),
            };
            return parse_open_write_stream_response(&attempt, &target, worker, response)
                .inspect_err(|err| self.channel_pool.invalidate_on_worker_run_mismatch(worker, err));
        }
        Err(last_transport_error
            .unwrap_or_else(|| ClientError::Worker("worker write has no reachable worker candidates".to_string())))
    }

    async fn write_block_bytes(
        &self,
        handle: &WorkerBlockWriteHandle,
        data: Bytes,
    ) -> ClientResult<proto::worker::WriteStreamResponseProto> {
        if data.is_empty() {
            return Ok(proto::worker::WriteStreamResponseProto {
                accepted: true,
                last_acked_seq: handle.next_seq.saturating_sub(1),
                written_through: 0,
            });
        }
        let mut client = self
            .channel_pool
            .worker_data_service_client(&handle.worker, "WriteStream")
            .await?;
        let expected_written_through = data.len() as u64;
        let requests = build_write_stream_requests(handle, data)?;
        let expected_last_seq = requests
            .last()
            .map(|request| request.seq)
            .unwrap_or_else(|| handle.next_seq.saturating_sub(1));
        let response = client
            .write_stream(tonic::Request::new(stream::iter(requests)))
            .await
            .map_err(|status| {
                ClientError::UnknownOutcome(format!(
                    "worker WriteStream outcome is unknown after transport status {}: {}",
                    status.code(),
                    status.message()
                ))
            })?
            .into_inner();
        if !response.accepted {
            return Err(ClientError::UnknownOutcome(
                "worker WriteStream did not accept the submitted frames".to_string(),
            ));
        }
        validate_write_stream_response(response, expected_last_seq, expected_written_through)
    }

    async fn commit_block_write(
        &self,
        attempt: AttemptContext,
        handle: &WorkerBlockWriteHandle,
        effective_len: u64,
        commit_seq: u64,
        require_sync: bool,
    ) -> ClientResult<WorkerCommitResult> {
        let mut client = self
            .channel_pool
            .worker_data_service_client(&handle.worker, "CommitWrite")
            .await?;
        let request = build_commit_write_request(&attempt, handle, effective_len, commit_seq, require_sync)?;
        let response = client
            .commit_write(build_tonic_request(&attempt, request))
            .await
            .map_err(|status| {
                ClientError::UnknownOutcome(format!(
                    "worker CommitWrite outcome is unknown after transport status {}: {}",
                    status.code(),
                    status.message()
                ))
            })?
            .into_inner();
        parse_commit_write_response(&attempt, handle, effective_len, response)
            .inspect_err(|err| self.channel_pool.invalidate_on_worker_run_mismatch(&handle.worker, err))
    }

    async fn sync_committed_block(
        &self,
        attempt: AttemptContext,
        handle: &WorkerBlockWriteHandle,
        expected_len: u64,
    ) -> ClientResult<WorkerBlockSyncResult> {
        let mut client = self
            .channel_pool
            .worker_data_service_client(&handle.worker, "SyncCommittedBlock")
            .await?;
        let request = build_sync_committed_block_request(&attempt, handle, expected_len)?;
        let response = client
            .sync_committed_block(build_tonic_request(&attempt, request))
            .await
            .map_err(|status| {
                ClientError::UnknownOutcome(format!(
                    "worker SyncCommittedBlock outcome is unknown after transport status {}: {}",
                    status.code(),
                    status.message()
                ))
            })?
            .into_inner();
        parse_sync_committed_block_response(&attempt, handle, expected_len, response)
            .inspect_err(|err| self.channel_pool.invalidate_on_worker_run_mismatch(&handle.worker, err))
    }

    async fn abort_block_write(&self, attempt: AttemptContext, handle: &WorkerBlockWriteHandle) -> ClientResult<()> {
        let mut client = self
            .channel_pool
            .worker_data_service_client(&handle.worker, "AbortWrite")
            .await?;
        let request = build_abort_write_request(&attempt, handle)?;
        let response = client
            .abort_write(build_tonic_request(&attempt, request))
            .await
            .map_err(|status| {
                ClientError::UnknownOutcome(format!(
                    "worker AbortWrite outcome is unknown after transport status {}: {}",
                    status.code(),
                    status.message()
                ))
            })?
            .into_inner();
        validate_abort_write_response(&attempt, response)
    }
}

/// Internal worker data-plane holder used by the public facade.
#[derive(Clone)]
pub(crate) struct WorkerDataPlane {
    client: Arc<dyn WorkerDataClient>,
}

impl WorkerDataPlane {
    /// Create a worker data-plane.
    pub(crate) fn new() -> Self {
        Self::with_client(Arc::new(GrpcWorkerDataClient::new()))
    }

    /// Create a worker data-plane from client config.
    pub(crate) fn from_config(config: &ClientConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::with_client(Arc::new(GrpcWorkerDataClient::from_config(config, metrics)))
    }

    /// Create a worker data-plane around an already selected worker client implementation.
    pub(crate) fn with_client(client: Arc<dyn WorkerDataClient>) -> Self {
        Self { client }
    }

    pub(crate) async fn read_block_ranges(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        block_reads: &[PlannedBlockRead],
    ) -> ClientResult<Bytes> {
        let total_len = block_reads.iter().map(|block_read| block_read.len as usize).sum();
        let mut output = BytesMut::with_capacity(total_len);
        for block_read in block_reads {
            if block_read.block_stamp == 0 {
                return Err(ClientError::InvalidLayout(
                    "planned block read has zero block_stamp".to_string(),
                ));
            }
            let expected_end = block_read
                .file_offset
                .checked_add(block_read.len as u64)
                .ok_or_else(|| ClientError::InvalidLayout("planned block read end overflow".to_string()))?;
            if expected_end != block_read.end_file_offset {
                return Err(ClientError::InvalidLayout(
                    "planned block read coverage is inconsistent".to_string(),
                ));
            }
            let result = self
                .client
                .read_block_range(attempt.clone(), group_name.clone(), block_read)
                .await?;
            validate_worker_read_result(block_read, &result)?;
            if result.bytes.len() != block_read.len as usize {
                return Err(ClientError::Worker(format!(
                    "worker read returned {} bytes for {} byte block range",
                    result.bytes.len(),
                    block_read.len
                )));
            }
            output.extend_from_slice(&result.bytes);
        }
        Ok(output.freeze())
    }

    pub(crate) async fn open_block_write(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        target: WriteTarget,
    ) -> ClientResult<WorkerBlockWriteHandle> {
        let worker_target = WorkerWriteTarget { group_name, target };
        self.client.open_block_write(attempt, worker_target).await
    }

    pub(crate) async fn write_block_bytes(
        &self,
        handle: &WorkerBlockWriteHandle,
        data: Bytes,
    ) -> ClientResult<proto::worker::WriteStreamResponseProto> {
        self.client.write_block_bytes(handle, data).await
    }

    pub(crate) async fn commit_block_write(
        &self,
        attempt: AttemptContext,
        handle: &WorkerBlockWriteHandle,
        effective_len: u64,
        commit_seq: u64,
        require_sync: bool,
    ) -> ClientResult<WorkerCommitResult> {
        self.client
            .commit_block_write(attempt, handle, effective_len, commit_seq, require_sync)
            .await
    }

    pub(crate) async fn sync_committed_block(
        &self,
        attempt: AttemptContext,
        handle: &WorkerBlockWriteHandle,
        expected_len: u64,
    ) -> ClientResult<WorkerBlockSyncResult> {
        self.client.sync_committed_block(attempt, handle, expected_len).await
    }

    pub(crate) async fn abort_block_write(
        &self,
        attempt: AttemptContext,
        handle: &WorkerBlockWriteHandle,
    ) -> ClientResult<()> {
        self.client.abort_block_write(attempt, handle).await
    }
}

impl fmt::Debug for WorkerDataPlane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerDataPlane").finish_non_exhaustive()
    }
}

impl Default for WorkerDataPlane {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use types::{BlockId, BlockIndex, ClientId, DataHandleId, WorkerEndpointInfo, WorkerId, WorkerNetProtocol};

    use crate::runtime::{OperationContext, OperationIdentity, OperationKind};

    #[tokio::test]
    async fn data_plane_rejects_zero_block_stamp_before_worker_io() {
        let worker = Arc::new(CountingWorkerDataClient::default());
        let data_plane = WorkerDataPlane::with_client(worker.clone());
        let block_read = planned_block_read(0);

        let err = data_plane
            .read_block_ranges(data_attempt_context(), test_group_name(), &[block_read])
            .await
            .expect_err("zero stamp must fail before worker IO");

        assert!(matches!(err, ClientError::InvalidLayout(msg) if msg.contains("block_stamp")));
        assert_eq!(worker.calls.load(Ordering::Relaxed), 0);
    }

    #[derive(Default)]
    struct CountingWorkerDataClient {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl WorkerDataClient for CountingWorkerDataClient {
        async fn read_block_range(
            &self,
            _attempt: AttemptContext,
            _group_name: GroupName,
            block_read: &PlannedBlockRead,
        ) -> ClientResult<WorkerReadResult> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(WorkerReadResult {
                bytes: Bytes::from(vec![0; block_read.len as usize]),
                block_stamp: block_read.block_stamp,
                committed_length: block_read.block_offset + u64::from(block_read.len),
            })
        }

        async fn open_block_write(
            &self,
            _attempt: AttemptContext,
            target: WorkerWriteTarget,
        ) -> ClientResult<WorkerBlockWriteHandle> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(WorkerBlockWriteHandle {
                group_name: target.group_name,
                worker: worker_endpoint(),
                target: target.target,
                stream_id: proto::common::StreamIdProto { high: 1, low: 1 },
                frame_size: 1024,
                next_seq: 1,
            })
        }

        async fn write_block_bytes(
            &self,
            _handle: &WorkerBlockWriteHandle,
            data: Bytes,
        ) -> ClientResult<proto::worker::WriteStreamResponseProto> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(proto::worker::WriteStreamResponseProto {
                accepted: true,
                last_acked_seq: 1,
                written_through: data.len() as u64,
            })
        }

        async fn commit_block_write(
            &self,
            _attempt: AttemptContext,
            handle: &WorkerBlockWriteHandle,
            effective_len: u64,
            _commit_seq: u64,
            _require_sync: bool,
        ) -> ClientResult<WorkerCommitResult> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(WorkerCommitResult {
                effective_len,
                block_stamp: handle.target.block_stamp,
                written_through: effective_len,
            })
        }

        async fn sync_committed_block(
            &self,
            _attempt: AttemptContext,
            handle: &WorkerBlockWriteHandle,
            expected_len: u64,
        ) -> ClientResult<WorkerBlockSyncResult> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(WorkerBlockSyncResult {
                effective_len: expected_len,
                block_stamp: handle.target.block_stamp,
            })
        }

        async fn abort_block_write(
            &self,
            _attempt: AttemptContext,
            _handle: &WorkerBlockWriteHandle,
        ) -> ClientResult<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
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
}
