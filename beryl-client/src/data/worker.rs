// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker data-plane orchestration owned by the client crate.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream;

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};

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
use crate::planner::{block_location_unavailable_error, PlannedBlockRead};
use crate::runtime::{AttemptContext, ErrorClass, ErrorClassifier};
use beryl_types::{GroupName, WriteTarget};

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

    fn map_side_effect_status(
        &self,
        worker: &beryl_types::WorkerEndpointInfo,
        operation: &'static str,
        status: tonic::Status,
    ) -> ClientError {
        if is_transient_worker_transport_status(&status) {
            self.channel_pool
                .mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
        }
        ClientError::UnknownOutcome(format!(
            "worker {operation} outcome is unknown after transport status {}: {}",
            status.code(),
            status.message()
        ))
    }

    fn worker_candidates<'a>(
        &self,
        workers: &'a [beryl_types::WorkerEndpointInfo],
    ) -> Vec<&'a beryl_types::WorkerEndpointInfo> {
        let mut active = Vec::with_capacity(workers.len());
        let mut cooling = Vec::new();
        for worker in workers {
            if self.channel_pool.is_worker_cooling_down(worker) {
                cooling.push(worker);
            } else {
                active.push(worker);
            }
        }
        if !active.is_empty() {
            return active;
        }
        // Cooldown is an avoidance preference, not availability authority. If
        // metadata gives no uncool alternatives, try the cooled candidates.
        for worker in &cooling {
            self.channel_pool.clear_worker_cooldown(worker);
        }
        cooling
    }
}

fn is_stale_read_location_error(err: &ClientError) -> bool {
    match err {
        ClientError::Action(action) => match action.action() {
            crate::rpc_error::ClientAction::Refresh { rpc_error, .. } => matches!(
                rpc_error.kind,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable)
                    | ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch)
                    | ErrorKind::Worker(WorkerErrorKind::RunMismatch)
                    | ErrorKind::Metadata(MetadataErrorKind::StaleState)
                    | ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch)
                    | ErrorKind::Worker(WorkerErrorKind::FullReportRequired)
                    | ErrorKind::Worker(WorkerErrorKind::NotRegistered)
            ),
            _ => false,
        },
        _ => false,
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
        if block_read.workers.is_empty() {
            return Err(block_location_unavailable_error(format!(
                "block location unavailable: no worker candidates for block {} file_offset={} len={} block_stamp={}",
                block_read.block_id, block_read.file_offset, block_read.len, block_read.block_stamp
            )));
        }
        let mut last_transport_error = None;
        let mut last_location_error = None;
        for worker in self.worker_candidates(&block_read.workers) {
            let mut client = self.channel_pool.worker_data_service_client(worker, "OpenReadStream")?;
            let request = build_open_read_stream_request(&attempt, &group_name, block_read, worker)?;
            let open_response = match client.open_read_stream(build_tonic_request(&attempt, request)).await {
                Ok(response) => response.into_inner(),
                Err(status) if is_transient_worker_transport_status(&status) => {
                    self.channel_pool
                        .mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(ClientError::from(status));
                    continue;
                }
                Err(status) => return Err(ClientError::from(status)),
            };
            if let Err(err) = parse_worker_control_header(&attempt, open_response.header.as_ref()) {
                self.channel_pool.invalidate_on_worker_run_mismatch(worker, &err);
                if is_stale_read_location_error(&err) {
                    last_location_error = Some(err);
                    continue;
                }
                return Err(err);
            }
            if let Err(err) = validate_open_read_stream_response(block_read, &open_response) {
                if is_stale_read_location_error(&err) {
                    last_location_error = Some(err);
                    continue;
                }
                return Err(err);
            }
            let stream_id = open_response
                .stream_id
                .ok_or_else(|| invalid_worker_header("worker OK response missing stream_id"))?;
            if stream_id.high == 0 && stream_id.low == 0 {
                return Err(invalid_worker_header(
                    "worker OK response invalid stream_id: zero value",
                ));
            }
            let stream_request = beryl_proto::worker::ReadStreamRequestProto {
                stream_id: Some(stream_id),
                max_bytes: open_response.frame_size.max(1),
            };
            let mut stream = match client.read_stream(build_tonic_request(&attempt, stream_request)).await {
                Ok(response) => response.into_inner(),
                Err(status) if is_transient_worker_transport_status(&status) => {
                    self.channel_pool
                        .mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(ClientError::from(status));
                    continue;
                }
                Err(status) => return Err(ClientError::from(status)),
            };
            let bytes = match read_stream_to_bytes(&mut stream, block_read).await {
                Ok(bytes) => bytes,
                Err(err) if ErrorClassifier.classify_error(&err) == ErrorClass::RetryableTransport => {
                    self.channel_pool
                        .mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(err);
                    continue;
                }
                Err(err) => return Err(err),
            };
            return Ok(WorkerReadResult {
                bytes,
                block_stamp: open_response.block_stamp,
                committed_length: open_response.committed_length,
            });
        }
        if let Some(err) = last_transport_error {
            return Err(err);
        }
        Err(last_location_error.unwrap_or_else(|| {
            block_location_unavailable_error(format!(
                "block location unavailable: no reachable worker candidates for block {} file_offset={} len={} block_stamp={}",
                block_read.block_id, block_read.file_offset, block_read.len, block_read.block_stamp
            ))
        }))
    }

    async fn open_block_write(
        &self,
        attempt: AttemptContext,
        target: WorkerWriteTarget,
    ) -> ClientResult<WorkerBlockWriteHandle> {
        let mut last_transport_error = None;
        for worker in self.worker_candidates(&target.target.worker_endpoints) {
            let mut client = self
                .channel_pool
                .worker_data_service_client(worker, "OpenWriteStream")?;
            let request = build_open_write_stream_request(&attempt, &target, worker)?;
            let response = match client.open_write_stream(build_tonic_request(&attempt, request)).await {
                Ok(response) => response.into_inner(),
                Err(status) if is_transient_worker_transport_status(&status) => {
                    self.channel_pool
                        .mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
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
        attempt: AttemptContext,
        handle: &WorkerBlockWriteHandle,
        data: Bytes,
    ) -> ClientResult<beryl_proto::worker::WriteStreamResponseProto> {
        if data.is_empty() {
            return Ok(beryl_proto::worker::WriteStreamResponseProto {
                accepted: true,
                last_acked_seq: handle.next_seq.saturating_sub(1),
                written_through: 0,
            });
        }
        let mut client = self
            .channel_pool
            .worker_data_service_client(&handle.worker, "WriteStream")?;
        let expected_written_through = data.len() as u64;
        let requests = build_write_stream_requests(handle, data)?;
        let expected_last_seq = requests
            .last()
            .map(|request| request.seq)
            .unwrap_or_else(|| handle.next_seq.saturating_sub(1));
        let response = client
            .write_stream(build_tonic_request(&attempt, stream::iter(requests)))
            .await
            .map_err(|status| self.map_side_effect_status(&handle.worker, "WriteStream", status))?
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
            .worker_data_service_client(&handle.worker, "CommitWrite")?;
        let request = build_commit_write_request(&attempt, handle, effective_len, commit_seq, require_sync)?;
        let response = client
            .commit_write(build_tonic_request(&attempt, request))
            .await
            .map_err(|status| self.map_side_effect_status(&handle.worker, "CommitWrite", status))?
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
            .worker_data_service_client(&handle.worker, "SyncCommittedBlock")?;
        let request = build_sync_committed_block_request(&attempt, handle, expected_len)?;
        let response = client
            .sync_committed_block(build_tonic_request(&attempt, request))
            .await
            .map_err(|status| self.map_side_effect_status(&handle.worker, "SyncCommittedBlock", status))?
            .into_inner();
        parse_sync_committed_block_response(&attempt, handle, expected_len, response)
            .inspect_err(|err| self.channel_pool.invalidate_on_worker_run_mismatch(&handle.worker, err))
    }

    async fn abort_block_write(&self, attempt: AttemptContext, handle: &WorkerBlockWriteHandle) -> ClientResult<()> {
        let mut client = self
            .channel_pool
            .worker_data_service_client(&handle.worker, "AbortWrite")?;
        let request = build_abort_write_request(&attempt, handle)?;
        let response = client
            .abort_write(build_tonic_request(&attempt, request))
            .await
            .map_err(|status| self.map_side_effect_status(&handle.worker, "AbortWrite", status))?
            .into_inner();
        validate_abort_write_response(&attempt, response)
            .inspect_err(|err| self.channel_pool.invalidate_on_worker_run_mismatch(&handle.worker, err))
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
        attempt: AttemptContext,
        handle: &WorkerBlockWriteHandle,
        data: Bytes,
    ) -> ClientResult<beryl_proto::worker::WriteStreamResponseProto> {
        self.client.write_block_bytes(attempt, handle, data).await
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

    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use beryl_common::error::rpc::{ErrorKind, RefreshHint as RpcRefreshHint, RpcErrorDetail};
    use beryl_proto::convert::rpc_error_to_proto;
    use beryl_proto::worker::worker_data_service_server::{WorkerDataService, WorkerDataServiceServer};
    use beryl_proto::worker::{
        AbortWriteRequestProto, AbortWriteResponseProto, CommitWriteRequestProto, CommitWriteResponseProto,
        DataRequestHeaderProto, DataResponseHeaderProto, OpenReadStreamRequestProto, OpenReadStreamResponseProto,
        OpenWriteStreamRequestProto, OpenWriteStreamResponseProto, ReadStreamRequestProto, ReadStreamResponseProto,
        SyncCommittedBlockRequestProto, SyncCommittedBlockResponseProto, WriteStreamRequestProto,
        WriteStreamResponseProto,
    };
    use beryl_types::lease::FencingToken;
    use beryl_types::{BlockId, BlockIndex, ClientId, DataHandleId, WorkerEndpointInfo, WorkerId, WorkerNetProtocol};
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetrics};
    use crate::runtime::{
        ErrorClass, ErrorClassifier, MetadataRefreshCause, OperationContext, OperationIdentity, OperationKind,
    };

    #[derive(Debug, Default)]
    struct RecordingMetrics {
        events: Mutex<Vec<ClientMetricEvent>>,
    }

    impl ClientMetrics for RecordingMetrics {
        fn record(&self, event: ClientMetricEvent) {
            self.events.lock().expect("events").push(event);
        }
    }

    impl RecordingMetrics {
        fn events(&self) -> Vec<ClientMetricEvent> {
            self.events.lock().expect("events").clone()
        }
    }

    #[tokio::test]
    async fn side_effect_transient_status_invalidates_channel_and_returns_unknown_outcome() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::ResourceExhausted,
        ] {
            let metrics = Arc::new(RecordingMetrics::default());
            let client = grpc_client_with_metrics(metrics.clone());
            let worker = worker_endpoint("127.0.0.1:19101", 1);
            client
                .channel_pool
                .worker_data_service_client(&worker, "WriteStream")
                .expect("seed channel");

            let err = client.map_side_effect_status(&worker, "WriteStream", Status::new(code, "transport down"));

            assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("WriteStream")));
            let err = client
                .channel_pool
                .worker_data_service_client(&worker, "WriteStream")
                .expect_err("worker endpoint should cool down after transient failure");
            assert!(matches!(err, ClientError::Worker(msg) if msg.contains("cooling down")));
            let events = metrics.events();
            assert_metric(&events, ClientMetric::CachePreciseInvalidation);
            assert_eq!(count_metric(&events, ClientMetric::WorkerChannelPoolMiss), 1);
        }
    }

    #[tokio::test]
    async fn side_effect_non_transient_status_keeps_channel_and_returns_unknown_outcome() {
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics.clone());
        let worker = worker_endpoint("127.0.0.1:19101", 1);
        client
            .channel_pool
            .worker_data_service_client(&worker, "CommitWrite")
            .expect("seed channel");

        let err = client.map_side_effect_status(&worker, "CommitWrite", Status::permission_denied("permission denied"));

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitWrite")));
        client
            .channel_pool
            .worker_data_service_client(&worker, "CommitWrite")
            .expect("cached channel remains usable");
        let events = metrics.events();
        assert_no_metric(&events, ClientMetric::CachePreciseInvalidation);
        assert_metric(&events, ClientMetric::WorkerChannelPoolHit);
    }

    #[tokio::test]
    async fn transient_read_stream_establishment_failure_tries_next_worker() {
        let first_state = Arc::new(MockWorkerDataState {
            read_stream_status: Some(tonic::Code::Unavailable),
            read_payload: Bytes::new(),
            ..MockWorkerDataState::default()
        });
        let second_state = Arc::new(MockWorkerDataState {
            read_payload: Bytes::from_static(b"data"),
            ..MockWorkerDataState::default()
        });
        let (first_worker, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
        let (second_worker, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics.clone());
        let block_read = planned_block_read(vec![first_worker, second_worker]);

        let result = client
            .read_block_range(data_attempt_context("OpenReadStream"), test_group_name(), &block_read)
            .await
            .expect("second worker should satisfy read");

        assert_eq!(result.bytes, Bytes::from_static(b"data"));
        assert_eq!(first_state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(first_state.read_stream_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.read_stream_calls.load(Ordering::SeqCst), 1);
        assert_metric(&metrics.events(), ClientMetric::CachePreciseInvalidation);
        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
    }

    #[tokio::test]
    async fn stale_read_location_error_tries_next_worker() {
        let first_state = Arc::new(MockWorkerDataState {
            open_read_response: Mutex::new(Some(open_read_error(RpcErrorDetail::refresh_metadata(
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                RpcRefreshHint::default(),
                "local block is not available for read",
            )))),
            ..MockWorkerDataState::default()
        });
        let second_state = Arc::new(MockWorkerDataState {
            read_payload: Bytes::from_static(b"data"),
            ..MockWorkerDataState::default()
        });
        let (first_worker, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
        let (second_worker, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics);
        let block_read = planned_block_read(vec![first_worker, second_worker]);

        let result = client
            .read_block_range(data_attempt_context("OpenReadStream"), test_group_name(), &block_read)
            .await
            .expect("second worker should satisfy read after stale first candidate");

        assert_eq!(result.bytes, Bytes::from_static(b"data"));
        assert_eq!(first_state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(first_state.read_stream_calls.load(Ordering::SeqCst), 0);
        assert_eq!(second_state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.read_stream_calls.load(Ordering::SeqCst), 1);
        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
    }

    #[tokio::test]
    async fn exhausted_missing_block_candidates_surface_block_location_unavailable() {
        let first_state = Arc::new(MockWorkerDataState {
            open_read_response: Mutex::new(Some(open_read_error(RpcErrorDetail::refresh_metadata(
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                RpcRefreshHint::default(),
                "local block is not available for read: first",
            )))),
            ..MockWorkerDataState::default()
        });
        let second_state = Arc::new(MockWorkerDataState {
            open_read_response: Mutex::new(Some(open_read_error(RpcErrorDetail::refresh_metadata(
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                RpcRefreshHint::default(),
                "local block is not available for read: second",
            )))),
            ..MockWorkerDataState::default()
        });
        let (first_worker, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
        let (second_worker, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics);
        let block_read = planned_block_read(vec![first_worker, second_worker]);

        let err = client
            .read_block_range(data_attempt_context("OpenReadStream"), test_group_name(), &block_read)
            .await
            .expect_err("exhausted stale candidates must surface typed location error");

        assert_rpc_refresh(
            &err,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
            MetadataRefreshCause::Unknown,
        );
        assert_eq!(first_state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.open_read_calls.load(Ordering::SeqCst), 1);
        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
    }

    #[tokio::test]
    async fn exhausted_block_stamp_mismatch_candidates_surface_block_stamp_mismatch() {
        let state = Arc::new(MockWorkerDataState {
            open_read_response: Mutex::new(Some(open_read_error(RpcErrorDetail::refresh_metadata(
                ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
                RpcRefreshHint::default(),
                "block stamp mismatch",
            )))),
            ..MockWorkerDataState::default()
        });
        let (worker, shutdown) = start_mock_worker(Arc::clone(&state), 1).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics);
        let block_read = planned_block_read(vec![worker]);

        let err = client
            .read_block_range(data_attempt_context("OpenReadStream"), test_group_name(), &block_read)
            .await
            .expect_err("stamp mismatch must surface typed refresh error");

        assert_rpc_refresh(
            &err,
            ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
            MetadataRefreshCause::BlockStampMismatch,
        );
        assert_eq!(state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.read_stream_calls.load(Ordering::SeqCst), 0);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn exhausted_stale_worker_run_candidates_surface_worker_run_mismatch() {
        let state = Arc::new(MockWorkerDataState {
            open_read_response: Mutex::new(Some(open_read_error(RpcErrorDetail::refresh_metadata(
                ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                RpcRefreshHint::default(),
                "worker run mismatch",
            )))),
            ..MockWorkerDataState::default()
        });
        let (worker, shutdown) = start_mock_worker(Arc::clone(&state), 1).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics);
        let block_read = planned_block_read(vec![worker]);

        let err = client
            .read_block_range(data_attempt_context("OpenReadStream"), test_group_name(), &block_read)
            .await
            .expect_err("worker run mismatch must surface typed refresh error");

        assert_rpc_refresh(
            &err,
            ErrorKind::Worker(WorkerErrorKind::RunMismatch),
            MetadataRefreshCause::WorkerRunMismatch,
        );
        assert_eq!(state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.read_stream_calls.load(Ordering::SeqCst), 0);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn transient_read_failure_cools_down_worker_for_followup_read() {
        let first_state = Arc::new(MockWorkerDataState {
            read_stream_status: Some(tonic::Code::Unavailable),
            read_payload: Bytes::new(),
            ..MockWorkerDataState::default()
        });
        let second_state = Arc::new(MockWorkerDataState {
            read_payload: Bytes::from_static(b"data"),
            ..MockWorkerDataState::default()
        });
        let (first_worker, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
        let (second_worker, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics);
        let block_read = planned_block_read(vec![first_worker, second_worker]);

        client
            .read_block_range(data_attempt_context("OpenReadStream"), test_group_name(), &block_read)
            .await
            .expect("second worker should satisfy first read");
        client
            .read_block_range(data_attempt_context("OpenReadStream"), test_group_name(), &block_read)
            .await
            .expect("cooled down first worker should be skipped");

        assert_eq!(first_state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(first_state.read_stream_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.open_read_calls.load(Ordering::SeqCst), 2);
        assert_eq!(second_state.read_stream_calls.load(Ordering::SeqCst), 2);
        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
    }

    #[tokio::test]
    async fn all_cooling_read_candidates_are_tried_when_no_alternative() {
        let state = Arc::new(MockWorkerDataState {
            read_payload: Bytes::from_static(b"data"),
            ..MockWorkerDataState::default()
        });
        let (worker, shutdown) = start_mock_worker(Arc::clone(&state), 1).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics);
        client
            .channel_pool
            .mark_worker_unavailable(&worker, CacheInvalidationReason::Unavailable);
        let block_read = planned_block_read(vec![worker]);

        let result = client
            .read_block_range(data_attempt_context("OpenReadStream"), test_group_name(), &block_read)
            .await
            .expect("single cooled read candidate should still be tried");

        assert_eq!(result.bytes, Bytes::from_static(b"data"));
        assert_eq!(state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.read_stream_calls.load(Ordering::SeqCst), 1);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn all_cooling_write_candidates_are_tried_when_no_alternative() {
        let state = Arc::new(MockWorkerDataState::default());
        let (worker, shutdown) = start_mock_worker(Arc::clone(&state), 1).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics);
        client
            .channel_pool
            .mark_worker_unavailable(&worker, CacheInvalidationReason::Unavailable);
        let target = WorkerWriteTarget {
            group_name: test_group_name(),
            target: write_target(worker.clone()),
        };

        let handle = client
            .open_block_write(data_attempt_context("OpenWriteStream"), target)
            .await
            .expect("single cooled write candidate should still be tried");

        assert_eq!(handle.worker.endpoint, worker.endpoint);
        assert_eq!(state.open_write_calls.load(Ordering::SeqCst), 1);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn transient_read_stream_consumption_failure_tries_next_worker_and_discards_partial_bytes() {
        let first_state = Arc::new(MockWorkerDataState {
            read_stream_frames: Mutex::new(Some(vec![
                Ok(read_frame(0, Bytes::from_static(b"xx"), false)),
                Err(Status::unavailable("read stream item unavailable")),
            ])),
            ..MockWorkerDataState::default()
        });
        let second_state = Arc::new(MockWorkerDataState {
            read_payload: Bytes::from_static(b"data"),
            ..MockWorkerDataState::default()
        });
        let (first_worker, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
        let (second_worker, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics.clone());
        let block_read = planned_block_read(vec![first_worker.clone(), second_worker.clone()]);

        let result = client
            .read_block_range(data_attempt_context("OpenReadStream"), test_group_name(), &block_read)
            .await
            .expect("second worker should satisfy read after first stream item failure");

        assert_eq!(result.bytes, Bytes::from_static(b"data"));
        assert_eq!(result.bytes.len(), block_read.len as usize);
        assert!(!result.bytes.windows(2).any(|window| window == b"xx"));
        assert_eq!(first_state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(first_state.read_stream_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.read_stream_calls.load(Ordering::SeqCst), 1);
        assert_worker_channel_cached(&client, &metrics, &first_worker, false);
        assert_worker_channel_cached(&client, &metrics, &second_worker, true);
        assert_metric(&metrics.events(), ClientMetric::CachePreciseInvalidation);
        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
    }

    #[tokio::test]
    async fn protocol_corrupt_read_stream_frame_does_not_try_next_worker() {
        let first_state = Arc::new(MockWorkerDataState {
            read_stream_frames: Mutex::new(Some(vec![Ok(read_frame(1, Bytes::from_static(b"data"), true))])),
            ..MockWorkerDataState::default()
        });
        let second_state = Arc::new(MockWorkerDataState {
            read_payload: Bytes::from_static(b"data"),
            ..MockWorkerDataState::default()
        });
        let (first_worker, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
        let (second_worker, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics.clone());
        let block_read = planned_block_read(vec![first_worker.clone(), second_worker.clone()]);

        let err = client
            .read_block_range(data_attempt_context("OpenReadStream"), test_group_name(), &block_read)
            .await
            .expect_err("protocol-corrupt stream frame must fail without failover");

        assert!(matches!(
            &err,
            ClientError::Worker(msg)
                if msg.contains("worker read frame offset mismatch")
                    && msg.contains("expected 0, got 1")
        ));
        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        assert_eq!(first_state.open_read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(first_state.read_stream_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.open_read_calls.load(Ordering::SeqCst), 0);
        assert_eq!(second_state.read_stream_calls.load(Ordering::SeqCst), 0);
        assert_worker_channel_cached(&client, &metrics, &first_worker, true);
        assert_worker_channel_cached(&client, &metrics, &second_worker, false);
        assert_no_metric(&metrics.events(), ClientMetric::CachePreciseInvalidation);
        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
    }

    #[tokio::test]
    async fn commit_write_transient_transport_failure_returns_unknown_outcome_and_invalidates_channel() {
        let commit_state = Arc::new(MockWorkerDataState {
            commit_status: Mutex::new(Some(Status::unavailable("commit transport down"))),
            ..MockWorkerDataState::default()
        });
        let (worker, shutdown) = start_mock_worker(Arc::clone(&commit_state), 1).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics.clone());
        client
            .channel_pool
            .worker_data_service_client(&worker, "CommitWrite")
            .expect("seed worker channel");
        assert_worker_channel_cached(&client, &metrics, &worker, true);
        let handle = worker_block_write_handle(worker.clone());

        let err = client
            .commit_block_write(
                data_attempt_context("CommitWrite"),
                &handle,
                handle.target.effective_len,
                1,
                false,
            )
            .await
            .expect_err("transient CommitWrite transport failure must be unknown outcome");

        match &err {
            ClientError::UnknownOutcome(msg) => {
                assert!(msg.contains("CommitWrite"), "{msg}");
                assert!(msg.contains(&tonic::Code::Unavailable.to_string()), "{msg}");
                assert!(msg.contains("commit transport down"), "{msg}");
            }
            other => panic!("expected UnknownOutcome, got {other:?}"),
        }
        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::UnknownOutcome);
        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        assert_eq!(commit_state.commit_calls.load(Ordering::SeqCst), 1);
        assert_worker_channel_cached(&client, &metrics, &worker, false);
        assert_metric(&metrics.events(), ClientMetric::CachePreciseInvalidation);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn abort_write_worker_run_mismatch_invalidates_channel() {
        let abort_state = Arc::new(MockWorkerDataState {
            abort_response: Mutex::new(Some(AbortWriteResponseProto {
                header: Some(data_header_with_error(RpcErrorDetail::refresh_metadata(
                    ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                    RpcRefreshHint::default(),
                    "worker run mismatch",
                ))),
                aborted: false,
            })),
            ..MockWorkerDataState::default()
        });
        let (worker, shutdown) = start_mock_worker(Arc::clone(&abort_state), 1).await;
        let metrics = Arc::new(RecordingMetrics::default());
        let client = grpc_client_with_metrics(metrics.clone());
        let handle = worker_block_write_handle(worker);

        let err = client
            .abort_block_write(data_attempt_context("AbortWrite"), &handle)
            .await
            .expect_err("worker run mismatch must fail");

        assert_eq!(
            ErrorClassifier.classify_error(&err),
            ErrorClass::RefreshMetadata(crate::runtime::MetadataRefreshCause::WorkerRunMismatch)
        );
        assert_metric(&metrics.events(), ClientMetric::CachePreciseInvalidation);
        let _ = shutdown.send(());
    }

    #[derive(Default)]
    struct MockWorkerDataState {
        open_read_calls: AtomicUsize,
        open_write_calls: AtomicUsize,
        read_stream_calls: AtomicUsize,
        commit_calls: AtomicUsize,
        abort_calls: AtomicUsize,
        read_stream_status: Option<tonic::Code>,
        read_stream_frames: Mutex<Option<Vec<Result<ReadStreamResponseProto, Status>>>>,
        read_payload: Bytes,
        open_read_response: Mutex<Option<OpenReadStreamResponseProto>>,
        commit_status: Mutex<Option<Status>>,
        abort_response: Mutex<Option<AbortWriteResponseProto>>,
    }

    #[derive(Clone)]
    struct MockWorkerDataService {
        state: Arc<MockWorkerDataState>,
    }

    #[tonic::async_trait]
    impl WorkerDataService for MockWorkerDataService {
        type ReadStreamStream = Pin<Box<dyn futures::Stream<Item = Result<ReadStreamResponseProto, Status>> + Send>>;

        async fn open_read_stream(
            &self,
            request: Request<OpenReadStreamRequestProto>,
        ) -> Result<Response<OpenReadStreamResponseProto>, Status> {
            self.state.open_read_calls.fetch_add(1, Ordering::SeqCst);
            let request = request.into_inner();
            if let Some(mut response) = self.state.open_read_response.lock().expect("open read response").take() {
                if let Some(header) = response.header.as_mut() {
                    header.client = request.header.as_ref().and_then(|header| header.client.clone());
                }
                return Ok(Response::new(response));
            }
            Ok(Response::new(OpenReadStreamResponseProto {
                header: Some(ok_data_header(request.header.as_ref())),
                stream_id: Some(beryl_proto::common::StreamIdProto { high: 1, low: 1 }),
                frame_size: request.frame_size.max(1),
                window_bytes: 0,
                block_stamp: request.block_stamp,
                committed_length: request.effective_len,
            }))
        }

        async fn read_stream(
            &self,
            _request: Request<ReadStreamRequestProto>,
        ) -> Result<Response<Self::ReadStreamStream>, Status> {
            self.state.read_stream_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(code) = self.state.read_stream_status {
                return Err(Status::new(code, "read stream transport failure"));
            }
            let frames = self
                .state
                .read_stream_frames
                .lock()
                .expect("read stream frames")
                .take()
                .unwrap_or_else(|| vec![Ok(read_frame(0, self.state.read_payload.clone(), true))]);
            Ok(Response::new(
                Box::pin(futures::stream::iter(frames)) as Self::ReadStreamStream
            ))
        }

        async fn open_write_stream(
            &self,
            request: Request<OpenWriteStreamRequestProto>,
        ) -> Result<Response<OpenWriteStreamResponseProto>, Status> {
            self.state.open_write_calls.fetch_add(1, Ordering::SeqCst);
            let request = request.into_inner();
            Ok(Response::new(OpenWriteStreamResponseProto {
                header: Some(ok_data_header(request.header.as_ref())),
                stream_id: Some(beryl_proto::common::StreamIdProto { high: 1, low: 1 }),
                frame_size: request.frame_size.max(1),
                window_bytes: 0,
                block_stamp: request.block_stamp,
                committed_length: 0,
            }))
        }

        async fn write_stream(
            &self,
            _request: Request<tonic::Streaming<WriteStreamRequestProto>>,
        ) -> Result<Response<WriteStreamResponseProto>, Status> {
            Err(Status::unimplemented("write stream unused in test"))
        }

        async fn commit_write(
            &self,
            request: Request<CommitWriteRequestProto>,
        ) -> Result<Response<CommitWriteResponseProto>, Status> {
            self.state.commit_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(status) = self.state.commit_status.lock().expect("commit status").take() {
                return Err(status);
            }
            let request = request.into_inner();
            Ok(Response::new(CommitWriteResponseProto {
                header: Some(ok_data_header(request.header.as_ref())),
                effective_len: request.effective_len,
                block_stamp: request.block_stamp,
                written_through: request.effective_len,
            }))
        }

        async fn sync_committed_block(
            &self,
            _request: Request<SyncCommittedBlockRequestProto>,
        ) -> Result<Response<SyncCommittedBlockResponseProto>, Status> {
            Err(Status::unimplemented("sync committed block unused in test"))
        }

        async fn abort_write(
            &self,
            request: Request<AbortWriteRequestProto>,
        ) -> Result<Response<AbortWriteResponseProto>, Status> {
            self.state.abort_calls.fetch_add(1, Ordering::SeqCst);
            let request = request.into_inner();
            let mut response = self
                .state
                .abort_response
                .lock()
                .expect("abort response")
                .take()
                .unwrap_or_else(|| AbortWriteResponseProto {
                    header: Some(ok_data_header(request.header.as_ref())),
                    aborted: true,
                });
            if let Some(header) = response.header.as_mut() {
                header.client = request.header.as_ref().and_then(|header| header.client.clone());
            }
            Ok(Response::new(response))
        }
    }

    async fn start_mock_worker(
        state: Arc<MockWorkerDataState>,
        worker_id: u64,
    ) -> (WorkerEndpointInfo, tokio::sync::oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock worker");
        let addr = listener.local_addr().expect("mock worker local addr");
        let incoming = futures::stream::try_unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await?;
            Ok::<_, std::io::Error>(Some((stream, listener)))
        });
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .add_service(WorkerDataServiceServer::new(MockWorkerDataService { state }))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("mock worker server");
        });

        (worker_endpoint(&addr.to_string(), worker_id), shutdown_tx)
    }

    fn grpc_client_with_metrics(metrics: Arc<dyn ClientMetrics>) -> GrpcWorkerDataClient {
        GrpcWorkerDataClient {
            channel_pool: GrpcWorkerChannelPool::new(true, 8, metrics),
        }
    }

    fn planned_block_read(workers: Vec<WorkerEndpointInfo>) -> PlannedBlockRead {
        PlannedBlockRead {
            file_offset: 0,
            len: 4,
            end_file_offset: 4,
            block_id: test_block_id(),
            block_offset: 0,
            block_stamp: 77,
            block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: 4096,
            chunk_size: 4096,
            effective_len: 4,
            workers,
        }
    }

    fn read_frame(offset_in_block: u64, data: Bytes, eos: bool) -> ReadStreamResponseProto {
        ReadStreamResponseProto {
            offset_in_block,
            data,
            checksum32: 0,
            eos,
        }
    }

    fn worker_block_write_handle(worker: WorkerEndpointInfo) -> WorkerBlockWriteHandle {
        WorkerBlockWriteHandle {
            group_name: test_group_name(),
            worker: worker.clone(),
            target: write_target(worker),
            stream_id: beryl_proto::common::StreamIdProto { high: 1, low: 1 },
            frame_size: 1024,
            next_seq: 1,
        }
    }

    fn write_target(worker: WorkerEndpointInfo) -> beryl_types::WriteTarget {
        let block_id = test_block_id();
        beryl_types::WriteTarget {
            block_id,
            file_offset: 0,
            block_size: 4096,
            effective_len: 4,
            worker_endpoints: vec![worker],
            fencing_token: FencingToken::new(block_id, ClientId::new(7), 1),
            block_stamp: 77,
            chunk_size: 4096,
            block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            tier: beryl_types::Tier::Mem,
        }
    }

    fn worker_endpoint(endpoint: &str, worker_id: u64) -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(worker_id),
            endpoint: endpoint.to_string(),
            worker_net_protocol: WorkerNetProtocol::Grpc,
            worker_run_id: "550e8400-e29b-41d4-a716-446655440000"
                .parse()
                .expect("valid test WorkerRunId"),
        }
    }

    fn test_block_id() -> BlockId {
        BlockId::new(DataHandleId::new(202), BlockIndex::new(0))
    }

    fn test_group_name() -> GroupName {
        GroupName::parse("root").expect("group name")
    }

    fn data_attempt_context(operation_name: &'static str) -> AttemptContext {
        let operation = OperationContext::new(
            ClientId::new(7),
            OperationKind::WorkerWriteData,
            operation_name,
            OperationIdentity::path("/alpha"),
        )
        .expect("operation context");
        AttemptContext::for_data(&operation, 0)
    }

    fn ok_data_header(request: Option<&DataRequestHeaderProto>) -> DataResponseHeaderProto {
        DataResponseHeaderProto {
            client: request.and_then(|header| header.client.clone()),
            error: None,
        }
    }

    fn data_header_with_error(rpc_error: RpcErrorDetail) -> DataResponseHeaderProto {
        let attempt = data_attempt_context("AbortWrite");
        DataResponseHeaderProto {
            client: Some(attempt.client_info()),
            error: Some(rpc_error_to_proto(&rpc_error)),
        }
    }

    fn open_read_error(rpc_error: RpcErrorDetail) -> OpenReadStreamResponseProto {
        OpenReadStreamResponseProto {
            header: Some(data_header_with_error(rpc_error)),
            stream_id: None,
            frame_size: 0,
            window_bytes: 0,
            block_stamp: 0,
            committed_length: 0,
        }
    }

    fn assert_rpc_refresh(err: &ClientError, expected_kind: ErrorKind, expected_reason: MetadataRefreshCause) {
        match err {
            ClientError::Action(action) => match action.action() {
                crate::rpc_error::ClientAction::Refresh { reason, rpc_error, .. } => {
                    assert_eq!(*reason, expected_reason);
                    assert_eq!(rpc_error.kind, expected_kind);
                }
                other => panic!("expected refresh action, got {other:?}"),
            },
            other => panic!("expected action error, got {other:?}"),
        }
    }

    fn assert_metric(events: &[ClientMetricEvent], metric: ClientMetric) {
        assert!(
            events.iter().any(|event| event.metric == metric),
            "missing metric {metric:?}: {events:?}"
        );
    }

    fn assert_no_metric(events: &[ClientMetricEvent], metric: ClientMetric) {
        assert!(
            events.iter().all(|event| event.metric != metric),
            "unexpected metric {metric:?}: {events:?}"
        );
    }

    fn assert_worker_channel_cached(
        client: &GrpcWorkerDataClient,
        metrics: &RecordingMetrics,
        worker: &WorkerEndpointInfo,
        expected_cached: bool,
    ) {
        let before = metrics.events();
        let hits = count_metric(&before, ClientMetric::WorkerChannelPoolHit);
        let misses = count_metric(&before, ClientMetric::WorkerChannelPoolMiss);

        let result = client.channel_pool.worker_data_service_client(worker, "CacheProbe");
        if !expected_cached {
            if let Err(ClientError::Worker(msg)) = &result {
                assert!(msg.contains("cooling down"), "unexpected worker error: {msg}");
                return;
            }
        }
        result.expect("cache probe channel");

        let after = metrics.events();
        let hit_delta = count_metric(&after, ClientMetric::WorkerChannelPoolHit) - hits;
        let miss_delta = count_metric(&after, ClientMetric::WorkerChannelPoolMiss) - misses;
        if expected_cached {
            assert_eq!(hit_delta, 1, "cached channel should record exactly one hit: {after:?}");
            assert_eq!(miss_delta, 0, "cached channel should not record a miss: {after:?}");
        } else {
            assert_eq!(hit_delta, 0, "uncached channel should not record a hit: {after:?}");
            assert_eq!(
                miss_delta, 1,
                "uncached channel should record exactly one miss: {after:?}"
            );
        }
    }

    fn count_metric(events: &[ClientMetricEvent], metric: ClientMetric) -> usize {
        events.iter().filter(|event| event.metric == metric).count()
    }
}
