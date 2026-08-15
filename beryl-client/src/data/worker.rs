// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker data-plane orchestration owned by the client crate.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream;

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_types::{GroupName, WriteTarget};

use super::channel_pool::GrpcWorkerChannelPool;
use super::protocol::{
    build_read_block_request, build_tonic_request, build_write_block_command, build_write_block_data,
    has_structured_worker_error, is_transient_worker_transport_status, parse_worker_data_status,
    read_block_stream_to_bytes,
};
use super::{WorkerDataClient, WorkerReadResult, WorkerWriteTarget};
use crate::cache::CacheInvalidationReason;
use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult};
use crate::metrics::{ClientMetrics, NoopClientMetrics};
use crate::planner::{block_location_unavailable_error, PlannedBlockRead};
use crate::runtime::{classify_error, AttemptContext, ErrorClass};

/// Concrete gRPC implementation of the client-side Worker data plane.
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
        for worker in &cooling {
            self.channel_pool.clear_worker_cooldown(worker);
        }
        cooling
    }

    /// Maps a terminal write status. Structured Worker rejection remains
    /// actionable; unstructured transport loss after request initiation is an
    /// unknown outcome and must never be replayed on another worker.
    fn map_write_status(
        &self,
        attempt: &AttemptContext,
        worker: &beryl_types::WorkerEndpointInfo,
        status: tonic::Status,
    ) -> ClientError {
        if has_structured_worker_error(&status) {
            let error = parse_worker_data_status(attempt, status);
            self.channel_pool.invalidate_on_worker_run_mismatch(worker, &error);
            return error;
        }
        if is_transient_worker_transport_status(&status) {
            self.channel_pool
                .mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
        }
        ClientError::UnknownOutcome(format!(
            "worker WriteBlock outcome is unknown after transport status {}: {}",
            status.code(),
            status.message()
        ))
    }

    /// Drives one block through one bidirectional RPC.
    ///
    /// The first response acknowledges staging ownership. After that point,
    /// unstructured transport loss is an unknown outcome and is never replayed.
    async fn write_one_block(
        &self,
        attempt: &AttemptContext,
        target: &WorkerWriteTarget,
        worker: &beryl_types::WorkerEndpointInfo,
        data: Bytes,
    ) -> ClientResult<()> {
        let mut client = self.channel_pool.worker_data_service_client(worker, "WriteBlock")?;
        let command = build_write_block_command(attempt, target, worker)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender
            .send(command)
            .await
            .map_err(|_| ClientError::Worker("WriteBlock request stream closed before command".to_string()))?;
        let requests = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|request| (request, receiver))
        });
        let mut responses = client
            .write_block(build_tonic_request(attempt, requests))
            .await
            .map_err(|status| self.map_write_status(attempt, worker, status))?
            .into_inner();

        match responses.message().await {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Err(ClientError::UnknownOutcome(
                    "worker WriteBlock ended before staging acknowledgement".to_string(),
                ));
            }
            Err(status) => return Err(self.map_write_status(attempt, worker, status)),
        }

        for offset in (0..data.len()).step_by(beryl_proto::DEFAULT_WORKER_DATA_FRAME_SIZE) {
            let end = (offset + beryl_proto::DEFAULT_WORKER_DATA_FRAME_SIZE).min(data.len());
            let request = build_write_block_data(data.slice(offset..end))?;
            if sender.send(request).await.is_err() {
                return match responses.message().await {
                    Err(status) => Err(self.map_write_status(attempt, worker, status)),
                    Ok(_) => Err(ClientError::UnknownOutcome(
                        "worker WriteBlock request stream closed after acknowledgement".to_string(),
                    )),
                };
            }
        }
        drop(sender);

        match responses.message().await {
            Ok(None) => Ok(()),
            Ok(Some(_)) => Err(ClientError::UnknownOutcome(
                "worker WriteBlock returned more than one acknowledgement".to_string(),
            )),
            Err(status) => Err(self.map_write_status(attempt, worker, status)),
        }
    }
}

fn is_stale_read_location_error(error: &ClientError) -> bool {
    match error {
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
            let mut client = self.channel_pool.worker_data_service_client(worker, "ReadBlock")?;
            let request = build_read_block_request(&attempt, &group_name, block_read, worker)?;
            let mut responses = match client.read_block(build_tonic_request(&attempt, request)).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    let error = parse_worker_data_status(&attempt, status);
                    self.channel_pool.invalidate_on_worker_run_mismatch(worker, &error);
                    if is_stale_read_location_error(&error) {
                        last_location_error = Some(error);
                        continue;
                    }
                    if classify_error(&error) != ErrorClass::RetryableTransport {
                        return Err(error);
                    }
                    self.channel_pool
                        .mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(error);
                    continue;
                }
            };
            let bytes = match read_block_stream_to_bytes(&attempt, &mut responses, block_read).await {
                Ok(bytes) => bytes,
                Err(error) if is_stale_read_location_error(&error) => {
                    self.channel_pool.invalidate_on_worker_run_mismatch(worker, &error);
                    last_location_error = Some(error);
                    continue;
                }
                Err(error) if classify_error(&error) == ErrorClass::RetryableTransport => {
                    self.channel_pool
                        .mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(error);
                    continue;
                }
                Err(error) => return Err(error),
            };
            return Ok(WorkerReadResult { bytes });
        }
        if let Some(error) = last_transport_error {
            return Err(error);
        }
        Err(last_location_error.unwrap_or_else(|| {
            block_location_unavailable_error(format!(
                "block location unavailable: no reachable worker candidates for block {} file_offset={} len={} block_stamp={}",
                block_read.block_id, block_read.file_offset, block_read.len, block_read.block_stamp
            ))
        }))
    }

    async fn write_block(&self, attempt: AttemptContext, target: WorkerWriteTarget, data: Bytes) -> ClientResult<()> {
        if data.is_empty() {
            return Err(ClientError::InvalidArgument(
                "WriteBlock requires at least one data byte".to_string(),
            ));
        }
        let worker = self
            .worker_candidates(&target.target.worker_endpoints)
            .into_iter()
            .next()
            .ok_or_else(|| ClientError::Worker("worker write has no candidates".to_string()))?;
        self.write_one_block(&attempt, &target, worker, data).await
    }
}

/// Internal worker data-plane holder used by the public facade.
#[derive(Clone)]
pub(crate) struct WorkerDataPlane {
    client: Arc<dyn WorkerDataClient>,
}

impl WorkerDataPlane {
    pub(crate) fn new() -> Self {
        Self::with_client(Arc::new(GrpcWorkerDataClient::new()))
    }

    pub(crate) fn from_config(config: &ClientConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::with_client(Arc::new(GrpcWorkerDataClient::from_config(config, metrics)))
    }

    pub(crate) fn with_client(client: Arc<dyn WorkerDataClient>) -> Self {
        Self { client }
    }

    /// Reads all planned block-local ranges in file order.
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
                .checked_add(u64::from(block_read.len))
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

    /// Writes one complete metadata-authorized block through one worker RPC.
    pub(crate) async fn write_block(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        target: WriteTarget,
        data: Bytes,
    ) -> ClientResult<()> {
        self.client
            .write_block(attempt, WorkerWriteTarget { group_name, target }, data)
            .await
    }
}

impl fmt::Debug for WorkerDataPlane {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("WorkerDataPlane").finish_non_exhaustive()
    }
}

impl Default for WorkerDataPlane {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use beryl_common::error::rpc::{ErrorKind, RefreshHint, RpcErrorDetail, WorkerErrorKind};
    use beryl_common::header::{HEADER_WORKER_DATA_ERROR_DETAIL, WORKER_DATA_ERROR_DETAIL_V1};
    use beryl_proto::convert::rpc_error_to_proto;
    use beryl_proto::worker::worker_data_service_server::{WorkerDataService, WorkerDataServiceServer};
    use beryl_proto::worker::{
        DataResponseHeaderProto, ReadBlockChunkProto, ReadBlockRequestProto, WriteBlockRequestProto,
        WriteBlockResponseProto,
    };
    use beryl_types::lease::FencingToken;
    use beryl_types::{
        BlockId, BlockIndex, ClientId, InodeId, WorkerEndpointInfo, WorkerId, WorkerNetProtocol, WorkerRunId,
    };
    use prost::Message;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::runtime::{OperationContext, OperationDeadline};

    #[derive(Clone, Copy)]
    enum ReadFailure {
        None,
        Transport,
        StructuredLocation,
        PartialTransport,
        EmptyChunk,
    }

    #[derive(Clone, Copy)]
    enum WriteBehavior {
        Success,
        AckThenUnavailable,
    }

    struct MockWorkerState {
        read_calls: AtomicUsize,
        write_calls: AtomicUsize,
        read_failure: ReadFailure,
        write_behavior: WriteBehavior,
    }

    impl MockWorkerState {
        fn new(read_failure: ReadFailure, write_behavior: WriteBehavior) -> Self {
            Self {
                read_calls: AtomicUsize::new(0),
                write_calls: AtomicUsize::new(0),
                read_failure,
                write_behavior,
            }
        }
    }

    #[derive(Clone)]
    struct MockWorkerService {
        state: Arc<MockWorkerState>,
    }

    #[tonic::async_trait]
    impl WorkerDataService for MockWorkerService {
        type ReadBlockStream = Pin<Box<dyn futures::Stream<Item = Result<ReadBlockChunkProto, Status>> + Send>>;
        type WriteBlockStream = Pin<Box<dyn futures::Stream<Item = Result<WriteBlockResponseProto, Status>> + Send>>;

        async fn read_block(
            &self,
            request: Request<ReadBlockRequestProto>,
        ) -> Result<Response<Self::ReadBlockStream>, Status> {
            self.state.read_calls.fetch_add(1, Ordering::SeqCst);
            let request = request.into_inner();
            match self.state.read_failure {
                ReadFailure::Transport => Err(Status::unavailable("read transport unavailable")),
                ReadFailure::StructuredLocation => Err(structured_location_status(request.header.as_ref())),
                ReadFailure::PartialTransport => Ok(Response::new(Box::pin(futures::stream::iter(vec![
                    Ok(ReadBlockChunkProto {
                        data: Bytes::from_static(b"xx"),
                    }),
                    Err(Status::unavailable("partial read transport unavailable")),
                ])))),
                ReadFailure::EmptyChunk => Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
                    ReadBlockChunkProto { data: Bytes::new() },
                )])))),
                ReadFailure::None => Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
                    ReadBlockChunkProto {
                        data: Bytes::from_static(b"data"),
                    },
                )])))),
            }
        }

        async fn write_block(
            &self,
            request: Request<tonic::Streaming<WriteBlockRequestProto>>,
        ) -> Result<Response<Self::WriteBlockStream>, Status> {
            self.state.write_calls.fetch_add(1, Ordering::SeqCst);
            let mut requests = request.into_inner();
            let first = requests
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("missing command"))?;
            if !matches!(
                first.payload,
                Some(beryl_proto::worker::write_block_request_proto::Payload::Command(_))
            ) {
                return Err(Status::invalid_argument("first payload must be command"));
            }
            let responses: Vec<Result<WriteBlockResponseProto, Status>> = match self.state.write_behavior {
                WriteBehavior::Success => vec![Ok(WriteBlockResponseProto {})],
                WriteBehavior::AckThenUnavailable => vec![
                    Ok(WriteBlockResponseProto {}),
                    Err(Status::unavailable("write transport unavailable after ack")),
                ],
            };
            Ok(Response::new(Box::pin(futures::stream::iter(responses))))
        }
    }

    async fn start_mock_worker(
        state: Arc<MockWorkerState>,
        worker_id: u64,
    ) -> (WorkerEndpointInfo, tokio::sync::oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock Worker");
        let address = listener.local_addr().expect("mock Worker address");
        let incoming = futures::stream::try_unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await?;
            Ok::<_, std::io::Error>(Some((stream, listener)))
        });
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .add_service(WorkerDataServiceServer::new(MockWorkerService { state }))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("mock Worker server");
        });
        (worker_endpoint(&address.to_string(), worker_id), shutdown_tx)
    }

    fn grpc_client() -> GrpcWorkerDataClient {
        GrpcWorkerDataClient {
            channel_pool: GrpcWorkerChannelPool::new(true, 8, Arc::new(NoopClientMetrics)),
        }
    }

    fn worker_endpoint(endpoint: &str, worker_id: u64) -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(worker_id),
            worker_run_id: WorkerRunId::new(),
            endpoint: endpoint.to_string(),
            worker_net_protocol: WorkerNetProtocol::Grpc,
        }
    }

    fn block_id() -> BlockId {
        BlockId::new(InodeId::new(202), BlockIndex::new(0))
    }

    fn group_name() -> GroupName {
        GroupName::parse("root").expect("group name")
    }

    fn attempt(operation_name: &'static str) -> AttemptContext {
        let operation = OperationContext::new_named(
            ClientId::new(7),
            "test-client",
            operation_name,
            Some("/alpha".to_string()),
            OperationDeadline::new(5_000),
        )
        .expect("operation context");
        AttemptContext::for_data(&operation, 0)
    }

    fn planned_read(workers: Vec<WorkerEndpointInfo>) -> PlannedBlockRead {
        PlannedBlockRead {
            file_offset: 0,
            len: 4,
            end_file_offset: 4,
            block_id: block_id(),
            block_offset: 0,
            block_stamp: 77,
            block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: 4096,
            chunk_size: 4096,
            effective_len: 4,
            workers,
        }
    }

    fn write_target(workers: Vec<WorkerEndpointInfo>) -> WorkerWriteTarget {
        let block_id = block_id();
        WorkerWriteTarget {
            group_name: group_name(),
            target: WriteTarget {
                block_id,
                file_offset: 0,
                block_size: 4096,
                worker_endpoints: workers,
                fencing_token: FencingToken::new(block_id, ClientId::new(7), 1),
                block_stamp: 77,
                chunk_size: 4096,
                block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE,
                tier: beryl_types::Tier::Mem,
            },
        }
    }

    fn structured_location_status(header: Option<&beryl_proto::worker::DataRequestHeaderProto>) -> Status {
        let error = RpcErrorDetail::refresh_metadata(
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
            RefreshHint::default(),
            "local block is unavailable",
        );
        let response = DataResponseHeaderProto {
            client: header.and_then(|header| header.client.clone()),
            error: Some(rpc_error_to_proto(&error)),
        };
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            HEADER_WORKER_DATA_ERROR_DETAIL,
            WORKER_DATA_ERROR_DETAIL_V1.parse().expect("error detail version"),
        );
        Status::with_details_and_metadata(
            tonic::Code::FailedPrecondition,
            error.message,
            Bytes::from(response.encode_to_vec()),
            metadata,
        )
    }

    #[tokio::test]
    async fn read_transport_and_structured_location_failures_try_the_next_worker() {
        for failure in [ReadFailure::Transport, ReadFailure::StructuredLocation] {
            let first_state = Arc::new(MockWorkerState::new(failure, WriteBehavior::Success));
            let second_state = Arc::new(MockWorkerState::new(ReadFailure::None, WriteBehavior::Success));
            let (first, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
            let (second, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;

            let result = grpc_client()
                .read_block_range(attempt("ReadBlock"), group_name(), &planned_read(vec![first, second]))
                .await
                .expect("second Worker satisfies read");

            assert_eq!(result.bytes, Bytes::from_static(b"data"));
            assert_eq!(first_state.read_calls.load(Ordering::SeqCst), 1);
            assert_eq!(second_state.read_calls.load(Ordering::SeqCst), 1);
            let _ = first_shutdown.send(());
            let _ = second_shutdown.send(());
        }
    }

    #[tokio::test]
    async fn partial_read_is_discarded_before_failover() {
        let first_state = Arc::new(MockWorkerState::new(
            ReadFailure::PartialTransport,
            WriteBehavior::Success,
        ));
        let second_state = Arc::new(MockWorkerState::new(ReadFailure::None, WriteBehavior::Success));
        let (first, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
        let (second, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;

        let result = grpc_client()
            .read_block_range(attempt("ReadBlock"), group_name(), &planned_read(vec![first, second]))
            .await
            .expect("second Worker replaces partial stream");

        assert_eq!(result.bytes, Bytes::from_static(b"data"));
        assert_eq!(first_state.read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.read_calls.load(Ordering::SeqCst), 1);
        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
    }

    #[tokio::test]
    async fn read_protocol_corruption_does_not_fail_over() {
        let first_state = Arc::new(MockWorkerState::new(ReadFailure::EmptyChunk, WriteBehavior::Success));
        let second_state = Arc::new(MockWorkerState::new(ReadFailure::None, WriteBehavior::Success));
        let (first, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
        let (second, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;

        let error = grpc_client()
            .read_block_range(attempt("ReadBlock"), group_name(), &planned_read(vec![first, second]))
            .await
            .expect_err("empty chunk fails closed");

        assert!(matches!(error, ClientError::Worker(message) if message.contains("empty chunk")));
        assert_eq!(first_state.read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.read_calls.load(Ordering::SeqCst), 0);
        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
    }

    #[tokio::test]
    async fn write_failure_after_ack_is_unknown_and_never_tries_another_worker() {
        let first_state = Arc::new(MockWorkerState::new(
            ReadFailure::None,
            WriteBehavior::AckThenUnavailable,
        ));
        let second_state = Arc::new(MockWorkerState::new(ReadFailure::None, WriteBehavior::Success));
        let (first, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
        let (second, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;

        let error = grpc_client()
            .write_block(
                attempt("WriteBlock"),
                write_target(vec![first, second]),
                Bytes::from_static(b"data"),
            )
            .await
            .expect_err("transport loss after ack has unknown outcome");

        assert!(matches!(error, ClientError::UnknownOutcome(message) if message.contains("WriteBlock")));
        assert_eq!(first_state.write_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.write_calls.load(Ordering::SeqCst), 0);
        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
    }
}
