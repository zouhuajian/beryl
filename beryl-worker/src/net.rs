// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! gRPC WorkerDataService adapter and server entry point.

use crate::config::{WorkerNetConfig, WorkerRegistrationConfig};
use crate::control::RegistrationState;
use crate::error::{WorkerError, WorkerResult};
use crate::observe;
use crate::runtime::{
    ActiveBlockRead, ActiveBlockWrite, DataRpcPermit, ReadBlockRequest, WorkerRuntime, WriteBlockRequest,
};
use anyhow::Context;
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RpcErrorDetail, WorkerErrorKind};
use beryl_common::grpc_server::{spawn_grpc_server, GrpcServerHandle};
use beryl_common::header::TraceContext;
use beryl_common::header::{
    HEADER_WORKER_DATA_ERROR_DETAIL, HEADER_WORKER_DATA_REJECTION, WORKER_DATA_ERROR_DETAIL_V1,
    WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT,
};
use beryl_common::observe::propagation::extract_trace_context;
use beryl_proto::common::RequestHeaderProto;
use beryl_proto::common::{ClientInfoProto, ErrorDetailProto};
use beryl_proto::convert::{self as proto_convert, require_worker_run_id};
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_proto::metadata::AuthorizeBlockWriteRequestProto;
use beryl_proto::worker::worker_data_service_server::{WorkerDataService, WorkerDataServiceServer};
use beryl_proto::worker::write_block_request_proto::Payload;
use beryl_proto::worker::{
    DataRequestHeaderProto, DataResponseHeaderProto, ReadBlockChunkProto, ReadBlockRequestProto,
    WriteBlockCommandProto, WriteBlockRequestProto, WriteBlockResponseProto,
};
use beryl_types::range::ByteRange;
use beryl_types::{CallId, ClientId, GroupName, WorkerRunId};
use bytes::Bytes;
use futures::{stream, Stream, StreamExt};
use prost::Message;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::service::Routes;
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::Span;

/// Worker data service with independent process-wide read and write admission.
#[derive(Clone)]
pub struct WorkerDataServiceImpl {
    worker_runtime: Arc<WorkerRuntime>,
    registration_state: Arc<RegistrationState>,
    read_slots: Arc<Semaphore>,
    write_slots: Arc<Semaphore>,
    metadata: FileSystemServiceProtoClient<Channel>,
    control_client_id: ClientId,
}

impl WorkerDataServiceImpl {
    /// Creates independent process-wide admission pools for the two data modes.
    pub fn new(
        worker_runtime: Arc<WorkerRuntime>,
        registration_state: Arc<RegistrationState>,
        max_concurrent_reads: usize,
        max_concurrent_writes: usize,
        metadata: &WorkerRegistrationConfig,
    ) -> Result<Self, WorkerError> {
        let endpoint = metadata
            .validate()
            .map_err(|e| WorkerError::InvalidArgument(e.message))?;
        let timeout = std::time::Duration::from_millis(metadata.request_timeout_ms);
        let channel = endpoint.connect_timeout(timeout).timeout(timeout).connect_lazy();
        Ok(Self {
            worker_runtime,
            registration_state,
            read_slots: Arc::new(Semaphore::new(max_concurrent_reads)),
            write_slots: Arc::new(Semaphore::new(max_concurrent_writes)),
            metadata: FileSystemServiceProtoClient::new(channel),
            control_client_id: ClientId::generate(),
        })
    }

    /// Checks the live session before local IO; transport failure cannot grant authority.
    async fn authorize_write(&self, req: &WriteBlockRequest) -> Result<u64, WorkerError> {
        let registration = self
            .registration_state
            .registration(&req.group_name)
            .ok_or_else(|| WorkerError::Unavailable("Worker registration is unavailable".into()))?;
        let header = RequestHeaderProto {
            client: Some(ClientInfoProto {
                call_id: CallId::new().to_string(),
                client_id: Some(self.control_client_id.into()),
                client_name: "worker-write-authorization".into(),
            }),
            group_name: req.group_name.to_string(),
            ..Default::default()
        };
        let response = self
            .metadata
            .clone()
            .authorize_block_write(AuthorizeBlockWriteRequestProto {
                header: Some(header),
                block_id: Some(req.block_id.into()),
                worker_id: registration.worker_id.as_raw(),
                worker_run_id: req.worker_run_id.to_string(),
                fencing_token: Some(req.fencing_token.into()),
                write_offset: req.write_offset,
                block_size: req.block_size,
                tier: beryl_proto::common::TierProto::from(req.tier) as i32,
            })
            .await
            .map_err(|e| WorkerError::Unavailable(format!("Metadata write authorization failed: {e}")))?
            .into_inner();
        let header = response
            .header
            .ok_or_else(|| WorkerError::Corrupt("write authorization response has no header".into()))?;
        if let Some(error) = header.error {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Worker(WorkerErrorKind::Fencing),
                message: error.message,
            });
        }
        if header.group_name != req.group_name.as_str() || response.visible_len > req.write_offset {
            return Err(WorkerError::Corrupt(
                "write authorization response has inconsistent scope".into(),
            ));
        }
        Ok(response.visible_len)
    }

    /// Acquires one read slot without queuing so write saturation cannot block reads.
    fn acquire_read_rpc(&self) -> Result<DataRpcPermit, Status> {
        Self::acquire_data_rpc(Arc::clone(&self.read_slots), "read")
    }

    /// Acquires one write slot without queuing so read saturation cannot block writes.
    fn acquire_write_rpc(&self) -> Result<DataRpcPermit, Status> {
        Self::acquire_data_rpc(Arc::clone(&self.write_slots), "write")
    }

    /// Rejects one mode before Worker write IO or read IO begins when its pool is full.
    fn acquire_data_rpc(slots: Arc<Semaphore>, mode: &'static str) -> Result<DataRpcPermit, Status> {
        match slots.try_acquire_owned() {
            Ok(permit) => Ok(DataRpcPermit::new(permit, mode)),
            Err(_) => {
                observe::record_data_rpc_capacity_rejection(mode);
                let mut metadata = MetadataMap::new();
                metadata.insert(
                    HEADER_WORKER_DATA_REJECTION,
                    MetadataValue::from_static(WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT),
                );
                Err(Status::with_metadata(
                    Code::ResourceExhausted,
                    format!("Worker {mode} RPC capacity exhausted"),
                    metadata,
                ))
            }
        }
    }

    fn error_response_header(header: Option<DataRequestHeaderProto>, error: WorkerError) -> DataResponseHeaderProto {
        DataResponseHeaderProto {
            client: Some(header.and_then(|value| value.client).unwrap_or_default()),
            error: Some(Self::error_detail(&error)),
        }
    }

    fn error_detail(error: &WorkerError) -> ErrorDetailProto {
        let rpc_error: RpcErrorDetail = error.clone().into();
        beryl_proto::convert::rpc_error_to_proto(&rpc_error)
    }

    /// Preserves the structured Worker error contract on streaming status errors.
    fn data_error_status(header: Option<DataRequestHeaderProto>, error: WorkerError) -> Status {
        let status = error.to_status();
        let details = Self::error_response_header(header, error).encode_to_vec();
        let mut metadata = MetadataMap::new();
        metadata.insert(
            HEADER_WORKER_DATA_ERROR_DETAIL,
            MetadataValue::from_static(WORKER_DATA_ERROR_DETAIL_V1),
        );
        Status::with_details_and_metadata(status.code(), status.message(), Bytes::from(details), metadata)
    }

    fn ensure_group_ready_for_run(
        &self,
        group_name: &str,
        worker_run_id: &str,
    ) -> Result<(GroupName, WorkerRunId), WorkerError> {
        let group_name = GroupName::parse(group_name)
            .map_err(|error| WorkerError::InvalidArgument(format!("group_name invalid: {error}")))?;
        let requested = require_worker_run_id(worker_run_id, "worker_run_id").map_err(WorkerError::InvalidArgument)?;
        let Some(registration) = self.registration_state.registration(&group_name) else {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
                message: format!("worker is not registered for metadata group {group_name}"),
            });
        };
        if !self.registration_state.is_ready(&group_name) {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
                message: format!("worker is not ready for metadata group {group_name}"),
            });
        }
        if requested != registration.worker_run_id {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                message: format!(
                    "worker_run_id mismatch: requested={requested}, current={}",
                    registration.worker_run_id
                ),
            });
        }
        Ok((group_name, requested))
    }

    /// Consumes the command before returning the response stream, making the
    /// first empty response an exact acknowledgement of block-open checkpoint.
    async fn begin_write_block<S>(
        &self,
        mut requests: S,
        rpc_permit: DataRpcPermit,
        transport_context: &TraceContext,
        started: Instant,
    ) -> Result<WriteBlockState<S>, Status>
    where
        S: Stream<Item = Result<WriteBlockRequestProto, Status>> + Unpin,
    {
        let first = match requests.next().await {
            Some(Ok(request)) => request,
            Some(Err(status)) => {
                observe::record_data_rpc(
                    "write_block",
                    "error",
                    status_error_kind(&status),
                    started.elapsed().as_secs_f64(),
                );
                return Err(status);
            }
            None => {
                let error = WorkerError::InvalidArgument("WriteBlock requires a command payload".to_string());
                observe::record_data_rpc(
                    "write_block",
                    "error",
                    observe::worker_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                return Err(Self::data_error_status(None, error));
            }
        };
        let command = match first.payload {
            Some(Payload::Command(command)) => command,
            Some(Payload::Data(_)) | None => {
                let error = WorkerError::InvalidArgument("first WriteBlock payload must be command".to_string());
                observe::record_data_rpc(
                    "write_block",
                    "error",
                    observe::worker_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                return Err(Self::data_error_status(None, error));
            }
        };
        record_transport_context(transport_context);
        let header = command.header.clone();
        let (group_name, worker_run_id) = self
            .ensure_group_ready_for_run(&command.group_name, &command.worker_run_id)
            .map_err(|error| {
                let error_kind = observe::worker_error_kind(&error);
                observe::record_stream_open("write", "error", error_kind);
                observe::record_data_rpc("write_block", "error", error_kind, started.elapsed().as_secs_f64());
                Self::data_error_status(header.clone(), error)
            })?;
        let domain = proto_to_write_block_request(*command, group_name, worker_run_id).map_err(|error| {
            let error_kind = observe::worker_error_kind(&error);
            observe::record_stream_open("write", "error", error_kind);
            observe::record_data_rpc("write_block", "error", error_kind, started.elapsed().as_secs_f64());
            Self::data_error_status(header.clone(), error)
        })?;
        let block_pin = self
            .worker_runtime
            .pin_write_authorization(&domain)
            .map_err(|error| Self::data_error_status(header.clone(), error))?;
        let visible_len = self
            .authorize_write(&domain)
            .await
            .map_err(|error| Self::data_error_status(header.clone(), error))?;
        let write = self
            .worker_runtime
            .begin_block_write(domain, rpc_permit, block_pin, visible_len)
            .await
            .map_err(|error| {
                let error_kind = observe::worker_error_kind(&error);
                observe::record_stream_open("write", "error", error_kind);
                observe::record_data_rpc("write_block", "error", error_kind, started.elapsed().as_secs_f64());
                Self::data_error_status(header.clone(), error)
            })?;
        observe::record_stream_open("write", "ok", "none");
        Ok(WriteBlockState {
            worker_runtime: Arc::clone(&self.worker_runtime),
            requests,
            write: Some(write),
            request_header: header,
            started,
            acknowledgement_pending: true,
            outcome: StreamOutcome::Active,
        })
    }
}

#[derive(Clone, Copy)]
enum StreamOutcome {
    Active,
    Success,
    Error(&'static str),
}

/// Owns one read response stream, its pin, and exact-once lifecycle metrics.
struct ReadBlockState {
    worker_runtime: Arc<WorkerRuntime>,
    read: ActiveBlockRead,
    request_header: Option<DataRequestHeaderProto>,
    started: Instant,
    outcome: StreamOutcome,
}

impl ReadBlockState {
    async fn next(mut self) -> Option<(Result<ReadBlockChunkProto, Status>, Self)> {
        if !matches!(self.outcome, StreamOutcome::Active) {
            return None;
        }
        match self.worker_runtime.read_block_chunk(&mut self.read).await {
            Ok(Some(data)) => {
                observe::record_stream_frame("read", "ok", "none", data.len() as u64);
                Some((Ok(ReadBlockChunkProto { data }), self))
            }
            Ok(None) => {
                self.outcome = StreamOutcome::Success;
                None
            }
            Err(error) => {
                let error_kind = observe::worker_error_kind(&error);
                self.outcome = StreamOutcome::Error(error_kind);
                observe::record_stream_frame("read", "error", error_kind, 0);
                let status = WorkerDataServiceImpl::data_error_status(self.request_header.clone(), error);
                Some((Err(status), self))
            }
        }
    }
}

impl Drop for ReadBlockState {
    fn drop(&mut self) {
        let (status, error_kind) = outcome_labels(self.outcome);
        observe::record_data_rpc("read_block", status, error_kind, self.started.elapsed().as_secs_f64());
    }
}

/// Owns the inbound stream and its single block write until durable Ready,
/// explicit failure cleanup, or cancellation-triggered deferred cleanup.
struct WriteBlockState<S> {
    worker_runtime: Arc<WorkerRuntime>,
    requests: S,
    write: Option<ActiveBlockWrite>,
    request_header: Option<DataRequestHeaderProto>,
    started: Instant,
    acknowledgement_pending: bool,
    outcome: StreamOutcome,
}

impl<S> WriteBlockState<S>
where
    S: Stream<Item = Result<WriteBlockRequestProto, Status>> + Unpin,
{
    async fn next(mut self) -> Option<(Result<WriteBlockResponseProto, Status>, Self)> {
        if self.acknowledgement_pending {
            self.acknowledgement_pending = false;
            return Some((Ok(WriteBlockResponseProto {}), self));
        }
        if !matches!(self.outcome, StreamOutcome::Active) {
            return None;
        }

        loop {
            let next = tokio::select! {
                biased;
                _ = self.write.as_ref().expect("active write").retired() => {
                    Err(WorkerError::Cancelled("block writer authority was revoked".into()))
                }
                next = self.requests.next() => Ok(next),
            };
            let next = match next {
                Ok(next) => next,
                Err(error) => return Some(self.fail(error).await),
            };
            match next {
                Some(Ok(request)) => match request.payload {
                    Some(Payload::Data(data)) => {
                        let len = data.len() as u64;
                        let write = self.write.as_mut().expect("active response state owns a block write");
                        if let Err(error) = self.worker_runtime.write_block_data(write, data).await {
                            observe::record_stream_frame("write", "error", observe::worker_error_kind(&error), len);
                            return Some(self.fail(error).await);
                        }
                        observe::record_stream_frame("write", "ok", "none", len);
                    }
                    Some(Payload::Command(_)) | None => {
                        let error = WorkerError::InvalidArgument(
                            "every WriteBlock payload after command must be data".to_string(),
                        );
                        return Some(self.fail(error).await);
                    }
                },
                Some(Err(status)) => {
                    let error_kind = status_error_kind(&status);
                    self.outcome = StreamOutcome::Error(error_kind);
                    self.abort_active().await;
                    return Some((Err(status), self));
                }
                None => {
                    let result = self
                        .worker_runtime
                        .finish_block_write(self.write.as_mut().expect("active response state owns a block write"))
                        .await;
                    match result {
                        Ok(()) => {
                            self.write.take();
                            self.outcome = StreamOutcome::Success;
                            return None;
                        }
                        Err(error) => return Some(self.fail(error).await),
                    }
                }
            }
        }
    }

    async fn fail(mut self, error: WorkerError) -> (Result<WriteBlockResponseProto, Status>, Self) {
        let error_kind = observe::worker_error_kind(&error);
        self.outcome = StreamOutcome::Error(error_kind);
        self.abort_active().await;
        let status = WorkerDataServiceImpl::data_error_status(self.request_header.clone(), error);
        (Err(status), self)
    }

    async fn abort_active(&mut self) {
        let write = self.write.take().expect("active response state owns a block write");
        if let Err(error) = self.worker_runtime.abort_block_write(write).await {
            tracing::warn!(
                target: "worker.state",
                op = "AbortBlockWrite",
                error_code = observe::worker_error_kind(&error),
                error = %error,
                "Failed block write cleanup retained local resources for retry"
            );
        }
    }
}

impl<S> Drop for WriteBlockState<S> {
    fn drop(&mut self) {
        let (status, error_kind) = outcome_labels(self.outcome);
        observe::record_data_rpc("write_block", status, error_kind, self.started.elapsed().as_secs_f64());
    }
}

#[tonic::async_trait]
impl WorkerDataService for WorkerDataServiceImpl {
    type ReadBlockStream = Pin<Box<dyn Stream<Item = Result<ReadBlockChunkProto, Status>> + Send>>;
    async fn read_block(
        &self,
        request: Request<ReadBlockRequestProto>,
    ) -> Result<Response<Self::ReadBlockStream>, Status> {
        let started = Instant::now();
        let rpc_permit = Arc::new(self.acquire_read_rpc().inspect_err(|status| {
            observe::record_data_rpc(
                "read_block",
                "error",
                status_error_kind(status),
                started.elapsed().as_secs_f64(),
            );
        })?);
        let transport_context = extract_trace_context(request.metadata());
        let request = request.into_inner();
        record_transport_context(&transport_context);
        let header = request.header.clone();
        let (group_name, _) = self
            .ensure_group_ready_for_run(&request.group_name, &request.worker_run_id)
            .map_err(|error| {
                let error_kind = observe::worker_error_kind(&error);
                observe::record_data_rpc("read_block", "error", error_kind, started.elapsed().as_secs_f64());
                Self::data_error_status(header.clone(), error)
            })?;
        let domain = proto_to_read_block_request(request, group_name).map_err(|error| {
            let error_kind = observe::worker_error_kind(&error);
            observe::record_data_rpc("read_block", "error", error_kind, started.elapsed().as_secs_f64());
            Self::data_error_status(header.clone(), error)
        })?;
        let read = self
            .worker_runtime
            .begin_block_read(domain, rpc_permit)
            .await
            .map_err(|error| {
                let error_kind = observe::worker_error_kind(&error);
                observe::record_data_rpc("read_block", "error", error_kind, started.elapsed().as_secs_f64());
                Self::data_error_status(header.clone(), error)
            })?;
        let state = ReadBlockState {
            worker_runtime: Arc::clone(&self.worker_runtime),
            read,
            request_header: header,
            started,
            outcome: StreamOutcome::Active,
        };
        Ok(Response::new(Box::pin(stream::unfold(state, |state| state.next()))))
    }

    type WriteBlockStream = Pin<Box<dyn Stream<Item = Result<WriteBlockResponseProto, Status>> + Send>>;

    async fn write_block(
        &self,
        request: Request<Streaming<WriteBlockRequestProto>>,
    ) -> Result<Response<Self::WriteBlockStream>, Status> {
        let started = Instant::now();
        let rpc_permit = self.acquire_write_rpc().inspect_err(|status| {
            observe::record_data_rpc(
                "write_block",
                "error",
                status_error_kind(status),
                started.elapsed().as_secs_f64(),
            );
        })?;
        let transport_context = extract_trace_context(request.metadata());
        let state = self
            .begin_write_block(request.into_inner(), rpc_permit, &transport_context, started)
            .await?;
        Ok(Response::new(Box::pin(stream::unfold(state, |state| state.next()))))
    }
}

fn record_transport_context(context: &TraceContext) {
    if let Some(traceparent) = &context.traceparent {
        Span::current().record("traceparent", traceparent);
    }
}

fn outcome_labels(outcome: StreamOutcome) -> (&'static str, &'static str) {
    match outcome {
        StreamOutcome::Active => ("cancelled", "cancelled"),
        StreamOutcome::Success => ("ok", "none"),
        StreamOutcome::Error(error_kind) => ("error", error_kind),
    }
}

fn status_error_kind(status: &Status) -> &'static str {
    match status.code() {
        Code::Ok => "none",
        Code::InvalidArgument => "invalid_argument",
        Code::NotFound => "not_found",
        Code::FailedPrecondition => "failed_precondition",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::Unavailable => "unavailable",
        Code::DeadlineExceeded => "timeout",
        Code::Unimplemented => "unimplemented",
        Code::Cancelled => "cancelled",
        Code::Internal => "internal",
        _ => "rpc_status",
    }
}

/// Converts the remaining read fields after group/run admission.
fn proto_to_read_block_request(proto: ReadBlockRequestProto, group_name: GroupName) -> WorkerResult<ReadBlockRequest> {
    let block_id =
        proto_convert::required_block_id(proto.block_id, "block_id").map_err(WorkerError::InvalidArgument)?;
    let byte_range = proto
        .byte_range
        .ok_or_else(|| WorkerError::InvalidArgument("missing byte_range".to_string()))?;

    Ok(ReadBlockRequest {
        group_name,
        block_id,
        byte_range: ByteRange {
            offset: byte_range.offset,
            len: byte_range.len,
        },

        block_size: proto.block_size,
        effective_len: proto.effective_len,
        frame_size: proto.frame_size,
    })
}

/// Converts the first write command using the identity parsed during admission.
fn proto_to_write_block_request(
    proto: WriteBlockCommandProto,
    group_name: GroupName,
    worker_run_id: WorkerRunId,
) -> WorkerResult<WriteBlockRequest> {
    let block_id =
        proto_convert::required_block_id(proto.block_id, "block_id").map_err(WorkerError::InvalidArgument)?;

    let tier = proto_convert::parse_known_tier(proto.tier)
        .map_err(|error| WorkerError::InvalidArgument(format!("tier invalid: {error}")))?;

    Ok(WriteBlockRequest {
        group_name,
        block_id,
        worker_run_id,
        fencing_token: proto_convert::required_fencing_token(proto.fencing_token, "fencing_token")
            .map_err(WorkerError::InvalidArgument)?,
        write_offset: proto.write_offset,
        block_size: proto.block_size,

        tier,
    })
}

/// Binds the Worker data plane under the process-owned connection tracker.
pub fn spawn_worker_data_with_registration(
    bind: SocketAddr,
    config: &WorkerNetConfig,
    worker_runtime: Arc<WorkerRuntime>,
    registration_state: Arc<RegistrationState>,
    metadata: &WorkerRegistrationConfig,
) -> anyhow::Result<GrpcServerHandle> {
    let service = WorkerDataServiceImpl::new(
        worker_runtime,
        registration_state,
        config.max_concurrent_reads,
        config.max_concurrent_writes,
        metadata,
    )?;
    let routes = Routes::new(
        WorkerDataServiceServer::new(service)
            .max_decoding_message_size(beryl_proto::MAX_WORKER_DATA_MESSAGE_SIZE)
            .max_encoding_message_size(beryl_proto::MAX_WORKER_DATA_MESSAGE_SIZE),
    );
    spawn_grpc_server(bind, routes).context("failed to bind Worker gRPC listener")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{Registration, RegistrationState};
    use crate::runtime::{WorkerRuntime, WriteBlockRequest};
    use crate::store::block::{BlockState, FullBlockFileStore, LocalBlockStore};
    use beryl_proto::worker::write_block_request_proto::Payload;
    use beryl_proto::worker::WriteBlockRequestProto;
    use beryl_types::ids::{BlockId, BlockIndex, InodeId, WorkerId};

    use beryl_types::{GroupName, WorkerRunId};
    use bytes::Bytes;
    use futures::stream;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use tokio::time::Instant as TokioInstant;
    use tonic::{Code, Status};

    fn group_name() -> GroupName {
        GroupName::parse("root").expect("group name")
    }

    fn block_id() -> BlockId {
        BlockId::new(InodeId::new(7), BlockIndex::new(3))
    }

    fn registered_service() -> (TempDir, Arc<FullBlockFileStore>, WorkerDataServiceImpl, WorkerRunId) {
        let temp = TempDir::new().expect("tempdir");
        let store = Arc::new(FullBlockFileStore::new(temp.path().to_path_buf()));
        let worker_runtime = Arc::new(WorkerRuntime::with_local_store(group_name(), 512, 2048, store.clone()));
        let registrations = Arc::new(RegistrationState::new());
        let worker_run_id = WorkerRunId::new();
        registrations.record_registered(Registration {
            group_name: group_name(),
            worker_id: WorkerId::new(5),
            worker_run_id,
        });
        registrations.record_heartbeat_success(&group_name(), Duration::from_secs(30));
        (
            temp,
            store,
            WorkerDataServiceImpl::new(
                worker_runtime,
                registrations,
                1,
                1,
                &WorkerRegistrationConfig::default(),
            )
            .unwrap(),
            worker_run_id,
        )
    }

    // Protocol tests start after online authorization; real RPC authorization is covered by e2e.
    async fn authorized_write<S>(
        service: &WorkerDataServiceImpl,
        worker_run_id: WorkerRunId,
        requests: S,
    ) -> WriteBlockState<S> {
        let req = WriteBlockRequest {
            group_name: group_name(),
            worker_run_id,
            block_id: block_id(),
            block_size: 4096,
            fencing_token: beryl_types::FencingToken::new(ClientId::new(9), beryl_types::LeaseEpoch::new(55)),
            write_offset: 0,
            tier: beryl_types::Tier::Hdd,
        };
        let permit = service.acquire_write_rpc().expect("write capacity");
        let pin = service.worker_runtime.pin_write_authorization(&req).unwrap();
        let write = service
            .worker_runtime
            .begin_block_write(req, permit, pin, 0)
            .await
            .unwrap();
        WriteBlockState {
            worker_runtime: service.worker_runtime.clone(),
            requests,
            write: Some(write),
            request_header: None,
            started: Instant::now(),
            acknowledgement_pending: true,
            outcome: StreamOutcome::Active,
        }
    }

    #[tokio::test]
    async fn write_block_acknowledges_open_then_checkpoints_on_request_eof() {
        let (_temp, store, service, worker_run_id) = registered_service();
        let requests = stream::iter(vec![
            Ok(WriteBlockRequestProto {
                payload: Some(Payload::Data(Bytes::from_static(b"abc"))),
            }),
            Ok(WriteBlockRequestProto {
                payload: Some(Payload::Data(Bytes::from_static(b"def"))),
            }),
        ]);
        let state = authorized_write(&service, worker_run_id, requests).await;
        let (ack, state) = state.next().await.expect("acknowledgement");
        ack.expect("acknowledgement is successful");
        assert!(state.next().await.is_none());

        let meta = store.load_meta(&group_name(), block_id()).expect("ready meta");
        assert_eq!(meta.block_state, BlockState::Ready);
        assert_eq!(meta.durable_len, 6);
    }

    #[tokio::test]
    async fn cancellation_after_ack_releases_write_through_owned_cleanup() {
        let (_temp, _store, service, worker_run_id) = registered_service();
        let requests = stream::empty();
        let state = authorized_write(&service, worker_run_id, requests).await;
        let (_ack, state) = state.next().await.expect("acknowledgement");
        drop(state);
        assert!(
            !service
                .worker_runtime
                .drain_block_writes_until(TokioInstant::now() + Duration::from_secs(1))
                .await
        );

        let replacement = stream::empty();
        let state = authorized_write(&service, worker_run_id, replacement).await;
        let (_ack, state) = state.next().await.expect("replacement acknowledgement");
        drop(state);
    }

    #[tokio::test]
    async fn a_second_command_fails_and_cleans_the_owned_block_write() {
        let (_temp, _store, service, worker_run_id) = registered_service();
        let requests = stream::iter(vec![Ok(WriteBlockRequestProto {
            payload: Some(Payload::Command(Box::default())),
        })]);
        let state = authorized_write(&service, worker_run_id, requests).await;
        let (_ack, state) = state.next().await.expect("acknowledgement");
        let (error, _state) = state.next().await.expect("terminal error");
        assert_eq!(
            error.expect_err("second command must fail").code(),
            Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn read_and_write_capacity_are_independent() {
        let (_temp, _store, service, _worker_run_id) = registered_service();
        let read = service.acquire_read_rpc().expect("read capacity");
        let write = service.acquire_write_rpc().expect("write capacity");
        let assert_capacity_rejection = |status: Status| {
            assert_eq!(status.code(), Code::ResourceExhausted);
            assert_eq!(
                status
                    .metadata()
                    .get(HEADER_WORKER_DATA_REJECTION)
                    .and_then(|value| value.to_str().ok()),
                Some(WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT)
            );
        };

        assert_capacity_rejection(service.acquire_read_rpc().expect_err("read limit"));
        assert_capacity_rejection(service.acquire_write_rpc().expect_err("write limit"));

        drop(read);
        service.acquire_read_rpc().expect("read capacity released");
        assert_capacity_rejection(service.acquire_write_rpc().expect_err("write remains full"));
        drop(write);
    }
    #[tokio::test]
    async fn reclaim_revokes_idle_stream_and_releases_its_pin_without_another_frame() {
        let (_temp, store, service, run) = registered_service();
        let state = authorized_write(&service, run, stream::pending()).await;
        let (_, state) = state.next().await.unwrap();
        let idle = state.next();
        tokio::pin!(idle);
        assert!(futures::poll!(&mut idle).is_pending());
        let reclaim = service
            .worker_runtime
            .reclaim_block(crate::store::block::ReclaimBlockRequest {
                group_name: group_name(),
                block_id: block_id(),
            });
        tokio::pin!(reclaim);
        assert!(futures::poll!(&mut reclaim).is_pending());
        let (error, state) = tokio::time::timeout(Duration::from_secs(1), idle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(error.unwrap_err().code(), Code::Cancelled);
        assert!(state.write.is_none());
        assert!(
            !service
                .worker_runtime
                .drain_block_writes_until(TokioInstant::now() + Duration::from_secs(1))
                .await
        );
        tokio::time::timeout(Duration::from_secs(1), reclaim)
            .await
            .unwrap()
            .unwrap();
        assert!(!store.paths(&group_name(), block_id()).data_path.exists());
        service
            .acquire_write_rpc()
            .expect("retired stream releases its RPC slot");
    }
}
