// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! gRPC WorkerDataService adapter and server entry point.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use beryl_common::header::{HEADER_WORKER_DATA_ERROR_DETAIL, WORKER_DATA_ERROR_DETAIL_V1};
use beryl_proto::common::{ClientInfoProto, ErrorDetailProto};
use beryl_proto::convert::require_worker_run_id;
use beryl_proto::worker::worker_data_service_server::{WorkerDataService, WorkerDataServiceServer};
use beryl_proto::worker::{
    AbortWriteRequestProto, AbortWriteResponseProto, CommitWriteRequestProto, CommitWriteResponseProto,
    DataRequestHeaderProto, DataResponseHeaderProto, OpenWriteStreamRequestProto, OpenWriteStreamResponseProto,
    ReadBlockChunkProto, ReadBlockRequestProto, SyncCommittedBlockRequestProto, SyncCommittedBlockResponseProto,
    WriteStreamRequestProto, WriteStreamResponseProto,
};
use bytes::Bytes;
use futures::{stream, Stream, StreamExt};
use prost::Message;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::service::Routes;
use tonic::{Request, Response, Status};

use crate::control::RegistrationSet;
use crate::data::convert::{
    proto_to_abort_write_request, proto_to_commit_write_request, proto_to_read_block_request, proto_to_stream_id,
    proto_to_sync_committed_block_request, proto_to_write_frame, proto_to_write_open_request, stream_id_to_proto,
};
use crate::data::core::{ActiveBlockRead, WorkerCore};
use crate::error::WorkerError;
use crate::observe;
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_common::observe::propagation::{extract_trace_context, ExtractedContext};
use beryl_types::GroupName;
use tracing::Span;

/// Worker data service implementation.
#[derive(Clone)]
pub struct WorkerDataServiceImpl {
    core: Arc<WorkerCore>,
    registration_state: Arc<RegistrationSet>,
}

impl WorkerDataServiceImpl {
    pub fn new(core: Arc<WorkerCore>, registration_state: Arc<RegistrationSet>) -> Self {
        Self {
            core,
            registration_state,
        }
    }

    fn error_response_header(header: Option<DataRequestHeaderProto>, error: WorkerError) -> DataResponseHeaderProto {
        DataResponseHeaderProto {
            client: Some(header.and_then(|h| h.client).unwrap_or_else(Self::default_client)),
            error: Some(Self::error_detail(&error)),
        }
    }

    fn ok_response_header(header: Option<DataRequestHeaderProto>) -> DataResponseHeaderProto {
        DataResponseHeaderProto {
            client: Some(header.and_then(|h| h.client).unwrap_or_else(Self::default_client)),
            error: None,
        }
    }

    fn default_client() -> ClientInfoProto {
        ClientInfoProto {
            call_id: String::new(),
            client_id: None,
            client_name: String::new(),
        }
    }

    fn error_detail(error: &WorkerError) -> ErrorDetailProto {
        let rpc_error: beryl_common::error::rpc::RpcErrorDetail = error.clone().into();
        beryl_proto::convert::rpc_error_to_proto(&rpc_error)
    }

    /// Encodes one Worker-owned error without turning it into an untyped transport failure.
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

    fn ensure_group_ready(&self, group_name: &str) -> Result<(), WorkerError> {
        let group_name = parse_group_name(group_name)?;
        if self.registration_state.is_ready(&group_name) {
            return Ok(());
        }

        Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
            message: format!("worker is not registered for metadata group {}", group_name),
        })
    }

    fn ensure_group_ready_for_run(&self, group_name: &str, worker_run_id: &str) -> Result<(), WorkerError> {
        let group_name = parse_group_name(group_name)?;
        let requested = require_worker_run_id(worker_run_id, "worker_run_id").map_err(WorkerError::InvalidArgument)?;
        let Some(registration) = self.registration_state.registration_for_group(&group_name) else {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
                message: format!("worker is not registered for metadata group {}", group_name),
            });
        };
        if !self.registration_state.is_ready(&group_name) {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
                message: format!("worker is not ready for metadata group {}", group_name),
            });
        }
        if !requested.matches(registration.worker_run_id) {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                message: format!(
                    "worker_run_id mismatch: requested={}, current={}",
                    requested, registration.worker_run_id
                ),
            });
        }
        Ok(())
    }

    fn ensure_any_ready(&self) -> Result<(), WorkerError> {
        if self.registration_state.is_any_ready() {
            return Ok(());
        }

        Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
            message: "worker is not registered with any metadata group".to_string(),
        })
    }

    pub(crate) async fn handle_write_frames<S>(&self, mut frames: S) -> Result<WriteStreamResponseProto, Status>
    where
        S: Stream<Item = Result<WriteStreamRequestProto, Status>> + Unpin,
    {
        let mut response = WriteStreamResponseProto {
            accepted: true,
            last_acked_seq: 0,
            written_through: 0,
        };
        let mut owned_stream_id = None;
        let mut cancellation_cleanup = WriteStreamCancellation::new(&self.core);
        while let Some(frame) = frames.next().await {
            let frame = match frame {
                Ok(frame) => frame,
                Err(status) => {
                    observe::record_stream_frame("write", "error", status_error_kind(&status), 0);
                    cleanup_write_stream_after_error(&self.core, owned_stream_id).await?;
                    cancellation_cleanup.disarm();
                    return Err(status);
                }
            };
            let frame_bytes = frame.data.len() as u64;
            let frame_stream_id = proto_to_stream_id(frame.stream_id, "stream_id").ok();
            let domain = match proto_to_write_frame(frame) {
                Ok(domain) => domain,
                Err(error) => {
                    observe::record_stream_frame("write", "error", observe::worker_error_kind(&error), frame_bytes);
                    cleanup_write_stream_after_error(&self.core, owned_stream_id.or(frame_stream_id)).await?;
                    cancellation_cleanup.disarm();
                    return Err(error.to_status());
                }
            };
            if let Some(owned) = owned_stream_id {
                if domain.stream_id != owned {
                    let error = WorkerError::InvalidArgument(format!(
                        "WriteStream changed stream identity: expected={owned}, actual={}",
                        domain.stream_id
                    ));
                    observe::record_stream_frame("write", "error", observe::worker_error_kind(&error), frame_bytes);
                    cleanup_write_stream_after_error(&self.core, Some(owned)).await?;
                    cancellation_cleanup.disarm();
                    return Err(error.to_status());
                }
            } else if self.core.stream_manager().contains(domain.stream_id) {
                owned_stream_id = Some(domain.stream_id);
                cancellation_cleanup.claim_stream(domain.stream_id);
            }
            let result = match self.core.write_stream(domain).await {
                Ok(result) => result,
                Err(error) => {
                    observe::record_stream_frame("write", "error", observe::worker_error_kind(&error), frame_bytes);
                    cleanup_write_stream_after_error(&self.core, owned_stream_id).await?;
                    cancellation_cleanup.disarm();
                    return Err(error.to_status());
                }
            };
            debug_assert!(
                owned_stream_id.is_some(),
                "accepted WriteStream frame must own a write stream"
            );
            observe::record_stream_frame("write", "ok", "none", frame_bytes);
            response = WriteStreamResponseProto {
                accepted: result.accepted,
                last_acked_seq: result.last_acked_seq,
                written_through: result.written_through,
            };
            if !response.accepted {
                cleanup_write_stream_after_error(&self.core, owned_stream_id).await?;
                cancellation_cleanup.disarm();
                break;
            }
        }
        cancellation_cleanup.disarm();
        Ok(response)
    }
}

/// Converts transport-task cancellation into a synchronous Retiring request.
///
/// Drop performs no filesystem IO and starts no detached task. The process-owned
/// maintenance loop completes the idempotent staging cleanup.
struct WriteStreamCancellation<'a> {
    core: &'a WorkerCore,
    stream_id: Option<beryl_types::StreamId>,
    armed: bool,
}

impl<'a> WriteStreamCancellation<'a> {
    fn new(core: &'a WorkerCore) -> Self {
        Self {
            core,
            stream_id: None,
            armed: true,
        }
    }

    fn claim_stream(&mut self, stream_id: beryl_types::StreamId) {
        debug_assert!(self.stream_id.is_none(), "WriteStream ownership is immutable");
        self.stream_id = Some(stream_id);
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WriteStreamCancellation<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(stream_id) = self.stream_id {
            self.core.stream_manager().request_write_retirement(stream_id);
        }
    }
}

/// Terminal outcome recorded when one `ReadBlock` response state is dropped.
#[derive(Clone, Copy)]
enum ReadBlockOutcome {
    Active,
    Success,
    Error(&'static str),
}

/// Owns one response stream, its read pin, and exact-once lifecycle metrics.
struct ReadBlockState {
    core: Arc<WorkerCore>,
    read: ActiveBlockRead,
    request_header: Option<DataRequestHeaderProto>,
    started: Instant,
    outcome: ReadBlockOutcome,
}

impl ReadBlockState {
    async fn next(mut self) -> Option<(Result<ReadBlockChunkProto, Status>, Self)> {
        if !matches!(self.outcome, ReadBlockOutcome::Active) {
            return None;
        }

        match self.core.read_block_chunk(&mut self.read).await {
            Ok(Some(data)) => {
                observe::record_stream_frame("read", "ok", "none", data.len() as u64);
                Some((Ok(ReadBlockChunkProto { data }), self))
            }
            Ok(None) => {
                self.outcome = ReadBlockOutcome::Success;
                None
            }
            Err(error) => {
                let error_kind = observe::worker_error_kind(&error);
                self.outcome = ReadBlockOutcome::Error(error_kind);
                observe::record_stream_frame("read", "error", error_kind, 0);
                let status = WorkerDataServiceImpl::data_error_status(self.request_header.clone(), error);
                Some((Err(status), self))
            }
        }
    }
}

impl Drop for ReadBlockState {
    fn drop(&mut self) {
        let (status, error_kind) = match self.outcome {
            ReadBlockOutcome::Active => ("cancelled", "cancelled"),
            ReadBlockOutcome::Success => ("ok", "none"),
            ReadBlockOutcome::Error(error_kind) => ("error", error_kind),
        };
        observe::decrement_stream_inflight("read");
        observe::record_data_rpc("read_block", status, error_kind, self.started.elapsed().as_secs_f64());
    }
}

#[tonic::async_trait]
impl WorkerDataService for WorkerDataServiceImpl {
    type ReadBlockStream = Pin<Box<dyn futures::Stream<Item = Result<ReadBlockChunkProto, Status>> + Send>>;

    async fn read_block(
        &self,
        request: Request<ReadBlockRequestProto>,
    ) -> Result<Response<Self::ReadBlockStream>, Status> {
        let started = Instant::now();
        let transport_context = extract_trace_context(request.metadata());
        let mut request = request.into_inner();
        merge_data_header_transport_context(&mut request.header, &transport_context);
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&request.group_name, &request.worker_run_id) {
            let error_kind = observe::worker_error_kind(&error);
            observe::record_data_rpc("read_block", "error", error_kind, started.elapsed().as_secs_f64());
            return Err(Self::data_error_status(header, error));
        }
        let domain = match proto_to_read_block_request(request) {
            Ok(domain) => domain,
            Err(error) => {
                let error_kind = observe::worker_error_kind(&error);
                observe::record_data_rpc("read_block", "error", error_kind, started.elapsed().as_secs_f64());
                return Err(Self::data_error_status(header, error));
            }
        };
        let read = match self.core.begin_block_read(domain).await {
            Ok(read) => read,
            Err(error) => {
                let error_kind = observe::worker_error_kind(&error);
                observe::record_data_rpc("read_block", "error", error_kind, started.elapsed().as_secs_f64());
                return Err(Self::data_error_status(header, error));
            }
        };
        observe::increment_stream_inflight("read");
        let state = ReadBlockState {
            core: Arc::clone(&self.core),
            read,
            request_header: header,
            started,
            outcome: ReadBlockOutcome::Active,
        };
        let responses = stream::unfold(state, |state| async move { state.next().await });
        Ok(Response::new(Box::pin(responses) as Self::ReadBlockStream))
    }

    async fn open_write_stream(
        &self,
        request: Request<OpenWriteStreamRequestProto>,
    ) -> Result<Response<OpenWriteStreamResponseProto>, Status> {
        let started = Instant::now();
        let transport_context = extract_trace_context(request.metadata());
        let mut request = request.into_inner();
        merge_data_header_transport_context(&mut request.header, &transport_context);
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&request.group_name, &request.worker_run_id) {
            let error_kind = observe::worker_error_kind(&error);
            let duration = started.elapsed().as_secs_f64();
            observe::record_data_rpc("open_write_stream", "error", error_kind, duration);
            observe::record_stream_open("write", "error", error_kind);
            return Ok(Response::new(OpenWriteStreamResponseProto {
                header: Some(Self::error_response_header(header, error)),
                stream_id: None,
                frame_size: 0,
                block_stamp: 0,
                committed_length: 0,
            }));
        }
        let response = match proto_to_write_open_request(request) {
            Ok(domain) => match self.core.open_write(domain).await {
                Ok(result) => OpenWriteStreamResponseProto {
                    header: Some(Self::ok_response_header(header)),
                    stream_id: Some(stream_id_to_proto(result.stream_id)),
                    frame_size: result.frame_size,
                    block_stamp: result.block_stamp,
                    committed_length: result.committed_length,
                },
                Err(error) => OpenWriteStreamResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    stream_id: None,
                    frame_size: 0,
                    block_stamp: 0,
                    committed_length: 0,
                },
            },
            Err(error) => OpenWriteStreamResponseProto {
                header: Some(Self::error_response_header(header, error)),
                stream_id: None,
                frame_size: 0,
                block_stamp: 0,
                committed_length: 0,
            },
        };
        let (status, error_kind) = response_status_error_kind(response.header.as_ref());
        let duration = started.elapsed().as_secs_f64();
        observe::record_data_rpc("open_write_stream", status, error_kind, duration);
        observe::record_stream_open("write", status, error_kind);

        Ok(Response::new(response))
    }

    async fn write_stream(
        &self,
        request: Request<tonic::Streaming<WriteStreamRequestProto>>,
    ) -> Result<Response<WriteStreamResponseProto>, Status> {
        let started = Instant::now();
        record_transport_context(&extract_trace_context(request.metadata()));
        if let Err(error) = self.ensure_any_ready() {
            observe::record_data_rpc(
                "write_stream",
                "error",
                observe::worker_error_kind(&error),
                started.elapsed().as_secs_f64(),
            );
            return Err(error.to_status());
        }
        let response = match self.handle_write_frames(request.into_inner()).await {
            Ok(response) => response,
            Err(status) => {
                observe::record_data_rpc(
                    "write_stream",
                    "error",
                    status_error_kind(&status),
                    started.elapsed().as_secs_f64(),
                );
                return Err(status);
            }
        };
        observe::record_data_rpc("write_stream", "ok", "none", started.elapsed().as_secs_f64());
        Ok(Response::new(response))
    }

    async fn commit_write(
        &self,
        request: Request<CommitWriteRequestProto>,
    ) -> Result<Response<CommitWriteResponseProto>, Status> {
        let started = Instant::now();
        let transport_context = extract_trace_context(request.metadata());
        let mut request = request.into_inner();
        merge_data_header_transport_context(&mut request.header, &transport_context);
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&request.group_name, &request.worker_run_id) {
            let error_kind = observe::worker_error_kind(&error);
            observe::record_data_rpc("commit_write", "error", error_kind, started.elapsed().as_secs_f64());
            observe::record_stream_commit("error", error_kind);
            return Ok(Response::new(CommitWriteResponseProto {
                header: Some(Self::error_response_header(header, error)),
                effective_len: 0,
                block_stamp: 0,
                written_through: 0,
            }));
        }
        let response = match proto_to_commit_write_request(request) {
            Ok(domain) => {
                let stream_id = domain.stream_id;
                match self.core.commit_write(domain).await {
                    Ok(result) => CommitWriteResponseProto {
                        header: Some(Self::ok_response_header(header)),
                        effective_len: result.effective_len,
                        block_stamp: result.block_stamp,
                        written_through: result.written_through,
                    },
                    Err(error) => {
                        if let Err(cleanup_error) = self.core.abort_write_stream_after_error(stream_id).await {
                            tracing::warn!(
                                stream_id = %stream_id,
                                error_code = observe::worker_error_kind(&cleanup_error),
                                error = %cleanup_error,
                                "Failed commit cleanup retained a Retiring write stream"
                            );
                        }
                        CommitWriteResponseProto {
                            header: Some(Self::error_response_header(header, error)),
                            effective_len: 0,
                            block_stamp: 0,
                            written_through: 0,
                        }
                    }
                }
            }
            Err(error) => CommitWriteResponseProto {
                header: Some(Self::error_response_header(header, error)),
                effective_len: 0,
                block_stamp: 0,
                written_through: 0,
            },
        };
        let (status, error_kind) = response_status_error_kind(response.header.as_ref());
        observe::record_data_rpc("commit_write", status, error_kind, started.elapsed().as_secs_f64());
        observe::record_stream_commit(status, error_kind);

        Ok(Response::new(response))
    }

    async fn sync_committed_block(
        &self,
        request: Request<SyncCommittedBlockRequestProto>,
    ) -> Result<Response<SyncCommittedBlockResponseProto>, Status> {
        let started = Instant::now();
        let transport_context = extract_trace_context(request.metadata());
        let mut request = request.into_inner();
        merge_data_header_transport_context(&mut request.header, &transport_context);
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&request.group_name, &request.worker_run_id) {
            observe::record_data_rpc(
                "sync_committed_block",
                "error",
                observe::worker_error_kind(&error),
                started.elapsed().as_secs_f64(),
            );
            return Ok(Response::new(SyncCommittedBlockResponseProto {
                header: Some(Self::error_response_header(header, error)),
                effective_len: 0,
                block_stamp: 0,
            }));
        }
        let response = match proto_to_sync_committed_block_request(request) {
            Ok(domain) => match self.core.sync_committed_block(domain).await {
                Ok(result) => SyncCommittedBlockResponseProto {
                    header: Some(Self::ok_response_header(header)),
                    effective_len: result.effective_len,
                    block_stamp: result.block_stamp,
                },
                Err(error) => SyncCommittedBlockResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    effective_len: 0,
                    block_stamp: 0,
                },
            },
            Err(error) => SyncCommittedBlockResponseProto {
                header: Some(Self::error_response_header(header, error)),
                effective_len: 0,
                block_stamp: 0,
            },
        };
        let (status, error_kind) = response_status_error_kind(response.header.as_ref());
        observe::record_data_rpc(
            "sync_committed_block",
            status,
            error_kind,
            started.elapsed().as_secs_f64(),
        );

        Ok(Response::new(response))
    }

    async fn abort_write(
        &self,
        request: Request<AbortWriteRequestProto>,
    ) -> Result<Response<AbortWriteResponseProto>, Status> {
        let started = Instant::now();
        let transport_context = extract_trace_context(request.metadata());
        let mut request = request.into_inner();
        merge_data_header_transport_context(&mut request.header, &transport_context);
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready(&request.group_name) {
            let error_kind = observe::worker_error_kind(&error);
            observe::record_data_rpc("abort_write", "error", error_kind, started.elapsed().as_secs_f64());
            observe::record_stream_abort("error", error_kind);
            return Ok(Response::new(AbortWriteResponseProto {
                header: Some(Self::error_response_header(header, error)),
                aborted: false,
            }));
        }
        let response = match proto_to_abort_write_request(request) {
            Ok(domain) => match self.core.abort_write(domain).await {
                Ok(result) => AbortWriteResponseProto {
                    header: Some(Self::ok_response_header(header)),
                    aborted: result.aborted,
                },
                Err(error) => AbortWriteResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    aborted: false,
                },
            },
            Err(error) => AbortWriteResponseProto {
                header: Some(Self::error_response_header(header, error)),
                aborted: false,
            },
        };
        let (status, error_kind) = response_status_error_kind(response.header.as_ref());
        observe::record_data_rpc("abort_write", status, error_kind, started.elapsed().as_secs_f64());
        observe::record_stream_abort(status, error_kind);

        Ok(Response::new(response))
    }
}

fn merge_data_header_transport_context(header: &mut Option<DataRequestHeaderProto>, context: &ExtractedContext) {
    record_transport_context(context);
    let Some(header) = header else {
        return;
    };
    if header.trace_context.as_ref().is_some_and(trace_context_proto_is_empty) {
        header.trace_context = None;
    }
    if context.is_empty() {
        return;
    }
    let trace_context = header.trace_context.get_or_insert_with(Default::default);
    if trace_context.traceparent.is_none() {
        trace_context.traceparent = context.traceparent.clone();
    }
    if trace_context.tracestate.is_none() {
        trace_context.tracestate = context.tracestate.clone();
    }
    if trace_context.baggage.is_none() {
        trace_context.baggage = context.baggage.clone();
    }
}

fn trace_context_proto_is_empty(context: &beryl_proto::common::TraceContextProto) -> bool {
    context.traceparent.is_none() && context.tracestate.is_none() && context.baggage.is_none()
}

fn record_transport_context(context: &ExtractedContext) {
    if let Some(traceparent) = &context.traceparent {
        Span::current().record("traceparent", traceparent);
    }
}

fn response_status_error_kind(header: Option<&DataResponseHeaderProto>) -> (&'static str, &'static str) {
    match header.and_then(|header| header.error.as_ref()) {
        Some(_) => ("error", "rpc_error"),
        None => ("ok", "none"),
    }
}

fn status_error_kind(status: &Status) -> &'static str {
    match status.code() {
        tonic::Code::Ok => "none",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::NotFound => "not_found",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DeadlineExceeded => "timeout",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Cancelled => "cancelled",
        tonic::Code::Internal => "internal",
        _ => "rpc_status",
    }
}

async fn cleanup_write_stream_after_error(
    core: &WorkerCore,
    stream_id: Option<beryl_types::StreamId>,
) -> Result<(), Status> {
    let Some(stream_id) = stream_id else {
        return Ok(());
    };
    core.abort_write_stream_after_error(stream_id)
        .await
        .map_err(|error| error.to_status())
}

fn parse_group_name(value: &str) -> Result<GroupName, WorkerError> {
    GroupName::parse(value).map_err(|err| WorkerError::InvalidArgument(format!("group_name invalid: {err}")))
}

/// Builds the Worker data-plane routes retained by the process-owned listener.
pub fn worker_data_routes(core: Arc<WorkerCore>, registration_state: Arc<RegistrationSet>) -> Routes {
    let service = WorkerDataServiceImpl::new(core, registration_state);
    Routes::new(
        WorkerDataServiceServer::new(service)
            .max_decoding_message_size(beryl_proto::MAX_WORKER_DATA_MESSAGE_SIZE)
            .max_encoding_message_size(beryl_proto::MAX_WORKER_DATA_MESSAGE_SIZE),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use beryl_common::error::rpc::{
        ErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction, RpcErrorDetail, WorkerErrorKind,
    };
    use beryl_proto::common::{BlockIdProto, ByteRangeProto, ClientInfoProto, FencingTokenProto};
    use beryl_proto::worker::worker_data_service_server::WorkerDataService;
    use beryl_proto::worker::{
        AbortWriteRequestProto, CommitWriteRequestProto, DataRequestHeaderProto, OpenWriteStreamRequestProto,
        ReadBlockRequestProto, SyncCommittedBlockRequestProto, WriteStreamRequestProto,
    };
    use beryl_types::ids::{BlockId, BlockIndex, ClientId, InodeId, StreamId, WorkerId};
    use beryl_types::layout::BlockFormatId;
    use beryl_types::lease::FencingToken;
    use beryl_types::{GroupName, Tier, WorkerRunId};
    use bytes::Bytes;
    use futures::StreamExt;
    use metrics::{Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use tempfile::TempDir;
    use tonic::Status;

    use crate::config::StoreDirConfig;
    use crate::control::{Registration, RegistrationSet};
    use crate::data::core::{WorkerCore, WorkerCoreResult, WriteFrame, WriteOpenRequest};
    use crate::error::WorkerError;
    use crate::net::server::grpc::WorkerDataServiceImpl;
    use crate::observe::WORKER_STREAM_INFLIGHT;
    use crate::store::block::{
        ChecksumKind, CreateStagingBlockRequest, FullBlockFileStore, FullBlockFileStoreConfig, PublishReadyRequest,
    };
    use crate::store::dirs::StoreDirs;

    const BLOCK_SIZE: u64 = 4096;
    const CHUNK_SIZE: u32 = 1024;
    const BLOCK_STAMP: u64 = 55;

    fn block_id() -> BlockId {
        BlockId::new(InodeId::new(7), BlockIndex::new(3))
    }

    fn group_name() -> GroupName {
        GroupName::parse("root").expect("test group name is valid")
    }

    fn stream_id() -> StreamId {
        StreamId::new((1u128 << 64) | 42)
    }

    fn token() -> FencingToken {
        FencingToken::new(block_id(), ClientId::new(9), 11)
    }

    fn test_block_id_proto() -> BlockIdProto {
        BlockIdProto {
            inode_id: 7,
            block_index: 3,
        }
    }

    fn test_token_proto() -> FencingTokenProto {
        FencingTokenProto {
            block_id: Some(test_block_id_proto()),
            owner: Some(ClientId::new(9).into()),
            epoch: 11,
        }
    }

    fn test_header() -> DataRequestHeaderProto {
        DataRequestHeaderProto {
            client: Some(ClientInfoProto {
                call_id: beryl_types::CallId::new().to_string(),
                client_id: Some(ClientId::new(9).into()),
                client_name: "worker-test".to_string(),
            }),
            trace_context: None,
        }
    }

    fn assert_header_recovery(
        error: &beryl_proto::common::ErrorDetailProto,
        expected_kind: ErrorKind,
    ) -> RpcErrorDetail {
        let rpc_error = beryl_proto::convert::rpc_error_from_proto(error);
        assert_eq!(rpc_error.kind, expected_kind, "{rpc_error:?}");
        rpc_error
    }

    fn assert_header_refresh_metadata(error: &beryl_proto::common::ErrorDetailProto, expected_kind: ErrorKind) {
        let rpc_error = assert_header_recovery(error, expected_kind);
        assert!(
            matches!(rpc_error.recovery, RecoveryAction::RefreshMetadata { .. }),
            "{rpc_error:?}"
        );
    }

    fn assert_not_found<T: std::fmt::Debug>(result: WorkerCoreResult<T>) {
        match result.expect_err("operation should fail") {
            WorkerError::NotFound(_) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    fn test_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
    }

    fn other_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440001".parse().unwrap()
    }

    fn mark_registered(state: &RegistrationSet) {
        state.record_registered(Registration {
            group_name: group_name(),
            worker_id: WorkerId::new(46),
            worker_run_id: test_worker_run_id(),
            advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        });
        state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    }

    fn registered_data_service(core: Arc<WorkerCore>) -> WorkerDataServiceImpl {
        let state = Arc::new(RegistrationSet::new());
        mark_registered(&state);
        WorkerDataServiceImpl::new(core, state)
    }
    fn write_open_request() -> WriteOpenRequest {
        WriteOpenRequest {
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: test_worker_run_id(),
            token: token(),
            block_stamp: BLOCK_STAMP,
            frame_size: 8192,
            block_size: BLOCK_SIZE,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            chunk_size: CHUNK_SIZE,
            checksum_kind: ChecksumKind::None,
            tier: Tier::Hdd,
        }
    }

    pub(super) fn payload() -> Bytes {
        Bytes::from((0..BLOCK_SIZE).map(|idx| (idx % 251) as u8).collect::<Vec<_>>())
    }

    fn core_with_store(default_frame_size: u32, max_frame_size: u32) -> (TempDir, Arc<FullBlockFileStore>, WorkerCore) {
        let temp = TempDir::new().expect("tempdir");
        let store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
            temp.path().to_path_buf(),
        )));
        let core = WorkerCore::with_local_store(
            default_frame_size,
            max_frame_size,
            Duration::from_secs(60),
            store.clone(),
        );
        (temp, store, core)
    }

    pub(super) fn report_store(temp: &TempDir) -> Arc<StoreDirs> {
        Arc::new(
            StoreDirs::open(
                BTreeMap::from([(
                    "hdd0".to_string(),
                    StoreDirConfig {
                        path: temp.path().join("hdd0"),
                        tier: Tier::Hdd,
                        capacity_bytes: 64 * 1024 * 1024,
                    },
                )]),
                0,
                30_000,
            )
            .expect("open report store"),
        )
    }

    fn publish_ready_block(store: &FullBlockFileStore, data: Bytes, block_stamp: u64) {
        store
            .create_staging_block(CreateStagingBlockRequest {
                group_name: group_name(),
                block_id: block_id(),
                block_size: BLOCK_SIZE,
                block_format_id: BlockFormatId::FULL_EFFECTIVE,
                chunk_size: CHUNK_SIZE,
                checksum_kind: ChecksumKind::None,
                tier: Tier::Hdd,
            })
            .expect("create staging block");
        store
            .write_at(&group_name(), block_id(), 0, data.clone())
            .expect("write staging block");
        store
            .publish_ready(PublishReadyRequest {
                group_name: group_name(),
                block_id: block_id(),
                effective_len: data.len() as u64,
                block_stamp,
            })
            .expect("publish ready block");
    }

    fn read_block_proto(offset: u64, len: u32, block_stamp: u64, frame_size: u32) -> ReadBlockRequestProto {
        ReadBlockRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset, len }),
            block_stamp,
            frame_size,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
        }
    }

    fn open_write_proto(frame_size: u32) -> OpenWriteStreamRequestProto {
        OpenWriteStreamRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            block_size: BLOCK_SIZE,
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_stamp: BLOCK_STAMP,
            chunk_size: CHUNK_SIZE,
            token: Some(test_token_proto()),
            frame_size,
            worker_run_id: test_worker_run_id().to_string(),
            tier: beryl_proto::common::TierProto::TierHdd as i32,
        }
    }

    fn commit_write_proto(stream_id: StreamId, commit_seq: u64, effective_len: u64) -> CommitWriteRequestProto {
        CommitWriteRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
            effective_len,
            block_stamp: BLOCK_STAMP,
            token: Some(test_token_proto()),
            commit_seq,
            require_sync: true,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        }
    }

    fn sync_committed_block_proto(block_stamp: u64, expected_block_len: u64) -> SyncCommittedBlockRequestProto {
        SyncCommittedBlockRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            block_stamp,
            expected_block_len,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        }
    }

    #[test]
    fn merge_empty_transport_context_keeps_trace_context_absent() {
        let mut header = Some(DataRequestHeaderProto {
            client: Some(ClientInfoProto {
                call_id: beryl_types::CallId::new().to_string(),
                client_id: Some(beryl_types::ClientId::new(7).into()),
                client_name: "test".to_string(),
            }),
            trace_context: None,
        });

        merge_data_header_transport_context(
            &mut header,
            &ExtractedContext {
                traceparent: None,
                tracestate: None,
                baggage: None,
            },
        );

        assert!(header.expect("header").trace_context.is_none());
    }

    #[tokio::test]
    async fn write_stream_cancellation_discards_partial_staging_state() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let core = Arc::new(core);
        let service = registered_data_service(Arc::clone(&core));
        let open = service
            .open_write_stream(tonic::Request::new(open_write_proto(0)))
            .await
            .expect("open write")
            .into_inner();
        let stream_id = crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");
        let cancelled = Status::cancelled("client cancelled write stream");

        let status = service
            .handle_write_frames(futures::stream::iter(vec![
                Ok(WriteStreamRequestProto {
                    stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                    seq: 1,
                    offset_in_block: 0,
                    data: Bytes::from_static(b"partial"),
                }),
                Err(cancelled),
            ]))
            .await
            .expect_err("cancelled write stream must fail");

        assert_eq!(status.code(), tonic::Code::Cancelled);
        assert!(core.stream_manager().get(stream_id).await.is_none());
        assert!(!store.paths(&group_name(), block_id()).meta_path.exists());
        assert_not_found(store.read_at(&group_name(), block_id(), 0, 1));
        assert!(store.scan_group_blocks(&group_name()).expect("scan group").is_empty());
    }

    #[tokio::test]
    async fn write_stream_identity_switch_cleans_only_the_owned_write() {
        let temp = TempDir::new().expect("tempdir");
        let store = report_store(&temp);
        let core = Arc::new(WorkerCore::with_local_store(
            512,
            2048,
            Duration::from_secs(60),
            store.clone(),
        ));
        let service = registered_data_service(Arc::clone(&core));
        let write = service
            .open_write_stream(tonic::Request::new(open_write_proto(512)))
            .await
            .expect("open write")
            .into_inner();
        let write_stream_id =
            crate::data::convert::proto_to_stream_id(write.stream_id, "stream_id").expect("write stream id");
        assert_eq!(store.report().expect("pending write report").pending_bytes, BLOCK_SIZE);

        let status = service
            .handle_write_frames(futures::stream::iter(vec![
                Ok(WriteStreamRequestProto {
                    stream_id: Some(crate::data::convert::stream_id_to_proto(write_stream_id)),
                    seq: 1,
                    offset_in_block: 0,
                    data: Bytes::from_static(b"partial"),
                }),
                Ok(WriteStreamRequestProto {
                    stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id())),
                    seq: 1,
                    offset_in_block: 0,
                    data: Bytes::from_static(b"abcd"),
                }),
            ]))
            .await
            .expect_err("WriteStream identity switch must fail");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(store.report().expect("cleaned write report").pending_bytes, 0);
        assert!(core.stream_manager().get(write_stream_id).await.is_none());
    }

    #[tokio::test]
    async fn grpc_data_service_maps_success_responses() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let service = registered_data_service(Arc::new(core));

        let response = service
            .open_write_stream(tonic::Request::new(open_write_proto(0)))
            .await
            .expect("open write response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert!(response.stream_id.is_some());
        assert_eq!(response.frame_size, 512);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
        assert_eq!(response.committed_length, 0);

        assert_commit_write_success_response().await;
        assert_sync_committed_block_success_response().await;
    }

    #[tokio::test]
    async fn guarded_data_service_rejects_invalid_registration_context() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let state = Arc::new(RegistrationSet::new());
        let service = WorkerDataServiceImpl::new(Arc::new(core), Arc::clone(&state));

        let response = service
            .open_write_stream(tonic::Request::new(open_write_proto(0)))
            .await
            .expect("open write response")
            .into_inner();
        let error = response.header.expect("header").error.expect("header error");

        assert_header_refresh_metadata(&error, ErrorKind::Metadata(MetadataErrorKind::StaleState));
        assert!(error.message.contains("not registered"));
        assert!(response.stream_id.is_none());

        assert_stale_worker_run_is_rejected().await;
    }

    async fn assert_stale_worker_run_is_rejected() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = registered_data_service(Arc::new(core));
        let mut request = read_block_proto(0, 1024, BLOCK_STAMP, 0);
        request.worker_run_id = other_worker_run_id().to_string();

        let status = match service.read_block(tonic::Request::new(request)).await {
            Ok(_) => panic!("stale Worker run unexpectedly read a block"),
            Err(status) => status,
        };
        let header = DataResponseHeaderProto::decode(status.details()).expect("structured status detail");
        let error = header.error.expect("header error");
        assert_header_refresh_metadata(&error, ErrorKind::Worker(WorkerErrorKind::RunMismatch));
    }

    #[tokio::test]
    async fn write_stream_returns_written_through() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let core = Arc::new(core);
        let state = Arc::new(RegistrationSet::new());
        mark_registered(&state);
        let service = WorkerDataServiceImpl::new(core.clone(), state);
        let open = core.open_write(write_open_request()).await.expect("open write");

        let response = service
            .handle_write_frames(futures::stream::iter(vec![Ok(WriteStreamRequestProto {
                stream_id: Some(crate::data::convert::stream_id_to_proto(open.stream_id)),
                seq: 1,
                offset_in_block: 0,
                data: Bytes::from_static(b"abcd"),
            })]))
            .await
            .expect("write stream response");

        assert!(response.accepted);
        assert_eq!(response.last_acked_seq, 1);
        assert_eq!(response.written_through, 4);
    }

    #[tokio::test]
    async fn write_stream_error_releases_store_dir_pending_reservation() {
        let temp = TempDir::new().expect("tempdir");
        let store = report_store(&temp);
        let core = Arc::new(WorkerCore::with_local_store(
            512,
            2048,
            Duration::from_secs(60),
            store.clone(),
        ));
        let service = registered_data_service(Arc::clone(&core));
        let open = service
            .open_write_stream(tonic::Request::new(open_write_proto(0)))
            .await
            .expect("open write")
            .into_inner();
        let stream_id = crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");

        assert_eq!(store.report().expect("store report").pending_bytes, BLOCK_SIZE);

        let response = service
            .handle_write_frames(futures::stream::iter(vec![Ok(WriteStreamRequestProto {
                stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                seq: 2,
                offset_in_block: 0,
                data: Bytes::from_static(b"abcd"),
            })]))
            .await
            .expect("write stream response");

        assert!(!response.accepted);
        assert_eq!(store.report().expect("store report").pending_bytes, 0);
        assert!(core.stream_manager().get(stream_id).await.is_none());
    }

    #[tokio::test]
    async fn write_stream_frame_error_decrements_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, _store, core) = core_with_store(512, 2048);
                let core = Arc::new(core);
                let service = registered_data_service(Arc::clone(&core));
                let open = service
                    .open_write_stream(tonic::Request::new(open_write_proto(0)))
                    .await
                    .expect("open write")
                    .into_inner();
                let stream_id =
                    crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");

                let response = service
                    .handle_write_frames(futures::stream::iter(vec![Ok(WriteStreamRequestProto {
                        stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                        seq: 2,
                        offset_in_block: 0,
                        data: Bytes::from_static(b"abcd"),
                    })]))
                    .await
                    .expect("write stream response");

                assert!(!response.accepted);
                assert!(core.stream_manager().get(stream_id).await.is_none());
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("write".to_string(), 1.0), ("write".to_string(), -1.0)]
        );
    }

    #[tokio::test]
    async fn commit_write_success_decrements_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, _store, core) = core_with_store(512, 2048);
                let service = registered_data_service(Arc::new(core));
                let open = service
                    .open_write_stream(tonic::Request::new(open_write_proto(2048)))
                    .await
                    .expect("open write")
                    .into_inner();
                let stream_id =
                    crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");
                let data = payload();

                service
                    .handle_write_frames(futures::stream::iter(vec![
                        Ok(WriteStreamRequestProto {
                            stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                            seq: 1,
                            offset_in_block: 0,
                            data: data.slice(0..2048),
                        }),
                        Ok(WriteStreamRequestProto {
                            stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                            seq: 2,
                            offset_in_block: 2048,
                            data: data.slice(2048..4096),
                        }),
                    ]))
                    .await
                    .expect("write frames");

                let response = service
                    .commit_write(tonic::Request::new(commit_write_proto(stream_id, 2, BLOCK_SIZE)))
                    .await
                    .expect("commit write")
                    .into_inner();

                assert!(response.header.expect("header").error.is_none());
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("write".to_string(), 1.0), ("write".to_string(), -1.0)]
        );
    }

    #[tokio::test]
    async fn commit_write_error_decrements_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, _store, core) = core_with_store(512, 2048);
                let core = Arc::new(core);
                let service = registered_data_service(Arc::clone(&core));
                let open = service
                    .open_write_stream(tonic::Request::new(open_write_proto(2048)))
                    .await
                    .expect("open write")
                    .into_inner();
                let stream_id =
                    crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");

                let response = service
                    .commit_write(tonic::Request::new(commit_write_proto(stream_id, 1, BLOCK_SIZE)))
                    .await
                    .expect("commit error response")
                    .into_inner();

                assert!(response.header.expect("header").error.is_some());
                assert!(core.stream_manager().get(stream_id).await.is_none());
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("write".to_string(), 1.0), ("write".to_string(), -1.0)]
        );
    }

    #[tokio::test]
    async fn abort_write_success_decrements_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, _store, core) = core_with_store(512, 2048);
                let service = registered_data_service(Arc::new(core));
                let open = service
                    .open_write_stream(tonic::Request::new(open_write_proto(2048)))
                    .await
                    .expect("open write")
                    .into_inner();
                let stream_id =
                    crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");

                let response = service
                    .abort_write(tonic::Request::new(AbortWriteRequestProto {
                        header: Some(test_header()),
                        group_name: "root".to_string(),
                        block_id: Some(test_block_id_proto()),
                        stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                        token: Some(test_token_proto()),
                    }))
                    .await
                    .expect("abort write")
                    .into_inner();

                assert!(response.header.expect("header").error.is_none());
                assert!(response.aborted);
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("write".to_string(), 1.0), ("write".to_string(), -1.0)]
        );
    }

    async fn assert_commit_write_success_response() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let core = Arc::new(core);
        let service = registered_data_service(Arc::clone(&core));
        let open = service
            .open_write_stream(tonic::Request::new(open_write_proto(2048)))
            .await
            .expect("open write")
            .into_inner();
        let stream_id = crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id,
            seq: 1,
            offset_in_block: 0,
            data: data.slice(0..2048),
            checksum32: 0,
        })
        .await
        .expect("first frame");
        core.write_stream(WriteFrame {
            stream_id,
            seq: 2,
            offset_in_block: 2048,
            data: data.slice(2048..4096),
            checksum32: 0,
        })
        .await
        .expect("second frame");

        let response = service
            .commit_write(tonic::Request::new(commit_write_proto(stream_id, 2, BLOCK_SIZE)))
            .await
            .expect("commit write response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert_eq!(response.effective_len, BLOCK_SIZE);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
        assert_eq!(response.written_through, BLOCK_SIZE);
    }

    async fn assert_sync_committed_block_success_response() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = registered_data_service(Arc::new(core));

        let response = service
            .sync_committed_block(tonic::Request::new(sync_committed_block_proto(BLOCK_STAMP, BLOCK_SIZE)))
            .await
            .expect("sync committed block response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert_eq!(response.effective_len, BLOCK_SIZE);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
    }

    fn structured_status_error(status: Status) -> RpcErrorDetail {
        assert_eq!(
            status
                .metadata()
                .get(HEADER_WORKER_DATA_ERROR_DETAIL)
                .and_then(|value| value.to_str().ok()),
            Some(WORKER_DATA_ERROR_DETAIL_V1)
        );
        let header = DataResponseHeaderProto::decode(status.details()).expect("structured Worker status");
        beryl_proto::convert::rpc_error_from_proto(&header.error.expect("structured Worker error"))
    }

    #[tokio::test]
    async fn read_block_streams_exact_range_and_balances_inflight() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, store, core) = core_with_store(512, 2048);
                let data = payload();
                publish_ready_block(&store, data.clone(), BLOCK_STAMP);
                let service = registered_data_service(Arc::new(core));

                let response_stream = service
                    .read_block(tonic::Request::new(read_block_proto(4, 1030, BLOCK_STAMP, 512)))
                    .await
                    .expect("read block response")
                    .into_inner();
                let chunks = response_stream
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .expect("read block chunks");

                assert_eq!(
                    chunks.iter().map(|chunk| chunk.data.len()).collect::<Vec<_>>(),
                    [512, 512, 6]
                );
                assert_eq!(
                    chunks.into_iter().flat_map(|chunk| chunk.data).collect::<Bytes>(),
                    data.slice(4..1034)
                );
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("read".to_string(), 1.0), ("read".to_string(), -1.0)]
        );
    }

    #[tokio::test]
    async fn read_block_errors_preserve_structured_worker_actions() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = registered_data_service(Arc::new(core));

        for (request, expected_kind, expected_recovery) in [
            (
                read_block_proto(0, 8, BLOCK_STAMP + 1, 512),
                ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
                "refresh",
            ),
            (
                read_block_proto(0, 8, 0, 512),
                ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument),
                "fail",
            ),
        ] {
            let status = match service.read_block(tonic::Request::new(request)).await {
                Ok(_) => panic!("invalid read unexpectedly succeeded"),
                Err(status) => status,
            };
            let error = structured_status_error(status);
            assert_eq!(error.kind, expected_kind);
            match expected_recovery {
                "refresh" => assert!(matches!(error.recovery, RecoveryAction::RefreshMetadata { .. })),
                "fail" => assert!(matches!(error.recovery, RecoveryAction::Fail)),
                _ => unreachable!(),
            }
        }

        let mut stream = service
            .read_block(tonic::Request::new(read_block_proto(0, 8, BLOCK_STAMP, 512)))
            .await
            .expect("start valid read")
            .into_inner();
        std::fs::remove_file(store.paths(&group_name(), block_id()).data_path).expect("remove block data");
        let status = stream
            .next()
            .await
            .expect("stream error item")
            .expect_err("missing data must fail the response stream");
        let error = structured_status_error(status);
        assert!(matches!(error.recovery, RecoveryAction::Fail));

        let (_temp, _store, core) = core_with_store(512, 2048);
        let service = registered_data_service(Arc::new(core));
        let status = match service
            .read_block(tonic::Request::new(read_block_proto(0, 8, BLOCK_STAMP, 512)))
            .await
        {
            Ok(_) => panic!("missing block unexpectedly succeeded"),
            Err(status) => status,
        };
        let error = structured_status_error(status);
        assert_eq!(error.kind, ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable));
        assert!(matches!(error.recovery, RecoveryAction::RefreshMetadata { .. }));
    }

    #[tokio::test]
    async fn dropping_read_block_response_releases_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, store, core) = core_with_store(512, 2048);
                publish_ready_block(&store, payload(), BLOCK_STAMP);
                let service = registered_data_service(Arc::new(core));

                let response_stream = service
                    .read_block(tonic::Request::new(read_block_proto(
                        0,
                        BLOCK_SIZE as u32,
                        BLOCK_STAMP,
                        512,
                    )))
                    .await
                    .expect("read block response")
                    .into_inner();
                drop(response_stream);
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("read".to_string(), 1.0), ("read".to_string(), -1.0)]
        );
    }

    #[derive(Default)]
    struct StreamGaugeRecorder {
        stream_values: Arc<Mutex<Vec<(String, f64)>>>,
    }

    impl StreamGaugeRecorder {
        fn stream_values(&self) -> Vec<(String, f64)> {
            self.stream_values.lock().expect("stream gauge values poisoned").clone()
        }
    }

    impl Recorder for StreamGaugeRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::noop()
        }

        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            if key.name() != WORKER_STREAM_INFLIGHT {
                return Gauge::noop();
            }
            let mode = key
                .labels()
                .find(|label| label.key() == "mode")
                .map(|label| label.value().to_string())
                .unwrap_or_default();
            Gauge::from_arc(Arc::new(StreamGauge {
                mode,
                values: Arc::clone(&self.stream_values),
            }))
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    struct StreamGauge {
        mode: String,
        values: Arc<Mutex<Vec<(String, f64)>>>,
    }

    impl GaugeFn for StreamGauge {
        fn increment(&self, value: f64) {
            self.values
                .lock()
                .expect("stream gauge values poisoned")
                .push((self.mode.clone(), value));
        }

        fn decrement(&self, value: f64) {
            self.values
                .lock()
                .expect("stream gauge values poisoned")
                .push((self.mode.clone(), -value));
        }

        fn set(&self, value: f64) {
            self.values
                .lock()
                .expect("stream gauge values poisoned")
                .push((self.mode.clone(), value));
        }
    }
}
