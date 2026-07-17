// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! gRPC WorkerDataService adapter and server entry point.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use beryl_proto::common::{ClientInfoProto, ErrorDetailProto};
use beryl_proto::convert::require_worker_run_id;
use beryl_proto::worker::worker_data_service_server::{WorkerDataService, WorkerDataServiceServer};
use beryl_proto::worker::{
    AbortWriteRequestProto, AbortWriteResponseProto, CommitWriteRequestProto, CommitWriteResponseProto,
    DataRequestHeaderProto, DataResponseHeaderProto, OpenReadStreamRequestProto, OpenReadStreamResponseProto,
    OpenWriteStreamRequestProto, OpenWriteStreamResponseProto, ReadStreamRequestProto, ReadStreamResponseProto,
    SyncCommittedBlockRequestProto, SyncCommittedBlockResponseProto, WriteStreamRequestProto, WriteStreamResponseProto,
};
use futures::{stream, Stream, StreamExt};
use tonic::transport as tonic_net;
use tonic::{Request, Response, Status};

use crate::control::RegistrationSet;
use crate::data::convert::{
    proto_to_abort_write_request, proto_to_commit_write_request, proto_to_read_open_request, proto_to_stream_id,
    proto_to_sync_committed_block_request, proto_to_write_frame, proto_to_write_open_request, stream_id_to_proto,
};
use crate::data::core::{StreamMode, WorkerCore};
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
        let mut active_stream_id = None;
        while let Some(frame) = frames.next().await {
            let frame = match frame {
                Ok(frame) => frame,
                Err(status) => {
                    observe::record_stream_frame("write", "error", status_error_kind(&status), 0);
                    cleanup_write_stream_after_error(&self.core, active_stream_id).await?;
                    return Err(status);
                }
            };
            let frame_bytes = frame.data.len() as u64;
            let frame_stream_id = proto_to_stream_id(frame.stream_id, "stream_id").ok();
            let domain = match proto_to_write_frame(frame) {
                Ok(domain) => domain,
                Err(error) => {
                    observe::record_stream_frame("write", "error", observe::worker_error_kind(&error), frame_bytes);
                    cleanup_write_stream_after_error(&self.core, frame_stream_id.or(active_stream_id)).await?;
                    return Err(error.to_status());
                }
            };
            active_stream_id = Some(domain.stream_id);
            let result = match self.core.write_stream(domain).await {
                Ok(result) => result,
                Err(error) => {
                    observe::record_stream_frame("write", "error", observe::worker_error_kind(&error), frame_bytes);
                    cleanup_write_stream_after_error(&self.core, active_stream_id).await?;
                    return Err(error.to_status());
                }
            };
            observe::record_stream_frame("write", "ok", "none", frame_bytes);
            response = WriteStreamResponseProto {
                accepted: result.accepted,
                last_acked_seq: result.last_acked_seq,
                written_through: result.written_through,
            };
            if !response.accepted {
                cleanup_write_stream_after_error(&self.core, active_stream_id).await?;
                break;
            }
        }
        Ok(response)
    }
}

struct ReadStreamState {
    core: Arc<WorkerCore>,
    stream_id: beryl_types::StreamId,
    max_bytes: u32,
    done: Arc<AtomicBool>,
    _cleanup: ReadStreamCleanup,
}

impl ReadStreamState {
    async fn next(self) -> Option<(Result<ReadStreamResponseProto, Status>, Self)> {
        if self.done.load(Ordering::Acquire) {
            return None;
        }

        match self.core.read_stream(self.stream_id, self.max_bytes).await {
            Ok(mut frames) => {
                let Some(frame) = frames.pop() else {
                    self.done.store(true, Ordering::Release);
                    return None;
                };
                if frame.eos {
                    self.done.store(true, Ordering::Release);
                }
                observe::record_stream_frame("read", "ok", "none", frame.data.len() as u64);
                Some((
                    Ok(ReadStreamResponseProto {
                        offset_in_block: frame.offset_in_block,
                        data: frame.data,
                        eos: frame.eos,
                    }),
                    self,
                ))
            }
            Err(error) => {
                self.done.store(true, Ordering::Release);
                observe::record_stream_frame("read", "error", observe::worker_error_kind(&error), 0);
                Some((Err(error.to_status()), self))
            }
        }
    }
}

struct ReadStreamCleanup {
    core: Arc<WorkerCore>,
    stream_id: beryl_types::StreamId,
    done: Arc<AtomicBool>,
}

impl Drop for ReadStreamCleanup {
    fn drop(&mut self) {
        if self.done.load(Ordering::Acquire) {
            return;
        }
        let core = Arc::clone(&self.core);
        let stream_id = self.stream_id;
        self.done.store(true, Ordering::Release);
        tokio::spawn(async move {
            core.stream_manager().remove(stream_id).await;
        });
    }
}

#[tonic::async_trait]
impl WorkerDataService for WorkerDataServiceImpl {
    type ReadStreamStream = Pin<Box<dyn futures::Stream<Item = Result<ReadStreamResponseProto, Status>> + Send>>;

    async fn open_read_stream(
        &self,
        request: Request<OpenReadStreamRequestProto>,
    ) -> Result<Response<OpenReadStreamResponseProto>, Status> {
        let started = Instant::now();
        let transport_context = extract_trace_context(request.metadata());
        let mut request = request.into_inner();
        merge_data_header_transport_context(&mut request.header, &transport_context);
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&request.group_name, &request.worker_run_id) {
            let error_kind = observe::worker_error_kind(&error);
            let duration = started.elapsed().as_secs_f64();
            observe::record_data_rpc("open_read_stream", "error", error_kind, duration);
            observe::record_stream_open("read", "error", error_kind);
            return Ok(Response::new(OpenReadStreamResponseProto {
                header: Some(Self::error_response_header(header, error)),
                stream_id: None,
                frame_size: 0,
                block_stamp: 0,
                committed_length: 0,
            }));
        }
        let response = match proto_to_read_open_request(request) {
            Ok(domain) => match self.core.open_read(domain).await {
                Ok(result) => OpenReadStreamResponseProto {
                    header: Some(Self::ok_response_header(header)),
                    stream_id: Some(stream_id_to_proto(result.stream_id)),
                    frame_size: result.frame_size,
                    block_stamp: result.block_stamp,
                    committed_length: result.committed_length,
                },
                Err(error) => OpenReadStreamResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    stream_id: None,
                    frame_size: 0,
                    block_stamp: 0,
                    committed_length: 0,
                },
            },
            Err(error) => OpenReadStreamResponseProto {
                header: Some(Self::error_response_header(header, error)),
                stream_id: None,
                frame_size: 0,
                block_stamp: 0,
                committed_length: 0,
            },
        };
        let (status, error_kind) = response_status_error_kind(response.header.as_ref());
        let duration = started.elapsed().as_secs_f64();
        observe::record_data_rpc("open_read_stream", status, error_kind, duration);
        observe::record_stream_open("read", status, error_kind);

        Ok(Response::new(response))
    }

    async fn read_stream(
        &self,
        request: Request<ReadStreamRequestProto>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        let started = Instant::now();
        record_transport_context(&extract_trace_context(request.metadata()));
        if let Err(error) = self.ensure_any_ready() {
            observe::record_data_rpc(
                "read_stream",
                "error",
                observe::worker_error_kind(&error),
                started.elapsed().as_secs_f64(),
            );
            return Err(error.to_status());
        }
        let request = request.into_inner();
        let stream_id = match proto_to_stream_id(request.stream_id, "stream_id") {
            Ok(stream_id) => stream_id,
            Err(error) => {
                observe::record_data_rpc(
                    "read_stream",
                    "error",
                    observe::worker_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                return Err(error.to_status());
            }
        };
        let Some(state) = self.core.stream_manager().get(stream_id).await else {
            let error = WorkerError::NotFound(format!("read stream not found: stream_id={stream_id}"));
            observe::record_data_rpc(
                "read_stream",
                "error",
                observe::worker_error_kind(&error),
                started.elapsed().as_secs_f64(),
            );
            return Err(error.to_status());
        };
        if state.context.mode != StreamMode::Read {
            let error = WorkerError::InvalidArgument(format!("stream is not a read stream: stream_id={stream_id}"));
            observe::record_data_rpc(
                "read_stream",
                "error",
                observe::worker_error_kind(&error),
                started.elapsed().as_secs_f64(),
            );
            return Err(error.to_status());
        }
        observe::record_data_rpc("read_stream", "ok", "none", started.elapsed().as_secs_f64());

        let done = Arc::new(AtomicBool::new(false));
        let cleanup = ReadStreamCleanup {
            core: Arc::clone(&self.core),
            stream_id,
            done: Arc::clone(&done),
        };
        let state = ReadStreamState {
            core: Arc::clone(&self.core),
            stream_id,
            max_bytes: request.max_bytes,
            done,
            _cleanup: cleanup,
        };
        let responses = stream::unfold(state, |state| async move { state.next().await });
        Ok(Response::new(Box::pin(responses) as Self::ReadStreamStream))
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
                        let _ = self.core.abort_write_stream_after_error(stream_id).await;
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

pub async fn serve_grpc_worker_data_with_registration(
    bind: SocketAddr,
    max_inflight: usize,
    core: Arc<WorkerCore>,
    registration_state: Arc<RegistrationSet>,
) -> anyhow::Result<()> {
    let service = WorkerDataServiceImpl::new(core, registration_state);
    serve_grpc_worker_data_with_service(bind, max_inflight, service).await
}

async fn serve_grpc_worker_data_with_service(
    bind: SocketAddr,
    max_inflight: usize,
    service: WorkerDataServiceImpl,
) -> anyhow::Result<()> {
    tonic_net::Server::builder()
        .concurrency_limit_per_connection(max_inflight)
        .add_service(WorkerDataServiceServer::new(service))
        .serve(bind)
        .await
        .context("worker gRPC data server failed")
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
