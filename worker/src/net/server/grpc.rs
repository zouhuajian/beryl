// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! gRPC WorkerDataService adapter and server entry point.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use futures::{stream, Stream, StreamExt};
use proto::common::{ClientInfoProto, ErrorDetailProto};
use proto::worker::worker_data_service_server::{WorkerDataService, WorkerDataServiceServer};
use proto::worker::{
    AbortWriteRequestProto, AbortWriteResponseProto, CommitWriteRequestProto, CommitWriteResponseProto,
    DataRequestHeaderProto, DataResponseHeaderProto, OpenReadStreamRequestProto, OpenReadStreamResponseProto,
    OpenWriteStreamRequestProto, OpenWriteStreamResponseProto, ReadStreamRequestProto, ReadStreamResponseProto,
    SyncCommittedBlockRequestProto, SyncCommittedBlockResponseProto, WriteStreamRequestProto, WriteStreamResponseProto,
};
use tonic::transport as tonic_net;
use tonic::{Request, Response, Status};

use crate::control::RegistrationSet;
use crate::data::convert::{
    proto_to_abort_write_request, proto_to_commit_write_request, proto_to_read_open_request, proto_to_stream_id,
    proto_to_sync_committed_block_request, proto_to_write_frame, proto_to_write_open_request, stream_id_to_proto,
};
use crate::data::core::WorkerCore;
use crate::error::WorkerError;
use common::error::canonical::RefreshReason;
use common::header::RpcErrorCode;
use types::{GroupName, WorkerRunId};

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
        let canonical: common::error::canonical::CanonicalError = error.clone().into();
        proto::convert::canonical_to_error_detail(&canonical)
    }

    fn ensure_group_ready(&self, group_name: &str) -> Result<(), WorkerError> {
        let group_name = parse_group_name(group_name)?;
        if self.registration_state.is_ready(&group_name) {
            return Ok(());
        }

        Err(WorkerError::NeedRefresh {
            code: RpcErrorCode::NodeUnavailable,
            reason: RefreshReason::StaleState,
            message: format!("worker is not registered for metadata group {}", group_name),
        })
    }

    fn ensure_group_ready_for_run(&self, group_name: &str, worker_run_id: &str) -> Result<(), WorkerError> {
        let group_name = parse_group_name(group_name)?;
        if worker_run_id.is_empty() {
            return Err(WorkerError::InvalidArgument(
                "worker_run_id must not be empty".to_string(),
            ));
        }
        let requested = worker_run_id
            .parse::<WorkerRunId>()
            .map_err(|err| WorkerError::InvalidArgument(format!("worker_run_id invalid: {err}")))?;
        let Some(registration) = self.registration_state.registration_for_group(&group_name) else {
            return Err(WorkerError::NeedRefresh {
                code: RpcErrorCode::NodeUnavailable,
                reason: RefreshReason::StaleState,
                message: format!("worker is not registered for metadata group {}", group_name),
            });
        };
        if !self.registration_state.is_ready(&group_name) {
            return Err(WorkerError::NeedRefresh {
                code: RpcErrorCode::NodeUnavailable,
                reason: RefreshReason::StaleState,
                message: format!("worker is not ready for metadata group {}", group_name),
            });
        }
        if requested != registration.worker_run_id {
            return Err(WorkerError::NeedRefresh {
                code: RpcErrorCode::WorkerRunMismatch,
                reason: RefreshReason::WorkerRunMismatch,
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

        Err(WorkerError::NeedRefresh {
            code: RpcErrorCode::NodeUnavailable,
            reason: RefreshReason::StaleState,
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
        while let Some(frame) = frames.next().await {
            let domain = proto_to_write_frame(frame?).map_err(|error| error.to_status())?;
            let result = self
                .core
                .write_stream(domain)
                .await
                .map_err(|error| error.to_status())?;
            response = WriteStreamResponseProto {
                accepted: result.accepted,
                last_acked_seq: result.last_acked_seq,
                written_through: result.written_through,
            };
            if !response.accepted {
                break;
            }
        }
        Ok(response)
    }
}

#[tonic::async_trait]
impl WorkerDataService for WorkerDataServiceImpl {
    type ReadStreamStream = Pin<Box<dyn futures::Stream<Item = Result<ReadStreamResponseProto, Status>> + Send>>;

    async fn open_read_stream(
        &self,
        request: Request<OpenReadStreamRequestProto>,
    ) -> Result<Response<OpenReadStreamResponseProto>, Status> {
        let request = request.into_inner();
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&request.group_name, &request.worker_run_id) {
            return Ok(Response::new(OpenReadStreamResponseProto {
                header: Some(Self::error_response_header(header, error)),
                stream_id: None,
                frame_size: 0,
                window_bytes: 0,
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
                    window_bytes: result.window_bytes,
                    block_stamp: result.block_stamp,
                    committed_length: result.committed_length,
                },
                Err(error) => OpenReadStreamResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    stream_id: None,
                    frame_size: 0,
                    window_bytes: 0,
                    block_stamp: 0,
                    committed_length: 0,
                },
            },
            Err(error) => OpenReadStreamResponseProto {
                header: Some(Self::error_response_header(header, error)),
                stream_id: None,
                frame_size: 0,
                window_bytes: 0,
                block_stamp: 0,
                committed_length: 0,
            },
        };

        Ok(Response::new(response))
    }

    async fn read_stream(
        &self,
        request: Request<ReadStreamRequestProto>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        self.ensure_any_ready().map_err(|error| error.to_status())?;
        let request = request.into_inner();
        let stream_id = proto_to_stream_id(request.stream_id, "stream_id").map_err(|error| error.to_status())?;
        let frames = self
            .core
            .read_stream(stream_id, request.max_bytes)
            .await
            .map_err(|error| error.to_status())?;
        let responses = frames.into_iter().map(|frame| {
            Ok(ReadStreamResponseProto {
                offset_in_block: frame.offset_in_block,
                data: frame.data,
                checksum32: frame.checksum32,
                eos: frame.eos,
            })
        });
        Ok(Response::new(
            Box::pin(stream::iter(responses)) as Self::ReadStreamStream
        ))
    }

    async fn open_write_stream(
        &self,
        request: Request<OpenWriteStreamRequestProto>,
    ) -> Result<Response<OpenWriteStreamResponseProto>, Status> {
        let request = request.into_inner();
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&request.group_name, &request.worker_run_id) {
            return Ok(Response::new(OpenWriteStreamResponseProto {
                header: Some(Self::error_response_header(header, error)),
                stream_id: None,
                frame_size: 0,
                window_bytes: 0,
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
                    window_bytes: result.window_bytes,
                    block_stamp: result.block_stamp,
                    committed_length: result.committed_length,
                },
                Err(error) => OpenWriteStreamResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    stream_id: None,
                    frame_size: 0,
                    window_bytes: 0,
                    block_stamp: 0,
                    committed_length: 0,
                },
            },
            Err(error) => OpenWriteStreamResponseProto {
                header: Some(Self::error_response_header(header, error)),
                stream_id: None,
                frame_size: 0,
                window_bytes: 0,
                block_stamp: 0,
                committed_length: 0,
            },
        };

        Ok(Response::new(response))
    }

    async fn write_stream(
        &self,
        request: Request<tonic::Streaming<WriteStreamRequestProto>>,
    ) -> Result<Response<WriteStreamResponseProto>, Status> {
        self.ensure_any_ready().map_err(|error| error.to_status())?;
        let response = self.handle_write_frames(request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn commit_write(
        &self,
        request: Request<CommitWriteRequestProto>,
    ) -> Result<Response<CommitWriteResponseProto>, Status> {
        let request = request.into_inner();
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&request.group_name, &request.worker_run_id) {
            return Ok(Response::new(CommitWriteResponseProto {
                header: Some(Self::error_response_header(header, error)),
                effective_block_len: 0,
                block_stamp: 0,
                written_through: 0,
            }));
        }
        let response = match proto_to_commit_write_request(request) {
            Ok(domain) => match self.core.commit_write(domain).await {
                Ok(result) => CommitWriteResponseProto {
                    header: Some(Self::ok_response_header(header)),
                    effective_block_len: result.effective_block_len,
                    block_stamp: result.block_stamp,
                    written_through: result.written_through,
                },
                Err(error) => CommitWriteResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    effective_block_len: 0,
                    block_stamp: 0,
                    written_through: 0,
                },
            },
            Err(error) => CommitWriteResponseProto {
                header: Some(Self::error_response_header(header, error)),
                effective_block_len: 0,
                block_stamp: 0,
                written_through: 0,
            },
        };

        Ok(Response::new(response))
    }

    async fn sync_committed_block(
        &self,
        request: Request<SyncCommittedBlockRequestProto>,
    ) -> Result<Response<SyncCommittedBlockResponseProto>, Status> {
        let request = request.into_inner();
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&request.group_name, &request.worker_run_id) {
            return Ok(Response::new(SyncCommittedBlockResponseProto {
                header: Some(Self::error_response_header(header, error)),
                effective_block_len: 0,
                block_stamp: 0,
            }));
        }
        let response = match proto_to_sync_committed_block_request(request) {
            Ok(domain) => match self.core.sync_committed_block(domain).await {
                Ok(result) => SyncCommittedBlockResponseProto {
                    header: Some(Self::ok_response_header(header)),
                    effective_block_len: result.effective_block_len,
                    block_stamp: result.block_stamp,
                },
                Err(error) => SyncCommittedBlockResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    effective_block_len: 0,
                    block_stamp: 0,
                },
            },
            Err(error) => SyncCommittedBlockResponseProto {
                header: Some(Self::error_response_header(header, error)),
                effective_block_len: 0,
                block_stamp: 0,
            },
        };

        Ok(Response::new(response))
    }

    async fn abort_write(
        &self,
        request: Request<AbortWriteRequestProto>,
    ) -> Result<Response<AbortWriteResponseProto>, Status> {
        let request = request.into_inner();
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready(&request.group_name) {
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

        Ok(Response::new(response))
    }
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
