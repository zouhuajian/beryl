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

use crate::data::convert::{
    proto_to_abort_write_request, proto_to_commit_write_request, proto_to_read_open_request, proto_to_stream_id,
    proto_to_sync_committed_block_request, proto_to_write_frame, proto_to_write_open_request, stream_id_to_proto,
};
use crate::data::core::WorkerCore;
use crate::error::WorkerError;

/// Worker data service implementation.
#[derive(Clone)]
pub struct WorkerDataServiceImpl {
    core: Arc<WorkerCore>,
}

impl WorkerDataServiceImpl {
    pub fn new(core: Arc<WorkerCore>) -> Self {
        Self { core }
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
            client_id: 0,
            client_name: String::new(),
        }
    }

    fn error_detail(error: &WorkerError) -> ErrorDetailProto {
        let canonical: common::error::canonical::CanonicalError = error.clone().into();
        proto::convert::canonical_to_error_detail(&canonical)
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
        let response = match proto_to_read_open_request(request) {
            Ok(domain) => match self.core.open_read(domain).await {
                Ok(result) => OpenReadStreamResponseProto {
                    header: Some(Self::ok_response_header(header)),
                    stream_id: Some(stream_id_to_proto(result.stream_id)),
                    frame_size: result.frame_size,
                    window_bytes: result.window_bytes,
                    block_stamp: result.block_stamp,
                    committed_length: result.committed_length,
                    chunk_size: result.chunk_size,
                },
                Err(error) => OpenReadStreamResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    stream_id: None,
                    frame_size: 0,
                    window_bytes: 0,
                    block_stamp: 0,
                    committed_length: 0,
                    chunk_size: self.core.chunk_size(),
                },
            },
            Err(error) => OpenReadStreamResponseProto {
                header: Some(Self::error_response_header(header, error)),
                stream_id: None,
                frame_size: 0,
                window_bytes: 0,
                block_stamp: 0,
                committed_length: 0,
                chunk_size: self.core.chunk_size(),
            },
        };

        Ok(Response::new(response))
    }

    async fn read_stream(
        &self,
        request: Request<ReadStreamRequestProto>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
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
        let response = match proto_to_write_open_request(request) {
            Ok(domain) => match self.core.open_write(domain).await {
                Ok(result) => OpenWriteStreamResponseProto {
                    header: Some(Self::ok_response_header(header)),
                    stream_id: Some(stream_id_to_proto(result.stream_id)),
                    frame_size: result.frame_size,
                    window_bytes: result.window_bytes,
                    block_stamp: result.block_stamp,
                    committed_length: result.committed_length,
                    chunk_size: result.chunk_size,
                },
                Err(error) => OpenWriteStreamResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    stream_id: None,
                    frame_size: 0,
                    window_bytes: 0,
                    block_stamp: 0,
                    committed_length: 0,
                    chunk_size: self.core.chunk_size(),
                },
            },
            Err(error) => OpenWriteStreamResponseProto {
                header: Some(Self::error_response_header(header, error)),
                stream_id: None,
                frame_size: 0,
                window_bytes: 0,
                block_stamp: 0,
                committed_length: 0,
                chunk_size: self.core.chunk_size(),
            },
        };

        Ok(Response::new(response))
    }

    async fn write_stream(
        &self,
        request: Request<tonic::Streaming<WriteStreamRequestProto>>,
    ) -> Result<Response<WriteStreamResponseProto>, Status> {
        let response = self.handle_write_frames(request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn commit_write(
        &self,
        request: Request<CommitWriteRequestProto>,
    ) -> Result<Response<CommitWriteResponseProto>, Status> {
        let request = request.into_inner();
        let header = request.header.clone();
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

pub async fn serve_grpc_worker_data(
    bind: SocketAddr,
    max_inflight: usize,
    core: Arc<WorkerCore>,
) -> anyhow::Result<()> {
    let service = WorkerDataServiceImpl::new(core);
    tonic_net::Server::builder()
        .concurrency_limit_per_connection(max_inflight)
        .add_service(WorkerDataServiceServer::new(service))
        .serve(bind)
        .await
        .context("worker gRPC data server failed")
}
