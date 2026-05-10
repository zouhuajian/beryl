// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! WorkerDataService gRPC adapter.

use std::pin::Pin;
use std::sync::Arc;

use proto::common::{
    error_detail_proto, ClientInfoProto, ErrorClassProto, ErrorDetailProto, FsErrnoProto, RefreshReasonProto,
};
use proto::worker::worker_data_service_server::WorkerDataService;
use proto::worker::{
    AbortWriteRequestProto, AbortWriteResponseProto, CommitWriteRequestProto, CommitWriteResponseProto,
    DataRequestHeaderProto, DataResponseHeaderProto, OpenReadStreamRequestProto, OpenReadStreamResponseProto,
    OpenWriteStreamRequestProto, OpenWriteStreamResponseProto, ReadStreamRequestProto, ReadStreamResponseProto,
    WriteStreamRequestProto, WriteStreamResponseProto,
};
use tonic::{Request, Response, Status};

use crate::data::convert::{
    proto_to_abort_write_request, proto_to_commit_write_request, proto_to_read_open_request,
    proto_to_write_open_request, stream_id_to_proto,
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
        let fs_errno = match error {
            WorkerError::InvalidArgument(_) => FsErrnoProto::FsErrnoEinval,
            WorkerError::NotFound(_) => FsErrnoProto::FsErrnoEnoent,
            WorkerError::PermissionDenied(_) => FsErrnoProto::FsErrnoEacces,
            WorkerError::ResourceExhausted(_) => FsErrnoProto::FsErrnoEagain,
            WorkerError::Unimplemented(_) => FsErrnoProto::FsErrnoEnotimpl,
            _ => FsErrnoProto::FsErrnoEnotsup,
        };
        let error_class = if error.is_retryable() {
            ErrorClassProto::ErrorClassRetryable
        } else {
            ErrorClassProto::ErrorClassFatal
        };

        ErrorDetailProto {
            error_class: error_class as i32,
            code: Some(error_detail_proto::Code::FsErrno(fs_errno as i32)),
            refresh_reason: RefreshReasonProto::RefreshReasonUnknown as i32,
            retry_after_ms: error.metadata().retry_after_ms,
            message: error.to_string(),
            refresh_hint: None,
        }
    }

    fn unimplemented_status(operation: &'static str) -> Status {
        Status::unimplemented(format!("{operation} worker core is not implemented"))
    }

    pub(crate) fn write_stream_placeholder_status() -> Status {
        Self::unimplemented_status("WriteStream")
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
        _request: Request<ReadStreamRequestProto>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        Err(Self::unimplemented_status("ReadStream"))
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
        _request: Request<tonic::Streaming<WriteStreamRequestProto>>,
    ) -> Result<Response<WriteStreamResponseProto>, Status> {
        Err(Self::write_stream_placeholder_status())
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
                    committed_length: result.committed_length,
                    block_stamp: result.block_stamp,
                    persisted_through: result.persisted_through,
                },
                Err(error) => CommitWriteResponseProto {
                    header: Some(Self::error_response_header(header, error)),
                    committed_length: 0,
                    block_stamp: 0,
                    persisted_through: 0,
                },
            },
            Err(error) => CommitWriteResponseProto {
                header: Some(Self::error_response_header(header, error)),
                committed_length: 0,
                block_stamp: 0,
                persisted_through: 0,
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
