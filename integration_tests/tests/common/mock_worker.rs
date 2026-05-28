// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Mock worker server utilities for integration tests.

use std::pin::Pin;

use proto::common::{
    error_detail_proto, ClientInfoProto, ErrorClassProto, ErrorDetailProto, FsErrnoProto, RefreshReasonProto,
};
use proto::worker::worker_data_service_server::WorkerDataService;
use proto::worker::{
    AbortWriteRequestProto, AbortWriteResponseProto, CommitWriteRequestProto, CommitWriteResponseProto,
    DataRequestHeaderProto, DataResponseHeaderProto, OpenReadStreamRequestProto, OpenReadStreamResponseProto,
    OpenWriteStreamRequestProto, OpenWriteStreamResponseProto, ReadStreamRequestProto, ReadStreamResponseProto,
    SyncCommittedBlockRequestProto, SyncCommittedBlockResponseProto, WriteStreamRequestProto, WriteStreamResponseProto,
};
use tonic::{Request, Response, Status};

/// Mock worker server for integration tests.
///
/// This mock stays intentionally limited to the stream wire surface.
/// It does not emulate worker IO or preserve the removed chunk RPCs.
#[derive(Clone)]
pub struct MockWorkerServer {
    client_template: ClientInfoProto,
}

impl MockWorkerServer {
    pub fn new(_default_version: u64) -> Self {
        Self {
            client_template: ClientInfoProto {
                call_id: "mock-worker".to_string(),
                client_id: 0,
                client_name: "mock".to_string(),
            },
        }
    }

    fn placeholder_header(&self, header: Option<DataRequestHeaderProto>, operation: &str) -> DataResponseHeaderProto {
        DataResponseHeaderProto {
            client: Some(
                header
                    .and_then(|h| h.client)
                    .unwrap_or_else(|| self.client_template.clone()),
            ),
            error: Some(Self::unimplemented_error(operation)),
        }
    }

    fn unimplemented_error(operation: &str) -> ErrorDetailProto {
        ErrorDetailProto {
            error_class: ErrorClassProto::ErrorClassFatal as i32,
            code: Some(error_detail_proto::Code::FsErrno(FsErrnoProto::FsErrnoEnotimpl as i32)),
            refresh_reason: RefreshReasonProto::RefreshReasonUnknown as i32,
            retry_after_ms: None,
            message: format!("{operation} stream-v2 worker execution is not implemented in phase 1"),
            refresh_hint: None,
        }
    }
}

#[tonic::async_trait]
impl WorkerDataService for MockWorkerServer {
    type ReadStreamStream = Pin<Box<dyn futures::Stream<Item = Result<ReadStreamResponseProto, Status>> + Send>>;

    async fn open_read_stream(
        &self,
        request: Request<OpenReadStreamRequestProto>,
    ) -> Result<Response<OpenReadStreamResponseProto>, Status> {
        let request = request.into_inner();
        Ok(Response::new(OpenReadStreamResponseProto {
            header: Some(self.placeholder_header(request.header, "OpenReadStream")),
            stream_id: None,
            frame_size: 0,
            window_bytes: 0,
            block_stamp: 0,
            committed_length: 0,
        }))
    }

    async fn read_stream(
        &self,
        _request: Request<ReadStreamRequestProto>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        Err(Status::unimplemented(
            "ReadStream stream-v2 worker execution is not implemented in phase 1",
        ))
    }

    async fn open_write_stream(
        &self,
        request: Request<OpenWriteStreamRequestProto>,
    ) -> Result<Response<OpenWriteStreamResponseProto>, Status> {
        let request = request.into_inner();
        Ok(Response::new(OpenWriteStreamResponseProto {
            header: Some(self.placeholder_header(request.header, "OpenWriteStream")),
            stream_id: None,
            frame_size: 0,
            window_bytes: 0,
            block_stamp: 0,
            committed_length: 0,
        }))
    }

    async fn write_stream(
        &self,
        _request: Request<tonic::Streaming<WriteStreamRequestProto>>,
    ) -> Result<Response<WriteStreamResponseProto>, Status> {
        Err(Status::unimplemented(
            "WriteStream stream-v2 worker execution is not implemented in phase 1",
        ))
    }

    async fn commit_write(
        &self,
        request: Request<CommitWriteRequestProto>,
    ) -> Result<Response<CommitWriteResponseProto>, Status> {
        let request = request.into_inner();
        Ok(Response::new(CommitWriteResponseProto {
            header: Some(self.placeholder_header(request.header, "CommitWrite")),
            effective_block_len: 0,
            block_stamp: 0,
            written_through: 0,
        }))
    }

    async fn sync_committed_block(
        &self,
        request: Request<SyncCommittedBlockRequestProto>,
    ) -> Result<Response<SyncCommittedBlockResponseProto>, Status> {
        let request = request.into_inner();
        Ok(Response::new(SyncCommittedBlockResponseProto {
            header: Some(self.placeholder_header(request.header, "SyncCommittedBlock")),
            effective_block_len: 0,
            block_stamp: 0,
        }))
    }

    async fn abort_write(
        &self,
        request: Request<AbortWriteRequestProto>,
    ) -> Result<Response<AbortWriteResponseProto>, Status> {
        let request = request.into_inner();
        Ok(Response::new(AbortWriteResponseProto {
            header: Some(self.placeholder_header(request.header, "AbortWrite")),
            aborted: false,
        }))
    }
}
