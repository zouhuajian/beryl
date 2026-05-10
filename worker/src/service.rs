// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! WorkerDataService v2 gRPC placeholder implementation.
//!
//! This phase owns the wire-contract cutover only. The real block-local stream
//! execution path is intentionally left for the next worker data-plane phase.

use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use common::audit::AuditLogger;
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
use types::ids::{ShardGroupId, WorkerId};
use types::layout::FileLayout;

use crate::block_manager::BlockManager;
use crate::block_store::BlockStore;
use crate::stream_manager::StreamManager;
use crate::ufs_fill::UfsFiller;

/// Worker data service implementation.
#[derive(Clone)]
pub struct WorkerDataServiceImpl {
    block_store: Arc<BlockStore>,
    block_manager: Option<Arc<BlockManager>>,
    audit_logger: Arc<AuditLogger>,
    layout: FileLayout,
    worker_id: WorkerId,
    worker_epoch: u64,
    default_group_id: ShardGroupId,
    ufs_filler: Option<Arc<UfsFiller>>,
    replication_client: Option<Arc<dyn crate::block_manager::ReplicationClient + Send + Sync>>,
    stream_manager: Arc<StreamManager>,
}

impl WorkerDataServiceImpl {
    pub fn new(
        block_store: Arc<BlockStore>,
        audit_logger: Arc<AuditLogger>,
        layout: FileLayout,
        worker_id: WorkerId,
        worker_epoch: u64,
    ) -> Self {
        let block_manager = Arc::new(BlockManager::new(Arc::clone(&block_store), layout.clone()));
        Self {
            block_store,
            block_manager: Some(block_manager),
            audit_logger,
            layout,
            worker_id,
            worker_epoch,
            default_group_id: ShardGroupId::new(0),
            ufs_filler: None,
            replication_client: None,
            stream_manager: Arc::new(StreamManager::with_default_timeout()),
        }
    }

    /// Create with replication client for later stream-v2 replication wiring.
    pub fn with_replication(
        block_store: Arc<BlockStore>,
        audit_logger: Arc<AuditLogger>,
        layout: FileLayout,
        worker_id: WorkerId,
        worker_epoch: u64,
        replication_client: Arc<dyn crate::block_manager::ReplicationClient + Send + Sync>,
    ) -> Self {
        let block_manager = Arc::new(BlockManager::new(Arc::clone(&block_store), layout.clone()));
        Self {
            block_store,
            block_manager: Some(block_manager),
            audit_logger,
            layout,
            worker_id,
            worker_epoch,
            default_group_id: ShardGroupId::new(0),
            ufs_filler: None,
            replication_client: Some(replication_client),
            stream_manager: Arc::new(StreamManager::with_default_timeout()),
        }
    }

    /// Create with UFS filler retained for construction compatibility only.
    pub fn with_ufs_filler(
        block_store: Arc<BlockStore>,
        audit_logger: Arc<AuditLogger>,
        layout: FileLayout,
        worker_id: WorkerId,
        worker_epoch: u64,
        ufs_filler: Arc<UfsFiller>,
    ) -> Self {
        let block_manager = Arc::new(BlockManager::new(Arc::clone(&block_store), layout.clone()));
        Self {
            block_store,
            block_manager: Some(block_manager),
            audit_logger,
            layout,
            worker_id,
            worker_epoch,
            default_group_id: ShardGroupId::new(0),
            ufs_filler: Some(ufs_filler),
            replication_client: None,
            stream_manager: Arc::new(StreamManager::with_default_timeout()),
        }
    }

    #[cfg(test)]
    pub(crate) fn stream_manager_for_test(&self) -> Arc<StreamManager> {
        Arc::clone(&self.stream_manager)
    }

    fn placeholder_header(header: Option<DataRequestHeaderProto>, operation: &str) -> DataResponseHeaderProto {
        DataResponseHeaderProto {
            client: Some(header.and_then(|h| h.client).unwrap_or_else(Self::default_client)),
            error: Some(Self::unimplemented_error(operation)),
        }
    }

    fn default_client() -> ClientInfoProto {
        ClientInfoProto {
            call_id: String::new(),
            client_id: 0,
            client_name: String::new(),
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
impl WorkerDataService for WorkerDataServiceImpl {
    type ReadStreamStream = Pin<Box<dyn futures::Stream<Item = Result<ReadStreamResponseProto, Status>> + Send>>;

    async fn open_read_stream(
        &self,
        request: Request<OpenReadStreamRequestProto>,
    ) -> Result<Response<OpenReadStreamResponseProto>, Status> {
        let request = request.into_inner();
        Ok(Response::new(OpenReadStreamResponseProto {
            header: Some(Self::placeholder_header(request.header, "OpenReadStream")),
            stream_id: None,
            accepted_frame_size: 0,
            flow_control_window_bytes: 0,
            current_block_stamp: 0,
            committed_length: 0,
            storage_chunk_size: self.layout.chunk_size,
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
            header: Some(Self::placeholder_header(request.header, "OpenWriteStream")),
            stream_id: None,
            accepted_frame_size: 0,
            flow_control_window_bytes: 0,
            current_block_stamp: 0,
            committed_length: 0,
            storage_chunk_size: self.layout.chunk_size,
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
            header: Some(Self::placeholder_header(request.header, "CommitWrite")),
            committed_length: 0,
            current_block_stamp: 0,
            persisted_through: 0,
        }))
    }

    async fn abort_write(
        &self,
        request: Request<AbortWriteRequestProto>,
    ) -> Result<Response<AbortWriteResponseProto>, Status> {
        let request = request.into_inner();
        Ok(Response::new(AbortWriteResponseProto {
            header: Some(Self::placeholder_header(request.header, "AbortWrite")),
            aborted: false,
        }))
    }
}
