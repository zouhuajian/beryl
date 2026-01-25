// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Mock worker server utilities for integration tests.

use common::error::canonical::{CanonicalError, RefreshReason};
use common::header::RpcErrorCode;
use parking_lot::RwLock as SyncRwLock;
use proto::common::{ChunkIdProto, WorkerEndpointInfoProto};
use proto::worker::worker_data_service_server::WorkerDataService;
use proto::worker::{
    ChunkDataProto, ChunkSliceProto, CommitWriteRequestProto, CommitWriteResponseProto, ReadChunkRequestProto,
    ReadChunkResponseProto, ReadRangeChunkProto, ReadRangeRequestProto, WriteChunkRequestProto,
    WriteChunkResponseProto,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

/// Mock worker server for testing.
#[derive(Clone)]
pub struct MockWorkerServer {
    blocks: Arc<RwLock<HashMap<(u64, u32, u32), Vec<u8>>>>,
    block_versions: Arc<RwLock<HashMap<(u64, u32), u64>>>,
    default_version: u64,
    worker_epoch: Arc<RwLock<u64>>,
    expected_fencing_epoch: Arc<RwLock<u64>>,
    client_template: proto::common::ClientInfoProto,
    worker_epoch_hint_override: Arc<SyncRwLock<Option<u64>>>,
}

impl MockWorkerServer {
    pub fn new(default_version: u64) -> Self {
        Self {
            blocks: Arc::new(RwLock::new(HashMap::new())),
            block_versions: Arc::new(RwLock::new(HashMap::new())),
            default_version,
            worker_epoch: Arc::new(RwLock::new(1)),
            expected_fencing_epoch: Arc::new(RwLock::new(1)),
            client_template: proto::common::ClientInfoProto {
                call_id: "mock-worker".to_string(),
                client_id: 0,
                client_name: "mock".to_string(),
            },
            worker_epoch_hint_override: Arc::new(SyncRwLock::new(None)),
        }
    }

    pub async fn add_block_data(&self, data_handle_id: u64, block_index: u32, chunk_idx: u32, data: Vec<u8>) {
        let mut blocks = self.blocks.write().await;
        blocks.insert((data_handle_id, block_index, chunk_idx), data);
    }

    pub async fn set_block_version(&self, data_handle_id: u64, block_index: u32, version: u64) {
        let mut versions = self.block_versions.write().await;
        versions.insert((data_handle_id, block_index), version);
    }

    pub async fn get_block_version(&self, data_handle_id: u64, block_index: u32) -> u64 {
        let versions = self.block_versions.read().await;
        versions
            .get(&(data_handle_id, block_index))
            .copied()
            .unwrap_or(self.default_version)
    }

    pub async fn increment_block_version(&self, data_handle_id: u64, block_index: u32) {
        let mut versions = self.block_versions.write().await;
        let current = versions
            .get(&(data_handle_id, block_index))
            .copied()
            .unwrap_or(self.default_version);
        versions.insert((data_handle_id, block_index), current + 1);
    }

    pub async fn set_worker_epoch(&self, epoch: u64) {
        let mut guard = self.worker_epoch.write().await;
        *guard = epoch;
    }

    pub async fn worker_epoch(&self) -> u64 {
        *self.worker_epoch.read().await
    }

    pub async fn set_fencing_epoch(&self, epoch: u64) {
        let mut guard = self.expected_fencing_epoch.write().await;
        *guard = epoch;
    }

    pub fn set_worker_epoch_hint_override(&self, hint: Option<u64>) {
        let mut guard = self.worker_epoch_hint_override.write();
        *guard = hint;
    }

    fn build_endpoint_hint(&self, worker_epoch: u64) -> WorkerEndpointInfoProto {
        WorkerEndpointInfoProto {
            worker_id: 0,
            endpoint: "mock://inproc".to_string(),
            net_transport_kind: proto::common::NetTransportKindProto::NetTransportKindGrpc as i32,
            worker_epoch,
        }
    }

    fn data_header(&self, canonical: CanonicalError, worker_epoch: u64) -> proto::worker::DataResponseHeaderProto {
        let hinted_epoch = self.worker_epoch_hint_override.read().clone().unwrap_or(worker_epoch);
        proto::worker::DataResponseHeaderProto {
            client: Some(self.client_template.clone()),
            error: Some(proto::convert::canonical_to_error_detail(&canonical)),
            worker_epoch: Some(hinted_epoch),
            endpoint_hint: Some(self.build_endpoint_hint(hinted_epoch)),
        }
    }
}

#[tonic::async_trait]
impl WorkerDataService for MockWorkerServer {
    async fn read_chunk(
        &self,
        request: Request<ReadChunkRequestProto>,
    ) -> Result<Response<ReadChunkResponseProto>, Status> {
        let req = request.into_inner();

        let chunk_ref = req.chunk.ok_or_else(|| Status::invalid_argument("missing chunk"))?;
        let block = chunk_ref
            .block
            .ok_or_else(|| Status::invalid_argument("missing block in chunk"))?;
        let data_handle_id = block.data_handle_id;
        let block_index = block.block_index;
        let chunk_idx = chunk_ref.chunk_index;

        let expected_version = req.expected_version;
        if expected_version > 0 {
            let current_version = self.get_block_version(data_handle_id, block_index).await;
            if expected_version != current_version {
                return Err(Status::new(
                    tonic::Code::FailedPrecondition,
                    format!(
                        "{}|Version mismatch: expected {}, actual {}",
                        14, expected_version, current_version
                    ),
                ));
            }
        }

        let blocks = self.blocks.read().await;
        let data = blocks
            .get(&(data_handle_id, block_index, chunk_idx))
            .cloned()
            .unwrap_or_default();

        let current_version = self.get_block_version(data_handle_id, block_index).await;

        let offset = req.offset_in_chunk as usize;
        let len = req.len as usize;
        let slice_data = if offset < data.len() {
            let end = (offset + len).min(data.len());
            data[offset..end].to_vec()
        } else {
            vec![]
        };

        let chunk_slice = ChunkSliceProto {
            chunk: Some(chunk_ref.clone()),
            offset_in_chunk: req.offset_in_chunk,
            len: req.len,
        };
        let chunk_data = ChunkDataProto {
            slice: Some(chunk_slice),
            data: slice_data.into(),
            checksum32: 0,
        };

        Ok(Response::new(ReadChunkResponseProto {
            data: Some(chunk_data),
            current_version,
        }))
    }

    type ReadRangeStream = std::pin::Pin<Box<dyn futures::Stream<Item = Result<ReadRangeChunkProto, Status>> + Send>>;

    async fn read_range(
        &self,
        request: Request<ReadRangeRequestProto>,
    ) -> Result<Response<Self::ReadRangeStream>, Status> {
        let req = request.into_inner();
        let data_handle_id = req.data_handle_id;
        let range = req.range.ok_or_else(|| Status::invalid_argument("missing range"))?;

        let block_size = 64 * 1024 * 1024;
        let block_index = (range.offset / block_size as u64) as u32;

        let expected_version = req.expected_version;
        if expected_version > 0 {
            let current_version = self.get_block_version(data_handle_id, block_index).await;
            if expected_version != current_version {
                return Err(Status::new(
                    tonic::Code::FailedPrecondition,
                    format!(
                        "{}|Version mismatch: expected {}, actual {}",
                        14, expected_version, current_version
                    ),
                ));
            }
        }

        let current_version = self.get_block_version(data_handle_id, block_index).await;

        let blocks = self.blocks.read().await;
        let data = blocks
            .get(&(data_handle_id, block_index, 0))
            .cloned()
            .unwrap_or_default();

        let offset = range.offset as usize;
        let len = range.len as usize;
        let slice_data = if offset < data.len() {
            let end = (offset + len).min(data.len());
            data[offset..end].to_vec()
        } else {
            vec![]
        };

        use futures::stream;
        let stream = stream::once(async move {
            if !slice_data.is_empty() {
                let chunk_ref = ChunkIdProto {
                    block: Some(proto::common::BlockIdProto {
                        data_handle_id,
                        block_index,
                    }),
                    chunk_index: 0,
                };
                let chunk_slice = ChunkSliceProto {
                    chunk: Some(chunk_ref),
                    offset_in_chunk: 0,
                    len: slice_data.len() as u32,
                };
                let chunk_data = ChunkDataProto {
                    slice: Some(chunk_slice),
                    data: slice_data.into(),
                    checksum32: 0,
                };
                Ok(ReadRangeChunkProto {
                    data: Some(chunk_data),
                    current_version,
                })
            } else {
                Err(Status::not_found("No data"))
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn write_chunk(
        &self,
        request: Request<WriteChunkRequestProto>,
    ) -> Result<Response<WriteChunkResponseProto>, Status> {
        let req = request.into_inner();

        let current_epoch = *self.worker_epoch.read().await;
        if req.worker_epoch != 0 && req.worker_epoch != current_epoch {
            let canonical = CanonicalError::need_refresh(
                RpcErrorCode::WorkerEpochMismatch,
                RefreshReason::WorkerEpochMismatch,
                "worker_epoch mismatch".to_string(),
            );
            let header = self.data_header(canonical, current_epoch);
            return Ok(Response::new(WriteChunkResponseProto {
                header: Some(header),
                stored: false,
            }));
        }

        if let Some(token) = req.token.as_ref() {
            let expected_epoch = *self.expected_fencing_epoch.read().await;
            if token.epoch != expected_epoch {
                let canonical = CanonicalError::need_refresh(
                    RpcErrorCode::Fencing,
                    RefreshReason::Fencing,
                    "fencing token epoch mismatch".to_string(),
                );
                let header = self.data_header(canonical, current_epoch);
                return Ok(Response::new(WriteChunkResponseProto {
                    header: Some(header),
                    stored: false,
                }));
            }
        }
        let chunk_data = req.data.ok_or_else(|| Status::invalid_argument("missing data"))?;
        let slice = chunk_data
            .slice
            .ok_or_else(|| Status::invalid_argument("missing slice"))?;

        let chunk_ref = slice
            .chunk
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing chunk in slice"))?;
        let block = chunk_ref
            .block
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing block in chunk"))?;
        let data_handle_id = block.data_handle_id;
        let block_index = block.block_index;
        let chunk_idx = chunk_ref.chunk_index;

        let mut blocks = self.blocks.write().await;
        blocks.insert((data_handle_id, block_index, chunk_idx), chunk_data.data.to_vec());

        let ok_header = proto::worker::DataResponseHeaderProto {
            client: Some(self.client_template.clone()),
            error: None,
            worker_epoch: Some(current_epoch),
            endpoint_hint: Some(self.build_endpoint_hint(current_epoch)),
        };
        Ok(Response::new(WriteChunkResponseProto {
            header: Some(ok_header),
            stored: true,
        }))
    }

    type ReadStreamStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<proto::worker::ReadStreamResponseProto, Status>> + Send>>;

    async fn open_read_stream(
        &self,
        _request: Request<proto::worker::OpenReadStreamRequestProto>,
    ) -> Result<Response<proto::worker::OpenReadStreamResponseProto>, Status> {
        Err(Status::unimplemented("Not implemented in mock"))
    }

    async fn open_write_stream(
        &self,
        _request: Request<proto::worker::OpenWriteStreamRequestProto>,
    ) -> Result<Response<proto::worker::OpenWriteStreamResponseProto>, Status> {
        Err(Status::unimplemented("Not implemented in mock"))
    }

    async fn read_stream(
        &self,
        _request: Request<proto::worker::ReadStreamRequestProto>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        Err(Status::unimplemented("Not implemented in mock"))
    }

    async fn write_stream(
        &self,
        _request: Request<tonic::Streaming<proto::worker::WriteStreamRequestProto>>,
    ) -> Result<Response<proto::worker::WriteStreamResponseProto>, Status> {
        Ok(Response::new(proto::worker::WriteStreamResponseProto {
            header: None,
            stored: true,
            acknowledged_offset: 0,
        }))
    }

    async fn commit_write(
        &self,
        request: Request<CommitWriteRequestProto>,
    ) -> Result<Response<CommitWriteResponseProto>, Status> {
        let req = request.into_inner();
        let current_epoch = *self.worker_epoch.read().await;

        if req.worker_epoch != 0 && req.worker_epoch != current_epoch {
            let canonical = CanonicalError::need_refresh(
                RpcErrorCode::WorkerEpochMismatch,
                RefreshReason::WorkerEpochMismatch,
                "worker_epoch mismatch".to_string(),
            );
            let header = self.data_header(canonical, current_epoch);
            return Ok(Response::new(CommitWriteResponseProto {
                header: Some(header),
                committed_length: 0,
                current_block_stamp: 0,
            }));
        }

        if let Some(token) = req.token.as_ref() {
            let expected_epoch = *self.expected_fencing_epoch.read().await;
            if token.epoch != expected_epoch {
                let canonical = CanonicalError::need_refresh(
                    RpcErrorCode::Fencing,
                    RefreshReason::Fencing,
                    "fencing token epoch mismatch".to_string(),
                );
                let header = self.data_header(canonical, current_epoch);
                return Ok(Response::new(CommitWriteResponseProto {
                    header: Some(header),
                    committed_length: 0,
                    current_block_stamp: 0,
                }));
            }
        }

        Ok(Response::new(CommitWriteResponseProto {
            header: Some(proto::worker::DataResponseHeaderProto {
                client: Some(self.client_template.clone()),
                error: None,
                worker_epoch: Some(current_epoch),
                endpoint_hint: Some(self.build_endpoint_hint(current_epoch)),
            }),
            committed_length: req.committed_length,
            current_block_stamp: 0,
        }))
    }
}
