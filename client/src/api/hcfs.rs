// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! HCFS-style API implementation.

use crate::cache::{FileMetaCache, RouteCache};
use crate::config::ClientConfig;
use crate::consistency::ConsistencyLevel;
use crate::error::{ClientError, ClientResult};
use crate::meta::{replay_policy_for_method, ActionMachine, RpcOp, TonicFileSystemRpc};
use crate::routing::{GroupRoleCache, RouteTable, WorkerSelector};
use crate::worker::client::WorkerEndpointInfo;
use crate::worker::WorkerClient;
use bytes::Bytes;
use common::header::RequestHeader;
use parking_lot::Mutex;
use proto::common::{BlockIdProto, ByteRangeProto, FileLayoutProto};
use proto::fs::FileAttrsProto;
use proto::metadata::get_block_locations_request_proto;
use proto::metadata::{
    AbortFileWriteRequestProto, AddBlockRequestProto, CommitFileRequestProto, CommittedBlockProto,
    CreateDispositionProto, CreateFileRequestProto, DeleteRequestProto, FileBlockLocationProto,
    GetBlockLocationsRequestProto, OpenFileRequestProto, WriteHandleProto, WriteTargetProto,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use types::fs::InodeId;
use types::ids::{BlockId, BlockIndex, DataHandleId};

const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024 * 1024;
const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024 * 1024;
const DEFAULT_REPLICATION: u32 = 1;

/// File open flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenFlags {
    /// Open for reading.
    Read,
    /// Open for writing.
    Write,
    /// Open for reading and writing.
    ReadWrite,
    /// Create file if it doesn't exist.
    Create,
    /// Truncate file on open.
    Truncate,
}

/// File handle (bound to data_handle_id for stability).
#[derive(Clone, Debug)]
pub struct Handle {
    /// File ID (stable identifier).
    pub data_handle_id: DataHandleId,
    /// Namespace identity (authoritative inode).
    pub inode_id: InodeId,
    /// File path (for logging/debugging).
    pub path: String,
    /// Open flags.
    pub flags: OpenFlags,
    /// File size observed when the handle was opened.
    pub file_size: u64,
    /// Optional file version observed when the handle was opened.
    pub file_version: Option<u64>,
    state: Arc<Mutex<HandleState>>,
}

#[derive(Clone, Debug)]
enum HandleState {
    Read,
    Write(WriteState),
    Aborted,
    Closed,
}

#[derive(Clone, Copy, Debug)]
enum WriteStart {
    Ready(WriteHandleProto),
    AbortUnsupported(WriteHandleProto),
}

#[derive(Clone, Debug)]
struct WriteState {
    write_handle: WriteHandleProto,
    data_handle_id: DataHandleId,
    base_size: u64,
    committed_blocks: Vec<CommittedBlockProto>,
}

/// Main client implementation.
pub struct Client {
    /// Client configuration.
    config: Arc<ClientConfig>,
    /// File metadata cache.
    file_meta_cache: Arc<FileMetaCache>,
    /// Route cache.
    _route_cache: Arc<RouteCache>,
    /// Route table.
    _route_table: Arc<RouteTable>,
    /// Group role cache.
    _group_role_cache: Arc<GroupRoleCache>,
    /// Worker selector.
    _worker_selector: Arc<WorkerSelector>,
    /// FileSystemService action machine.
    fs_machine: Arc<ActionMachine>,
    /// Per-client write id source for worker idempotency.
    next_write_id: AtomicU64,
}

impl Client {
    /// Create a new client.
    pub async fn new(config: ClientConfig) -> ClientResult<Self> {
        let endpoint = config
            .metadata_endpoints
            .first()
            .ok_or_else(|| ClientError::Metadata("No metadata endpoints available".to_string()))?;
        let rpc = Arc::new(TonicFileSystemRpc::connect(endpoint).await?);
        let fs_machine = Arc::new(ActionMachine::new(rpc, config.metadata_endpoints.clone()));

        // Create caches
        let file_meta_cache = Arc::new(FileMetaCache::new(
            config.cache.max_file_meta_entries,
            config.cache.file_meta_ttl_secs,
        ));
        let route_cache = RouteCache::new(config.cache.max_route_entries, config.cache.route_ttl_secs);

        // Create route table (clone cache for route table)
        let route_table = Arc::new(RouteTable::new(route_cache.clone()));
        let route_cache = Arc::new(route_cache);

        // Create group role cache
        let group_role_cache = Arc::new(GroupRoleCache::new(60)); // 60s health timeout

        // Create worker selector
        let worker_selector = Arc::new(WorkerSelector::new(crate::routing::SelectionStrategy::First));

        Ok(Self {
            config: Arc::new(config),
            file_meta_cache,
            _route_cache: route_cache,
            _route_table: route_table,
            _group_role_cache: group_role_cache,
            _worker_selector: worker_selector,
            fs_machine,
            next_write_id: AtomicU64::new(1),
        })
    }

    /// Open a file.
    pub async fn open(&self, path: &str, flags: OpenFlags) -> ClientResult<Handle> {
        match flags {
            OpenFlags::Read => self.open_read(path, flags).await,
            OpenFlags::Write | OpenFlags::Create | OpenFlags::Truncate | OpenFlags::ReadWrite => {
                self.open_create(path, flags).await
            }
        }
    }

    async fn open_read(&self, path: &str, flags: OpenFlags) -> ClientResult<Handle> {
        let request = OpenFileRequestProto {
            header: Some(self.request_header_proto()),
            path: path.to_string(),
            range: None,
            include_locations: false,
        };
        let response = self.call_filesystem(RpcOp::open_file(request)).await?;
        let inode_id = inode_id_from_proto(response.inode_id, "OpenFileResponseProto.inode_id")?;
        let data_handle_id =
            data_handle_id_from_proto(response.data_handle_id, "OpenFileResponseProto.data_handle_id")?;
        Ok(Handle {
            data_handle_id,
            inode_id,
            path: path.to_string(),
            flags,
            file_size: response.file_size,
            file_version: response.file_version,
            state: Arc::new(Mutex::new(HandleState::Read)),
        })
    }

    async fn open_create(&self, path: &str, flags: OpenFlags) -> ClientResult<Handle> {
        let disposition = match flags {
            OpenFlags::Truncate | OpenFlags::ReadWrite => CreateDispositionProto::Overwrite,
            _ => CreateDispositionProto::CreateNew,
        };
        let request = CreateFileRequestProto {
            header: Some(self.request_header_proto()),
            path: path.to_string(),
            attrs: Some(default_file_attrs()),
            layout: Some(default_file_layout()),
            disposition: disposition as i32,
            desired_len: None,
        };
        let response = self.call_filesystem(RpcOp::create_file(request)).await?;
        let inode_id = inode_id_from_proto(response.inode_id, "CreateFileResponseProto.inode_id")?;
        let data_handle_id =
            data_handle_id_from_proto(response.data_handle_id, "CreateFileResponseProto.data_handle_id")?;
        let write_handle = response
            .write_handle
            .ok_or_else(|| ClientError::Metadata("CreateFileResponseProto missing write_handle".to_string()))?;
        Ok(Handle {
            data_handle_id,
            inode_id,
            path: path.to_string(),
            flags,
            file_size: response.base_size,
            file_version: None,
            state: Arc::new(Mutex::new(HandleState::Write(WriteState {
                write_handle,
                data_handle_id,
                base_size: response.base_size,
                committed_blocks: Vec::new(),
            }))),
        })
    }

    /// Read from a file.
    pub async fn read(
        &self,
        handle: &Handle,
        offset: u64,
        len: u32,
        consistency: Option<ConsistencyLevel>,
    ) -> ClientResult<Bytes> {
        let consistency = consistency.unwrap_or(self.config.default_consistency);

        // Try cache first (if consistency allows)
        if consistency.allows_cache() {
            if let Some(_meta) = self.file_meta_cache.get(&handle.data_handle_id) {
                // TODO: Use cached metadata to read from worker
                // For now, fall through to metadata
            }
        }

        if len == 0 || offset >= handle.file_size {
            return Ok(Bytes::new());
        }
        let clipped_len = ((handle.file_size - offset).min(len as u64)) as u32;
        let request = GetBlockLocationsRequestProto {
            header: Some(self.request_header_proto()),
            target: Some(get_block_locations_request_proto::Target::Path(handle.path.clone())),
            range: Some(ByteRangeProto {
                offset,
                len: clipped_len,
            }),
        };
        let response = self.call_filesystem(RpcOp::get_block_locations(request)).await?;
        let read_plan = select_single_read_plan(&response.locations, offset, clipped_len)?;
        let mut worker = WorkerClient::new(read_plan.worker, None).await?;
        let ctx = self.request_header();
        let (bytes, _version) = worker
            .read_chunk(
                &ctx,
                types::chunk::ChunkRef::new(read_plan.block_id, 0),
                read_plan.offset_in_block,
                clipped_len,
                handle.file_version,
                self.config.default_read_mode,
                None,
            )
            .await?;
        Ok(bytes)
    }

    /// Write to a file.
    pub async fn write(&self, handle: &Handle, offset: u64, data: Bytes) -> ClientResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        let data_len = data.len() as u64;
        let write_handle = match self.prepare_write(handle, offset)? {
            WriteStart::Ready(write_handle) => write_handle,
            WriteStart::AbortUnsupported(write_handle) => {
                self.abort_file_write(write_handle).await?;
                return Err(ClientError::NotSupported(
                    "HCFS MVP supports exactly one sequential write per create handle".to_string(),
                ));
            }
        };

        let target = match self.add_block(write_handle, data_len).await {
            Ok(target) => target,
            Err(err) => {
                self.mark_aborted(handle);
                let abort_result = self.abort_file_write(write_handle).await;
                return match abort_result {
                    Ok(()) => Err(err),
                    Err(abort_err) => Err(ClientError::Metadata(format!(
                        "AddBlock failed: {}; abort failed: {}",
                        err, abort_err
                    ))),
                };
            }
        };

        let write_result = self.write_target_to_worker(&target, data).await;
        if let Err(write_err) = write_result {
            self.mark_aborted(handle);
            let abort_result = self.abort_file_write(write_handle).await;
            return match abort_result {
                Ok(()) => Err(write_err),
                Err(abort_err) => Err(ClientError::Worker(format!(
                    "worker write failed: {}; abort failed: {}",
                    write_err, abort_err
                ))),
            };
        }

        let committed = committed_block_from_target(&target)?;
        let mut state = handle.state.lock();
        match &mut *state {
            HandleState::Write(write) => {
                write.committed_blocks.push(committed);
                Ok(())
            }
            HandleState::Aborted => Err(ClientError::Metadata(
                "write handle was aborted during write".to_string(),
            )),
            HandleState::Closed => Err(ClientError::Metadata("write handle is closed".to_string())),
            HandleState::Read => Err(ClientError::Metadata("handle is not open for writing".to_string())),
        }
    }

    fn prepare_write(&self, handle: &Handle, offset: u64) -> ClientResult<WriteStart> {
        let mut state = handle.state.lock();
        match &mut *state {
            HandleState::Write(write) => {
                if !write.committed_blocks.is_empty() || offset != write.base_size {
                    let write_handle = write.write_handle;
                    *state = HandleState::Aborted;
                    return Ok(WriteStart::AbortUnsupported(write_handle));
                }
                Ok(WriteStart::Ready(write.write_handle))
            }
            HandleState::Aborted => Err(ClientError::Metadata(
                "write handle has already been aborted".to_string(),
            )),
            HandleState::Closed => Err(ClientError::Metadata("write handle is closed".to_string())),
            HandleState::Read => Err(ClientError::Metadata("handle is not open for writing".to_string())),
        }
    }

    async fn add_block(&self, write_handle: WriteHandleProto, len: u64) -> ClientResult<WriteTargetProto> {
        let request = AddBlockRequestProto {
            header: Some(self.request_header_proto()),
            write_handle: Some(write_handle),
            desired_len: Some(len),
        };
        let response = self.call_filesystem(RpcOp::add_block(request)).await?;
        response
            .target
            .ok_or_else(|| ClientError::Metadata("AddBlockResponseProto missing target".to_string()))
    }

    async fn write_target_to_worker(&self, target: &WriteTargetProto, data: Bytes) -> ClientResult<()> {
        let data_len = u32::try_from(data.len()).map_err(|_| {
            ClientError::NotSupported("HCFS MVP single worker chunk write is limited to u32::MAX bytes".to_string())
        })?;
        if data.len() as u64 > target.len {
            return Err(ClientError::Metadata(format!(
                "worker write data length {} exceeds AddBlock target len {}",
                data.len(),
                target.len
            )));
        }
        let block_id = block_id_from_proto(target.block_id, "WriteTargetProto.block_id")?;
        let worker_endpoint = target
            .worker_endpoints
            .first()
            .cloned()
            .ok_or_else(|| ClientError::Metadata("WriteTargetProto has no worker endpoints".to_string()))?;
        let token = target
            .fencing_token
            .ok_or_else(|| ClientError::Metadata("WriteTargetProto missing fencing_token".to_string()))?;
        let worker_epoch = worker_endpoint.worker_epoch;
        let mut worker = WorkerClient::new(WorkerEndpointInfo::from_proto(worker_endpoint), None).await?;
        let request = proto::worker::WriteChunkRequestProto {
            token: Some(token),
            data: Some(proto::worker::ChunkDataProto {
                slice: Some(proto::worker::ChunkSliceProto {
                    chunk: Some(proto::common::ChunkIdProto {
                        block: Some(block_id_to_proto(block_id)),
                        chunk_index: 0,
                    }),
                    offset_in_chunk: 0,
                    len: data_len,
                }),
                data,
                checksum32: 0,
            }),
            write_id: self.next_write_id.fetch_add(1, Ordering::Relaxed),
            write_mode: proto::common::WriteModeProto::from(self.config.default_write_mode) as i32,
            route_epoch: 0,
            worker_epoch,
            file_version: 0,
        };
        let metadata_refresh = Box::new(move |_worker_id| {
            Box::pin(async {
                Err(ClientError::Metadata(
                    "worker endpoint refresh is not wired for HCFS MVP write replay".to_string(),
                ))
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = ClientResult<WorkerEndpointInfo>> + Send>>
        });
        worker
            .write_chunk_with_refresh(&self.request_header(), request, metadata_refresh, None)
            .await
            .map(|_| ())
    }

    /// Close a file handle.
    pub async fn close(&self, handle: Handle) -> ClientResult<()> {
        let commit = {
            let mut state = handle.state.lock();
            match &mut *state {
                HandleState::Read => {
                    *state = HandleState::Closed;
                    return Ok(());
                }
                HandleState::Closed => return Ok(()),
                HandleState::Aborted => return Err(ClientError::Metadata("write handle has been aborted".to_string())),
                HandleState::Write(write) => {
                    let final_size = write
                        .committed_blocks
                        .iter()
                        .map(|block| block.file_offset + block.len)
                        .max()
                        .unwrap_or(write.base_size);
                    (
                        write.write_handle,
                        write.data_handle_id,
                        write.committed_blocks.clone(),
                        final_size,
                    )
                }
            }
        };

        let (write_handle, data_handle_id, committed_blocks, final_size) = commit;
        let request = CommitFileRequestProto {
            header: Some(self.request_header_proto()),
            write_handle: Some(write_handle),
            data_handle_id: Some(proto::common::DataHandleIdProto {
                value: data_handle_id.as_raw(),
            }),
            committed_blocks,
            final_size,
        };
        self.call_filesystem(RpcOp::commit_file(request)).await?;
        *handle.state.lock() = HandleState::Closed;
        Ok(())
    }

    /// Get file status.
    pub async fn stat(&self, _path: &str) -> ClientResult<FileStatus> {
        // TODO: Implement stat
        Err(ClientError::Unimplemented("stat not yet implemented".to_string()))
    }

    /// List directory.
    pub async fn list(&self, _path: &str) -> ClientResult<Vec<FileStatus>> {
        // TODO: Implement list
        Err(ClientError::Unimplemented("list not yet implemented".to_string()))
    }

    /// Rename a file or directory.
    pub async fn rename(&self, _src: &str, _dst: &str) -> ClientResult<()> {
        // TODO: Implement rename
        Err(ClientError::Unimplemented("rename not yet implemented".to_string()))
    }

    async fn call_filesystem<T>(&self, op: RpcOp<T>) -> ClientResult<T> {
        let policy = replay_policy_for_method(op.method());
        self.fs_machine.call_with_refresh(policy, op).await
    }

    fn request_header(&self) -> RequestHeader {
        RequestHeader::new(types::ClientId::new(self.client_id()))
    }

    fn request_header_proto(&self) -> proto::common::RequestHeaderProto {
        (&self.request_header()).into()
    }

    fn client_id(&self) -> u64 {
        self.config.inner.as_flat().get_i64("client.id").unwrap_or(0) as u64
    }

    async fn abort_file_write(&self, write_handle: WriteHandleProto) -> ClientResult<()> {
        let request = AbortFileWriteRequestProto {
            header: Some(self.request_header_proto()),
            write_handle: Some(write_handle),
        };
        self.call_filesystem(RpcOp::abort_file_write(request)).await?;
        Ok(())
    }

    fn mark_aborted(&self, handle: &Handle) {
        *handle.state.lock() = HandleState::Aborted;
    }

    /// Delete a file, symlink, or empty directory.
    pub async fn delete(&self, path: &str, recursive: bool) -> ClientResult<()> {
        let request = DeleteRequestProto {
            header: Some(self.request_header_proto()),
            path: path.to_string(),
            recursive,
        };
        self.call_filesystem(RpcOp::delete(request)).await.map(|_| ())
    }
}

#[derive(Clone, Debug)]
struct ReadPlan {
    block_id: BlockId,
    offset_in_block: u32,
    worker: WorkerEndpointInfo,
}

fn default_file_attrs() -> FileAttrsProto {
    FileAttrsProto {
        mode: 0o644,
        uid: 0,
        gid: 0,
        size: 0,
        atime_ms: 0,
        mtime_ms: 0,
        ctime_ms: 0,
        nlink: 1,
    }
}

fn default_file_layout() -> FileLayoutProto {
    FileLayoutProto {
        block_size: DEFAULT_BLOCK_SIZE,
        chunk_size: DEFAULT_CHUNK_SIZE,
        replication: DEFAULT_REPLICATION,
    }
}

fn inode_id_from_proto(value: Option<proto::fs::InodeIdProto>, field: &str) -> ClientResult<InodeId> {
    value
        .map(|id| InodeId::new(id.value))
        .ok_or_else(|| ClientError::Metadata(format!("{} missing", field)))
}

fn data_handle_id_from_proto(
    value: Option<proto::common::DataHandleIdProto>,
    field: &str,
) -> ClientResult<DataHandleId> {
    value
        .map(|id| DataHandleId::new(id.value))
        .ok_or_else(|| ClientError::Metadata(format!("{} missing", field)))
}

fn block_id_from_proto(value: Option<BlockIdProto>, field: &str) -> ClientResult<BlockId> {
    let block = value.ok_or_else(|| ClientError::Metadata(format!("{} missing", field)))?;
    Ok(BlockId::new(
        DataHandleId::new(block.data_handle_id),
        BlockIndex::new(block.block_index),
    ))
}

fn block_id_to_proto(block_id: BlockId) -> BlockIdProto {
    BlockIdProto {
        data_handle_id: block_id.data_handle_id.as_raw(),
        block_index: block_id.index.as_raw(),
    }
}

fn select_single_read_plan(locations: &[FileBlockLocationProto], offset: u64, len: u32) -> ClientResult<ReadPlan> {
    let end = offset + len as u64;
    let mut overlapping = locations
        .iter()
        .filter(|location| location.file_offset < end && location.file_offset + location.len > offset);
    let location = overlapping
        .next()
        .ok_or_else(|| ClientError::Metadata("read has no block location".to_string()))?;
    if overlapping.next().is_some() || offset < location.file_offset || end > location.file_offset + location.len {
        return Err(ClientError::NotSupported(
            "HCFS MVP multi-block read is not supported".to_string(),
        ));
    }
    let offset_in_block = u32::try_from(offset - location.file_offset)
        .map_err(|_| ClientError::NotSupported("HCFS MVP read offset does not fit worker chunk request".to_string()))?;
    let worker = location
        .workers
        .first()
        .cloned()
        .ok_or_else(|| ClientError::Metadata("read block location has no worker endpoints".to_string()))?;
    Ok(ReadPlan {
        block_id: block_id_from_proto(location.block_id, "FileBlockLocationProto.block_id")?,
        offset_in_block,
        worker: WorkerEndpointInfo::from_proto(worker),
    })
}

fn committed_block_from_target(target: &WriteTargetProto) -> ClientResult<CommittedBlockProto> {
    let block_id = target
        .block_id
        .ok_or_else(|| ClientError::Metadata("WriteTargetProto.block_id missing".to_string()))?;
    Ok(CommittedBlockProto {
        block_id: Some(block_id),
        file_offset: target.file_offset,
        len: target.len,
        checksum: None,
    })
}

/// File status information.
#[derive(Clone, Debug)]
pub struct FileStatus {
    /// File path.
    pub path: String,
    /// File ID.
    pub data_handle_id: DataHandleId,
    /// Is directory.
    pub is_directory: bool,
    /// File length.
    pub length: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::header::{ClientInfo, ResponseHeader};
    use futures::Stream;
    use proto::common::{BlockIdProto, ClientInfoProto, DataHandleIdProto, FencingTokenProto, WorkerEndpointInfoProto};
    use proto::fs::InodeIdProto;
    use proto::metadata::file_system_service_proto_server::{FileSystemServiceProto, FileSystemServiceProtoServer};
    use proto::metadata::get_block_locations_request_proto;
    use proto::metadata::*;
    use proto::worker::worker_data_service_server::{WorkerDataService, WorkerDataServiceServer};
    use proto::worker::*;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};
    use types::ClientId;

    #[derive(Default)]
    struct MetadataState {
        calls: Vec<&'static str>,
        get_locations: Vec<GetBlockLocationsRequestProto>,
        commit_requests: Vec<CommitFileRequestProto>,
        aborts: usize,
    }

    #[derive(Clone)]
    struct MockMetadata {
        state: Arc<Mutex<MetadataState>>,
        locations: Vec<FileBlockLocationProto>,
        worker_endpoint: WorkerEndpointInfoProto,
    }

    #[derive(Default)]
    struct WorkerState {
        reads: Vec<ReadChunkRequestProto>,
        writes: Vec<WriteChunkRequestProto>,
        blocks: HashMap<(u64, u32), Bytes>,
    }

    #[derive(Clone)]
    struct MockWorker {
        state: Arc<Mutex<WorkerState>>,
        fail_write: bool,
    }

    struct TestEnv {
        client: Client,
        metadata: Arc<Mutex<MetadataState>>,
        worker: Arc<Mutex<WorkerState>>,
    }

    impl MockMetadata {
        fn new(
            state: Arc<Mutex<MetadataState>>,
            locations: Vec<FileBlockLocationProto>,
            worker_endpoint: WorkerEndpointInfoProto,
        ) -> Self {
            Self {
                state,
                locations,
                worker_endpoint,
            }
        }

        fn record(&self, call: &'static str) {
            self.state.lock().expect("metadata state").calls.push(call);
        }

        fn file_size(&self) -> u64 {
            self.locations
                .iter()
                .map(|location| location.file_offset + location.len)
                .max()
                .unwrap_or(5)
        }
    }

    fn ok_header() -> proto::common::ResponseHeaderProto {
        (&ResponseHeader::ok(ClientInfo::new(ClientId::new(1)))).into()
    }

    fn data_header() -> DataResponseHeaderProto {
        DataResponseHeaderProto {
            client: Some(ClientInfoProto {
                call_id: "test-call".to_string(),
                client_id: 1,
                client_name: "hcfs-test".to_string(),
            }),
            error: None,
            worker_epoch: None,
            endpoint_hint: None,
        }
    }

    fn block_id(index: u32) -> BlockIdProto {
        BlockIdProto {
            data_handle_id: 42,
            block_index: index,
        }
    }

    fn fencing_token(epoch: u64) -> FencingTokenProto {
        FencingTokenProto {
            block_id: Some(block_id(0)),
            owner: 7,
            epoch,
        }
    }

    fn file_location(worker: WorkerEndpointInfoProto, offset: u64, len: u64, index: u32) -> FileBlockLocationProto {
        FileBlockLocationProto {
            block_id: Some(block_id(index)),
            file_offset: offset,
            len,
            workers: vec![worker],
            worker_epoch: Some(100),
        }
    }

    #[tonic::async_trait]
    impl FileSystemServiceProto for MockMetadata {
        async fn get_status(
            &self,
            _request: Request<GetStatusRequestProto>,
        ) -> Result<Response<GetStatusResponseProto>, Status> {
            Err(Status::unimplemented("get_status"))
        }

        async fn list_status(
            &self,
            _request: Request<ListStatusRequestProto>,
        ) -> Result<Response<ListStatusResponseProto>, Status> {
            Err(Status::unimplemented("list_status"))
        }

        async fn create_directory(
            &self,
            _request: Request<CreateDirectoryRequestProto>,
        ) -> Result<Response<CreateDirectoryResponseProto>, Status> {
            Err(Status::unimplemented("create_directory"))
        }

        async fn delete(&self, _request: Request<DeleteRequestProto>) -> Result<Response<DeleteResponseProto>, Status> {
            Err(Status::unimplemented("delete"))
        }

        async fn rename(&self, _request: Request<RenameRequestProto>) -> Result<Response<RenameResponseProto>, Status> {
            Err(Status::unimplemented("rename"))
        }

        async fn open_file(
            &self,
            request: Request<OpenFileRequestProto>,
        ) -> Result<Response<OpenFileResponseProto>, Status> {
            self.record("open_file");
            let request = request.into_inner();
            Ok(Response::new(OpenFileResponseProto {
                header: Some(ok_header()),
                inode_id: Some(InodeIdProto { value: 11 }),
                data_handle_id: Some(DataHandleIdProto { value: 42 }),
                file_size: self.file_size(),
                file_version: Some(9),
                locations: if request.include_locations {
                    self.locations.clone()
                } else {
                    Vec::new()
                },
            }))
        }

        async fn get_block_locations(
            &self,
            request: Request<GetBlockLocationsRequestProto>,
        ) -> Result<Response<GetBlockLocationsResponseProto>, Status> {
            self.record("get_block_locations");
            self.state
                .lock()
                .expect("metadata state")
                .get_locations
                .push(request.into_inner());
            Ok(Response::new(GetBlockLocationsResponseProto {
                header: Some(ok_header()),
                inode_id: Some(InodeIdProto { value: 11 }),
                data_handle_id: Some(DataHandleIdProto { value: 42 }),
                file_size: self.file_size(),
                locations: self.locations.clone(),
                file_version: Some(9),
            }))
        }

        async fn create_file(
            &self,
            _request: Request<CreateFileRequestProto>,
        ) -> Result<Response<CreateFileResponseProto>, Status> {
            self.record("create_file");
            Ok(Response::new(CreateFileResponseProto {
                header: Some(ok_header()),
                write_handle: Some(WriteHandleProto {
                    handle_id: 1000,
                    lease_id: Some(proto::common::LeaseIdProto { high: 0, low: 55 }),
                    lease_epoch: 77,
                    open_epoch: 88,
                    fencing_token: Some(fencing_token(77)),
                }),
                inode_id: Some(InodeIdProto { value: 11 }),
                data_handle_id: Some(DataHandleIdProto { value: 42 }),
                base_size: 0,
                initial_targets: Vec::new(),
                expires_at_ms: 9999,
            }))
        }

        async fn append_file(
            &self,
            _request: Request<AppendFileRequestProto>,
        ) -> Result<Response<AppendFileResponseProto>, Status> {
            Err(Status::unimplemented("append_file"))
        }

        async fn add_block(
            &self,
            request: Request<AddBlockRequestProto>,
        ) -> Result<Response<AddBlockResponseProto>, Status> {
            self.record("add_block");
            let desired_len = request.into_inner().desired_len.unwrap_or(5);
            Ok(Response::new(AddBlockResponseProto {
                header: Some(ok_header()),
                target: Some(WriteTargetProto {
                    block_id: Some(block_id(0)),
                    file_offset: 0,
                    len: desired_len,
                    worker_endpoints: vec![self.worker_endpoint.clone()],
                    fencing_token: Some(fencing_token(99)),
                }),
            }))
        }

        async fn commit_file(
            &self,
            request: Request<CommitFileRequestProto>,
        ) -> Result<Response<CommitFileResponseProto>, Status> {
            self.record("commit_file");
            self.state
                .lock()
                .expect("metadata state")
                .commit_requests
                .push(request.into_inner());
            Ok(Response::new(CommitFileResponseProto {
                header: Some(ok_header()),
                committed_size: 5,
                file_version: Some(10),
            }))
        }

        async fn abort_file_write(
            &self,
            _request: Request<AbortFileWriteRequestProto>,
        ) -> Result<Response<AbortFileWriteResponseProto>, Status> {
            self.record("abort_file_write");
            self.state.lock().expect("metadata state").aborts += 1;
            Ok(Response::new(AbortFileWriteResponseProto {
                header: Some(ok_header()),
            }))
        }

        async fn renew_lease(
            &self,
            _request: Request<RenewLeaseRequestProto>,
        ) -> Result<Response<RenewLeaseResponseProto>, Status> {
            Err(Status::unimplemented("renew_lease"))
        }

        async fn hflush(&self, _request: Request<HflushRequestProto>) -> Result<Response<HflushResponseProto>, Status> {
            Err(Status::unimplemented("hflush"))
        }

        async fn hsync(&self, _request: Request<HsyncRequestProto>) -> Result<Response<HsyncResponseProto>, Status> {
            Err(Status::unimplemented("hsync"))
        }

        async fn msync(&self, _request: Request<MsyncRequestProto>) -> Result<Response<MsyncResponseProto>, Status> {
            Err(Status::unimplemented("msync"))
        }
    }

    #[tonic::async_trait]
    impl WorkerDataService for MockWorker {
        async fn read_chunk(
            &self,
            request: Request<ReadChunkRequestProto>,
        ) -> Result<Response<ReadChunkResponseProto>, Status> {
            let request = request.into_inner();
            let chunk = request
                .chunk
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing chunk"))?;
            let block = chunk
                .block
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing block"))?;
            let data = {
                let mut state = self.state.lock().expect("worker state");
                state.reads.push(request);
                state
                    .blocks
                    .get(&(block.data_handle_id, block.block_index))
                    .cloned()
                    .unwrap_or_else(|| Bytes::from_static(b"hello"))
            };
            let start = request.offset_in_chunk as usize;
            let end = (start + request.len as usize).min(data.len());
            Ok(Response::new(ReadChunkResponseProto {
                data: Some(ChunkDataProto {
                    slice: Some(ChunkSliceProto {
                        chunk: Some(*chunk),
                        offset_in_chunk: request.offset_in_chunk,
                        len: request.len,
                    }),
                    data: data.slice(start..end),
                    checksum32: 0,
                }),
                current_version: 9,
            }))
        }

        async fn write_chunk(
            &self,
            request: Request<WriteChunkRequestProto>,
        ) -> Result<Response<WriteChunkResponseProto>, Status> {
            let request = request.into_inner();
            if self.fail_write {
                return Err(Status::internal("forced write failure"));
            }
            let data = request
                .data
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing data"))?;
            let chunk = data
                .slice
                .as_ref()
                .and_then(|slice| slice.chunk.as_ref())
                .ok_or_else(|| Status::invalid_argument("missing chunk"))?;
            let block = chunk
                .block
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing block"))?;
            let mut state = self.state.lock().expect("worker state");
            state.writes.push(request.clone());
            state
                .blocks
                .insert((block.data_handle_id, block.block_index), data.data.clone());
            Ok(Response::new(WriteChunkResponseProto {
                header: Some(data_header()),
                stored: true,
            }))
        }

        type ReadRangeStream = Pin<Box<dyn Stream<Item = Result<ReadRangeChunkProto, Status>> + Send>>;

        async fn read_range(
            &self,
            _request: Request<ReadRangeRequestProto>,
        ) -> Result<Response<Self::ReadRangeStream>, Status> {
            Err(Status::unimplemented("read_range"))
        }

        async fn open_read_stream(
            &self,
            _request: Request<OpenReadStreamRequestProto>,
        ) -> Result<Response<OpenReadStreamResponseProto>, Status> {
            Err(Status::unimplemented("open_read_stream"))
        }

        type ReadStreamStream = Pin<Box<dyn Stream<Item = Result<ReadStreamResponseProto, Status>> + Send>>;

        async fn read_stream(
            &self,
            _request: Request<ReadStreamRequestProto>,
        ) -> Result<Response<Self::ReadStreamStream>, Status> {
            Err(Status::unimplemented("read_stream"))
        }

        async fn open_write_stream(
            &self,
            _request: Request<OpenWriteStreamRequestProto>,
        ) -> Result<Response<OpenWriteStreamResponseProto>, Status> {
            Err(Status::unimplemented("open_write_stream"))
        }

        async fn write_stream(
            &self,
            _request: Request<tonic::Streaming<WriteStreamRequestProto>>,
        ) -> Result<Response<WriteStreamResponseProto>, Status> {
            Err(Status::unimplemented("write_stream"))
        }

        async fn commit_write(
            &self,
            _request: Request<CommitWriteRequestProto>,
        ) -> Result<Response<CommitWriteResponseProto>, Status> {
            Err(Status::unimplemented("commit_write"))
        }
    }

    async fn start_worker(fail_write: bool) -> (SocketAddr, Arc<Mutex<WorkerState>>) {
        let state = Arc::new(Mutex::new(WorkerState::default()));
        let service = MockWorker {
            state: state.clone(),
            fail_write,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
        let addr = listener.local_addr().expect("worker addr");
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(WorkerDataServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("worker server");
        });
        (addr, state)
    }

    async fn start_env(
        build_locations: impl FnOnce(WorkerEndpointInfoProto) -> Vec<FileBlockLocationProto>,
        fail_write: bool,
    ) -> TestEnv {
        let (worker_addr, worker) = start_worker(fail_write).await;
        let worker_endpoint = WorkerEndpointInfoProto {
            worker_id: 7,
            endpoint: worker_addr.to_string(),
            net_transport_kind: proto::common::NetTransportKindProto::NetTransportKindGrpc as i32,
            worker_epoch: 100,
        };
        let locations = build_locations(worker_endpoint.clone());

        let metadata = Arc::new(Mutex::new(MetadataState::default()));
        let service = MockMetadata::new(metadata.clone(), locations, worker_endpoint);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind metadata");
        let metadata_addr = listener.local_addr().expect("metadata addr");
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FileSystemServiceProtoServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("metadata server");
        });

        let config = ClientConfig {
            metadata_endpoints: vec![format!("http://{}", metadata_addr)],
            ..Default::default()
        };
        let client = Client::new(config).await.expect("client");
        TestEnv {
            client,
            metadata,
            worker,
        }
    }

    async fn default_env(fail_write: bool) -> TestEnv {
        start_env(|worker| vec![file_location(worker, 0, 5, 0)], fail_write).await
    }

    #[tokio::test]
    async fn open_file_uses_metadata() {
        let env = default_env(false).await;

        let handle = env.client.open("/file", OpenFlags::Read).await.expect("open");

        assert_eq!(handle.inode_id, InodeId::new(11));
        assert_eq!(handle.data_handle_id, DataHandleId::new(42));
        assert!(env.metadata.lock().expect("metadata").calls.contains(&"open_file"));
    }

    #[tokio::test]
    async fn read_uses_block_locations() {
        let env = default_env(false).await;
        let handle = env.client.open("/file", OpenFlags::Read).await.expect("open");

        let bytes = env.client.read(&handle, 0, 5, None).await.expect("read");

        assert_eq!(bytes, Bytes::from_static(b"hello"));
        let metadata = env.metadata.lock().expect("metadata");
        assert_eq!(metadata.get_locations.len(), 1);
        assert!(matches!(
            metadata.get_locations[0].target.as_ref(),
            Some(get_block_locations_request_proto::Target::Path(path)) if path == "/file"
        ));
        drop(metadata);
        let worker = env.worker.lock().expect("worker");
        assert_eq!(worker.reads.len(), 1);
        assert_eq!(
            worker.reads[0].chunk.as_ref().and_then(|chunk| chunk.block.as_ref()),
            Some(&block_id(0))
        );
        assert_eq!(worker.reads[0].offset_in_chunk, 0);
        assert_eq!(worker.reads[0].len, 5);
    }

    #[tokio::test]
    async fn create_write_commit_read_one_block() {
        let env = default_env(false).await;
        let handle = env.client.open("/file", OpenFlags::Create).await.expect("create");

        env.client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("write");
        env.client.close(handle).await.expect("close");

        let read_handle = env.client.open("/file", OpenFlags::Read).await.expect("open read");
        let bytes = env.client.read(&read_handle, 0, 5, None).await.expect("read back");
        assert_eq!(bytes, Bytes::from_static(b"hello"));
        assert_eq!(env.worker.lock().expect("worker").writes.len(), 1);
        assert_eq!(env.metadata.lock().expect("metadata").commit_requests.len(), 1);
    }

    #[tokio::test]
    async fn write_uses_fencing_token() {
        let env = default_env(false).await;
        let handle = env.client.open("/file", OpenFlags::Create).await.expect("create");

        env.client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("write");

        let worker = env.worker.lock().expect("worker");
        let token = worker.writes[0].token.as_ref().expect("token");
        assert_eq!(token, &fencing_token(99));
    }

    #[tokio::test]
    async fn commit_uses_written_block() {
        let env = default_env(false).await;
        let handle = env.client.open("/file", OpenFlags::Create).await.expect("create");

        env.client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("write");
        env.client.close(handle).await.expect("close");

        let metadata = env.metadata.lock().expect("metadata");
        let commit = metadata.commit_requests.first().expect("commit request");
        assert_eq!(commit.final_size, 5);
        assert_eq!(commit.committed_blocks.len(), 1);
        assert_eq!(commit.committed_blocks[0].block_id, Some(block_id(0)));
        assert_eq!(commit.committed_blocks[0].len, 5);
    }

    #[tokio::test]
    async fn abort_after_write_failure() {
        let env = default_env(true).await;
        let handle = env.client.open("/file", OpenFlags::Create).await.expect("create");

        let err = env
            .client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("write should fail");

        assert!(matches!(err, ClientError::Worker(_) | ClientError::Action(_)));
        let metadata = env.metadata.lock().expect("metadata");
        assert_eq!(metadata.aborts, 1);
        assert!(metadata.commit_requests.is_empty());
    }

    #[tokio::test]
    async fn read_missing_location_fails() {
        let env = start_env(|_| Vec::new(), false).await;
        let handle = env.client.open("/file", OpenFlags::Read).await.expect("open");

        let err = env.client.read(&handle, 0, 5, None).await.expect_err("read");

        assert!(matches!(err, ClientError::Metadata(msg) if msg.contains("block location")));
    }

    #[tokio::test]
    async fn multi_block_read_not_supported() {
        let env = start_env(
            |worker| {
                vec![
                    file_location(worker.clone(), 0, 5, 0),
                    file_location(worker.clone(), 5, 5, 1),
                ]
            },
            false,
        )
        .await;
        let handle = env.client.open("/file", OpenFlags::Read).await.expect("open");

        let err = env.client.read(&handle, 0, 10, None).await.expect_err("read");

        assert!(matches!(err, ClientError::NotSupported(msg) if msg.contains("multi-block")));
        assert!(env.worker.lock().expect("worker").reads.is_empty());
    }
}
