// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! HCFS-style API implementation.

use crate::cache::{FileMetaCache, RouteCache};
use crate::config::ClientConfig;
use crate::consistency::ConsistencyLevel;
use crate::error::{ClientError, ClientResult};
use crate::meta::{replay_policy_for_method, ActionMachine, RpcOp, TonicFileSystemRpc};
use crate::routing::{GroupRoleCache, RouteTable, WorkerSelector};
use bytes::Bytes;
use common::header::RequestHeader;
use parking_lot::Mutex;
use proto::common::FileLayoutProto;
use proto::fs::FileAttrsProto;
use proto::metadata::{
    CommitFileRequestProto, CommittedBlockProto, CreateDispositionProto, CreateFileRequestProto, DeleteRequestProto,
    OpenFileRequestProto, WriteHandleProto,
};
use std::sync::Arc;
use types::fs::InodeId;
use types::ids::DataHandleId;

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
    Closed,
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
        Err(ClientError::Unimplemented(
            "HCFS direct read must be rewired to WorkerDataService stream v2".to_string(),
        ))
    }

    /// Write to a file.
    pub async fn write(&self, handle: &Handle, offset: u64, data: Bytes) -> ClientResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        let _ = (handle, offset);
        Err(ClientError::Unimplemented(
            "HCFS direct write must be rewired to WorkerDataService stream v2".to_string(),
        ))
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
    use proto::common::{DataHandleIdProto, FencingTokenProto};
    use proto::fs::InodeIdProto;
    use proto::metadata::file_system_service_proto_server::{FileSystemServiceProto, FileSystemServiceProtoServer};
    use proto::metadata::*;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport as tonic_net;
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
        file_size: u64,
    }

    struct TestEnv {
        client: Client,
        metadata: Arc<Mutex<MetadataState>>,
    }

    impl MockMetadata {
        fn new(state: Arc<Mutex<MetadataState>>, file_size: u64) -> Self {
            Self { state, file_size }
        }

        fn record(&self, call: &'static str) {
            self.state.lock().expect("metadata state").calls.push(call);
        }

        fn file_size(&self) -> u64 {
            self.file_size
        }
    }

    fn ok_header() -> proto::common::ResponseHeaderProto {
        (&ResponseHeader::ok(ClientInfo::new(ClientId::new(1)))).into()
    }

    fn fencing_token(epoch: u64) -> FencingTokenProto {
        FencingTokenProto {
            block_id: None,
            owner: 7,
            epoch,
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
            _request: Request<OpenFileRequestProto>,
        ) -> Result<Response<OpenFileResponseProto>, Status> {
            self.record("open_file");
            Ok(Response::new(OpenFileResponseProto {
                header: Some(ok_header()),
                inode_id: Some(InodeIdProto { value: 11 }),
                data_handle_id: Some(DataHandleIdProto { value: 42 }),
                file_size: self.file_size(),
                file_version: Some(9),
                locations: Vec::new(),
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
                locations: Vec::new(),
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
                    block_id: None,
                    file_offset: 0,
                    len: desired_len,
                    worker_endpoints: Vec::new(),
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

    async fn start_env(file_size: u64) -> TestEnv {
        let metadata = Arc::new(Mutex::new(MetadataState::default()));
        let service = MockMetadata::new(metadata.clone(), file_size);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind metadata");
        let metadata_addr = listener.local_addr().expect("metadata addr");
        tokio::spawn(async move {
            tonic_net::Server::builder()
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
        TestEnv { client, metadata }
    }

    async fn default_env() -> TestEnv {
        start_env(5).await
    }

    #[tokio::test]
    async fn open_file_uses_metadata() {
        let env = default_env().await;

        let handle = env.client.open("/file", OpenFlags::Read).await.expect("open");

        assert_eq!(handle.inode_id, InodeId::new(11));
        assert_eq!(handle.data_handle_id, DataHandleId::new(42));
        assert!(env.metadata.lock().expect("metadata").calls.contains(&"open_file"));
    }

    #[tokio::test]
    async fn non_empty_read_is_explicitly_unimplemented_until_stream_v2_wiring() {
        let env = default_env().await;
        let handle = env.client.open("/file", OpenFlags::Read).await.expect("open");

        let err = env.client.read(&handle, 0, 5, None).await.expect_err("read");

        assert!(matches!(err, ClientError::Unimplemented(msg) if msg.contains("WorkerDataService stream v2")));
        assert!(env.metadata.lock().expect("metadata").get_locations.is_empty());
    }

    #[tokio::test]
    async fn zero_length_read_returns_empty_without_worker_io() {
        let env = default_env().await;
        let handle = env.client.open("/file", OpenFlags::Read).await.expect("open");

        let bytes = env.client.read(&handle, 0, 0, None).await.expect("read");

        assert!(bytes.is_empty());
        assert!(env.metadata.lock().expect("metadata").get_locations.is_empty());
    }

    #[tokio::test]
    async fn non_empty_write_is_explicitly_unimplemented_until_stream_v2_wiring() {
        let env = default_env().await;
        let handle = env.client.open("/file", OpenFlags::Create).await.expect("create");

        let err = env
            .client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("write");

        assert!(matches!(err, ClientError::Unimplemented(msg) if msg.contains("WorkerDataService stream v2")));
        let metadata = env.metadata.lock().expect("metadata");
        assert_eq!(metadata.aborts, 0);
        assert!(metadata.commit_requests.is_empty());
    }

    #[tokio::test]
    async fn close_unwritten_create_handle_commits_empty_file() {
        let env = default_env().await;
        let handle = env.client.open("/file", OpenFlags::Create).await.expect("create");

        env.client.close(handle).await.expect("close");

        let metadata = env.metadata.lock().expect("metadata");
        let commit = metadata.commit_requests.first().expect("commit request");
        assert_eq!(commit.final_size, 0);
        assert!(commit.committed_blocks.is_empty());
    }

    #[tokio::test]
    async fn read_past_eof_returns_empty_without_worker_io() {
        let env = start_env(5).await;
        let handle = env.client.open("/file", OpenFlags::Read).await.expect("open");

        let bytes = env.client.read(&handle, 5, 5, None).await.expect("read");

        assert!(bytes.is_empty());
        assert!(env.metadata.lock().expect("metadata").get_locations.is_empty());
    }
}
