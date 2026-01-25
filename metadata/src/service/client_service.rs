// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataClientService implementation.

use super::extract_and_inject_context;
use super::{fatal_fs_header, need_refresh_header, ok_header_from_request, retryable_header};
use crate::error::{MetadataError, MetadataResult};
use crate::file_handle::FileHandleManager;
use crate::lease_runtime::LeaseRuntimeTable;
use crate::maintenance::MaintenanceService;
use crate::metrics::MetadataMetrics;
use crate::mount::MountTable;
use crate::raft::RocksDBStorage;
use crate::state::StateStore;
use crate::ufs_proxy::UfsMetadataProxy;
use crate::worker::WorkerManager;
use common::error::canonical::{CanonicalError, RefreshReason};
use common::header::{ResponseHeader, RpcErrorCode};
use proto::metadata::metadata_client_service_server::MetadataClientService;
use proto::metadata::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{instrument, warn};
use types::block::{BlockPlacement, BlockState};
use types::chunk::ByteRange;
use types::fs::InodeId;
use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, WorkerId};
use types::layout::FileLayout;
use types::lease::FencingToken;
use types::RaftLogId;

/// MetadataClientService implementation.
pub struct MetadataClientServiceImpl {
    state_store: Arc<dyn StateStore>,
    mount_table: Arc<MountTable>,
    worker_manager: Arc<WorkerManager>,
    storage: Option<Arc<RocksDBStorage>>, // Optional storage for presence hints
    ufs_proxy: Option<Arc<UfsMetadataProxy>>, // Optional UFS metadata proxy
    maintenance_service: Option<Arc<MaintenanceService>>, // Optional maintenance service for ref counting
    file_handle_manager: Arc<FileHandleManager>,          // File handle manager
    metrics: Arc<MetadataMetrics>,                        // Metrics
    raft_node: Option<Arc<crate::raft::RaftNode>>, // Optional Raft node for leader/follower info
    lease_runtime: Option<Arc<LeaseRuntimeTable>>, // Optional lease runtime table (leader-only)
}

impl MetadataClientServiceImpl {
    pub fn new(
        state_store: Arc<dyn StateStore>,
        mount_table: Arc<MountTable>,
        worker_manager: Arc<WorkerManager>,
        ufs_metadata_proxy: Arc<UfsMetadataProxy>,
        maintenance_service: Arc<MaintenanceService>,
    ) -> Self {
        Self {
            state_store,
            mount_table,
            worker_manager,
            storage: None,
            ufs_proxy: Some(ufs_metadata_proxy),
            maintenance_service: Some(maintenance_service),
            file_handle_manager: Arc::new(FileHandleManager::new()),
            metrics: Arc::new(MetadataMetrics::new()),
            raft_node: None,
            lease_runtime: None,
        }
    }

    /// Set Raft node for leader/follower information (optional).
    pub fn with_raft_node(mut self, raft_node: Arc<crate::raft::RaftNode>) -> Self {
        self.raft_node = Some(raft_node);
        self
    }

    /// Set storage for presence hints (optional).
    pub fn with_storage(mut self, storage: Arc<RocksDBStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set lease runtime table (optional, for RenewLease support).
    pub fn with_lease_runtime(mut self, lease_runtime: Arc<LeaseRuntimeTable>) -> Self {
        self.lease_runtime = Some(lease_runtime);
        self
    }

    /// Get all node IDs from Raft membership (leader and followers).
    fn get_raft_membership(&self) -> (Option<u64>, Vec<u64>) {
        if let Some(ref raft_node) = self.raft_node {
            raft_node.get_membership_nodes()
        } else {
            (None, vec![])
        }
    }

    /// Get shard-to-group mapping from router or storage.
    async fn get_shard_to_group_mapping(
        &self,
        data_handle_id: Option<DataHandleId>,
    ) -> std::collections::HashMap<u64, u64> {
        // Hash-based routing removed; metadata routing is inode/mount-owner based.
        // Response keeps shape but returns empty mapping.
        let _ = data_handle_id;
        std::collections::HashMap::new()
    }

    /// Get metrics for export.
    pub fn metrics(&self) -> Arc<MetadataMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Helper: get current state_id from Raft node.
    /// Returns None if Raft node is not available.
    fn get_current_state_id(&self) -> Option<proto::common::RaftLogIdProto> {
        self.raft_node
            .as_ref()
            .and_then(|rn| rn.get_last_applied_state_id())
            .map(|sid| sid.into())
    }

    /// Helper: create a response header with state_id.
    fn create_response_header(&self, call_id: types::CallId) -> ResponseHeader {
        let client = common::header::ClientInfo {
            call_id,
            client_id: types::ClientId::new(0),
            client_name: None,
        };
        self.create_response_header_from_client(client, None)
    }

    /// Helper: create a response header from ClientInfo with state_id and group_id.
    fn create_response_header_from_client(
        &self,
        client: common::header::ClientInfo,
        group_id: Option<u64>,
    ) -> ResponseHeader {
        let mut header = ResponseHeader::ok(client);
        if let Some(sid) = self
            .raft_node
            .as_ref()
            .and_then(|rn| rn.get_last_applied_state_id())
        {
            header = header.with_state_id(sid);
        }
        if let Some(gid) = group_id {
            header = header.with_group_id(gid);
        }
        header
    }

    // DEPRECATED: Use error_helpers::ok_header_from_request instead.
    /// Helper: create a response header from request, extracting group_id and filling state_id.
    /// group_id is extracted from request.header.group_id (if present) or derived from routing.
    #[allow(dead_code)]
    fn create_response_header_from_request(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        group_id: Option<u64>,
    ) -> ResponseHeader {
        if let Some(header) = req_header {
            let client = header
                .client
                .as_ref()
                .map(|c| common::header::ClientInfo::try_from(c.clone()).ok())
                .flatten()
                .unwrap_or_else(|| common::header::ClientInfo::new(types::ClientId::new(0)));
            // Use group_id from request if provided, otherwise use the passed group_id
            let final_group_id = if header.group_id != 0 {
                Some(header.group_id)
            } else {
                group_id
            };
            self.create_response_header_from_client(client, final_group_id)
        } else {
            self.create_response_header_from_client(
                common::header::ClientInfo::new(types::ClientId::new(0)),
                group_id,
            )
        }
    }

    /// Helper: resolve group_id from request header or inode (authoritative).
    /// Returns error if group_id cannot be derived or mismatches mount owner.
    fn resolve_group_id(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        data_handle_id: Option<DataHandleId>,
        _path: Option<&str>,
    ) -> MetadataResult<types::ids::ShardGroupId> {
        // Prefer explicit group_id from header when provided.
        let mut header_group = req_header
            .as_ref()
            .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None });

        // Try to derive from inode->mount owner when data_handle_id is supplied.
        let derived_group = if let Some(fid) = data_handle_id {
            let storage = self.storage.as_ref().ok_or_else(|| {
                MetadataError::InvalidArgument(
                    "group_id missing and storage unavailable to resolve mount owner".to_string(),
                )
            })?;
            let inode_id = types::fs::InodeId::new(fid.as_raw());
            let inode = storage.get_inode(inode_id)?.ok_or_else(|| {
                MetadataError::StaleState(format!(
                    "Inode {} not found; client must refresh mount table",
                    inode_id
                ))
            })?;
            let mount = self.mount_table.get_mount(inode.mount_id)?.ok_or_else(|| {
                MetadataError::StaleState(format!(
                    "Mount {:?} not found for inode {}; client must refresh",
                    inode.mount_id, inode_id
                ))
            })?;
            Some(mount.namespace_owner_group_id.as_raw())
        } else {
            None
        };

        if let Some(dg) = derived_group {
            if let Some(hg) = header_group {
                if hg != dg {
                    return Err(MetadataError::StaleState(format!(
                        "group_id mismatch: header={} mount_owner={}",
                        hg, dg
                    )));
                }
            } else {
                header_group = Some(dg);
            }
        }

        header_group
            .map(types::ids::ShardGroupId::new)
            .ok_or_else(|| {
                MetadataError::InvalidArgument(
                    "group_id is required and could not be derived from inode/mount".to_string(),
                )
            })
    }

    /// Helper: check if this node can serve a read request (follower read-gating).
    /// Returns Ok(()) if read can be served, Err with STALE_STATE if not.
    fn check_read_gating(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        group_id: types::ids::ShardGroupId,
    ) -> MetadataResult<()> {
        // Only check if we have a Raft node and request has state_id
        if let (Some(ref raft_node), Some(ref header)) = (self.raft_node.as_ref(), req_header) {
            if let Some(ref requested_state_id) = header.state_id {
                let requested = RaftLogId::from(requested_state_id.clone());
                if let Some(last_applied) = raft_node.get_last_applied_state_id() {
                    // Check if last_applied >= requested_state_id
                    // Compare by index (assuming same group)
                    if last_applied.index < requested.index {
                        return Err(MetadataError::StaleState(format!(
                            "Follower last_applied={} < requested={}",
                            last_applied.index, requested.index
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Helper: convert FileLayout from proto to types.
    fn proto_to_file_layout(
        layout: Option<proto::common::FileLayoutProto>,
    ) -> MetadataResult<FileLayout> {
        let layout = layout
            .ok_or_else(|| MetadataError::InvalidArgument("Missing FileLayout".to_string()))?;
        Ok(FileLayout::new(
            layout.block_size,
            layout.chunk_size,
            layout.replication as u8,
        ))
    }

    /// Helper: convert FileLayout from types to proto.
    fn file_layout_to_proto(layout: &FileLayout) -> proto::common::FileLayoutProto {
        proto::common::FileLayoutProto {
            block_size: layout.block_size,
            chunk_size: layout.chunk_size,
            replication: layout.replication as u32,
        }
    }

    /// Helper: convert BlockPlacement from types to proto.
    fn block_placement_to_proto(placement: &BlockPlacement) -> proto::common::BlockPlacementProto {
        proto::common::BlockPlacementProto {
            primary_worker_id: placement.primary.as_raw(),
            replica_worker_ids: placement.replicas.iter().map(|w| w.as_raw()).collect(),
        }
    }

    /// Helper: convert BlockState from types to proto.
    fn block_state_to_proto(state: BlockState) -> i32 {
        match state {
            BlockState::Open => 1,      // BLOCK_STATE_OPEN
            BlockState::Sealed => 2,    // BLOCK_STATE_SEALED
            BlockState::Aborted => 3,   // BLOCK_STATE_ABORTED
            BlockState::Deleted => 3, // Map Deleted to Aborted for now (proto doesn't have Deleted yet)
            BlockState::Compacted => 3, // Map Compacted to Aborted for now (proto doesn't have Compacted yet)
        }
    }

    /// Helper: convert BlockMeta from state to proto.
    fn block_meta_to_proto(
        block_meta: &crate::state::BlockMetaState,
        layout: &FileLayout,
    ) -> proto::common::BlockMetaProto {
        let start_offset = layout.block_start_offset(block_meta.block_id.index);
        proto::common::BlockMetaProto {
            inode_id: block_meta.inode_id.as_raw(),
            data_handle_id: block_meta.data_handle_id.as_raw(),
            block_index: block_meta.block_id.index.as_raw(),
            start_offset,
            state: Self::block_state_to_proto(block_meta.state),
            placement: Some(Self::block_placement_to_proto(&block_meta.placement)),
            committed_length: block_meta.committed_length,
            block_stamp: 0, // TODO: implement block_stamp tracking
        }
    }

    /// Helper: allocate block placement using worker registry.
    fn allocate_block_placement(&self, replication: u8) -> MetadataResult<BlockPlacement> {
        // Use worker registry to select primary and replicas
        self.worker_manager
            .select_workers_for_placement(replication, None)
            .map_err(|e| {
                MetadataError::ServiceUnavailable(format!("Failed to allocate placement: {}", e))
            })
    }
}

#[tonic::async_trait]
impl MetadataClientService for MetadataClientServiceImpl {
    #[instrument(skip(self), fields(call_id, client_id))]
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let path = req.path;
        let group_id = self.resolve_group_id(&req.header, None, Some(&path)).ok();
        let layout = Self::proto_to_file_layout(req.layout)?;

        // Check if path already exists (try UFS first, then metadata)
        if let Some(ref ufs_proxy) = self.ufs_proxy {
            if let Ok(Some(_)) = self.mount_table.resolve_path(&path) {
                // Path is mounted, check UFS
                match ufs_proxy.exists(&path, &caller_ctx).await {
                    Ok(true) => {
                        return Err(MetadataError::AlreadyExists(format!(
                            "File already exists: {}",
                            path
                        ))
                        .into());
                    }
                    Ok(false) => {
                        // File doesn't exist in UFS, continue to create
                    }
                    Err(e) => {
                        warn!(path = %path, error = %e, "UFS exists check failed, continuing");
                        // Fall through to metadata check
                    }
                }
            }
        }

        // LEGACY: Path-based file operations are deprecated.
        // Use MetadataFsServiceProto::Create (inode-based) instead.
        return Err(MetadataError::InvalidArgument(
            "Path-based file operations are deprecated. Use FS service (inode/dentry-based) instead.".to_string(),
        ).into());

        Ok(Response::new(CreateFileResponse {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                ))
                    .into(),
            ),
            data_handle_id: file_meta.data_handle_id.as_raw(),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_file_status(
        &self,
        request: Request<GetFileStatusRequest>,
    ) -> Result<Response<GetFileStatusResponse>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        // If data_handle_id is provided, use it directly
        if req.data_handle_id != 0 {
            // LEGACY: get_file removed. Use FS service (inode-based) instead.
            return Err(MetadataError::InvalidArgument(
                "get_file_status with data_handle_id is no longer supported. Use FS service (inode-based) instead.".to_string(),
            ).into());

            return Ok(Response::new(GetFileStatusResponse {
                header: Some(
                    (&self.create_response_header_from_request(
                        &req.header,
                        group_id.map(|g| g.as_raw()),
                    ))
                        .into(),
                ),
                data_handle_id: file_meta.data_handle_id.as_raw(),
                layout: Some(Self::file_layout_to_proto(&file_meta.layout)),
                committed_length: file_meta.committed_length,
                exists: true,
            }));
        }

        // If data_handle_id is 0, we can't look up by path without a path field
        // For now, just return an error
        let err: CanonicalError =
            MetadataError::InvalidArgument("data_handle_id is required".to_string()).into();
        let resp_header = super::header_from_canonical_error(&req.header, None, None, &err);
        Ok(Response::new(GetFileStatusResponse {
            header: Some(resp_header),
            data_handle_id: 0,
            layout: None,
            committed_length: 0,
            exists: false,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn list_status(
        &self,
        request: Request<ListStatusRequest>,
    ) -> Result<Response<ListStatusResponse>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let group_id = self
            .resolve_group_id(&req.header, None, Some(&req.path))
            .ok();
        // Check read-gating for follower reads
        if let Some(gid) = group_id {
            self.check_read_gating(&req.header, gid)?;
        }

        // Try UFS proxy first if path is mounted
        if let Some(ref ufs_proxy) = self.ufs_proxy {
            if let Ok(Some(_)) = self.mount_table.resolve_path(&req.path) {
                // Path is mounted, try UFS list
                match ufs_proxy.list(&req.path, &caller_ctx).await {
                    Ok(ufs_entries) => {
                        let entries = ufs_entries
                            .into_iter()
                            .map(|e| FileStatus {
                                path: e.path,
                                data_handle_id: 0, // UFS entries don't have data_handle_id
                                is_directory: e.is_dir,
                                length: e.size.unwrap_or(0),
                            })
                            .collect();
                        return Ok(Response::new(ListStatusResponse {
                            header: Some(
                                (&self.create_response_header_from_request(
                                    &req.header,
                                    group_id.map(|g| g.as_raw()),
                                ))
                                    .into(),
                            ),
                            entries,
                        }));
                    }
                    Err(e) => {
                        warn!(path = %req.path, error = %e, "UFS list failed, falling back to metadata");
                        // Fall through to metadata list
                    }
                }
            }
        }

        // LEGACY: Path-based list_status is deprecated.
        // Use MetadataPathServiceProto::ListStatusPath or MetadataFsServiceProto::ReadDir instead.
        return Err(MetadataError::InvalidArgument(
            "Path-based list_status is deprecated. Use MetadataPathServiceProto::ListStatusPath or MetadataFsServiceProto::ReadDir instead.".to_string(),
        ).into());
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rename(
        &self,
        request: Request<RenameRequest>,
    ) -> Result<Response<RenameResponse>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        let group_id = self
            .resolve_group_id(&req.header, None, Some(&req.src_path))
            .ok();

        // Try UFS proxy first if paths are mounted
        if let Some(ref ufs_proxy) = self.ufs_proxy {
            let src_mounted = self
                .mount_table
                .resolve_path(&req.src_path)
                .is_ok_and(|r| r.is_some());
            let dst_mounted = self
                .mount_table
                .resolve_path(&req.dst_path)
                .is_ok_and(|r| r.is_some());

            if src_mounted || dst_mounted {
                // At least one path is mounted, try UFS rename
                match ufs_proxy
                    .rename(&req.src_path, &req.dst_path, &caller_ctx)
                    .await
                {
                    Ok(()) => {
                        // LEGACY: Path-based rename is deprecated. Use FS service (inode-based) instead.
                        // UFS rename succeeded, but we can't update metadata path (path is not authoritative).
                        return Ok(Response::new(RenameResponse {
                            header: Some(
                                (&self.create_response_header_from_client(
                                    caller_ctx.client.clone(),
                                    group_id.map(|g| g.as_raw()),
                                ))
                                    .into(),
                            ),
                            success: true,
                        }));
                    }
                    Err(e) => {
                        warn!(
                            src_path = %req.src_path,
                            dst_path = %req.dst_path,
                            error = %e,
                            "UFS rename failed, falling back to metadata"
                        );
                        self.metrics
                            .ufs_operations_failed_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        // Fall through to metadata rename
                    }
                }
            }
        }

        // LEGACY: Path-based rename is deprecated. Use MetadataFsServiceProto::Rename (inode-based) instead.
        return Err(MetadataError::InvalidArgument(
            "Path-based rename is deprecated. Use FS service (inode/dentry-based) instead.".to_string(),
        ).into());

        // Use group_id from file_meta's data_handle_id
        let group_id = self
            .resolve_group_id(&req.header, Some(file_meta.data_handle_id), None)
            .ok();
        Ok(Response::new(RenameResponse {
            header: Some(
                (&self.create_response_header_from_client(
                    caller_ctx.client.clone(),
                    group_id.map(|g| g.as_raw()),
                ))
                    .into(),
            ),
            success: true,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        let group_id = self
            .resolve_group_id(&req.header, None, Some(&req.path))
            .ok();

        // Try UFS proxy first if path is mounted
        if let Some(ref ufs_proxy) = self.ufs_proxy {
            if let Ok(Some(_)) = self.mount_table.resolve_path(&req.path) {
                // Path is mounted, try UFS delete
                match ufs_proxy
                    .delete(&req.path, req.recursive, &caller_ctx)
                    .await
                {
                    Ok(()) => {
                        return Ok(Response::new(DeleteResponse {
                            header: Some(
                                (&self.create_response_header_from_client(
                                    caller_ctx.client.clone(),
                                    group_id.map(|g| g.as_raw()),
                                ))
                                    .into(),
                            ),
                            success: true,
                        }));
                    }
                    Err(e) => {
                        warn!(path = %req.path, error = %e, "UFS delete failed, falling back to metadata");
                        // Fall through to metadata delete
                    }
                }
            }
        }

        // LEGACY: Path-based delete is deprecated. Use MetadataFsServiceProto::Unlink (inode-based) instead.
        return Err(MetadataError::InvalidArgument(
            "Path-based delete is deprecated. Use FS service (inode/dentry-based) instead.".to_string(),
        ).into());

        // Use group_id from file_meta's data_handle_id
        let final_group_id = self
            .resolve_group_id(&req.header, Some(file_meta.data_handle_id), None)
            .ok();
        Ok(Response::new(DeleteResponse {
            header: Some(
                (&self.create_response_header_from_client(
                    caller_ctx.client.clone(),
                    final_group_id.map(|g| g.as_raw()),
                ))
                    .into(),
            ),
            success: true,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn mkdirs(
        &self,
        request: Request<MkdirsRequest>,
    ) -> Result<Response<MkdirsResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Implement directory support: recursively create directory nodes
        // Paths are normalized (remove trailing slashes)
        let path = req.path.trim_end_matches('/');
        let group_id = self.resolve_group_id(&req.header, None, Some(&path)).ok();
        if path.is_empty() || path == "/" {
            // Root directory always exists
            return Ok(Response::new(MkdirsResponse {
                header: Some(
                    (&self.create_response_header_from_request(
                        &req.header,
                        group_id.map(|g| g.as_raw()),
                    ))
                        .into(),
                ),
                success: true,
            }));
        }

        // Split path into components and create each directory level
        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let mut current_path = String::new();

        for component in components {
            if current_path.is_empty() {
                current_path = format!("/{}", component);
            } else {
                current_path = format!("{}/{}", current_path, component);
            }

            // LEGACY: Path-based mkdirs is deprecated. Use MetadataFsServiceProto::Mkdir (inode-based) instead.
            // Skip path-based directory creation - path is not authoritative.
        }

        Ok(Response::new(MkdirsResponse {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                ))
                    .into(),
            ),
            success: true,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn locate(
        &self,
        request: Request<LocateRequest>,
    ) -> Result<Response<LocateResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        let data_handle_id = DataHandleId::new(req.data_handle_id);
        let group_id = self.resolve_group_id(&req.header, Some(data_handle_id), None).ok();
        // Check read-gating for follower reads
        if let Some(gid) = group_id {
            self.check_read_gating(&req.header, gid)?;
        }
        let range = req
            .range
            .ok_or_else(|| MetadataError::InvalidArgument("Missing ByteRange".to_string()))?;

        // LEGACY: get_file removed. Use FS service (inode-based) instead.
        return Err(MetadataError::InvalidArgument(
            "locate is no longer supported with data_handle_id. Use FS service (inode-based) instead.".to_string(),
        ).into());
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn acquire_lease(
        &self,
        request: Request<AcquireLeaseRequest>,
    ) -> Result<Response<AcquireLeaseResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Require inode_id; resolve to current data_handle_id for data-plane lease.
        let inode_id = InodeId::new(req.inode_id);
        let inode = self
            .state_store
            .get_inode(inode_id)
            .await?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;
        let data_handle_id = inode.current_data_handle_id;

        // Routing remains inode-based; group derive uses inode.
        let group_id = self.resolve_group_id(&req.header, Some(data_handle_id), Some(inode_id)).ok();
        if let Some(gid) = group_id {
            self.check_read_gating(&req.header, gid)?;
        }

        let block_index = BlockIndex::new(req.block_index);
        let block_id = BlockId::new(data_handle_id, block_index);
        let client_id = ClientId::new(req.client_id);

        // Get or create block using inode layout.
        let layout = self.state_store.get_layout(inode_id).await?;
        let block_meta = if let Some(meta) = self.state_store.get_block(block_id).await? {
            meta
        } else {
            let placement = self.allocate_block_placement(layout.replication)?;
            self.state_store.create_block(inode_id, block_id, placement).await?
        };

        // Acquire lease
        let layout_version = self.state_store.get_layout_version().await?;
        let epoch = layout_version.as_u64();
        let expires_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 30_000; // 30 seconds lease

        let lease_state = self
            .state_store
            .acquire_lease(block_id, client_id, epoch, expires_at_ms)
            .await?;

        let token = FencingToken {
            block_id,
            owner: client_id,
            epoch: lease_state.lease.epoch,
        };

        Ok(Response::new(AcquireLeaseResponse {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                ))
                .into(),
            ),
            token: Some(proto::common::FencingTokenProto {
                block_id: Some(block_id.into()),
                owner: client_id.as_raw(),
                epoch: token.epoch,
            }),
            placement: Some(Self::block_placement_to_proto(&block_meta.placement)),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn renew_lease(
        &self,
        request: Request<RenewLeaseRequest>,
    ) -> Result<Response<RenewLeaseResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        let data_handle_id = DataHandleId::new(req.data_handle_id);
        let group_id = self.resolve_group_id(&req.header, Some(data_handle_id), None).ok();

        // Leader-only check
        let is_leader = self
            .raft_node
            .as_ref()
            .map(|n| n.is_leader())
            .unwrap_or(false);
        if !is_leader {
            let resp_header = need_refresh_header(
                &req.header,
                RpcErrorCode::NotLeader,
                RefreshReason::NotLeader,
                "RenewLease requires leader. Please refresh and retry.",
                group_id.map(|g| g.as_raw()),
                None,
            );
            return Ok(Response::new(RenewLeaseResponse {
                header: Some(resp_header),
                lease_deadline_ms: 0,
                renew_after_ms: 0,
                server_epoch: None,
            }));
        }

        // Check if lease runtime is available
        let lease_runtime = match self.lease_runtime.as_ref() {
            Some(rt) => rt,
            None => {
                let err: CanonicalError =
                    MetadataError::ServiceUnavailable("Lease runtime not available".to_string())
                        .into();
                let resp_header = super::header_from_canonical_error(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                    None,
                    &err,
                );
                return Ok(Response::new(RenewLeaseResponse {
                    header: Some(resp_header),
                    lease_deadline_ms: 0,
                    renew_after_ms: 0,
                    server_epoch: None,
                }));
            }
        };

        let data_handle_id = DataHandleId::new(req.data_handle_id);
        let group_id = self.resolve_group_id(&req.header, Some(data_handle_id), None).ok();
        let block_index = BlockIndex::new(req.block_index);
        let block_id = BlockId::new(data_handle_id, block_index);
        let client_id = ClientId::new(req.client_id);
        let lease_epoch = req.lease_epoch;

        // Verify lease exists and epoch matches (CAS check)
        let lease_state = match self.state_store.get_lease(block_id).await {
            Ok(Some(lease)) => lease,
            Ok(None) => {
                let err: CanonicalError =
                    MetadataError::NotFound(format!("Lease not found for block: {:?}", block_id))
                        .into();
                let resp_header = super::header_from_canonical_error(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                    None,
                    &err,
                );
                return Ok(Response::new(RenewLeaseResponse {
                    header: Some(resp_header),
                    lease_deadline_ms: 0,
                    renew_after_ms: 0,
                    server_epoch: None,
                }));
            }
            Err(e) => {
                let err: CanonicalError =
                    MetadataError::Internal(format!("Failed to get lease: {}", e)).into();
                let resp_header = super::header_from_canonical_error(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                    None,
                    &err,
                );
                return Ok(Response::new(RenewLeaseResponse {
                    header: Some(resp_header),
                    lease_deadline_ms: 0,
                    renew_after_ms: 0,
                    server_epoch: None,
                }));
            }
        };

        // CAS check: epoch must match
        if lease_state.lease.epoch != lease_epoch {
            let err: CanonicalError = MetadataError::LeaseFenced {
                expected: lease_state.lease.epoch,
                got: lease_epoch,
            }
            .into();
            let resp_header = super::header_from_canonical_error(
                &req.header,
                group_id.map(|g| g.as_raw()),
                None,
                &err,
            );
            return Ok(Response::new(RenewLeaseResponse {
                header: Some(resp_header),
                lease_deadline_ms: 0,
                renew_after_ms: 0,
                server_epoch: None,
            }));
        }

        // Verify owner matches
        if lease_state.lease.owner.as_raw() != client_id.as_raw() {
            let err: CanonicalError = MetadataError::InvalidArgument(format!(
                "Lease owner mismatch: expected {}, got {}",
                lease_state.lease.owner.as_raw(),
                client_id.as_raw()
            ))
            .into();
            let resp_header = super::header_from_canonical_error(
                &req.header,
                group_id.map(|g| g.as_raw()),
                None,
                &err,
            );
            return Ok(Response::new(RenewLeaseResponse {
                header: Some(resp_header),
                lease_deadline_ms: 0,
                renew_after_ms: 0,
                server_epoch: None,
            }));
        }

        // Update runtime (in-memory only, no Raft write)
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let (lease_deadline_ms, renew_after_ms) =
            lease_runtime.renew_lease(block_id, client_id, now_ms);

        // Update metrics
        self.metrics
            .renew_lease_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let layout_version = match self.state_store.get_layout_version().await {
            Ok(version) => version,
            Err(e) => {
                let err: CanonicalError =
                    MetadataError::Internal(format!("Failed to get layout version: {}", e)).into();
                let resp_header = super::header_from_canonical_error(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                    None,
                    &err,
                );
                return Ok(Response::new(RenewLeaseResponse {
                    header: Some(resp_header),
                    lease_deadline_ms: 0,
                    renew_after_ms: 0,
                    server_epoch: None,
                }));
            }
        };
        let server_epoch = layout_version.as_u64();

        Ok(Response::new(RenewLeaseResponse {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                ))
                    .into(),
            ),
            lease_deadline_ms,
            renew_after_ms,
            server_epoch: Some(server_epoch),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn seal_block(
        &self,
        request: Request<SealBlockRequest>,
    ) -> Result<Response<SealBlockResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        let data_handle_id = DataHandleId::new(req.data_handle_id);
        let group_id = self.resolve_group_id(&req.header, Some(data_handle_id), None).ok();
        let block_index = BlockIndex::new(req.block_index);
        let block_id = BlockId::new(data_handle_id, block_index);

        let token = req
            .token
            .ok_or_else(|| MetadataError::InvalidArgument("Missing FencingToken".to_string()))?;

        // Verify lease
        let lease_state = self.state_store.get_lease(block_id).await?.ok_or_else(|| {
            MetadataError::NotFound(format!("Lease not found for block: {:?}", block_id))
        })?;

        if lease_state.lease.owner.as_raw() != token.owner {
            return Err(MetadataError::LeaseFenced {
                expected: lease_state.lease.epoch,
                got: token.epoch,
            }
            .into());
        }

        if lease_state.lease.epoch != token.epoch {
            return Err(MetadataError::LeaseFenced {
                expected: lease_state.lease.epoch,
                got: token.epoch,
            }
            .into());
        }

        // Seal block
        self.state_store
            .update_block_state(block_id, BlockState::Sealed)
            .await?;

        Ok(Response::new(SealBlockResponse {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                ))
                    .into(),
            ),
            sealed: true,
        }))
    }

    // LEGACY: commit_length removed. Use FS service (inode-based) instead.

    // Note: report_presence RPC removed - presence is now maintained via
    // block_report (memory-only in WorkerManager), not via PresenceHint.

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn open_file(
        &self,
        request: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        let data_handle_id = DataHandleId::new(req.data_handle_id);
        let group_id = self.resolve_group_id(&req.header, Some(data_handle_id), None).ok();
        // Check read-gating for follower reads
        if let Some(gid) = group_id {
            self.check_read_gating(&req.header, gid)?;
        }
        // LEGACY: open_file removed. Use FS service (inode-based) instead.
        return Err(MetadataError::InvalidArgument(
            "open_file is no longer supported. Use FS service (inode-based) instead.".to_string(),
        ).into());
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn close_file(
        &self,
        request: Request<CloseFileRequest>,
    ) -> Result<Response<CloseFileResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Close the file handle
        let file_handle = crate::file_handle::FileHandle::new(req.file_handle);
        // For close_file, we can use file_handle as data_handle_id (since handle = data_handle_id in current impl)
        let data_handle_id = DataHandleId::new(req.file_handle);
        let group_id = self.resolve_group_id(&req.header, Some(data_handle_id), None).ok();
        self.file_handle_manager
            .close_file(file_handle)
            .map_err(|e| MetadataError::InvalidArgument(format!("Invalid file handle: {}", e)))?;

        Ok(Response::new(CloseFileResponse {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                ))
                    .into(),
            ),
            success: true,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn truncate_file(
        &self,
        request: Request<TruncateFileRequest>,
    ) -> Result<Response<TruncateFileResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        let data_handle_id = DataHandleId::new(req.data_handle_id);
        let group_id = self.resolve_group_id(&req.header, Some(data_handle_id), None).ok();
        let new_length = req.new_length;

        // LEGACY: truncate_file removed. Use FS service Truncate (inode-based) instead.
        return Err(MetadataError::InvalidArgument(
            "truncate_file is no longer supported. Use FS service Truncate (inode-based) instead.".to_string(),
        ).into());

        Ok(Response::new(TruncateFileResponse {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                ))
                    .into(),
            ),
            success: true,
        }))
    }

    // LEGACY: get_file_meta removed. Use FS service (inode-based) instead.

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn refresh_route(
        &self,
        request: Request<RefreshRouteRequest>,
    ) -> Result<Response<RefreshRouteResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Get current route_epoch (use layout_version as route_epoch)
        let route_epoch = self.state_store.get_layout_version().await?.as_u64();

        // Get shard-to-group mapping
        let data_handle_id = if req.data_handle_id != 0 {
            Some(DataHandleId::new(req.data_handle_id))
        } else {
            None
        };
        let group_id = self.resolve_group_id(&req.header, data_handle_id, None).ok();
        let shard_to_group = self.get_shard_to_group_mapping(data_handle_id).await;

        Ok(Response::new(RefreshRouteResponse {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    group_id.map(|g| g.as_raw()),
                ))
                    .into(),
            ),
            route_epoch,
            shard_to_group,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn msync(
        &self,
        request: Request<MsyncRequest>,
    ) -> Result<Response<MsyncResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Get group_id from request header (required)
        let group_id = if let Some(ref header) = req.header {
            if header.group_id != 0 {
                types::ids::ShardGroupId::new(header.group_id)
            } else {
                let err: CanonicalError = MetadataError::InvalidArgument(
                    "MsyncRequest.header.group_id is required".to_string(),
                )
                .into();
                let resp_header = super::header_from_canonical_error(&req.header, None, None, &err);
                return Ok(Response::new(MsyncResponse {
                    header: Some(resp_header),
                    readable_follower_ids: vec![],
                }));
            }
        } else {
            let err: CanonicalError =
                MetadataError::InvalidArgument("MsyncRequest.header is required".to_string())
                    .into();
            let resp_header = super::header_from_canonical_error(&None, None, None, &err);
            return Ok(Response::new(MsyncResponse {
                header: Some(resp_header),
                readable_follower_ids: vec![],
            }));
        };

        // Get min_state_id from request header.state_id (if provided)
        let required_watermark = req
            .header
            .as_ref()
            .and_then(|h| h.state_id.as_ref())
            .map(|sid| {
                let log_id = RaftLogId::from(sid.clone());
                types::GroupWatermark::new(group_id, log_id)
            });

        // Get timeout from request header.deadline_ms + gRPC deadline
        let deadline_ms = req
            .header
            .as_ref()
            .map(|h| h.deadline_ms)
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
                    + 5000 // Default 5s timeout
            });
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let timeout_ms = (deadline_ms - now_ms).max(0) as u64;

        // If min_state_id is specified, wait until we reach that state_id (or timeout)
        if let Some(required) = required_watermark {
            if let Some(ref raft_node) = self.raft_node {
                let start = std::time::Instant::now();
                let timeout = std::time::Duration::from_millis(timeout_ms);

                // Poll until last_applied >= required_state_id (same group)
                loop {
                    let current_log_id = raft_node.get_last_applied_state_id();

                    if let Some(applied) = current_log_id {
                        let applied_watermark = types::GroupWatermark::new(group_id, applied);

                        // Use explicit same-group comparison
                        if let Some(ord) = applied_watermark.cmp_same_group(&required) {
                            if ord != std::cmp::Ordering::Less {
                                break; // Reached required state
                            }
                        } else {
                            // Cross-group watermark comparison should not happen, but handle it
                            let err: CanonicalError = MetadataError::Internal(
                                "Cross-group watermark comparison detected".to_string(),
                            )
                            .into();
                            let resp_header = super::header_from_canonical_error(
                                &req.header,
                                Some(group_id.as_raw()),
                                None,
                                &err,
                            );
                            return Ok(Response::new(MsyncResponse {
                                header: Some(resp_header),
                                readable_follower_ids: vec![],
                            }));
                        }
                    } else {
                        // No log applied yet, wait
                    }

                    if start.elapsed() >= timeout {
                        let err: CanonicalError = MetadataError::ServiceUnavailable(format!(
                            "Msync timeout: requested watermark={:?}, current={:?}",
                            required,
                            current_log_id.map(|id| types::GroupWatermark::new(group_id, id))
                        ))
                        .into();
                        let resp_header = super::header_from_canonical_error(
                            &req.header,
                            Some(group_id.as_raw()),
                            None,
                            &err,
                        );
                        return Ok(Response::new(MsyncResponse {
                            header: Some(resp_header),
                            readable_follower_ids: vec![],
                        }));
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }

        // Get current state_id (last_applied) after waiting if needed
        let current_state_id = match self
            .raft_node
            .as_ref()
            .and_then(|rn| rn.get_last_applied_state_id())
        {
            Some(id) => id,
            None => {
                let err: CanonicalError =
                    MetadataError::ServiceUnavailable("Raft node not available".to_string()).into();
                let resp_header = super::header_from_canonical_error(
                    &req.header,
                    Some(group_id.as_raw()),
                    None,
                    &err,
                );
                return Ok(Response::new(MsyncResponse {
                    header: Some(resp_header),
                    readable_follower_ids: vec![],
                }));
            }
        };

        // Get readable follower IDs if requested
        let readable_follower_ids = if req.include_readable_followers {
            let (_, follower_ids) = self.get_raft_membership();
            // TODO: Filter followers that have applied at least required_state_id
            // For now, return all followers (actual implementation would query each follower's last_applied)
            follower_ids
        } else {
            vec![]
        };

        // Create response header with group_id and state_id
        let mut response_header =
            self.create_response_header_from_request(&req.header, Some(group_id.as_raw()));
        response_header = response_header.with_state_id(current_state_id);

        Ok(Response::new(MsyncResponse {
            header: Some((&response_header).into()),
            readable_follower_ids,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_route_table(
        &self,
        request: Request<GetRouteTableRequest>,
    ) -> Result<Response<GetRouteTableResponse>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Get current route_epoch
        let route_epoch = self.state_store.get_layout_version().await?.as_u64();

        // Get leader and follower IDs from Raft if available
        let (leader_id, follower_ids) = self.get_raft_membership();
        let leader_id = leader_id.unwrap_or(0);

        // Get shard-to-group mapping (for all shards)
        let shard_to_group = self.get_shard_to_group_mapping(None).await;

        // Build group-to-leader and group-to-followers mappings
        // Get unique group IDs from shard_to_group
        let group_ids: std::collections::HashSet<u64> = shard_to_group.values().copied().collect();

        // If no groups found, use default group 0
        let groups: Vec<u64> = if group_ids.is_empty() {
            vec![0]
        } else {
            group_ids.into_iter().collect()
        };

        let mut group_to_leader = std::collections::HashMap::new();
        let mut group_to_followers = std::collections::HashMap::new();

        // Map each group to leader and followers
        // For now, all groups share the same leader/followers (single Raft cluster)
        // In a true multi-Raft setup, each group would have its own leader/followers
        for group_id in groups {
            group_to_leader.insert(group_id, leader_id);
            if !follower_ids.is_empty() {
                let mut node_list = proto::metadata::NodeListProto::default();
                node_list.node_ids = follower_ids.clone();
                group_to_followers.insert(group_id, node_list);
            }
        }

        // For get_route_table, we can use group_id from header or default to 0
        let group_id = req
            .header
            .as_ref()
            .and_then(|h| {
                if h.group_id != 0 {
                    Some(h.group_id)
                } else {
                    None
                }
            })
            .or(Some(0));

        Ok(Response::new(GetRouteTableResponse {
            header: Some((&self.create_response_header_from_request(&req.header, group_id)).into()),
            route_epoch,
            shard_to_group,
            group_to_leader,
            group_to_followers,
        }))
    }
}
