// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataFsServiceProto implementation.
//!
//! FS write routing convergence - all FS write operations
//! must route to mount.namespace_owner_group_id.

use super::domain::{
    CloseWriteInput, CloseWriteIntent, FileRange, Freshness, FsyncBarrierInput, GetFileLayoutInput, OpenWriteInput,
    ReleaseSessionInput, RenewLeaseInput,
};
use super::fs_core::FsCore;
use super::guard::{AuthzContext, GuardChain, GuardSpec, LeadershipChecker};
use super::{
    extent_from_proto, extent_to_proto, extract_and_inject_context, fatal_fs_header, fencing_to_proto,
    header_from_canonical_error, header_from_core_failure, lease_id_from_proto, lease_id_to_proto, location_to_proto,
    need_refresh_header, ok_header_from_core_success, ok_header_from_request, presented_fencing_from_proto,
    request_context_from_proto, write_target_to_proto,
};
use super::{AuthzOp, AuthzProvider, AuthzTarget};
use crate::data_io::DataIoOp;
use crate::error::{to_canonical_fs, MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::raft::{AppDataResponse, AppRaftNode, Command, DedupKey, FsCommandResult, RocksDBStorage};
use crate::readiness::RootReadinessGate;
use crate::state::StateStore;
use common::error::canonical::RefreshReason;
use common::header::{RequestHeader, RpcErrorCode};
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProto;
use proto::metadata::*;
use proto::worker::CommitWriteRequestProto;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
use tracing::{debug, instrument, warn};
use types::fs::{FileAttrs, FsErrorCode, InodeId, InodeKind};
use types::ids::{LeaseId, MountId, ShardGroupId};
use types::layout::FileLayout;
use types::RaftLogId;

type CommitHook = Arc<dyn Fn(CommitWriteRequestProto) -> proto::worker::CommitWriteResponseProto + Send + Sync>;

/// Routed FS write context.
/// Contains mount information needed for FS write operations.
#[derive(Clone, Debug)]
pub struct RoutedFsWriteCtx {
    /// Mount ID.
    pub mount_id: MountId,
    /// Namespace owner group ID (target Raft group for this write).
    pub namespace_owner_group_id: ShardGroupId,
    /// Mount epoch (for validation).
    pub mount_epoch: u64,
    /// Latest state ID (if available).
    pub latest_state_id: Option<RaftLogId>,
}

/// FS write operation type (for AuthZ hook and logging).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsWriteOp {
    Create,
    Mkdir,
    Unlink,
    Rmdir,
    Rename,
    SetAttr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FsRpcAuthz {
    Lookup,
    GetAttr,
    SetAttr,
    Mkdir,
    Create,
    ReadDir,
    Unlink,
    Rmdir,
    Rename,
    Open,
    Release,
    Fsync,
    OpenWrite,
    CloseWrite,
    GetFileLayout,
    RenewInodeLease,
    Truncate,
    SetXattr,
    GetXattr,
    ListXattr,
    RemoveXattr,
}

/// MetadataFsServiceProto implementation.
pub struct MetadataFsServiceImpl {
    fs_core: Arc<FsCore>,
    mount_table: Arc<MountTable>,
    storage: Option<Arc<RocksDBStorage>>,
    raft_node: Option<Arc<AppRaftNode>>,
    metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
    write_session_manager: Arc<crate::write_session::WriteSessionManager>,
    /// Worker manager (for block allocation).
    worker_manager: Option<Arc<crate::worker::WorkerManager>>,
    inode_lease_manager: Arc<crate::inode_lease::InodeLeaseManager>,
    worker_commit_hook: Arc<Mutex<Option<CommitHook>>>,
    guard_chain: GuardChain,
}

impl MetadataFsServiceImpl {
    pub fn new(state_store: Arc<dyn StateStore>, mount_table: Arc<MountTable>) -> Self {
        let write_session_manager = Arc::new(crate::write_session::WriteSessionManager::default());
        let inode_lease_manager = Arc::new(crate::inode_lease::InodeLeaseManager::default());
        let worker_commit_hook: Arc<Mutex<Option<CommitHook>>> = Arc::new(Mutex::new(None));
        let fs_core = Arc::new(FsCore::new(
            Arc::clone(&state_store),
            Arc::clone(&mount_table),
            Arc::clone(&write_session_manager),
            Arc::clone(&inode_lease_manager),
            Arc::clone(&worker_commit_hook),
        ));
        Self {
            fs_core,
            guard_chain: GuardChain::new(Arc::clone(&mount_table)),
            mount_table,
            storage: None,
            raft_node: None,
            metrics: None,
            write_session_manager,
            worker_manager: None,
            inode_lease_manager,
            worker_commit_hook,
        }
    }

    /// Set storage for inode/dentry access (required for FS operations).
    pub fn with_storage(mut self, storage: Arc<RocksDBStorage>) -> Self {
        self.storage = Some(storage);
        Arc::get_mut(&mut self.fs_core)
            .expect("fs_core should be uniquely owned during builder configuration")
            .set_storage(self.storage.as_ref().unwrap().clone());
        self
    }

    /// Set Raft node for leader/follower information (optional).
    pub fn with_raft_node(mut self, raft_node: Arc<AppRaftNode>) -> Self {
        self.guard_chain.set_leadership_checker(Arc::clone(&raft_node));
        self.raft_node = Some(raft_node);
        Arc::get_mut(&mut self.fs_core)
            .expect("fs_core should be uniquely owned during builder configuration")
            .set_raft_node(self.raft_node.as_ref().unwrap().clone());
        self
    }

    /// Set a custom leadership checker (primarily for tests).
    pub fn with_leadership_checker<T>(mut self, checker: Arc<T>) -> Self
    where
        T: LeadershipChecker + 'static,
    {
        self.guard_chain.set_leadership_checker(checker);
        self
    }

    /// Set metrics for FS write routing tracking (optional).
    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::MetadataMetrics>) -> Self {
        self.metrics = Some(metrics);
        Arc::get_mut(&mut self.fs_core)
            .expect("fs_core should be uniquely owned during builder configuration")
            .set_metrics(self.metrics.as_ref().unwrap().clone());
        self
    }

    pub fn with_readiness_gate(mut self, readiness_gate: Arc<RootReadinessGate>) -> Self {
        self.guard_chain.set_readiness_gate(readiness_gate);
        self
    }

    pub fn with_authz_provider(mut self, provider: Arc<dyn AuthzProvider>) -> Self {
        self.guard_chain.set_authz_provider(provider);
        self
    }

    /// Test helper: override the worker commit hook for injected responses.
    pub fn set_worker_commit_hook_for_test(
        &self,
        hook: Arc<
            dyn Fn(proto::worker::CommitWriteRequestProto) -> proto::worker::CommitWriteResponseProto + Send + Sync,
        >,
    ) {
        let mut guard = self.worker_commit_hook.lock().unwrap();
        *guard = Some(hook);
    }

    /// Test helper: clear any overridden commit hook.
    pub fn clear_worker_commit_hook_for_test(&self) {
        let mut guard = self.worker_commit_hook.lock().unwrap();
        guard.take();
    }

    /// Test helper: expose the write session manager.
    pub fn write_session_manager_for_test(&self) -> Arc<crate::write_session::WriteSessionManager> {
        Arc::clone(&self.write_session_manager)
    }

    /// Test helper: expose the inode lease manager.
    pub fn inode_lease_manager_for_test(&self) -> Arc<crate::inode_lease::InodeLeaseManager> {
        Arc::clone(&self.inode_lease_manager)
    }

    fn dedup_key(&self, caller_ctx: &RequestHeader) -> MetadataResult<DedupKey> {
        let client_id = caller_ctx.client.client_id;
        if client_id.as_raw() == 0 {
            return Err(MetadataError::InvalidArgument(
                "client_id must be provided for dedup".to_string(),
            ));
        }
        Ok(DedupKey::new(client_id, caller_ctx.client.call_id))
    }

    fn header_from_error(
        req_header: &Option<proto::common::RequestHeaderProto>,
        err: MetadataError,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> proto::common::ResponseHeaderProto {
        let err = to_canonical_fs(err);
        header_from_canonical_error(req_header, group_id, mount_epoch, &err)
    }

    /// Set worker manager for block allocation (optional).
    pub fn with_worker_manager(mut self, worker_manager: Arc<crate::worker::WorkerManager>) -> Self {
        self.worker_manager = Some(worker_manager);
        Arc::get_mut(&mut self.fs_core)
            .expect("fs_core should be uniquely owned during builder configuration")
            .set_worker_manager(self.worker_manager.as_ref().unwrap().clone());
        self
    }

    pub fn fs_core(&self) -> Arc<FsCore> {
        Arc::clone(&self.fs_core)
    }

    // RPC -> authz mapping table for inode/handle service handlers:
    // READ_META: lookup/get_attr/read_dir/open/get_xattr/list_xattr
    // WRITE_META: mkdir/create/unlink/rmdir/rename/set_attr/set_xattr/remove_xattr
    // DATA_IO: release/open_write/close_write/fsync/hsync/hflush/truncate/get_file_layout/renew_inode_lease
    fn authz_for_rpc(rpc: FsRpcAuthz, target: AuthzTarget) -> AuthzContext {
        let op = match rpc {
            FsRpcAuthz::Lookup | FsRpcAuthz::GetAttr | FsRpcAuthz::ReadDir | FsRpcAuthz::Open => AuthzOp::Read,
            FsRpcAuthz::SetAttr | FsRpcAuthz::Mkdir | FsRpcAuthz::Create => AuthzOp::Write,
            FsRpcAuthz::Unlink | FsRpcAuthz::Rmdir => AuthzOp::Delete,
            FsRpcAuthz::Rename => AuthzOp::Rename,
            FsRpcAuthz::GetXattr | FsRpcAuthz::ListXattr | FsRpcAuthz::SetXattr | FsRpcAuthz::RemoveXattr => {
                AuthzOp::Xattr
            }
            FsRpcAuthz::Release
            | FsRpcAuthz::Fsync
            | FsRpcAuthz::OpenWrite
            | FsRpcAuthz::CloseWrite
            | FsRpcAuthz::GetFileLayout
            | FsRpcAuthz::RenewInodeLease
            | FsRpcAuthz::Truncate => AuthzOp::Write,
        };
        AuthzContext {
            op,
            targets: vec![target],
        }
    }

    /// Route FS write operation to mount.namespace_owner_group_id.
    ///
    /// This function:
    /// 1. Reads parent inode(s) to get mount_id
    /// 2. Queries mount table to get namespace_owner_group_id
    /// 3. Validates mount_epoch/state_id if provided
    /// 4. Returns RoutedFsWriteCtx for use in Raft command
    fn route_fs_write_ctx(
        &self,
        op: FsWriteOp,
        parent_inode_ids: &[InodeId],
        req_header: &Option<proto::common::RequestHeaderProto>,
    ) -> MetadataResult<RoutedFsWriteCtx> {
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;

        // Read first parent inode to get mount_id
        let parent_inode_id = parent_inode_ids
            .first()
            .ok_or_else(|| MetadataError::InvalidArgument("No parent inode provided".to_string()))?;
        let parent_inode = storage
            .get_inode(*parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;

        let mount_id = parent_inode.mount_id;
        for other_parent in parent_inode_ids.iter().skip(1) {
            let inode = storage
                .get_inode(*other_parent)?
                .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", other_parent)))?;
            if inode.mount_id != mount_id {
                return Err(MetadataError::CrossMountRename(
                    "cross-mount operation is not allowed".to_string(),
                ));
            }
        }

        // Get mount entry
        let mount_entry = self
            .mount_table
            .get_mount(mount_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount not found: {:?}", mount_id)))?;

        // Validate mount_epoch if provided
        // If mismatch, return error that will be converted to NEED_REFRESH/MOVED
        if let Some(header) = req_header {
            if let Some(client_mount_epoch) = header.mount_epoch {
                let current_mount_epoch = mount_entry.config_version;
                if client_mount_epoch != current_mount_epoch {
                    return Err(MetadataError::MountEpochMismatch {
                        expected: current_mount_epoch,
                        got: client_mount_epoch,
                        mount_id: Some(mount_id),
                    });
                }
            }
        }

        // Get latest state_id if available
        let latest_state_id = if let Some(ref raft_node) = self.raft_node {
            raft_node.get_last_applied_state_id()
        } else {
            None
        };

        // Log routing decision
        debug!(
            op = ?op,
            mount_id = %mount_id.as_raw(),
            owner_group_id = %mount_entry.namespace_owner_group_id.as_raw(),
            mount_epoch = mount_entry.config_version,
            "FS write routed to mount namespace owner group"
        );

        if let Some(ref metrics) = self.metrics {
            metrics
                .fs_write_routed_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(RoutedFsWriteCtx {
            mount_id,
            namespace_owner_group_id: mount_entry.namespace_owner_group_id,
            mount_epoch: mount_entry.config_version,
            latest_state_id,
        })
    }

    async fn guard_request(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        caller_ctx: &RequestHeader,
        mut spec: GuardSpec,
        mount_id: Option<MountId>,
        authz: Option<AuthzContext>,
    ) -> Option<proto::common::ResponseHeaderProto> {
        if authz.is_some() {
            spec = spec.with_authz();
        }
        match self
            .guard_chain
            .check_request(req_header, caller_ctx, spec, mount_id, authz)
            .await
        {
            Ok(()) => None,
            Err(failure) => Some(header_from_canonical_error(
                req_header,
                failure.group_id,
                failure.mount_epoch,
                &failure.err,
            )),
        }
    }

    // DEPRECATED: Use core_util::fatal_fs_header or need_refresh_header instead.
    // This method is kept temporarily for compatibility but will be removed.
    #[allow(dead_code)]
    fn create_fs_error_response(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        error_code: FsErrorCode,
        message: String,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> proto::common::ResponseHeaderProto {
        fatal_fs_header(req_header, error_code, message, group_id, mount_epoch)
    }

    /// Propose FS write command to Raft and update metrics.
    /// This is the unified entry point for all FS write operations that write to Raft.
    /// It ensures we can track and guard against write amplification.
    async fn propose_fs_write_command(&self, op: FsWriteOp, command: Command) -> MetadataResult<FsCommandResult> {
        let raft_node = self
            .raft_node
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Raft node not available".to_string()))?;

        let dedup_key = command.dedup_key().clone();
        let fingerprint = command.fingerprint();

        if let Some(storage) = &self.storage {
            if let Some(existing) = storage.get_applied_result(&dedup_key)? {
                if existing.fingerprint != fingerprint {
                    return Err(MetadataError::InvalidArgument(format!(
                        "call_id {} reused with different command payload",
                        dedup_key.call_id
                    )));
                }
                return Ok(match existing.result {
                    AppDataResponse::Fs(res) => res,
                    _ => FsCommandResult::ok(),
                });
            }
        }

        // Update metrics before proposing
        if let Some(metrics) = &self.metrics {
            metrics.fs_raft_appends_total.fetch_add(1, Ordering::Relaxed);
            match op {
                FsWriteOp::Create => {
                    metrics.fs_raft_appends_create.fetch_add(1, Ordering::Relaxed);
                }
                FsWriteOp::Mkdir => {
                    metrics.fs_raft_appends_mkdir.fetch_add(1, Ordering::Relaxed);
                }
                FsWriteOp::Unlink => {
                    metrics.fs_raft_appends_unlink.fetch_add(1, Ordering::Relaxed);
                }
                FsWriteOp::Rmdir => {
                    metrics.fs_raft_appends_rmdir.fetch_add(1, Ordering::Relaxed);
                }
                FsWriteOp::Rename => {
                    metrics.fs_raft_appends_rename.fetch_add(1, Ordering::Relaxed);
                }
                FsWriteOp::SetAttr => {
                    metrics.fs_raft_appends_setattr.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Propose to Raft
        let response = raft_node
            .propose(command)
            .await
            .map_err(|e| MetadataError::Internal(format!("Failed to propose command: {}", e)))?;

        let fs_result = match response {
            AppDataResponse::Fs(res) => res,
            _ => FsCommandResult::ok(),
        };

        Ok(fs_result)
    }

    // DEPRECATED: Use core_util::ok_header_from_request instead.
    #[allow(dead_code)]
    fn create_response_header_from_request(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        group_id: Option<u64>,
    ) -> proto::common::ResponseHeaderProto {
        ok_header_from_request(req_header, group_id, None)
    }

    /// Convert proto FileAttrsProto to types FileAttrs.
    fn proto_to_file_attrs(attrs: Option<proto::fs::FileAttrsProto>) -> MetadataResult<FileAttrs> {
        let attrs = attrs.ok_or_else(|| MetadataError::InvalidArgument("Missing FileAttrs".to_string()))?;
        Ok(FileAttrs {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        })
    }

    /// Convert types FileAttrs to proto FileAttrsProto.
    fn file_attrs_to_proto(attrs: &FileAttrs) -> proto::fs::FileAttrsProto {
        proto::fs::FileAttrsProto {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        }
    }

    /// Convert proto FileLayoutProto to types FileLayout.
    fn proto_to_file_layout(layout: Option<proto::common::FileLayoutProto>) -> MetadataResult<FileLayout> {
        let layout = layout.ok_or_else(|| MetadataError::InvalidArgument("Missing FileLayout".to_string()))?;
        Ok(FileLayout::new(
            layout.block_size,
            layout.chunk_size,
            layout.replication as u8,
        ))
    }
}

#[tonic::async_trait]
impl MetadataFsServiceProto for MetadataFsServiceImpl {
    #[instrument(skip(self), fields(call_id, client_id))]
    async fn lookup(&self, request: Request<LookupRequestProto>) -> Result<Response<LookupResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let parent_inode_id = match req.parent_inode_id {
            Some(parent_inode_id) => InodeId::new(parent_inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing parent_inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(LookupResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read().with_authz(),
                None,
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::Lookup,
                    AuthzTarget::for_inode(parent_inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(LookupResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(LookupResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        // Get dentry
        let child_inode_id = match storage.get_dentry(parent_inode_id, &req.name) {
            Ok(Some(child_inode_id)) => child_inode_id,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!(
                        "Entry not found: parent={}, name={}",
                        parent_inode_id, req.name
                    )),
                    None,
                    None,
                );
                return Ok(Response::new(LookupResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(LookupResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        // Get child inode
        let child_inode = match storage.get_inode(child_inode_id) {
            Ok(Some(child_inode)) => child_inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Inode not found: {}", child_inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(LookupResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(LookupResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        // Convert to proto
        let (file_data, dir_data, symlink_data) = match &child_inode.data {
            types::fs::InodeData::File { extents, lease_epoch } => (
                Some(proto::fs::InodeFileProto {
                    extents: extents
                        .iter()
                        .map(|e| proto::fs::ExtentProto {
                            file_offset: e.file_offset,
                            block_id: Some(proto::common::BlockIdProto {
                                data_handle_id: e.block_id.data_handle_id.as_raw(),
                                block_index: e.block_id.index.as_raw(),
                            }),
                            block_offset: e.block_offset,
                            len: e.len,
                            file_version: e.file_version,
                            block_stamp: e.block_stamp,
                        })
                        .collect(),
                    lease_epoch: *lease_epoch,
                    lease_id: None, // Optional, not stored in inode currently
                }),
                None,
                None,
            ),
            types::fs::InodeData::Dir => (None, Some(proto::fs::InodeDirectoryProto {}), None),
            types::fs::InodeData::Symlink { target } => (
                None,
                None,
                target.as_ref().map(|t| proto::fs::InodeSymlinkProto {
                    target: Some(t.clone()),
                }),
            ),
        };

        // Build oneof data field
        use proto::fs::inode_proto::Data;
        let data = if let Some(file) = file_data {
            Some(Data::File(file))
        } else if let Some(dir) = dir_data {
            Some(Data::Dir(dir))
        } else if let Some(symlink) = symlink_data {
            Some(Data::Symlink(symlink))
        } else {
            None
        };

        let inode_proto = proto::fs::InodeProto {
            inode_id: Some(proto::fs::InodeIdProto {
                value: child_inode.inode_id.as_raw(),
            }),
            kind: match child_inode.kind {
                InodeKind::File => proto::fs::InodeKindProto::InodeKindFile as i32,
                InodeKind::Dir => proto::fs::InodeKindProto::InodeKindDir as i32,
                InodeKind::Symlink => proto::fs::InodeKindProto::InodeKindSymlink as i32,
            },
            attrs: Some(Self::file_attrs_to_proto(&child_inode.attrs)),
            data,
            mount_id: Some(proto::common::MountIdProto {
                value: child_inode.mount_id.as_raw(),
            }),
            xattrs: child_inode
                .xattrs
                .iter()
                .map(|(k, v)| proto::fs::XattrProto {
                    name: k.clone(),
                    value: v.clone(),
                })
                .collect(),
        };

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(LookupResponseProto {
            header: Some(resp_header),
            inode: Some(inode_proto),
            attrs: Some(Self::file_attrs_to_proto(&child_inode.attrs)),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_attr(&self, request: Request<GetAttrRequestProto>) -> Result<Response<GetAttrResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let inode_id = match req.inode_id {
            Some(inode_id) => InodeId::new(inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(GetAttrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read().with_authz(),
                None,
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::GetAttr,
                    AuthzTarget::for_inode(inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(GetAttrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(GetAttrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(GetAttrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(GetAttrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(GetAttrResponseProto {
            header: Some(resp_header),
            attrs: Some(Self::file_attrs_to_proto(&inode.attrs)),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn set_attr(&self, request: Request<SetAttrRequestProto>) -> Result<Response<SetAttrResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let inode_id = match req.inode_id {
            Some(inode_id) => InodeId::new(inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
        };

        // Route FS write operation
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
        };
        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
        };

        // Route and validate mount_epoch
        let ctx = match self.route_fs_write_ctx(FsWriteOp::SetAttr, &[inode_id], &req.header) {
            Ok(ctx) => ctx,
            Err(MetadataError::MountEpochMismatch { expected, got, .. }) => {
                // Mount epoch mismatch - return NEED_REFRESH
                // Update metrics
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .fs_write_mount_epoch_mismatch_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    inode_id = %inode_id,
                    expected = expected,
                    got = got,
                    "FS write rejected: mount epoch mismatch (NEED_REFRESH)"
                );
                let mount_entry = self.mount_table.get_mount(inode.mount_id).ok().flatten();
                let mount_epoch = mount_entry.map(|e| e.config_version).or(Some(expected));
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: client={}, server={}", got, expected),
                    None,
                    mount_epoch,
                );
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
            Err(e) => {
                let err = to_canonical_fs(e);
                let resp_header = super::header_from_canonical_error(&req.header, None, None, &err);
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(inode.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::SetAttr,
                    AuthzTarget::for_inode(inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(SetAttrResponseProto {
                header: Some(resp_header),
                attrs: None,
            }));
        }

        // Convert attrs
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
        };

        let dedup = match self.dedup_key(&caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
        };
        // Send Raft command
        let command = Command::SetAttr {
            dedup,
            inode_id,
            mask: req.mask,
            attrs,
        };

        // Propose via unified helper (tracks metrics)
        if let Err(err) = self.propose_fs_write_command(FsWriteOp::SetAttr, command).await {
            let resp_header = Self::header_from_error(
                &req.header,
                err,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
            return Ok(Response::new(SetAttrResponseProto {
                header: Some(resp_header),
                attrs: None,
            }));
        }

        // Read updated inode
        let updated_inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Inode disappeared after update".to_string()),
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
        };

        let mut resp_header = ok_header_from_request(
            &req.header,
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
        );
        if let Some(state_id) = ctx.latest_state_id {
            resp_header.state_id = Some(proto::common::RaftLogIdProto {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            });
        }

        Ok(Response::new(SetAttrResponseProto {
            header: Some(resp_header),
            attrs: Some(Self::file_attrs_to_proto(&updated_inode.attrs)),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn mkdir(&self, request: Request<MkdirRequestProto>) -> Result<Response<MkdirResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let parent_inode_id = match req.parent_inode_id {
            Some(parent_inode_id) => InodeId::new(parent_inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing parent_inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(MkdirResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        let ctx = match self.route_fs_write_ctx(FsWriteOp::Mkdir, &[parent_inode_id], &req.header) {
            Ok(ctx) => ctx,
            Err(MetadataError::MountEpochMismatch { expected, got, .. }) => {
                // Mount epoch mismatch - return NEED_REFRESH
                // Update metrics
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .fs_write_mount_epoch_mismatch_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    parent_inode_id = %parent_inode_id,
                    expected = expected,
                    got = got,
                    "FS write rejected: mount epoch mismatch (NEED_REFRESH)"
                );
                let mount_epoch = if let Some(storage) = self.storage.as_ref() {
                    let parent_inode = storage.get_inode(parent_inode_id).ok().flatten();
                    let parent_mount_id = parent_inode.map(|i| i.mount_id);
                    let mount_entry = parent_mount_id.and_then(|id| self.mount_table.get_mount(id).ok().flatten());
                    mount_entry.map(|e| e.config_version).or(Some(expected))
                } else {
                    Some(expected)
                };
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: client={}, server={} ", got, expected),
                    None,
                    mount_epoch,
                );
                return Ok(Response::new(MkdirResponseProto {
                    header: Some(resp_header),
                    inode: None,
                    attrs: None,
                }));
            }
            Err(e) => {
                let err = to_canonical_fs(e);
                let resp_header = super::header_from_canonical_error(&req.header, None, None, &err);
                return Ok(Response::new(MkdirResponseProto {
                    header: Some(resp_header),
                    inode: None,
                    attrs: None,
                }));
            }
        };

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(MkdirResponseProto {
                    header: Some(resp_header),
                    inode: None,
                    attrs: None,
                }));
            }
        };
        let parent_inode = match storage.get_inode(parent_inode_id) {
            Ok(Some(parent_inode)) => parent_inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)),
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(MkdirResponseProto {
                    header: Some(resp_header),
                    inode: None,
                    attrs: None,
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(MkdirResponseProto {
                    header: Some(resp_header),
                    inode: None,
                    attrs: None,
                }));
            }
        };
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(parent_inode.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::Mkdir,
                    AuthzTarget::for_inode(parent_inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(MkdirResponseProto {
                header: Some(resp_header),
                inode: None,
                attrs: None,
            }));
        }

        // Convert attrs
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(MkdirResponseProto {
                    header: Some(resp_header),
                    inode: None,
                    attrs: None,
                }));
            }
        };

        let dedup = match self.dedup_key(&caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(MkdirResponseProto {
                    header: Some(resp_header),
                    inode: None,
                    attrs: None,
                }));
            }
        };
        // Send Raft command
        let command = Command::Mkdir {
            dedup,
            parent_inode_id,
            name: req.name,
            attrs,
        };

        if let Err(err) = self.propose_fs_write_command(FsWriteOp::Mkdir, command).await {
            let resp_header = Self::header_from_error(
                &req.header,
                err,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
            return Ok(Response::new(MkdirResponseProto {
                header: Some(resp_header),
                inode: None,
                attrs: None,
            }));
        }

        // TODO: Parse result to get created inode_id
        // For now, return placeholder
        let mut resp_header = ok_header_from_request(
            &req.header,
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
        );
        if let Some(state_id) = ctx.latest_state_id {
            resp_header.state_id = Some(proto::common::RaftLogIdProto {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            });
        }

        Ok(Response::new(MkdirResponseProto {
            header: Some(resp_header),
            inode: None, // TODO: Return created inode
            attrs: None, // TODO: Return created attrs
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn create(&self, request: Request<CreateRequestProto>) -> Result<Response<CreateResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let parent_inode_id = match req.parent_inode_id {
            Some(parent_inode_id) => InodeId::new(parent_inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing parent_inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(CreateResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        let ctx = match self.route_fs_write_ctx(FsWriteOp::Create, &[parent_inode_id], &req.header) {
            Ok(ctx) => ctx,
            Err(MetadataError::MountEpochMismatch { expected, got, .. }) => {
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .fs_write_mount_epoch_mismatch_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    parent_inode_id = %parent_inode_id,
                    expected = expected,
                    got = got,
                    "FS write rejected: mount epoch mismatch (NEED_REFRESH)"
                );
                let mount_epoch = Some(expected);
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: client={}, server={} ", got, expected),
                    None,
                    mount_epoch,
                );
                return Ok(Response::new(CreateResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(CreateResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(ctx.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::Create,
                    AuthzTarget::for_inode(parent_inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(CreateResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        // Convert attrs and layout
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(CreateResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        let layout = match Self::proto_to_file_layout(req.layout) {
            Ok(layout) => layout,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(CreateResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        let dedup = match self.dedup_key(&caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(CreateResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        // Send Raft command
        let command = Command::Create {
            dedup,
            parent_inode_id,
            name: req.name,
            attrs,
            layout,
        };

        let result = match self.propose_fs_write_command(FsWriteOp::Create, command).await {
            Ok(result) => result,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(CreateResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        match result {
            FsCommandResult::Ok(ok) => {
                let mut resp_header = ok_header_from_request(
                    &req.header,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                if let Some(state_id) = ctx.latest_state_id {
                    resp_header.state_id = Some(proto::common::RaftLogIdProto {
                        term: state_id.term,
                        leader_node_id: state_id.leader_node_id,
                        index: state_id.index,
                    });
                }

                Ok(Response::new(CreateResponseProto {
                    header: Some(resp_header),
                    inode: None, // TODO: Return created inode
                    attrs: None, // TODO: Return created attrs
                    data_handle_id: ok
                        .data_handle_id
                        .map(types::ids::DataHandleId::as_raw)
                        .unwrap_or_default(),
                }))
            }
            FsCommandResult::Err(err) => {
                let resp_header = fatal_fs_header(
                    &req.header,
                    err.errno,
                    err.message,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                Ok(Response::new(CreateResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }))
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn read_dir(&self, request: Request<ReadDirRequestProto>) -> Result<Response<ReadDirResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let parent_inode_id = match req.parent_inode_id {
            Some(parent_inode_id) => InodeId::new(parent_inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing parent_inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(ReadDirResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read().with_authz(),
                None,
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::ReadDir,
                    AuthzTarget::for_inode(parent_inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(ReadDirResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(ReadDirResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        // Verify parent is a directory
        let parent_inode = match storage.get_inode(parent_inode_id) {
            Ok(Some(parent_inode)) => parent_inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(ReadDirResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(ReadDirResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        if !parent_inode.kind.is_dir() {
            let err = to_canonical_fs(MetadataError::InvalidArgument(format!(
                "Parent is not a directory: {}",
                parent_inode_id
            )));
            let resp_header = super::header_from_canonical_error(&req.header, None, None, &err);
            return Ok(Response::new(ReadDirResponseProto {
                header: Some(resp_header),
                entries: vec![],
                next_cursor_key: vec![],
                eof: true,
            }));
        }

        // Parse cursor_key (opaque bytes)
        let cursor_key = if req.cursor_key.is_empty() {
            None
        } else {
            Some(req.cursor_key.as_slice())
        };

        // Parse max_entries (0 means no limit)
        let max_entries = if req.max_entries == 0 {
            None
        } else {
            Some(req.max_entries as usize)
        };

        // List dentries with pagination
        let (entries, next_cursor_key, eof) =
            match storage.list_dentries_with_cursor(parent_inode_id, cursor_key, max_entries) {
                Ok(result) => result,
                Err(err) => {
                    let resp_header = Self::header_from_error(&req.header, err, None, None);
                    return Ok(Response::new(ReadDirResponseProto {
                        header: Some(resp_header),
                        entries: vec![],
                        next_cursor_key: vec![],
                        eof: true,
                    }));
                }
            };

        // Convert to DirEntryProto
        let mut dir_entries = Vec::new();
        for (name, child_inode_id) in entries {
            // Optionally load child inode for attrs (for optimization)
            let child_inode = storage.get_inode(child_inode_id).ok().flatten();
            let attrs = child_inode.as_ref().map(|i| Self::file_attrs_to_proto(&i.attrs));
            let kind = child_inode
                .map(|i| match i.kind {
                    InodeKind::File => proto::fs::InodeKindProto::InodeKindFile as i32,
                    InodeKind::Dir => proto::fs::InodeKindProto::InodeKindDir as i32,
                    InodeKind::Symlink => proto::fs::InodeKindProto::InodeKindSymlink as i32,
                })
                .unwrap_or(proto::fs::InodeKindProto::InodeKindUnspecified as i32);

            dir_entries.push(proto::fs::DirEntryProto {
                name,
                inode_id: Some(proto::fs::InodeIdProto {
                    value: child_inode_id.as_raw(),
                }),
                kind,
                attrs,
            });
        }

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(ReadDirResponseProto {
            header: Some(resp_header),
            entries: dir_entries,
            next_cursor_key: next_cursor_key.unwrap_or_default(),
            eof,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn unlink(&self, request: Request<UnlinkRequestProto>) -> Result<Response<UnlinkResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let parent_inode_id = match req.parent_inode_id {
            Some(parent_inode_id) => InodeId::new(parent_inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing parent_inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(UnlinkResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        let ctx = match self.route_fs_write_ctx(FsWriteOp::Unlink, &[parent_inode_id], &req.header) {
            Ok(ctx) => ctx,
            Err(MetadataError::MountEpochMismatch { expected, got, .. }) => {
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .fs_write_mount_epoch_mismatch_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    parent_inode_id = %parent_inode_id,
                    expected = expected,
                    got = got,
                    "FS write rejected: mount epoch mismatch (NEED_REFRESH)"
                );
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: client={}, server={} ", got, expected),
                    None,
                    Some(expected),
                );
                return Ok(Response::new(UnlinkResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(UnlinkResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(ctx.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::Unlink,
                    AuthzTarget::for_inode(parent_inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(UnlinkResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let dedup = match self.dedup_key(&caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(UnlinkResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        let command = Command::Unlink {
            dedup,
            parent_inode_id,
            name: req.name,
        };

        let result = match self.propose_fs_write_command(FsWriteOp::Unlink, command).await {
            Ok(result) => result,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(UnlinkResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        match result {
            FsCommandResult::Ok(_) => {
                let mut resp_header = ok_header_from_request(
                    &req.header,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                if let Some(state_id) = ctx.latest_state_id {
                    resp_header.state_id = Some(proto::common::RaftLogIdProto {
                        term: state_id.term,
                        leader_node_id: state_id.leader_node_id,
                        index: state_id.index,
                    });
                }

                Ok(Response::new(UnlinkResponseProto {
                    header: Some(resp_header),
                }))
            }
            FsCommandResult::Err(err) => {
                let resp_header = fatal_fs_header(
                    &req.header,
                    err.errno,
                    err.message,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                Ok(Response::new(UnlinkResponseProto {
                    header: Some(resp_header),
                }))
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rmdir(&self, request: Request<RmdirRequestProto>) -> Result<Response<RmdirResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let parent_inode_id = match req.parent_inode_id {
            Some(parent_inode_id) => InodeId::new(parent_inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing parent_inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(RmdirResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        let ctx = match self.route_fs_write_ctx(FsWriteOp::Rmdir, &[parent_inode_id], &req.header) {
            Ok(ctx) => ctx,
            Err(MetadataError::MountEpochMismatch { expected, got, .. }) => {
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .fs_write_mount_epoch_mismatch_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    parent_inode_id = %parent_inode_id,
                    expected = expected,
                    got = got,
                    "FS write rejected: mount epoch mismatch (NEED_REFRESH)"
                );
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: client={}, server={} ", got, expected),
                    None,
                    Some(expected),
                );
                return Ok(Response::new(RmdirResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(RmdirResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(ctx.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::Rmdir,
                    AuthzTarget::for_inode(parent_inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(RmdirResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let dedup = match self.dedup_key(&caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(RmdirResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        let command = Command::Rmdir {
            dedup,
            parent_inode_id,
            name: req.name,
        };

        let result = match self.propose_fs_write_command(FsWriteOp::Rmdir, command).await {
            Ok(result) => result,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(RmdirResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        match result {
            FsCommandResult::Ok(_) => {
                let mut resp_header = ok_header_from_request(
                    &req.header,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                if let Some(state_id) = ctx.latest_state_id {
                    resp_header.state_id = Some(proto::common::RaftLogIdProto {
                        term: state_id.term,
                        leader_node_id: state_id.leader_node_id,
                        index: state_id.index,
                    });
                }

                Ok(Response::new(RmdirResponseProto {
                    header: Some(resp_header),
                }))
            }
            FsCommandResult::Err(err) => {
                let resp_header = fatal_fs_header(
                    &req.header,
                    err.errno,
                    err.message,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                Ok(Response::new(RmdirResponseProto {
                    header: Some(resp_header),
                }))
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rename(&self, request: Request<FsRenameRequestProto>) -> Result<Response<FsRenameResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        // Reject unsupported flags (only RENAME_NOREPLACE supported).
        let supported_mask: u32 = 0x1;
        if req.flags & !supported_mask != 0 {
            let resp_header = Self::header_from_error(
                &req.header,
                MetadataError::NotSupported(format!("Unsupported rename flags: {}", req.flags)),
                None,
                None,
            );
            return Ok(Response::new(FsRenameResponseProto {
                header: Some(resp_header),
            }));
        }

        let src_parent_inode_id = match req.src_parent_inode_id {
            Some(src_parent_inode_id) => InodeId::new(src_parent_inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing src_parent_inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
        };
        let dst_parent_inode_id = match req.dst_parent_inode_id {
            Some(dst_parent_inode_id) => InodeId::new(dst_parent_inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing dst_parent_inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
        };

        // Check cross-mount rename
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
        };
        let src_parent_inode = match storage.get_inode(src_parent_inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Source parent inode not found: {}", src_parent_inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
        };
        let dst_parent_inode = match storage.get_inode(dst_parent_inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Destination parent inode not found: {}", dst_parent_inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
        };

        if src_parent_inode.mount_id != dst_parent_inode.mount_id {
            // Cross-mount rename - return EXDEV immediately
            // Update metrics
            if let Some(ref metrics) = self.metrics {
                metrics
                    .fs_write_cross_mount_rename_exdev_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            debug!(
                src_mount_id = %src_parent_inode.mount_id.as_raw(),
                dst_mount_id = %dst_parent_inode.mount_id.as_raw(),
                "Cross-mount rename rejected with EXDEV"
            );
            let mount_entry = match self.mount_table.get_mount(src_parent_inode.mount_id) {
                Ok(Some(entry)) => entry,
                Ok(None) => {
                    let resp_header = Self::header_from_error(
                        &req.header,
                        MetadataError::Internal("Mount disappeared".to_string()),
                        None,
                        None,
                    );
                    return Ok(Response::new(FsRenameResponseProto {
                        header: Some(resp_header),
                    }));
                }
                Err(err) => {
                    let resp_header = Self::header_from_error(&req.header, err, None, None);
                    return Ok(Response::new(FsRenameResponseProto {
                        header: Some(resp_header),
                    }));
                }
            };
            let resp_header = Self::header_from_error(
                &req.header,
                MetadataError::CrossMountRename(format!(
                    "Cross-mount rename not allowed: src_mount={:?}, dst_mount={:?}",
                    src_parent_inode.mount_id, dst_parent_inode.mount_id
                )),
                None,
                Some(mount_entry.config_version),
            );
            return Ok(Response::new(FsRenameResponseProto {
                header: Some(resp_header),
            }));
        }

        // Route FS write operation (both parents in same mount)
        let ctx = match self.route_fs_write_ctx(
            FsWriteOp::Rename,
            &[src_parent_inode_id, dst_parent_inode_id],
            &req.header,
        ) {
            Ok(ctx) => ctx,
            Err(MetadataError::MountEpochMismatch { expected, got, .. }) => {
                // Mount epoch mismatch - return NEED_REFRESH
                // Update metrics
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .fs_write_mount_epoch_mismatch_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    src_parent_inode_id = %src_parent_inode_id,
                    dst_parent_inode_id = %dst_parent_inode_id,
                    expected = expected,
                    got = got,
                    "FS write rejected: mount epoch mismatch (NEED_REFRESH)"
                );
                let mount_entry = self.mount_table.get_mount(src_parent_inode.mount_id).ok().flatten();
                let mount_epoch = mount_entry.map(|e| e.config_version).or(Some(expected));
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: client={}, server={} ", got, expected),
                    None,
                    mount_epoch,
                );
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
            Err(e) => {
                let err = to_canonical_fs(e);
                let resp_header = super::header_from_canonical_error(&req.header, None, None, &err);
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(src_parent_inode.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::Rename,
                    AuthzTarget::for_inode(src_parent_inode_id).with_parent(dst_parent_inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(FsRenameResponseProto {
                header: Some(resp_header),
            }));
        }

        if req.flags & 0x1 != 0 {
            if let Some(ref raft_node) = self.raft_node {
                if raft_node.is_leader() {
                    let mut can_precheck = true;
                    if let Some(required_state_id) = req
                        .header
                        .as_ref()
                        .and_then(|h| h.state_id.as_ref())
                        .map(|sid| types::RaftLogId::new(sid.term, sid.leader_node_id, sid.index))
                    {
                        if let Some(last_applied) = raft_node.get_last_applied_state_id() {
                            if last_applied < required_state_id {
                                let resp_header = need_refresh_header(
                                    &req.header,
                                    RpcErrorCode::StaleState,
                                    RefreshReason::StaleState,
                                    format!(
                                        "Stale state: last_applied={:?} < required={:?}",
                                        last_applied, required_state_id
                                    ),
                                    Some(ctx.namespace_owner_group_id.as_raw()),
                                    Some(ctx.mount_epoch),
                                );
                                return Ok(Response::new(FsRenameResponseProto {
                                    header: Some(resp_header),
                                }));
                            }
                        } else {
                            can_precheck = false;
                        }
                    }

                    if can_precheck {
                        match storage.get_dentry(dst_parent_inode_id, &req.dst_name) {
                            Ok(Some(_)) => {
                                let resp_header = Self::header_from_error(
                                    &req.header,
                                    MetadataError::AlreadyExists(format!(
                                        "Destination exists and RENAME_NOREPLACE set: {}",
                                        req.dst_name
                                    )),
                                    Some(ctx.namespace_owner_group_id.as_raw()),
                                    Some(ctx.mount_epoch),
                                );
                                return Ok(Response::new(FsRenameResponseProto {
                                    header: Some(resp_header),
                                }));
                            }
                            Ok(None) => {}
                            Err(err) => {
                                let resp_header = Self::header_from_error(
                                    &req.header,
                                    err,
                                    Some(ctx.namespace_owner_group_id.as_raw()),
                                    Some(ctx.mount_epoch),
                                );
                                return Ok(Response::new(FsRenameResponseProto {
                                    header: Some(resp_header),
                                }));
                            }
                        }
                    }
                }
            }
        }

        // Send Raft command
        if self.raft_node.as_ref().is_none() {
            let resp_header = Self::header_from_error(
                &req.header,
                MetadataError::Internal("Raft node not available".to_string()),
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
            return Ok(Response::new(FsRenameResponseProto {
                header: Some(resp_header),
            }));
        }

        let dedup = match self.dedup_key(&caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
        };
        let command = Command::Rename {
            dedup,
            src_parent_inode_id,
            src_name: req.src_name,
            dst_parent_inode_id,
            dst_name: req.dst_name,
            flags: req.flags,
        };

        if let Err(err) = self.propose_fs_write_command(FsWriteOp::Rename, command).await {
            let resp_header = Self::header_from_error(
                &req.header,
                err,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
            return Ok(Response::new(FsRenameResponseProto {
                header: Some(resp_header),
            }));
        }

        let mut resp_header = ok_header_from_request(
            &req.header,
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
        );
        if let Some(state_id) = ctx.latest_state_id {
            resp_header.state_id = Some(proto::common::RaftLogIdProto {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            });
        }

        Ok(Response::new(FsRenameResponseProto {
            header: Some(resp_header),
        }))
    }

    // TODO(fs_service): implement extended operations (open/release/fsync)
    async fn open(&self, request: Request<OpenRequestProto>) -> Result<Response<OpenResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        let authz = req.inode_id.as_ref().map(|inode_id| {
            Self::authz_for_rpc(FsRpcAuthz::Open, AuthzTarget::for_inode(InodeId::new(inode_id.value)))
        });
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read().with_authz(),
                None,
                authz,
            )
            .await
        {
            return Ok(Response::new(OpenResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let resp_header = ok_header_from_request(&req.header, None, None);
        Ok(Response::new(OpenResponseProto {
            header: Some(resp_header),
            file_handle: 0,
        }))
    }

    async fn release(&self, request: Request<ReleaseRequestProto>) -> Result<Response<ReleaseResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Some(session) = self.fs_core.write_session_for_handle(req.file_handle) {
            if let Some(resp_header) = self
                .guard_request(
                    &req.header,
                    &caller_ctx,
                    GuardSpec::data_io(DataIoOp::CloseWrite).with_leader().with_authz(),
                    Some(session.mount_id),
                    Some(Self::authz_for_rpc(
                        FsRpcAuthz::Release,
                        AuthzTarget::for_session(req.file_handle, Some(session.inode_id)),
                    )),
                )
                .await
            {
                return Ok(Response::new(ReleaseResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        } else if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read().with_authz(),
                None,
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::Release,
                    AuthzTarget::for_file_handle(req.file_handle),
                )),
            )
            .await
        {
            return Ok(Response::new(ReleaseResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let req_ctx = request_context_from_proto(&req.header);
        let resp = match self
            .fs_core
            .release_session(ReleaseSessionInput {
                ctx: req_ctx.clone(),
                file_handle: req.file_handle,
            })
            .await
        {
            Ok(success) => ReleaseResponseProto {
                header: Some(ok_header_from_core_success(&req_ctx, &success)),
            },
            Err(failure) => ReleaseResponseProto {
                header: Some(header_from_core_failure(&req_ctx, &failure)),
            },
        };
        Ok(Response::new(resp))
    }

    async fn fsync(&self, request: Request<FsyncRequestProto>) -> Result<Response<FsyncResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let inode_id = match req.inode_id {
            Some(inode_id) => InodeId::new(inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(FsyncResponseProto {
                    header: Some(resp_header),
                }));
            }
        };

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(FsyncResponseProto {
                    header: Some(resp_header),
                }));
            }
        };

        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(FsyncResponseProto {
                    header: Some(resp_header),
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(FsyncResponseProto {
                    header: Some(resp_header),
                }));
            }
        };

        if !inode.kind.is_file() {
            let resp_header = Self::header_from_error(
                &req.header,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
                None,
            );
            return Ok(Response::new(FsyncResponseProto {
                header: Some(resp_header),
            }));
        }

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::data_io(DataIoOp::Fsync).with_leader().with_authz(),
                Some(inode.mount_id),
                Some(Self::authz_for_rpc(FsRpcAuthz::Fsync, AuthzTarget::for_inode(inode_id))),
            )
            .await
        {
            return Ok(Response::new(FsyncResponseProto {
                header: Some(resp_header),
            }));
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req
                .route_epoch
                .or_else(|| req.header.as_ref().and_then(|h| h.route_epoch)),
            worker_epoch: req.worker_epoch,
        };
        let resp = match self
            .fs_core
            .fsync_barrier(FsyncBarrierInput {
                ctx: req_ctx.clone(),
                inode_id,
                file_handle: req.file_handle,
                lease_id: lease_id_from_proto(req.lease_id),
                lease_epoch: req.lease_epoch,
                fencing_token: presented_fencing_from_proto(req.fencing_token),
                target_size: req.target_size,
                flags: req.flags as i32,
                freshness,
            })
            .await
        {
            Ok(success) => FsyncResponseProto {
                header: Some(ok_header_from_core_success(&req_ctx, &success)),
            },
            Err(failure) => FsyncResponseProto {
                header: Some(header_from_core_failure(&req_ctx, &failure)),
            },
        };
        Ok(Response::new(resp))
    }

    async fn hsync(&self, request: Request<HsyncRequestProto>) -> Result<Response<HsyncResponseProto>, Status> {
        // Reuse fsync semantics
        let inner = request.into_inner();
        let fsync_req = match inner.fsync {
            Some(req) => req,
            None => {
                let resp_header = Self::header_from_error(
                    &None,
                    MetadataError::InvalidArgument("missing fsync body".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(HsyncResponseProto {
                    header: Some(resp_header),
                }));
            }
        };
        let resp = self.fsync(Request::new(fsync_req)).await?;
        Ok(Response::new(HsyncResponseProto {
            header: resp.into_inner().header,
        }))
    }

    async fn hflush(&self, request: Request<HflushRequestProto>) -> Result<Response<HflushResponseProto>, Status> {
        let inner = request.into_inner();
        let fsync_req = match inner.fsync {
            Some(req) => req,
            None => {
                let resp_header = Self::header_from_error(
                    &None,
                    MetadataError::InvalidArgument("missing fsync body".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(HflushResponseProto {
                    header: Some(resp_header),
                }));
            }
        };
        let resp = self.fsync(Request::new(fsync_req)).await?;
        Ok(Response::new(HflushResponseProto {
            header: resp.into_inner().header,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn open_write(
        &self,
        request: Request<OpenWriteRequestProto>,
    ) -> Result<Response<OpenWriteResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let inode_id = match req.inode_id {
            Some(inode_id) => InodeId::new(inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(OpenWriteResponseProto {
                    header: Some(resp_header),
                    file_handle: 0,
                    lease_id: None,
                    fencing_token: None,
                    write_targets: vec![],
                    base_size: 0,
                    open_epoch: 0,
                    lease_epoch: 0,
                    expires_at_ms: 0,
                }));
            }
        };

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(OpenWriteResponseProto {
                    header: Some(resp_header),
                    file_handle: 0,
                    lease_id: None,
                    fencing_token: None,
                    write_targets: vec![],
                    base_size: 0,
                    open_epoch: 0,
                    lease_epoch: 0,
                    expires_at_ms: 0,
                }));
            }
        };

        // Get inode and verify it's a file
        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(OpenWriteResponseProto {
                    header: Some(resp_header),
                    file_handle: 0,
                    lease_id: None,
                    fencing_token: None,
                    write_targets: vec![],
                    base_size: 0,
                    open_epoch: 0,
                    lease_epoch: 0,
                    expires_at_ms: 0,
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(OpenWriteResponseProto {
                    header: Some(resp_header),
                    file_handle: 0,
                    lease_id: None,
                    fencing_token: None,
                    write_targets: vec![],
                    base_size: 0,
                    open_epoch: 0,
                    lease_epoch: 0,
                    expires_at_ms: 0,
                }));
            }
        };

        if !inode.kind.is_file() {
            let resp_header = Self::header_from_error(
                &req.header,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
                None,
            );
            return Ok(Response::new(OpenWriteResponseProto {
                header: Some(resp_header),
                file_handle: 0,
                lease_id: None,
                fencing_token: None,
                write_targets: vec![],
                base_size: 0,
                open_epoch: 0,
                lease_epoch: 0,
                expires_at_ms: 0,
            }));
        }

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::data_io(DataIoOp::OpenWrite).with_leader().with_authz(),
                Some(inode.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::OpenWrite,
                    AuthzTarget::for_inode(inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(OpenWriteResponseProto {
                header: Some(resp_header),
                file_handle: 0,
                lease_id: None,
                fencing_token: None,
                write_targets: vec![],
                base_size: 0,
                open_epoch: 0,
                lease_epoch: 0,
                expires_at_ms: 0,
            }));
        }

        let req_ctx = request_context_from_proto(&req.header);
        let mode = match req.mode {
            x if x == WriteModeProto::WriteModeAppend as i32 => crate::inode_lease::WriteMode::Append,
            _ => crate::inode_lease::WriteMode::Write,
        };
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        let resp = match self
            .fs_core
            .open_write(OpenWriteInput {
                ctx: req_ctx.clone(),
                inode_id,
                desired_len: req.desired_len,
                mode,
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                OpenWriteResponseProto {
                    header: Some(header),
                    file_handle: payload.session_key.file_handle,
                    lease_id: Some(lease_id_to_proto(payload.session_key.lease_id)),
                    fencing_token: Some(fencing_to_proto(payload.session_key.fencing_token)),
                    write_targets: payload.write_targets.iter().map(write_target_to_proto).collect(),
                    base_size: payload.base_size,
                    open_epoch: payload.session_key.open_epoch,
                    lease_epoch: payload.session_key.lease_epoch,
                    expires_at_ms: payload.expires_at_ms,
                }
            }
            Err(failure) => OpenWriteResponseProto {
                header: Some(header_from_core_failure(&req_ctx, &failure)),
                file_handle: 0,
                lease_id: None,
                fencing_token: None,
                write_targets: vec![],
                base_size: 0,
                open_epoch: 0,
                lease_epoch: 0,
                expires_at_ms: 0,
            },
        };
        Ok(Response::new(resp))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn close_write(
        &self,
        request: Request<CloseWriteRequestProto>,
    ) -> Result<Response<CloseWriteResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let session = match self.fs_core.write_session_for_handle(req.file_handle) {
            Some(session) => session,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Write session not found: {}", req.file_handle)),
                    None,
                    None,
                );
                return Ok(Response::new(CloseWriteResponseProto {
                    header: Some(resp_header),
                    committed_size: 0,
                    file_version: None,
                }));
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::data_io(DataIoOp::CloseWrite).with_leader().with_authz(),
                Some(session.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::CloseWrite,
                    AuthzTarget::for_session(req.file_handle, Some(session.inode_id)),
                )),
            )
            .await
        {
            return Ok(Response::new(CloseWriteResponseProto {
                header: Some(resp_header),
                committed_size: 0,
                file_version: None,
            }));
        }

        let extents = match req
            .extents
            .into_iter()
            .map(extent_from_proto)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(extents) => extents,
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(CloseWriteResponseProto {
                    header: Some(resp_header),
                    committed_size: 0,
                    file_version: None,
                }));
            }
        };

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        let resp = match self
            .fs_core
            .close_write(CloseWriteInput {
                ctx: req_ctx.clone(),
                file_handle: req.file_handle,
                lease_id: lease_id_from_proto(req.lease_id),
                lease_epoch: req.lease_epoch,
                open_epoch: req.open_epoch,
                fencing_token: presented_fencing_from_proto(req.fencing_token),
                intent: CloseWriteIntent {
                    extents,
                    final_size: req.final_size,
                },
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                CloseWriteResponseProto {
                    header: Some(header),
                    committed_size: payload.committed_size,
                    file_version: payload.file_version,
                }
            }
            Err(failure) => CloseWriteResponseProto {
                header: Some(header_from_core_failure(&req_ctx, &failure)),
                committed_size: 0,
                file_version: None,
            },
        };
        Ok(Response::new(resp))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_file_layout(
        &self,
        request: Request<GetFileLayoutRequestProto>,
    ) -> Result<Response<GetFileLayoutResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let inode_id = match req.inode_id {
            Some(inode_id) => InodeId::new(inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(GetFileLayoutResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(GetFileLayoutResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(GetFileLayoutResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(GetFileLayoutResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        if !inode.kind.is_file() {
            let resp_header = Self::header_from_error(
                &req.header,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
                None,
            );
            return Ok(Response::new(GetFileLayoutResponseProto {
                header: Some(resp_header),
                extents: vec![],
                file_size: 0,
                locations: Vec::new(),
            }));
        }

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::data_io(DataIoOp::Read).with_authz(),
                Some(inode.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::GetFileLayout,
                    AuthzTarget::for_inode(inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(GetFileLayoutResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        let range = req.range.map(|r| FileRange {
            offset: r.offset,
            len: r.len as u64,
        });
        let resp = match self
            .fs_core
            .get_file_layout(GetFileLayoutInput {
                ctx: req_ctx.clone(),
                inode_id,
                range,
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                GetFileLayoutResponseProto {
                    header: Some(header),
                    extents: payload.extents.iter().map(extent_to_proto).collect(),
                    file_size: payload.file_size,
                    locations: payload.locations.iter().map(location_to_proto).collect(),
                }
            }
            Err(failure) => GetFileLayoutResponseProto {
                header: Some(header_from_core_failure(&req_ctx, &failure)),
                ..Default::default()
            },
        };
        Ok(Response::new(resp))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn renew_inode_lease(
        &self,
        request: Request<RenewInodeLeaseRequestProto>,
    ) -> Result<Response<RenewInodeLeaseResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let session = match self.fs_core.write_session_for_handle(req.file_handle) {
            Some(session) => session,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Write session not found: {}", req.file_handle)),
                    None,
                    None,
                );
                return Ok(Response::new(RenewInodeLeaseResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::data_io(DataIoOp::RenewLease).with_leader().with_authz(),
                Some(session.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::RenewInodeLease,
                    AuthzTarget::for_session(req.file_handle, Some(session.inode_id)),
                )),
            )
            .await
        {
            return Ok(Response::new(RenewInodeLeaseResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        let resp = match self
            .fs_core
            .renew_lease(RenewLeaseInput {
                ctx: req_ctx.clone(),
                file_handle: req.file_handle,
                lease_id: lease_id_from_proto(req.lease_id),
                lease_epoch: req.lease_epoch,
                freshness,
            })
            .await
        {
            Ok(success) => RenewInodeLeaseResponseProto {
                header: Some(ok_header_from_core_success(&req_ctx, &success)),
                expires_at_ms: success.payload.expires_at_ms,
            },
            Err(failure) => RenewInodeLeaseResponseProto {
                header: Some(header_from_core_failure(&req_ctx, &failure)),
                ..Default::default()
            },
        };
        Ok(Response::new(resp))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn truncate(
        &self,
        request: Request<TruncateRequestProto>,
    ) -> Result<Response<TruncateResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let inode_id = match req.inode_id {
            Some(inode_id) => InodeId::new(inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(TruncateResponseProto {
                    header: Some(resp_header),
                    new_size: 0,
                }));
            }
        };

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(TruncateResponseProto {
                    header: Some(resp_header),
                    new_size: 0,
                }));
            }
        };

        // Get inode
        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(TruncateResponseProto {
                    header: Some(resp_header),
                    new_size: 0,
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(TruncateResponseProto {
                    header: Some(resp_header),
                    new_size: 0,
                }));
            }
        };

        if !inode.kind.is_file() {
            let resp_header = Self::header_from_error(
                &req.header,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
                None,
            );
            return Ok(Response::new(TruncateResponseProto {
                header: Some(resp_header),
                new_size: 0,
            }));
        }

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::data_io(DataIoOp::Truncate).with_leader().with_authz(),
                Some(inode.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::Truncate,
                    AuthzTarget::for_inode(inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(TruncateResponseProto {
                header: Some(resp_header),
                new_size: 0,
            }));
        }

        // Validate lease (required for truncate)
        let lease_id_proto = match req.lease_id {
            Some(lease_id_proto) => lease_id_proto,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(TruncateResponseProto {
                    header: Some(resp_header),
                    new_size: 0,
                }));
            }
        };
        let lease_id_raw = (lease_id_proto.high as u128) << 64 | lease_id_proto.low as u128;
        let lease_id_typed = LeaseId::new(lease_id_raw);

        if let Err(e) = self
            .inode_lease_manager
            .validate_lease(inode_id, lease_id_typed, req.lease_epoch)
        {
            let resp_header = fatal_fs_header(
                &req.header,
                e,
                format!(
                    "Lease validation failed for truncate: inode={}, lease_id={:?}",
                    inode_id, lease_id_typed
                ),
                None,
                None,
            );
            return Ok(Response::new(TruncateResponseProto {
                header: Some(resp_header),
                new_size: 0,
            }));
        }

        // Validate new_size
        let current_size = inode.attrs.size;
        if req.new_size > current_size {
            // Truncate grow not supported
            let resp_header = Self::header_from_error(
                &req.header,
                MetadataError::NotSupported(format!(
                    "Truncate grow not supported: current_size={}, new_size={}",
                    current_size, req.new_size
                )),
                None,
                None,
            );
            return Ok(Response::new(TruncateResponseProto {
                header: Some(resp_header),
                new_size: 0,
            }));
        }

        if req.new_size == current_size {
            // No-op truncate
            let resp_header = ok_header_from_request(&req.header, None, None);
            return Ok(Response::new(TruncateResponseProto {
                header: Some(resp_header),
                new_size: req.new_size,
            }));
        }

        // Route FS write operation
        let ctx = match self.route_fs_write_ctx(FsWriteOp::SetAttr, &[inode_id], &req.header) {
            Ok(ctx) => ctx,
            Err(MetadataError::MountEpochMismatch { expected, got, .. }) => {
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: client={}, server={} ", got, expected),
                    None,
                    Some(expected),
                );
                return Ok(Response::new(TruncateResponseProto {
                    header: Some(resp_header),
                    new_size: 0,
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(TruncateResponseProto {
                    header: Some(resp_header),
                    new_size: 0,
                }));
            }
        };

        let dedup = match self.dedup_key(&caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(TruncateResponseProto {
                    header: Some(resp_header),
                    new_size: 0,
                }));
            }
        };
        // Send Raft command
        let command = Command::Truncate {
            dedup,
            inode_id,
            new_size: req.new_size,
            lease_id: lease_id_typed,
            lease_epoch: req.lease_epoch,
        };

        // Propose via unified helper
        if let Err(err) = self.propose_fs_write_command(FsWriteOp::SetAttr, command).await {
            let resp_header = Self::header_from_error(
                &req.header,
                err,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
            return Ok(Response::new(TruncateResponseProto {
                header: Some(resp_header),
                new_size: 0,
            }));
        }

        let resp_header = ok_header_from_request(
            &req.header,
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
        );

        Ok(Response::new(TruncateResponseProto {
            header: Some(resp_header),
            new_size: req.new_size,
        }))
    }

    async fn stat_fs(&self, request: Request<StatFsRequestProto>) -> Result<Response<StatFsResponseProto>, Status> {
        let req = request.into_inner();
        let resp_header = Self::header_from_error(
            &req.header,
            MetadataError::NotSupported("StatFs not yet implemented".to_string()),
            None,
            None,
        );
        Ok(Response::new(StatFsResponseProto {
            header: Some(resp_header),
            ..Default::default()
        }))
    }

    async fn access(&self, request: Request<AccessRequestProto>) -> Result<Response<AccessResponseProto>, Status> {
        let req = request.into_inner();
        let resp_header = Self::header_from_error(
            &req.header,
            MetadataError::NotSupported("Access not yet implemented".to_string()),
            None,
            None,
        );
        Ok(Response::new(AccessResponseProto {
            header: Some(resp_header),
            ..Default::default()
        }))
    }

    async fn symlink(&self, request: Request<SymlinkRequestProto>) -> Result<Response<SymlinkResponseProto>, Status> {
        let req = request.into_inner();
        let resp_header = Self::header_from_error(
            &req.header,
            MetadataError::NotSupported("Symlink not yet implemented".to_string()),
            None,
            None,
        );
        Ok(Response::new(SymlinkResponseProto {
            header: Some(resp_header),
            ..Default::default()
        }))
    }

    async fn readlink(
        &self,
        request: Request<ReadlinkRequestProto>,
    ) -> Result<Response<ReadlinkResponseProto>, Status> {
        let req = request.into_inner();
        let resp_header = Self::header_from_error(
            &req.header,
            MetadataError::NotSupported("Readlink not yet implemented".to_string()),
            None,
            None,
        );
        Ok(Response::new(ReadlinkResponseProto {
            header: Some(resp_header),
            ..Default::default()
        }))
    }

    async fn link(&self, request: Request<LinkRequestProto>) -> Result<Response<LinkResponseProto>, Status> {
        let req = request.into_inner();
        let resp_header = Self::header_from_error(
            &req.header,
            MetadataError::NotSupported("Link not yet implemented".to_string()),
            None,
            None,
        );
        Ok(Response::new(LinkResponseProto {
            header: Some(resp_header),
            ..Default::default()
        }))
    }

    async fn set_xattr(
        &self,
        request: Request<SetXattrRequestProto>,
    ) -> Result<Response<SetXattrResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        let inode_id = match req.inode_id {
            Some(inode_id) => InodeId::new(inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(SetXattrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        // Route
        let ctx = match self.route_fs_write_ctx(FsWriteOp::SetAttr, &[inode_id], &req.header) {
            Ok(ctx) => ctx,
            Err(MetadataError::MountEpochMismatch { expected, got, .. }) => {
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: client={}, server={} ", got, expected),
                    None,
                    Some(expected),
                );
                return Ok(Response::new(SetXattrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(SetXattrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(ctx.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::SetXattr,
                    AuthzTarget::for_inode(inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(SetXattrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let dedup = match self.dedup_key(&caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(SetXattrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        let command = Command::SetXattr {
            dedup,
            inode_id,
            name: req.name,
            value: req.value,
            create: req.create,
            replace: req.replace,
        };
        if let Err(err) = self.propose_fs_write_command(FsWriteOp::SetAttr, command).await {
            let resp_header = Self::header_from_error(
                &req.header,
                err,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
            return Ok(Response::new(SetXattrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let resp_header = ok_header_from_request(
            &req.header,
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
        );
        Ok(Response::new(SetXattrResponseProto {
            header: Some(resp_header),
        }))
    }

    async fn get_xattr(
        &self,
        request: Request<GetXattrRequestProto>,
    ) -> Result<Response<GetXattrResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        let inode_id = match req.inode_id {
            Some(inode_id) => InodeId::new(inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(GetXattrResponseProto {
                    header: Some(resp_header),
                    value: vec![],
                }));
            }
        };
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read().with_authz(),
                None,
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::GetXattr,
                    AuthzTarget::for_inode(inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(GetXattrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(GetXattrResponseProto {
                    header: Some(resp_header),
                    value: vec![],
                }));
            }
        };
        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(GetXattrResponseProto {
                    header: Some(resp_header),
                    value: vec![],
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(GetXattrResponseProto {
                    header: Some(resp_header),
                    value: vec![],
                }));
            }
        };
        let value = match inode.xattrs.get(&req.name) {
            Some(value) => value.clone(),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("xattr not found: {}", req.name)),
                    None,
                    None,
                );
                return Ok(Response::new(GetXattrResponseProto {
                    header: Some(resp_header),
                    value: vec![],
                }));
            }
        };
        let resp_header = ok_header_from_request(&req.header, None, None);
        Ok(Response::new(GetXattrResponseProto {
            header: Some(resp_header),
            value,
        }))
    }

    async fn list_xattr(
        &self,
        request: Request<ListXattrRequestProto>,
    ) -> Result<Response<ListXattrResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        let inode_id = match req.inode_id {
            Some(inode_id) => InodeId::new(inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(ListXattrResponseProto {
                    header: Some(resp_header),
                    names: vec![],
                }));
            }
        };
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read().with_authz(),
                None,
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::ListXattr,
                    AuthzTarget::for_inode(inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(ListXattrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(ListXattrResponseProto {
                    header: Some(resp_header),
                    names: vec![],
                }));
            }
        };
        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
                return Ok(Response::new(ListXattrResponseProto {
                    header: Some(resp_header),
                    names: vec![],
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(ListXattrResponseProto {
                    header: Some(resp_header),
                    names: vec![],
                }));
            }
        };
        let names = inode.xattrs.keys().cloned().collect::<Vec<_>>();
        let resp_header = ok_header_from_request(&req.header, None, None);
        Ok(Response::new(ListXattrResponseProto {
            header: Some(resp_header),
            names,
        }))
    }

    async fn remove_xattr(
        &self,
        request: Request<RemoveXattrRequestProto>,
    ) -> Result<Response<RemoveXattrResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        let inode_id = match req.inode_id {
            Some(inode_id) => InodeId::new(inode_id.value),
            None => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    MetadataError::InvalidArgument("Missing inode_id".to_string()),
                    None,
                    None,
                );
                return Ok(Response::new(RemoveXattrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        let ctx = match self.route_fs_write_ctx(FsWriteOp::SetAttr, &[inode_id], &req.header) {
            Ok(ctx) => ctx,
            Err(MetadataError::MountEpochMismatch { expected, got, .. }) => {
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: client={}, server={} ", got, expected),
                    None,
                    Some(expected),
                );
                return Ok(Response::new(RemoveXattrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
            Err(err) => {
                let resp_header = Self::header_from_error(&req.header, err, None, None);
                return Ok(Response::new(RemoveXattrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(ctx.mount_id),
                Some(Self::authz_for_rpc(
                    FsRpcAuthz::RemoveXattr,
                    AuthzTarget::for_inode(inode_id),
                )),
            )
            .await
        {
            return Ok(Response::new(RemoveXattrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let dedup = match self.dedup_key(&caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                let resp_header = Self::header_from_error(
                    &req.header,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                return Ok(Response::new(RemoveXattrResponseProto {
                    header: Some(resp_header),
                    ..Default::default()
                }));
            }
        };
        let command = Command::RemoveXattr {
            dedup,
            inode_id,
            name: req.name,
        };
        if let Err(err) = self.propose_fs_write_command(FsWriteOp::SetAttr, command).await {
            let resp_header = Self::header_from_error(
                &req.header,
                err,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
            return Ok(Response::new(RemoveXattrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let resp_header = ok_header_from_request(
            &req.header,
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
        );
        Ok(Response::new(RemoveXattrResponseProto {
            header: Some(resp_header),
        }))
    }
}
