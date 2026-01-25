// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataFsServiceProto implementation.
//!
//! FS write routing convergence - all FS write operations
//! must route to mount.namespace_owner_group_id.

use super::extract_and_inject_context;
use super::guard::{AuthzContext, AuthzOp, GuardChain, GuardSpec};
use super::{fatal_fs_header, header_from_canonical_error, need_refresh_header, ok_header_from_request};
use crate::data_io::DataIoOp;
use crate::error::{MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::raft::{AppRaftNode, Command, RocksDBStorage};
use crate::readiness::RootReadinessGate;
use crate::state::StateStore;
use common::error::canonical::CanonicalError;
use common::error::canonical::RefreshReason;
use common::header::{RequestHeader, RpcErrorCode};
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProto;
use proto::metadata::*;
use proto::worker::worker_data_service_client::WorkerDataServiceClient;
use proto::worker::CommitWriteRequestProto;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
type CommitHook =
    Arc<dyn Fn(proto::worker::CommitWriteRequestProto) -> proto::worker::CommitWriteResponseProto + Send + Sync>;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};
use tracing::{debug, instrument, warn};
use types::fs::{Extent, FileAttrs, FsErrorCode, InodeId, InodeKind};
use types::ids::{BlockId, BlockIndex, DataHandleId, LeaseId, MountId, ShardGroupId};
use types::layout::FileLayout;
use types::lease::FencingToken;
use types::RaftLogId;

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

fn canonical_from_error_detail(detail: proto::common::ErrorDetailProto) -> CanonicalError {
    use common::error::canonical::{ErrorClass, ErrorCode};
    let class = match detail.error_class {
        1 => ErrorClass::NeedRefresh,
        2 => ErrorClass::Retryable,
        3 => ErrorClass::Fatal,
        _ => ErrorClass::Ok,
    };
    let code = match detail.code {
        Some(proto::common::error_detail_proto::Code::FsErrno(errno)) => {
            types::fs::FsErrorCode::from_u32(errno as u32).map(ErrorCode::FsErrno)
        }
        Some(proto::common::error_detail_proto::Code::RpcCode(code)) => {
            let rpc = match code {
                1 => RpcErrorCode::NoSuchMethod,
                2 => RpcErrorCode::InvalidHeader,
                3 => RpcErrorCode::VersionMismatch,
                4 => RpcErrorCode::DeserializeRequest,
                5 => RpcErrorCode::SerializeResponse,
                20 => RpcErrorCode::Unauthenticated,
                21 => RpcErrorCode::PermissionDenied,
                40 => RpcErrorCode::NotLeader,
                41 => RpcErrorCode::StaleState,
                42 => RpcErrorCode::Fencing,
                43 => RpcErrorCode::ShardMoved,
                44 => RpcErrorCode::NodeUnavailable,
                50 => RpcErrorCode::MountEpochMismatch,
                51 => RpcErrorCode::RouteEpochMismatch,
                52 => RpcErrorCode::WorkerEpochMismatch,
                53 => RpcErrorCode::BlockStampMismatch,
                54 => RpcErrorCode::EpochMismatch,
                _ => RpcErrorCode::Unspecified,
            };
            Some(ErrorCode::RpcCode(rpc))
        }
        None => None,
    };
    let reason = if class == ErrorClass::NeedRefresh {
        Some(match detail.refresh_reason {
            2 => RefreshReason::Moved,
            3 => RefreshReason::StaleState,
            4 => RefreshReason::MountEpochMismatch,
            5 => RefreshReason::RouteEpochMismatch,
            6 => RefreshReason::WorkerEpochMismatch,
            7 => RefreshReason::BlockStampMismatch,
            8 => RefreshReason::Fencing,
            9 => RefreshReason::EpochMismatch,
            _ => RefreshReason::Unknown,
        })
    } else {
        None
    };
    CanonicalError {
        class,
        code,
        reason,
        retry_after_ms: detail.retry_after_ms,
        message: detail.message,
    }
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

/// MetadataFsServiceProto implementation.
pub struct MetadataFsServiceImpl {
    state_store: Arc<dyn StateStore>,
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
        Self {
            state_store,
            guard_chain: GuardChain::new(Arc::clone(&mount_table)),
            mount_table,
            storage: None,
            raft_node: None,
            metrics: None,
            write_session_manager: Arc::new(crate::write_session::WriteSessionManager::default()),
            worker_manager: None,
            inode_lease_manager: Arc::new(crate::inode_lease::InodeLeaseManager::default()),
            worker_commit_hook: Arc::new(Mutex::new(None)),
        }
    }

    /// Set storage for inode/dentry access (required for FS operations).
    pub fn with_storage(mut self, storage: Arc<RocksDBStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set Raft node for leader/follower information (optional).
    pub fn with_raft_node(mut self, raft_node: Arc<AppRaftNode>) -> Self {
        self.guard_chain.set_leadership_checker(Arc::clone(&raft_node));
        self.raft_node = Some(raft_node);
        self
    }

    /// Set metrics for FS write routing tracking (optional).
    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::MetadataMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_readiness_gate(mut self, readiness_gate: Arc<RootReadinessGate>) -> Self {
        self.guard_chain.set_readiness_gate(readiness_gate);
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

    /// Set worker manager for block allocation (optional).
    pub fn with_worker_manager(mut self, worker_manager: Arc<crate::worker::WorkerManager>) -> Self {
        self.worker_manager = Some(worker_manager);
        self
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
                    // Return error that indicates need for refresh
                    // This will be converted to ResponseHeaderProto.error with error_class=NEED_REFRESH
                    return Err(MetadataError::StaleState(format!(
                        "Mount epoch mismatch: client={}, server={} (mount_id={:?}). Client must refresh mount table.",
                        client_mount_epoch, current_mount_epoch, mount_id
                    )));
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

    fn guard_request(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        caller_ctx: &RequestHeader,
        spec: GuardSpec,
        mount_id: Option<MountId>,
        authz: Option<AuthzContext>,
    ) -> Option<proto::common::ResponseHeaderProto> {
        match self
            .guard_chain
            .check_request(req_header, caller_ctx, spec, mount_id, authz)
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

    // DEPRECATED: Use error_helpers::fatal_fs_header or need_refresh_header instead.
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
    async fn propose_fs_write_command(&self, op: FsWriteOp, command: Command) -> MetadataResult<Vec<u8>> {
        let raft_node = self
            .raft_node
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Raft node not available".to_string()))?;

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
        raft_node
            .propose(command)
            .await
            .map_err(|e| MetadataError::Internal(format!("Failed to propose command: {}", e)))
    }

    // DEPRECATED: Use error_helpers::ok_header_from_request instead.
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
        if let Some(resp_header) = self.guard_request(&req.header, &caller_ctx, GuardSpec::metadata_read(), None, None)
        {
            return Ok(Response::new(LookupResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let parent_inode_id = InodeId::new(
            req.parent_inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing parent_inode_id".to_string()))?
                .value,
        );

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;

        // Get dentry
        let child_inode_id = storage.get_dentry(parent_inode_id, &req.name)?.ok_or_else(|| {
            MetadataError::NotFound(format!(
                "Entry not found: parent={}, name={}",
                parent_inode_id, req.name
            ))
        })?;

        // Get child inode
        let child_inode = storage
            .get_inode(child_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", child_inode_id)))?;

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
        if let Some(resp_header) = self.guard_request(&req.header, &caller_ctx, GuardSpec::metadata_read(), None, None)
        {
            return Ok(Response::new(GetAttrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let inode_id = InodeId::new(
            req.inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing inode_id".to_string()))?
                .value,
        );

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;

        let inode = storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

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

        let inode_id = InodeId::new(
            req.inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing inode_id".to_string()))?
                .value,
        );

        // Route FS write operation
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;
        let inode = storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

        // Route and validate mount_epoch
        let ctx = match self.route_fs_write_ctx(FsWriteOp::SetAttr, &[inode_id], &req.header) {
            Ok(ctx) => ctx,
            Err(MetadataError::StaleState(msg)) => {
                // Mount epoch mismatch - return NEED_REFRESH
                // Update metrics
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .fs_write_mount_epoch_mismatch_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    inode_id = %inode_id,
                    msg = %msg,
                    "FS write rejected: mount epoch mismatch (NEED_REFRESH)"
                );
                let mount_entry = self.mount_table.get_mount(inode.mount_id).ok().flatten();
                let mount_epoch = mount_entry.map(|e| e.config_version);
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: {}", msg),
                    None,
                    mount_epoch,
                );
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
            Err(e) => {
                let err: CanonicalError = e.into();
                let resp_header = super::header_from_canonical_error(&req.header, None, None, &err);
                return Ok(Response::new(SetAttrResponseProto {
                    header: Some(resp_header),
                    attrs: None,
                }));
            }
        };

        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::metadata_write(),
            Some(inode.mount_id),
            Some(AuthzContext {
                op: AuthzOp::FsWrite(FsWriteOp::SetAttr),
                mount_id: Some(inode.mount_id),
                inode_id: Some(inode_id),
            }),
        ) {
            return Ok(Response::new(SetAttrResponseProto {
                header: Some(resp_header),
                attrs: None,
            }));
        }

        // Convert attrs
        let attrs = Self::proto_to_file_attrs(req.attrs)?;

        // Send Raft command
        let command = Command::SetAttr {
            request_id: caller_ctx.client.call_id,
            inode_id,
            mask: req.mask,
            attrs,
        };

        // Propose via unified helper (tracks metrics)
        let _result = self.propose_fs_write_command(FsWriteOp::SetAttr, command).await?;

        // Read updated inode
        let updated_inode = storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::Internal("Inode disappeared after update".to_string()))?;

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

        let parent_inode_id = InodeId::new(
            req.parent_inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing parent_inode_id".to_string()))?
                .value,
        );

        let ctx = match self.route_fs_write_ctx(FsWriteOp::Mkdir, &[parent_inode_id], &req.header) {
            Ok(ctx) => ctx,
            Err(MetadataError::StaleState(msg)) => {
                // Mount epoch mismatch - return NEED_REFRESH
                // Update metrics
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .fs_write_mount_epoch_mismatch_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    parent_inode_id = %parent_inode_id,
                    msg = %msg,
                    "FS write rejected: mount epoch mismatch (NEED_REFRESH)"
                );
                let storage = self
                    .storage
                    .as_ref()
                    .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;
                let parent_inode = storage.get_inode(parent_inode_id).ok().flatten();
                let mount_id = parent_inode.map(|i| i.mount_id);
                let mount_entry = mount_id.and_then(|id| self.mount_table.get_mount(id).ok().flatten());
                let mount_epoch = mount_entry.map(|e| e.config_version);
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: {}", msg),
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
                let err: CanonicalError = e.into();
                let resp_header = super::header_from_canonical_error(&req.header, None, None, &err);
                return Ok(Response::new(MkdirResponseProto {
                    header: Some(resp_header),
                    inode: None,
                    attrs: None,
                }));
            }
        };

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;
        let parent_inode = storage
            .get_inode(parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;
        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::metadata_write(),
            Some(parent_inode.mount_id),
            Some(AuthzContext {
                op: AuthzOp::FsWrite(FsWriteOp::Mkdir),
                mount_id: Some(parent_inode.mount_id),
                inode_id: Some(parent_inode_id),
            }),
        ) {
            return Ok(Response::new(MkdirResponseProto {
                header: Some(resp_header),
                inode: None,
                attrs: None,
            }));
        }

        // Convert attrs
        let attrs = Self::proto_to_file_attrs(req.attrs)?;

        // Send Raft command
        let command = Command::Mkdir {
            request_id: caller_ctx.client.call_id,
            parent_inode_id,
            name: req.name,
            attrs,
        };

        let _result = self.propose_fs_write_command(FsWriteOp::Mkdir, command).await?;

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

        let parent_inode_id = InodeId::new(
            req.parent_inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing parent_inode_id".to_string()))?
                .value,
        );

        let ctx = self.route_fs_write_ctx(FsWriteOp::Create, &[parent_inode_id], &req.header)?;

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;
        let parent_inode = storage
            .get_inode(parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;
        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::metadata_write(),
            Some(parent_inode.mount_id),
            Some(AuthzContext {
                op: AuthzOp::FsWrite(FsWriteOp::Create),
                mount_id: Some(parent_inode.mount_id),
                inode_id: Some(parent_inode_id),
            }),
        ) {
            return Ok(Response::new(CreateResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        // Convert attrs and layout
        let attrs = Self::proto_to_file_attrs(req.attrs)?;
        let layout = Self::proto_to_file_layout(req.layout)?;

        // Send Raft command
        let command = Command::Create {
            request_id: caller_ctx.client.call_id,
            parent_inode_id,
            name: req.name,
            attrs,
            layout,
        };

        let _result = self.propose_fs_write_command(FsWriteOp::Create, command).await?;

        // TODO: Parse result to get created inode_id and data_handle_id
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
            inode: None,       // TODO: Return created inode
            attrs: None,       // TODO: Return created attrs
            data_handle_id: 0, // TODO: Return created data_handle_id
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn read_dir(&self, request: Request<ReadDirRequestProto>) -> Result<Response<ReadDirResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Some(resp_header) = self.guard_request(&req.header, &caller_ctx, GuardSpec::metadata_read(), None, None)
        {
            return Ok(Response::new(ReadDirResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        let parent_inode_id = InodeId::new(
            req.parent_inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing parent_inode_id".to_string()))?
                .value,
        );

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;

        // Verify parent is a directory
        let parent_inode = storage
            .get_inode(parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;
        if !parent_inode.kind.is_dir() {
            let err: CanonicalError =
                MetadataError::InvalidArgument(format!("Parent is not a directory: {}", parent_inode_id)).into();
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
            storage.list_dentries_with_cursor(parent_inode_id, cursor_key, max_entries)?;

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

        let parent_inode_id = InodeId::new(
            req.parent_inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing parent_inode_id".to_string()))?
                .value,
        );

        let ctx = self.route_fs_write_ctx(FsWriteOp::Unlink, &[parent_inode_id], &req.header)?;

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;
        let parent_inode = storage
            .get_inode(parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;
        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::metadata_write(),
            Some(parent_inode.mount_id),
            Some(AuthzContext {
                op: AuthzOp::FsWrite(FsWriteOp::Unlink),
                mount_id: Some(parent_inode.mount_id),
                inode_id: Some(parent_inode_id),
            }),
        ) {
            return Ok(Response::new(UnlinkResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        // Send Raft command
        let _raft_node = self
            .raft_node
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Raft node not available".to_string()))?;

        let command = Command::Unlink {
            request_id: caller_ctx.client.call_id,
            parent_inode_id,
            name: req.name,
        };

        let _result = self.propose_fs_write_command(FsWriteOp::Unlink, command).await?;

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

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rmdir(&self, request: Request<RmdirRequestProto>) -> Result<Response<RmdirResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let parent_inode_id = InodeId::new(
            req.parent_inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing parent_inode_id".to_string()))?
                .value,
        );

        let ctx = self.route_fs_write_ctx(FsWriteOp::Rmdir, &[parent_inode_id], &req.header)?;

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;
        let parent_inode = storage
            .get_inode(parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;
        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::metadata_write(),
            Some(parent_inode.mount_id),
            Some(AuthzContext {
                op: AuthzOp::FsWrite(FsWriteOp::Rmdir),
                mount_id: Some(parent_inode.mount_id),
                inode_id: Some(parent_inode_id),
            }),
        ) {
            return Ok(Response::new(RmdirResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        // Send Raft command
        let _raft_node = self
            .raft_node
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Raft node not available".to_string()))?;

        let command = Command::Rmdir {
            request_id: caller_ctx.client.call_id,
            parent_inode_id,
            name: req.name,
        };

        let result = self.propose_fs_write_command(FsWriteOp::Rmdir, command).await;

        // Handle Raft errors and convert InvalidArgument("Directory not empty") to ENOTEMPTY
        match result {
            Ok(_) => {
                let mut resp_header =
                    self.create_response_header_from_request(&req.header, Some(ctx.namespace_owner_group_id.as_raw()));
                resp_header.mount_epoch = Some(ctx.mount_epoch);
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
            Err(MetadataError::InvalidArgument(msg)) if msg.contains("not empty") => {
                // Convert "Directory not empty" to ENOTEMPTY error code
                let resp_header = fatal_fs_header(
                    &req.header,
                    FsErrorCode::ENotEmpty,
                    msg,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
                Ok(Response::new(RmdirResponseProto {
                    header: Some(resp_header),
                }))
            }
            Err(e) => {
                let err: CanonicalError = e.into();
                let resp_header = super::header_from_canonical_error(
                    &req.header,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                    &err,
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
            let resp_header = fatal_fs_header(
                &req.header,
                FsErrorCode::ENotsup,
                format!("Unsupported rename flags: {}", req.flags),
                None,
                None,
            );
            return Ok(Response::new(FsRenameResponseProto {
                header: Some(resp_header),
            }));
        }

        let src_parent_inode_id = InodeId::new(
            req.src_parent_inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing src_parent_inode_id".to_string()))?
                .value,
        );
        let dst_parent_inode_id = InodeId::new(
            req.dst_parent_inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing dst_parent_inode_id".to_string()))?
                .value,
        );

        // Check cross-mount rename
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;
        let src_parent_inode = storage.get_inode(src_parent_inode_id)?.ok_or_else(|| {
            MetadataError::NotFound(format!("Source parent inode not found: {}", src_parent_inode_id))
        })?;
        let dst_parent_inode = storage.get_inode(dst_parent_inode_id)?.ok_or_else(|| {
            MetadataError::NotFound(format!("Destination parent inode not found: {}", dst_parent_inode_id))
        })?;

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
            let mount_entry = self
                .mount_table
                .get_mount(src_parent_inode.mount_id)?
                .ok_or_else(|| MetadataError::Internal("Mount disappeared".to_string()))?;
            let resp_header = fatal_fs_header(
                &req.header,
                FsErrorCode::EXDev,
                format!(
                    "Cross-mount rename not allowed: src_mount={:?}, dst_mount={:?}",
                    src_parent_inode.mount_id, dst_parent_inode.mount_id
                ),
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
            Err(MetadataError::StaleState(msg)) => {
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
                    msg = %msg,
                    "FS write rejected: mount epoch mismatch (NEED_REFRESH)"
                );
                let mount_entry = self.mount_table.get_mount(src_parent_inode.mount_id).ok().flatten();
                let mount_epoch = mount_entry.map(|e| e.config_version);
                let resp_header = need_refresh_header(
                    &req.header,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    format!("Mount epoch mismatch: {}", msg),
                    None,
                    mount_epoch,
                );
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
            Err(e) => {
                let err: CanonicalError = e.into();
                let resp_header = super::header_from_canonical_error(&req.header, None, None, &err);
                return Ok(Response::new(FsRenameResponseProto {
                    header: Some(resp_header),
                }));
            }
        };

        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::metadata_write(),
            Some(src_parent_inode.mount_id),
            Some(AuthzContext {
                op: AuthzOp::FsWrite(FsWriteOp::Rename),
                mount_id: Some(src_parent_inode.mount_id),
                inode_id: Some(src_parent_inode_id),
            }),
        ) {
            return Ok(Response::new(FsRenameResponseProto {
                header: Some(resp_header),
            }));
        }

        // Send Raft command
        let _raft_node = self
            .raft_node
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Raft node not available".to_string()))?;

        let command = Command::Rename {
            request_id: caller_ctx.client.call_id,
            src_parent_inode_id,
            src_name: req.src_name,
            dst_parent_inode_id,
            dst_name: req.dst_name,
            flags: req.flags,
        };

        let _result = self.propose_fs_write_command(FsWriteOp::Rename, command).await?;

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
        if let Some(resp_header) = self.guard_request(&req.header, &caller_ctx, GuardSpec::metadata_read(), None, None)
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
        if let Some(resp_header) = self.guard_request(&req.header, &caller_ctx, GuardSpec::metadata_read(), None, None)
        {
            return Ok(Response::new(ReleaseResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let resp_header = ok_header_from_request(&req.header, None, None);
        Ok(Response::new(ReleaseResponseProto {
            header: Some(resp_header),
        }))
    }

    async fn fsync(&self, request: Request<FsyncRequestProto>) -> Result<Response<FsyncResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let inode_id = InodeId::new(
            req.inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing inode_id".to_string()))?
                .value,
        );

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;

        let inode = storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

        if !inode.kind.is_file() {
            let resp_header = fatal_fs_header(
                &req.header,
                FsErrorCode::EIsDir,
                format!("Inode is not a file: {}", inode_id),
                None,
                None,
            );
            return Ok(Response::new(FsyncResponseProto {
                header: Some(resp_header),
            }));
        }

        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::data_io(DataIoOp::Fsync).with_leader(),
            Some(inode.mount_id),
            Some(AuthzContext {
                op: AuthzOp::DataIo(DataIoOp::Fsync),
                mount_id: Some(inode.mount_id),
                inode_id: Some(inode_id),
            }),
        ) {
            return Ok(Response::new(FsyncResponseProto {
                header: Some(resp_header),
            }));
        }

        // Resolve lease and fencing
        let mut lease_id_proto = req.lease_id;
        let mut lease_epoch = req.lease_epoch.unwrap_or(0);
        let mut fencing_token = req.fencing_token;

        if let Some(handle) = req.file_handle {
            if let Some(session) = self.write_session_manager.get_session(handle) {
                if session.inode_id != inode_id {
                    let resp_header = fatal_fs_header(
                        &req.header,
                        FsErrorCode::EInval,
                        "File handle does not match inode".to_string(),
                        None,
                        None,
                    );
                    return Ok(Response::new(FsyncResponseProto {
                        header: Some(resp_header),
                    }));
                }
                if lease_id_proto.is_none() {
                    lease_id_proto = Some(proto::common::LeaseIdProto {
                        high: (session.lease_id.as_raw() >> 64) as u64,
                        low: session.lease_id.as_raw() as u64,
                    });
                }
                if lease_epoch == 0 {
                    lease_epoch = session.lease_epoch;
                }
                if fencing_token.is_none() {
                    fencing_token = Some(proto::common::FencingTokenProto {
                        block_id: Some(proto::common::BlockIdProto {
                            data_handle_id: session.fencing_token.block_id.data_handle_id.as_raw(),
                            block_index: session.fencing_token.block_id.index.as_raw(),
                        }),
                        owner: session.fencing_token.owner.as_raw(),
                        epoch: session.fencing_token.epoch,
                    });
                }
            } else {
                let resp_header = fatal_fs_header(
                    &req.header,
                    FsErrorCode::EInval,
                    "File handle not found".to_string(),
                    None,
                    None,
                );
                return Ok(Response::new(FsyncResponseProto {
                    header: Some(resp_header),
                }));
            }
        }

        let lease_id_proto =
            lease_id_proto.ok_or_else(|| MetadataError::InvalidArgument("Missing lease_id".to_string()))?;
        let lease_id_raw = (lease_id_proto.high as u128) << 64 | lease_id_proto.low as u128;
        let lease_id_typed = LeaseId::new(lease_id_raw);

        // Validate lease
        if let Err(e) = self
            .inode_lease_manager
            .validate_lease(inode_id, lease_id_typed, lease_epoch)
        {
            let resp_header = fatal_fs_header(
                &req.header,
                e,
                format!(
                    "Lease validation failed for fsync: inode={}, lease_id={:?}",
                    inode_id, lease_id_typed
                ),
                None,
                None,
            );
            return Ok(Response::new(FsyncResponseProto {
                header: Some(resp_header),
            }));
        }

        // Collect worker endpoints from write session (preferred)
        let mut commit_workers: Vec<proto::common::WorkerEndpointInfoProto> = Vec::new();
        let mut target_size = req.target_size.unwrap_or(inode.attrs.size);
        if let Some(handle) = req.file_handle {
            if let Some(session) = self.write_session_manager.get_session(handle) {
                // Only use write_session targets per requirements
                for wt in &session.write_targets {
                    commit_workers.extend(wt.worker_endpoints.clone());
                }
                // Effective target size: never less than session base_size/last_written
                target_size = target_size.max(session.base_size).max(session.last_written);
            } else {
                let resp_header = fatal_fs_header(
                    &req.header,
                    FsErrorCode::EInval,
                    "File handle not found".to_string(),
                    None,
                    None,
                );
                return Ok(Response::new(FsyncResponseProto {
                    header: Some(resp_header),
                }));
            }
        }

        if commit_workers.is_empty() {
            // Metadata-only fsync fallback (no worker hints)
            target_size = target_size.max(inode.attrs.size);
        } else {
            // Call CommitWrite on all workers
            let mut tasks = Vec::new();
            for ep in commit_workers {
                let endpoint = format!("http://{}", ep.endpoint);
                let header_client = proto::common::ClientInfoProto {
                    call_id: caller_ctx.client.call_id.to_string(),
                    client_id: caller_ctx.client.client_id.as_raw(),
                    client_name: caller_ctx.client.client_name.clone().unwrap_or_default(),
                };
                // Build request
                let commit_req = CommitWriteRequestProto {
                    header: Some(proto::worker::DataRequestHeaderProto {
                        client: Some(header_client.clone()),
                        traceparent: req.header.as_ref().map(|h| h.traceparent.clone()).unwrap_or_default(),
                    }),
                    block_id: fencing_token.as_ref().and_then(|t| t.block_id.clone()).or_else(|| {
                        // fallback: inode->data_handle
                        Some(proto::common::BlockIdProto {
                            data_handle_id: inode.current_data_handle_id.as_raw(),
                            block_index: 0,
                        })
                    }),
                    token: fencing_token.clone(),
                    lease_epoch,
                    route_epoch: req.route_epoch.unwrap_or(0),
                    worker_epoch: ep.worker_epoch,
                    file_version: 0,
                    committed_length: target_size,
                };
                if let Some(hook) = self.worker_commit_hook.lock().unwrap().clone() {
                    let req_clone = commit_req.clone();
                    tasks.push(tokio::spawn(async move { Ok(Response::new(hook(req_clone))) }));
                    continue;
                }
                let mut client = WorkerDataServiceClient::connect(endpoint.clone())
                    .await
                    .map_err(|e| Status::unavailable(format!("Failed to connect worker {}: {}", endpoint, e)))?;
                tasks.push(tokio::spawn(async move {
                    client.commit_write(Request::new(commit_req)).await
                }));
            }

            for t in tasks {
                let resp = t.await.map_err(|e| Status::internal(format!("Join error: {}", e)))??;
                let inner = resp.into_inner();
                if let Some(err) = inner.header.and_then(|h| h.error) {
                    let cerr = canonical_from_error_detail(err);
                    let resp_header = super::header_from_canonical_error(&req.header, None, None, &cerr);
                    return Ok(Response::new(FsyncResponseProto {
                        header: Some(resp_header),
                    }));
                }
            }
        }

        // Persist mtime/ctime (and optional target_size) through Raft
        let mut attrs = inode.attrs.clone();
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        attrs.size = attrs.size.max(target_size);
        attrs.update_mtime_ctime(now_ms);

        let ctx = self.route_fs_write_ctx(FsWriteOp::SetAttr, &[inode_id], &req.header)?;
        let command = Command::SetAttr {
            request_id: caller_ctx.client.call_id,
            inode_id,
            mask: 1 | 32, // size + mtime
            attrs,
        };
        let _ = self.propose_fs_write_command(FsWriteOp::SetAttr, command).await?;

        let resp_header = ok_header_from_request(
            &req.header,
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
        );

        Ok(Response::new(FsyncResponseProto {
            header: Some(resp_header),
        }))
    }

    async fn hsync(&self, request: Request<HsyncRequestProto>) -> Result<Response<HsyncResponseProto>, Status> {
        // Reuse fsync semantics
        let inner = request.into_inner();
        let fsync_req = inner
            .fsync
            .ok_or_else(|| Status::invalid_argument("missing fsync body"))?;
        let resp = self.fsync(Request::new(fsync_req)).await?;
        Ok(Response::new(HsyncResponseProto {
            header: resp.into_inner().header,
        }))
    }

    async fn hflush(&self, request: Request<HflushRequestProto>) -> Result<Response<HflushResponseProto>, Status> {
        let inner = request.into_inner();
        let fsync_req = inner
            .fsync
            .ok_or_else(|| Status::invalid_argument("missing fsync body"))?;
        let resp = self.fsync(Request::new(fsync_req)).await?;
        Ok(Response::new(HflushResponseProto {
            header: resp.into_inner().header,
        }))
    }

    async fn stat_fs(&self, _request: Request<StatFsRequestProto>) -> Result<Response<StatFsResponseProto>, Status> {
        Err(Status::unimplemented("StatFs not yet implemented"))
    }

    async fn access(&self, _request: Request<AccessRequestProto>) -> Result<Response<AccessResponseProto>, Status> {
        Err(Status::unimplemented("Access not yet implemented"))
    }

    async fn symlink(&self, _request: Request<SymlinkRequestProto>) -> Result<Response<SymlinkResponseProto>, Status> {
        Err(Status::unimplemented("Symlink not yet implemented"))
    }

    async fn readlink(
        &self,
        _request: Request<ReadlinkRequestProto>,
    ) -> Result<Response<ReadlinkResponseProto>, Status> {
        Err(Status::unimplemented("Readlink not yet implemented"))
    }

    async fn link(&self, _request: Request<LinkRequestProto>) -> Result<Response<LinkResponseProto>, Status> {
        Err(Status::unimplemented("Link not yet implemented"))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn open_write(
        &self,
        request: Request<OpenWriteRequestProto>,
    ) -> Result<Response<OpenWriteResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let inode_id = InodeId::new(
            req.inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing inode_id".to_string()))?
                .value,
        );

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;

        // Get inode and verify it's a file
        let inode = storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

        if !inode.kind.is_file() {
            let resp_header = fatal_fs_header(
                &req.header,
                FsErrorCode::EIsDir,
                format!("Inode is not a file: {}", inode_id),
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

        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::data_io(DataIoOp::OpenWrite).with_leader(),
            Some(inode.mount_id),
            Some(AuthzContext {
                op: AuthzOp::DataIo(DataIoOp::OpenWrite),
                mount_id: Some(inode.mount_id),
                inode_id: Some(inode_id),
            }),
        ) {
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

        // Determine write mode (mode is i32 in proto)
        let mode = match req.mode {
            2 => crate::inode_lease::WriteMode::Append, // WRITE_MODE_APPEND
            1 => crate::inode_lease::WriteMode::Write,  // WRITE_MODE_WRITE
            _ => crate::inode_lease::WriteMode::Write,  // Default to WRITE
        };

        // Get base size: for APPEND, use file_size; for WRITE, use 0 (future: support offset)
        let base_size = match mode {
            crate::inode_lease::WriteMode::Append => inode.attrs.size,
            crate::inode_lease::WriteMode::Write => 0, // TODO: Support offset in future
        };

        // Get current lease_epoch from inode (persisted)
        let current_lease_epoch = match &inode.data {
            types::fs::InodeData::File { lease_epoch, .. } => *lease_epoch,
            _ => None,
        };

        // Try to acquire lease
        let (lease_id, lease_epoch, expires_at_ms) = match self.inode_lease_manager.try_acquire(
            inode_id,
            caller_ctx.client.client_id,
            Some(caller_ctx.client.call_id.clone()),
            mode,
            current_lease_epoch,
        ) {
            Ok(result) => result,
            Err(FsErrorCode::EBusy) => {
                let resp_header = fatal_fs_header(
                    &req.header,
                    FsErrorCode::EBusy,
                    format!("File already has an active write lease: {}", inode_id),
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
            Err(e) => {
                let resp_header = fatal_fs_header(
                    &req.header,
                    e,
                    format!("Failed to acquire lease: {}", inode_id),
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

        // Update inode with new lease_epoch (persist to Raft)
        // This is done via a SetAttr-like command, but we'll do it atomically with OpenWrite
        // For now, we'll update it in CloseWrite to avoid extra Raft write
        // TODO: Consider batching lease_epoch update with block allocation

        let open_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Get data_handle_id from inode_id (for block allocation)
        let data_handle_id = DataHandleId::new(inode_id.as_raw());

        // Allocate block(s) for write
        let desired_len = req.desired_len.unwrap_or(4 * 1024 * 1024); // Default 4MB
        let block_size = 4 * 1024 * 1024; // TODO: Get from file layout
        let num_blocks = (desired_len + block_size - 1) / block_size;
        let num_blocks = num_blocks.max(1).min(10); // Limit to 10 blocks for now

        let mut write_targets = Vec::new();
        let worker_manager = self
            .worker_manager
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Worker manager not available".to_string()))?;

        for i in 0..num_blocks {
            let block_index = BlockIndex::new(i as u32);
            let block_id = BlockId::new(data_handle_id, block_index);

            // Select workers for placement
            let placement = worker_manager
                .select_workers_for_placement(3, None)
                .map_err(|e| MetadataError::Internal(format!("Failed to select workers: {}", e)))?;

            // Create fencing token (use lease_epoch for fencing)
            let _fencing_token = FencingToken {
                block_id,
                owner: caller_ctx.client.client_id,
                epoch: lease_epoch,
            };

            // Convert worker IDs to endpoints (simplified)
            let mut worker_endpoints = Vec::new();
            for worker_id in placement.all_workers() {
                if let Some(worker_info) = worker_manager.get_worker(worker_id) {
                    worker_endpoints.push(proto::common::WorkerEndpointInfoProto {
                        worker_id: worker_id.as_raw(),
                        endpoint: format!("{}:{}", worker_info.address, 0), // TODO: Get port
                        net_transport_kind: worker_info.net_transport_kind as i32,
                        worker_epoch: worker_info.worker_epoch,
                    });
                }
            }

            write_targets.push(proto::metadata::WriteTargetProto {
                block_id: Some(proto::common::BlockIdProto {
                    data_handle_id: data_handle_id.as_raw(),
                    block_index: block_index.as_raw(),
                }),
                worker_endpoints,
                fencing_token: Some(proto::common::FencingTokenProto {
                    block_id: Some(proto::common::BlockIdProto {
                        data_handle_id: data_handle_id.as_raw(),
                        block_index: block_index.as_raw(),
                    }),
                    owner: caller_ctx.client.client_id.as_raw(),
                    epoch: lease_epoch,
                }),
            });
        }

        // Create write session
        let file_handle = self.write_session_manager.create_session(
            inode_id,
            inode.mount_id,
            lease_id,
            lease_epoch,
            FencingToken {
                block_id: BlockId::new(data_handle_id, BlockIndex::new(0)),
                owner: caller_ctx.client.client_id,
                epoch: lease_epoch,
            },
            open_epoch,
            base_size,
            mode,
            write_targets.clone(),
            crate::write_session::WriterIdentity {
                client_id: caller_ctx.client.client_id,
                call_id: caller_ctx.client.call_id,
            },
        );

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(OpenWriteResponseProto {
            header: Some(resp_header),
            file_handle,
            lease_id: Some(proto::common::LeaseIdProto {
                high: (lease_id.as_raw() >> 64) as u64,
                low: lease_id.as_raw() as u64,
            }),
            fencing_token: Some(proto::common::FencingTokenProto {
                block_id: Some(proto::common::BlockIdProto {
                    data_handle_id: data_handle_id.as_raw(),
                    block_index: 0,
                }),
                owner: caller_ctx.client.client_id.as_raw(),
                epoch: lease_epoch,
            }),
            write_targets,
            base_size,
            open_epoch,
            lease_epoch,
            expires_at_ms,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn close_write(
        &self,
        request: Request<CloseWriteRequestProto>,
    ) -> Result<Response<CloseWriteResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let file_handle = req.file_handle;

        // Get write session
        let session = self
            .write_session_manager
            .get_session(file_handle)
            .ok_or_else(|| MetadataError::NotFound(format!("Write session not found: {}", file_handle)))?;

        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::data_io(DataIoOp::CloseWrite).with_leader(),
            Some(session.mount_id),
            Some(AuthzContext {
                op: AuthzOp::DataIo(DataIoOp::CloseWrite),
                mount_id: Some(session.mount_id),
                inode_id: Some(session.inode_id),
            }),
        ) {
            return Ok(Response::new(CloseWriteResponseProto {
                header: Some(resp_header),
                committed_size: 0,
                file_version: None,
            }));
        }

        // Validate lease_id and lease_epoch (fencing check)
        let lease_id_proto = req
            .lease_id
            .ok_or_else(|| MetadataError::InvalidArgument("Missing lease_id".to_string()))?;
        let lease_id_raw = (lease_id_proto.high as u128) << 64 | lease_id_proto.low as u128;
        let lease_id_typed = LeaseId::new(lease_id_raw);

        // Get lease_epoch from request
        let request_lease_epoch = req.lease_epoch;

        // Validate lease using InodeLeaseManager (fencing)
        if let Err(e) = self
            .inode_lease_manager
            .validate_lease(session.inode_id, lease_id_typed, request_lease_epoch)
        {
            let resp_header = fatal_fs_header(
                &req.header,
                e,
                format!(
                    "Lease validation failed (fencing): inode={}, lease_id={:?}",
                    session.inode_id, lease_id_typed
                ),
                None,
                None,
            );
            return Ok(Response::new(CloseWriteResponseProto {
                header: Some(resp_header),
                committed_size: 0,
                file_version: None,
            }));
        }

        // Validate open_epoch
        if req.open_epoch != session.open_epoch {
            let resp_header = fatal_fs_header(
                &req.header,
                FsErrorCode::EPerm,
                format!(
                    "Open epoch mismatch: expected {}, got {}",
                    session.open_epoch, req.open_epoch
                ),
                None,
                None,
            );
            return Ok(Response::new(CloseWriteResponseProto {
                header: Some(resp_header),
                committed_size: 0,
                file_version: None,
            }));
        }

        // Convert proto extents to Rust extents
        let mut extents = Vec::new();
        for proto_extent in req.extents {
            let block_id = proto_extent
                .block_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing block_id in extent".to_string()))?;
            extents.push(Extent {
                file_offset: proto_extent.file_offset,
                block_id: BlockId::new(
                    DataHandleId::new(block_id.data_handle_id),
                    BlockIndex::new(block_id.block_index),
                ),
                block_offset: proto_extent.block_offset,
                len: proto_extent.len,
                file_version: proto_extent.file_version,
                block_stamp: proto_extent.block_stamp,
            });
        }

        // Validate append-only: if mode is APPEND, extents must start from base_size and be contiguous
        if session.mode == crate::inode_lease::WriteMode::Append {
            let mut expected_offset = session.base_size;
            for extent in &extents {
                if extent.file_offset != expected_offset {
                    let resp_header = fatal_fs_header(
                        &req.header,
                        FsErrorCode::EInval,
                        format!(
                            "Extent file_offset mismatch: expected {}, got {} (append mode requires contiguous writes from base_size)",
                            expected_offset, extent.file_offset
                        ),
                        None,
                        None,
                    );
                    return Ok(Response::new(CloseWriteResponseProto {
                        header: Some(resp_header),
                        committed_size: 0,
                        file_version: None,
                    }));
                }
                expected_offset += extent.len;
            }
        } else {
            // WRITE mode: future support for random writes
            // For now, we still validate basic constraints
            for extent in &extents {
                if extent.file_offset + extent.len > req.final_size {
                    let resp_header = fatal_fs_header(
                        &req.header,
                        FsErrorCode::EInval,
                        format!(
                            "Extent extends beyond final_size: extent_end={}, final_size={}",
                            extent.file_offset + extent.len,
                            req.final_size
                        ),
                        None,
                        None,
                    );
                    return Ok(Response::new(CloseWriteResponseProto {
                        header: Some(resp_header),
                        committed_size: 0,
                        file_version: None,
                    }));
                }
            }
        }

        // Validate final_size (for APPEND mode, must match expected_offset)
        if session.mode == crate::inode_lease::WriteMode::Append {
            let expected_offset = session.base_size + extents.iter().map(|e| e.len).sum::<u64>();
            if req.final_size != expected_offset {
                let resp_header = fatal_fs_header(
                    &req.header,
                    FsErrorCode::EInval,
                    format!(
                        "Final size mismatch: expected {}, got {} (append mode)",
                        expected_offset, req.final_size
                    ),
                    None,
                    None,
                );
                return Ok(Response::new(CloseWriteResponseProto {
                    header: Some(resp_header),
                    committed_size: 0,
                    file_version: None,
                }));
            }
        }

        // Note: Idempotency is checked in state machine, not here

        // Route FS write operation
        let ctx = self.route_fs_write_ctx(FsWriteOp::SetAttr, &[session.inode_id], &req.header)?;

        // Send Raft command
        let command = Command::CloseWrite {
            request_id: caller_ctx.client.call_id.clone(),
            inode_id: session.inode_id,
            extents,
            final_size: req.final_size,
            lease_id: session.lease_id,
            open_epoch: session.open_epoch,
            lease_epoch: request_lease_epoch,
        };

        // Propose via unified helper
        let _result = self.propose_fs_write_command(FsWriteOp::SetAttr, command).await?;

        // Release lease
        self.inode_lease_manager
            .release(session.inode_id, lease_id_typed, session.lease_epoch);

        // Remove session
        self.write_session_manager.remove_session(file_handle);

        let resp_header = ok_header_from_request(
            &req.header,
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
        );

        Ok(Response::new(CloseWriteResponseProto {
            header: Some(resp_header),
            committed_size: req.final_size,
            file_version: None,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_file_layout(
        &self,
        request: Request<GetFileLayoutRequestProto>,
    ) -> Result<Response<GetFileLayoutResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let inode_id = InodeId::new(
            req.inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing inode_id".to_string()))?
                .value,
        );

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;

        let inode = storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

        if !inode.kind.is_file() {
            let resp_header = fatal_fs_header(
                &req.header,
                FsErrorCode::EIsDir,
                format!("Inode is not a file: {}", inode_id),
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

        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::data_io(DataIoOp::Read),
            Some(inode.mount_id),
            Some(AuthzContext {
                op: AuthzOp::DataIo(DataIoOp::Read),
                mount_id: Some(inode.mount_id),
                inode_id: Some(inode_id),
            }),
        ) {
            return Ok(Response::new(GetFileLayoutResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        // Get extents from inode
        let extents = match &inode.data {
            types::fs::InodeData::File { extents, .. } => extents.clone(),
            _ => Vec::new(),
        };

        // Filter by range if provided
        let filtered_extents: Vec<proto::fs::ExtentProto> = if let Some(range) = req.range {
            extents
                .into_iter()
                .filter(|e| {
                    let extent_end = e.file_offset + e.len;
                    let range_end = range.offset + range.len as u64;
                    e.file_offset < range_end && extent_end > range.offset
                })
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
                .collect()
        } else {
            extents
                .into_iter()
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
                .collect()
        };

        let mut locations = Vec::with_capacity(filtered_extents.len());
        for extent in &filtered_extents {
            if let Some(block_id) = &extent.block_id {
                locations.push(proto::metadata::FileBlockLocationProto {
                    block_id: Some(block_id.clone()),
                    file_offset: extent.file_offset,
                    len: extent.len,
                    workers: Vec::new(),
                    worker_epoch: None,
                });
            }
        }

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(GetFileLayoutResponseProto {
            header: Some(resp_header),
            extents: filtered_extents,
            file_size: inode.attrs.size,
            locations,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn renew_inode_lease(
        &self,
        request: Request<RenewInodeLeaseRequestProto>,
    ) -> Result<Response<RenewInodeLeaseResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let file_handle = req.file_handle;

        // Get write session to find inode_id
        let session = self
            .write_session_manager
            .get_session(file_handle)
            .ok_or_else(|| MetadataError::NotFound(format!("Write session not found: {}", file_handle)))?;

        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::data_io(DataIoOp::RenewLease).with_leader(),
            Some(session.mount_id),
            Some(AuthzContext {
                op: AuthzOp::DataIo(DataIoOp::RenewLease),
                mount_id: Some(session.mount_id),
                inode_id: Some(session.inode_id),
            }),
        ) {
            return Ok(Response::new(RenewInodeLeaseResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }

        // Validate lease_id and lease_epoch
        let lease_id_proto = req
            .lease_id
            .ok_or_else(|| MetadataError::InvalidArgument("Missing lease_id".to_string()))?;
        let lease_id_raw = (lease_id_proto.high as u128) << 64 | lease_id_proto.low as u128;
        let lease_id_typed = LeaseId::new(lease_id_raw);

        // Renew lease (runtime-only, does not write to Raft)
        let expires_at_ms = match self
            .inode_lease_manager
            .renew(session.inode_id, lease_id_typed, req.lease_epoch)
        {
            Ok(expires) => expires,
            Err(e) => {
                let resp_header = fatal_fs_header(
                    &req.header,
                    e,
                    format!(
                        "Lease renewal failed: inode={}, lease_id={:?}",
                        session.inode_id, lease_id_typed
                    ),
                    None,
                    None,
                );
                return Ok(Response::new(RenewInodeLeaseResponseProto {
                    header: Some(resp_header),
                    expires_at_ms: 0,
                }));
            }
        };

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(RenewInodeLeaseResponseProto {
            header: Some(resp_header),
            expires_at_ms,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn truncate(
        &self,
        request: Request<TruncateRequestProto>,
    ) -> Result<Response<TruncateResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let inode_id = InodeId::new(
            req.inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing inode_id".to_string()))?
                .value,
        );

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;

        // Get inode
        let inode = storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

        if !inode.kind.is_file() {
            let resp_header = fatal_fs_header(
                &req.header,
                FsErrorCode::EIsDir,
                format!("Inode is not a file: {}", inode_id),
                None,
                None,
            );
            return Ok(Response::new(TruncateResponseProto {
                header: Some(resp_header),
                new_size: 0,
            }));
        }

        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::data_io(DataIoOp::Truncate).with_leader(),
            Some(inode.mount_id),
            Some(AuthzContext {
                op: AuthzOp::DataIo(DataIoOp::Truncate),
                mount_id: Some(inode.mount_id),
                inode_id: Some(inode_id),
            }),
        ) {
            return Ok(Response::new(TruncateResponseProto {
                header: Some(resp_header),
                new_size: 0,
            }));
        }

        // Validate lease (required for truncate)
        let lease_id_proto = req
            .lease_id
            .ok_or_else(|| MetadataError::InvalidArgument("Missing lease_id".to_string()))?;
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
            let resp_header = fatal_fs_header(
                &req.header,
                FsErrorCode::ENotsup,
                format!(
                    "Truncate grow not supported: current_size={}, new_size={}",
                    current_size, req.new_size
                ),
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
        let ctx = self.route_fs_write_ctx(FsWriteOp::SetAttr, &[inode_id], &req.header)?;

        // Send Raft command
        let command = Command::Truncate {
            request_id: caller_ctx.client.call_id.clone(),
            inode_id,
            new_size: req.new_size,
            lease_id: lease_id_typed,
            lease_epoch: req.lease_epoch,
        };

        // Propose via unified helper
        let _result = self.propose_fs_write_command(FsWriteOp::SetAttr, command).await?;

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

    async fn set_xattr(
        &self,
        request: Request<SetXattrRequestProto>,
    ) -> Result<Response<SetXattrResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);
        let inode_id = InodeId::new(
            req.inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing inode_id".to_string()))?
                .value,
        );
        // Route
        let ctx = self.route_fs_write_ctx(FsWriteOp::SetAttr, &[inode_id], &req.header)?;
        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::metadata_write(),
            Some(ctx.mount_id),
            Some(AuthzContext {
                op: AuthzOp::FsWrite(FsWriteOp::SetAttr),
                mount_id: Some(ctx.mount_id),
                inode_id: Some(inode_id),
            }),
        ) {
            return Ok(Response::new(SetXattrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let command = Command::SetXattr {
            request_id: caller_ctx.client.call_id,
            inode_id,
            name: req.name,
            value: req.value,
            create: req.create,
            replace: req.replace,
        };
        let _ = self.propose_fs_write_command(FsWriteOp::SetAttr, command).await?;
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
        if let Some(resp_header) = self.guard_request(&req.header, &caller_ctx, GuardSpec::metadata_read(), None, None)
        {
            return Ok(Response::new(GetXattrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let inode_id = InodeId::new(
            req.inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing inode_id".to_string()))?
                .value,
        );
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;
        let inode = storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;
        let value = inode
            .xattrs
            .get(&req.name)
            .ok_or_else(|| MetadataError::NotFound(format!("xattr not found: {}", req.name)))?
            .clone();
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
        if let Some(resp_header) = self.guard_request(&req.header, &caller_ctx, GuardSpec::metadata_read(), None, None)
        {
            return Ok(Response::new(ListXattrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let inode_id = InodeId::new(
            req.inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing inode_id".to_string()))?
                .value,
        );
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;
        let inode = storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;
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
        let inode_id = InodeId::new(
            req.inode_id
                .ok_or_else(|| MetadataError::InvalidArgument("Missing inode_id".to_string()))?
                .value,
        );
        let ctx = self.route_fs_write_ctx(FsWriteOp::SetAttr, &[inode_id], &req.header)?;
        if let Some(resp_header) = self.guard_request(
            &req.header,
            &caller_ctx,
            GuardSpec::metadata_write(),
            Some(ctx.mount_id),
            Some(AuthzContext {
                op: AuthzOp::FsWrite(FsWriteOp::SetAttr),
                mount_id: Some(ctx.mount_id),
                inode_id: Some(inode_id),
            }),
        ) {
            return Ok(Response::new(RemoveXattrResponseProto {
                header: Some(resp_header),
                ..Default::default()
            }));
        }
        let command = Command::RemoveXattr {
            request_id: caller_ctx.client.call_id,
            inode_id,
            name: req.name,
        };
        let _ = self.propose_fs_write_command(FsWriteOp::SetAttr, command).await?;
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
