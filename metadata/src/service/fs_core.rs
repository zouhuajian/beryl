// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Shared filesystem core semantics used by path service RPC handlers.

use super::domain::{
    AccessInput, AccessOutput, CloseWriteInput, CloseWriteOutput, CoreFailure, CoreResult, CoreSuccess, CreateInput,
    CreateOutput, DeleteOpPlan, FileBlockLocation, Freshness, FsyncBarrierInput, FsyncBarrierOutput, GetAttrInput,
    GetAttrOutput, GetFileLayoutInput, GetFileLayoutOutput, GetXattrInput, GetXattrOutput, InodeMountGuardInputs,
    InodeOwner, LinkInput, LinkOutput, ListXattrInput, ListXattrOutput, LookupInput, LookupOutput, MkdirInput,
    MkdirOutput, OpenInput, OpenOutput, OpenWriteInput, OpenWriteOutput, PresentedFencingToken, ReadDirEntry,
    ReadDirInput, ReadDirOutput, ReadlinkInput, ReadlinkOutput, ReleaseSessionInput, ReleaseSessionOutput,
    RemoveXattrInput, RemoveXattrOutput, RenameInput, RenameOpPlan, RenameOutput, RenewLeaseInput, RenewLeaseOutput,
    RequestContext, RmdirInput, RmdirOutput, SessionGuardInputs, SessionKey, SetAttrInput, SetAttrOutput,
    SetXattrInput, SetXattrOutput, StatFsInput, StatFsOutput, SymlinkInput, SymlinkOutput, TruncateInput,
    TruncateOutput, UnlinkInput, UnlinkOutput, WorkerHint, WriteTarget,
};
use crate::error::{to_canonical_fs, MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::raft::{AppDataResponse, AppRaftNode, Command, DedupKey, FsCommandResult, RocksDBStorage};
use crate::state::StateStore;
use common::error::canonical::{
    CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshHint, RefreshReason, WorkerEndpointHint,
};
use common::header::{RequestHeader, RpcErrorCode};
use proto::worker::worker_data_service_client::WorkerDataServiceClient;
use proto::worker::CommitWriteRequestProto;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;
use types::fs::{Extent, FsErrorCode, InodeId};
use types::ids::{BlockId, BlockIndex, DataHandleId, MountId, ShardGroupId, WorkerId};
use types::lease::FencingToken;
use types::RaftLogId;

pub(crate) type WorkerCommitHook =
    Arc<dyn Fn(CommitWriteRequestProto) -> proto::worker::CommitWriteResponseProto + Send + Sync>;
pub type SharedWorkerCommitHook = Arc<Mutex<Option<WorkerCommitHook>>>;

#[derive(Clone, Debug)]
struct RoutedFsWriteCtx {
    mount_id: MountId,
    namespace_owner_group_id: ShardGroupId,
    mount_epoch: u64,
    latest_state_id: Option<RaftLogId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CoreWriteOp {
    Create,
    Mkdir,
    Unlink,
    Rmdir,
    Rename,
    SetAttr,
}

fn canonical_from_error_detail(detail: proto::common::ErrorDetailProto) -> CanonicalError {
    proto::convert::error_detail_to_canonical(&detail)
}

pub struct FsCore {
    state_store: Arc<dyn StateStore>,
    mount_table: Arc<MountTable>,
    storage: Option<Arc<RocksDBStorage>>,
    raft_node: Option<Arc<AppRaftNode>>,
    metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
    write_session_manager: Arc<crate::write_session::WriteSessionManager>,
    worker_manager: Option<Arc<crate::worker::WorkerManager>>,
    inode_lease_manager: Arc<crate::inode_lease::InodeLeaseManager>,
    worker_commit_hook: SharedWorkerCommitHook,
}

impl FsCore {
    #[cfg(test)]
    pub fn new_default(state_store: Arc<dyn StateStore>, mount_table: Arc<MountTable>) -> Self {
        let write_session_manager = Arc::new(crate::write_session::WriteSessionManager::default());
        let inode_lease_manager = Arc::new(crate::inode_lease::InodeLeaseManager::default());
        let worker_commit_hook: SharedWorkerCommitHook = Arc::new(Mutex::new(None));
        Self::new(
            state_store,
            mount_table,
            write_session_manager,
            inode_lease_manager,
            worker_commit_hook,
        )
    }

    pub fn new(
        state_store: Arc<dyn StateStore>,
        mount_table: Arc<MountTable>,
        write_session_manager: Arc<crate::write_session::WriteSessionManager>,
        inode_lease_manager: Arc<crate::inode_lease::InodeLeaseManager>,
        worker_commit_hook: SharedWorkerCommitHook,
    ) -> Self {
        Self {
            state_store,
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

    pub fn set_storage(&mut self, storage: Arc<RocksDBStorage>) {
        self.storage = Some(storage);
    }

    pub fn set_raft_node(&mut self, raft_node: Arc<AppRaftNode>) {
        self.raft_node = Some(raft_node);
    }

    pub fn set_metrics(&mut self, metrics: Arc<crate::metrics::MetadataMetrics>) {
        self.metrics = Some(metrics);
    }

    pub fn set_worker_manager(&mut self, worker_manager: Arc<crate::worker::WorkerManager>) {
        self.worker_manager = Some(worker_manager);
    }

    pub fn raft_node(&self) -> Option<Arc<AppRaftNode>> {
        self.raft_node.clone()
    }

    #[cfg(test)]
    pub(crate) fn set_worker_commit_hook_for_test(&self, hook: WorkerCommitHook) {
        let mut guard = self
            .worker_commit_hook
            .lock()
            .expect("worker commit hook lock poisoned");
        *guard = Some(hook);
    }

    #[cfg(debug_assertions)]
    pub(crate) fn set_worker_commit_hook_debug(&self, hook: WorkerCommitHook) {
        let mut guard = self
            .worker_commit_hook
            .lock()
            .expect("worker commit hook lock poisoned");
        *guard = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn clear_worker_commit_hook_for_test(&self) {
        let mut guard = self
            .worker_commit_hook
            .lock()
            .expect("worker commit hook lock poisoned");
        guard.take();
    }

    #[cfg(debug_assertions)]
    pub(crate) fn clear_worker_commit_hook_debug(&self) {
        let mut guard = self
            .worker_commit_hook
            .lock()
            .expect("worker commit hook lock poisoned");
        guard.take();
    }

    #[cfg(test)]
    pub(crate) fn write_session_manager_for_test(&self) -> Arc<crate::write_session::WriteSessionManager> {
        Arc::clone(&self.write_session_manager)
    }

    #[cfg(debug_assertions)]
    pub(crate) fn debug_write_session_manager(&self) -> Arc<crate::write_session::WriteSessionManager> {
        Arc::clone(&self.write_session_manager)
    }

    #[cfg(test)]
    pub(crate) fn inode_lease_manager_for_test(&self) -> Arc<crate::inode_lease::InodeLeaseManager> {
        Arc::clone(&self.inode_lease_manager)
    }

    #[cfg(debug_assertions)]
    pub(crate) fn debug_inode_lease_manager(&self) -> Arc<crate::inode_lease::InodeLeaseManager> {
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

    fn state_id_from_ctx(ctx: &RequestContext) -> Option<RaftLogId> {
        ctx.caller.state_id
    }

    async fn authoritative_route_epoch(&self) -> Option<u64> {
        self.state_store.get_layout_version().await.ok().map(|v| v.as_u64())
    }

    fn success<T>(
        &self,
        ctx: &RequestContext,
        payload: T,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> CoreResult<T> {
        self.success_with_route_epoch(ctx, payload, group_id, mount_epoch, None)
    }

    fn success_with_route_epoch<T>(
        &self,
        ctx: &RequestContext,
        payload: T,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> CoreResult<T> {
        Ok(CoreSuccess {
            payload,
            group_id,
            mount_epoch,
            route_epoch,
            state_id: Self::state_id_from_ctx(ctx),
        })
    }

    fn failure_from_error<T>(
        &self,
        ctx: &RequestContext,
        err: MetadataError,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> CoreResult<T> {
        self.failure_from_error_with_route_epoch(ctx, err, group_id, mount_epoch, None)
    }

    fn failure_from_error_with_route_epoch<T>(
        &self,
        ctx: &RequestContext,
        err: MetadataError,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> CoreResult<T> {
        let err = to_canonical_fs(err);
        Err(CoreFailure::new(
            err,
            group_id,
            mount_epoch,
            route_epoch,
            Self::state_id_from_ctx(ctx),
        ))
    }

    fn failure_from_canonical<T>(
        &self,
        ctx: &RequestContext,
        err: CanonicalError,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> CoreResult<T> {
        self.failure_from_canonical_with_route_epoch(ctx, err, group_id, mount_epoch, None)
    }

    fn failure_from_canonical_with_route_epoch<T>(
        &self,
        ctx: &RequestContext,
        err: CanonicalError,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> CoreResult<T> {
        Err(CoreFailure::new(
            err,
            group_id,
            mount_epoch,
            route_epoch,
            Self::state_id_from_ctx(ctx),
        ))
    }

    fn need_refresh_failure<T>(
        &self,
        ctx: &RequestContext,
        rpc_code: RpcErrorCode,
        reason: RefreshReason,
        message: impl Into<String>,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> CoreResult<T> {
        self.need_refresh_failure_with_hint(ctx, rpc_code, reason, message, group_id, mount_epoch, None, None)
    }

    fn need_refresh_failure_with_hint<T>(
        &self,
        ctx: &RequestContext,
        rpc_code: RpcErrorCode,
        reason: RefreshReason,
        message: impl Into<String>,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        mut hint: Option<RefreshHint>,
    ) -> CoreResult<T> {
        if let Some(group_id_value) = group_id {
            hint.get_or_insert_with(RefreshHint::default).group_id = Some(group_id_value);
        }
        if let Some(mount_epoch_value) = mount_epoch {
            hint.get_or_insert_with(RefreshHint::default).mount_epoch = Some(mount_epoch_value);
        }
        if let Some(route_epoch_value) = route_epoch {
            hint.get_or_insert_with(RefreshHint::default).route_epoch = Some(route_epoch_value);
        }

        let canonical = match hint {
            Some(hint) => CanonicalError::need_refresh_with_hint(rpc_code, reason, hint, message),
            None => CanonicalError::need_refresh(rpc_code, reason, message),
        };
        self.failure_from_canonical_with_route_epoch(ctx, canonical, group_id, mount_epoch, route_epoch)
    }

    fn fatal_fs_failure<T>(
        &self,
        ctx: &RequestContext,
        errno: FsErrorCode,
        message: impl Into<String>,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> CoreResult<T> {
        self.failure_from_canonical(ctx, CanonicalError::fatal_fs(errno, message), group_id, mount_epoch)
    }

    fn session_terminal_failure<T>(
        &self,
        ctx: &RequestContext,
        reason: RefreshReason,
        rpc_code: RpcErrorCode,
        message: impl Into<String>,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> CoreResult<T> {
        let canonical = CanonicalError {
            class: ErrorClass::Fatal,
            code: Some(CanonicalErrorCode::RpcCode(rpc_code)),
            reason: Some(reason),
            retry_after_ms: None,
            message: message.into(),
            refresh_hint: None,
        };
        self.failure_from_canonical(ctx, canonical, group_id, mount_epoch)
    }

    fn replay_hint(intent: &str) -> String {
        format!("refresh metadata and re-open write session, then replay {}", intent)
    }

    fn worker_refresh_hint_from_session(
        session: &crate::write_session::WriteSession,
        worker_epoch: Option<u64>,
        resolve_required: bool,
    ) -> RefreshHint {
        let mut worker_endpoints = Vec::new();
        for target in &session.write_targets {
            for endpoint in &target.worker_endpoints {
                worker_endpoints.push(WorkerEndpointHint {
                    worker_id: endpoint.worker_id,
                    endpoint: endpoint.endpoint.clone(),
                    net_transport_kind: endpoint.net_transport_kind,
                    worker_epoch: endpoint.worker_epoch,
                });
            }
        }

        RefreshHint {
            worker_epoch,
            worker_endpoints,
            worker_resolve_required: resolve_required,
            ..Default::default()
        }
    }

    pub(crate) fn mount_hints_for_mount(&self, mount_id: MountId) -> (Option<u64>, Option<u64>) {
        match self.mount_table.get_mount(mount_id) {
            Ok(Some(mount_entry)) => (
                Some(mount_entry.namespace_owner_group_id.as_raw()),
                Some(mount_entry.config_version),
            ),
            _ => (None, None),
        }
    }

    fn validate_mount_epoch_for_mount(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
    ) -> Result<(Option<u64>, Option<u64>), CoreFailure> {
        let (group_id, mount_epoch) = self.mount_hints_for_mount(mount_id);
        if let (Some(client_mount_epoch), Some(server_mount_epoch)) =
            (freshness.mount_epoch.or(ctx.caller.mount_epoch), mount_epoch)
        {
            if client_mount_epoch != server_mount_epoch {
                return Err(CoreFailure::new(
                    CanonicalError::need_refresh_with_hint(
                        RpcErrorCode::MountEpochMismatch,
                        RefreshReason::MountEpochMismatch,
                        RefreshHint {
                            group_id,
                            mount_epoch: Some(server_mount_epoch),
                            ..Default::default()
                        },
                        format!(
                            "mount_epoch mismatch: client={}, server={}; {}",
                            client_mount_epoch,
                            server_mount_epoch,
                            Self::replay_hint("request")
                        ),
                    ),
                    group_id,
                    Some(server_mount_epoch),
                    None,
                    Self::state_id_from_ctx(ctx),
                ));
            }
        }
        Ok((group_id, mount_epoch))
    }

    async fn validate_route_epoch(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
        intent: &str,
    ) -> Result<Option<u64>, CoreFailure> {
        let client_route_epoch = freshness.route_epoch.or(ctx.route_epoch);

        let server_route_epoch = match self.state_store.get_layout_version().await {
            Ok(v) => v.as_u64(),
            Err(err) => {
                return Err(CoreFailure::new(
                    to_canonical_fs(err),
                    group_id,
                    mount_epoch,
                    None,
                    Self::state_id_from_ctx(ctx),
                ));
            }
        };

        if let Some(client_route_epoch) = client_route_epoch {
            if client_route_epoch != server_route_epoch {
                return Err(CoreFailure::new(
                    CanonicalError::need_refresh_with_hint(
                        RpcErrorCode::RouteEpochMismatch,
                        RefreshReason::RouteEpochMismatch,
                        RefreshHint {
                            group_id,
                            route_epoch: Some(server_route_epoch),
                            mount_epoch,
                            ..Default::default()
                        },
                        format!(
                            "route_epoch mismatch: client={}, server={}; refresh route and replay {}",
                            client_route_epoch, server_route_epoch, intent
                        ),
                    ),
                    group_id,
                    mount_epoch,
                    Some(server_route_epoch),
                    Self::state_id_from_ctx(ctx),
                ));
            }
        }

        Ok(Some(server_route_epoch))
    }

    fn fencing_token_matches_session(
        session: &crate::write_session::WriteSession,
        token: &PresentedFencingToken,
    ) -> bool {
        let session_block_id = session.fencing_token.block_id;
        let req_block = token.block_id.as_ref();
        let block_ok = req_block
            .map(|b| b.data_handle_id == session_block_id.data_handle_id && b.index == session_block_id.index)
            .unwrap_or(false);

        block_ok && token.owner == session.fencing_token.owner.as_raw() && token.epoch == session.fencing_token.epoch
    }

    fn route_ctx_for_write(
        &self,
        req_ctx: &RequestContext,
        op: CoreWriteOp,
        parent_inode_ids: &[InodeId],
        freshness: Freshness,
    ) -> Result<RoutedFsWriteCtx, CoreFailure> {
        let client_mount_epoch = freshness.mount_epoch.or(req_ctx.caller.mount_epoch);
        match self.route_fs_write_ctx(op, parent_inode_ids, client_mount_epoch) {
            Ok(ctx) => Ok(ctx),
            Err(err) => {
                let (group_id, mount_epoch, canonical) = match err {
                    MetadataError::MountEpochMismatch {
                        expected,
                        got,
                        mount_id,
                    } => {
                        if let Some(metrics) = &self.metrics {
                            metrics
                                .fs_write_mount_epoch_mismatch_total
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        let (group_id, mount_epoch) = mount_id
                            .map(|id| self.mount_hints_for_mount(id))
                            .unwrap_or((None, None));
                        let canonical = CanonicalError::need_refresh_with_hint(
                            RpcErrorCode::MountEpochMismatch,
                            RefreshReason::MountEpochMismatch,
                            RefreshHint {
                                group_id,
                                mount_epoch,
                                ..Default::default()
                            },
                            format!(
                                "mount_epoch mismatch: client={}, server={}; {}",
                                got,
                                expected,
                                Self::replay_hint("request")
                            ),
                        );
                        (group_id, mount_epoch, canonical)
                    }
                    other => (None, None, to_canonical_fs(other)),
                };
                Err(CoreFailure::new(
                    canonical,
                    group_id,
                    mount_epoch,
                    None,
                    Self::state_id_from_ctx(req_ctx),
                ))
            }
        }
    }

    fn storage_for_ctx<'a>(&'a self, req_ctx: &RequestContext) -> Result<&'a Arc<RocksDBStorage>, CoreFailure> {
        self.storage.as_ref().ok_or_else(|| {
            CoreFailure::new(
                to_canonical_fs(MetadataError::Internal("Storage not available".to_string())),
                None,
                None,
                None,
                Self::state_id_from_ctx(req_ctx),
            )
        })
    }

    fn owner_from_inode(inode: &types::fs::Inode) -> InodeOwner {
        InodeOwner {
            uid: inode.attrs.uid,
            gid: inode.attrs.gid,
        }
    }

    fn owner_for_inode_id(
        &self,
        req_ctx: &RequestContext,
        storage: &RocksDBStorage,
        inode_id: InodeId,
    ) -> Result<Option<InodeOwner>, CoreFailure> {
        let inode = match storage.get_inode(inode_id) {
            Ok(inode) => inode,
            Err(err) => {
                return Err(CoreFailure::new(
                    to_canonical_fs(err),
                    None,
                    None,
                    None,
                    Self::state_id_from_ctx(req_ctx),
                ));
            }
        };
        Ok(inode.as_ref().map(Self::owner_from_inode))
    }

    pub(crate) async fn plan_unlink(
        &self,
        req_ctx: &RequestContext,
        parent_inode_id: InodeId,
        name: &str,
    ) -> CoreResult<DeleteOpPlan> {
        let storage = match self.storage_for_ctx(req_ctx) {
            Ok(storage) => storage,
            Err(failure) => return Err(failure),
        };

        let target_inode_id = match storage.get_dentry(parent_inode_id, name) {
            Ok(inode_id) => inode_id,
            Err(err) => return self.failure_from_error(req_ctx, err, None, None),
        };
        let parent_owner = match self.owner_for_inode_id(req_ctx, storage, parent_inode_id) {
            Ok(owner) => owner,
            Err(failure) => return Err(failure),
        };
        let target_owner = match target_inode_id {
            Some(inode_id) => match self.owner_for_inode_id(req_ctx, storage, inode_id) {
                Ok(owner) => owner,
                Err(failure) => return Err(failure),
            },
            None => None,
        };

        self.success(
            req_ctx,
            DeleteOpPlan {
                parent_inode_id,
                target_inode_id,
                parent_owner,
                target_owner,
            },
            None,
            None,
        )
    }

    pub(crate) async fn plan_rmdir(
        &self,
        req_ctx: &RequestContext,
        parent_inode_id: InodeId,
        name: &str,
    ) -> CoreResult<DeleteOpPlan> {
        self.plan_unlink(req_ctx, parent_inode_id, name).await
    }

    pub(crate) async fn plan_rename(
        &self,
        req_ctx: &RequestContext,
        src_parent_inode_id: InodeId,
        src_name: &str,
        dst_parent_inode_id: InodeId,
        dst_name: &str,
    ) -> CoreResult<RenameOpPlan> {
        let storage = match self.storage_for_ctx(req_ctx) {
            Ok(storage) => storage,
            Err(failure) => return Err(failure),
        };

        let src_inode_id = match storage.get_dentry(src_parent_inode_id, src_name) {
            Ok(inode_id) => inode_id,
            Err(err) => return self.failure_from_error(req_ctx, err, None, None),
        };
        let dst_inode_id = match storage.get_dentry(dst_parent_inode_id, dst_name) {
            Ok(inode_id) => inode_id,
            Err(err) => return self.failure_from_error(req_ctx, err, None, None),
        };

        let src_parent_owner = match self.owner_for_inode_id(req_ctx, storage, src_parent_inode_id) {
            Ok(owner) => owner,
            Err(failure) => return Err(failure),
        };
        let dst_parent_owner = match self.owner_for_inode_id(req_ctx, storage, dst_parent_inode_id) {
            Ok(owner) => owner,
            Err(failure) => return Err(failure),
        };
        let src_owner = match src_inode_id {
            Some(inode_id) => match self.owner_for_inode_id(req_ctx, storage, inode_id) {
                Ok(owner) => owner,
                Err(failure) => return Err(failure),
            },
            None => None,
        };
        let dst_owner = match dst_inode_id {
            Some(inode_id) => match self.owner_for_inode_id(req_ctx, storage, inode_id) {
                Ok(owner) => owner,
                Err(failure) => return Err(failure),
            },
            None => None,
        };

        self.success(
            req_ctx,
            RenameOpPlan {
                src_parent_inode_id,
                dst_parent_inode_id,
                src_inode_id,
                dst_inode_id,
                src_parent_owner,
                src_owner,
                dst_parent_owner,
                dst_owner,
            },
            None,
            None,
        )
    }

    pub(crate) async fn plan_session(
        &self,
        req_ctx: &RequestContext,
        file_handle: u64,
    ) -> CoreResult<SessionGuardInputs> {
        let session = self.write_session_manager.get_session(file_handle);
        self.success(
            req_ctx,
            SessionGuardInputs {
                file_handle,
                inode_id: session.as_ref().map(|s| s.inode_id),
                mount_id: session.as_ref().map(|s| s.mount_id),
            },
            None,
            None,
        )
    }

    pub(crate) async fn plan_inode_mount(
        &self,
        req_ctx: &RequestContext,
        inode_id: InodeId,
    ) -> CoreResult<InodeMountGuardInputs> {
        let storage = match self.storage_for_ctx(req_ctx) {
            Ok(storage) => storage,
            Err(failure) => return Err(failure),
        };
        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    req_ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(req_ctx, err, None, None),
        };
        self.success(
            req_ctx,
            InodeMountGuardInputs {
                inode_id,
                mount_id: inode.mount_id,
            },
            None,
            None,
        )
    }

    pub(crate) async fn execute_lookup(&self, req: LookupInput) -> CoreResult<LookupOutput> {
        let storage = match self.storage_for_ctx(&req.ctx) {
            Ok(storage) => storage,
            Err(failure) => return Err(failure),
        };

        let child_inode_id = match storage.get_dentry(req.parent_inode_id, &req.name) {
            Ok(Some(child_inode_id)) => child_inode_id,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!(
                        "Entry not found: parent={}, name={}",
                        req.parent_inode_id, req.name
                    )),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        let child_inode = match storage.get_inode(child_inode_id) {
            Ok(Some(child_inode)) => child_inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", child_inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        let (group_id, mount_epoch) = self.mount_hints_for_mount(child_inode.mount_id);
        let route_epoch = self.authoritative_route_epoch().await;
        self.success_with_route_epoch(
            &req.ctx,
            LookupOutput { inode: child_inode },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_set_attr(&self, req: SetAttrInput) -> CoreResult<SetAttrOutput> {
        let ctx = match self.route_ctx_for_write(&req.ctx, CoreWriteOp::SetAttr, &[req.inode_id], req.freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        let command = Command::SetAttr {
            dedup,
            inode_id: req.inode_id,
            mask: req.mask,
            attrs: req.attrs,
        };
        let result = match self.propose_fs_write_command(CoreWriteOp::SetAttr, command).await {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        match result {
            FsCommandResult::Ok(_) => {
                let storage = match self.storage_for_ctx(&req.ctx) {
                    Ok(storage) => storage,
                    Err(failure) => return Err(failure),
                };
                let inode = match storage.get_inode(req.inode_id) {
                    Ok(Some(inode)) => inode,
                    Ok(None) => {
                        return self.failure_from_error(
                            &req.ctx,
                            MetadataError::Internal("Inode disappeared after update".to_string()),
                            Some(ctx.namespace_owner_group_id.as_raw()),
                            Some(ctx.mount_epoch),
                        );
                    }
                    Err(err) => {
                        return self.failure_from_error(
                            &req.ctx,
                            err,
                            Some(ctx.namespace_owner_group_id.as_raw()),
                            Some(ctx.mount_epoch),
                        );
                    }
                };
                self.success(
                    &req.ctx,
                    SetAttrOutput { attrs: inode.attrs },
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                )
            }
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                &req.ctx,
                err.errno,
                err.message,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            ),
        }
    }

    pub(crate) async fn execute_get_attr(&self, req: GetAttrInput) -> CoreResult<GetAttrOutput> {
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let inode = match storage.get_inode(req.inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", req.inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        let (group_id, mount_epoch) = self.mount_hints_for_mount(inode.mount_id);
        let route_epoch = self.authoritative_route_epoch().await;
        self.success_with_route_epoch(
            &req.ctx,
            GetAttrOutput {
                attrs: inode.attrs.clone(),
            },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_mkdir(&self, req: MkdirInput) -> CoreResult<MkdirOutput> {
        let ctx = match self.route_ctx_for_write(&req.ctx, CoreWriteOp::Mkdir, &[req.parent_inode_id], req.freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        let result = match self
            .propose_fs_write_command(
                CoreWriteOp::Mkdir,
                Command::Mkdir {
                    dedup,
                    parent_inode_id: req.parent_inode_id,
                    name: req.name,
                    attrs: req.attrs,
                },
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        match result {
            FsCommandResult::Ok(ok) => {
                let created_attrs = match (self.storage.as_ref(), ok.inode_id) {
                    (Some(storage), Some(inode_id)) => storage
                        .get_inode(inode_id)
                        .ok()
                        .flatten()
                        .map(|inode| inode.attrs.clone()),
                    _ => None,
                };

                self.success(
                    &req.ctx,
                    MkdirOutput {
                        inode_id: ok.inode_id,
                        attrs: created_attrs,
                    },
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                )
            }
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                &req.ctx,
                err.errno,
                err.message,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            ),
        }
    }

    pub(crate) async fn execute_create(&self, req: CreateInput) -> CoreResult<CreateOutput> {
        let ctx = match self.route_ctx_for_write(&req.ctx, CoreWriteOp::Create, &[req.parent_inode_id], req.freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        let result = match self
            .propose_fs_write_command(
                CoreWriteOp::Create,
                Command::Create {
                    dedup,
                    parent_inode_id: req.parent_inode_id,
                    name: req.name,
                    attrs: req.attrs,
                    layout: req.layout,
                },
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        match result {
            FsCommandResult::Ok(ok) => {
                let created_attrs = match (self.storage.as_ref(), ok.inode_id) {
                    (Some(storage), Some(inode_id)) => storage
                        .get_inode(inode_id)
                        .ok()
                        .flatten()
                        .map(|inode| inode.attrs.clone()),
                    _ => None,
                };

                self.success(
                    &req.ctx,
                    CreateOutput {
                        inode_id: ok.inode_id,
                        attrs: created_attrs,
                        data_handle_id: ok.data_handle_id,
                    },
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                )
            }
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                &req.ctx,
                err.errno,
                err.message,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            ),
        }
    }

    pub(crate) async fn execute_unlink(&self, req: UnlinkInput) -> CoreResult<UnlinkOutput> {
        let ctx = match self.route_ctx_for_write(&req.ctx, CoreWriteOp::Unlink, &[req.parent_inode_id], req.freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        let result = match self
            .propose_fs_write_command(
                CoreWriteOp::Unlink,
                Command::Unlink {
                    dedup,
                    parent_inode_id: req.parent_inode_id,
                    name: req.name,
                },
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        match result {
            FsCommandResult::Ok(_) => self.success(
                &req.ctx,
                UnlinkOutput,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            ),
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                &req.ctx,
                err.errno,
                err.message,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            ),
        }
    }

    pub(crate) async fn execute_rmdir(&self, req: RmdirInput) -> CoreResult<RmdirOutput> {
        let ctx = match self.route_ctx_for_write(&req.ctx, CoreWriteOp::Rmdir, &[req.parent_inode_id], req.freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        let result = match self
            .propose_fs_write_command(
                CoreWriteOp::Rmdir,
                Command::Rmdir {
                    dedup,
                    parent_inode_id: req.parent_inode_id,
                    name: req.name,
                },
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        match result {
            FsCommandResult::Ok(_) => self.success(
                &req.ctx,
                RmdirOutput,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            ),
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                &req.ctx,
                err.errno,
                err.message,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            ),
        }
    }

    pub(crate) async fn execute_rename(&self, req: RenameInput) -> CoreResult<RenameOutput> {
        let supported_mask: u32 = 0x1;
        if req.flags & !supported_mask != 0 {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::NotSupported(format!("Unsupported rename flags: {}", req.flags)),
                None,
                None,
            );
        }

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let src_parent_inode = match storage.get_inode(req.src_parent_inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Source parent inode not found: {}", req.src_parent_inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };
        let dst_parent_inode = match storage.get_inode(req.dst_parent_inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!(
                        "Destination parent inode not found: {}",
                        req.dst_parent_inode_id
                    )),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        if src_parent_inode.mount_id != dst_parent_inode.mount_id {
            if let Some(metrics) = &self.metrics {
                metrics
                    .fs_write_cross_mount_rename_exdev_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            let (group_id, mount_epoch) = self.mount_hints_for_mount(src_parent_inode.mount_id);
            return self.failure_from_error(
                &req.ctx,
                MetadataError::CrossMountRename(format!(
                    "Cross-mount rename not allowed: src_mount={:?}, dst_mount={:?}",
                    src_parent_inode.mount_id, dst_parent_inode.mount_id
                )),
                group_id,
                mount_epoch,
            );
        }

        let ctx = match self.route_ctx_for_write(
            &req.ctx,
            CoreWriteOp::Rename,
            &[req.src_parent_inode_id, req.dst_parent_inode_id],
            req.freshness,
        ) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        if req.flags & 0x1 != 0 {
            if let Some(raft_node) = self.raft_node.as_ref() {
                if raft_node.is_leader() {
                    let mut can_precheck = true;
                    if let Some(required_state_id) = req.ctx.caller.state_id {
                        if let Some(last_applied) = raft_node.get_last_applied_state_id() {
                            if last_applied < required_state_id {
                                return self.need_refresh_failure(
                                    &req.ctx,
                                    RpcErrorCode::StaleState,
                                    RefreshReason::StaleState,
                                    format!(
                                        "Stale state: last_applied={:?} < required={:?}",
                                        last_applied, required_state_id
                                    ),
                                    Some(ctx.namespace_owner_group_id.as_raw()),
                                    Some(ctx.mount_epoch),
                                );
                            }
                        } else {
                            can_precheck = false;
                        }
                    }

                    if can_precheck {
                        match storage.get_dentry(req.dst_parent_inode_id, &req.dst_name) {
                            Ok(Some(_)) => {
                                return self.failure_from_error(
                                    &req.ctx,
                                    MetadataError::AlreadyExists(format!(
                                        "Destination exists and RENAME_NOREPLACE set: {}",
                                        req.dst_name
                                    )),
                                    Some(ctx.namespace_owner_group_id.as_raw()),
                                    Some(ctx.mount_epoch),
                                );
                            }
                            Ok(None) => {}
                            Err(err) => {
                                return self.failure_from_error(
                                    &req.ctx,
                                    err,
                                    Some(ctx.namespace_owner_group_id.as_raw()),
                                    Some(ctx.mount_epoch),
                                );
                            }
                        }
                    }
                }
            }
        }

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        let result = match self
            .propose_fs_write_command(
                CoreWriteOp::Rename,
                Command::Rename {
                    dedup,
                    src_parent_inode_id: req.src_parent_inode_id,
                    src_name: req.src_name,
                    dst_parent_inode_id: req.dst_parent_inode_id,
                    dst_name: req.dst_name,
                    flags: req.flags,
                },
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        match result {
            FsCommandResult::Ok(_) => self.success(
                &req.ctx,
                RenameOutput,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            ),
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                &req.ctx,
                err.errno,
                err.message,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            ),
        }
    }

    pub(crate) async fn execute_read_dir(&self, req: ReadDirInput) -> CoreResult<ReadDirOutput> {
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let parent_inode = match storage.get_inode(req.parent_inode_id) {
            Ok(Some(parent_inode)) => parent_inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Parent inode not found: {}", req.parent_inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };
        if !parent_inode.kind.is_dir() {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::InvalidArgument(format!("Parent is not a directory: {}", req.parent_inode_id)),
                None,
                None,
            );
        }

        let cursor_key = req.cursor_key.as_deref();
        let (entries, next_cursor_key, eof) =
            match storage.list_dentries_with_cursor(req.parent_inode_id, cursor_key, req.max_entries) {
                Ok(result) => result,
                Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
            };

        let mut dir_entries = Vec::new();
        for (name, child_inode_id) in entries {
            let child_inode = storage.get_inode(child_inode_id).ok().flatten();
            dir_entries.push(ReadDirEntry {
                name,
                inode_id: child_inode_id,
                kind: child_inode.as_ref().map(|i| i.kind),
                attrs: child_inode.as_ref().map(|i| i.attrs.clone()),
            });
        }

        let (group_id, mount_epoch) = self.mount_hints_for_mount(parent_inode.mount_id);
        let route_epoch = self.authoritative_route_epoch().await;
        self.success_with_route_epoch(
            &req.ctx,
            ReadDirOutput {
                entries: dir_entries,
                next_cursor_key: next_cursor_key.unwrap_or_default(),
                eof,
            },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_open(&self, req: OpenInput) -> CoreResult<OpenOutput> {
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let inode = match storage.get_inode(req.inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", req.inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        let _flags = req.flags;
        let (group_id, mount_epoch) = self.mount_hints_for_mount(inode.mount_id);
        let route_epoch = self.authoritative_route_epoch().await;
        self.success_with_route_epoch(
            &req.ctx,
            OpenOutput { file_handle: 0 },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_truncate(&self, req: TruncateInput) -> CoreResult<TruncateOutput> {
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let inode = match storage.get_inode(req.inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", req.inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        if !inode.kind.is_file() {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", req.inode_id)),
                None,
                None,
            );
        }

        let (group_id, mount_epoch) = self.mount_hints_for_mount(inode.mount_id);

        let lease_id = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };
        if let Err(errno) = self
            .inode_lease_manager
            .validate_lease(req.inode_id, lease_id, req.lease_epoch)
        {
            return self.fatal_fs_failure(
                &req.ctx,
                errno,
                format!(
                    "Lease validation failed for truncate: inode={}, lease_id={:?}",
                    req.inode_id, lease_id
                ),
                group_id,
                mount_epoch,
            );
        }

        let current_size = inode.attrs.size;
        if req.new_size > current_size {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::NotSupported(format!(
                    "Truncate grow not supported: current_size={}, new_size={}",
                    current_size, req.new_size
                )),
                group_id,
                mount_epoch,
            );
        }
        if req.new_size == current_size {
            return self.success(
                &req.ctx,
                TruncateOutput { new_size: req.new_size },
                group_id,
                mount_epoch,
            );
        }

        let route_ctx = match self.route_ctx_for_write(&req.ctx, CoreWriteOp::SetAttr, &[req.inode_id], req.freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(route_ctx.namespace_owner_group_id.as_raw()),
                    Some(route_ctx.mount_epoch),
                );
            }
        };

        let result = match self
            .propose_fs_write_command(
                CoreWriteOp::SetAttr,
                Command::Truncate {
                    dedup,
                    inode_id: req.inode_id,
                    new_size: req.new_size,
                    lease_id,
                    lease_epoch: req.lease_epoch,
                },
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(route_ctx.namespace_owner_group_id.as_raw()),
                    Some(route_ctx.mount_epoch),
                );
            }
        };

        match result {
            FsCommandResult::Ok(_) => self.success(
                &req.ctx,
                TruncateOutput { new_size: req.new_size },
                Some(route_ctx.namespace_owner_group_id.as_raw()),
                Some(route_ctx.mount_epoch),
            ),
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                &req.ctx,
                err.errno,
                err.message,
                Some(route_ctx.namespace_owner_group_id.as_raw()),
                Some(route_ctx.mount_epoch),
            ),
        }
    }

    pub(crate) async fn execute_set_xattr(&self, req: SetXattrInput) -> CoreResult<SetXattrOutput> {
        let route_ctx = match self.route_ctx_for_write(&req.ctx, CoreWriteOp::SetAttr, &[req.inode_id], req.freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(route_ctx.namespace_owner_group_id.as_raw()),
                    Some(route_ctx.mount_epoch),
                );
            }
        };

        let result = match self
            .propose_fs_write_command(
                CoreWriteOp::SetAttr,
                Command::SetXattr {
                    dedup,
                    inode_id: req.inode_id,
                    name: req.name,
                    value: req.value,
                    create: req.create,
                    replace: req.replace,
                },
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(route_ctx.namespace_owner_group_id.as_raw()),
                    Some(route_ctx.mount_epoch),
                );
            }
        };

        match result {
            FsCommandResult::Ok(_) => self.success(
                &req.ctx,
                SetXattrOutput,
                Some(route_ctx.namespace_owner_group_id.as_raw()),
                Some(route_ctx.mount_epoch),
            ),
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                &req.ctx,
                err.errno,
                err.message,
                Some(route_ctx.namespace_owner_group_id.as_raw()),
                Some(route_ctx.mount_epoch),
            ),
        }
    }

    pub(crate) async fn execute_get_xattr(&self, req: GetXattrInput) -> CoreResult<GetXattrOutput> {
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let inode = match storage.get_inode(req.inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", req.inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        let value = match inode.xattrs.get(&req.name) {
            Some(value) => value.clone(),
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("xattr not found: {}", req.name)),
                    None,
                    None,
                );
            }
        };

        let (group_id, mount_epoch) = self.mount_hints_for_mount(inode.mount_id);
        self.success(&req.ctx, GetXattrOutput { value }, group_id, mount_epoch)
    }

    pub(crate) async fn execute_list_xattr(&self, req: ListXattrInput) -> CoreResult<ListXattrOutput> {
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let inode = match storage.get_inode(req.inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", req.inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        let names = inode.xattrs.keys().cloned().collect::<Vec<_>>();
        let (group_id, mount_epoch) = self.mount_hints_for_mount(inode.mount_id);
        self.success(&req.ctx, ListXattrOutput { names }, group_id, mount_epoch)
    }

    pub(crate) async fn execute_remove_xattr(&self, req: RemoveXattrInput) -> CoreResult<RemoveXattrOutput> {
        let route_ctx = match self.route_ctx_for_write(&req.ctx, CoreWriteOp::SetAttr, &[req.inode_id], req.freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(route_ctx.namespace_owner_group_id.as_raw()),
                    Some(route_ctx.mount_epoch),
                );
            }
        };

        let result = match self
            .propose_fs_write_command(
                CoreWriteOp::SetAttr,
                Command::RemoveXattr {
                    dedup,
                    inode_id: req.inode_id,
                    name: req.name,
                },
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(route_ctx.namespace_owner_group_id.as_raw()),
                    Some(route_ctx.mount_epoch),
                );
            }
        };

        match result {
            FsCommandResult::Ok(_) => self.success(
                &req.ctx,
                RemoveXattrOutput,
                Some(route_ctx.namespace_owner_group_id.as_raw()),
                Some(route_ctx.mount_epoch),
            ),
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                &req.ctx,
                err.errno,
                err.message,
                Some(route_ctx.namespace_owner_group_id.as_raw()),
                Some(route_ctx.mount_epoch),
            ),
        }
    }

    pub(crate) fn write_session_for_handle(&self, file_handle: u64) -> Option<crate::write_session::WriteSession> {
        self.write_session_manager.get_session(file_handle)
    }

    pub(crate) async fn execute_release(&self, req: ReleaseSessionInput) -> CoreResult<ReleaseSessionOutput> {
        let session = match self.write_session_manager.get_session(req.file_handle) {
            Some(session) => session,
            None => {
                // Release is best-effort cleanup and intentionally idempotent for replay safety.
                let route_epoch = self.authoritative_route_epoch().await;
                return self.success_with_route_epoch(&req.ctx, ReleaseSessionOutput, None, None, route_epoch);
            }
        };

        self.inode_lease_manager
            .release(session.inode_id, session.lease_id, session.lease_epoch);
        self.write_session_manager.remove_session(req.file_handle);

        let (group_id, mount_epoch) = self.mount_hints_for_mount(session.mount_id);
        let route_epoch = self.authoritative_route_epoch().await;
        self.success_with_route_epoch(&req.ctx, ReleaseSessionOutput, group_id, mount_epoch, route_epoch)
    }

    pub(crate) async fn execute_get_file_layout(&self, req: GetFileLayoutInput) -> CoreResult<GetFileLayoutOutput> {
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let inode = match storage.get_inode(req.inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", req.inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => {
                return self.failure_from_error(&req.ctx, err, None, None);
            }
        };

        if !inode.kind.is_file() {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", req.inode_id)),
                None,
                None,
            );
        }

        let (group_id, mount_epoch) = match self.validate_mount_epoch_for_mount(&req.ctx, req.freshness, inode.mount_id)
        {
            Ok(hints) => hints,
            Err(err) => return Err(err),
        };

        let route_epoch = match self
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "GetFileLayout")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let extents = match &inode.data {
            types::fs::InodeData::File { extents, .. } => extents.clone(),
            _ => Vec::new(),
        };

        let filtered_extents: Vec<Extent> = if let Some(range) = req.range {
            extents
                .into_iter()
                .filter(|e| {
                    let extent_end = e.file_offset + e.len;
                    let range_end = range.offset + range.len;
                    e.file_offset < range_end && extent_end > range.offset
                })
                .collect()
        } else {
            extents
        };

        let locations: Vec<FileBlockLocation> = filtered_extents
            .iter()
            .map(|extent| FileBlockLocation {
                block_id: extent.block_id,
                file_offset: extent.file_offset,
                len: extent.len,
                workers: Vec::new(),
                worker_epoch: None,
            })
            .collect();

        self.success_with_route_epoch(
            &req.ctx,
            GetFileLayoutOutput {
                extents: filtered_extents,
                file_size: inode.attrs.size,
                locations,
            },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_renew_inode_lease(&self, req: RenewLeaseInput) -> CoreResult<RenewLeaseOutput> {
        let file_handle = req.file_handle;

        let session = match self.write_session_manager.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "write session not found for handle={}; reopen and replay RenewWriteSessionLease",
                        file_handle,
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_id, mount_epoch) =
            match self.validate_mount_epoch_for_mount(&req.ctx, req.freshness, session.mount_id) {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let lease_id_typed = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };

        if lease_id_typed != session.lease_id || req.lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!(
                    "lease/session mismatch: expected lease_id={:?} lease_epoch={}, got lease_id={:?} lease_epoch={}; reopen and replay RenewWriteSessionLease",
                    session.lease_id,
                    session.lease_epoch,
                    lease_id_typed,
                    req.lease_epoch,
                ),
                group_id,
                mount_epoch,
            );
        }

        let expires_at_ms = match self
            .inode_lease_manager
            .renew(session.inode_id, lease_id_typed, req.lease_epoch)
        {
            Ok(expires) => expires,
            Err(_) => {
                return self.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionExpired,
                    RpcErrorCode::Fencing,
                    format!(
                        "lease renewal rejected for handle={}; session expired, reopen and replay RenewWriteSessionLease",
                        file_handle,
                    ),
                    group_id,
                    mount_epoch,
                );
            }
        };

        let route_epoch = self.authoritative_route_epoch().await;
        self.success_with_route_epoch(
            &req.ctx,
            RenewLeaseOutput { expires_at_ms },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_open_write(&self, req: OpenWriteInput) -> CoreResult<OpenWriteOutput> {
        let caller_ctx = &req.ctx.caller;
        let inode_id = req.inode_id;

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => {
                return self.failure_from_error(&req.ctx, err, None, None);
            }
        };

        if !inode.kind.is_file() {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
                None,
            );
        }

        let (group_id, mount_epoch) = match self.validate_mount_epoch_for_mount(&req.ctx, req.freshness, inode.mount_id)
        {
            Ok(hints) => hints,
            Err(err) => return Err(err),
        };

        let route_epoch = match self
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "OpenWriteByPath")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let mode = req.mode;
        let base_size = match mode {
            crate::inode_lease::WriteMode::Append => inode.attrs.size,
            crate::inode_lease::WriteMode::Write => 0,
        };

        let current_lease_epoch = match &inode.data {
            types::fs::InodeData::File { lease_epoch, .. } => *lease_epoch,
            _ => None,
        };

        let (lease_id, lease_epoch, expires_at_ms) = match self.inode_lease_manager.try_acquire(
            inode_id,
            caller_ctx.client.client_id,
            Some(caller_ctx.client.call_id),
            mode,
            current_lease_epoch,
        ) {
            Ok(result) => result,
            Err(FsErrorCode::EBusy) => {
                return self.fatal_fs_failure(
                    &req.ctx,
                    FsErrorCode::EBusy,
                    format!("File already has an active write lease: {}", inode_id),
                    group_id,
                    mount_epoch,
                );
            }
            Err(e) => {
                return self.fatal_fs_failure(
                    &req.ctx,
                    e,
                    format!("Failed to acquire lease: {}", inode_id),
                    group_id,
                    mount_epoch,
                );
            }
        };

        let open_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let data_handle_id = DataHandleId::new(inode_id.as_raw());
        let desired_len = req.desired_len.unwrap_or(4 * 1024 * 1024);
        let block_size = 4 * 1024 * 1024;
        let num_blocks = ((desired_len + block_size - 1) / block_size).max(1).min(10);

        let worker_manager = match self.worker_manager.as_ref() {
            Some(worker_manager) => worker_manager,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Worker manager not available".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };

        let mut write_targets = Vec::new();
        let mut write_targets_proto = Vec::new();
        for i in 0..num_blocks {
            let block_index = BlockIndex::new(i as u32);
            let block_id = BlockId::new(data_handle_id, block_index);
            let placement = match worker_manager.select_workers_for_placement(3, None) {
                Ok(placement) => placement,
                Err(e) => {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::Internal(format!("Failed to select workers: {}", e)),
                        group_id,
                        mount_epoch,
                    );
                }
            };

            let mut worker_endpoints = Vec::new();
            let mut worker_endpoints_proto = Vec::new();
            for worker_id in placement.all_workers() {
                if let Some(worker_info) = worker_manager.get_worker(worker_id) {
                    let endpoint = format!("{}:{}", worker_info.address, 0);
                    worker_endpoints.push(WorkerHint {
                        worker_id,
                        endpoint: endpoint.clone(),
                        net_transport_kind: worker_info.net_transport_kind as i32,
                        worker_epoch: worker_info.worker_epoch,
                    });
                    worker_endpoints_proto.push(proto::common::WorkerEndpointInfoProto {
                        worker_id: worker_id.as_raw(),
                        endpoint,
                        net_transport_kind: worker_info.net_transport_kind as i32,
                        worker_epoch: worker_info.worker_epoch,
                    });
                }
            }

            let target_token = FencingToken {
                block_id,
                owner: caller_ctx.client.client_id,
                epoch: lease_epoch,
            };
            write_targets.push(WriteTarget {
                block_id,
                worker_endpoints,
                fencing_token: target_token,
            });
            write_targets_proto.push(proto::metadata::WriteTargetProto {
                block_id: Some(proto::common::BlockIdProto {
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                }),
                worker_endpoints: worker_endpoints_proto,
                fencing_token: Some(proto::common::FencingTokenProto {
                    block_id: Some(proto::common::BlockIdProto {
                        data_handle_id: block_id.data_handle_id.as_raw(),
                        block_index: block_id.index.as_raw(),
                    }),
                    owner: caller_ctx.client.client_id.as_raw(),
                    epoch: lease_epoch,
                }),
            });
        }

        let session_token = FencingToken {
            block_id: BlockId::new(data_handle_id, BlockIndex::new(0)),
            owner: caller_ctx.client.client_id,
            epoch: lease_epoch,
        };
        let file_handle = self.write_session_manager.create_session(
            inode_id,
            inode.mount_id,
            lease_id,
            lease_epoch,
            session_token,
            open_epoch,
            base_size,
            mode,
            write_targets_proto,
            crate::write_session::WriterIdentity {
                client_id: caller_ctx.client.client_id,
                call_id: caller_ctx.client.call_id,
            },
        );

        self.success_with_route_epoch(
            &req.ctx,
            OpenWriteOutput {
                session_key: SessionKey {
                    file_handle,
                    lease_id,
                    lease_epoch,
                    open_epoch,
                    fencing_token: session_token,
                },
                write_targets,
                base_size,
                expires_at_ms,
            },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_close_write(&self, req: CloseWriteInput) -> CoreResult<CloseWriteOutput> {
        let caller_ctx = &req.ctx.caller;
        let file_handle = req.file_handle;

        let session = match self.write_session_manager.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "write session not found for handle={}; reopen and replay CloseWriteSession",
                        file_handle,
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_id, mount_epoch) =
            match self.validate_mount_epoch_for_mount(&req.ctx, req.freshness, session.mount_id) {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let route_epoch = match self
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "CloseWriteSession")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        if let Some(worker_manager) = self.worker_manager.as_ref() {
            for target in &session.write_targets {
                for endpoint in &target.worker_endpoints {
                    let worker_id = WorkerId::new(endpoint.worker_id);
                    let current_epoch = worker_manager.get_descriptor(worker_id).map(|d| d.worker_epoch);
                    if current_epoch != Some(endpoint.worker_epoch) {
                        let hint = Self::worker_refresh_hint_from_session(&session, current_epoch, true);
                        return self.need_refresh_failure_with_hint(
                            &req.ctx,
                            RpcErrorCode::WorkerEpochMismatch,
                            RefreshReason::WorkerEpochMismatch,
                            format!(
                                "worker_epoch mismatch for worker_id={}: client/session={}, server={:?}; {}",
                                endpoint.worker_id,
                                endpoint.worker_epoch,
                                current_epoch,
                                Self::replay_hint("CloseWriteSession")
                            ),
                            group_id,
                            mount_epoch,
                            route_epoch,
                            Some(hint),
                        );
                    }
                }
            }
        }

        let lease_id_typed = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };
        let request_lease_epoch = req.lease_epoch;

        if lease_id_typed != session.lease_id || request_lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!(
                    "lease/session mismatch: expected lease_id={:?} lease_epoch={}, got lease_id={:?} lease_epoch={}; reopen and replay CloseWriteSession",
                    session.lease_id,
                    session.lease_epoch,
                    lease_id_typed,
                    request_lease_epoch,
                ),
                group_id,
                mount_epoch,
            );
        }

        let req_token = match req.fencing_token.as_ref() {
            Some(token) => token,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "missing fencing_token for handle={}; reopen and replay CloseWriteSession",
                        file_handle,
                    ),
                    group_id,
                    mount_epoch,
                );
            }
        };
        if !Self::fencing_token_matches_session(&session, req_token) {
            return self.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!(
                    "fencing_token mismatch for handle={}; reopen and replay CloseWriteSession",
                    file_handle,
                ),
                group_id,
                mount_epoch,
            );
        }

        if req.open_epoch != session.open_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::EpochMismatch,
                format!(
                    "open_epoch mismatch: expected {}, got {}; reopen and replay CloseWriteSession",
                    session.open_epoch, req.open_epoch,
                ),
                group_id,
                mount_epoch,
            );
        }

        if self
            .inode_lease_manager
            .validate_lease(session.inode_id, lease_id_typed, request_lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionExpired,
                RpcErrorCode::Fencing,
                format!(
                    "lease validation rejected for handle={}; session expired, reopen and replay CloseWriteSession",
                    file_handle,
                ),
                group_id,
                mount_epoch,
            );
        }

        let extents = req.intent.extents;
        if session.mode == crate::inode_lease::WriteMode::Append {
            let mut expected_offset = session.base_size;
            for extent in &extents {
                if extent.file_offset != expected_offset {
                    return self.fatal_fs_failure(
                        &req.ctx,
                        FsErrorCode::EInval,
                        format!(
                            "Extent file_offset mismatch: expected {}, got {}",
                            expected_offset, extent.file_offset
                        ),
                        group_id,
                        mount_epoch,
                    );
                }
                expected_offset += extent.len;
            }
            let expected_final_size = session.base_size + extents.iter().map(|e| e.len).sum::<u64>();
            if req.intent.final_size != expected_final_size {
                return self.fatal_fs_failure(
                    &req.ctx,
                    FsErrorCode::EInval,
                    format!(
                        "Final size mismatch: expected {}, got {} (append mode)",
                        expected_final_size, req.intent.final_size
                    ),
                    group_id,
                    mount_epoch,
                );
            }
        } else {
            for extent in &extents {
                if extent.file_offset + extent.len > req.intent.final_size {
                    return self.fatal_fs_failure(
                        &req.ctx,
                        FsErrorCode::EInval,
                        format!(
                            "Extent extends beyond final_size: extent_end={}, final_size={}",
                            extent.file_offset + extent.len,
                            req.intent.final_size
                        ),
                        group_id,
                        mount_epoch,
                    );
                }
            }
        }

        let ctx = match self.route_fs_write_ctx(
            CoreWriteOp::SetAttr,
            &[session.inode_id],
            req.freshness.mount_epoch.or(req.ctx.caller.mount_epoch),
        ) {
            Ok(ctx) => ctx,
            Err(err) => {
                return self.failure_from_error(&req.ctx, err, group_id, mount_epoch);
            }
        };

        let dedup = match self.dedup_key(caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        let command = Command::CloseWrite {
            dedup,
            inode_id: session.inode_id,
            extents,
            final_size: req.intent.final_size,
            lease_id: session.lease_id,
            open_epoch: session.open_epoch,
            lease_epoch: request_lease_epoch,
        };
        if let Err(err) = self.propose_fs_write_command(CoreWriteOp::SetAttr, command).await {
            return self.failure_from_error(
                &req.ctx,
                err,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
        }

        self.inode_lease_manager
            .release(session.inode_id, lease_id_typed, session.lease_epoch);
        self.write_session_manager.remove_session(file_handle);

        self.success_with_route_epoch(
            &req.ctx,
            CloseWriteOutput {
                committed_size: req.intent.final_size,
                file_version: None,
            },
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
            route_epoch,
        )
    }

    pub(crate) async fn execute_fsync(&self, req: FsyncBarrierInput) -> CoreResult<FsyncBarrierOutput> {
        let caller_ctx = &req.ctx.caller;
        let inode_id = req.inode_id;

        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        if !inode.kind.is_file() {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
                None,
            );
        }

        let (group_id, mount_epoch) = match self.validate_mount_epoch_for_mount(&req.ctx, req.freshness, inode.mount_id)
        {
            Ok(hints) => hints,
            Err(err) => return Err(err),
        };

        let route_epoch = match self
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "FsyncSession")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let mut lease_id = req.lease_id;
        let mut lease_epoch = req.lease_epoch.unwrap_or(0);
        let presented_token = req.fencing_token;
        let mut fencing_token = presented_token.as_ref().and_then(|token| {
            token.block_id.map(|block_id| FencingToken {
                block_id,
                owner: types::ids::ClientId::new(token.owner),
                epoch: token.epoch,
            })
        });
        let mut commit_workers: Vec<proto::common::WorkerEndpointInfoProto> = Vec::new();
        let mut target_size = req.target_size.unwrap_or(inode.attrs.size);

        if let Some(handle) = req.file_handle {
            let session = match self.write_session_manager.get_session(handle) {
                Some(session) => session,
                None => {
                    return self.session_terminal_failure(
                        &req.ctx,
                        RefreshReason::SessionInvalid,
                        RpcErrorCode::Fencing,
                        format!(
                            "write session not found for handle={}; reopen and replay FsyncSession",
                            handle,
                        ),
                        group_id,
                        mount_epoch,
                    );
                }
            };

            if session.inode_id != inode_id {
                return self.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::EpochMismatch,
                    format!(
                        "file_handle={} does not match inode={}; reopen and replay FsyncSession",
                        handle, inode_id,
                    ),
                    group_id,
                    mount_epoch,
                );
            }

            if lease_id.is_none() {
                lease_id = Some(session.lease_id);
            }
            if lease_epoch == 0 {
                lease_epoch = session.lease_epoch;
            }
            if presented_token.is_none() {
                fencing_token = Some(session.fencing_token);
            } else if let Some(token) = presented_token.as_ref() {
                if !Self::fencing_token_matches_session(&session, token) {
                    return self.session_terminal_failure(
                        &req.ctx,
                        RefreshReason::SessionInvalid,
                        RpcErrorCode::Fencing,
                        format!(
                            "fencing_token mismatch for handle={}; reopen and replay FsyncSession",
                            handle,
                        ),
                        group_id,
                        mount_epoch,
                    );
                }
            }

            for wt in &session.write_targets {
                commit_workers.extend(wt.worker_endpoints.clone());
            }
            target_size = target_size.max(session.base_size).max(session.last_written);

            if let Some(client_worker_epoch) = req.freshness.worker_epoch {
                let mismatch = session
                    .write_targets
                    .iter()
                    .flat_map(|t| t.worker_endpoints.iter())
                    .any(|ep| ep.worker_epoch != client_worker_epoch);
                if mismatch {
                    let server_worker_epoch = session
                        .write_targets
                        .iter()
                        .flat_map(|t| t.worker_endpoints.iter())
                        .map(|ep| ep.worker_epoch)
                        .max();
                    let hint = Self::worker_refresh_hint_from_session(&session, server_worker_epoch, false);
                    return self.need_refresh_failure_with_hint(
                        &req.ctx,
                        RpcErrorCode::WorkerEpochMismatch,
                        RefreshReason::WorkerEpochMismatch,
                        format!(
                            "worker_epoch mismatch: client={}, session_targets differ; {}",
                            client_worker_epoch,
                            Self::replay_hint("FsyncSession")
                        ),
                        group_id,
                        mount_epoch,
                        route_epoch,
                        Some(hint),
                    );
                }
            }
        }

        let lease_id_typed = match lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };

        if self
            .inode_lease_manager
            .validate_lease(inode_id, lease_id_typed, lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionExpired,
                RpcErrorCode::Fencing,
                format!(
                    "lease validation rejected for inode={}; session expired, reopen and replay FsyncSession",
                    inode_id,
                ),
                group_id,
                mount_epoch,
            );
        }

        if commit_workers.is_empty() {
            target_size = target_size.max(inode.attrs.size);
        } else {
            let mut tasks = Vec::new();
            let effective_route_epoch = req.freshness.route_epoch.or(req.ctx.route_epoch).unwrap_or(0);
            for ep in commit_workers {
                let endpoint = format!("http://{}", ep.endpoint);
                let header_client = proto::common::ClientInfoProto {
                    call_id: caller_ctx.client.call_id.to_string(),
                    client_id: caller_ctx.client.client_id.as_raw(),
                    client_name: caller_ctx.client.client_name.clone().unwrap_or_default(),
                };
                let commit_req = CommitWriteRequestProto {
                    header: Some(proto::worker::DataRequestHeaderProto {
                        client: Some(header_client.clone()),
                        traceparent: req.ctx.traceparent.clone().unwrap_or_default(),
                    }),
                    block_id: fencing_token
                        .as_ref()
                        .map(|t| proto::common::BlockIdProto {
                            data_handle_id: t.block_id.data_handle_id.as_raw(),
                            block_index: t.block_id.index.as_raw(),
                        })
                        .or_else(|| {
                            Some(proto::common::BlockIdProto {
                                data_handle_id: inode.current_data_handle_id.as_raw(),
                                block_index: 0,
                            })
                        }),
                    token: fencing_token.as_ref().map(|t| proto::common::FencingTokenProto {
                        block_id: Some(proto::common::BlockIdProto {
                            data_handle_id: t.block_id.data_handle_id.as_raw(),
                            block_index: t.block_id.index.as_raw(),
                        }),
                        owner: t.owner.as_raw(),
                        epoch: t.epoch,
                    }),
                    lease_epoch,
                    route_epoch: effective_route_epoch,
                    worker_epoch: ep.worker_epoch,
                    file_version: 0,
                    committed_length: target_size,
                };
                if let Some(hook) = self.worker_commit_hook.lock().unwrap().clone() {
                    let req_clone = commit_req.clone();
                    tasks.push(tokio::spawn(async move {
                        Ok::<proto::worker::CommitWriteResponseProto, MetadataError>(hook(req_clone))
                    }));
                    continue;
                }
                let mut client = match WorkerDataServiceClient::connect(endpoint.clone()).await {
                    Ok(client) => client,
                    Err(e) => {
                        return self.failure_from_error(
                            &req.ctx,
                            MetadataError::ServiceUnavailable(format!("Failed to connect worker {}: {}", endpoint, e)),
                            group_id,
                            mount_epoch,
                        );
                    }
                };
                tasks.push(tokio::spawn(async move {
                    client
                        .commit_write(commit_req)
                        .await
                        .map(|resp| resp.into_inner())
                        .map_err(|status| {
                            MetadataError::ServiceUnavailable(format!("Worker commit failed: {}", status))
                        })
                }));
            }

            for t in tasks {
                let inner = match t.await {
                    Ok(Ok(resp)) => resp,
                    Ok(Err(err)) => {
                        return self.failure_from_error(&req.ctx, err, group_id, mount_epoch);
                    }
                    Err(e) => {
                        return self.failure_from_error(
                            &req.ctx,
                            MetadataError::ServiceUnavailable(format!("Join error: {}", e)),
                            group_id,
                            mount_epoch,
                        );
                    }
                };
                if let Some(err) = inner.header.and_then(|h| h.error) {
                    let cerr = canonical_from_error_detail(err);
                    return self.failure_from_canonical_with_route_epoch(
                        &req.ctx,
                        cerr,
                        group_id,
                        mount_epoch,
                        route_epoch,
                    );
                }
            }
        }

        let mut attrs = inode.attrs.clone();
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        attrs.size = attrs.size.max(target_size);
        attrs.update_mtime_ctime(now_ms);

        let ctx = match self.route_fs_write_ctx(
            CoreWriteOp::SetAttr,
            &[inode_id],
            req.freshness.mount_epoch.or(req.ctx.caller.mount_epoch),
        ) {
            Ok(ctx) => ctx,
            Err(err) => return self.failure_from_error(&req.ctx, err, group_id, mount_epoch),
        };

        let dedup = if caller_ctx.client.client_id.as_raw() == 0 {
            DedupKey::system()
        } else {
            match self.dedup_key(caller_ctx) {
                Ok(k) => k,
                Err(err) => {
                    return self.failure_from_error(
                        &req.ctx,
                        err,
                        Some(ctx.namespace_owner_group_id.as_raw()),
                        Some(ctx.mount_epoch),
                    );
                }
            }
        };
        let command = Command::SetAttr {
            dedup,
            inode_id,
            mask: 1 | 32,
            attrs,
        };
        if let Err(err) = self.propose_fs_write_command(CoreWriteOp::SetAttr, command).await {
            return self.failure_from_error(
                &req.ctx,
                err,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
        }

        self.success_with_route_epoch(
            &req.ctx,
            FsyncBarrierOutput,
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
            route_epoch,
        )
    }

    pub(crate) async fn execute_hsync(&self, req: FsyncBarrierInput) -> CoreResult<FsyncBarrierOutput> {
        self.execute_fsync(req).await
    }

    pub(crate) async fn execute_hflush(&self, req: FsyncBarrierInput) -> CoreResult<FsyncBarrierOutput> {
        self.execute_fsync(req).await
    }

    pub(crate) async fn execute_stat_fs(&self, req: StatFsInput) -> CoreResult<StatFsOutput> {
        self.failure_from_error(
            &req.ctx,
            MetadataError::NotSupported("StatFs not yet implemented".to_string()),
            None,
            None,
        )
    }

    pub(crate) async fn execute_access(&self, req: AccessInput) -> CoreResult<AccessOutput> {
        let _ = req.mode;
        self.failure_from_error(
            &req.ctx,
            MetadataError::NotSupported("Access not yet implemented".to_string()),
            None,
            None,
        )
    }

    pub(crate) async fn execute_symlink(&self, req: SymlinkInput) -> CoreResult<SymlinkOutput> {
        self.failure_from_error(
            &req.ctx,
            MetadataError::NotSupported("Symlink not yet implemented".to_string()),
            None,
            None,
        )
    }

    pub(crate) async fn execute_readlink(&self, req: ReadlinkInput) -> CoreResult<ReadlinkOutput> {
        self.failure_from_error(
            &req.ctx,
            MetadataError::NotSupported("Readlink not yet implemented".to_string()),
            None,
            None,
        )
    }

    pub(crate) async fn execute_link(&self, req: LinkInput) -> CoreResult<LinkOutput> {
        self.failure_from_error(
            &req.ctx,
            MetadataError::NotSupported("Link not yet implemented".to_string()),
            None,
            None,
        )
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
        op: CoreWriteOp,
        parent_inode_ids: &[InodeId],
        client_mount_epoch: Option<u64>,
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
        if let Some(client_mount_epoch) = client_mount_epoch {
            let current_mount_epoch = mount_entry.config_version;
            if client_mount_epoch != current_mount_epoch {
                return Err(MetadataError::MountEpochMismatch {
                    expected: current_mount_epoch,
                    got: client_mount_epoch,
                    mount_id: Some(mount_id),
                });
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

    /// Propose FS write command to Raft and update metrics.
    /// This is the unified entry point for all FS write operations that write to Raft.
    /// It ensures we can track and guard against write amplification.
    async fn propose_fs_write_command(&self, op: CoreWriteOp, command: Command) -> MetadataResult<FsCommandResult> {
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
                CoreWriteOp::Create => {
                    metrics.fs_raft_appends_create.fetch_add(1, Ordering::Relaxed);
                }
                CoreWriteOp::Mkdir => {
                    metrics.fs_raft_appends_mkdir.fetch_add(1, Ordering::Relaxed);
                }
                CoreWriteOp::Unlink => {
                    metrics.fs_raft_appends_unlink.fetch_add(1, Ordering::Relaxed);
                }
                CoreWriteOp::Rmdir => {
                    metrics.fs_raft_appends_rmdir.fetch_add(1, Ordering::Relaxed);
                }
                CoreWriteOp::Rename => {
                    metrics.fs_raft_appends_rename.fetch_add(1, Ordering::Relaxed);
                }
                CoreWriteOp::SetAttr => {
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
}
