// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Shared filesystem core semantics used by path service RPC handlers.

mod freshness;
mod mutation;
mod read;
mod write_session;
mod write_session_coordinator;

use super::domain::{
    CoreFailure, CoreResult, CoreSuccess, Freshness, InodeOwner, PresentedFencingToken, RequestContext,
};
use crate::error::{to_canonical_fs, MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::raft::{AppDataResponse, AppRaftNode, Command, DedupKey, FsCommandResult, RocksDBStorage};
use crate::state::StateStore;
use common::error::canonical::{
    CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshHint, RefreshReason,
};
use common::header::{RequestHeader, RpcErrorCode};
use proto::worker::CommitWriteRequestProto;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tracing::debug;
use types::fs::{FsErrorCode, InodeId};
use types::ids::{MountId, ShardGroupId};
use types::RaftLogId;

use freshness::{FreshnessValidator, StaleStateStatus};

pub(crate) type WorkerCommitHook =
    Arc<dyn Fn(CommitWriteRequestProto) -> proto::worker::CommitWriteResponseProto + Send + Sync>;
pub type SharedWorkerCommitHook = Arc<Mutex<Option<WorkerCommitHook>>>;

#[derive(Clone, Debug)]
struct RoutedFsWriteCtx {
    mount_id: MountId,
    namespace_owner_group_id: ShardGroupId,
    mount_epoch: u64,
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

pub(crate) struct FsCore {
    mount_table: Arc<MountTable>,
    freshness_validator: FreshnessValidator,
    storage: Option<Arc<RocksDBStorage>>,
    raft_node: Option<Arc<AppRaftNode>>,
    metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
    write_session_manager: Arc<crate::write_session::WriteSessionManager>,
    worker_manager: Option<Arc<crate::worker::WorkerManager>>,
    inode_lease_manager: Arc<crate::inode_lease::InodeLeaseManager>,
    worker_commit_hook: SharedWorkerCommitHook,
}

impl FsCore {
    pub(crate) fn new(
        state_store: Arc<dyn StateStore>,
        mount_table: Arc<MountTable>,
        write_session_manager: Arc<crate::write_session::WriteSessionManager>,
        inode_lease_manager: Arc<crate::inode_lease::InodeLeaseManager>,
        worker_commit_hook: SharedWorkerCommitHook,
    ) -> Self {
        Self {
            freshness_validator: FreshnessValidator::new(Arc::clone(&state_store), Arc::clone(&mount_table)),
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

    pub(crate) fn set_storage(&mut self, storage: Arc<RocksDBStorage>) {
        self.storage = Some(storage);
    }

    pub(crate) fn set_raft_node(&mut self, raft_node: Arc<AppRaftNode>) {
        self.raft_node = Some(raft_node);
    }

    pub(crate) fn set_metrics(&mut self, metrics: Arc<crate::metrics::MetadataMetrics>) {
        self.metrics = Some(metrics);
    }

    pub(crate) fn set_worker_manager(&mut self, worker_manager: Arc<crate::worker::WorkerManager>) {
        self.worker_manager = Some(worker_manager);
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
        FreshnessValidator::state_id_from_ctx(ctx)
    }

    async fn authoritative_route_epoch(&self) -> Option<u64> {
        self.freshness_validator.authoritative_route_epoch().await
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

    pub(crate) fn mount_hints_for_mount(&self, mount_id: MountId) -> (Option<u64>, Option<u64>) {
        self.freshness_validator.mount_hints_for_mount(mount_id)
    }

    fn validate_mount_epoch_for_mount(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
    ) -> Result<(Option<u64>, Option<u64>), CoreFailure> {
        self.freshness_validator
            .validate_mount_epoch_for_mount(ctx, freshness, mount_id)
    }

    pub(crate) fn validate_mount_freshness(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
    ) -> Result<(Option<u64>, Option<u64>), CoreFailure> {
        self.freshness_validator
            .validate_mount_freshness(ctx, freshness, mount_id)
    }

    async fn validate_route_epoch(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
        intent: &str,
    ) -> Result<Option<u64>, CoreFailure> {
        self.freshness_validator
            .validate_route_epoch(ctx, freshness, group_id, mount_epoch, intent)
            .await
    }

    fn validate_stale_state(
        &self,
        ctx: &RequestContext,
        last_applied: Option<RaftLogId>,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> Result<StaleStateStatus, CoreFailure> {
        self.freshness_validator
            .validate_stale_state(ctx, last_applied, group_id, mount_epoch)
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
        self.route_ctx_for_write_with_error_hints(req_ctx, op, parent_inode_ids, freshness, None, None)
    }

    fn route_ctx_for_write_with_error_hints(
        &self,
        req_ctx: &RequestContext,
        op: CoreWriteOp,
        parent_inode_ids: &[InodeId],
        freshness: Freshness,
        error_group_id: Option<u64>,
        error_mount_epoch: Option<u64>,
    ) -> Result<RoutedFsWriteCtx, CoreFailure> {
        let ctx = match self.route_fs_write_ctx(op, parent_inode_ids) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(CoreFailure::new(
                    to_canonical_fs(err),
                    error_group_id,
                    error_mount_epoch,
                    None,
                    Self::state_id_from_ctx(req_ctx),
                ));
            }
        };

        if let Err(failure) =
            self.freshness_validator
                .validate_routed_write_mount_epoch(req_ctx, freshness, ctx.mount_id)
        {
            if let Some(metrics) = &self.metrics {
                metrics
                    .fs_write_mount_epoch_mismatch_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Err(failure);
        }
        Ok(ctx)
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

    fn route_fs_write_ctx(&self, op: CoreWriteOp, parent_inode_ids: &[InodeId]) -> MetadataResult<RoutedFsWriteCtx> {
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetadataError::Internal("Storage not available".to_string()))?;

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

        let mount_entry = self
            .mount_table
            .get_mount(mount_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount not found: {:?}", mount_id)))?;

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
        })
    }

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

#[cfg(test)]
mod tests;
