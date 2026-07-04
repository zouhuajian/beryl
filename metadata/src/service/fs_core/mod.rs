// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Shared filesystem core semantics used by path service RPC handlers.

mod freshness;
mod mutation;
mod read;
mod write_session;

use super::core_util::{
    core_failure_from_metadata_error, fatal_fs_core_failure, need_refresh_core_failure, terminal_rpc_core_failure,
};
use super::domain::{CoreFailure, CoreResult, CoreSuccess, Freshness, PresentedFencingToken, RequestContext};
use crate::error::{MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::observe;
use crate::raft::{AppDataResponse, AppRaftNode, Command, DedupKey, FsCommandResult, RocksDBStorage};
use crate::state::StateStore;
use common::error::canonical::{RefreshHint, RefreshReason};
use common::header::{RequestHeader, RpcErrorCode};
use proto::worker::CommitWriteRequestProto;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::debug;
use types::fs::{FsErrorCode, InodeId};
use types::ids::MountId;
use types::{GroupName, GroupStateWatermark, RaftLogId};

use freshness::{FreshnessValidator, StaleStateStatus};

pub(crate) type WorkerCommitHook =
    Arc<dyn Fn(CommitWriteRequestProto) -> proto::worker::CommitWriteResponseProto + Send + Sync>;
pub type SharedWorkerCommitHook = Arc<Mutex<Option<WorkerCommitHook>>>;

#[derive(Clone, Debug)]
struct RoutedFsWriteCtx {
    mount_id: MountId,
    namespace_owner_group_name: GroupName,
    mount_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CoreWriteOp {
    Create,
    Mkdir,
    Unlink,
    DeleteEmptyDir,
    DeleteTree,
    Rename,
    SetAttr,
}

impl CoreWriteOp {
    fn metric_label(self) -> &'static str {
        match self {
            CoreWriteOp::Create => "create_file",
            CoreWriteOp::Mkdir => "create_directory",
            CoreWriteOp::Unlink => "delete_file",
            CoreWriteOp::DeleteEmptyDir => "delete_empty_dir",
            CoreWriteOp::DeleteTree => "delete_tree",
            CoreWriteOp::Rename => "rename",
            CoreWriteOp::SetAttr => "set_attr",
        }
    }
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
    _worker_commit_hook: SharedWorkerCommitHook,
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
            _worker_commit_hook: worker_commit_hook,
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

    async fn authoritative_route_epoch(&self) -> Option<u64> {
        self.freshness_validator.authoritative_route_epoch().await
    }

    fn response_state_for_success(&self, group_name: Option<&GroupName>) -> Vec<GroupStateWatermark> {
        let (Some(group_name), Some(raft_node)) = (group_name, self.raft_node.as_ref()) else {
            // A response without a known owner group cannot authorize a state cache advance.
            return Vec::new();
        };
        if !raft_node.is_leader() {
            return Vec::new();
        }
        raft_node
            .get_last_applied_state_id()
            .map(|state_id| GroupStateWatermark::new(group_name.clone(), state_id))
            .into_iter()
            .collect()
    }

    fn success<T>(
        &self,
        ctx: &RequestContext,
        payload: T,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> CoreResult<T> {
        self.success_with_route_epoch(ctx, payload, group_name, mount_epoch, None)
    }

    fn success_with_route_epoch<T>(
        &self,
        _ctx: &RequestContext,
        payload: T,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> CoreResult<T> {
        Ok(CoreSuccess {
            payload,
            group_name: group_name.clone(),
            mount_epoch,
            route_epoch,
            state: self.response_state_for_success(group_name.as_ref()),
        })
    }

    fn failure_from_error<T>(
        &self,
        ctx: &RequestContext,
        err: MetadataError,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> CoreResult<T> {
        self.failure_from_error_with_route_epoch(ctx, err, group_name, mount_epoch, None)
    }

    fn failure_from_error_with_route_epoch<T>(
        &self,
        ctx: &RequestContext,
        err: MetadataError,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> CoreResult<T> {
        Err(core_failure_from_metadata_error(
            ctx,
            err,
            group_name,
            mount_epoch,
            route_epoch,
        ))
    }

    fn require_worker_lookup_group(
        &self,
        ctx: &RequestContext,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        intent: &str,
    ) -> Result<GroupName, CoreFailure> {
        group_name.clone().ok_or_else(|| {
            core_failure_from_metadata_error(
                ctx,
                MetadataError::Internal(format!("{intent} worker lookup requires authoritative metadata group")),
                group_name,
                mount_epoch,
                route_epoch,
            )
        })
    }

    // Refresh failures must keep caller and server hint fields explicit.
    #[allow(clippy::too_many_arguments)]
    fn need_refresh_failure_with_hint<T>(
        &self,
        ctx: &RequestContext,
        rpc_code: RpcErrorCode,
        reason: RefreshReason,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        mut hint: Option<RefreshHint>,
    ) -> CoreResult<T> {
        if let Some(group_name_value) = &group_name {
            hint.get_or_insert_with(RefreshHint::default).group_name = Some(group_name_value.to_string());
        }
        if let Some(mount_epoch_value) = mount_epoch {
            hint.get_or_insert_with(RefreshHint::default).mount_epoch = Some(mount_epoch_value);
        }
        if let Some(route_epoch_value) = route_epoch {
            hint.get_or_insert_with(RefreshHint::default).route_epoch = Some(route_epoch_value);
        }

        Err(need_refresh_core_failure(
            ctx,
            rpc_code,
            reason,
            message,
            group_name.clone(),
            mount_epoch,
            route_epoch,
            hint,
        ))
    }

    fn fatal_fs_failure<T>(
        &self,
        ctx: &RequestContext,
        errno: FsErrorCode,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> CoreResult<T> {
        Err(fatal_fs_core_failure(ctx, errno, message, group_name, mount_epoch))
    }

    fn session_terminal_failure<T>(
        &self,
        ctx: &RequestContext,
        reason: RefreshReason,
        rpc_code: RpcErrorCode,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> CoreResult<T> {
        Err(terminal_rpc_core_failure(
            ctx,
            reason,
            rpc_code,
            message,
            group_name,
            mount_epoch,
        ))
    }

    fn replay_hint(intent: &str) -> String {
        format!("refresh metadata and reopen write handle, then replay {}", intent)
    }

    pub(crate) fn mount_hints_for_mount(&self, mount_id: MountId) -> (Option<GroupName>, Option<u64>) {
        self.freshness_validator.mount_hints_for_mount(mount_id)
    }

    fn validate_mount_epoch_for_mount(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
    ) -> Result<(Option<GroupName>, Option<u64>), CoreFailure> {
        self.freshness_validator
            .validate_mount_epoch_for_mount(ctx, freshness, mount_id)
    }

    async fn validate_route_epoch(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        intent: &str,
    ) -> Result<Option<u64>, CoreFailure> {
        self.freshness_validator
            .validate_route_epoch(ctx, freshness, group_name, mount_epoch, intent)
            .await
    }

    fn validate_stale_state(
        &self,
        ctx: &RequestContext,
        last_applied: Option<RaftLogId>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> Result<StaleStateStatus, CoreFailure> {
        self.freshness_validator
            .validate_stale_state(ctx, last_applied, group_name, mount_epoch)
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

        block_ok && token.owner == session.fencing_token.owner && token.epoch == session.fencing_token.epoch
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
        error_group_name: Option<GroupName>,
        error_mount_epoch: Option<u64>,
    ) -> Result<RoutedFsWriteCtx, CoreFailure> {
        let ctx = match self.route_fs_write_ctx(op, parent_inode_ids) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(core_failure_from_metadata_error(
                    req_ctx,
                    err,
                    error_group_name,
                    error_mount_epoch,
                    None,
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
            core_failure_from_metadata_error(
                req_ctx,
                MetadataError::Internal("Storage not available".to_string()),
                None,
                None,
                None,
            )
        })
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
            owner_group_name = %mount_entry.namespace_owner_group_name,
            mount_epoch = mount_entry.mount_epoch,
            "FS write routed to mount namespace owner group"
        );

        if let Some(ref metrics) = self.metrics {
            metrics
                .fs_write_routed_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(RoutedFsWriteCtx {
            mount_id,
            namespace_owner_group_name: mount_entry.namespace_owner_group_name,
            mount_epoch: mount_entry.mount_epoch,
        })
    }

    async fn propose_fs_write_command(&self, op: CoreWriteOp, command: Command) -> MetadataResult<FsCommandResult> {
        let started = Instant::now();
        let raft_node = self.raft_node.as_ref().ok_or_else(|| {
            let error = MetadataError::Internal("Raft node not available".to_string());
            observe::record_fs_op(
                op.metric_label(),
                "error",
                observe::metadata_error_kind(&error),
                started.elapsed().as_secs_f64(),
            );
            error
        })?;

        let dedup_key = command.dedup_key().clone();
        let fingerprint = command.fingerprint();

        if let Some(storage) = &self.storage {
            if let Some(existing) = storage.get_applied_result(&dedup_key).inspect_err(|error| {
                observe::record_fs_op(
                    op.metric_label(),
                    "error",
                    observe::metadata_error_kind(error),
                    started.elapsed().as_secs_f64(),
                );
            })? {
                if existing.fingerprint != fingerprint {
                    let error = MetadataError::InvalidArgument(format!(
                        "call_id {} reused with different command payload",
                        dedup_key.call_id
                    ));
                    observe::record_fs_op(
                        op.metric_label(),
                        "error",
                        observe::metadata_error_kind(&error),
                        started.elapsed().as_secs_f64(),
                    );
                    return Err(error);
                }
                let result = match existing.result {
                    AppDataResponse::Fs(res) => res,
                    _ => FsCommandResult::ok(),
                };
                record_fs_write_result(op, started, &result);
                return Ok(result);
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
                CoreWriteOp::DeleteEmptyDir => {
                    metrics.fs_raft_appends_directory_delete.fetch_add(1, Ordering::Relaxed);
                }
                CoreWriteOp::DeleteTree => {
                    metrics.fs_raft_appends_directory_delete.fetch_add(1, Ordering::Relaxed);
                }
                CoreWriteOp::Rename => {
                    metrics.fs_raft_appends_rename.fetch_add(1, Ordering::Relaxed);
                }
                CoreWriteOp::SetAttr => {
                    metrics.fs_raft_appends_setattr.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let response = match raft_node.propose(command).await {
            Ok(response) => response,
            Err(e) => {
                let error = MetadataError::Internal(format!("Failed to propose command: {}", e));
                observe::record_fs_op(
                    op.metric_label(),
                    "error",
                    observe::metadata_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                return Err(error);
            }
        };

        let fs_result = match response {
            AppDataResponse::Fs(res) => res,
            _ => FsCommandResult::ok(),
        };

        record_fs_write_result(op, started, &fs_result);
        Ok(fs_result)
    }
}

fn record_fs_write_result(op: CoreWriteOp, started: Instant, result: &FsCommandResult) {
    match result {
        FsCommandResult::Ok(_) => {
            observe::record_fs_op(op.metric_label(), "ok", "none", started.elapsed().as_secs_f64());
        }
        FsCommandResult::Err(err) => {
            observe::record_fs_op(
                op.metric_label(),
                "error",
                observe::fs_errno_kind(err.errno),
                started.elapsed().as_secs_f64(),
            );
        }
    }
}

#[cfg(test)]
mod tests;
