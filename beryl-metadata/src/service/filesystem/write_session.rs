// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::file_write::validate_active_write_layout;
use super::{
    worker_endpoint_from_parts, AdmissionFailure, Freshness, FsFailure, FsResult, MetadataFileSystem,
    PresentedFencingToken, PresentedWriteHandle, RequestContext, SessionKey,
};
use crate::error::MetadataError;
use crate::placement::{PlacementOp, PlacementPlanner, PlacementRequest, PlacementStatus};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind};
use beryl_common::header::CallerContextFields;
use beryl_types::fs::{FsErrorCode, InodeId};
use beryl_types::ids::{BlockId, BlockIndex, DataHandleId, LeaseId};
use beryl_types::layout::FileLayout;
use beryl_types::lease::FencingToken;
use beryl_types::{BlockShape, GroupName, Tier, WorkerEndpointInfo, WriteTarget};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
struct AbortWriteInput {
    ctx: RequestContext,
    file_handle: u64,
    lease_id: Option<LeaseId>,
    lease_epoch: u64,
    open_epoch: u64,
    fencing_token: Option<PresentedFencingToken>,
    freshness: Freshness,
}

#[derive(Clone, Debug)]
pub(super) struct OpenWriteInput {
    pub(super) ctx: RequestContext,
    pub(super) inode_id: InodeId,
    pub(super) desired_len: Option<u64>,
    pub(super) mode: crate::inode_lease::WriteMode,
    pub(super) freshness: Freshness,
}

#[derive(Clone, Debug)]
pub(super) struct OpenWriteOutput {
    pub(super) inode_id: InodeId,
    pub(super) data_handle_id: DataHandleId,
    pub(super) session_key: SessionKey,
    pub(super) layout: FileLayout,
    pub(super) write_targets: Vec<WriteTarget>,
    pub(super) base_size: u64,
    pub(super) expires_at_ms: u64,
}

#[derive(Clone, Debug)]
pub(super) struct AddBlockInput {
    pub(super) ctx: RequestContext,
    pub(super) file_handle: u64,
    pub(super) lease_id: Option<LeaseId>,
    pub(super) lease_epoch: u64,
    pub(super) open_epoch: u64,
    pub(super) fencing_token: Option<PresentedFencingToken>,
    pub(super) desired_len: Option<u64>,
    pub(super) freshness: Freshness,
}

#[derive(Clone, Debug)]
pub(crate) struct AddBlockOutput {
    pub(crate) target: WriteTarget,
}

#[derive(Clone, Debug)]
struct RenewLeaseInput {
    ctx: RequestContext,
    file_handle: u64,
    lease_id: Option<LeaseId>,
    lease_epoch: u64,
    open_epoch: u64,
    fencing_token: Option<PresentedFencingToken>,
    freshness: Freshness,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RenewLeaseOutput {
    pub(crate) expires_at_ms: u64,
}

pub(crate) struct AddBlockArgs {
    pub(crate) handle: PresentedWriteHandle,
    pub(crate) desired_len: Option<u64>,
    pub(crate) freshness: Freshness,
}

pub(crate) struct AbortFileWriteArgs {
    pub(crate) handle: PresentedWriteHandle,
    pub(crate) freshness: Freshness,
}

pub(crate) struct RenewLeaseArgs {
    pub(crate) handle: PresentedWriteHandle,
    pub(crate) freshness: Freshness,
}

impl MetadataFileSystem {
    pub(crate) async fn add_block(&self, ctx: &RequestContext, args: AddBlockArgs) -> FsResult<AddBlockOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.file_handle).await {
            return self.failure_from_admission(failure);
        }
        let handle = args.handle;
        let result = self
            .add_block_resolved(AddBlockInput {
                ctx: ctx.clone(),
                file_handle: handle.file_handle,
                lease_id: handle.lease_id,
                lease_epoch: handle.lease_epoch,
                open_epoch: handle.open_epoch,
                fencing_token: handle.fencing_token,
                desired_len: args.desired_len,
                freshness: args.freshness,
            })
            .await;
        match &result {
            Ok(success) => {
                let target = &success.payload.target;
                tracing::info!(
                    target: "metadata.block",
                    op = "AddBlock",
                    result = "allocated",
                    error_code = "none",
                    client_id = %ctx.caller.client.client_id,
                    call_id = %ctx.caller.client.call_id,
                    block_id = %target.block_id,
                    block_index = target.block_id.index.as_raw(),
                    group_id = success.group_name.as_ref().map(|group| group.as_str()),
                    desired_len = args.desired_len,
                    target_count = target.worker_endpoints.len(),
                    targets_sample = ?target.worker_endpoints.iter().take(3).map(|endpoint| endpoint.worker_id.as_raw()).collect::<Vec<_>>(),
                    data_handle_id = target.block_id.data_handle_id.as_raw(),
                    file_handle = handle.file_handle,
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "AddBlock allocated"
                );
            }
            Err(failure) => tracing::warn!(
                target: "metadata.block",
                op = "AddBlock",
                result = "rejected",
                error_code = crate::observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                desired_len = args.desired_len,
                file_handle = handle.file_handle,
                lease_epoch = handle.lease_epoch,
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "AddBlock rejected"
            ),
        }
        result
    }

    pub(crate) async fn abort_file_write(&self, ctx: &RequestContext, args: AbortFileWriteArgs) -> FsResult<()> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.file_handle).await {
            return self.failure_from_admission(failure);
        }
        let handle = args.handle;
        let result = self
            .abort_write_resolved(AbortWriteInput {
                ctx: ctx.clone(),
                file_handle: handle.file_handle,
                lease_id: handle.lease_id,
                lease_epoch: handle.lease_epoch,
                open_epoch: handle.open_epoch,
                fencing_token: handle.fencing_token,
                freshness: args.freshness,
            })
            .await;
        let lease_id = handle.lease_id.map(|lease_id| lease_id.as_raw());
        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "AbortFileWrite",
                result = "completed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                file_handle = handle.file_handle,
                lease_id,
                lease_epoch = handle.lease_epoch,
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "AbortFileWrite completed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "AbortFileWrite",
                result = "rejected",
                error_code = crate::observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                file_handle = handle.file_handle,
                lease_id,
                lease_epoch = handle.lease_epoch,
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "AbortFileWrite rejected"
            ),
        }
        result
    }

    pub(crate) async fn renew_lease(&self, ctx: &RequestContext, args: RenewLeaseArgs) -> FsResult<RenewLeaseOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.file_handle).await {
            return self.failure_from_admission(failure);
        }
        let handle = args.handle;
        let result = self
            .renew_lease_resolved(RenewLeaseInput {
                ctx: ctx.clone(),
                file_handle: handle.file_handle,
                lease_id: handle.lease_id,
                lease_epoch: handle.lease_epoch,
                open_epoch: handle.open_epoch,
                fencing_token: handle.fencing_token,
                freshness: args.freshness,
            })
            .await;
        let lease_id = handle.lease_id.map(|lease_id| lease_id.as_raw());
        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "RenewLease",
                result = "completed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                file_handle = handle.file_handle,
                lease_id,
                lease_epoch = handle.lease_epoch,
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "RenewLease completed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "RenewLease",
                result = "rejected",
                error_code = crate::observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                file_handle = handle.file_handle,
                lease_id,
                lease_epoch = handle.lease_epoch,
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "RenewLease rejected"
            ),
        }
        result
    }

    pub(super) async fn session_write_admission_failure(
        &self,
        ctx: &RequestContext,
        file_handle: u64,
    ) -> Option<AdmissionFailure> {
        if let Some(session) = self.session_registry.get_session(file_handle) {
            self.admission.check_data_write(ctx, session.mount_id).await.err()
        } else {
            self.admission.check_meta_write(ctx).await.err()
        }
    }
}

struct PlannedWriteTarget {
    block_id: BlockId,
    file_offset: u64,
    block_size: u64,
    effective_len: u64,
    worker_endpoints: Vec<WorkerEndpointInfo>,
    tier: Tier,
}

impl MetadataFileSystem {
    async fn abort_write_resolved(&self, req: AbortWriteInput) -> FsResult<()> {
        let file_handle = req.file_handle;
        let session = match self.session_registry.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!(
                        "write handle not found for handle={}; AbortFileWrite cannot be replayed automatically",
                        file_handle,
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(&req.ctx, req.freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };
        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(
                &req.ctx,
                req.freshness,
                group_name.clone(),
                mount_epoch,
                "AbortFileWrite",
            )
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let lease_id = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_name,
                    mount_epoch,
                );
            }
        };
        if lease_id != session.lease_id || req.lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "lease/write handle mismatch for handle={}; AbortFileWrite cannot be replayed automatically",
                    file_handle
                ),
                group_name,
                mount_epoch,
            );
        }
        if req.open_epoch != session.open_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "open_epoch mismatch: expected {}, got {}; AbortFileWrite cannot be replayed automatically",
                    session.open_epoch, req.open_epoch
                ),
                group_name,
                mount_epoch,
            );
        }
        let token = match req.fencing_token.as_ref() {
            Some(token) => token,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!(
                        "missing fencing_token for handle={}; AbortFileWrite cannot be replayed automatically",
                        file_handle
                    ),
                    group_name,
                    mount_epoch,
                );
            }
        };
        if !MetadataFileSystem::fencing_token_matches_session(&session, token) {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "fencing_token mismatch for handle={}; AbortFileWrite cannot be replayed automatically",
                    file_handle
                ),
                group_name,
                mount_epoch,
            );
        }
        if self
            .lease_manager
            .validate_lease(session.inode_id, lease_id, req.lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!(
                    "lease validation rejected for handle={}; write lease expired and AbortFileWrite cannot be replayed automatically",
                    file_handle,
                ),
                group_name,
                mount_epoch,
            );
        }

        self.lease_manager
            .release(session.inode_id, lease_id, session.lease_epoch);
        self.session_registry.remove_session(file_handle);

        self.success_with_route_epoch(&req.ctx, (), group_name, mount_epoch, route_epoch)
    }

    async fn renew_lease_resolved(&self, req: RenewLeaseInput) -> FsResult<RenewLeaseOutput> {
        let file_handle = req.file_handle;

        let session = match self.session_registry.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!(
                        "write handle not found for handle={}; RenewLease cannot be replayed automatically",
                        file_handle,
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(&req.ctx, req.freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let lease_id_typed = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_name,
                    mount_epoch,
                );
            }
        };

        if lease_id_typed != session.lease_id || req.lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "lease/write handle mismatch: expected lease_id={:?} lease_epoch={}, got lease_id={:?} lease_epoch={}; RenewLease cannot be replayed automatically",
                    session.lease_id,
                    session.lease_epoch,
                    lease_id_typed,
                    req.lease_epoch,
                ),
                group_name,
                mount_epoch,
            );
        }

        if req.open_epoch != session.open_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "open_epoch mismatch: expected {}, got {}; RenewLease cannot be replayed automatically",
                    session.open_epoch, req.open_epoch,
                ),
                group_name,
                mount_epoch,
            );
        }

        let req_token = match req.fencing_token.as_ref() {
            Some(token) => token,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!(
                        "missing fencing_token for handle={}; RenewLease cannot be replayed automatically",
                        file_handle,
                    ),
                    group_name,
                    mount_epoch,
                );
            }
        };
        if !MetadataFileSystem::fencing_token_matches_session(&session, req_token) {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "fencing_token mismatch for handle={}; RenewLease cannot be replayed automatically",
                    file_handle,
                ),
                group_name,
                mount_epoch,
            );
        }

        let expires_at_ms = match self
            .lease_manager
            .renew(session.inode_id, lease_id_typed, req.lease_epoch)
        {
            Ok(expires) => expires,
            Err(_) => {
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!(
                        "lease renewal rejected for handle={}; write lease expired and RenewLease cannot be replayed automatically",
                        file_handle,
                    ),
                    group_name,
                    mount_epoch,
                );
            }
        };

        let route_epoch = match self.authoritative_route_epoch().await {
            Ok(route_epoch) => Some(route_epoch),
            Err(error) => return self.failure_from_error(&req.ctx, error, group_name, mount_epoch),
        };
        self.success_with_route_epoch(
            &req.ctx,
            RenewLeaseOutput { expires_at_ms },
            group_name,
            mount_epoch,
            route_epoch,
        )
    }

    pub(super) async fn open_write_resolved(&self, req: OpenWriteInput) -> FsResult<OpenWriteOutput> {
        let caller_ctx = &req.ctx.caller;
        let inode_id = req.inode_id;

        let storage = &self.storage;

        let inode = match self.read_inode(inode_id) {
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

        let data_handle_id = inode.current_data_handle_id;
        if data_handle_id.as_raw() == 0 {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::Internal(format!("File inode {} is missing current_data_handle_id", inode_id)),
                None,
                None,
            );
        }
        if let Err(err) = storage.validate_data_handle_owner(data_handle_id, Some(inode_id)) {
            return self.failure_from_error(&req.ctx, err, None, None);
        }

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(&req.ctx, req.freshness, inode.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(&req.ctx, req.freshness, group_name.clone(), mount_epoch, "OpenWrite")
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

        let desired_len = req.desired_len.unwrap_or(4 * 1024 * 1024);
        let layout = match self.read_layout(inode_id) {
            Ok(layout) => layout,
            Err(err) => {
                return self.failure_from_error(&req.ctx, err, group_name, mount_epoch);
            }
        };
        if let Err(err) = validate_active_write_layout(&layout) {
            return self.failure_from_error(&req.ctx, err, group_name, mount_epoch);
        }
        let block_size = u64::from(layout.block_size);
        let chunk_size = layout.chunk_size;
        let current_file_version = match &inode.data {
            beryl_types::fs::InodeData::File { file_version, .. } => *file_version,
            _ => None,
        };
        let block_stamp = match current_file_version.unwrap_or(0).checked_add(1) {
            Some(block_stamp) => block_stamp,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument(format!("file_version overflow for inode {}", inode_id)),
                    group_name,
                    mount_epoch,
                );
            }
        };
        let num_blocks = desired_len.div_ceil(block_size).clamp(1, 10);
        let start_index = match &inode.data {
            beryl_types::fs::InodeData::File { extents, .. } => extents
                .iter()
                .map(|extent| extent.block_id.index.as_raw())
                .max()
                .map(|index| index + 1)
                .unwrap_or(0),
            _ => 0,
        };

        let worker_manager = match self.worker_manager.as_ref() {
            Some(worker_manager) => worker_manager,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::ServiceUnavailable("Worker manager not available".to_string()),
                    group_name,
                    mount_epoch,
                );
            }
        };
        let placement_group_name =
            self.require_worker_lookup_group(&req.ctx, group_name.clone(), mount_epoch, route_epoch, "OpenWrite")?;

        let placement_views = worker_manager.collect_worker_placement_views(&placement_group_name);
        let caller = req
            .ctx
            .caller
            .caller_context
            .as_ref()
            .map(CallerContextFields::from_caller_context);
        let planner = PlacementPlanner;
        let mut planned_targets = Vec::with_capacity(num_blocks as usize);
        for i in 0..num_blocks {
            let block_index = BlockIndex::new(start_index + i as u32);
            let block_id = BlockId::new(data_handle_id, block_index);
            let file_offset = base_size + i * block_size;
            let effective_len = desired_len.saturating_sub(i * block_size).min(block_size).max(1);
            let placement_req = PlacementRequest {
                group_name: placement_group_name.clone(),
                op: PlacementOp::Write,
                block_id,
                block_stamp: Some(block_stamp),
                layout,
                caller: caller.clone(),
                existing: Vec::new(),
                exclude_workers: Vec::new(),
                target_replicas: layout.replication,
            };
            let placement = planner.plan(&placement_req, &placement_views);
            if placement.status != PlacementStatus::Ok {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::ServiceUnavailable(format!(
                        "Failed to select write placement: {}",
                        placement.failure_message(&placement_req)
                    )),
                    group_name,
                    mount_epoch,
                );
            }

            let mut worker_endpoints = Vec::with_capacity(placement.workers.len());
            let mut selected_tier = None;
            for worker in placement.workers {
                selected_tier = selected_tier.or(worker.tier);
                let endpoint = match worker_endpoint_from_parts(
                    worker.worker_id,
                    worker.endpoint,
                    worker.worker_net_protocol,
                    worker.worker_run_id,
                ) {
                    Ok(endpoint) => endpoint,
                    Err(err) => {
                        return self.failure_from_error(&req.ctx, err, group_name, mount_epoch);
                    }
                };
                worker_endpoints.push(endpoint);
            }
            let Some(tier) = selected_tier else {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::ServiceUnavailable("selected write placement is missing storage tier".to_string()),
                    group_name,
                    mount_epoch,
                );
            };

            if worker_endpoints.is_empty() {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::ServiceUnavailable("selected placement has no live worker endpoints".to_string()),
                    group_name,
                    mount_epoch,
                );
            }

            planned_targets.push(PlannedWriteTarget {
                block_id,
                file_offset,
                block_size,
                effective_len,
                worker_endpoints,
                tier,
            });
        }

        let current_lease_epoch = match &inode.data {
            beryl_types::fs::InodeData::File { lease_epoch, .. } => *lease_epoch,
            _ => None,
        };

        let (lease_id, lease_epoch, expires_at_ms) = match self.lease_manager.try_acquire(
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
                    group_name,
                    mount_epoch,
                );
            }
            Err(e) => {
                return self.fatal_fs_failure(
                    &req.ctx,
                    e,
                    format!("Failed to acquire lease: {}", inode_id),
                    group_name,
                    mount_epoch,
                );
            }
        };

        let open_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let mut write_targets = Vec::with_capacity(planned_targets.len());
        for planned in planned_targets {
            let block_id = planned.block_id;
            let target_token = FencingToken {
                block_id,
                owner: caller_ctx.client.client_id,
                epoch: lease_epoch,
            };
            let target = WriteTarget {
                block_id,
                file_offset: planned.file_offset,
                block_size: planned.block_size,
                effective_len: planned.effective_len,
                worker_endpoints: planned.worker_endpoints,
                fencing_token: target_token,
                block_stamp,
                chunk_size,
                block_format_id: layout.block_format_id,
                tier: planned.tier,
            };
            let target_shape = match BlockShape::new(
                target.block_format_id,
                target.block_size,
                target.chunk_size,
                target.effective_len,
            ) {
                Ok(shape) => shape,
                Err(err) => {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::InvalidArgument(format!("invalid write target shape: {err}")),
                        group_name,
                        mount_epoch,
                    );
                }
            };
            let expected_shape = match BlockShape::for_effective_len(&layout, target.effective_len) {
                Ok(shape) => shape,
                Err(err) => {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::InvalidArgument(format!("invalid write target shape: {err}")),
                        group_name,
                        mount_epoch,
                    );
                }
            };
            if target_shape != expected_shape {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument(
                        "write target shape does not match persisted FileLayout".to_string(),
                    ),
                    group_name,
                    mount_epoch,
                );
            }
            write_targets.push(target);
        }

        let session_token = FencingToken {
            block_id: BlockId::new(data_handle_id, BlockIndex::new(0)),
            owner: caller_ctx.client.client_id,
            epoch: lease_epoch,
        };
        self.session_registry
            .remove_inactive_for_inode(inode_id, self.lease_manager.as_ref());
        let file_handle = self
            .session_registry
            .create_session(crate::session_registry::CreateSessionInput {
                inode_id,
                mount_id: inode.mount_id,
                data_handle_id,
                lease_id,
                lease_epoch,
                fencing_token: session_token,
                open_epoch,
                base_size,
                mode,
                write_targets: write_targets.clone(),
            });

        self.success_with_route_epoch(
            &req.ctx,
            OpenWriteOutput {
                inode_id,
                data_handle_id,
                session_key: SessionKey {
                    file_handle,
                    lease_id,
                    lease_epoch,
                    open_epoch,
                    fencing_token: session_token,
                },
                layout,
                write_targets,
                base_size,
                expires_at_ms,
            },
            group_name,
            mount_epoch,
            route_epoch,
        )
    }

    pub(super) async fn add_block_resolved(&self, req: AddBlockInput) -> FsResult<AddBlockOutput> {
        let file_handle = req.file_handle;
        let session = match self.session_registry.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!(
                        "write handle not found for handle={}; reopen before AddBlock",
                        file_handle
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(&req.ctx, req.freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };
        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(&req.ctx, req.freshness, group_name.clone(), mount_epoch, "AddBlock")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let lease_id = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_name,
                    mount_epoch,
                );
            }
        };
        if lease_id != session.lease_id || req.lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("lease mismatch for handle={}; reopen before AddBlock", file_handle),
                group_name,
                mount_epoch,
            );
        }
        if req.open_epoch != session.open_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::EpochMismatch),
                format!(
                    "open_epoch mismatch: expected {}, got {}; reopen before AddBlock",
                    session.open_epoch, req.open_epoch
                ),
                group_name,
                mount_epoch,
            );
        }
        let token = match req.fencing_token.as_ref() {
            Some(token) => token,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!(
                        "missing fencing_token for handle={}; reopen before AddBlock",
                        file_handle
                    ),
                    group_name,
                    mount_epoch,
                );
            }
        };
        if !MetadataFileSystem::fencing_token_matches_session(&session, token) {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "fencing_token mismatch for handle={}; reopen before AddBlock",
                    file_handle
                ),
                group_name,
                mount_epoch,
            );
        }
        if self
            .lease_manager
            .validate_lease(session.inode_id, lease_id, req.lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!(
                    "lease validation rejected for handle={}; reopen before AddBlock",
                    file_handle
                ),
                group_name,
                mount_epoch,
            );
        }

        let target = match self.session_registry.allocate_target(file_handle, req.desired_len) {
            Some(target) => target,
            None => {
                return self.fatal_fs_failure(
                    &req.ctx,
                    FsErrorCode::EAgain,
                    "no preallocated write target available; reopen with a larger desired_len",
                    group_name,
                    mount_epoch,
                );
            }
        };
        self.success_with_route_epoch(
            &req.ctx,
            AddBlockOutput { target },
            group_name,
            mount_epoch,
            route_epoch,
        )
    }

    pub(super) fn preflight_open_write_runtime(
        &self,
        req_ctx: &RequestContext,
        desired_len: Option<u64>,
        layout: FileLayout,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> Option<FsFailure> {
        let worker_manager = match self.worker_manager.as_ref() {
            Some(worker_manager) => worker_manager,
            None => {
                return self
                    .failure_from_error::<()>(
                        req_ctx,
                        MetadataError::ServiceUnavailable("Worker manager not available".to_string()),
                        group_name,
                        mount_epoch,
                    )
                    .err();
            }
        };
        if let Err(err) = validate_active_write_layout(&layout) {
            return self
                .failure_from_error::<()>(req_ctx, err, group_name, mount_epoch)
                .err();
        }
        let placement_group_name = match self.require_worker_lookup_group(
            req_ctx,
            group_name.clone(),
            mount_epoch,
            None,
            "OpenWrite preflight",
        ) {
            Ok(group_name) => group_name,
            Err(failure) => return Some(failure),
        };

        let desired_len = desired_len.unwrap_or(4 * 1024 * 1024);
        let block_size = u64::from(layout.block_size);
        let num_blocks = desired_len.div_ceil(block_size).clamp(1, 10);
        let placement_views = worker_manager.collect_worker_placement_views(&placement_group_name);
        let caller = req_ctx
            .caller
            .caller_context
            .as_ref()
            .map(CallerContextFields::from_caller_context);
        let planner = PlacementPlanner;
        for i in 0..num_blocks {
            let block_id = BlockId::new(DataHandleId::new(0), BlockIndex::new(i as u32));
            let placement_req = PlacementRequest {
                group_name: placement_group_name.clone(),
                op: PlacementOp::Write,
                block_id,
                block_stamp: None,
                layout,
                caller: caller.clone(),
                existing: Vec::new(),
                exclude_workers: Vec::new(),
                target_replicas: layout.replication,
            };
            let placement = planner.plan(&placement_req, &placement_views);
            if placement.status != PlacementStatus::Ok || placement.workers.is_empty() {
                return self
                    .failure_from_error::<()>(
                        req_ctx,
                        MetadataError::ServiceUnavailable(format!(
                            "Failed to select write placement: {}",
                            placement.failure_message(&placement_req)
                        )),
                        group_name,
                        mount_epoch,
                    )
                    .err();
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::filesystem::test_support::*;

    fn abort_input_for_session(
        session: &crate::session_registry::WriteSession,
        file_handle: u64,
        ctx: RequestContext,
    ) -> AbortWriteInput {
        AbortWriteInput {
            ctx,
            file_handle,
            lease_id: Some(session.lease_id),
            lease_epoch: session.lease_epoch,
            open_epoch: session.open_epoch,
            fencing_token: Some(presented_session_token(session)),
            freshness: Freshness::default(),
        }
    }

    fn renew_input_for_session(
        session: &crate::session_registry::WriteSession,
        file_handle: u64,
        ctx: RequestContext,
    ) -> RenewLeaseInput {
        RenewLeaseInput {
            ctx,
            file_handle,
            lease_id: Some(session.lease_id),
            lease_epoch: session.lease_epoch,
            open_epoch: session.open_epoch,
            fencing_token: Some(presented_session_token(session)),
            freshness: Freshness::default(),
        }
    }

    #[tokio::test]
    async fn abort_releases_lease() {
        let mount_id = MountId::new(41);
        let group_name_value = group_name("g4");
        let inode_id = InodeId::new(410);
        let filesystem = filesystem_with_mount(mount_id, 9, &group_name_value);
        let file_handle = install_write_session(&filesystem, inode_id, mount_id);
        let session = filesystem
            .write_session_for_handle(file_handle)
            .expect("session should be installed");

        let success = filesystem
            .abort_write_resolved(abort_input_for_session(&session, file_handle, request_context()))
            .await
            .expect("abort succeeds");

        assert!(filesystem.write_session_for_handle(file_handle).is_none());
        assert!(filesystem.lease_manager().get_active_lease(inode_id).is_none());
        assert_eq!(success.mount_epoch, Some(9));
        assert_eq!(success.group_name, Some(group_name_value));
    }

    #[tokio::test]
    async fn abort_checks_handle() {
        let mount_id = MountId::new(43);
        let inode_id = InodeId::new(430);
        let filesystem = filesystem_with_mount(mount_id, 9, &group_name("g6"));

        let failure = filesystem
            .abort_write_resolved(AbortWriteInput {
                ctx: request_context(),
                file_handle: 999,
                lease_id: Some(LeaseId::new(1)),
                lease_epoch: 1,
                open_epoch: 1,
                fencing_token: Some(PresentedFencingToken {
                    block_id: Some(BlockId::new(DataHandleId::new(1), BlockIndex::new(0))),
                    owner: ClientId::new(7),
                    epoch: 1,
                }),
                freshness: Freshness::default(),
            })
            .await
            .expect_err("missing write handle must be rejected");

        assert_reopen_write_session(&failure.error, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid));
        let roundtrip = crate::service::wire::header_from_fs_failure(&request_context(), &failure);
        let roundtrip_error = beryl_proto::convert::rpc_error_from_proto(
            roundtrip.error.as_ref().expect("session failure must carry wire error"),
        );
        assert_reopen_write_session(&roundtrip_error, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid));

        let file_handle = install_write_session(&filesystem, inode_id, mount_id);
        let session = filesystem
            .write_session_for_handle(file_handle)
            .expect("session should be installed");
        let mut stale = abort_input_for_session(&session, file_handle, request_context());
        stale.lease_epoch += 1;

        let stale_failure = filesystem
            .abort_write_resolved(stale)
            .await
            .expect_err("stale abort handle must be rejected");

        assert_reopen_write_session(
            &stale_failure.error,
            ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
        );
        assert!(filesystem.write_session_for_handle(file_handle).is_some());
        assert!(filesystem.lease_manager().get_active_lease(inode_id).is_some());
    }

    #[tokio::test]
    async fn renew_lease_rejects_invalid_session_identity() {
        let mount_id = MountId::new(44);
        let inode_id = InodeId::new(440);
        let filesystem = filesystem_with_mount(mount_id, 9, &group_name("g6"));
        let file_handle = install_write_session(&filesystem, inode_id, mount_id);
        let session = filesystem
            .write_session_for_handle(file_handle)
            .expect("session should be installed");

        filesystem
            .renew_lease_resolved(renew_input_for_session(&session, file_handle, request_context()))
            .await
            .expect("valid full write handle should renew lease");

        let mut stale_open = renew_input_for_session(&session, file_handle, request_context());
        stale_open.open_epoch += 1;
        let failure = filesystem
            .renew_lease_resolved(stale_open)
            .await
            .expect_err("open_epoch mismatch must be rejected");

        assert_reopen_write_session(&failure.error, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid));

        let mut stale_lease = renew_input_for_session(&session, file_handle, request_context());
        stale_lease.lease_epoch += 1;
        let failure = filesystem
            .renew_lease_resolved(stale_lease)
            .await
            .expect_err("lease_epoch mismatch must be rejected");

        assert_reopen_write_session(&failure.error, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid));

        let mut missing_handle = renew_input_for_session(&session, file_handle, request_context());
        missing_handle.file_handle = 404;
        let failure = filesystem
            .renew_lease_resolved(missing_handle)
            .await
            .expect_err("missing write handle must be rejected");

        assert_reopen_write_session(&failure.error, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid));
    }

    #[tokio::test]
    async fn renew_lease_checks_fencing() {
        let mount_id = MountId::new(45);
        let inode_id = InodeId::new(450);
        let filesystem = filesystem_with_mount(mount_id, 9, &group_name("g6"));
        let file_handle = install_write_session(&filesystem, inode_id, mount_id);
        let session = filesystem
            .write_session_for_handle(file_handle)
            .expect("session should be installed");

        let mut stale_fencing = renew_input_for_session(&session, file_handle, request_context());
        stale_fencing.fencing_token = Some(PresentedFencingToken {
            block_id: Some(BlockId::new(DataHandleId::new(999_999), BlockIndex::new(0))),
            owner: session.fencing_token.owner,
            epoch: session.fencing_token.epoch,
        });
        let failure = filesystem
            .renew_lease_resolved(stale_fencing)
            .await
            .expect_err("fencing token mismatch must be rejected");

        assert_reopen_write_session(&failure.error, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid));

        let mut missing_fencing = renew_input_for_session(&session, file_handle, request_context());
        missing_fencing.fencing_token = None;
        let missing = filesystem
            .renew_lease_resolved(missing_fencing)
            .await
            .expect_err("missing fencing token must be rejected");

        assert_reopen_write_session(&missing.error, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid));
    }

    #[tokio::test]
    async fn open_write_rejects_file_missing_current_data_handle() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(52);
        let inode_id = InodeId::new(520);
        storage
            .put_inode(&Inode::new_file(
                inode_id,
                FileAttrs::new(),
                mount_id,
                DataHandleId::new(0),
            ))
            .unwrap();

        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g10"))
            .with_storage(storage)
            .with_worker_manager(worker_manager_for_write_targets(&group_name("g10")))
            .build();

        let failure = filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id,
                desired_len: Some(4096),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .unwrap_err();

        assert!(failure.error.message.contains("missing current_data_handle_id"));
    }

    #[tokio::test]
    async fn open_write_target_uses_stored_file_layout_shape() {
        let env = write_flow_env(0).await;
        let layout = FileLayout::with_block_format(8192, 1024, 1, beryl_types::BlockFormatId::FULL_EFFECTIVE);
        env.storage.put_layout(env.inode_id, layout).unwrap();
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(2048),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("open write should use stored layout");
        let key = open.payload.session_key;

        let target = add_block_for_key(&env.filesystem, &key, 2048).await;

        assert_eq!(target.block_format_id, layout.block_format_id);
        assert_eq!(target.block_size, u64::from(layout.block_size));
        assert_eq!(target.chunk_size, layout.chunk_size);
        assert_eq!(target.effective_len, 2048);
    }

    #[tokio::test]
    async fn open_write_target_uses_metadata_selected_storage_tier() {
        let env = write_flow_env_for_tier(0, Tier::Ssd, 4096).await;
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(2048),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("open write should select SSD worker");
        let key = open.payload.session_key;

        let target = add_block_for_key(&env.filesystem, &key, 2048).await;

        assert_eq!(target.tier, Tier::Ssd);
    }

    #[tokio::test]
    async fn open_returns_file_version() {
        let env = write_flow_env(64).await;
        seed_committed_file_version(&env, 41, 900);
        publish_env_block_location(&env, BlockId::new(env.data_handle_id, BlockIndex::new(0)), 41, 1);

        let read = env
            .filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                range: None,
                requested_data_handle_id: None,
                freshness: Freshness::default(),
            })
            .await
            .expect("open/read layout should succeed");

        assert_eq!(read.payload.file_version, Some(41));
    }
}
