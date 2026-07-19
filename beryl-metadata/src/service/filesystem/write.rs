// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Leader-local write lease, placement, and session lifecycle.

use super::{missing_resolved_target_error, validate_active_write_layout};
use super::{
    worker_endpoint_from_parts, AdmissionFailure, Freshness, FsResult, MetadataFileSystem, PresentedWriteHandle,
    RequestContext,
};
use crate::error::MetadataError;
use crate::inode_lease::WriteMode;
use crate::observe;
use crate::placement::{PlacementOp, PlacementPlanner, PlacementRequest, PlacementStatus};
use crate::raft::FsCommandResult;
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind};
use beryl_common::header::CallerContextFields;
use beryl_types::fs::{FsErrorCode, InodeId};
use beryl_types::ids::{BlockId, BlockIndex, DataHandleId};
use beryl_types::layout::FileLayout;
use beryl_types::lease::FencingToken;
use beryl_types::{BlockShape, Tier, WorkerEndpointInfo, WriteTarget};

#[derive(Clone, Debug)]
pub(crate) struct OpenWriteOutput {
    pub(crate) inode_id: InodeId,
    pub(crate) data_handle_id: DataHandleId,
    pub(crate) lease_epoch: u64,
    pub(crate) layout: FileLayout,
    pub(crate) base_size: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) content_revision: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct AddBlockOutput {
    pub(crate) target: WriteTarget,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RenewLeaseOutput {
    pub(crate) expires_at_ms: u64,
}

pub(crate) struct AddBlockArgs {
    pub(crate) handle: PresentedWriteHandle,
    pub(crate) desired_len: Option<u64>,
    pub(crate) previous_block_id: Option<BlockId>,
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
        if let Some(failure) = self
            .session_write_admission_failure(ctx, args.handle.data_handle_id)
            .await
        {
            return self.failure_from_admission(failure);
        }
        let handle = args.handle;
        let result = self
            .add_block_session(
                ctx,
                handle.data_handle_id,
                handle.lease_epoch,
                args.desired_len,
                args.previous_block_id,
                args.freshness,
            )
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
                    handle_data_handle_id = handle.data_handle_id.as_raw(),
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
                handle_data_handle_id = handle.data_handle_id.as_raw(),
                lease_epoch = handle.lease_epoch,
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "AddBlock rejected"
            ),
        }
        result
    }

    pub(crate) async fn abort_file_write(&self, ctx: &RequestContext, args: AbortFileWriteArgs) -> FsResult<()> {
        if let Some(failure) = self
            .session_write_admission_failure(ctx, args.handle.data_handle_id)
            .await
        {
            return self.failure_from_admission(failure);
        }
        let handle = args.handle;
        let result = self
            .abort_session(ctx, handle.data_handle_id, handle.lease_epoch, args.freshness)
            .await;
        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "AbortFileWrite",
                result = "completed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                data_handle_id = handle.data_handle_id.as_raw(),
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
                data_handle_id = handle.data_handle_id.as_raw(),
                lease_epoch = handle.lease_epoch,
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "AbortFileWrite rejected"
            ),
        }
        result
    }

    pub(crate) async fn renew_lease(&self, ctx: &RequestContext, args: RenewLeaseArgs) -> FsResult<RenewLeaseOutput> {
        if let Some(failure) = self
            .session_write_admission_failure(ctx, args.handle.data_handle_id)
            .await
        {
            return self.failure_from_admission(failure);
        }
        let handle = args.handle;
        let result = self
            .renew_session(ctx, handle.data_handle_id, handle.lease_epoch, args.freshness)
            .await;
        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "RenewLease",
                result = "completed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                data_handle_id = handle.data_handle_id.as_raw(),
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
                data_handle_id = handle.data_handle_id.as_raw(),
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
        data_handle_id: DataHandleId,
    ) -> Option<AdmissionFailure> {
        if let Some(session) = self.session_registry.get_session(data_handle_id) {
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
    async fn abort_session(
        &self,
        ctx: &RequestContext,
        data_handle_id: DataHandleId,
        lease_epoch: u64,
        freshness: Freshness,
    ) -> FsResult<()> {
        let session = match self.session_registry.get_session(data_handle_id) {
            Some(session) => session,
            None => return self.success(ctx, (), None, None),
        };
        if session.open_client_id != ctx.caller.client.client_id {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("AbortFileWrite client does not own data_handle_id={data_handle_id}"),
                None,
                None,
            );
        }

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(ctx, freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };
        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, "AbortFileWrite")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        if lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for data_handle_id={data_handle_id}: expected {}, got {}",
                    session.lease_epoch, lease_epoch
                ),
                group_name,
                mount_epoch,
            );
        }

        let ended_epoch = match self
            .propose_fs_write_command(crate::raft::Command::EndWriteLease {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                inode_id: session.inode_id,
                lease_epoch,
            })
            .await
        {
            Ok(FsCommandResult::Ok(ok)) => ok.lease_epoch,
            Ok(FsCommandResult::Err(err)) => {
                return self.fatal_fs_failure(ctx, err.errno, err.message, group_name, mount_epoch);
            }
            Err(err) => return self.failure_from_error(ctx, err, group_name, mount_epoch),
        };
        if ended_epoch != lease_epoch.checked_add(1) {
            return self.failure_from_error(
                ctx,
                MetadataError::Internal("EndWriteLease returned an unexpected lease epoch".to_string()),
                group_name,
                mount_epoch,
            );
        }
        self.lease_manager.release(session.inode_id, lease_epoch);
        self.session_registry
            .remove_session_if_epoch(data_handle_id, lease_epoch);

        self.success_with_route_epoch(ctx, (), group_name, mount_epoch, route_epoch)
    }

    async fn renew_session(
        &self,
        ctx: &RequestContext,
        data_handle_id: DataHandleId,
        lease_epoch: u64,
        freshness: Freshness,
    ) -> FsResult<RenewLeaseOutput> {
        let session = match self.session_registry.get_session(data_handle_id) {
            Some(session) => session,
            None => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for data_handle_id={data_handle_id}",),
                    None,
                    None,
                );
            }
        };
        if session.open_client_id != ctx.caller.client.client_id {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("RenewLease client does not own data_handle_id={data_handle_id}"),
                None,
                None,
            );
        }

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(ctx, freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        if lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for data_handle_id={data_handle_id}: expected {}, got {}",
                    session.lease_epoch, lease_epoch
                ),
                group_name,
                mount_epoch,
            );
        }

        let expires_at_ms = match self
            .lease_manager
            .renew(session.inode_id, lease_epoch, ctx.caller.client.client_id)
        {
            Ok(expires) => expires,
            Err(_) => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!("lease renewal rejected for data_handle_id={data_handle_id}; write lease expired",),
                    group_name,
                    mount_epoch,
                );
            }
        };

        let route_epoch = match self.authoritative_route_epoch().await {
            Ok(route_epoch) => Some(route_epoch),
            Err(error) => return self.failure_from_error(ctx, error, group_name, mount_epoch),
        };
        self.success_with_route_epoch(
            ctx,
            RenewLeaseOutput { expires_at_ms },
            group_name,
            mount_epoch,
            route_epoch,
        )
    }

    pub(super) async fn open_write_inode(
        &self,
        ctx: &RequestContext,
        inode_id: InodeId,
        desired_len: Option<u64>,
        mode: crate::inode_lease::WriteMode,
        freshness: Freshness,
    ) -> FsResult<OpenWriteOutput> {
        let caller_ctx = &ctx.caller;

        let storage = &self.storage;

        let inode = match self.read_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => {
                return self.failure_from_error(ctx, err, None, None);
            }
        };

        if !inode.kind.is_file() {
            return self.failure_from_error(
                ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
                None,
            );
        }

        let data_handle_id = inode.data_handle_id;
        if data_handle_id.as_raw() == 0 {
            return self.failure_from_error(
                ctx,
                MetadataError::Internal(format!("File inode {} is missing data_handle_id", inode_id)),
                None,
                None,
            );
        }
        if let Err(err) = storage.validate_data_handle_owner(data_handle_id, Some(inode_id)) {
            return self.failure_from_error(ctx, err, None, None);
        }

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(ctx, freshness, inode.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, "OpenWrite")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let base_size = match mode {
            crate::inode_lease::WriteMode::Append => inode.attrs.size,
            crate::inode_lease::WriteMode::Write => 0,
        };

        let desired_len = desired_len.unwrap_or(4 * 1024 * 1024);
        let layout = match self.read_layout(inode_id) {
            Ok(layout) => layout,
            Err(err) => {
                return self.failure_from_error(ctx, err, group_name, mount_epoch);
            }
        };
        if let Err(err) = validate_active_write_layout(&layout) {
            return self.failure_from_error(ctx, err, group_name, mount_epoch);
        }
        let block_size = u64::from(layout.block_size);
        let chunk_size = layout.chunk_size;
        let current_content_revision = match &inode.data {
            beryl_types::fs::InodeData::File { content_revision, .. } => *content_revision,
            _ => None,
        };
        let block_stamp = match current_content_revision.unwrap_or(0).checked_add(1) {
            Some(block_stamp) => block_stamp,
            None => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::InvalidArgument(format!("content_revision overflow for inode {}", inode_id)),
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
                    ctx,
                    MetadataError::ServiceUnavailable("Worker manager not available".to_string()),
                    group_name,
                    mount_epoch,
                );
            }
        };
        let placement_group_name =
            self.require_worker_lookup_group(ctx, group_name.clone(), mount_epoch, route_epoch, "OpenWrite")?;

        let placement_views = worker_manager.collect_worker_placement_views(&placement_group_name);
        let caller = ctx
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
                    ctx,
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
                        return self.failure_from_error(ctx, err, group_name, mount_epoch);
                    }
                };
                worker_endpoints.push(endpoint);
            }
            let Some(tier) = selected_tier else {
                return self.failure_from_error(
                    ctx,
                    MetadataError::ServiceUnavailable("selected write placement is missing storage tier".to_string()),
                    group_name,
                    mount_epoch,
                );
            };

            if worker_endpoints.is_empty() {
                return self.failure_from_error(
                    ctx,
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

        let (lease_epoch, expires_at_ms) =
            match self
                .lease_manager
                .try_acquire(inode_id, caller_ctx.client.client_id, mode, current_lease_epoch)
            {
                Ok(result) => result,
                Err(FsErrorCode::EBusy) => {
                    return self.fatal_fs_failure(
                        ctx,
                        FsErrorCode::EBusy,
                        format!("File already has an active write lease: {}", inode_id),
                        group_name,
                        mount_epoch,
                    );
                }
                Err(e) => {
                    return self.fatal_fs_failure(
                        ctx,
                        e,
                        format!("Failed to acquire lease: {}", inode_id),
                        group_name,
                        mount_epoch,
                    );
                }
            };

        let lease_result = self
            .propose_fs_write_command(crate::raft::Command::AcquireWriteLease {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                inode_id,
                expected_lease_epoch: current_lease_epoch.unwrap_or(0),
            })
            .await;
        match lease_result {
            Ok(FsCommandResult::Ok(ok)) if ok.lease_epoch == Some(lease_epoch) => {}
            Ok(FsCommandResult::Ok(_)) => {
                self.lease_manager.release(inode_id, lease_epoch);
                return self.failure_from_error(
                    ctx,
                    MetadataError::Internal("AcquireWriteLease returned an unexpected lease epoch".to_string()),
                    group_name,
                    mount_epoch,
                );
            }
            Ok(FsCommandResult::Err(err)) => {
                self.lease_manager.release(inode_id, lease_epoch);
                return self.fatal_fs_failure(ctx, err.errno, err.message, group_name, mount_epoch);
            }
            Err(err) => {
                self.lease_manager.release(inode_id, lease_epoch);
                return self.failure_from_error(ctx, err, group_name, mount_epoch);
            }
        }

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
                        ctx,
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
                        ctx,
                        MetadataError::InvalidArgument(format!("invalid write target shape: {err}")),
                        group_name,
                        mount_epoch,
                    );
                }
            };
            if target_shape != expected_shape {
                return self.failure_from_error(
                    ctx,
                    MetadataError::InvalidArgument(
                        "write target shape does not match persisted FileLayout".to_string(),
                    ),
                    group_name,
                    mount_epoch,
                );
            }
            write_targets.push(target);
        }

        self.session_registry
            .remove_inactive_for_inode(inode_id, self.lease_manager.as_ref());
        let session = match self
            .session_registry
            .create_session(crate::session_registry::CreateSessionInput {
                inode_id,
                mount_id: inode.mount_id,
                data_handle_id,
                lease_epoch,
                base_size,
                content_revision: current_content_revision.unwrap_or(0),
                mode,
                open_client_id: caller_ctx.client.client_id,
                layout,
                expires_at_ms,
                write_targets: write_targets.clone(),
            }) {
            Ok(result) => result,
            Err(message) => {
                return self.failure_from_error(ctx, MetadataError::InvalidArgument(message), group_name, mount_epoch)
            }
        };

        self.success_with_route_epoch(ctx, open_write_output(&session), group_name, mount_epoch, route_epoch)
    }

    pub(super) async fn add_block_session(
        &self,
        ctx: &RequestContext,
        data_handle_id: DataHandleId,
        lease_epoch: u64,
        desired_len: Option<u64>,
        previous_block_id: Option<BlockId>,
        freshness: Freshness,
    ) -> FsResult<AddBlockOutput> {
        let session = match self.session_registry.get_session(data_handle_id) {
            Some(session) => session,
            None => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for data_handle_id={data_handle_id}"),
                    None,
                    None,
                );
            }
        };
        if session.open_client_id != ctx.caller.client.client_id {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("AddBlock client does not own data_handle_id={data_handle_id}"),
                None,
                None,
            );
        }

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(ctx, freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };
        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, "AddBlock")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        if lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for data_handle_id={data_handle_id}: expected {}, got {}",
                    session.lease_epoch, lease_epoch
                ),
                group_name,
                mount_epoch,
            );
        }
        if self
            .lease_manager
            .validate_lease(session.inode_id, lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!("lease validation rejected for data_handle_id={data_handle_id}; reopen before AddBlock"),
                group_name,
                mount_epoch,
            );
        }

        let target =
            match self
                .session_registry
                .allocate_target(data_handle_id, lease_epoch, previous_block_id, desired_len)
            {
                Ok(target) => target,
                Err(message) => {
                    return self.failure_from_error(
                        ctx,
                        MetadataError::InvalidArgument(format!(
                            "AddBlock rejected for data_handle_id={data_handle_id}: {message}"
                        )),
                        group_name,
                        mount_epoch,
                    );
                }
            };
        self.success_with_route_epoch(ctx, AddBlockOutput { target }, group_name, mount_epoch, route_epoch)
    }
}

fn open_write_output(session: &crate::session_registry::WriteSession) -> OpenWriteOutput {
    OpenWriteOutput {
        inode_id: session.inode_id,
        data_handle_id: session.data_handle_id,
        lease_epoch: session.lease_epoch,
        layout: session.layout,
        base_size: session.base_size,
        expires_at_ms: session.expires_at_ms,
        content_revision: session.content_revision,
    }
}

pub(crate) struct OpenWriteArgs {
    pub(crate) path: String,
    pub(crate) desired_len: Option<u64>,
    pub(crate) mode: WriteMode,
    pub(crate) freshness: Freshness,
}

impl MetadataFileSystem {
    pub(crate) async fn open_write(&self, ctx: &RequestContext, args: OpenWriteArgs) -> FsResult<OpenWriteOutput> {
        let path = args.path.clone();
        let result = self.open_write_inner(ctx, args).await;
        match &result {
            Ok(success) => {
                let payload = &success.payload;
                tracing::info!(
                    target: "metadata.state",
                    op = "OpenWrite",
                    result = "opened",
                    error_code = "none",
                    client_id = %ctx.caller.client.client_id,
                    call_id = %ctx.caller.client.call_id,
                    path = %path,
                    inode_id = payload.inode_id.as_raw(),
                    data_handle_id = payload.data_handle_id.as_raw(),
                    lease_epoch = payload.lease_epoch,
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "OpenWrite opened"
                );
            }
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "OpenWrite",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %path,
                "OpenWrite rejected"
            ),
        }
        result
    }

    async fn open_write_inner(&self, ctx: &RequestContext, args: OpenWriteArgs) -> FsResult<OpenWriteOutput> {
        if let Err(failure) = self.admission.check_meta_write(ctx).await {
            return self.failure_from_admission(failure);
        }
        let open_path = match crate::path_resolver::PathResolver::normalize(&args.path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
        };
        let resolved = match self.path_resolver.resolve_path(&open_path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
        };
        let Some(inode_id) = resolved.inode_id else {
            return self.failure_from_resolved_path_error(
                ctx,
                missing_resolved_target_error(&resolved),
                Some(&resolved.mount_ctx),
            );
        };
        if let Err(failure) = self.admission.check_data_write(ctx, resolved.mount_ctx.mount_id).await {
            return self.failure_from_admission(failure);
        }
        self.open_write_inode(ctx, inode_id, args.desired_len, args.mode, args.freshness)
            .await
    }
}

#[cfg(test)]
mod open_write_tests {
    use super::*;
    use crate::service::filesystem::test_support::*;

    #[tokio::test]
    async fn open_write_cleans_lease_on_error() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(50);
        let inode_id = InodeId::new(500);
        let data_handle_id = DataHandleId::new(9500);
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g7"))
            .with_storage(storage)
            .build();

        let failure = filesystem
            .open_write_inode(
                &request_context(),
                inode_id,
                Some(4096),
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect_err("missing worker manager should fail open_write");

        assert!(failure.error.message.contains("Worker manager not available"));
        assert!(filesystem.lease_manager().get_active_lease(inode_id).is_none());
    }

    #[tokio::test]
    async fn open_write_uses_current_data_handle_and_duplicate_fails_without_advancing_epoch() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(51);
        let group_name_value = group_name("g9");
        let inode_id = InodeId::new(510);
        let data_handle_id = DataHandleId::new(9510);
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_worker_manager(worker_manager_for_write_targets(&group_name_value))
            .build();

        let success = filesystem
            .open_write_inode(
                &request_context(),
                inode_id,
                Some(4096),
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open_write should succeed");

        assert_ne!(inode_id.as_raw(), data_handle_id.as_raw());
        let session = filesystem
            .write_session_for_handle(success.payload.data_handle_id)
            .expect("session should be stored");
        assert!(!session.write_targets.is_empty());
        for target in &session.write_targets {
            assert_eq!(target.block_id.data_handle_id, data_handle_id);
            assert_eq!(target.block_size, 4096);
            assert_eq!(target.effective_len, 4096);
            assert_eq!(target.chunk_size, 4096);
            assert_eq!(target.block_format_id, beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE);
        }
        assert_eq!(success.payload.data_handle_id, data_handle_id);
        assert_eq!(session.data_handle_id, data_handle_id);

        let persisted_epoch = storage
            .get_inode(inode_id)
            .unwrap()
            .and_then(|inode| match inode.data {
                beryl_types::fs::InodeData::File { lease_epoch, .. } => lease_epoch,
                _ => None,
            })
            .expect("OpenWrite must persist the acquired lease epoch");
        let duplicate = filesystem
            .open_write_inode(
                &request_context(),
                inode_id,
                Some(4096),
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect_err("a duplicate OpenWrite must fail closed while the lease is active");
        assert_fail(
            &duplicate.error,
            beryl_common::error::rpc::ErrorKind::Fs(FsErrorCode::EBusy),
        );
        let epoch_after_duplicate = storage.get_inode(inode_id).unwrap().and_then(|inode| match inode.data {
            beryl_types::fs::InodeData::File { lease_epoch, .. } => lease_epoch,
            _ => None,
        });
        assert_eq!(epoch_after_duplicate, Some(persisted_epoch));
    }

    #[tokio::test]
    async fn open_write_rejects_missing_file_layout_without_default_fallback() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(52);
        let group_name_value = group_name("g9");
        let inode_id = InodeId::new(520);
        let data_handle_id = DataHandleId::new(9520);
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name_value)
            .with_storage(storage)
            .with_worker_manager(worker_manager_for_write_targets(&group_name_value))
            .build();

        let failure = filesystem
            .open_write_inode(
                &request_context(),
                inode_id,
                Some(4096),
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect_err("missing persisted layout must fail open_write");

        assert!(failure.error.message.contains("Layout not found"));
    }

    #[tokio::test]
    async fn open_write_rejects_multi_replica_layout_until_durable_replication_exists() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(54);
        let group_name_value = group_name("g9");
        let inode_id = InodeId::new(540);
        let data_handle_id = DataHandleId::new(9540);
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 2)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name_value)
            .with_storage(storage)
            .with_worker_manager(worker_manager_for_write_targets(&group_name_value))
            .build();

        let failure = filesystem
            .open_write_inode(
                &request_context(),
                inode_id,
                Some(4096),
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect_err("multi-replica layout must fail active write");

        assert!(
            failure
                .error
                .message
                .contains("multi-replica write is not supported yet; replication must be 1"),
            "unexpected error: {}",
            failure.error.message
        );
    }
}
