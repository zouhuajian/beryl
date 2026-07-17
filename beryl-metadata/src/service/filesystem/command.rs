// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared routing and Raft proposal boundary for filesystem writes.

use super::{fs_failure_from_metadata_error, Freshness, FsFailure, MetadataFileSystem, RequestContext};
use crate::error::{MetadataError, MetadataResult};
use crate::observe;
use crate::raft::{AppDataResponse, Command, FsCommandResult};
use beryl_types::fs::InodeId;
use beryl_types::ids::MountId;
use beryl_types::GroupName;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::debug;

#[derive(Clone, Debug)]
pub(super) struct RoutedFsWriteCtx {
    pub(super) mount_id: MountId,
    pub(super) namespace_owner_group_name: GroupName,
    pub(super) mount_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WriteCommandKind {
    Create,
    Mkdir,
    Delete,
    Rename,
    SetAttr,
}

impl WriteCommandKind {
    fn metric_label(self) -> &'static str {
        match self {
            WriteCommandKind::Create => "create_file",
            WriteCommandKind::Mkdir => "create_directory",
            WriteCommandKind::Delete => "delete",
            WriteCommandKind::Rename => "rename",
            WriteCommandKind::SetAttr => "set_attr",
        }
    }
}

impl MetadataFileSystem {
    pub(super) fn route_ctx_for_write(
        &self,
        req_ctx: &RequestContext,
        op: WriteCommandKind,
        parent_inode_ids: &[InodeId],
        freshness: Freshness,
    ) -> Result<RoutedFsWriteCtx, FsFailure> {
        self.route_ctx_for_write_with_error_hints(req_ctx, op, parent_inode_ids, freshness, None, None)
    }

    pub(super) fn route_ctx_for_write_with_error_hints(
        &self,
        req_ctx: &RequestContext,
        op: WriteCommandKind,
        parent_inode_ids: &[InodeId],
        freshness: Freshness,
        error_group_name: Option<GroupName>,
        error_mount_epoch: Option<u64>,
    ) -> Result<RoutedFsWriteCtx, FsFailure> {
        let ctx = match self.route_fs_write_ctx(op, parent_inode_ids) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(fs_failure_from_metadata_error(
                    req_ctx,
                    err,
                    error_group_name,
                    error_mount_epoch,
                    None,
                ));
            }
        };

        if let Err(failure) = self
            .freshness_validator
            .validate_mount_epoch(req_ctx, freshness, ctx.mount_id)
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

    pub(super) fn route_fs_write_ctx(
        &self,
        op: WriteCommandKind,
        parent_inode_ids: &[InodeId],
    ) -> MetadataResult<RoutedFsWriteCtx> {
        let parent_inode_id = parent_inode_ids
            .first()
            .ok_or_else(|| MetadataError::InvalidArgument("No parent inode provided".to_string()))?;
        let parent_inode = self
            .read_inode(*parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;

        let mount_id = parent_inode.mount_id;
        for other_parent in parent_inode_ids.iter().skip(1) {
            let inode = self
                .read_inode(*other_parent)?
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

    pub(super) async fn propose_fs_write_command(
        &self,
        op: WriteCommandKind,
        command: Command,
    ) -> MetadataResult<FsCommandResult> {
        let started = Instant::now();
        let dedup = command.dedup_key();
        if self.session_registry.has_call_id(dedup.client_id, dedup.call_id)
            || self.lease_manager.has_renew_call(dedup.client_id, dedup.call_id)
        {
            return Err(MetadataError::InvalidArgument(format!(
                "call_id {} was already used by an active write session RPC",
                dedup.call_id
            )));
        }
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

        if let Some(metrics) = &self.metrics {
            metrics.fs_raft_appends_total.fetch_add(1, Ordering::Relaxed);
            match op {
                WriteCommandKind::Create => {
                    metrics.fs_raft_appends_create.fetch_add(1, Ordering::Relaxed);
                }
                WriteCommandKind::Mkdir => {
                    metrics.fs_raft_appends_mkdir.fetch_add(1, Ordering::Relaxed);
                }
                WriteCommandKind::Delete => {}
                WriteCommandKind::Rename => {
                    metrics.fs_raft_appends_rename.fetch_add(1, Ordering::Relaxed);
                }
                WriteCommandKind::SetAttr => {
                    metrics.fs_raft_appends_setattr.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let response = match raft_node.propose(command).await {
            Ok(response) => response,
            Err(error) => {
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

fn record_fs_write_result(op: WriteCommandKind, started: Instant, result: &FsCommandResult) {
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
