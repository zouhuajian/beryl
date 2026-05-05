// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::{CoreWriteOp, FsCore, StaleStateStatus};
use crate::error::MetadataError;
use crate::raft::{Command, FsCommandResult};
use crate::service::domain::{
    CoreResult, CreateInput, CreateOutput, MkdirInput, MkdirOutput, RenameInput, RenameOutput, RmdirInput, RmdirOutput,
    UnlinkInput, UnlinkOutput,
};
use std::sync::atomic::Ordering;

impl FsCore {
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

        if let Some(storage) = self.storage.as_ref() {
            match storage.get_dentry(req.parent_inode_id, &req.name) {
                Ok(Some(child_inode_id)) => match storage.get_inode(child_inode_id) {
                    Ok(Some(inode)) if inode.kind.is_file() => {
                        if self.write_session_manager.has_active_session(child_inode_id)
                            || self.inode_lease_manager.has_active_lease(child_inode_id)
                        {
                            return self.fatal_fs_failure(
                                &req.ctx,
                                types::fs::FsErrorCode::EBusy,
                                format!("File has an active write session or lease: {}", child_inode_id),
                                Some(ctx.namespace_owner_group_id.as_raw()),
                                Some(ctx.mount_epoch),
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        return self.failure_from_error(
                            &req.ctx,
                            err,
                            Some(ctx.namespace_owner_group_id.as_raw()),
                            Some(ctx.mount_epoch),
                        );
                    }
                },
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
                    match self.validate_stale_state(
                        &req.ctx,
                        raft_node.get_last_applied_state_id(),
                        Some(ctx.namespace_owner_group_id.as_raw()),
                        Some(ctx.mount_epoch),
                    ) {
                        Ok(StaleStateStatus::Ready) => {}
                        Ok(StaleStateStatus::UnknownLastApplied) => {
                            can_precheck = false;
                        }
                        Err(failure) => return Err(failure),
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

        match storage.get_dentry(req.dst_parent_inode_id, &req.dst_name) {
            Ok(Some(dst_inode_id)) => match storage.get_inode(dst_inode_id) {
                Ok(Some(inode)) if inode.kind.is_file() => {
                    if self.write_session_manager.has_active_session(dst_inode_id)
                        || self.inode_lease_manager.has_active_lease(dst_inode_id)
                    {
                        return self.fatal_fs_failure(
                            &req.ctx,
                            types::fs::FsErrorCode::EBusy,
                            format!("Rename target has an active write session or lease: {}", dst_inode_id),
                            Some(ctx.namespace_owner_group_id.as_raw()),
                            Some(ctx.mount_epoch),
                        );
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    return self.failure_from_error(
                        &req.ctx,
                        err,
                        Some(ctx.namespace_owner_group_id.as_raw()),
                        Some(ctx.mount_epoch),
                    );
                }
            },
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
}
