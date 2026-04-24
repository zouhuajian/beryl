// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::{CoreWriteOp, FsCore, StaleStateStatus};
use crate::error::MetadataError;
use crate::raft::{Command, FsCommandResult};
use crate::service::domain::{
    CoreResult, CreateInput, CreateOutput, DeleteOpPlan, MkdirInput, MkdirOutput, RemoveXattrInput, RemoveXattrOutput,
    RenameInput, RenameOpPlan, RenameOutput, RequestContext, RmdirInput, RmdirOutput, SetAttrInput, SetAttrOutput,
    SetXattrInput, SetXattrOutput, TruncateInput, TruncateOutput, UnlinkInput, UnlinkOutput,
};
use std::sync::atomic::Ordering;
use types::fs::InodeId;

impl FsCore {
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
}
