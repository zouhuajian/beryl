// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::{Freshness, FsResult, FsSuccess, MetadataFileSystem, RequestContext, WriteCommandKind};
use crate::error::MetadataError;
use crate::observe;
use crate::raft::{AppDataResponse, Command, FsCommandResult};
use beryl_types::fs::{FileAttrs, InodeId};
use std::sync::atomic::Ordering;

#[derive(Clone, Debug)]
struct MkdirInput {
    ctx: RequestContext,
    path: String,
    parent_inode_id: InodeId,
    name: String,
    attrs: FileAttrs,
    freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
struct MkdirOutput {
    inode_id: Option<InodeId>,
    attrs: Option<FileAttrs>,
}

#[derive(Clone, Debug)]
struct RenameInput {
    ctx: RequestContext,
    src_path: String,
    dst_path: String,
    src_parent_inode_id: InodeId,
    src_name: String,
    dst_parent_inode_id: InodeId,
    dst_name: String,
    flags: u32,
    freshness: Freshness,
}

pub(crate) struct CreateDirectoryArgs {
    pub(crate) path: String,
    // Deferring wire conversion errors until after write admission preserves failure precedence.
    pub(crate) parsed_attrs: Result<FileAttrs, MetadataError>,
    pub(crate) recursive: bool,
    pub(crate) freshness: Freshness,
}

struct ValidatedCreateDirectoryArgs {
    path: String,
    attrs: FileAttrs,
    recursive: bool,
    freshness: Freshness,
}

pub(crate) struct CreateDirectoryOutput {
    pub(crate) inode_id: Option<InodeId>,
    pub(crate) attrs: FileAttrs,
}

pub(crate) struct RenameArgs {
    pub(crate) src_path: String,
    pub(crate) dst_path: String,
    pub(crate) flags: u32,
    pub(crate) freshness: Freshness,
}

impl MetadataFileSystem {
    pub(crate) async fn create_directory(
        &self,
        ctx: &RequestContext,
        args: CreateDirectoryArgs,
    ) -> FsResult<CreateDirectoryOutput> {
        if let Err(failure) = self.admission.check_meta_write(ctx).await {
            return self.failure_from_admission(failure);
        }

        let CreateDirectoryArgs {
            path,
            parsed_attrs,
            recursive,
            freshness,
        } = args;
        let attrs = match parsed_attrs {
            Ok(attrs) => attrs,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let path = match crate::path_resolver::PathResolver::normalize(&path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let (mount_ctx, _) = match self.path_resolver.resolve_mount_components(&path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let fingerprint = Command::create_directory_fingerprint(&path, &attrs, recursive);
        match self.replay_namespace_result(&ctx.caller, fingerprint) {
            Ok(Some(FsCommandResult::Ok(ok))) => {
                let Some(attrs) = ok.attrs else {
                    return self.failure_from_resolved_path_error(
                        ctx,
                        MetadataError::Internal("CreateDirectory replay result is missing attrs".to_string()),
                        Some(&mount_ctx),
                    );
                };
                return self.success(
                    ctx,
                    CreateDirectoryOutput {
                        inode_id: ok.inode_id,
                        attrs,
                    },
                    Some(mount_ctx.owner_group_name),
                    Some(mount_ctx.mount_epoch),
                );
            }
            Ok(Some(FsCommandResult::Err(err))) => {
                return self.fatal_fs_failure(
                    ctx,
                    err.errno,
                    err.message,
                    Some(mount_ctx.owner_group_name),
                    Some(mount_ctx.mount_epoch),
                );
            }
            Ok(None) => {}
            Err(err) => return self.failure_from_resolved_path_error(ctx, err, Some(&mount_ctx)),
        }
        if let Err(err) = self.reject_active_session_call_reuse(&ctx.caller) {
            return self.failure_from_resolved_path_error(ctx, err, Some(&mount_ctx));
        }
        let args = ValidatedCreateDirectoryArgs {
            path,
            attrs,
            recursive,
            freshness,
        };

        let path = args.path.clone();
        let result = if args.recursive {
            self.create_directory_recursive(ctx, args).await
        } else {
            self.create_directory_once(ctx, args).await
        };
        let parent_inode_id = self
            .path_resolver
            .resolve_path(&path)
            .ok()
            .and_then(|resolved| resolved.parent_inode_id);

        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "CreateDirectory",
                result = "committed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %path,
                inode_id = success.payload.inode_id.map(|id| id.as_raw()),
                parent_inode_id = parent_inode_id.map(|id| id.as_raw()),
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "CreateDirectory committed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "CreateDirectory",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %path,
                parent_inode_id = parent_inode_id.map(|id| id.as_raw()),
                "CreateDirectory rejected"
            ),
        }
        result
    }

    async fn create_directory_once(
        &self,
        ctx: &RequestContext,
        args: ValidatedCreateDirectoryArgs,
    ) -> FsResult<CreateDirectoryOutput> {
        let resolved = match self.path_resolver.resolve_path(&args.path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
        };
        let (Some(parent_inode_id), Some(name)) = (resolved.parent_inode_id, resolved.name.clone()) else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot operate on mount root".to_string()),
                Some(&resolved.mount_ctx),
            );
        };

        let success = self
            .mkdir_resolved(MkdirInput {
                ctx: ctx.clone(),
                path: args.path,
                parent_inode_id,
                name,
                attrs: args.attrs,
                freshness: args.freshness,
            })
            .await?;
        let Some(attrs) = success.payload.attrs else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::Internal("CreateDirectory succeeded without attrs".to_string()),
                Some(&resolved.mount_ctx),
            );
        };
        Ok(FsSuccess {
            payload: CreateDirectoryOutput {
                inode_id: success.payload.inode_id,
                attrs,
            },
            group_name: success.group_name,
            mount_epoch: success.mount_epoch,
            route_epoch: success.route_epoch,
            state: success.state,
        })
    }

    async fn create_directory_recursive(
        &self,
        ctx: &RequestContext,
        args: ValidatedCreateDirectoryArgs,
    ) -> FsResult<CreateDirectoryOutput> {
        let (mount_ctx, components) = match self.path_resolver.resolve_mount_components(&args.path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
        };
        if components.is_empty() {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot operate on mount root".to_string()),
                Some(&mount_ctx),
            );
        }

        let routed =
            match self.route_ctx_for_write(ctx, WriteCommandKind::Mkdir, &[mount_ctx.root_inode_id], args.freshness) {
                Ok(routed) => routed,
                Err(err) => return Err(err),
            };
        let dedup = match self.dedup_key(&ctx.caller) {
            Ok(key) => key,
            Err(err) => {
                return self.failure_from_error(
                    ctx,
                    err,
                    Some(routed.namespace_owner_group_name.clone()),
                    Some(routed.mount_epoch),
                );
            }
        };
        let result = match self
            .propose_fs_write_command(
                WriteCommandKind::Mkdir,
                Command::new_namespace(
                    dedup,
                    crate::raft::proposal_timestamp_ms(),
                    crate::raft::CanonicalNamespaceRequest::CreateDirectory {
                        path: args.path,
                        attrs: args.attrs.clone(),
                        recursive: true,
                    },
                    crate::raft::Mutation::CreateDirectory {
                        root_inode_id: mount_ctx.root_inode_id,
                        components,
                        attrs: args.attrs,
                    },
                ),
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    ctx,
                    err,
                    Some(routed.namespace_owner_group_name.clone()),
                    Some(routed.mount_epoch),
                );
            }
        };
        match result {
            FsCommandResult::Ok(ok) => {
                let Some(inode_id) = ok.inode_id else {
                    return self.failure_from_error(
                        ctx,
                        MetadataError::Internal("CreateDirectory succeeded without inode_id".to_string()),
                        Some(routed.namespace_owner_group_name.clone()),
                        Some(routed.mount_epoch),
                    );
                };
                let Some(attrs) = ok.attrs else {
                    return self.failure_from_error(
                        ctx,
                        MetadataError::Internal("CreateDirectory succeeded without frozen attrs".to_string()),
                        Some(routed.namespace_owner_group_name.clone()),
                        Some(routed.mount_epoch),
                    );
                };
                self.success(
                    ctx,
                    CreateDirectoryOutput {
                        inode_id: Some(inode_id),
                        attrs,
                    },
                    Some(routed.namespace_owner_group_name),
                    Some(routed.mount_epoch),
                )
            }
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                ctx,
                err.errno,
                err.message,
                Some(routed.namespace_owner_group_name),
                Some(routed.mount_epoch),
            ),
        }
    }

    pub(crate) async fn rename(&self, ctx: &RequestContext, args: RenameArgs) -> FsResult<()> {
        if let Err(failure) = self.admission.check_meta_write(ctx).await {
            return self.failure_from_admission(failure);
        }
        let src_path = match crate::path_resolver::PathResolver::normalize(&args.src_path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &args.src_path, err),
        };
        let dst_path = match crate::path_resolver::PathResolver::normalize(&args.dst_path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &args.dst_path, err),
        };
        let (src_mount_ctx, _) = match self.path_resolver.resolve_mount_components(&src_path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &src_path, err),
        };
        let fingerprint = Command::rename_fingerprint(&src_path, &dst_path, args.flags);
        match self.replay_namespace_result(&ctx.caller, fingerprint) {
            Ok(Some(FsCommandResult::Ok(_))) => {
                return self.success(
                    ctx,
                    (),
                    Some(src_mount_ctx.owner_group_name),
                    Some(src_mount_ctx.mount_epoch),
                );
            }
            Ok(Some(FsCommandResult::Err(err))) => {
                return self.fatal_fs_failure(
                    ctx,
                    err.errno,
                    err.message,
                    Some(src_mount_ctx.owner_group_name),
                    Some(src_mount_ctx.mount_epoch),
                );
            }
            Ok(None) => {}
            Err(err) => return self.failure_from_resolved_path_error(ctx, err, Some(&src_mount_ctx)),
        }
        if let Err(err) = self.reject_active_session_call_reuse(&ctx.caller) {
            return self.failure_from_resolved_path_error(ctx, err, Some(&src_mount_ctx));
        }

        let (src_resolved, dst_resolved) = match self.path_resolver.resolve_rename(&src_path, &dst_path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_error(ctx, err, None, None),
        };
        let (Some(src_parent_inode_id), Some(src_name)) = (src_resolved.parent_inode_id, src_resolved.name.clone())
        else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot rename a mount root".to_string()),
                Some(&src_resolved.mount_ctx),
            );
        };
        let (Some(dst_parent_inode_id), Some(dst_name)) = (dst_resolved.parent_inode_id, dst_resolved.name.clone())
        else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot rename to a mount root".to_string()),
                Some(&dst_resolved.mount_ctx),
            );
        };

        let result = self
            .rename_resolved(RenameInput {
                ctx: ctx.clone(),
                src_path,
                dst_path,
                src_parent_inode_id,
                src_name,
                dst_parent_inode_id,
                dst_name,
                flags: args.flags,
                freshness: args.freshness,
            })
            .await;

        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "Rename",
                result = "committed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                src = %args.src_path,
                dst = %args.dst_path,
                parent_inode_id = src_parent_inode_id.as_raw(),
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "Rename committed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "Rename",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                src = %args.src_path,
                dst = %args.dst_path,
                parent_inode_id = src_parent_inode_id.as_raw(),
                "Rename rejected"
            ),
        }
        result
    }

    async fn mkdir_resolved(&self, req: MkdirInput) -> FsResult<MkdirOutput> {
        let ctx =
            match self.route_ctx_for_write(&req.ctx, WriteCommandKind::Mkdir, &[req.parent_inode_id], req.freshness) {
                Ok(ctx) => ctx,
                Err(err) => return Err(err),
            };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        let result = match self
            .propose_fs_write_command(
                WriteCommandKind::Mkdir,
                Command::new_namespace(
                    dedup,
                    crate::raft::proposal_timestamp_ms(),
                    crate::raft::CanonicalNamespaceRequest::CreateDirectory {
                        path: req.path,
                        attrs: req.attrs.clone(),
                        recursive: false,
                    },
                    crate::raft::Mutation::Mkdir {
                        parent_inode_id: req.parent_inode_id,
                        name: req.name,
                        attrs: req.attrs,
                    },
                ),
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        match result {
            FsCommandResult::Ok(ok) => self.success(
                &req.ctx,
                MkdirOutput {
                    inode_id: ok.inode_id,
                    attrs: ok.attrs,
                },
                Some(ctx.namespace_owner_group_name.clone()),
                Some(ctx.mount_epoch),
            ),
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                &req.ctx,
                err.errno,
                err.message,
                Some(ctx.namespace_owner_group_name.clone()),
                Some(ctx.mount_epoch),
            ),
        }
    }

    async fn rename_resolved(&self, req: RenameInput) -> FsResult<()> {
        let supported_mask: u32 = 0x1;
        if req.flags & !supported_mask != 0 {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::NotSupported(format!("Unsupported rename flags: {}", req.flags)),
                None,
                None,
            );
        }

        let src_parent_inode = match self.read_inode(req.src_parent_inode_id) {
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
        let dst_parent_inode = match self.read_inode(req.dst_parent_inode_id) {
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
            let (group_name, mount_epoch) = self
                .freshness_validator
                .mount_hints_for_mount(src_parent_inode.mount_id);
            return self.failure_from_error(
                &req.ctx,
                MetadataError::CrossMountRename(format!(
                    "Cross-mount rename not allowed: src_mount={:?}, dst_mount={:?}",
                    src_parent_inode.mount_id, dst_parent_inode.mount_id
                )),
                group_name,
                mount_epoch,
            );
        }

        let ctx = match self.route_ctx_for_write(
            &req.ctx,
            WriteCommandKind::Rename,
            &[req.src_parent_inode_id, req.dst_parent_inode_id],
            req.freshness,
        ) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(key) => key,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
        };
        let command = Command::new_namespace(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::CanonicalNamespaceRequest::Rename {
                src_path: req.src_path,
                dst_path: req.dst_path,
                flags: req.flags,
            },
            crate::raft::Mutation::Rename {
                src_parent_inode_id: req.src_parent_inode_id,
                src_name: req.src_name.clone(),
                dst_parent_inode_id: req.dst_parent_inode_id,
                dst_name: req.dst_name.clone(),
                flags: req.flags,
            },
        );
        let fingerprint = command.fingerprint();
        match self.storage.get_applied_result(&dedup) {
            Ok(Some(existing)) => {
                if existing.fingerprint != fingerprint {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::InvalidArgument(format!(
                            "call_id {} reused with different command payload",
                            dedup.call_id
                        )),
                        Some(ctx.namespace_owner_group_name.clone()),
                        Some(ctx.mount_epoch),
                    );
                }
                let result = match existing.result {
                    AppDataResponse::Fs(result) => result,
                    _ => FsCommandResult::ok(),
                };
                return self.rename_result(&req.ctx, &ctx, result);
            }
            Ok(None) => {}
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
        }

        if let Err(err) = self.reject_active_session_call_reuse(&req.ctx.caller) {
            return self.failure_from_error(
                &req.ctx,
                err,
                Some(ctx.namespace_owner_group_name.clone()),
                Some(ctx.mount_epoch),
            );
        }

        match self.read_dentry(req.dst_parent_inode_id, &req.dst_name) {
            Ok(Some(dst_inode_id)) => match self.read_inode(dst_inode_id) {
                Ok(Some(inode)) if inode.kind.is_file() => {
                    if self.has_active_write(dst_inode_id) {
                        return self.fatal_fs_failure(
                            &req.ctx,
                            beryl_types::fs::FsErrorCode::EBusy,
                            format!("Rename target has an active write lease: {}", dst_inode_id),
                            Some(ctx.namespace_owner_group_name.clone()),
                            Some(ctx.mount_epoch),
                        );
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    return self.failure_from_error(
                        &req.ctx,
                        err,
                        Some(ctx.namespace_owner_group_name.clone()),
                        Some(ctx.mount_epoch),
                    );
                }
            },
            Ok(None) => {}
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
        }

        let result = match self.propose_fs_write_command(WriteCommandKind::Rename, command).await {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        self.rename_result(&req.ctx, &ctx, result)
    }

    fn rename_result(
        &self,
        request_ctx: &RequestContext,
        ctx: &super::RoutedFsWriteCtx,
        result: FsCommandResult,
    ) -> FsResult<()> {
        match result {
            FsCommandResult::Ok(_) => self.success(
                request_ctx,
                (),
                Some(ctx.namespace_owner_group_name.clone()),
                Some(ctx.mount_epoch),
            ),
            FsCommandResult::Err(err) => self.fatal_fs_failure(
                request_ctx,
                err.errno,
                err.message,
                Some(ctx.namespace_owner_group_name.clone()),
                Some(ctx.mount_epoch),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::filesystem::test_support::*;

    #[tokio::test]
    async fn rename_rejects_active_write_target() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(56);
        let group_name_value = group_name("g14");
        let parent_inode_id = InodeId::new(560);
        let source_inode_id = InodeId::new(561);
        let target_inode_id = InodeId::new(562);
        let source_handle = DataHandleId::new(563);
        let target_handle = DataHandleId::new(564);
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .build();

        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_file(
                source_inode_id,
                FileAttrs::new(),
                mount_id,
                source_handle,
            ))
            .unwrap();
        storage
            .put_inode(&Inode::new_file(
                target_inode_id,
                FileAttrs::new(),
                mount_id,
                target_handle,
            ))
            .unwrap();
        storage.put_dentry(parent_inode_id, "source", source_inode_id).unwrap();
        storage.put_dentry(parent_inode_id, "target", target_inode_id).unwrap();
        storage
            .put_layout(source_inode_id, FileLayout::new(4096, 4096, 1))
            .unwrap();
        storage
            .put_layout(target_inode_id, FileLayout::new(4096, 4096, 1))
            .unwrap();
        storage.put_data_handle_owner(source_handle, source_inode_id).unwrap();
        storage.put_data_handle_owner(target_handle, target_inode_id).unwrap();
        let file_handle = install_write_session(&filesystem, target_inode_id, mount_id);

        let failure = filesystem
            .rename_resolved(RenameInput {
                ctx: request_context(),
                src_path: "/source".to_string(),
                dst_path: "/target".to_string(),
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
                freshness: Freshness::default(),
            })
            .await
            .unwrap_err();

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EBusy));
        assert!(filesystem.write_session_for_handle(file_handle).is_some());
        assert_eq!(
            storage.get_dentry(parent_inode_id, "source").unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(target_inode_id)
        );
        assert!(storage.get_inode(target_inode_id).unwrap().is_some());
    }

    #[tokio::test]
    async fn expired_target_write_lease_does_not_leave_rename_permanently_busy() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(66);
        let group_name_value = group_name("g18");
        let parent_inode_id = InodeId::new(660);
        let source_inode_id = InodeId::new(661);
        let target_inode_id = InodeId::new(662);
        let source_handle = DataHandleId::new(663);
        let target_handle = DataHandleId::new(664);
        let lease_manager = Arc::new(crate::inode_lease::LeaseManager::new(0, 1_000));
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value)
            .with_lease_manager(Arc::clone(&lease_manager));
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .build();

        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_file(
                source_inode_id,
                FileAttrs::new(),
                mount_id,
                source_handle,
            ))
            .unwrap();
        storage
            .put_inode(&Inode::new_file(
                target_inode_id,
                FileAttrs::new(),
                mount_id,
                target_handle,
            ))
            .unwrap();
        storage.put_dentry(parent_inode_id, "source", source_inode_id).unwrap();
        storage.put_dentry(parent_inode_id, "target", target_inode_id).unwrap();
        storage
            .put_layout(source_inode_id, FileLayout::new(4096, 4096, 1))
            .unwrap();
        storage
            .put_layout(target_inode_id, FileLayout::new(4096, 4096, 1))
            .unwrap();
        storage.put_data_handle_owner(source_handle, source_inode_id).unwrap();
        storage.put_data_handle_owner(target_handle, target_inode_id).unwrap();
        let file_handle = install_write_session(&filesystem, target_inode_id, mount_id);

        assert!(!lease_manager.has_active_lease(target_inode_id));
        assert!(filesystem.write_session_for_handle(file_handle).is_some());

        filesystem
            .rename_resolved(RenameInput {
                ctx: request_context(),
                src_path: "/source".to_string(),
                dst_path: "/target".to_string(),
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
                freshness: Freshness::default(),
            })
            .await
            .expect("expired target lease must not leave rename permanently busy");

        assert_eq!(storage.get_dentry(parent_inode_id, "source").unwrap(), None);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(source_inode_id)
        );
        assert!(storage.get_inode(target_inode_id).unwrap().is_none());
        assert!(filesystem.write_session_for_handle(file_handle).is_none());
    }

    #[tokio::test]
    async fn rename_keeps_file_version() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(59);
        let group_name_value = group_name("g16");
        let parent_inode_id = InodeId::new(590);
        let source_inode_id = InodeId::new(591);
        let target_inode_id = InodeId::new(592);
        let source_handle = DataHandleId::new(593);
        let target_handle = DataHandleId::new(594);
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .build();

        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
            .unwrap();
        let mut source = Inode::new_file(source_inode_id, FileAttrs::new(), mount_id, source_handle);
        if let beryl_types::fs::InodeData::File {
            file_version,
            lease_epoch,
            ..
        } = &mut source.data
        {
            *file_version = Some(77);
            *lease_epoch = Some(900);
        }
        let mut target = Inode::new_file(target_inode_id, FileAttrs::new(), mount_id, target_handle);
        if let beryl_types::fs::InodeData::File {
            file_version,
            lease_epoch,
            ..
        } = &mut target.data
        {
            *file_version = Some(12);
            *lease_epoch = Some(12);
        }
        storage.put_inode(&source).unwrap();
        storage.put_inode(&target).unwrap();
        storage.put_dentry(parent_inode_id, "source", source_inode_id).unwrap();
        storage.put_dentry(parent_inode_id, "target", target_inode_id).unwrap();
        storage
            .put_layout(source_inode_id, FileLayout::new(4096, 4096, 1))
            .unwrap();
        storage
            .put_layout(target_inode_id, FileLayout::new(4096, 4096, 1))
            .unwrap();
        storage.put_data_handle_owner(source_handle, source_inode_id).unwrap();
        storage.put_data_handle_owner(target_handle, target_inode_id).unwrap();

        filesystem
            .rename_resolved(RenameInput {
                ctx: request_context(),
                src_path: "/source".to_string(),
                dst_path: "/target".to_string(),
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
                freshness: Freshness::default(),
            })
            .await
            .expect("same-mount overwrite rename should succeed");

        assert_eq!(storage.get_dentry(parent_inode_id, "source").unwrap(), None);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(stored_file_version(&storage, source_inode_id), Some(77));
        assert!(storage.get_inode(target_inode_id).unwrap().is_none());
        assert_eq!(storage.get_inode_by_data_handle(target_handle).unwrap(), None);
    }

    #[tokio::test]
    async fn rename_rejects_cross_mount() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let src_mount_id = MountId::new(57);
        let dst_mount_id = MountId::new(58);
        let src_parent_inode_id = InodeId::new(570);
        let dst_parent_inode_id = InodeId::new(580);
        let source_inode_id = InodeId::new(571);
        let filesystem = filesystem_builder_with_mount(src_mount_id, 9, &group_name("g15"))
            .with_storage(Arc::clone(&storage))
            .build();

        storage
            .put_inode(&Inode::new_dir(src_parent_inode_id, FileAttrs::new(), src_mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_dir(dst_parent_inode_id, FileAttrs::new(), dst_mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_file(
                source_inode_id,
                FileAttrs::new(),
                src_mount_id,
                DataHandleId::new(571),
            ))
            .unwrap();
        storage
            .put_dentry(src_parent_inode_id, "source", source_inode_id)
            .unwrap();

        let failure = filesystem
            .rename_resolved(RenameInput {
                ctx: request_context(),
                src_path: "/source".to_string(),
                dst_path: "/target".to_string(),
                src_parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
                freshness: Freshness::default(),
            })
            .await
            .unwrap_err();

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EXDev));
        assert_eq!(
            storage.get_dentry(src_parent_inode_id, "source").unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(storage.get_dentry(dst_parent_inode_id, "target").unwrap(), None);
    }
}
