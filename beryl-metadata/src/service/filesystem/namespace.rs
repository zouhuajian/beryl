// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::{Freshness, FsResult, MetadataFileSystem, RequestContext};
use crate::error::MetadataError;
use crate::observe;
use crate::raft::{Command, FsCommandResult};
use beryl_types::fs::{FileAttrs, InodeId};
use std::sync::atomic::Ordering;

pub(crate) struct CreateDirectoryArgs {
    pub(crate) path: String,
    // Deferring wire conversion errors until after write admission preserves failure precedence.
    pub(crate) parsed_attrs: Result<FileAttrs, MetadataError>,
    pub(crate) recursive: bool,
    pub(crate) freshness: Freshness,
}

pub(crate) struct CreateDirectoryOutput {
    pub(crate) inode_id: InodeId,
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
        let result = if recursive {
            self.create_directory_recursive(ctx, &path, attrs, freshness).await
        } else {
            self.create_directory_once(ctx, &path, attrs, freshness).await
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
                inode_id = success.payload.inode_id.as_raw(),
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
        path: &str,
        attrs: FileAttrs,
        freshness: Freshness,
    ) -> FsResult<CreateDirectoryOutput> {
        let resolved = match self.path_resolver.resolve_path(path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, path, err),
        };
        let (Some(parent_inode_id), Some(name)) = (resolved.parent_inode_id, resolved.name.clone()) else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot operate on mount root".to_string()),
                Some(&resolved.mount_ctx),
            );
        };

        self.execute_create_directory(
            ctx,
            Command::CreateDirectory {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                root_inode_id: parent_inode_id,
                components: vec![name],
                attrs,
                recursive: false,
            },
            freshness,
        )
        .await
    }

    async fn create_directory_recursive(
        &self,
        ctx: &RequestContext,
        path: &str,
        attrs: FileAttrs,
        freshness: Freshness,
    ) -> FsResult<CreateDirectoryOutput> {
        let (mount_ctx, components) = match self.path_resolver.resolve_mount_components(path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, path, err),
        };
        if components.is_empty() {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot operate on mount root".to_string()),
                Some(&mount_ctx),
            );
        }

        self.execute_create_directory(
            ctx,
            Command::CreateDirectory {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                root_inode_id: mount_ctx.root_inode_id,
                components,
                attrs,
                recursive: true,
            },
            freshness,
        )
        .await
    }

    async fn execute_create_directory(
        &self,
        ctx: &RequestContext,
        command: Command,
        freshness: Freshness,
    ) -> FsResult<CreateDirectoryOutput> {
        let root_inode_id = match &command {
            Command::CreateDirectory { root_inode_id, .. } => *root_inode_id,
            _ => unreachable!("execute_create_directory requires CreateDirectory"),
        };
        let routed = match self.route_ctx_for_write(ctx, &[root_inode_id], freshness) {
            Ok(routed) => routed,
            Err(err) => return Err(err),
        };
        let result = match self.propose_fs_write_command(command).await {
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
                    CreateDirectoryOutput { inode_id, attrs },
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
        let Some(expected_src_inode_id) = src_resolved.inode_id else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::NotFound(format!("Source not found: {src_path}")),
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
        let expected_dst_lease_epoch = match dst_resolved.inode_id {
            Some(dst_inode_id) => match self.read_inode(dst_inode_id) {
                Ok(Some(inode)) => match &inode.data {
                    beryl_types::fs::InodeData::File { lease_epoch, .. } => Some(lease_epoch.unwrap_or(0)),
                    _ => None,
                },
                Ok(None) => None,
                Err(err) => return self.failure_from_resolved_path_error(ctx, err, Some(&dst_resolved.mount_ctx)),
            },
            None => None,
        };

        let result = self
            .execute_rename(
                ctx,
                Command::Rename {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    src_parent_inode_id,
                    src_name,
                    expected_src_inode_id,
                    dst_parent_inode_id,
                    dst_name,
                    expected_dst_inode_id: dst_resolved.inode_id,
                    expected_dst_lease_epoch,
                    flags: args.flags,
                },
                args.freshness,
            )
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

    async fn execute_rename(
        &self,
        request_ctx: &RequestContext,
        command: Command,
        freshness: Freshness,
    ) -> FsResult<()> {
        let Command::Rename {
            src_parent_inode_id,
            src_name: _,
            expected_src_inode_id: _,
            dst_parent_inode_id,
            ref dst_name,
            expected_dst_inode_id: _,
            expected_dst_lease_epoch: _,
            flags,
            ..
        } = command
        else {
            unreachable!("execute_rename requires Rename")
        };
        let supported_mask: u32 = 0x1;
        if flags & !supported_mask != 0 {
            return self.failure_from_error(
                request_ctx,
                MetadataError::NotSupported(format!("Unsupported rename flags: {flags}")),
                None,
                None,
            );
        }

        let src_parent_inode = match self.read_inode(src_parent_inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::NotFound(format!("Source parent inode not found: {src_parent_inode_id}")),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(request_ctx, err, None, None),
        };
        let dst_parent_inode = match self.read_inode(dst_parent_inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::NotFound(format!("Destination parent inode not found: {dst_parent_inode_id}")),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(request_ctx, err, None, None),
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
                request_ctx,
                MetadataError::CrossMountRename(format!(
                    "Cross-mount rename not allowed: src_mount={:?}, dst_mount={:?}",
                    src_parent_inode.mount_id, dst_parent_inode.mount_id
                )),
                group_name,
                mount_epoch,
            );
        }

        let ctx = match self.route_ctx_for_write(request_ctx, &[src_parent_inode_id, dst_parent_inode_id], freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        match self.read_dentry(dst_parent_inode_id, dst_name) {
            Ok(Some(dst_inode_id)) => match self.read_inode(dst_inode_id) {
                Ok(Some(inode)) if inode.kind.is_file() => {
                    if self.has_active_write(dst_inode_id) {
                        return self.fatal_fs_failure(
                            request_ctx,
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
                        request_ctx,
                        err,
                        Some(ctx.namespace_owner_group_name.clone()),
                        Some(ctx.mount_epoch),
                    );
                }
            },
            Ok(None) => {}
            Err(err) => {
                return self.failure_from_error(
                    request_ctx,
                    err,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
        }

        let result = match self.propose_fs_write_command(command).await {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(
                    request_ctx,
                    err,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        self.rename_result(request_ctx, &ctx, result)
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
            .execute_rename(
                &request_context(),
                Command::Rename {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    expected_src_inode_id: source_inode_id,
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "target".to_string(),
                    expected_dst_inode_id: Some(target_inode_id),
                    expected_dst_lease_epoch: Some(0),
                    flags: 0,
                },
                Freshness::default(),
            )
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
            .execute_rename(
                &request_context(),
                Command::Rename {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    expected_src_inode_id: source_inode_id,
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "target".to_string(),
                    expected_dst_inode_id: Some(target_inode_id),
                    expected_dst_lease_epoch: Some(0),
                    flags: 0,
                },
                Freshness::default(),
            )
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
    async fn rename_keeps_content_revision() {
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
            content_revision,
            lease_epoch,
            ..
        } = &mut source.data
        {
            *content_revision = Some(77);
            *lease_epoch = Some(900);
        }
        let mut target = Inode::new_file(target_inode_id, FileAttrs::new(), mount_id, target_handle);
        if let beryl_types::fs::InodeData::File {
            content_revision,
            lease_epoch,
            ..
        } = &mut target.data
        {
            *content_revision = Some(12);
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
            .execute_rename(
                &request_context(),
                Command::Rename {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    expected_src_inode_id: source_inode_id,
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "target".to_string(),
                    expected_dst_inode_id: Some(target_inode_id),
                    expected_dst_lease_epoch: Some(12),
                    flags: 0,
                },
                Freshness::default(),
            )
            .await
            .expect("same-mount overwrite rename should succeed");

        assert_eq!(storage.get_dentry(parent_inode_id, "source").unwrap(), None);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(stored_content_revision(&storage, source_inode_id), Some(77));
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
            .execute_rename(
                &request_context(),
                Command::Rename {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    src_parent_inode_id,
                    src_name: "source".to_string(),
                    expected_src_inode_id: source_inode_id,
                    dst_parent_inode_id,
                    dst_name: "target".to_string(),
                    expected_dst_inode_id: None,
                    expected_dst_lease_epoch: None,
                    flags: 0,
                },
                Freshness::default(),
            )
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
