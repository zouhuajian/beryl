// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::{Freshness, FsResult, MetadataFileSystem, RequestContext, WriteCommandKind};
use crate::error::MetadataError;
use crate::observe;
use crate::raft::{AppDataResponse, Command, FsCommandResult};
use types::fs::InodeId;

#[derive(Clone, Debug)]
struct UnlinkInput {
    ctx: RequestContext,
    parent_inode_id: InodeId,
    name: String,
    freshness: Freshness,
}

#[derive(Clone, Debug)]
struct DeleteEmptyDirInput {
    ctx: RequestContext,
    parent_inode_id: InodeId,
    name: String,
    freshness: Freshness,
}

#[derive(Clone, Debug)]
struct DeleteTreeInput {
    ctx: RequestContext,
    parent_inode_id: InodeId,
    name: String,
    freshness: Freshness,
}

pub(crate) struct DeleteArgs {
    pub(crate) path: String,
    pub(crate) recursive: bool,
    pub(crate) freshness: Freshness,
}

impl MetadataFileSystem {
    pub(crate) async fn delete(&self, ctx: &RequestContext, args: DeleteArgs) -> FsResult<()> {
        if let Err(failure) = self.admission.check_meta_write(ctx).await {
            return self.failure_from_admission(failure);
        }

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
        let target_inode = match resolved.inode_id {
            Some(target_inode_id) => match self.read_inode(target_inode_id) {
                Ok(Some(inode)) => Some(inode),
                Ok(None) => {
                    return self.failure_from_resolved_path_error(
                        ctx,
                        MetadataError::NotFound(format!("Target inode not found: {}", target_inode_id)),
                        Some(&resolved.mount_ctx),
                    )
                }
                Err(err) => return self.failure_from_resolved_path_error(ctx, err, Some(&resolved.mount_ctx)),
            },
            None => None,
        };

        let result = if args.recursive && target_inode.as_ref().map(|inode| inode.kind.is_dir()).unwrap_or(true) {
            self.delete_tree_resolved(DeleteTreeInput {
                ctx: ctx.clone(),
                parent_inode_id,
                name,
                freshness: args.freshness,
            })
            .await
        } else if target_inode.as_ref().is_some_and(|inode| inode.kind.is_dir()) {
            self.delete_empty_dir_resolved(DeleteEmptyDirInput {
                ctx: ctx.clone(),
                parent_inode_id,
                name,
                freshness: args.freshness,
            })
            .await
        } else {
            if target_inode.is_none() {
                return self.failure_from_resolved_path_error(
                    ctx,
                    MetadataError::NotFound(format!("Entry not found: {}", name)),
                    Some(&resolved.mount_ctx),
                );
            }
            self.unlink_resolved(UnlinkInput {
                ctx: ctx.clone(),
                parent_inode_id,
                name,
                freshness: args.freshness,
            })
            .await
        };

        match &result {
            Ok(_) => tracing::info!(
                target: "metadata.state",
                op = "Delete",
                result = "committed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %args.path,
                inode_id = target_inode.as_ref().map(|inode| inode.inode_id.as_raw()),
                parent_inode_id = parent_inode_id.as_raw(),
                recursive = args.recursive,
                "Delete committed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "Delete",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %args.path,
                parent_inode_id = parent_inode_id.as_raw(),
                recursive = args.recursive,
                "Delete rejected"
            ),
        }
        result
    }

    async fn unlink_resolved(&self, req: UnlinkInput) -> FsResult<()> {
        let ctx = match self.route_ctx_for_write(
            &req.ctx,
            WriteCommandKind::Unlink,
            &[req.parent_inode_id],
            req.freshness,
        ) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        match self.read_dentry(req.parent_inode_id, &req.name) {
            Ok(Some(child_inode_id)) => match self.read_inode(child_inode_id) {
                Ok(Some(inode)) if inode.kind.is_file() => {
                    if self.has_active_write(child_inode_id) {
                        return self.fatal_fs_failure(
                            &req.ctx,
                            types::fs::FsErrorCode::EBusy,
                            format!("File has an active write lease: {}", child_inode_id),
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
                WriteCommandKind::Unlink,
                Command::new(
                    dedup,
                    crate::raft::proposal_timestamp_ms(),
                    crate::raft::Mutation::Unlink {
                        parent_inode_id: req.parent_inode_id,
                        name: req.name,
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
            FsCommandResult::Ok(_) => self.success(
                &req.ctx,
                (),
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

    async fn delete_empty_dir_resolved(&self, req: DeleteEmptyDirInput) -> FsResult<()> {
        let ctx = match self.route_ctx_for_write(
            &req.ctx,
            WriteCommandKind::DeleteEmptyDir,
            &[req.parent_inode_id],
            req.freshness,
        ) {
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
                WriteCommandKind::DeleteEmptyDir,
                Command::new(
                    dedup,
                    crate::raft::proposal_timestamp_ms(),
                    crate::raft::Mutation::DeleteEmptyDir {
                        parent_inode_id: req.parent_inode_id,
                        name: req.name,
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
            FsCommandResult::Ok(_) => self.success(
                &req.ctx,
                (),
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

    async fn delete_tree_resolved(&self, req: DeleteTreeInput) -> FsResult<()> {
        let ctx = match self.route_ctx_for_write(
            &req.ctx,
            WriteCommandKind::DeleteTree,
            &[req.parent_inode_id],
            req.freshness,
        ) {
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
        let command = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::DeleteTree {
                parent_inode_id: req.parent_inode_id,
                name: req.name.clone(),
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
                return self.delete_tree_result(&req, &ctx, result);
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

        if let Err(err) = self.preflight_delete_tree_runtime(&self.storage, req.parent_inode_id, &req.name) {
            return self.failure_from_error(
                &req.ctx,
                err,
                Some(ctx.namespace_owner_group_name.clone()),
                Some(ctx.mount_epoch),
            );
        }

        let result = match self
            .propose_fs_write_command(WriteCommandKind::DeleteTree, command)
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

        self.delete_tree_result(&req, &ctx, result)
    }

    fn delete_tree_result(
        &self,
        req: &DeleteTreeInput,
        ctx: &super::RoutedFsWriteCtx,
        result: FsCommandResult,
    ) -> FsResult<()> {
        match result {
            FsCommandResult::Ok(_) => self.success(
                &req.ctx,
                (),
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

    fn preflight_delete_tree_runtime(
        &self,
        storage: &crate::raft::RocksDBStorage,
        parent_inode_id: types::fs::InodeId,
        name: &str,
    ) -> Result<(), MetadataError> {
        let Some(root_inode_id) = self.read_dentry(parent_inode_id, name)? else {
            return Ok(());
        };
        let Some(root_inode) = self.read_inode(root_inode_id)? else {
            return Ok(());
        };
        let mount_id = root_inode.mount_id;
        let mut stack = vec![(root_inode_id, root_inode)];

        while let Some((inode_id, inode)) = stack.pop() {
            if inode.mount_id != mount_id {
                continue;
            }
            if inode.kind.is_file() && self.has_active_write(inode_id) {
                return Err(MetadataError::Busy(format!(
                    "File has an active write lease: {}",
                    inode_id
                )));
            }
            if inode.kind.is_dir() {
                for (_, child_inode_id) in storage.list_dentries(inode_id)? {
                    if let Some(child_inode) = self.read_inode(child_inode_id)? {
                        stack.push((child_inode_id, child_inode));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::filesystem::test_support::*;

    #[tokio::test]
    async fn delete_file_with_active_write_session_returns_busy_without_namespace_mutation() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(55);
        let group_name_value = group_name("g13");
        let parent_inode_id = InodeId::new(550);
        let inode_id = InodeId::new(551);
        let data_handle_id = DataHandleId::new(552);
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
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage.put_dentry(parent_inode_id, "busy", inode_id).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();
        let file_handle = install_write_session(&filesystem, inode_id, mount_id);

        let failure = filesystem
            .unlink_resolved(UnlinkInput {
                ctx: request_context(),
                parent_inode_id,
                name: "busy".to_string(),
                freshness: Freshness::default(),
            })
            .await
            .unwrap_err();

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EBusy));
        assert!(filesystem.write_session_for_handle(file_handle).is_some());
        assert_eq!(storage.get_dentry(parent_inode_id, "busy").unwrap(), Some(inode_id));
        assert!(storage.get_inode(inode_id).unwrap().is_some());
    }

    #[tokio::test]
    async fn expired_write_lease_does_not_leave_delete_permanently_busy() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(65);
        let group_name_value = group_name("g17");
        let parent_inode_id = InodeId::new(650);
        let inode_id = InodeId::new(651);
        let data_handle_id = DataHandleId::new(652);
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
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage.put_dentry(parent_inode_id, "expired", inode_id).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();
        let file_handle = install_write_session(&filesystem, inode_id, mount_id);

        assert!(!lease_manager.has_active_lease(inode_id));
        assert!(filesystem.write_session_for_handle(file_handle).is_some());

        filesystem
            .unlink_resolved(UnlinkInput {
                ctx: request_context(),
                parent_inode_id,
                name: "expired".to_string(),
                freshness: Freshness::default(),
            })
            .await
            .expect("expired lease must not leave delete permanently busy");

        assert_eq!(storage.get_dentry(parent_inode_id, "expired").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert!(filesystem.write_session_for_handle(file_handle).is_none());
    }
}
