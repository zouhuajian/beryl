// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::write_session::OpenWriteOutput;
use super::{missing_resolved_target_error, Freshness, FsResult, MetadataFileSystem, RequestContext};
use crate::error::MetadataError;
use crate::inode_lease::WriteMode;
use crate::observe;
use crate::raft::{Command, FsCommandResult};
use beryl_types::fs::{FileAttrs, InodeId};
use beryl_types::ids::DataHandleId;
use beryl_types::layout::FileLayout;

pub(crate) struct CreateFileArgs {
    pub(crate) path: String,
    // Deferring wire conversion errors until after write admission preserves failure precedence.
    pub(crate) parsed_attrs: Result<FileAttrs, MetadataError>,
    pub(crate) parsed_layout: Result<FileLayout, MetadataError>,
    pub(crate) parsed_mode: Result<(), MetadataError>,
    pub(crate) freshness: Freshness,
}

pub(crate) struct OpenWriteArgs {
    pub(crate) path: String,
    pub(crate) desired_len: Option<u64>,
    pub(crate) mode: WriteMode,
    pub(crate) freshness: Freshness,
}

pub(crate) struct CreatedFileOutput {
    pub(crate) inode_id: InodeId,
    pub(crate) data_handle_id: DataHandleId,
    pub(crate) layout: FileLayout,
    pub(crate) file_size: u64,
}

impl MetadataFileSystem {
    pub(crate) async fn create_file(&self, ctx: &RequestContext, args: CreateFileArgs) -> FsResult<CreatedFileOutput> {
        let path = args.path.clone();
        let result = self.create_file_inner(ctx, args).await;
        match &result {
            Ok(success) => {
                let payload = &success.payload;
                tracing::info!(
                    target: "metadata.state",
                    op = "CreateFile",
                    result = "committed",
                    error_code = "none",
                    client_id = %ctx.caller.client.client_id,
                    call_id = %ctx.caller.client.call_id,
                    path = %path,
                    inode_id = payload.inode_id.as_raw(),
                    data_handle_id = payload.data_handle_id.as_raw(),
                    layout_block_size = payload.layout.block_size,
                    layout_chunk_size = payload.layout.chunk_size,
                    replication = payload.layout.replication,
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "CreateFile committed"
                );
            }
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "CreateFile",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %path,
                "CreateFile rejected"
            ),
        }
        result
    }

    async fn create_file_inner(&self, ctx: &RequestContext, args: CreateFileArgs) -> FsResult<CreatedFileOutput> {
        if let Err(failure) = self.admission.check_meta_write(ctx).await {
            return self.failure_from_admission(failure);
        }

        let CreateFileArgs {
            path,
            parsed_attrs,
            parsed_layout,
            parsed_mode,
            freshness,
        } = args;
        match parsed_mode {
            Ok(()) => {}
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        }
        let attrs = match parsed_attrs {
            Ok(attrs) => attrs,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let layout = match parsed_layout {
            Ok(layout) => layout,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        if let Err(err) = validate_active_write_layout(&layout) {
            return self.failure_from_path_error(ctx, &path, err);
        }

        let path = match crate::path_resolver::PathResolver::normalize(&path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let resolved = match self.path_resolver.resolve_path(&path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let (Some(parent_inode_id), Some(name)) = (resolved.parent_inode_id, resolved.name.clone()) else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot operate on mount root".to_string()),
                Some(&resolved.mount_ctx),
            );
        };
        if let Err(failure) = self.admission.check_data_write(ctx, resolved.mount_ctx.mount_id).await {
            return self.failure_from_admission(failure);
        }
        let success = self
            .create_resolved(ctx, parent_inode_id, name, attrs, layout, freshness)
            .await?;
        Ok(success)
    }

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

    async fn create_resolved(
        &self,
        request_ctx: &RequestContext,
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
        layout: FileLayout,
        freshness: Freshness,
    ) -> FsResult<CreatedFileOutput> {
        if let Err(err) = validate_active_write_layout(&layout) {
            return self.failure_from_error(request_ctx, err, None, None);
        }

        let ctx = match self.route_ctx_for_write(request_ctx, &[parent_inode_id], freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let result = match self
            .propose_fs_write_command(Command::CreateFile {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                parent_inode_id,
                name,
                attrs,
                layout,
            })
            .await
        {
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

        match result {
            FsCommandResult::Ok(ok) => match (ok.inode_id, ok.data_handle_id, ok.attrs, ok.layout) {
                (Some(inode_id), Some(data_handle_id), Some(attrs), Some(layout)) => self.success(
                    request_ctx,
                    CreatedFileOutput {
                        inode_id,
                        data_handle_id,
                        layout,
                        file_size: attrs.size,
                    },
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                ),
                _ => self.failure_from_error(
                    request_ctx,
                    MetadataError::Internal("CreateFile returned an incomplete command result".to_string()),
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                ),
            },
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

pub(super) fn validate_active_write_layout(layout: &FileLayout) -> Result<(), MetadataError> {
    if layout.replication != 1 {
        return Err(MetadataError::InvalidArgument(
            "multi-replica write is not supported yet; replication must be 1".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
    async fn create_file_persists_valid_client_layout_shape() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(59);
        let group_name_value = group_name("g9");
        let parent_inode_id = InodeId::new(590);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
            .unwrap();
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_worker_manager(worker_manager_for_write_targets(&group_name_value))
            .build();
        let layout = FileLayout::with_block_format(8192, 1024, 1, beryl_types::BlockFormatId::FULL_EFFECTIVE);

        let success = filesystem
            .create_resolved(
                &request_context(),
                parent_inode_id,
                "file".to_string(),
                FileAttrs::new(),
                layout,
                Freshness::default(),
            )
            .await
            .expect("valid create layout should succeed");
        let inode_id = success.payload.inode_id;

        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
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
