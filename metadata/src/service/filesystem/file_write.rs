// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::write_session::{OpenWriteInput, OpenWriteOutput};
use super::{
    missing_resolved_target_error, Freshness, FsResult, FsSuccess, MetadataFileSystem, RequestContext, SessionKey,
    WriteCommandKind,
};
use crate::error::MetadataError;
use crate::inode_lease::WriteMode;
use crate::observe;
use crate::raft::{Command, FsCommandResult};
use types::fs::{FileAttrs, InodeId};
use types::ids::DataHandleId;
use types::layout::FileLayout;
use types::WriteTarget;

#[derive(Clone, Debug)]
struct CreateInput {
    ctx: RequestContext,
    parent_inode_id: InodeId,
    name: String,
    attrs: FileAttrs,
    layout: FileLayout,
    freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
struct CreateOutput {
    inode_id: Option<InodeId>,
}

pub(crate) enum CreateFileMode {
    CreateNew,
    CreateOrOverwrite,
}

pub(crate) struct CreateFileArgs {
    pub(crate) path: String,
    // Deferring wire conversion errors until after write admission preserves failure precedence.
    pub(crate) parsed_attrs: Result<FileAttrs, MetadataError>,
    pub(crate) parsed_layout: Result<FileLayout, MetadataError>,
    pub(crate) parsed_mode: Result<CreateFileMode, MetadataError>,
    pub(crate) desired_len: Option<u64>,
    pub(crate) freshness: Freshness,
}

struct ValidatedCreateFileArgs {
    path: String,
    attrs: FileAttrs,
    layout: FileLayout,
    mode: CreateFileMode,
    desired_len: Option<u64>,
    freshness: Freshness,
}

pub(crate) struct AppendFileArgs {
    pub(crate) path: String,
    pub(crate) desired_len: Option<u64>,
    pub(crate) freshness: Freshness,
}

pub(crate) struct OpenedWriteOutput {
    pub(crate) inode_id: InodeId,
    pub(crate) data_handle_id: DataHandleId,
    pub(crate) session_key: SessionKey,
    pub(crate) layout: FileLayout,
    pub(crate) write_targets: Vec<WriteTarget>,
    pub(crate) base_size: u64,
    pub(crate) expires_at_ms: u64,
}

impl MetadataFileSystem {
    pub(crate) async fn create_file(&self, ctx: &RequestContext, args: CreateFileArgs) -> FsResult<OpenedWriteOutput> {
        let path = args.path.clone();
        let desired_len = args.desired_len;
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
                    file_handle = payload.session_key.file_handle,
                    lease_id = payload.session_key.lease_id.as_raw(),
                    lease_epoch = payload.session_key.lease_epoch,
                    layout_block_size = payload.layout.block_size(),
                    layout_chunk_size = payload.layout.chunk_size(),
                    replication = payload.layout.replication(),
                    initial_target_count = payload.write_targets.len(),
                    desired_len,
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

    async fn create_file_inner(&self, ctx: &RequestContext, args: CreateFileArgs) -> FsResult<OpenedWriteOutput> {
        if let Err(failure) = self.admission.check_meta_write(ctx).await {
            return self.failure_from_admission(failure);
        }

        let CreateFileArgs {
            path,
            parsed_attrs,
            parsed_layout,
            parsed_mode,
            desired_len,
            freshness,
        } = args;
        let mode = match parsed_mode {
            Ok(mode) => mode,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let attrs = match parsed_attrs {
            Ok(attrs) => attrs,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let layout = match parsed_layout {
            Ok(layout) => layout,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let args = ValidatedCreateFileArgs {
            path,
            attrs,
            layout,
            mode,
            desired_len,
            freshness,
        };
        if let Err(err) = validate_active_write_layout(&args.layout) {
            return self.failure_from_path_error(ctx, &args.path, err);
        }

        let inode_id = if matches!(args.mode, CreateFileMode::CreateOrOverwrite) {
            match self.path_resolver.resolve_path(&args.path) {
                Ok(resolved) => match resolved.inode_id {
                    Some(inode_id) => {
                        if let Err(failure) = self.admission.check_data_write(ctx, resolved.mount_ctx.mount_id).await {
                            return self.failure_from_admission(failure);
                        }
                        inode_id
                    }
                    None => self.create_new_file_inode(ctx, &args).await?.payload,
                },
                Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
            }
        } else {
            self.create_new_file_inode(ctx, &args).await?.payload
        };

        self.open_write_resolved(OpenWriteInput {
            ctx: ctx.clone(),
            inode_id,
            desired_len: args.desired_len,
            mode: WriteMode::Write,
            freshness: args.freshness,
        })
        .await
        .map(Self::opened_write_success)
    }

    async fn create_new_file_inode(&self, ctx: &RequestContext, args: &ValidatedCreateFileArgs) -> FsResult<InodeId> {
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
        if let Err(failure) = self.admission.check_data_write(ctx, resolved.mount_ctx.mount_id).await {
            return self.failure_from_admission(failure);
        }
        if let Some(failure) = self.preflight_open_write_runtime(
            ctx,
            args.desired_len,
            args.layout,
            Some(resolved.mount_ctx.owner_group_name.clone()),
            Some(resolved.mount_ctx.mount_epoch),
        ) {
            return Err(failure);
        }

        let success = self
            .create_resolved(CreateInput {
                ctx: ctx.clone(),
                parent_inode_id,
                name,
                attrs: args.attrs.clone(),
                layout: args.layout,
                freshness: args.freshness,
            })
            .await?;
        match success.payload.inode_id {
            Some(inode_id) => Ok(FsSuccess {
                payload: inode_id,
                group_name: success.group_name,
                mount_epoch: success.mount_epoch,
                route_epoch: success.route_epoch,
                state: success.state,
            }),
            None => self.failure_from_resolved_path_error(
                ctx,
                MetadataError::Internal("create did not return inode_id".to_string()),
                Some(&resolved.mount_ctx),
            ),
        }
    }

    pub(crate) async fn append_file(&self, ctx: &RequestContext, args: AppendFileArgs) -> FsResult<OpenedWriteOutput> {
        let path = args.path.clone();
        let result = self.append_file_inner(ctx, args).await;
        match &result {
            Ok(success) => {
                let payload = &success.payload;
                tracing::info!(
                    target: "metadata.state",
                    op = "AppendFile",
                    result = "opened",
                    error_code = "none",
                    client_id = %ctx.caller.client.client_id,
                    call_id = %ctx.caller.client.call_id,
                    path = %path,
                    inode_id = payload.inode_id.as_raw(),
                    data_handle_id = payload.data_handle_id.as_raw(),
                    file_handle = payload.session_key.file_handle,
                    lease_id = payload.session_key.lease_id.as_raw(),
                    lease_epoch = payload.session_key.lease_epoch,
                    initial_target_count = payload.write_targets.len(),
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "AppendFile opened"
                );
            }
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "AppendFile",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %path,
                "AppendFile rejected"
            ),
        }
        result
    }

    async fn append_file_inner(&self, ctx: &RequestContext, args: AppendFileArgs) -> FsResult<OpenedWriteOutput> {
        if let Err(failure) = self.admission.check_meta_write(ctx).await {
            return self.failure_from_admission(failure);
        }
        let resolved = match self.path_resolver.resolve_path(&args.path) {
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
        self.open_write_resolved(OpenWriteInput {
            ctx: ctx.clone(),
            inode_id,
            desired_len: args.desired_len,
            mode: WriteMode::Append,
            freshness: args.freshness,
        })
        .await
        .map(Self::opened_write_success)
    }

    fn opened_write_success(success: FsSuccess<OpenWriteOutput>) -> FsSuccess<OpenedWriteOutput> {
        let output = success.payload;
        FsSuccess {
            payload: OpenedWriteOutput {
                inode_id: output.inode_id,
                data_handle_id: output.data_handle_id,
                session_key: output.session_key,
                layout: output.layout,
                write_targets: output.write_targets,
                base_size: output.base_size,
                expires_at_ms: output.expires_at_ms,
            },
            group_name: success.group_name,
            mount_epoch: success.mount_epoch,
            route_epoch: success.route_epoch,
            state: success.state,
        }
    }

    async fn create_resolved(&self, req: CreateInput) -> FsResult<CreateOutput> {
        if let Err(err) = validate_active_write_layout(&req.layout) {
            return self.failure_from_error(&req.ctx, err, None, None);
        }

        let ctx = match self.route_ctx_for_write(
            &req.ctx,
            WriteCommandKind::Create,
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
                WriteCommandKind::Create,
                Command::new(
                    dedup,
                    crate::raft::proposal_timestamp_ms(),
                    crate::raft::Mutation::CreateFile {
                        parent_inode_id: req.parent_inode_id,
                        name: req.name,
                        attrs: req.attrs,
                        layout: req.layout,
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
                CreateOutput { inode_id: ok.inode_id },
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
}

pub(super) fn validate_active_write_layout(layout: &FileLayout) -> Result<(), MetadataError> {
    if layout.replication() != 1 {
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
        storage
            .put_layout(inode_id, FileLayout::try_new(4096, 4096, 1).unwrap())
            .unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g7"))
            .with_storage(storage)
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
            .expect_err("missing worker manager should fail open_write");

        assert!(failure.error.message.contains("Worker manager not available"));
        assert!(filesystem.lease_manager().get_active_lease(inode_id).is_none());
    }

    #[tokio::test]
    async fn open_write_targets_use_inode_current_data_handle() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(51);
        let group_name_value = group_name("g9");
        let inode_id = InodeId::new(510);
        let data_handle_id = DataHandleId::new(9510);
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage
            .put_layout(inode_id, FileLayout::try_new(4096, 4096, 1).unwrap())
            .unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name_value)
            .with_storage(storage)
            .with_worker_manager(worker_manager_for_write_targets(&group_name_value))
            .build();

        let success = filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id,
                desired_len: Some(4096),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("open_write should succeed");

        assert_ne!(inode_id.as_raw(), data_handle_id.as_raw());
        assert!(!success.payload.write_targets.is_empty());
        for target in &success.payload.write_targets {
            assert_eq!(target.block_id.data_handle_id, data_handle_id);
            assert_eq!(target.block_size, 4096);
            assert_eq!(target.effective_len, 4096);
            assert_eq!(target.chunk_size, 4096);
            assert_eq!(target.block_format_id, types::BlockFormatId::CURRENT_FOR_NEW_FILE);
        }
        assert_eq!(
            success.payload.session_key.fencing_token.block_id.data_handle_id,
            data_handle_id
        );
        let session = filesystem
            .write_session_for_handle(success.payload.session_key.file_handle)
            .expect("session should be stored");
        assert_eq!(session.data_handle_id, data_handle_id);
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
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id,
                desired_len: Some(4096),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
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
        let layout = FileLayout::try_with_block_format(8192, 1024, 1, types::BlockFormatId::FULL_EFFECTIVE).unwrap();

        let success = filesystem
            .create_resolved(CreateInput {
                ctx: request_context(),
                parent_inode_id,
                name: "file".to_string(),
                attrs: FileAttrs::new(),
                layout,
                freshness: Freshness::default(),
            })
            .await
            .expect("valid create layout should succeed");
        let inode_id = success.payload.inode_id.expect("created inode id");

        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
    }

    #[tokio::test]
    async fn create_replay_advances_applied_state_without_allocating_again() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(60);
        let group_name_value = group_name("g10");
        let parent_inode_id = InodeId::new(600);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
            .unwrap();
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(Arc::clone(&raft_node))
            .with_worker_manager(worker_manager_for_write_targets(&group_name_value))
            .build();
        let request = CreateInput {
            ctx: request_context(),
            parent_inode_id,
            name: "replayed-file".to_string(),
            attrs: FileAttrs::new(),
            layout: FileLayout::try_new(4096, 4096, 1).unwrap(),
            freshness: Freshness::default(),
        };

        let first = filesystem.create_resolved(request.clone()).await.unwrap();
        let first_applied = raft_node.get_last_applied_state_id().unwrap();
        let next_inode_after_first = storage.get_next_inode_id().unwrap();
        let replay = filesystem.create_resolved(request.clone()).await.unwrap();
        let replay_applied = raft_node.get_last_applied_state_id().unwrap();
        let mut mismatch = request;
        mismatch.name = "different-file".to_string();
        let mismatch_failure = filesystem.create_resolved(mismatch).await.unwrap_err();
        let mismatch_applied = raft_node.get_last_applied_state_id().unwrap();

        assert_eq!(replay.payload.inode_id, first.payload.inode_id);
        assert!(replay_applied.index > first_applied.index);
        assert_fail(&mismatch_failure.error, ErrorKind::Fs(FsErrorCode::EInval));
        assert!(mismatch_applied.index > replay_applied.index);
        assert_eq!(storage.get_next_inode_id().unwrap(), next_inode_after_first);
        assert_eq!(storage.get_dentry(parent_inode_id, "different-file").unwrap(), None);
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
        storage
            .put_layout(inode_id, FileLayout::try_new(4096, 4096, 2).unwrap())
            .unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name_value)
            .with_storage(storage)
            .with_worker_manager(worker_manager_for_write_targets(&group_name_value))
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

    #[test]
    fn open_write_preflight_rejects_placement_without_authoritative_group() {
        let filesystem = filesystem_builder_without_mount()
            .with_worker_manager(worker_manager_for_write_targets(&group_name("root")))
            .build();

        let failure = filesystem
            .preflight_open_write_runtime(
                &request_context(),
                Some(4096),
                FileLayout::try_new(4096, 4096, 1).unwrap(),
                None,
                None,
            )
            .expect("missing authoritative group must reject placement preflight");

        assert!(
            failure
                .error
                .message
                .contains("OpenWrite preflight worker lookup requires authoritative metadata group"),
            "unexpected error: {}",
            failure.error.message
        );
    }
}
