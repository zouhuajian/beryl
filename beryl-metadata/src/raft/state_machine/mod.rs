// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Raft state machine implementation.
//!
//! Applies commands to the state machine and maintains consistency.

mod namespace;
mod worker;
mod write;

use crate::error::{to_fs_error_detail, MetadataError, MetadataResult};
use crate::raft::command::{Command, PublishMode};
use crate::raft::response::{
    ApplyRejection, CommandResult, FatalApplyError, FsCommandResult, FsErrnoResult, FsOkResult,
};
use crate::raft::storage::{
    BootstrapNamespaceState, DeleteTreeAtomicUpdate, DeleteTreeEntry, FileAllocation, InodeAllocation,
    RecursiveMkdirEntry, RenameAtomicUpdate, RenameOverwriteCleanup, RocksDBStorage,
};
use crate::raft::types::AppMetadataRaftState;
use crate::raft::RoutingDelta;
use beryl_types::fs::{Extent, FileAttrs, FsErrorCode, Inode, InodeData, InodeId};
use beryl_types::ids::{BlockId, BlockIndex, DataHandleId, MountId, WorkerId};
use beryl_types::layout::FileLayout;
use beryl_types::GroupName;
use std::sync::Arc;

fn meta_err_to_fs_errno(err: &MetadataError) -> Option<FsErrorCode> {
    match to_fs_error_detail(err.clone()).kind {
        beryl_common::error::rpc::ErrorKind::Fs(errno) => Some(errno),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    pub(crate) use super::*;
    pub(crate) use beryl_types::fs::{FileAttrs, Inode};
    pub(crate) use beryl_types::ids::{BlockId, DataHandleId, MountId, WorkerId};
    pub(crate) use beryl_types::layout::FileLayout;
    pub(crate) use tempfile::TempDir;

    impl AppRaftStateMachine {
        pub(crate) fn apply(&self, command: Command) -> MetadataResult<CommandResult> {
            match self.apply_committed(command, &AppMetadataRaftState::default()) {
                Ok(CommittedApply {
                    response: CommandResult::Rejected(rejection),
                    ..
                }) => Err(rejection.into_metadata_error()),
                Ok(applied) => Ok(applied.response),
                Err(fatal) => Err(fatal.into_inner()),
            }
        }
    }

    pub(crate) fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    pub(crate) fn bootstrap_command(group_name: &str, proposed_at_ms: u64) -> Command {
        Command::BootstrapNamespace {
            proposed_at_ms,
            group_name: GroupName::parse(group_name).unwrap(),
        }
    }

    pub(crate) fn expect_fs_ok(raw: CommandResult) -> FsOkResult {
        match raw {
            CommandResult::Fs(FsCommandResult::Ok(ok)) => ok,
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_fs_errno(raw: CommandResult, errno: FsErrorCode) {
        match raw {
            CommandResult::Fs(FsCommandResult::Err(err)) => assert_eq!(err.errno, errno),
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_mount_upserted(raw: CommandResult) -> crate::mount::MountEntry {
        match raw {
            CommandResult::MountUpserted(entry) => entry,
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_worker_upserted(raw: CommandResult) -> WorkerId {
        match raw {
            CommandResult::WorkerUpserted(worker_id) => worker_id,
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn extent(block_id: BlockId, file_offset: u64, len: u64) -> Extent {
        Extent {
            file_offset,
            block_id,
            block_offset: 0,
            len,
            content_revision: None,
            block_stamp: None,
        }
    }

    pub(crate) fn install_file_with_extents(
        storage: &RocksDBStorage,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        extents: Vec<Extent>,
        size: u64,
    ) -> Inode {
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), parent.mount_id, data_handle_id);
        inode.attrs.size = size;
        let next_block_index = extents
            .iter()
            .map(|extent| u64::from(extent.block_id.index.as_raw()) + 1)
            .max()
            .unwrap_or(0);
        let InodeData::File {
            extents: stored_extents,
            lease_epoch,
            next_block_index: stored_next_block_index,
            ..
        } = &mut inode.data
        else {
            unreachable!("new file must carry file data");
        };
        *stored_extents = extents;
        *lease_epoch = Some(1);
        *stored_next_block_index = next_block_index;
        storage.put_inode(&parent).unwrap();
        storage.put_inode(&inode).unwrap();
        storage.put_dentry(parent_inode_id, name, inode_id).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();
        inode
    }

    #[test]
    fn bootstrap_namespace_is_convergent_and_creates_one_root() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let first = expect_mount_upserted(sm.apply(bootstrap_command("root", 10)).unwrap());
        let second = expect_mount_upserted(sm.apply(bootstrap_command("root", 20)).unwrap());

        assert_eq!(first.mount_id, second.mount_id);
        assert_eq!(first.root_inode_id, second.root_inode_id);
        assert_eq!(storage.list_mounts().unwrap().len(), 1);
        assert_eq!(storage.max_inode_id().unwrap(), Some(crate::mount::ROOT_INODE_ID));
    }

    #[test]
    fn bootstrap_namespace_rejects_partial_authority_state() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        storage
            .put_inode(&Inode::new_dir(
                crate::mount::ROOT_INODE_ID,
                FileAttrs::new(),
                MountId::new(1),
            ))
            .unwrap();
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let error = sm.apply(bootstrap_command("root", 10)).unwrap_err();

        assert!(error.to_string().contains("partially initialized"));
        assert!(storage.list_mounts().unwrap().is_empty());
    }

    #[test]
    fn command_timestamp_does_not_regress_parent_time() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        expect_mount_upserted(sm.apply(bootstrap_command("root", 10)).unwrap());
        let mut root = storage.get_inode(crate::mount::ROOT_INODE_ID).unwrap().unwrap();
        root.attrs.update_timestamps(5_000);
        storage.put_inode(&root).unwrap();

        let response = sm
            .apply(Command::CreateDirectory {
                proposed_at_ms: 1_000,
                root_inode_id: crate::mount::ROOT_INODE_ID,
                components: vec!["child".to_string()],
                attrs: FileAttrs::new(),
                recursive: false,
            })
            .unwrap();
        let child_id = expect_fs_ok(response).inode_id.unwrap();

        assert_eq!(storage.get_inode(child_id).unwrap().unwrap().attrs.mtime_ms, 1_000);
        assert_eq!(
            storage
                .get_inode(crate::mount::ROOT_INODE_ID)
                .unwrap()
                .unwrap()
                .attrs
                .mtime_ms,
            5_000
        );
    }
}

/// Raft state machine.
pub(crate) struct AppRaftStateMachine {
    storage: Arc<RocksDBStorage>,
}

pub(crate) struct CommittedApply {
    pub(crate) response: CommandResult,
    pub(crate) routing_delta: RoutingDelta,
}

#[derive(Clone, Copy)]
enum RoutingIntent {
    None,
    Upsert,
}

impl From<&Command> for RoutingIntent {
    fn from(command: &Command) -> Self {
        match command {
            Command::BootstrapNamespace { .. } => Self::Upsert,
            _ => Self::None,
        }
    }
}

impl CommittedApply {
    fn new(intent: RoutingIntent, response: CommandResult) -> Self {
        let routing_delta = match (intent, &response) {
            (RoutingIntent::Upsert, CommandResult::MountUpserted(entry)) => RoutingDelta::Upsert(entry.clone()),
            _ => RoutingDelta::None,
        };
        Self {
            response,
            routing_delta,
        }
    }
}

struct PreparedRenameOverwrite {
    inode_id: InodeId,
    data_handle_id: Option<DataHandleId>,
}

struct PreparedRename {
    src_inode_id: InodeId,
    overwritten_target: Option<PreparedRenameOverwrite>,
    updated_src_parent: Option<Inode>,
    updated_dst_parent: Option<Inode>,
    updated_src_inode: Inode,
}

struct PreparedDeleteTree {
    updated_parent: Inode,
    entries: Vec<DeleteTreeEntry>,
    file_lease_epochs: Vec<(InodeId, u64)>,
}

struct DeleteTreePlan {
    root_mount_id: MountId,
    entries: Vec<DeleteTreeEntry>,
    file_lease_epochs: Vec<(InodeId, u64)>,
}

type PreparedUnlink = (InodeId, Option<DataHandleId>, Inode, FsOkResult);

impl AppRaftStateMachine {
    pub fn new(storage: Arc<RocksDBStorage>) -> Self {
        Self { storage }
    }

    /// Apply a command to the state machine.
    pub(crate) fn apply_committed(
        &self,
        command: Command,
        raft_state: &AppMetadataRaftState,
    ) -> Result<CommittedApply, FatalApplyError> {
        let routing_intent = RoutingIntent::from(&command);
        let outcome: MetadataResult<CommandResult> = (|| match command {
            Command::BootstrapNamespace {
                proposed_at_ms,
                group_name,
            } => {
                let result = self.apply_bootstrap_namespace(group_name, proposed_at_ms, raft_state)?;
                Ok(CommandResult::MountUpserted(result))
            }
            Command::RegisterWorkerDescriptor {
                proposed_at_ms: _,
                group_name,
                worker_id,
                address,
                worker_net_protocol,
                fault_domain,
            } => {
                let result = self.apply_register_worker(
                    group_name,
                    worker_id,
                    address,
                    worker_net_protocol,
                    fault_domain,
                    raft_state,
                )?;
                Ok(CommandResult::WorkerUpserted(result))
            }
            Command::CreateDirectory {
                proposed_at_ms,
                root_inode_id,
                components,
                attrs,
                recursive,
            } => {
                let result = if recursive {
                    self.apply_create_directory(root_inode_id, components, attrs, proposed_at_ms, raft_state)?
                } else {
                    let mut components = components;
                    if components.len() != 1 {
                        return Err(MetadataError::InvalidArgument(
                            "non-recursive CreateDirectory requires exactly one path component".to_string(),
                        ));
                    }
                    self.apply_mkdir(
                        root_inode_id,
                        components.pop().expect("checked one component"),
                        attrs,
                        proposed_at_ms,
                        raft_state,
                    )?
                };
                Ok(CommandResult::Fs(result))
            }
            Command::CreateFile {
                proposed_at_ms,
                parent_inode_id,
                name,
                attrs,
                layout,
            } => {
                let result = self.apply_create(parent_inode_id, name, attrs, layout, proposed_at_ms, raft_state)?;
                Ok(CommandResult::Fs(result))
            }
            Command::Delete {
                proposed_at_ms,
                parent_inode_id,
                name,
                expected_inode_id,
                expected_file_lease_epochs,
                recursive,
            } => {
                let result = self.apply_delete(
                    parent_inode_id,
                    name,
                    expected_inode_id,
                    expected_file_lease_epochs,
                    recursive,
                    proposed_at_ms,
                    raft_state,
                )?;
                Ok(CommandResult::Fs(result))
            }
            Command::Rename {
                proposed_at_ms,
                src_parent_inode_id,
                src_name,
                expected_src_inode_id,
                dst_parent_inode_id,
                dst_name,
                expected_dst_inode_id,
                expected_dst_lease_epoch,
                flags,
            } => {
                let result = self.apply_rename(
                    src_parent_inode_id,
                    src_name,
                    expected_src_inode_id,
                    dst_parent_inode_id,
                    dst_name,
                    expected_dst_inode_id,
                    expected_dst_lease_epoch,
                    flags,
                    proposed_at_ms,
                    raft_state,
                )?;
                Ok(CommandResult::Fs(result))
            }
            Command::SetAttr {
                proposed_at_ms,
                inode_id,
                mask,
                attrs,
            } => {
                let result = self.apply_set_attr(inode_id, mask, attrs, proposed_at_ms, raft_state)?;
                Ok(CommandResult::Fs(result))
            }
            Command::AcquireWriteLease {
                proposed_at_ms: _,
                inode_id,
                expected_lease_epoch,
            } => {
                let result = self.apply_acquire_write_lease(inode_id, expected_lease_epoch, raft_state)?;
                Ok(CommandResult::Fs(result))
            }
            Command::AllocateBlock {
                inode_id,
                data_handle_id,
                lease_epoch,
            } => {
                let block_id = self.apply_allocate_block(inode_id, data_handle_id, lease_epoch, raft_state)?;
                Ok(CommandResult::BlockAllocated(block_id))
            }
            Command::EndWriteLease {
                proposed_at_ms: _,
                inode_id,
                lease_epoch,
            } => {
                let result = self.apply_end_write_lease(inode_id, lease_epoch, raft_state)?;
                Ok(CommandResult::Fs(result))
            }
            Command::PublishFile {
                proposed_at_ms,
                inode_id,
                extents,
                target_size,
                expected_content_revision,
                expected_file_size,
                lease_epoch,
                mode,
            } => {
                let result = self.apply_publish_file(
                    inode_id,
                    extents,
                    target_size,
                    expected_content_revision,
                    expected_file_size,
                    lease_epoch,
                    mode,
                    proposed_at_ms,
                    raft_state,
                )?;
                Ok(CommandResult::Fs(result))
            }
        })();

        match outcome {
            Ok(response) => Ok(CommittedApply::new(routing_intent, response)),
            Err(error) => {
                let rejection = ApplyRejection::from_metadata_error(error)?;
                let response = CommandResult::Rejected(rejection);
                self.storage
                    .commit_applied_state(raft_state)
                    .map_err(FatalApplyError::new)?;
                Ok(CommittedApply::new(routing_intent, response))
            }
        }
    }

    fn mutation_timestamp(inode: &Inode, proposed_at_ms: u64) -> u64 {
        proposed_at_ms.max(inode.attrs.mtime_ms).max(inode.attrs.ctime_ms)
    }

    fn fs_command_result(result: MetadataResult<FsOkResult>) -> FsCommandResult {
        match result {
            Ok(ok) => FsCommandResult::Ok(ok),
            Err(err) => {
                let errno = meta_err_to_fs_errno(&err).unwrap_or(FsErrorCode::EInval);
                FsCommandResult::Err(FsErrnoResult {
                    errno,
                    message: err.to_string(),
                })
            }
        }
    }

    fn persist_fs_apply_result(
        &self,
        result: FsCommandResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        self.storage.commit_applied_state(raft_state)?;
        Ok(result)
    }

    fn persist_fs_error(
        &self,
        error: MetadataError,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let error = match ApplyRejection::from_metadata_error(error) {
            Ok(rejection) => rejection.into_metadata_error(),
            Err(fatal) => return Err(fatal.into_inner()),
        };
        self.persist_fs_apply_result(Self::fs_command_result(Err(error)), raft_state)
    }

    fn extent_end(extent: &Extent) -> MetadataResult<u64> {
        extent.file_offset.checked_add(extent.len).ok_or_else(|| {
            MetadataError::InvalidArgument(format!(
                "Extent end overflows: file_offset={}, len={}",
                extent.file_offset, extent.len
            ))
        })
    }

    fn extent_matches_visible(existing: &[Extent], candidate: &Extent) -> bool {
        Self::matching_visible_extent(existing, candidate).is_some()
    }

    fn matching_visible_extent<'a>(existing: &'a [Extent], candidate: &Extent) -> Option<&'a Extent> {
        existing.iter().find(|visible| {
            visible.block_id == candidate.block_id
                && visible.file_offset == candidate.file_offset
                && visible.block_offset == candidate.block_offset
                && visible.len == candidate.len
        })
    }

    fn visible_suffix_matches(existing: &[Extent], requested: &[Extent], start_offset: u64, target_size: u64) -> bool {
        let mut visible = existing.iter().filter(|extent| extent.file_offset >= start_offset);
        let mut expected_offset = start_offset;
        for candidate in requested {
            let Some(extent) = visible.next() else {
                return false;
            };
            if extent.file_offset != expected_offset
                || Self::matching_visible_extent(std::slice::from_ref(extent), candidate).is_none()
            {
                return false;
            }
            let Some(end) = extent.file_offset.checked_add(extent.len) else {
                return false;
            };
            if end > target_size {
                return false;
            }
            expected_offset = end;
        }
        expected_offset == target_size && visible.next().is_none()
    }

    fn stamp_extents(extents: &mut [Extent], existing: &[Extent], content_revision: u64) {
        for extent in extents {
            if let Some(visible) = Self::matching_visible_extent(existing, extent) {
                if let Some(block_stamp) = visible.block_stamp {
                    extent.content_revision = Some(content_revision);
                    extent.block_stamp = Some(block_stamp);
                    continue;
                }
            }
            extent.content_revision = Some(content_revision);
            // The Raft apply boundary assigns the metadata-authoritative stamp
            // that direct readers must present to workers for newly visible
            // blocks.
            extent.block_stamp = Some(content_revision);
        }
    }

    fn validate_contiguous_extents(
        extents: &[Extent],
        start_offset: u64,
        target_size: u64,
        label: &str,
    ) -> MetadataResult<()> {
        let mut expected_offset = start_offset;
        for extent in extents {
            if extent.file_offset != expected_offset {
                return Err(MetadataError::InvalidArgument(format!(
                    "{} extent file_offset mismatch: expected {}, got {}",
                    label, expected_offset, extent.file_offset
                )));
            }
            expected_offset = Self::extent_end(extent)?;
        }
        if expected_offset != target_size {
            return Err(MetadataError::InvalidArgument(format!(
                "{} target_size mismatch: expected {}, got {}",
                label, expected_offset, target_size
            )));
        }
        Ok(())
    }

    /// Return the suffix that still needs publication for append-style commits.
    fn append_extents_not_already_visible(
        existing: &[Extent],
        requested: &[Extent],
        current_size: u64,
        target_size: u64,
        label: &str,
    ) -> MetadataResult<Vec<Extent>> {
        let mut expected_offset = current_size;
        let mut publish = Vec::new();
        for extent in requested {
            if Self::extent_end(extent)? <= current_size && Self::extent_matches_visible(existing, extent) {
                continue;
            }
            if extent.file_offset != expected_offset {
                return Err(MetadataError::InvalidArgument(format!(
                    "{} extent file_offset mismatch: expected {}, got {}",
                    label, expected_offset, extent.file_offset
                )));
            }
            expected_offset = Self::extent_end(extent)?;
            publish.push(extent.clone());
        }
        if expected_offset != target_size {
            return Err(MetadataError::InvalidArgument(format!(
                "{} target_size mismatch: expected {}, got {}",
                label, expected_offset, target_size
            )));
        }
        Ok(publish)
    }

    fn next_content_revision(inode_id: InodeId, current_content_revision: Option<u64>) -> MetadataResult<u64> {
        current_content_revision.unwrap_or(0).checked_add(1).ok_or_else(|| {
            MetadataError::Internal(format!(
                "content_revision overflow for inode {} at {:?}",
                inode_id, current_content_revision
            ))
        })
    }
}
