// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Raft state machine implementation.
//!
//! Applies commands to the state machine and maintains consistency.

mod detached_root;
mod namespace;
mod worker;
mod write;

use crate::error::{MetadataError, MetadataResult};
use crate::raft::command::{Command, PublishMode};
use crate::raft::response::{
    ApplyRejection, ApplySuccess, DetachedRootReclaimResult, FatalApplyError, RaftApplyResult,
};
use crate::raft::storage::{
    BootstrapNamespaceState, DetachedRoot, DetachedRootReclaimEntry, DetachedRootReclaimUpdate, InodeAllocation,
    RecursiveMkdirEntry, RenameAtomicUpdate, RenameOverwriteCleanup, RocksDBStorage,
};
use crate::raft::types::AppMetadataRaftState;
use crate::raft::RoutingDelta;
use beryl_types::fs::{Extent, FileAttrs, Inode, InodeData, InodeId};
use beryl_types::ids::{BlockId, BlockIndex, MountId, WorkerId};
use beryl_types::layout::FileLayout;
use beryl_types::{GroupName, MAX_FILE_EXTENTS};
use std::sync::Arc;

/// Raft state machine.
pub(crate) struct AppRaftStateMachine {
    storage: Arc<RocksDBStorage>,
}

/// Persisted apply outcome and any routing publication it makes authoritative.
///
/// The storage adapter must publish `routing_delta` before exposing the new
/// in-memory applied state so readers cannot observe an index ahead of routing.
pub(crate) struct CommittedApply {
    pub(crate) response: RaftApplyResult,
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
    fn new(intent: RoutingIntent, response: RaftApplyResult) -> Self {
        let routing_delta = match (intent, &response) {
            (RoutingIntent::Upsert, Ok(ApplySuccess::MountUpserted(entry))) => RoutingDelta::Upsert(entry.clone()),
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
}

struct PreparedRename {
    src_inode_id: InodeId,
    overwritten_target: Option<PreparedRenameOverwrite>,
    updated_src_parent: Option<Inode>,
    updated_dst_parent: Option<Inode>,
    updated_src_inode: Inode,
}

type PreparedUnlink = (InodeId, Inode);

impl AppRaftStateMachine {
    pub fn new(storage: Arc<RocksDBStorage>) -> Self {
        Self { storage }
    }

    /// Apply one committed application command under the supplied Raft state.
    ///
    /// Successful mutations persist their authority change and applied index
    /// atomically. Deterministic domain errors commit only the applied index and
    /// become `ApplyRejection`; storage, infrastructure, and invariant failures
    /// return `FatalApplyError` without advancing applied state.
    pub(crate) fn apply_committed(
        &self,
        command: Command,
        raft_state: &AppMetadataRaftState,
    ) -> Result<CommittedApply, FatalApplyError> {
        let routing_intent = RoutingIntent::from(&command);
        let outcome: MetadataResult<ApplySuccess> = (|| match command {
            Command::BootstrapNamespace {
                proposed_at_ms,
                group_name,
            } => {
                let result = self.apply_bootstrap_namespace(group_name, proposed_at_ms, raft_state)?;
                Ok(ApplySuccess::MountUpserted(result))
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
                Ok(ApplySuccess::WorkerUpserted(result))
            }
            Command::CreateDirectory {
                proposed_at_ms,
                root_inode_id,
                components,
                attrs,
                recursive,
            } => {
                let (inode_id, attrs) = if recursive {
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
                Ok(ApplySuccess::DirectoryEnsured { inode_id, attrs })
            }
            Command::CreateFile {
                proposed_at_ms,
                parent_inode_id,
                name,
                attrs,
                layout,
            } => {
                let inode_id = self.apply_create(parent_inode_id, name, attrs, layout, proposed_at_ms, raft_state)?;
                Ok(ApplySuccess::FileCreated { inode_id, layout })
            }
            Command::Delete {
                proposed_at_ms,
                mount_id,
                expected_mount_epoch,
                mount_root_inode_id,
                relative_components,
                expected_inode_id,
                expected_file_lease_epoch,
                recursive,
            } => {
                self.apply_delete(
                    mount_id,
                    expected_mount_epoch,
                    mount_root_inode_id,
                    relative_components,
                    expected_inode_id,
                    expected_file_lease_epoch,
                    recursive,
                    proposed_at_ms,
                    raft_state,
                )?;
                Ok(ApplySuccess::DeleteApplied)
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
                self.apply_rename(
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
                Ok(ApplySuccess::RenameApplied)
            }
            Command::AcquireWriteLease {
                proposed_at_ms: _,
                inode_id,
                expected_lease_epoch,
            } => {
                let lease_epoch = self.apply_acquire_write_lease(inode_id, expected_lease_epoch, raft_state)?;
                Ok(ApplySuccess::WriteLeaseAcquired { inode_id, lease_epoch })
            }
            Command::AllocateBlock { inode_id, lease_epoch } => {
                let block_id = self.apply_allocate_block(inode_id, lease_epoch, raft_state)?;
                Ok(ApplySuccess::BlockAllocated(block_id))
            }
            Command::EndWriteLease {
                proposed_at_ms: _,
                inode_id,
                lease_epoch,
            } => {
                let lease_epoch = self.apply_end_write_lease(inode_id, lease_epoch, raft_state)?;
                Ok(ApplySuccess::WriteLeaseEnded { inode_id, lease_epoch })
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
                if extents.len() > MAX_FILE_EXTENTS {
                    return Err(MetadataError::ResourceExhausted(format!(
                        "PublishFile extent count {} exceeds maximum {}",
                        extents.len(),
                        MAX_FILE_EXTENTS
                    )));
                }
                let content_revision = self.apply_publish_file(
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
                Ok(ApplySuccess::FilePublished {
                    inode_id,
                    content_revision,
                })
            }
            Command::ReclaimDetachedRoots {
                candidate_root_inode_ids,
                max_entries,
                max_batch_bytes,
            } => {
                let result = self.apply_reclaim_detached_roots(
                    candidate_root_inode_ids,
                    max_entries,
                    max_batch_bytes,
                    raft_state,
                )?;
                Ok(ApplySuccess::DetachedRootsReclaimed(result))
            }
        })();

        match outcome {
            Ok(success) => Ok(CommittedApply::new(routing_intent, Ok(success))),
            Err(error) => {
                let rejection = ApplyRejection::from_metadata_error(error)?;
                self.storage
                    .commit_applied_state(raft_state)
                    .map_err(FatalApplyError::new)?;
                Ok(CommittedApply::new(routing_intent, Err(rejection)))
            }
        }
    }

    fn mutation_timestamp(inode: &Inode, proposed_at_ms: u64) -> u64 {
        proposed_at_ms.max(inode.attrs.mtime_ms).max(inode.attrs.ctime_ms)
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

#[cfg(test)]
pub(crate) mod tests {
    pub(crate) use super::*;
    use crate::raft::response::ApplyRejectionKind;
    pub(crate) use beryl_types::fs::{FileAttrs, Inode};
    pub(crate) use beryl_types::ids::{BlockId, InodeId, MountId, WorkerId};
    pub(crate) use beryl_types::layout::FileLayout;
    pub(crate) use tempfile::TempDir;

    impl AppRaftStateMachine {
        pub(crate) fn apply(&self, command: Command) -> MetadataResult<ApplySuccess> {
            self.apply_with_raft_state(command, &AppMetadataRaftState::default())
        }

        pub(crate) fn apply_with_raft_state(
            &self,
            command: Command,
            raft_state: &AppMetadataRaftState,
        ) -> MetadataResult<ApplySuccess> {
            match self.apply_committed(command, raft_state) {
                Ok(CommittedApply {
                    response: Ok(success), ..
                }) => Ok(success),
                Ok(CommittedApply {
                    response: Err(rejection),
                    ..
                }) => Err(rejection.into_metadata_error()),
                Err(fatal) => Err(fatal.as_inner().clone()),
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

    pub(crate) fn expect_directory_ensured(raw: ApplySuccess) -> (InodeId, FileAttrs) {
        match raw {
            ApplySuccess::DirectoryEnsured { inode_id, attrs } => (inode_id, attrs),
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_file_created(raw: ApplySuccess) -> (InodeId, FileLayout) {
        match raw {
            ApplySuccess::FileCreated { inode_id, layout } => (inode_id, layout),
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_delete_applied(raw: ApplySuccess) {
        assert!(
            matches!(&raw, ApplySuccess::DeleteApplied),
            "unexpected apply response: {raw:?}"
        );
    }

    pub(crate) fn expect_apply_rejection(result: MetadataResult<ApplySuccess>, expected: ApplyRejectionKind) {
        let error = result.expect_err("command must be rejected");
        let rejection = ApplyRejection::from_metadata_error(error).expect("expected deterministic apply rejection");
        assert_eq!(rejection.kind, expected);
    }

    pub(crate) fn expect_mount_upserted(raw: ApplySuccess) -> crate::mount::MountEntry {
        match raw {
            ApplySuccess::MountUpserted(entry) => entry,
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_worker_upserted(raw: ApplySuccess) -> WorkerId {
        match raw {
            ApplySuccess::WorkerUpserted(worker_id) => worker_id,
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_write_lease_acquired(raw: ApplySuccess) -> (InodeId, u64) {
        match raw {
            ApplySuccess::WriteLeaseAcquired { inode_id, lease_epoch } => (inode_id, lease_epoch),
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_write_lease_ended(raw: ApplySuccess) -> (InodeId, u64) {
        match raw {
            ApplySuccess::WriteLeaseEnded { inode_id, lease_epoch } => (inode_id, lease_epoch),
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_file_published(raw: ApplySuccess) -> (InodeId, u64) {
        match raw {
            ApplySuccess::FilePublished {
                inode_id,
                content_revision,
            } => (inode_id, content_revision),
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
        extents: Vec<Extent>,
        size: u64,
    ) -> Inode {
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), parent.mount_id);
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
        let child_id = expect_directory_ensured(response).0;

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
