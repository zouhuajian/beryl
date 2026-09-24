// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Raft state machine implementation.
//!
//! Applies commands to the state machine and maintains consistency.

mod detached_root;
mod namespace;
mod write;

use crate::error::{MetadataError, MetadataResult};
use crate::inode::InodeAttrs;
use crate::inode::{Inode, InodeKind};
use crate::raft::command::Command;
use crate::raft::response::{
    ApplyRejection, ApplySuccess, DetachedRootReclaimResult, FatalApplyError, RaftApplyResult,
};
use crate::raft::storage::{
    BootstrapNamespaceState, CreateFileReplayRecord, DetachedRoot, DetachedRootReclaimEntry, DetachedRootReclaimUpdate,
    RecursiveMkdirEntry, RenameAtomicUpdate, RocksDBStorage,
};
use crate::raft::types::AppMetadataRaftState;
use crate::session_registry::CreateFileOperationId;
use beryl_types::ids::{BlockId, BlockIndex, InodeId, MountId};
use beryl_types::{ContentGeneration, GroupName, LeaseEpoch};
use std::sync::Arc;

/// Raft state machine.
pub(crate) struct AppRaftStateMachine {
    storage: Arc<RocksDBStorage>,
}

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
    ) -> Result<RaftApplyResult, FatalApplyError> {
        let outcome: MetadataResult<ApplySuccess> = (|| match command {
            Command::BootstrapNamespace {
                proposed_at_ms,
                group_name,
            } => {
                let result = self.apply_bootstrap_namespace(group_name, proposed_at_ms, raft_state)?;
                Ok(ApplySuccess::MountUpserted(result))
            }
            Command::RegisterWorkerDescriptor {
                group_name,
                worker_id,
                address,
            } => {
                let descriptor = crate::worker::WorkerDescriptor {
                    group_name,
                    worker_id,
                    address,
                };
                self.storage.register_worker_atomic(&descriptor, raft_state)?;
                Ok(ApplySuccess::WorkerUpserted)
            }
            Command::CreateDirectory {
                proposed_at_ms,
                root_inode_id,
                components,
                recursive,
            } => {
                let (inode_id, attrs) = if recursive {
                    self.apply_create_directory(root_inode_id, components, proposed_at_ms, raft_state)?
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
                        proposed_at_ms,
                        raft_state,
                    )?
                };
                Ok(ApplySuccess::DirectoryEnsured { inode_id, attrs })
            }
            Command::CreateFile {
                proposed_at_ms,
                operation_id,
                request_deadline_ms,
                session_expires_at_ms,
                mount_id,
                mount_root_inode_id,
                relative_components,
                block_size,
            } => {
                let result = self.apply_create(
                    operation_id,
                    request_deadline_ms,
                    session_expires_at_ms,
                    mount_id,
                    mount_root_inode_id,
                    relative_components,
                    block_size,
                    proposed_at_ms,
                    raft_state,
                )?;
                Ok(ApplySuccess::FileCreated {
                    inode_id: result.inode_id,
                    block_size: result.block_size,
                    lease_epoch: LeaseEpoch::new(1),
                    expires_at_ms: result.expires_at_ms,
                    generation: ContentGeneration::new(0),
                })
            }
            Command::Delete {
                proposed_at_ms,
                mount_id,
                mount_root_inode_id,
                relative_components,
                expected_inode_id,
                expected_file_lease_epoch,
                recursive,
            } => {
                self.apply_delete(
                    mount_id,
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
                proposed_at_ms,
                inode_id,
                expected_lease_epoch,
            } => {
                self.apply_acquire_write_lease(inode_id, expected_lease_epoch, proposed_at_ms, raft_state)?;
                Ok(ApplySuccess::WriteLeaseAcquired)
            }
            Command::AllocateBlock { inode_id, lease_epoch } => {
                let block_id = self.apply_allocate_block(inode_id, lease_epoch, raft_state)?;
                Ok(ApplySuccess::BlockAllocated(block_id))
            }
            Command::EndWriteLease { inode_id, lease_epoch } => {
                self.apply_end_write_lease(inode_id, lease_epoch, raft_state)?;
                Ok(ApplySuccess::WriteLeaseEnded)
            }
            Command::PublishFile {
                proposed_at_ms,
                inode_id,
                publication,
            } => {
                let generation = self.apply_publish_file(inode_id, publication, proposed_at_ms, raft_state)?;
                Ok(ApplySuccess::FilePublished { generation })
            }
            Command::CommitFile {
                proposed_at_ms,
                inode_id,
                client_id,
                call_id,
                publication,
            } => {
                let ended_epoch = publication
                    .lease_epoch
                    .checked_next()
                    .ok_or_else(|| MetadataError::InvalidArgument("write lease epoch overflow".into()))?;
                let generation = self.apply_commit_file(
                    inode_id,
                    (client_id, call_id),
                    publication,
                    ended_epoch,
                    proposed_at_ms,
                    raft_state,
                )?;
                Ok(ApplySuccess::FileCommitted { generation })
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
            Ok(success) => Ok(Ok(success)),
            Err(error) => {
                let rejection = ApplyRejection::from_metadata_error(error)?;
                self.storage
                    .commit_applied_state(raft_state)
                    .map_err(FatalApplyError::new)?;
                Ok(Err(rejection))
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    pub(crate) use super::*;
    pub(crate) use crate::inode::Inode;
    pub(crate) use crate::inode::InodeAttrs;
    use crate::mount::MountEntry;
    use crate::raft::response::ApplyRejectionKind;
    pub(crate) use beryl_types::ids::{BlockId, InodeId, MountId};
    use beryl_types::WorkerId;

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
                Ok(Ok(success)) => Ok(success),
                Ok(Err(rejection)) => Err(rejection.into_metadata_error()),
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

    pub(crate) fn expect_directory_ensured(raw: ApplySuccess) -> (InodeId, InodeAttrs) {
        match raw {
            ApplySuccess::DirectoryEnsured { inode_id, attrs } => (inode_id, attrs),
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_file_created(raw: ApplySuccess) -> (InodeId, u32) {
        match raw {
            ApplySuccess::FileCreated {
                inode_id, block_size, ..
            } => (inode_id, block_size),
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

    pub(crate) fn expect_mount_upserted(raw: ApplySuccess) -> MountEntry {
        match raw {
            ApplySuccess::MountUpserted(entry) => entry,
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_worker_upserted(raw: ApplySuccess) {
        match raw {
            ApplySuccess::WorkerUpserted => (),
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    pub(crate) fn expect_write_lease_acquired(raw: ApplySuccess) {
        match raw {
            ApplySuccess::WriteLeaseAcquired => (),
            other => panic!("unexpected apply response: {other:?}"),
        }
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
        assert!(storage.get_root_mount().unwrap().is_some());
        assert_eq!(storage.max_inode_id().unwrap(), Some(crate::mount::ROOT_INODE_ID));
    }

    #[test]
    fn bootstrap_namespace_rejects_partial_authority_state() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        storage
            .put_inode(&Inode::new_dir(
                crate::mount::ROOT_INODE_ID,
                InodeAttrs::new(),
                MountId::new(1),
            ))
            .unwrap();
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let error = sm.apply(bootstrap_command("root", 10)).unwrap_err();

        assert!(error.to_string().contains("partially initialized"));
        assert!(storage.get_root_mount().unwrap().is_none());
    }

    #[test]
    fn command_timestamp_does_not_regress_parent_time() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        expect_mount_upserted(sm.apply(bootstrap_command("root", 10)).unwrap());
        let mut root = storage.get_inode(crate::mount::ROOT_INODE_ID).unwrap().unwrap();
        root.attrs.initialize(5_000);
        storage.put_inode(&root).unwrap();

        let response = sm
            .apply(Command::CreateDirectory {
                proposed_at_ms: 1_000,
                root_inode_id: crate::mount::ROOT_INODE_ID,
                components: vec!["child".to_string()],
                recursive: false,
            })
            .unwrap();
        let child_id = expect_directory_ensured(response).0;

        assert_eq!(storage.get_inode(child_id).unwrap().unwrap().attrs.modify_time, 1_000);
        assert_eq!(
            storage
                .get_inode(crate::mount::ROOT_INODE_ID)
                .unwrap()
                .unwrap()
                .attrs
                .modify_time,
            5_000
        );
    }

    #[test]
    fn register_worker_apply_replays_and_replaces_durable_descriptor() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let worker_id = WorkerId::new(760);

        let first = Command::RegisterWorkerDescriptor {
            group_name: group_name("root"),
            worker_id,
            address: "127.0.0.1:17060".to_string(),
        };
        let second = Command::RegisterWorkerDescriptor {
            group_name: group_name("root"),
            worker_id,
            address: "127.0.0.1:17061".to_string(),
        };

        expect_worker_upserted(sm.apply(first.clone()).unwrap());
        expect_worker_upserted(sm.apply(first).unwrap());
        expect_worker_upserted(sm.apply(second).unwrap());
        let stored = storage
            .get_worker_in_group(&group_name("root"), worker_id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.address, "127.0.0.1:17061");
    }
}
