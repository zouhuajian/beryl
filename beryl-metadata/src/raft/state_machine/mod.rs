// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Raft state machine implementation.
//!
//! Applies commands to the state machine and maintains consistency.

mod namespace;
mod worker;
mod write;

use crate::error::{to_fs_error_detail, MetadataError, MetadataResult};
use crate::raft::command::{Command, FileCommitMode, Mutation};
use crate::raft::response::{
    AppDataResponse, ApplyRejection, FatalApplyError, FsCommandResult, FsErrnoResult, FsOkResult, MountCommandResult,
    WorkerCommandResult,
};
use crate::raft::storage::{
    AppliedResult, AuthorityBatch, BootstrapNamespaceState, DeleteTreeAtomicUpdate, DeleteTreeEntry, FileAllocation,
    InodeAllocation, RenameAtomicUpdate, RenameOverwriteCleanup, RocksDBStorage,
};
use crate::raft::types::{AppMetadataRaftState, CommandFingerprint, DedupKey};
use crate::raft::RoutingDelta;
use beryl_types::fs::{Extent, FileAttrs, FsErrorCode, Inode, InodeData, InodeId};
use beryl_types::ids::{DataHandleId, LeaseId, MountId, WorkerId};
use beryl_types::layout::FileLayout;
use beryl_types::GroupName;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::warn;

fn meta_err_to_fs_errno(err: &MetadataError) -> Option<FsErrorCode> {
    match to_fs_error_detail(err.clone()).kind {
        beryl_common::error::rpc::ErrorKind::Fs(errno) => Some(errno),
        _ => None,
    }
}

/// Raft state machine.
pub(crate) struct AppRaftStateMachine {
    storage: Arc<RocksDBStorage>,
}

pub(crate) struct CommittedApply {
    pub(crate) response: AppDataResponse,
    pub(crate) routing_delta: RoutingDelta,
}

#[derive(Clone, Copy)]
enum RoutingIntent {
    None,
    Upsert,
}

impl From<&Mutation> for RoutingIntent {
    fn from(mutation: &Mutation) -> Self {
        match mutation {
            Mutation::BootstrapNamespace { .. } => Self::Upsert,
            _ => Self::None,
        }
    }
}

impl CommittedApply {
    fn new(intent: RoutingIntent, response: AppDataResponse) -> Self {
        let routing_delta = match (intent, &response) {
            (RoutingIntent::Upsert, AppDataResponse::Mount(MountCommandResult::Upserted(entry))) => {
                RoutingDelta::Upsert(entry.clone())
            }
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
}

struct DeleteTreePlan {
    root_mount_id: MountId,
    entries: Vec<DeleteTreeEntry>,
}

type PreparedUnlink = (InodeId, Option<DataHandleId>, Inode, FsOkResult);
type PreparedCloseWrite = (Inode, FileLayout, FsOkResult);
type PreparedSyncWrite = (Inode, FileLayout, FsOkResult, bool);

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
        command.validate_version().map_err(FatalApplyError::new)?;
        let routing_intent = RoutingIntent::from(command.mutation());
        let dedup_key = command.dedup_key().clone();
        let fingerprint = command.fingerprint();

        // Dedup hit returns the persisted replay record without re-running the mutation.
        if let Some(applied) = self
            .storage
            .get_applied_result(&dedup_key)
            .map_err(FatalApplyError::new)?
        {
            if applied.fingerprint != fingerprint {
                crate::metrics::DEDUP_LOOKUP_MISMATCH_TOTAL.fetch_add(1, Ordering::Relaxed);
                warn!(
                    client_id = %dedup_key.client_id.as_raw(),
                    call_id = %dedup_key.call_id,
                    stored_fp = %applied.fingerprint.0,
                    new_fp = %fingerprint.0,
                    "dedup fingerprint mismatch"
                );
                let rejection = ApplyRejection::from_metadata_error(MetadataError::InvalidArgument(format!(
                    "call_id {} reused with different command payload",
                    dedup_key.call_id
                )))?;
                self.storage
                    .commit_applied_state(raft_state)
                    .map_err(FatalApplyError::new)?;
                return Ok(CommittedApply::new(
                    routing_intent,
                    AppDataResponse::Rejected(rejection),
                ));
            }
            self.storage
                .commit_applied_state(raft_state)
                .map_err(FatalApplyError::new)?;
            return Ok(CommittedApply::new(routing_intent, applied.result));
        }

        let (proposed_at_ms, mutation) = command.into_parts();
        let outcome: MetadataResult<AppDataResponse> = (|| {
            match mutation {
                Mutation::BootstrapNamespace { group_name } => {
                    let result = self.apply_bootstrap_namespace(
                        group_name,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Mount(result))
                }
                Mutation::RegisterWorkerDescriptor {
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
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Worker(result))
                }
                Mutation::Mkdir {
                    parent_inode_id,
                    name,
                    attrs,
                } => {
                    // Create/mkdir/rename persist namespace mutation, replay result together.
                    let result = self.apply_mkdir(
                        parent_inode_id,
                        name,
                        attrs,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Fs(result))
                }
                Mutation::CreateFile {
                    parent_inode_id,
                    name,
                    attrs,
                    layout,
                } => {
                    let result = self.apply_create(
                        parent_inode_id,
                        name,
                        attrs,
                        layout,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Fs(result))
                }
                Mutation::Unlink { parent_inode_id, name } => {
                    let result = self.apply_unlink(
                        parent_inode_id,
                        name,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Fs(result))
                }
                Mutation::DeleteEmptyDir { parent_inode_id, name } => {
                    let result = self.apply_delete_empty_dir(
                        parent_inode_id,
                        name,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Fs(result))
                }
                Mutation::DeleteTree { parent_inode_id, name } => {
                    let result = self.apply_delete_tree(
                        parent_inode_id,
                        name,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Fs(result))
                }
                Mutation::Rename {
                    src_parent_inode_id,
                    src_name,
                    dst_parent_inode_id,
                    dst_name,
                    flags,
                } => {
                    let result = self.apply_rename(
                        src_parent_inode_id,
                        src_name,
                        dst_parent_inode_id,
                        dst_name,
                        flags,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Fs(result))
                }
                Mutation::SetAttr { inode_id, mask, attrs } => {
                    let result = self.apply_set_attr(
                        inode_id,
                        mask,
                        attrs,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Fs(result))
                }
                Mutation::CloseWrite {
                    inode_id,
                    extents,
                    final_size,
                    lease_id,
                    open_epoch,
                    lease_epoch,
                    commit_mode,
                } => {
                    let result = self.apply_close_write(
                        inode_id,
                        extents,
                        final_size,
                        lease_id,
                        open_epoch,
                        lease_epoch,
                        commit_mode,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Fs(result))
                }
                Mutation::SyncWrite {
                    inode_id,
                    extents,
                    target_size,
                    lease_id,
                    open_epoch,
                    lease_epoch,
                    commit_mode,
                } => {
                    let (result, _) = self.apply_sync_write(
                        inode_id,
                        extents,
                        target_size,
                        lease_id,
                        open_epoch,
                        lease_epoch,
                        commit_mode,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Fs(result))
                }
                Mutation::Truncate {
                    inode_id,
                    new_size,
                    lease_id,
                    lease_epoch,
                } => {
                    let result = self.apply_truncate(
                        inode_id,
                        new_size,
                        lease_id,
                        lease_epoch,
                        proposed_at_ms,
                        &dedup_key,
                        fingerprint,
                        raft_state,
                    )?;
                    Ok(AppDataResponse::Fs(result))
                }
            }
        })();

        match outcome {
            Ok(response) => Ok(CommittedApply::new(routing_intent, response)),
            Err(error) => {
                let rejection = ApplyRejection::from_metadata_error(error)?;
                let response = AppDataResponse::Rejected(rejection);
                let applied_result = Self::make_applied_result(fingerprint, response.clone());
                self.storage
                    .commit_apply_batch(AuthorityBatch::default(), &dedup_key, applied_result, raft_state)
                    .map_err(FatalApplyError::new)?;
                Ok(CommittedApply::new(routing_intent, response))
            }
        }
    }

    fn make_applied_result(fingerprint: CommandFingerprint, result: AppDataResponse) -> AppliedResult {
        AppliedResult {
            fingerprint,
            result,
            created_at_ms: Self::replay_record_timestamp_ms(),
            size_bytes: 0,
        }
    }

    fn replay_record_timestamp_ms() -> u64 {
        u64::MAX
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
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage
            .put_apply_result_atomic(dedup_key, applied_result, raft_state)?;
        Ok(result)
    }

    fn persist_fs_error(
        &self,
        error: MetadataError,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let error = match ApplyRejection::from_metadata_error(error) {
            Ok(rejection) => rejection.into_metadata_error(),
            Err(fatal) => return Err(fatal.into_inner()),
        };
        self.persist_fs_apply_result(Self::fs_command_result(Err(error)), dedup_key, fingerprint, raft_state)
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

    fn stamp_extents(extents: &mut [Extent], existing: &[Extent], file_version: u64) {
        for extent in extents {
            if let Some(visible) = Self::matching_visible_extent(existing, extent) {
                if let Some(block_stamp) = visible.block_stamp {
                    extent.file_version = Some(file_version);
                    extent.block_stamp = Some(block_stamp);
                    continue;
                }
            }
            extent.file_version = Some(file_version);
            // The Raft apply boundary assigns the metadata-authoritative stamp
            // that direct readers must present to workers for newly visible
            // blocks.
            extent.block_stamp = Some(file_version);
        }
    }

    /// Validate that a no-op SyncWrite request matches the already visible layout prefix.
    fn validate_noop_sync_prefix(existing: &[Extent], requested: &[Extent], target_size: u64) -> MetadataResult<()> {
        if requested.is_empty() {
            return Ok(());
        }
        Self::validate_contiguous_extents(requested, requested[0].file_offset, target_size, "SyncWrite no-op")?;
        for extent in requested {
            if !Self::extent_matches_visible(existing, extent) {
                return Err(MetadataError::InvalidArgument(format!(
                    "SyncWrite no-op block {} does not match visible layout",
                    extent.block_id
                )));
            }
        }
        Ok(())
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

    fn truncate_layout_to_size(
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        extents: &[beryl_types::fs::Extent],
        new_size: u64,
    ) -> MetadataResult<Vec<beryl_types::fs::Extent>> {
        let mut new_extents = Vec::with_capacity(extents.len());

        for extent in extents {
            if extent.block_id.data_handle_id != data_handle_id {
                return Err(MetadataError::InvalidArgument(format!(
                    "Extent block data_handle_id {} does not match inode {} current_data_handle_id {}",
                    extent.block_id.data_handle_id, inode_id, data_handle_id
                )));
            }
            let extent_end = extent.file_offset.checked_add(extent.len).ok_or_else(|| {
                MetadataError::InvalidArgument(format!(
                    "Extent end overflows: file_offset={}, len={}",
                    extent.file_offset, extent.len
                ))
            })?;
            if extent_end <= new_size {
                new_extents.push(extent.clone());
            } else if extent.file_offset < new_size {
                let mut truncated_extent = extent.clone();
                truncated_extent.len = new_size - extent.file_offset;
                new_extents.push(truncated_extent);
            }
        }

        Ok(new_extents)
    }

    fn expected_inode_lease_id(inode_id: InodeId, lease_epoch: u64) -> MetadataResult<LeaseId> {
        let high = (inode_id.as_raw() as u128)
            .checked_shl(64)
            .ok_or_else(|| MetadataError::Internal("inode lease id shift overflow".to_string()))?;
        Ok(LeaseId::new(high | lease_epoch as u128))
    }

    fn validate_truncate_lease(
        inode_id: InodeId,
        stored_lease_epoch: Option<u64>,
        lease_id: LeaseId,
        lease_epoch: u64,
    ) -> MetadataResult<()> {
        let expected_lease_id = Self::expected_inode_lease_id(inode_id, lease_epoch)?;
        if lease_id != expected_lease_id {
            return Err(MetadataError::PermissionDenied(format!(
                "truncate lease_id mismatch for inode {}: expected {:?}, got {:?}",
                inode_id, expected_lease_id, lease_id
            )));
        }

        let stored_epoch = stored_lease_epoch.unwrap_or(0);
        if lease_epoch != stored_epoch + 1 {
            return Err(MetadataError::PermissionDenied(format!(
                "truncate lease_epoch mismatch for inode {}: stored={}, got={}",
                inode_id, stored_epoch, lease_epoch
            )));
        }
        if lease_epoch == 0 {
            return Err(MetadataError::PermissionDenied(format!(
                "truncate lease_epoch must be non-zero for inode {}",
                inode_id
            )));
        }
        Ok(())
    }

    fn next_file_version(inode_id: InodeId, current_file_version: Option<u64>) -> MetadataResult<u64> {
        current_file_version.unwrap_or(0).checked_add(1).ok_or_else(|| {
            MetadataError::Internal(format!(
                "file_version overflow for inode {} at {:?}",
                inode_id, current_file_version
            ))
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    pub(crate) use super::*;
    pub(crate) use beryl_types::fs::{FileAttrs, Inode};
    pub(crate) use beryl_types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, MountId, WorkerId};
    pub(crate) use beryl_types::layout::FileLayout;
    pub(crate) use beryl_types::CallId;
    pub(crate) use tempfile::TempDir;

    impl AppRaftStateMachine {
        pub(crate) fn apply(&self, command: Command) -> MetadataResult<AppDataResponse> {
            match self.apply_committed(command, &AppMetadataRaftState::default()) {
                Ok(CommittedApply {
                    response: AppDataResponse::Rejected(rejection),
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

    pub(crate) fn dedup_for_test(client: u128) -> crate::raft::DedupKey {
        crate::raft::DedupKey::new(ClientId::new(client), CallId::new())
    }

    pub(crate) fn bootstrap_command(client: u128, group_name: &str, proposed_at_ms: u64) -> Command {
        Command::new(
            dedup_for_test(client),
            proposed_at_ms,
            crate::raft::Mutation::BootstrapNamespace {
                group_name: GroupName::parse(group_name).unwrap(),
            },
        )
    }

    #[test]
    pub(crate) fn unsupported_command_version_fails_before_namespace_apply() {
        #[derive(serde::Serialize)]
        struct WireCommand {
            version: u16,
            dedup: DedupKey,
            proposed_at_ms: u64,
            mutation: Mutation,
        }

        let bytes = bincode::serde::encode_to_vec(
            WireCommand {
                version: crate::raft::command::COMMAND_FORMAT_VERSION + 1,
                dedup: dedup_for_test(99),
                proposed_at_ms: 100,
                mutation: Mutation::BootstrapNamespace {
                    group_name: GroupName::parse("root").unwrap(),
                },
            },
            bincode::config::standard(),
        )
        .unwrap();
        let (command, _) =
            bincode::serde::decode_from_slice::<Command, _>(&bytes, bincode::config::standard()).unwrap();
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let error = sm.apply(command).unwrap_err();

        assert!(error
            .to_string()
            .contains("unsupported metadata command format version"));
        assert!(storage.list_mounts().unwrap().is_empty());
        assert!(storage.get_inode(crate::mount::ROOT_INODE_ID).unwrap().is_none());
    }

    pub(crate) fn expect_fs_ok(raw: AppDataResponse) -> FsOkResult {
        match raw {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok,
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    pub(crate) fn expect_fs_errno(raw: AppDataResponse, errno: FsErrorCode) {
        match raw {
            AppDataResponse::Fs(FsCommandResult::Err(err)) => assert_eq!(err.errno, errno),
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    pub(crate) fn close_extent(
        data_handle_id: DataHandleId,
        block_index: u32,
        file_offset: u64,
        len: u64,
    ) -> beryl_types::fs::Extent {
        beryl_types::fs::Extent {
            file_offset,
            block_id: BlockId::new(data_handle_id, BlockIndex::new(block_index)),
            block_offset: 0,
            len,
            file_version: None,
            block_stamp: None,
        }
    }

    pub(crate) fn close_write_command(
        dedup: DedupKey,
        inode_id: InodeId,
        extents: Vec<beryl_types::fs::Extent>,
        final_size: u64,
        commit_mode: FileCommitMode,
    ) -> Command {
        Command::new(
            dedup,
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CloseWrite {
                inode_id,
                extents,
                final_size,
                lease_id: beryl_types::ids::LeaseId::new(1),
                open_epoch: 1,
                lease_epoch: 1,
                commit_mode,
            },
        )
    }

    #[test]
    pub(crate) fn bootstrap_namespace_atomically_creates_writable_root_and_allocators() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let response = sm.apply(bootstrap_command(1, "root", 1234)).unwrap();

        let AppDataResponse::Mount(MountCommandResult::Upserted(root)) = response else {
            panic!("unexpected bootstrap response");
        };
        assert_eq!(root.mount_id, MountId::new(1));
        assert_eq!(root.mount_prefix, crate::mount::ROOT_MOUNT_PREFIX);
        assert_eq!(root.data_io_policy, crate::mount::DataIoPolicy::Allow);
        assert_eq!(storage.get_route_epoch().unwrap(), crate::state::RouteEpoch::new(1));
        assert_eq!(storage.get_mount_epoch().unwrap(), 1);
        assert_eq!(storage.get_next_inode_id().unwrap(), Some(InodeId::new(2)));
        let inode = storage.get_inode(crate::mount::ROOT_INODE_ID).unwrap().unwrap();
        assert_eq!(inode.attrs.atime_ms, 1234);
        assert_eq!(
            storage
                .bootstrap_namespace_state(&GroupName::parse("root").unwrap(), 1234)
                .unwrap(),
            BootstrapNamespaceState::Matching
        );
    }

    #[test]
    pub(crate) fn bootstrap_namespace_accepts_complete_matching_state_with_new_dedup_identity() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        expect_mount_upserted(sm.apply(bootstrap_command(2, "root", 10)).unwrap());

        expect_mount_upserted(sm.apply(bootstrap_command(3, "root", 10)).unwrap());

        assert_eq!(storage.list_mounts().unwrap().len(), 1);
        assert_eq!(storage.max_inode_id().unwrap(), Some(crate::mount::ROOT_INODE_ID));
    }

    #[test]
    pub(crate) fn bootstrap_namespace_rejects_partial_state_without_completing_it() {
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

        let error = sm.apply(bootstrap_command(4, "root", 10)).unwrap_err();

        assert!(error.to_string().contains("partially initialized"));
        assert!(storage.list_mounts().unwrap().is_empty());
        assert_eq!(storage.get_next_inode_id().unwrap(), None);
    }

    #[test]
    pub(crate) fn bootstrap_namespace_rejects_corrupt_matching_root_attributes() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        expect_mount_upserted(sm.apply(bootstrap_command(6, "root", 10)).unwrap());
        let mut root = storage.get_inode(crate::mount::ROOT_INODE_ID).unwrap().unwrap();
        root.attrs.nlink = 0;
        storage.put_inode(&root).unwrap();

        let error = sm.apply(bootstrap_command(7, "root", 10)).unwrap_err();

        assert!(error.to_string().contains("conflicts"));
        assert_eq!(
            storage
                .get_inode(crate::mount::ROOT_INODE_ID)
                .unwrap()
                .unwrap()
                .attrs
                .nlink,
            0
        );
    }

    #[test]
    pub(crate) fn namespace_apply_uses_proposal_time_without_regressing_parent_timestamps() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        expect_mount_upserted(sm.apply(bootstrap_command(8, "root", 10)).unwrap());
        let mut root = storage.get_inode(crate::mount::ROOT_INODE_ID).unwrap().unwrap();
        root.attrs.update_timestamps(5_000);
        storage.put_inode(&root).unwrap();

        let response = sm
            .apply(Command::new(
                dedup_for_test(9),
                1_000,
                Mutation::Mkdir {
                    parent_inode_id: crate::mount::ROOT_INODE_ID,
                    name: "child".to_string(),
                    attrs: FileAttrs::new(),
                },
            ))
            .unwrap();
        let child_id = expect_fs_ok(response).inode_id.unwrap();

        let child = storage.get_inode(child_id).unwrap().unwrap();
        assert_eq!(child.attrs.mtime_ms, 1_000);
        assert_eq!(child.attrs.ctime_ms, 1_000);
        let root = storage.get_inode(crate::mount::ROOT_INODE_ID).unwrap().unwrap();
        assert_eq!(root.attrs.mtime_ms, 5_000);
        assert_eq!(root.attrs.ctime_ms, 5_000);
    }

    pub(crate) fn expect_mount_upserted(raw: AppDataResponse) -> crate::mount::MountEntry {
        match raw {
            AppDataResponse::Mount(MountCommandResult::Upserted(entry)) => entry,
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    pub(crate) fn expect_worker_upserted(raw: AppDataResponse) -> WorkerId {
        match raw {
            AppDataResponse::Worker(WorkerCommandResult::Upserted(worker_id)) => worker_id,
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    pub(crate) fn lease_id_for_inode_epoch(inode_id: InodeId, lease_epoch: u64) -> beryl_types::ids::LeaseId {
        beryl_types::ids::LeaseId::new((inode_id.as_raw() as u128) << 64 | (lease_epoch as u128))
    }

    pub(crate) fn extent(block_id: BlockId, file_offset: u64, len: u64) -> beryl_types::fs::Extent {
        beryl_types::fs::Extent {
            file_offset,
            block_id,
            block_offset: 0,
            len,
            file_version: None,
            block_stamp: None,
        }
    }

    pub(crate) fn install_file_with_extents(
        storage: &RocksDBStorage,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        extents: Vec<beryl_types::fs::Extent>,
        size: u64,
    ) -> Inode {
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), parent.mount_id, data_handle_id);
        inode.attrs.size = size;
        if let InodeData::File {
            extents: stored_extents,
            lease_epoch,
            ..
        } = &mut inode.data
        {
            *stored_extents = extents;
            *lease_epoch = Some(1);
        }
        storage.put_inode(&parent).unwrap();
        storage.put_inode(&inode).unwrap();
        storage.put_dentry(parent_inode_id, name, inode_id).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();
        inode
    }
}
