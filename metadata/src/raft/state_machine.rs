// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft state machine implementation.
//!
//! Applies commands to the state machine and maintains consistency.

use crate::error::{to_canonical_fs, MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::raft::command::{Command, FileCommitMode};
use crate::raft::storage::{AppliedResult, RenameAtomicUpdate, RenameOverwriteCleanup, RocksDBStorage};
use crate::raft::types::{
    AppDataResponse, BlockCommandResult, CommandFingerprint, DedupKey, DeleteIntentStatusResult, DeleteIntentsResult,
    FsCommandResult, FsErrnoResult, FsOkResult, LeaseCommandResult, MountCommandResult, ShardGroupInfo,
    WorkerCommandResult,
};
use crate::state::{BlockMetaState, DeleteIntentStatus, LeaseState, RouteEpoch};
use parking_lot::RwLock;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::warn;
use types::block::{BlockPlacement, BlockState};
use types::fs::{FileAttrs, FsErrorCode, Inode, InodeData, InodeId, InodeKind};
use types::ids::{BlockId, ClientId, DataHandleId, LeaseId, MountId, ShardGroupId, ShardId};
use types::layout::FileLayout;
use types::lease::{FencingToken, Lease};

fn meta_err_to_fs_errno(err: &MetadataError) -> Option<FsErrorCode> {
    match to_canonical_fs(err.clone()).code {
        Some(common::error::canonical::ErrorCode::FsErrno(errno)) => Some(errno),
        _ => None,
    }
}

/// Raft state machine.
pub struct AppRaftStateMachine {
    storage: Arc<RocksDBStorage>,
    mount_table: Arc<MountTable>,
    _next_mount_id: Arc<RwLock<u64>>,
}

struct PreparedRenameOverwrite {
    inode_id: InodeId,
    data_handle_id: Option<DataHandleId>,
    released_block_ids: Vec<BlockId>,
}

struct PreparedRename {
    src_inode_id: InodeId,
    overwritten_target: Option<PreparedRenameOverwrite>,
    updated_src_parent: Option<Inode>,
    updated_dst_parent: Option<Inode>,
    updated_src_inode: Inode,
}

type PreparedUnlink = (InodeId, Option<DataHandleId>, Inode, Vec<BlockId>, FsOkResult);
type PreparedCloseWrite = (Inode, FileLayout, Vec<BlockId>, Vec<BlockId>, u64, FsOkResult);

impl AppRaftStateMachine {
    pub fn new(storage: Arc<RocksDBStorage>, mount_table: Arc<MountTable>) -> Self {
        Self {
            storage,
            mount_table,
            _next_mount_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Apply a command to the state machine.
    pub fn apply(&self, command: Command) -> MetadataResult<AppDataResponse> {
        let dedup_key = command.dedup_key().clone();
        let fingerprint = command.fingerprint();

        // Dedup hit returns the persisted replay record without re-running the mutation.
        if let Some(applied) = self.storage.get_applied_result_without_ttl_eviction(&dedup_key)? {
            if applied.fingerprint != fingerprint {
                crate::metrics::DEDUP_LOOKUP_MISMATCH_TOTAL.fetch_add(1, Ordering::Relaxed);
                warn!(
                    client_id = %dedup_key.client_id.as_raw(),
                    call_id = %dedup_key.call_id,
                    stored_fp = %applied.fingerprint.0,
                    new_fp = %fingerprint.0,
                    "dedup fingerprint mismatch"
                );
                return Err(MetadataError::InvalidArgument(format!(
                    "call_id {} reused with different command payload",
                    dedup_key.call_id
                )));
            }
            return Ok(applied.result);
        }

        match command {
            Command::AllocateBlock {
                inode_id,
                block_id,
                placement,
                ..
            } => {
                let result = self.apply_allocate_block(inode_id, block_id, placement, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Block(result))
            }
            Command::CommitBlock { block_id, token, .. } => {
                let result = self.apply_commit_block(block_id, token, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Block(result))
            }
            Command::UpdateBlockState { block_id, state, .. } => {
                let result = self.apply_update_block_state(block_id, state, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Block(result))
            }
            Command::AcquireLease {
                block_id,
                client_id,
                epoch,
                expires_at_ms,
                ..
            } => {
                let result =
                    self.apply_acquire_lease(block_id, client_id, epoch, expires_at_ms, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Lease(result))
            }
            Command::ReleaseLease { block_id, .. } => {
                let result = self.apply_release_lease(block_id, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Lease(result))
            }
            Command::CreateMount {
                mount_id,
                mount_prefix,
                mount_kind,
                ufs_uri,
                data_io_policy,
                namespace_owner_group_id,
                root_inode_id,
                ..
            } => {
                let result = self.apply_create_mount(
                    mount_id,
                    mount_prefix,
                    mount_kind,
                    ufs_uri,
                    data_io_policy,
                    namespace_owner_group_id,
                    root_inode_id,
                    &dedup_key,
                    fingerprint,
                )?;
                Ok(AppDataResponse::Mount(result))
            }
            Command::DeleteMount { mount_id, .. } => {
                let result = self.apply_delete_mount(mount_id, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Mount(result))
            }
            Command::AddShardGroup {
                shard_group_id,
                shard_ids,
                initial_members,
                ..
            } => {
                let result =
                    self.apply_add_shard_group(shard_group_id, shard_ids, initial_members, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::ShardGroup(result))
            }
            Command::RegisterWorker {
                identity,
                address,
                net_transport_kind,
                worker_epoch,
                fault_domain,
                ..
            } => {
                let result = self.apply_register_worker(
                    identity,
                    address,
                    net_transport_kind,
                    worker_epoch,
                    fault_domain,
                    &dedup_key,
                    fingerprint,
                )?;
                Ok(AppDataResponse::Worker(result))
            }
            Command::CreateDeleteIntents { intents, .. } => {
                let result = self.apply_create_delete_intents(intents, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::DeleteIntents(result))
            }
            Command::AllocateDeleteIntents { intents, .. } => {
                let result = self.apply_allocate_delete_intents(intents, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::DeleteIntents(result))
            }
            Command::UpdateDeleteIntentStatus {
                intent_id,
                status,
                finished_at_ms,
                error_msg,
                ..
            } => {
                let result = self.apply_update_delete_intent_status(
                    intent_id,
                    status,
                    finished_at_ms,
                    error_msg,
                    &dedup_key,
                    fingerprint,
                )?;
                Ok(AppDataResponse::DeleteIntentStatus(result))
            }
            Command::Mkdir {
                parent_inode_id,
                name,
                attrs,
                ..
            } => {
                // Create/mkdir/rename persist namespace mutation, replay result together.
                let result = self.apply_mkdir(parent_inode_id, name, attrs, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Fs(result))
            }
            Command::Create {
                parent_inode_id,
                name,
                attrs,
                layout,
                ..
            } => {
                let result = self.apply_create(parent_inode_id, name, attrs, layout, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Fs(result))
            }
            Command::Unlink {
                parent_inode_id, name, ..
            } => {
                let result = self.apply_unlink(parent_inode_id, name, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Fs(result))
            }
            Command::Rmdir {
                parent_inode_id, name, ..
            } => {
                let result = self.apply_rmdir(parent_inode_id, name, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Fs(result))
            }
            Command::Rename {
                src_parent_inode_id,
                src_name,
                dst_parent_inode_id,
                dst_name,
                flags,
                ..
            } => {
                let result = self.apply_rename(
                    src_parent_inode_id,
                    src_name,
                    dst_parent_inode_id,
                    dst_name,
                    flags,
                    &dedup_key,
                    fingerprint,
                )?;
                Ok(AppDataResponse::Fs(result))
            }
            Command::SetAttr {
                inode_id, mask, attrs, ..
            } => {
                let result = self.apply_set_attr(inode_id, mask, attrs, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Fs(result))
            }
            Command::CloseWrite {
                inode_id,
                extents,
                final_size,
                lease_id,
                open_epoch,
                lease_epoch,
                commit_mode,
                ..
            } => {
                let result = self.apply_close_write(
                    inode_id,
                    extents,
                    final_size,
                    lease_id,
                    open_epoch,
                    lease_epoch,
                    commit_mode,
                    &dedup_key,
                    fingerprint,
                )?;
                Ok(AppDataResponse::Fs(result))
            }
            Command::Truncate {
                inode_id,
                new_size,
                lease_id,
                lease_epoch,
                ..
            } => {
                let result = self.apply_truncate(inode_id, new_size, lease_id, lease_epoch, &dedup_key, fingerprint)?;
                Ok(AppDataResponse::Fs(result))
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

    fn apply_timestamp_ms() -> u64 {
        0
    }

    fn apply_allocate_block(
        &self,
        inode_id: InodeId,
        block_id: BlockId,
        placement: BlockPlacement,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<BlockCommandResult> {
        // Check if block already exists
        if self.storage.get_block(block_id)?.is_some() {
            return Err(MetadataError::AlreadyExists(format!(
                "Block already exists: {:?}",
                block_id
            )));
        }

        // Validate mapping between data handle and inode.
        self.storage
            .validate_data_handle_owner(block_id.data_handle_id, Some(inode_id))?;

        // Create block metadata
        let block_meta = BlockMetaState {
            block_id,
            inode_id,
            data_handle_id: block_id.data_handle_id,
            state: BlockState::Open,
            placement,
            committed_length: 0,
        };

        let result = BlockCommandResult::Allocated(block_meta.clone());
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Block(result.clone()));
        self.storage
            .put_block_with_apply_result_atomic(&block_meta, dedup_key, applied_result)?;

        Ok(result)
    }

    fn apply_commit_block(
        &self,
        block_id: BlockId,
        token: FencingToken,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<BlockCommandResult> {
        // Verify lease
        let lease_state = self
            .storage
            .get_lease(block_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Lease not found for block: {:?}", block_id)))?;

        if lease_state.lease.owner.as_raw() != token.owner.as_raw() {
            return Err(MetadataError::LeaseFenced {
                expected: lease_state.lease.epoch,
                got: token.epoch,
            });
        }

        if lease_state.lease.epoch != token.epoch {
            return Err(MetadataError::LeaseFenced {
                expected: lease_state.lease.epoch,
                got: token.epoch,
            });
        }

        // Update block state to Sealed
        let mut block_meta = self
            .storage
            .get_block(block_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Block not found: {:?}", block_id)))?;

        block_meta.state = BlockState::Sealed;
        let result = BlockCommandResult::Committed;
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Block(result.clone()));
        self.storage
            .put_block_with_apply_result_atomic(&block_meta, dedup_key, applied_result)?;

        Ok(result)
    }

    fn apply_update_block_state(
        &self,
        block_id: BlockId,
        state: BlockState,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<BlockCommandResult> {
        let mut block_meta = self
            .storage
            .get_block(block_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Block not found: {:?}", block_id)))?;

        block_meta.state = state;
        let result = BlockCommandResult::StateUpdated;
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Block(result.clone()));
        self.storage
            .put_block_with_apply_result_atomic(&block_meta, dedup_key, applied_result)?;

        Ok(result)
    }

    fn apply_acquire_lease(
        &self,
        block_id: BlockId,
        client_id: ClientId,
        epoch: u64,
        expires_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<LeaseCommandResult> {
        // Ensure the data_handle_id is still bound to the expected inode (authoritative mapping).
        if let Some(meta) = self.storage.get_block(block_id)? {
            self.storage
                .validate_data_handle_owner(block_id.data_handle_id, Some(meta.inode_id))?;
        } else {
            let owner = self.storage.validate_data_handle_owner(block_id.data_handle_id, None)?;
            return Err(MetadataError::NotFound(format!(
                "Block {} not found for inode {}",
                block_id, owner
            )));
        }

        // Check existing lease
        if let Some(existing) = self.storage.get_lease(block_id)? {
            if existing.lease.epoch >= epoch {
                return Err(MetadataError::LeaseFenced {
                    expected: existing.lease.epoch + 1,
                    got: epoch,
                });
            }
        }

        let lease = Lease {
            owner: client_id,
            epoch,
            expires_at_ms,
        };

        let lease_state = LeaseState { block_id, lease };

        let result = LeaseCommandResult::Acquired(lease_state);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Lease(result.clone()));
        if let LeaseCommandResult::Acquired(lease_state) = &result {
            self.storage
                .acquire_lease_with_apply_result_atomic(lease_state, dedup_key, applied_result)?;
        }

        Ok(result)
    }

    fn apply_release_lease(
        &self,
        block_id: BlockId,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<LeaseCommandResult> {
        let result = LeaseCommandResult::Released;
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Lease(result.clone()));
        self.storage
            .release_lease_with_apply_result_atomic(block_id, dedup_key, applied_result)?;
        Ok(result)
    }

    // Raft apply helpers mirror command payload fields for replay clarity.
    #[allow(clippy::too_many_arguments)]
    fn apply_create_mount(
        &self,
        mount_id: MountId,
        mount_prefix: String,
        mount_kind: crate::mount::MountKind,
        ufs_uri: Option<String>,
        data_io_policy: crate::mount::DataIoPolicy,
        namespace_owner_group_id: ShardGroupId,
        root_inode_id: InodeId,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<MountCommandResult> {
        if namespace_owner_group_id.as_raw() == 0 {
            return Err(MetadataError::InvalidArgument(
                "namespace_owner_group_id must be provided".to_string(),
            ));
        }

        if mount_prefix == crate::mount::ROOT_MOUNT_PREFIX {
            if root_inode_id != crate::mount::ROOT_INODE_ID {
                return Err(MetadataError::InvalidArgument(format!(
                    "root inode invariant violated: expected inode_id={}, got {}. storage must be migrated or wiped",
                    crate::mount::ROOT_INODE_ID.as_raw(),
                    root_inode_id.as_raw()
                )));
            }
            if mount_kind != crate::mount::MountKind::Internal {
                return Err(MetadataError::InvalidArgument(
                    "root mount must be Internal".to_string(),
                ));
            }
            if ufs_uri.is_some() {
                return Err(MetadataError::InvalidArgument(
                    "root mount must not bind UFS".to_string(),
                ));
            }
            if data_io_policy != crate::mount::DataIoPolicy::Forbid {
                return Err(MetadataError::InvalidArgument(
                    "root mount must forbid data IO".to_string(),
                ));
            }
        }

        if mount_kind == crate::mount::MountKind::Internal && ufs_uri.is_some() {
            return Err(MetadataError::InvalidArgument(
                "internal mount must not bind UFS".to_string(),
            ));
        }
        if mount_kind == crate::mount::MountKind::External && ufs_uri.is_none() {
            return Err(MetadataError::InvalidArgument(
                "ufs mount must provide ufs_uri".to_string(),
            ));
        }

        if let Some(existing) = self
            .storage
            .list_mounts()?
            .into_iter()
            .find(|entry| entry.mount_prefix == mount_prefix)
        {
            if existing.mount_kind == mount_kind
                && existing.ufs_uri == ufs_uri
                && existing.data_io_policy == data_io_policy
                && existing.root_inode_id == root_inode_id
            {
                let result = MountCommandResult::Upserted(existing);
                let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Mount(result.clone()));
                self.storage.put_apply_result_atomic(dedup_key, applied_result)?;
                return Ok(result);
            }
            return Err(MetadataError::AlreadyExists(format!(
                "Mount prefix already exists: {}",
                mount_prefix
            )));
        }

        if let Some(existing) = self.storage.get_mount(mount_id)? {
            if existing.mount_prefix == mount_prefix {
                let result = MountCommandResult::Upserted(existing);
                let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Mount(result.clone()));
                self.storage.put_apply_result_atomic(dedup_key, applied_result)?;
                return Ok(result);
            }
            return Err(MetadataError::AlreadyExists(format!(
                "Mount ID already exists: {:?}",
                mount_id
            )));
        }

        // Get current mount version
        let mount_version = self.storage.get_mount_version()?;
        let new_version = mount_version + 1;

        // Validate root inode exists and is a directory.
        let mut root_inode_to_create = None;
        let root_inode = match self.storage.get_inode(root_inode_id)? {
            Some(inode) => inode,
            None => {
                if mount_prefix != crate::mount::ROOT_MOUNT_PREFIX {
                    return Err(MetadataError::NotFound(format!(
                        "Root inode not found: {}",
                        root_inode_id
                    )));
                }
                let mut attrs = FileAttrs::new();
                let now_ms = Self::apply_timestamp_ms();
                attrs.update_timestamps(now_ms);
                attrs.nlink = 1;
                let inode = Inode::new_dir(root_inode_id, attrs, mount_id);
                root_inode_to_create = Some(inode.clone());
                inode
            }
        };
        if root_inode.kind != InodeKind::Dir {
            return Err(MetadataError::InvalidArgument(format!(
                "Root inode {} is not a directory",
                root_inode_id
            )));
        }
        if root_inode.mount_id != mount_id {
            return Err(MetadataError::InvalidArgument(format!(
                "Root inode mount_id {:?} does not match mount {:?}",
                root_inode.mount_id, mount_id
            )));
        }

        // Create mount entry
        let entry = crate::mount::MountEntry {
            mount_id,
            mount_prefix: mount_prefix.clone(),
            mount_kind,
            ufs_uri,
            data_io_policy,
            config_version: new_version,
            namespace_owner_group_id,
            root_inode_id,
        };

        let new_route_epoch = self.next_authoritative_route_epoch()?;
        let result = MountCommandResult::Upserted(entry.clone());
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Mount(result.clone()));
        self.storage.create_mount_with_apply_result_atomic(
            &entry,
            root_inode_to_create.as_ref(),
            new_version,
            new_route_epoch,
            dedup_key,
            applied_result,
        )?;

        // Synchronize in-memory MountTable (must succeed after RocksDB write)
        self.mount_table
            .upsert(entry.clone())
            .map_err(|e| MetadataError::Internal(format!("Failed to update MountTable after RocksDB write: {}", e)))?;

        Ok(result)
    }

    fn apply_delete_mount(
        &self,
        mount_id: MountId,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<MountCommandResult> {
        // Check if mount exists
        let entry = self
            .storage
            .get_mount(mount_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount not found: {:?}", mount_id)))?;
        if entry.mount_prefix == crate::mount::ROOT_MOUNT_PREFIX {
            return Err(MetadataError::InvalidArgument(
                "root mount cannot be deleted".to_string(),
            ));
        }

        let mount_version = self.storage.get_mount_version()?;
        let new_route_epoch = self.next_authoritative_route_epoch()?;
        let result = MountCommandResult::Deleted;
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Mount(result.clone()));
        self.storage.delete_mount_with_apply_result_atomic(
            mount_id,
            mount_version + 1,
            new_route_epoch,
            dedup_key,
            applied_result,
        )?;

        // Synchronize in-memory MountTable (must succeed after RocksDB delete)
        self.mount_table
            .remove(mount_id)
            .map_err(|e| MetadataError::Internal(format!("Failed to update MountTable after RocksDB delete: {}", e)))?;

        Ok(result)
    }

    fn next_authoritative_route_epoch(&self) -> MetadataResult<RouteEpoch> {
        let current = self.storage.get_route_epoch()?;
        Ok(RouteEpoch::new(current.as_u64() + 1))
    }

    fn apply_add_shard_group(
        &self,
        shard_group_id: ShardGroupId,
        shard_ids: Vec<ShardId>,
        initial_members: Vec<u64>,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<ShardGroupInfo> {
        // Check if group already exists
        if self.storage.get_shard_group(shard_group_id)?.is_some() {
            return Err(MetadataError::AlreadyExists(format!(
                "Shard group already exists: {:?}",
                shard_group_id
            )));
        }

        // Create shard group info
        let info = ShardGroupInfo {
            group_id: shard_group_id,
            shard_ids: shard_ids.iter().map(|s| s.as_raw()).collect(),
            initial_members,
            version: 1,
        };

        // Shard-group registration is not part of the filesystem-facing route_epoch contract.
        // FsCore stale-route validation is keyed to mount routing ownership changes instead.
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::ShardGroup(info.clone()));
        self.storage
            .add_shard_group_with_apply_result_atomic(&info, &shard_ids, dedup_key, applied_result)?;

        Ok(info)
    }

    // Raft apply helpers mirror command payload fields for replay clarity.
    #[allow(clippy::too_many_arguments)]
    fn apply_register_worker(
        &self,
        identity: String,
        address: String,
        net_transport_kind: i32,
        worker_epoch: u64,
        fault_domain: Option<String>,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<WorkerCommandResult> {
        let worker_info = self.storage.prepare_worker_registration(
            &identity,
            address,
            net_transport_kind,
            worker_epoch,
            fault_domain,
        )?;
        let result = WorkerCommandResult::Upserted(worker_info.worker_id);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Worker(result.clone()));
        self.storage
            .register_worker_with_apply_result_atomic(&identity, &worker_info, dedup_key, applied_result)?;

        Ok(result)
    }

    fn apply_create_delete_intents(
        &self,
        intents: Vec<crate::state::DeleteIntent>,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<DeleteIntentsResult> {
        let intent_count = intents.len();
        let result = DeleteIntentsResult {
            created: intent_count as u64,
        };
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::DeleteIntents(result.clone()));
        self.storage
            .create_delete_intents_with_apply_result_atomic(intents, dedup_key, applied_result)?;
        Ok(result)
    }

    fn apply_allocate_delete_intents(
        &self,
        intents: Vec<crate::state::DeleteIntent>,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<DeleteIntentsResult> {
        let intent_count = intents.len();
        let result = DeleteIntentsResult {
            created: intent_count as u64,
        };
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::DeleteIntents(result.clone()));
        self.storage
            .allocate_delete_intents_with_apply_result_atomic(intents, dedup_key, applied_result)?;
        Ok(result)
    }

    fn apply_update_delete_intent_status(
        &self,
        intent_id: u64,
        status: DeleteIntentStatus,
        finished_at_ms: Option<u64>,
        error_msg: Option<String>,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<DeleteIntentStatusResult> {
        let result = DeleteIntentStatusResult { intent_id, status };
        let applied_result =
            Self::make_applied_result(fingerprint, AppDataResponse::DeleteIntentStatus(result.clone()));
        self.storage.update_delete_intent_status_with_apply_result_atomic(
            intent_id,
            status,
            finished_at_ms,
            error_msg,
            dedup_key,
            applied_result,
        )?;
        Ok(result)
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

    /// Get block metadata (for external access).
    pub fn get_block(&self, block_id: BlockId) -> MetadataResult<Option<BlockMetaState>> {
        self.storage.get_block(block_id)
    }

    /// Get storage reference (for worker_id generation, etc.).
    pub fn storage(&self) -> Arc<RocksDBStorage> {
        Arc::clone(&self.storage)
    }

    /// Helper: allocate the next inode ID from replicated storage.
    fn next_inode_id(&self) -> MetadataResult<InodeId> {
        self.storage.allocate_inode_id()
    }

    fn next_data_handle_id(&self) -> MetadataResult<DataHandleId> {
        // Data-plane identities are allocated durably via RocksDB meta key.
        self.storage.get_and_increment_data_handle_id()
    }

    fn persist_fs_apply_result(
        &self,
        result: FsCommandResult,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<FsCommandResult> {
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.put_apply_result_atomic(dedup_key, applied_result)?;
        Ok(result)
    }

    /// Apply Mkdir command.
    fn apply_mkdir(
        &self,
        parent_inode_id: InodeId,
        name: String,
        mut attrs: FileAttrs,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(Inode, Inode, FsOkResult)> = (|| {
            // Check parent exists and is a directory
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;
            if !parent_inode.kind.is_dir() {
                return Err(MetadataError::NotDir(format!(
                    "Parent is not a directory: {}",
                    parent_inode_id
                )));
            }

            // Check if name already exists
            if self.storage.get_dentry(parent_inode_id, &name)?.is_some() {
                return Err(MetadataError::AlreadyExists(format!(
                    "Directory already exists: {}",
                    name
                )));
            }

            // Generate inode ID
            let inode_id = self.next_inode_id()?;
            let now_ms = Self::apply_timestamp_ms();

            // Initialize attrs
            attrs.update_timestamps(now_ms);
            attrs.nlink = 1; // Directory starts with 1 link (self)

            // Create directory inode (inherit mount_id from parent)
            let inode = Inode::new_dir(inode_id, attrs, parent_inode.mount_id);

            // Update parent directory mtime/ctime
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(now_ms);
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            Ok(FsOkResult {
                inode_id: Some(inode_id),
                data_handle_id: None,
                file_version: None,
            })
            .map(|ok| (inode, updated_parent, ok))
        })();

        let (inode, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.create_dir_with_apply_result_atomic(
            parent_inode_id,
            &name,
            &inode,
            &updated_parent,
            dedup_key,
            applied_result,
        )?;
        Ok(result)
    }

    /// Apply Create command.
    fn apply_create(
        &self,
        parent_inode_id: InodeId,
        name: String,
        mut attrs: FileAttrs,
        layout: FileLayout,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(Inode, Inode, FsOkResult)> = (|| {
            // Check parent exists and is a directory
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;
            if !parent_inode.kind.is_dir() {
                return Err(MetadataError::NotDir(format!(
                    "Parent is not a directory: {}",
                    parent_inode_id
                )));
            }

            // Check if name already exists
            if self.storage.get_dentry(parent_inode_id, &name)?.is_some() {
                return Err(MetadataError::AlreadyExists(format!("File already exists: {}", name)));
            }

            // Generate inode ID
            let inode_id = self.next_inode_id()?;
            let data_handle_id = self.next_data_handle_id()?;
            let now_ms = Self::apply_timestamp_ms();

            // Initialize attrs
            attrs.update_timestamps(now_ms);
            attrs.nlink = 1;

            // Create file inode (inherit mount_id from parent) with a freshly allocated data handle.
            let inode = Inode::new_file(inode_id, attrs, parent_inode.mount_id, data_handle_id);

            // Update parent directory mtime/ctime
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(now_ms);
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            Ok(FsOkResult {
                inode_id: Some(inode_id),
                data_handle_id: Some(data_handle_id),
                file_version: None,
            })
            .map(|ok| (inode, updated_parent, ok))
        })();

        let (inode, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.create_file_with_apply_result_atomic(
            parent_inode_id,
            &name,
            &inode,
            &updated_parent,
            layout,
            dedup_key,
            applied_result,
        )?;
        Ok(result)
    }

    fn collect_unique_released_blocks(
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        extents: &[types::fs::Extent],
    ) -> MetadataResult<Vec<BlockId>> {
        let mut seen = std::collections::HashSet::with_capacity(extents.len());
        let mut blocks = Vec::with_capacity(extents.len());
        for extent in extents {
            if extent.block_id.data_handle_id != data_handle_id {
                return Err(MetadataError::InvalidArgument(format!(
                    "Extent block data_handle_id {} does not match inode {} current_data_handle_id {}",
                    extent.block_id.data_handle_id, inode_id, data_handle_id
                )));
            }
            if seen.insert(extent.block_id) {
                blocks.push(extent.block_id);
            }
        }
        blocks.sort_by_key(|block_id| (block_id.data_handle_id.as_raw(), block_id.index.as_raw()));
        Ok(blocks)
    }

    fn truncate_layout_to_size(
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        extents: &[types::fs::Extent],
        new_size: u64,
    ) -> MetadataResult<(Vec<types::fs::Extent>, Vec<BlockId>)> {
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

        let old_blocks = Self::collect_unique_released_blocks(inode_id, data_handle_id, extents)?;
        let new_blocks = Self::collect_unique_released_blocks(inode_id, data_handle_id, &new_extents)?;
        let new_block_set: std::collections::HashSet<BlockId> = new_blocks.into_iter().collect();
        let released_blocks = old_blocks
            .into_iter()
            .filter(|block_id| !new_block_set.contains(block_id))
            .collect();
        Ok((new_extents, released_blocks))
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

    fn validate_released_block_refcounts(&self, released_block_ids: &[BlockId]) -> MetadataResult<()> {
        let mut seen = std::collections::HashSet::with_capacity(released_block_ids.len());
        for block_id in released_block_ids {
            if !seen.insert(*block_id) {
                continue;
            }
            let count = self.storage.get_block_ref_count(*block_id)?.ok_or_else(|| {
                MetadataError::InvalidArgument(format!("Missing block refcount for released block {}", block_id))
            })?;
            if count == 0 {
                return Err(MetadataError::InvalidArgument(format!(
                    "Block refcount underflow for released block {}",
                    block_id
                )));
            }
        }
        Ok(())
    }

    /// Apply Unlink command.
    fn apply_unlink(
        &self,
        parent_inode_id: InodeId,
        name: String,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<PreparedUnlink> = (|| {
            // Get dentry
            let child_inode_id = self
                .storage
                .get_dentry(parent_inode_id, &name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Entry not found: {}", name)))?;

            // Get child inode
            let child_inode = self
                .storage
                .get_inode(child_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Child inode not found: {}", child_inode_id)))?;

            // Check it's not a directory
            if child_inode.kind.is_dir() {
                return Err(MetadataError::IsDir(format!("Cannot unlink directory: {}", name)));
            }

            let now_ms = Self::apply_timestamp_ms();

            // Update parent directory mtime/ctime
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(now_ms);
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            let data_handle_id = match &child_inode.data {
                InodeData::File { .. } => {
                    let data_handle_id = child_inode.current_data_handle_id;
                    if data_handle_id.as_raw() == 0 {
                        return Err(MetadataError::Internal(format!(
                            "File inode {} is missing current_data_handle_id",
                            child_inode_id
                        )));
                    }
                    self.storage
                        .validate_data_handle_owner(data_handle_id, Some(child_inode_id))?;
                    Some(data_handle_id)
                }
                InodeData::Symlink { .. } => None,
                InodeData::Dir => return Err(MetadataError::IsDir(format!("Cannot unlink directory: {}", name))),
            };

            let released_block_ids = match &child_inode.data {
                InodeData::File { extents, .. } => {
                    let blocks = Self::collect_unique_released_blocks(
                        child_inode_id,
                        child_inode.current_data_handle_id,
                        extents,
                    )?;
                    self.validate_released_block_refcounts(&blocks)?;
                    blocks
                }
                _ => Vec::new(),
            };

            Ok(FsOkResult::default()).map(|ok| (child_inode_id, data_handle_id, updated_parent, released_block_ids, ok))
        })();

        let (child_inode_id, data_handle_id, updated_parent, released_block_ids, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        if released_block_ids.is_empty() {
            self.storage.delete_empty_file_with_apply_result_atomic(
                parent_inode_id,
                &name,
                child_inode_id,
                data_handle_id,
                &updated_parent,
                dedup_key,
                applied_result,
            )?;
        } else {
            let data_handle_id = data_handle_id.ok_or_else(|| {
                MetadataError::Internal(format!("Missing data_handle_id for file inode {}", child_inode_id))
            })?;
            self.storage.delete_file_with_extents_and_apply_result_atomic(
                parent_inode_id,
                &name,
                child_inode_id,
                data_handle_id,
                &updated_parent,
                &released_block_ids,
                Self::apply_timestamp_ms(),
                dedup_key,
                applied_result,
            )?;
        }
        Ok(result)
    }

    /// Apply Rmdir command.
    fn apply_rmdir(
        &self,
        parent_inode_id: InodeId,
        name: String,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(InodeId, Inode, FsOkResult)> = (|| {
            // Get dentry
            let child_inode_id = self
                .storage
                .get_dentry(parent_inode_id, &name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Directory not found: {}", name)))?;

            // Get child inode
            let child_inode = self
                .storage
                .get_inode(child_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Child inode not found: {}", child_inode_id)))?;

            // Check it's a directory
            if !child_inode.kind.is_dir() {
                return Err(MetadataError::NotDir(format!("Not a directory: {}", name)));
            }

            // Check directory is empty
            if !self.storage.is_directory_empty(child_inode_id)? {
                return Err(MetadataError::DirectoryNotEmpty(format!(
                    "Directory not empty: {}",
                    name
                )));
            }

            let now_ms = Self::apply_timestamp_ms();

            // Update parent directory mtime/ctime
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(now_ms);
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            Ok(FsOkResult::default()).map(|ok| (child_inode_id, updated_parent, ok))
        })();

        let (child_inode_id, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.delete_empty_dir_with_apply_result_atomic(
            parent_inode_id,
            &name,
            child_inode_id,
            &updated_parent,
            dedup_key,
            applied_result,
        )?;
        Ok(result)
    }

    /// Apply Rename command (atomic within mount).
    // Raft apply helpers mirror command payload fields for replay clarity.
    #[allow(clippy::too_many_arguments)]
    fn apply_rename(
        &self,
        src_parent_inode_id: InodeId,
        src_name: String,
        dst_parent_inode_id: InodeId,
        dst_name: String,
        flags: u32,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<PreparedRename> = (|| {
            // Get source dentry
            let src_inode_id = self
                .storage
                .get_dentry(src_parent_inode_id, &src_name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Source not found: {}", src_name)))?;

            // Get source inode
            let src_inode = self
                .storage
                .get_inode(src_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Source inode not found: {}", src_inode_id)))?;

            let mut overwritten_target = None;

            // Check if destination exists
            if let Some(dst_inode_id) = self.storage.get_dentry(dst_parent_inode_id, &dst_name)? {
                // NOREPLACE flag set -> fail when destination exists
                if flags & 0x1 != 0 {
                    return Err(MetadataError::AlreadyExists(format!(
                        "Destination exists and RENAME_NOREPLACE set: {}",
                        dst_name
                    )));
                }
                if src_inode_id == dst_inode_id {
                    return Ok(PreparedRename {
                        src_inode_id,
                        overwritten_target: None,
                        updated_src_parent: None,
                        updated_dst_parent: None,
                        updated_src_inode: src_inode,
                    });
                }
                // Destination exists - check if it's a directory and empty (if source is directory)
                let dst_inode = self
                    .storage
                    .get_inode(dst_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Destination inode disappeared".to_string()))?;

                if src_inode.kind.is_dir() {
                    if !dst_inode.kind.is_dir() {
                        return Err(MetadataError::NotDir(
                            "Cannot overwrite non-directory with directory".to_string(),
                        ));
                    }
                    if !self.storage.is_directory_empty(dst_inode_id)? {
                        return Err(MetadataError::DirectoryNotEmpty(
                            "Cannot overwrite non-empty directory".to_string(),
                        ));
                    }
                } else {
                    if dst_inode.kind.is_dir() {
                        return Err(MetadataError::IsDir("Cannot overwrite directory with file".to_string()));
                    }
                }
                overwritten_target = Some(self.prepare_rename_overwrite_target_cleanup(dst_inode_id, &dst_inode)?);
            }

            let now_ms = Self::apply_timestamp_ms();

            // Update parent directories mtime/ctime
            let (updated_src_parent, updated_dst_parent) = if src_parent_inode_id != dst_parent_inode_id {
                // Different parents - update both
                let src_parent = self
                    .storage
                    .get_inode(src_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Source parent disappeared".to_string()))?;
                let mut src_attrs = src_parent.attrs.clone();
                src_attrs.update_mtime_ctime(now_ms);
                let mut src_parent = src_parent.clone();
                src_parent.attrs = src_attrs;
                let dst_parent = self
                    .storage
                    .get_inode(dst_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Destination parent disappeared".to_string()))?;
                let mut dst_attrs = dst_parent.attrs.clone();
                dst_attrs.update_mtime_ctime(now_ms);
                let mut dst_parent = dst_parent.clone();
                dst_parent.attrs = dst_attrs;
                (Some(src_parent), Some(dst_parent))
            } else {
                let parent = self
                    .storage
                    .get_inode(src_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Parent disappeared".to_string()))?;
                let mut attrs = parent.attrs.clone();
                attrs.update_mtime_ctime(now_ms);
                let mut parent = parent.clone();
                parent.attrs = attrs;
                (Some(parent), None)
            };

            // Update source inode ctime
            let mut src_attrs = src_inode.attrs.clone();
            src_attrs.update_ctime(now_ms);
            let mut updated_src_inode = src_inode.clone();
            updated_src_inode.attrs = src_attrs;

            Ok(PreparedRename {
                src_inode_id,
                overwritten_target,
                updated_src_parent,
                updated_dst_parent,
                updated_src_inode,
            })
        })();

        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint);
            }
        };
        let result = FsCommandResult::Ok(FsOkResult::default());
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.rename_with_apply_result_atomic(
            RenameAtomicUpdate {
                src_parent_inode_id,
                src_name: &src_name,
                dst_parent_inode_id,
                dst_name: &dst_name,
                src_inode_id: prepared.src_inode_id,
                overwritten_target: prepared
                    .overwritten_target
                    .as_ref()
                    .map(|target| RenameOverwriteCleanup {
                        inode_id: target.inode_id,
                        data_handle_id: target.data_handle_id,
                        released_block_ids: &target.released_block_ids,
                        now_ms: Self::apply_timestamp_ms(),
                    }),
                updated_src_parent: prepared.updated_src_parent.as_ref(),
                updated_dst_parent: prepared.updated_dst_parent.as_ref(),
                updated_src_inode: &prepared.updated_src_inode,
            },
            dedup_key,
            applied_result,
        )?;

        Ok(result)
    }

    fn prepare_rename_overwrite_target_cleanup(
        &self,
        dst_inode_id: InodeId,
        dst_inode: &Inode,
    ) -> MetadataResult<PreparedRenameOverwrite> {
        match &dst_inode.data {
            InodeData::File { extents, .. } => {
                let data_handle_id = dst_inode.current_data_handle_id;
                if data_handle_id.as_raw() == 0 {
                    return Err(MetadataError::Internal(format!(
                        "File inode {} is missing current_data_handle_id",
                        dst_inode_id
                    )));
                }
                self.storage
                    .validate_data_handle_owner(data_handle_id, Some(dst_inode_id))?;
                let released_block_ids = Self::collect_unique_released_blocks(dst_inode_id, data_handle_id, extents)?;
                self.validate_released_block_refcounts(&released_block_ids)?;
                Ok(PreparedRenameOverwrite {
                    inode_id: dst_inode_id,
                    data_handle_id: Some(data_handle_id),
                    released_block_ids,
                })
            }
            InodeData::Dir => {
                if !self.storage.is_directory_empty(dst_inode_id)? {
                    return Err(MetadataError::DirectoryNotEmpty(
                        "Cannot overwrite non-empty directory".to_string(),
                    ));
                }
                Ok(PreparedRenameOverwrite {
                    inode_id: dst_inode_id,
                    data_handle_id: None,
                    released_block_ids: Vec::new(),
                })
            }
            InodeData::Symlink { .. } => Ok(PreparedRenameOverwrite {
                inode_id: dst_inode_id,
                data_handle_id: None,
                released_block_ids: Vec::new(),
            }),
        }
    }

    /// Apply SetAttr command.
    fn apply_set_attr(
        &self,
        inode_id: InodeId,
        mask: u32,
        new_attrs: FileAttrs,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(Inode, FsOkResult)> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            let now_ms = Self::apply_timestamp_ms();
            let size_changes_visible_file_state =
                inode.kind.is_file() && mask & 1 != 0 && new_attrs.size != inode.attrs.size;

            // Apply mask: only update fields specified by mask
            // Bit flags: 1=size, 2=mode, 4=uid, 8=gid, 16=atime, 32=mtime
            if mask & 1 != 0 {
                inode.attrs.size = new_attrs.size;
            }
            if mask & 2 != 0 {
                inode.attrs.mode = new_attrs.mode;
            }
            if mask & 4 != 0 {
                inode.attrs.uid = new_attrs.uid;
            }
            if mask & 8 != 0 {
                inode.attrs.gid = new_attrs.gid;
            }
            if mask & 16 != 0 {
                inode.attrs.atime_ms = new_attrs.atime_ms;
            }
            if mask & 32 != 0 {
                inode.attrs.mtime_ms = new_attrs.mtime_ms;
            }

            // Always update ctime
            inode.attrs.ctime_ms = now_ms;

            let file_version = if size_changes_visible_file_state {
                match &mut inode.data {
                    InodeData::File {
                        extents, file_version, ..
                    } => {
                        let next = Self::next_file_version(inode_id, *file_version)?;
                        for extent in extents.iter_mut() {
                            extent.file_version = Some(next);
                        }
                        *file_version = Some(next);
                        Some(next)
                    }
                    _ => None,
                }
            } else {
                None
            };

            Ok((
                inode,
                FsOkResult {
                    inode_id: Some(inode_id),
                    file_version,
                    ..FsOkResult::default()
                },
            ))
        })();

        let (inode, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage
            .put_inode_with_apply_result_atomic(&inode, dedup_key, applied_result)?;
        Ok(result)
    }

    /// Apply CloseWrite command.
    // Raft apply helpers mirror command payload fields for replay clarity.
    #[allow(clippy::too_many_arguments)]
    fn apply_close_write(
        &self,
        inode_id: InodeId,
        extents: Vec<types::fs::Extent>,
        final_size: u64,
        lease_id: types::ids::LeaseId,
        open_epoch: u64,
        lease_epoch: u64,
        commit_mode: FileCommitMode,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<PreparedCloseWrite> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            if !inode.kind.is_file() {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {}",
                    inode_id
                )));
            }

            let expected_data_handle_id = inode.current_data_handle_id;
            if expected_data_handle_id.as_raw() == 0 {
                return Err(MetadataError::Internal(format!(
                    "File inode {} is missing current_data_handle_id",
                    inode_id
                )));
            }

            // lease_id/open_epoch are part of the command fingerprint and replay
            // identity, but the Raft apply layer has no authoritative runtime
            // write-session table after restart. FsCore validates the live session
            // before proposing; apply can only persist the lease_epoch carried here.
            let _ = (lease_id, open_epoch);

            let layout = self.storage.get_layout(inode_id)?;
            let now_ms = Self::apply_timestamp_ms();

            let mut existing_block_ids = std::collections::HashSet::new();
            let old_size = inode.attrs.size;
            let (existing_extents_snapshot, current_file_version) = match &inode.data {
                InodeData::File {
                    extents, file_version, ..
                } => {
                    for extent in extents {
                        existing_block_ids.insert(extent.block_id);
                    }
                    (extents.clone(), *file_version)
                }
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode data is not File: {}",
                        inode_id
                    )));
                }
            };

            let mut committed_block_ids = std::collections::HashSet::with_capacity(extents.len());
            let file_version = Self::next_file_version(inode_id, current_file_version)?;
            let mut ordered_extents = extents;
            ordered_extents.sort_by_key(|extent| (extent.file_offset, extent.block_id.index.as_raw()));
            let mut previous_end = None;
            let mut max_committed_end = 0;

            for extent in &ordered_extents {
                if extent.len == 0 {
                    return Err(MetadataError::InvalidArgument(
                        "Committed extent len must be greater than 0".to_string(),
                    ));
                }
                if extent.block_id.data_handle_id != expected_data_handle_id {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent block data_handle_id {} does not match inode {} current_data_handle_id {}",
                        extent.block_id.data_handle_id, inode_id, expected_data_handle_id
                    )));
                }
                if !committed_block_ids.insert(extent.block_id) {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Committed block {} was submitted more than once",
                        extent.block_id
                    )));
                }
                let extent_end = extent.file_offset.checked_add(extent.len).ok_or_else(|| {
                    MetadataError::InvalidArgument(format!(
                        "Extent end overflows: file_offset={}, len={}",
                        extent.file_offset, extent.len
                    ))
                })?;
                if previous_end.map(|prev| extent.file_offset < prev).unwrap_or(false) {
                    return Err(MetadataError::InvalidArgument(
                        "Committed extents must not overlap".to_string(),
                    ));
                }
                if extent_end > final_size {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent extends beyond final_size: extent_end={}, final_size={}",
                        extent_end, final_size
                    )));
                }
                previous_end = Some(extent_end);
                max_committed_end = max_committed_end.max(extent_end);
            }

            match commit_mode {
                FileCommitMode::Replace => {
                    if ordered_extents.is_empty() && final_size != 0 {
                        return Err(MetadataError::InvalidArgument(format!(
                            "Empty replace commit cannot publish nonzero final_size={}",
                            final_size
                        )));
                    }
                    if final_size < max_committed_end {
                        return Err(MetadataError::InvalidArgument(format!(
                            "Replace final_size {} is smaller than committed end {}",
                            final_size, max_committed_end
                        )));
                    }
                }
                FileCommitMode::Append => {
                    let mut expected_offset = old_size;
                    for extent in &ordered_extents {
                        if existing_block_ids.contains(&extent.block_id) {
                            return Err(MetadataError::InvalidArgument(format!(
                                "Append committed block {} already exists in authoritative layout",
                                extent.block_id
                            )));
                        }
                        if extent.file_offset != expected_offset {
                            return Err(MetadataError::InvalidArgument(format!(
                                "Append extent file_offset mismatch: expected {}, got {}",
                                expected_offset, extent.file_offset
                            )));
                        }
                        expected_offset = extent.file_offset.checked_add(extent.len).ok_or_else(|| {
                            MetadataError::InvalidArgument(format!(
                                "Extent end overflows: file_offset={}, len={}",
                                extent.file_offset, extent.len
                            ))
                        })?;
                    }
                    if final_size != expected_offset {
                        return Err(MetadataError::InvalidArgument(format!(
                            "Append final_size mismatch: expected {}, got {}",
                            expected_offset, final_size
                        )));
                    }
                }
            }

            // Update inode: publish extents and update size/mtime/ctime/file_version/lease_epoch.
            for extent in &mut ordered_extents {
                extent.file_version = Some(file_version);
            }
            match &mut inode.data {
                types::fs::InodeData::File {
                    extents: existing_extents,
                    file_version: stored_file_version,
                    lease_epoch: stored_lease_epoch,
                    ..
                } => {
                    match commit_mode {
                        FileCommitMode::Replace => {
                            *existing_extents = ordered_extents.clone();
                        }
                        FileCommitMode::Append => {
                            existing_extents.extend(ordered_extents.clone());
                        }
                    }
                    for extent in existing_extents.iter_mut() {
                        extent.file_version = Some(file_version);
                    }
                    *stored_file_version = Some(file_version);
                    // Update lease_epoch (persisted for fencing after restart)
                    *stored_lease_epoch = Some(lease_epoch);
                }
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode data is not File: {}",
                        inode_id
                    )));
                }
            }

            // Update file size and timestamps
            inode.attrs.size = final_size;
            inode.attrs.update_mtime_ctime(now_ms);

            let block_ref_increments = committed_block_ids
                .difference(&existing_block_ids)
                .copied()
                .collect::<Vec<_>>();
            let block_ref_decrements = if commit_mode == FileCommitMode::Replace {
                existing_extents_snapshot
                    .iter()
                    .map(|extent| extent.block_id)
                    .collect::<std::collections::HashSet<_>>()
                    .difference(&committed_block_ids)
                    .copied()
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };

            Ok((
                inode,
                layout,
                block_ref_increments,
                block_ref_decrements,
                now_ms,
                FsOkResult {
                    inode_id: Some(inode_id),
                    data_handle_id: Some(expected_data_handle_id),
                    file_version: Some(file_version),
                },
            ))
        })();

        let (inode, layout, block_ref_increments, block_ref_decrements, now_ms, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.close_write_with_apply_result_atomic(
            &inode,
            layout,
            &block_ref_increments,
            &block_ref_decrements,
            now_ms,
            dedup_key,
            applied_result,
        )?;
        Ok(result)
    }

    /// Apply Truncate command.
    fn apply_truncate(
        &self,
        inode_id: InodeId,
        new_size: u64,
        lease_id: types::ids::LeaseId,
        lease_epoch: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(Inode, FileLayout, Vec<BlockId>, FsOkResult)> = (|| {
            // Get inode
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            if !inode.kind.is_file() {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {}",
                    inode_id
                )));
            }

            let (stored_lease_epoch, current_file_version) = match &inode.data {
                types::fs::InodeData::File {
                    lease_epoch,
                    file_version,
                    ..
                } => (*lease_epoch, *file_version),
                _ => (None, None),
            };
            Self::validate_truncate_lease(inode_id, stored_lease_epoch, lease_id, lease_epoch)?;

            let current_size = inode.attrs.size;
            if new_size > current_size {
                return Err(MetadataError::NotSupported(format!(
                    "Truncate grow not supported: current_size={}, new_size={}",
                    current_size, new_size
                )));
            }

            if new_size == current_size {
                return Ok((
                    inode,
                    self.storage.get_layout(inode_id)?,
                    Vec::new(),
                    FsOkResult::default(),
                ));
            }

            let now_ms = Self::apply_timestamp_ms();
            let layout = self.storage.get_layout(inode_id)?;
            let data_handle_id = inode.current_data_handle_id;
            if data_handle_id.as_raw() == 0 {
                return Err(MetadataError::Internal(format!(
                    "File inode {} is missing current_data_handle_id",
                    inode_id
                )));
            }
            self.storage
                .validate_data_handle_owner(data_handle_id, Some(inode_id))?;

            let next_file_version = Self::next_file_version(inode_id, current_file_version)?;
            let released_block_ids = match &mut inode.data {
                types::fs::InodeData::File {
                    extents,
                    file_version: stored_file_version,
                    lease_epoch: stored_lease_epoch,
                    ..
                } => {
                    let (new_extents, released_block_ids) =
                        Self::truncate_layout_to_size(inode_id, data_handle_id, extents, new_size)?;
                    *extents = new_extents;
                    for extent in extents.iter_mut() {
                        extent.file_version = Some(next_file_version);
                    }
                    *stored_file_version = Some(next_file_version);
                    *stored_lease_epoch = Some(lease_epoch);
                    released_block_ids
                }
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode data is not File: {}",
                        inode_id
                    )));
                }
            };
            self.validate_released_block_refcounts(&released_block_ids)?;

            // Update file size and timestamps
            inode.attrs.size = new_size;
            inode.attrs.update_mtime_ctime(now_ms);

            Ok((
                inode,
                layout,
                released_block_ids,
                FsOkResult {
                    inode_id: Some(inode_id),
                    data_handle_id: Some(data_handle_id),
                    file_version: Some(next_file_version),
                },
            ))
        })();

        let (inode, layout, released_block_ids, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.truncate_file_with_apply_result_atomic(
            &inode,
            layout,
            &released_block_ids,
            Self::apply_timestamp_ms(),
            dedup_key,
            applied_result,
        )?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use types::block::{BlockPlacement, BlockState};
    use types::fs::{FileAttrs, Inode};
    use types::ids::{BlockIndex, ClientId, DataHandleId, MountId, WorkerId};
    use types::layout::FileLayout;
    use types::CallId;

    fn dedup_for_test(client: u64) -> crate::raft::types::DedupKey {
        crate::raft::types::DedupKey::new(ClientId::new(client), CallId::new())
    }

    fn expect_fs_ok(raw: AppDataResponse) -> FsOkResult {
        match raw {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok,
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    fn expect_fs_errno(raw: AppDataResponse, errno: FsErrorCode) {
        match raw {
            AppDataResponse::Fs(FsCommandResult::Err(err)) => assert_eq!(err.errno, errno),
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    fn close_extent(data_handle_id: DataHandleId, block_index: u32, file_offset: u64, len: u64) -> types::fs::Extent {
        types::fs::Extent {
            file_offset,
            block_id: BlockId::new(data_handle_id, BlockIndex::new(block_index)),
            block_offset: 0,
            len,
            file_version: None,
            block_stamp: None,
        }
    }

    fn close_write_command(
        dedup: DedupKey,
        inode_id: InodeId,
        extents: Vec<types::fs::Extent>,
        final_size: u64,
        commit_mode: FileCommitMode,
    ) -> Command {
        Command::CloseWrite {
            dedup,
            inode_id,
            extents,
            final_size,
            lease_id: types::ids::LeaseId::new(1),
            open_epoch: 1,
            lease_epoch: 1,
            commit_mode,
        }
    }

    fn expect_mount_upserted(raw: AppDataResponse) -> crate::mount::MountEntry {
        match raw {
            AppDataResponse::Mount(MountCommandResult::Upserted(entry)) => entry,
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    fn expect_mount_deleted(raw: AppDataResponse) {
        match raw {
            AppDataResponse::Mount(MountCommandResult::Deleted) => {}
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    fn expect_worker_upserted(raw: AppDataResponse) -> WorkerId {
        match raw {
            AppDataResponse::Worker(WorkerCommandResult::Upserted(worker_id)) => worker_id,
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    fn expect_lease_acquired(raw: AppDataResponse) -> LeaseState {
        match raw {
            AppDataResponse::Lease(LeaseCommandResult::Acquired(lease)) => lease,
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    fn expect_lease_released(raw: AppDataResponse) {
        match raw {
            AppDataResponse::Lease(LeaseCommandResult::Released) => {}
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    fn expect_shard_group(raw: AppDataResponse) -> ShardGroupInfo {
        match raw {
            AppDataResponse::ShardGroup(info) => info,
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    fn expect_delete_intents(raw: AppDataResponse) -> DeleteIntentsResult {
        match raw {
            AppDataResponse::DeleteIntents(result) => result,
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    fn expect_delete_intent_status(raw: AppDataResponse) -> DeleteIntentStatusResult {
        match raw {
            AppDataResponse::DeleteIntentStatus(result) => result,
            other => panic!("unexpected apply response: {:?}", other),
        }
    }

    fn make_delete_intent(intent_id: u64, block_id: BlockId) -> crate::state::DeleteIntent {
        crate::state::DeleteIntent {
            intent_id,
            block_id,
            reason: crate::state::DeleteIntentReason::Gc,
            created_at_ms: 1,
            not_before_ms: 1,
            shard_group_id: None,
            guard_watermark: None,
            mount_epoch: None,
            guard_state_id: types::RaftLogId {
                term: 0,
                leader_node_id: 0,
                index: 0,
            },
            target_workers: Vec::new(),
            status: crate::state::DeleteIntentStatus::Pending,
            finished_at_ms: None,
            last_error_msg: None,
        }
    }

    fn lease_id_for_inode_epoch(inode_id: InodeId, lease_epoch: u64) -> types::ids::LeaseId {
        types::ids::LeaseId::new((inode_id.as_raw() as u128) << 64 | (lease_epoch as u128))
    }

    fn extent(block_id: BlockId, file_offset: u64, len: u64) -> types::fs::Extent {
        types::fs::Extent {
            file_offset,
            block_id,
            block_offset: 0,
            len,
            file_version: None,
            block_stamp: None,
        }
    }

    fn install_file_with_extents(
        storage: &RocksDBStorage,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        extents: Vec<types::fs::Extent>,
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

    fn read_next_delete_intent_id(storage: &RocksDBStorage) -> Option<u64> {
        let cf = storage.cf("meta").unwrap();
        storage.db().get_cf(cf, b"next_delete_intent_id").unwrap().map(|value| {
            bincode::serde::decode_from_slice::<u64, _>(&value, bincode::config::standard())
                .unwrap()
                .0
        })
    }

    fn write_next_delete_intent_id(storage: &RocksDBStorage, next_id: u64) {
        let cf = storage.cf("meta").unwrap();
        let value = bincode::serde::encode_to_vec(next_id, bincode::config::standard()).unwrap();
        storage.db().put_cf(cf, b"next_delete_intent_id", value).unwrap();
    }

    #[test]
    fn create_mount_requires_owner_group() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let cmd = Command::CreateMount {
            dedup: dedup_for_test(1),
            mount_id: MountId::new(1),
            mount_prefix: "/mnt/a".to_string(),
            mount_kind: crate::mount::MountKind::External,
            ufs_uri: Some("ufs://a".to_string()),
            data_io_policy: crate::mount::DataIoPolicy::Allow,
            namespace_owner_group_id: ShardGroupId::new(0),
            root_inode_id: InodeId::new(1),
        };
        let res = sm.apply(cmd);
        assert!(matches!(res, Err(MetadataError::InvalidArgument(_))));
    }

    #[test]
    fn create_mount_validates_root_inode() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let mount_id = MountId::new(7);
        let root_inode_id = InodeId::new(11);
        let attrs = FileAttrs::new();
        let inode = Inode::new(
            root_inode_id,
            InodeKind::File,
            attrs.clone(),
            mount_id,
            DataHandleId::new(1),
        );
        storage.put_inode(&inode).unwrap();

        let cmd = Command::CreateMount {
            dedup: dedup_for_test(2),
            mount_id,
            mount_prefix: "/mnt/b".to_string(),
            mount_kind: crate::mount::MountKind::External,
            ufs_uri: Some("ufs://b".to_string()),
            data_io_policy: crate::mount::DataIoPolicy::Allow,
            namespace_owner_group_id: ShardGroupId::new(2),
            root_inode_id,
        };
        let res = sm.apply(cmd);
        assert!(matches!(res, Err(MetadataError::InvalidArgument(_))));
    }

    #[test]
    fn create_mount_succeeds_with_valid_root() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let mount_id = MountId::new(3);
        let root_inode_id = InodeId::new(21);
        let attrs = FileAttrs::new();
        let inode = Inode::new(
            root_inode_id,
            InodeKind::Dir,
            attrs.clone(),
            mount_id,
            DataHandleId::new(0),
        );
        storage.put_inode(&inode).unwrap();

        let cmd = Command::CreateMount {
            dedup: dedup_for_test(3),
            mount_id,
            mount_prefix: "/mnt/c".to_string(),
            mount_kind: crate::mount::MountKind::External,
            ufs_uri: Some("ufs://c".to_string()),
            data_io_policy: crate::mount::DataIoPolicy::Allow,
            namespace_owner_group_id: ShardGroupId::new(5),
            root_inode_id,
        };
        sm.apply(cmd).unwrap();

        let entry = mount_table.get_mount(mount_id).unwrap().unwrap();
        assert_eq!(entry.namespace_owner_group_id, ShardGroupId::new(5));
        assert_eq!(entry.root_inode_id, root_inode_id);
    }

    #[test]
    fn create_file_persists_data_handle_mapping() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        let mount_id = MountId::new(1);
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id);
        storage.put_inode(&parent).unwrap();

        let cmd = Command::Create {
            dedup: crate::raft::types::DedupKey::new(ClientId::new(10), CallId::new()),
            parent_inode_id,
            name: "file".to_string(),
            attrs: FileAttrs::new(),
            layout: FileLayout::new(4096, 4096, 1),
        };

        let raw = sm.apply(cmd).unwrap();
        let inode_id = match raw {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.expect("inode id should be returned"),
            other => panic!("unexpected apply response: {:?}", other),
        };

        let inode = storage.get_inode(inode_id).unwrap().expect("inode should exist");
        let handle = inode.current_data_handle_id;
        assert_ne!(handle.as_raw(), 0, "create must allocate a data handle");

        let mapped = storage
            .get_inode_by_data_handle(handle)
            .unwrap()
            .expect("mapping should exist");
        assert_eq!(mapped, inode_id, "data handle owner mapping must match created inode");
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert_eq!(storage.get_layout(inode_id).unwrap(), FileLayout::new(4096, 4096, 1));
    }

    #[test]
    fn create_reapply_returns_original_success_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let dedup = dedup_for_test(41);
        let cmd = Command::Create {
            dedup: dedup.clone(),
            parent_inode_id,
            name: "file".to_string(),
            attrs: FileAttrs::new(),
            layout: FileLayout::new(4096, 4096, 1),
        };

        let first = expect_fs_ok(sm.apply(cmd.clone()).unwrap());

        let second = expect_fs_ok(sm.apply(cmd).unwrap());
        assert_eq!(second, first);

        let inode_id = first.inode_id.expect("inode id should be returned");
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn mkdir_persists_inode_and_dentry() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let raw = sm
            .apply(Command::Mkdir {
                dedup: dedup_for_test(29),
                parent_inode_id,
                name: "dir".to_string(),
                attrs: FileAttrs::new(),
            })
            .unwrap();
        let inode_id = match raw {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.expect("inode id should be returned"),
            other => panic!("unexpected apply response: {:?}", other),
        };

        assert!(storage.get_inode(inode_id).unwrap().unwrap().kind.is_dir());
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(inode_id));
    }

    #[test]
    fn mkdir_reapply_returns_original_success_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let dedup = dedup_for_test(42);
        let cmd = Command::Mkdir {
            dedup: dedup.clone(),
            parent_inode_id,
            name: "dir".to_string(),
            attrs: FileAttrs::new(),
        };

        let first = expect_fs_ok(sm.apply(cmd.clone()).unwrap());

        let second = expect_fs_ok(sm.apply(cmd).unwrap());
        assert_eq!(second, first);

        let inode_id = first.inode_id.expect("inode id should be returned");
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(inode_id));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn rename_moves_dentry_and_preserves_inode() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let created = sm
            .apply(Command::Create {
                dedup: dedup_for_test(36),
                parent_inode_id,
                name: "old".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap();
        let inode_id = match created {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected apply response: {:?}", other),
        };

        sm.apply(Command::Rename {
            dedup: dedup_for_test(37),
            src_parent_inode_id: parent_inode_id,
            src_name: "old".to_string(),
            dst_parent_inode_id: parent_inode_id,
            dst_name: "new".to_string(),
            flags: 0,
        })
        .unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "old").unwrap(), None);
        assert_eq!(storage.get_dentry(parent_inode_id, "new").unwrap(), Some(inode_id));
        assert!(storage.get_inode(inode_id).unwrap().is_some());
    }

    #[test]
    fn rename_reapply_returns_original_success_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let created = expect_fs_ok(
            sm.apply(Command::Create {
                dedup: dedup_for_test(43),
                parent_inode_id,
                name: "old".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap(),
        );
        let inode_id = created.inode_id.unwrap();

        let dedup = dedup_for_test(44);
        let cmd = Command::Rename {
            dedup: dedup.clone(),
            src_parent_inode_id: parent_inode_id,
            src_name: "old".to_string(),
            dst_parent_inode_id: parent_inode_id,
            dst_name: "new".to_string(),
            flags: 0,
        };

        let first = expect_fs_ok(sm.apply(cmd.clone()).unwrap());
        assert_eq!(first, FsOkResult::default());

        let second = expect_fs_ok(sm.apply(cmd).unwrap());
        assert_eq!(second, first);
        assert_eq!(storage.get_dentry(parent_inode_id, "old").unwrap(), None);
        assert_eq!(storage.get_dentry(parent_inode_id, "new").unwrap(), Some(inode_id));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn set_attr_reapply_returns_original_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(70);
        storage
            .put_inode(&Inode::new_file(
                inode_id,
                FileAttrs::new(),
                MountId::new(1),
                DataHandleId::new(700),
            ))
            .unwrap();

        let mut attrs = FileAttrs::new();
        attrs.uid = 123;
        let set_attr = Command::SetAttr {
            dedup: dedup_for_test(70),
            inode_id,
            mask: 4,
            attrs,
        };
        let first = expect_fs_ok(sm.apply(set_attr.clone()).unwrap());
        let ctime_after_first = storage.get_inode(inode_id).unwrap().unwrap().attrs.ctime_ms;
        let second = expect_fs_ok(sm.apply(set_attr).unwrap());
        assert_eq!(second, first);
        let stored = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(stored.attrs.uid, 123);
        assert_eq!(stored.attrs.ctime_ms, ctime_after_first);
    }

    #[test]
    fn mount_commands_reapply_return_original_result_and_update_mount_table_after_apply() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let mount_id = MountId::new(73);
        let root_inode_id = InodeId::new(730);
        storage
            .put_inode(&Inode::new_dir(root_inode_id, FileAttrs::new(), mount_id))
            .unwrap();

        let create_mount = Command::CreateMount {
            dedup: dedup_for_test(73),
            mount_id,
            mount_prefix: "/mnt/reapply".to_string(),
            mount_kind: crate::mount::MountKind::External,
            ufs_uri: Some("ufs://reapply".to_string()),
            data_io_policy: crate::mount::DataIoPolicy::Allow,
            namespace_owner_group_id: ShardGroupId::new(73),
            root_inode_id,
        };
        let first = expect_mount_upserted(sm.apply(create_mount.clone()).unwrap());
        let second = expect_mount_upserted(sm.apply(create_mount).unwrap());
        assert_eq!(second.mount_id, first.mount_id);
        assert_eq!(second.config_version, first.config_version);
        assert_eq!(
            mount_table.get_mount(mount_id).unwrap().unwrap().mount_prefix,
            first.mount_prefix
        );
        assert_eq!(mount_table.list_mounts().len(), 1);

        let delete_mount = Command::DeleteMount {
            dedup: dedup_for_test(74),
            mount_id,
        };
        expect_mount_deleted(sm.apply(delete_mount.clone()).unwrap());
        expect_mount_deleted(sm.apply(delete_mount).unwrap());
        assert!(storage.get_mount(mount_id).unwrap().is_none());
        assert!(mount_table.get_mount(mount_id).unwrap().is_none());
    }

    #[test]
    fn shard_group_reapply_returns_original_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let cmd = Command::AddShardGroup {
            dedup: dedup_for_test(75),
            shard_group_id: ShardGroupId::new(75),
            shard_ids: vec![ShardId::new(750), ShardId::new(751)],
            initial_members: vec![1, 2],
        };

        let first = expect_shard_group(sm.apply(cmd.clone()).unwrap());
        let second = expect_shard_group(sm.apply(cmd).unwrap());
        assert_eq!(second, first);
        assert_eq!(
            storage.get_shard_routing(ShardId::new(750)).unwrap(),
            Some(ShardGroupId::new(75))
        );
    }

    #[test]
    fn worker_descriptor_reapply_returns_original_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let cmd = Command::RegisterWorker {
            dedup: dedup_for_test(76),
            identity: "worker-identity-a".to_string(),
            address: "127.0.0.1:17076".to_string(),
            net_transport_kind: 1,
            worker_epoch: 3,
            fault_domain: Some("rack-a".to_string()),
        };

        let worker_id = WorkerId::new(1);
        assert_eq!(expect_worker_upserted(sm.apply(cmd.clone()).unwrap()), worker_id);
        assert_eq!(expect_worker_upserted(sm.apply(cmd).unwrap()), worker_id);
        let stored = storage.get_worker(worker_id).unwrap().unwrap();
        assert_eq!(stored.address, "127.0.0.1:17076");
        assert_eq!(stored.worker_epoch, 3);
    }

    #[test]
    fn register_worker_reuses_identity_updates_descriptor_and_persists_allocator() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let first = Command::RegisterWorker {
            dedup: dedup_for_test(760),
            identity: "worker-identity-reuse".to_string(),
            address: "127.0.0.1:17060".to_string(),
            net_transport_kind: 1,
            worker_epoch: 3,
            fault_domain: None,
        };
        let second = Command::RegisterWorker {
            dedup: dedup_for_test(761),
            identity: "worker-identity-reuse".to_string(),
            address: "127.0.0.1:17061".to_string(),
            net_transport_kind: 2,
            worker_epoch: 4,
            fault_domain: Some("rack-b".to_string()),
        };

        assert_eq!(
            expect_worker_upserted(sm.apply(first.clone()).unwrap()),
            WorkerId::new(1)
        );
        assert_eq!(expect_worker_upserted(sm.apply(first).unwrap()), WorkerId::new(1));
        assert_eq!(expect_worker_upserted(sm.apply(second).unwrap()), WorkerId::new(1));
        let stored = storage.get_worker(WorkerId::new(1)).unwrap().unwrap();
        assert_eq!(stored.address, "127.0.0.1:17061");
        assert_eq!(stored.net_transport_kind, 2);
        assert_eq!(stored.worker_epoch, 4);

        drop(sm);
        drop(storage);
        let reopened = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let reopened_sm = AppRaftStateMachine::new(Arc::clone(&reopened), Arc::clone(&mount_table));
        let third = Command::RegisterWorker {
            dedup: dedup_for_test(762),
            identity: "worker-identity-new".to_string(),
            address: "127.0.0.1:17062".to_string(),
            net_transport_kind: 1,
            worker_epoch: 1,
            fault_domain: None,
        };
        assert_eq!(
            expect_worker_upserted(reopened_sm.apply(third).unwrap()),
            WorkerId::new(2)
        );
    }

    #[test]
    fn create_delete_intents_replay_and_fingerprint_mismatch_do_not_duplicate_intents() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle_id = DataHandleId::new(90);
        let first_block = BlockId::new(data_handle_id, BlockIndex::new(0));
        let second_block = BlockId::new(data_handle_id, BlockIndex::new(1));
        let dedup = dedup_for_test(90);
        let command = Command::CreateDeleteIntents {
            dedup: dedup.clone(),
            intents: vec![
                make_delete_intent(900, first_block),
                make_delete_intent(901, second_block),
            ],
        };

        assert_eq!(expect_delete_intents(sm.apply(command.clone()).unwrap()).created, 2);
        assert_eq!(storage.list_pending_delete_intents(10, 10).unwrap().len(), 2);

        assert_eq!(expect_delete_intents(sm.apply(command).unwrap()).created, 2);
        assert_eq!(storage.list_pending_delete_intents(10, 10).unwrap().len(), 2);

        let mismatch = Command::CreateDeleteIntents {
            dedup,
            intents: vec![make_delete_intent(
                902,
                BlockId::new(data_handle_id, BlockIndex::new(2)),
            )],
        };
        assert!(matches!(sm.apply(mismatch), Err(MetadataError::InvalidArgument(_))));
        assert!(storage.get_delete_intent(902).unwrap().is_none());
    }

    #[test]
    fn create_delete_intents_rejects_duplicate_ids_without_partial_write() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle_id = DataHandleId::new(906);
        let first_block = BlockId::new(data_handle_id, BlockIndex::new(0));
        let second_block = BlockId::new(data_handle_id, BlockIndex::new(1));
        let command = Command::CreateDeleteIntents {
            dedup: dedup_for_test(906),
            intents: vec![
                make_delete_intent(9060, first_block),
                make_delete_intent(9060, second_block),
            ],
        };

        assert!(matches!(
            sm.apply(command.clone()),
            Err(MetadataError::InvalidArgument(_))
        ));
        assert!(matches!(sm.apply(command), Err(MetadataError::InvalidArgument(_))));
        assert!(storage.get_delete_intent(9060).unwrap().is_none());
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
    }

    #[test]
    fn create_delete_intents_rejects_existing_id_without_overwrite() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle_id = DataHandleId::new(907);
        let existing_block = BlockId::new(data_handle_id, BlockIndex::new(0));
        let existing = make_delete_intent(9070, existing_block);
        storage.put_delete_intent(&existing).unwrap();

        let collision_block = BlockId::new(data_handle_id, BlockIndex::new(1));
        let command = Command::CreateDeleteIntents {
            dedup: dedup_for_test(907),
            intents: vec![make_delete_intent(9070, collision_block)],
        };

        assert!(matches!(
            sm.apply(command.clone()),
            Err(MetadataError::InvalidArgument(_))
        ));
        assert!(matches!(sm.apply(command), Err(MetadataError::InvalidArgument(_))));
        let stored = storage
            .get_delete_intent(9070)
            .unwrap()
            .expect("existing intent should remain");
        assert_eq!(stored.intent_id, existing.intent_id);
        assert_eq!(stored.block_id, existing.block_id);
        assert!(matches!(stored.status, crate::state::DeleteIntentStatus::Pending));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 1);
    }

    #[test]
    fn allocate_delete_intents_assigns_ids_in_apply_and_replay_is_noop() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle_id = DataHandleId::new(908);
        let mut existing = make_delete_intent(5, BlockId::new(data_handle_id, BlockIndex::new(0)));
        existing.status = crate::state::DeleteIntentStatus::Failed;
        storage.put_delete_intent(&existing).unwrap();

        let mut first = make_delete_intent(0, BlockId::new(data_handle_id, BlockIndex::new(1)));
        let second = make_delete_intent(0, BlockId::new(data_handle_id, BlockIndex::new(2)));
        first.reason = crate::state::DeleteIntentReason::Orphan;
        let command = Command::AllocateDeleteIntents {
            dedup: dedup_for_test(908),
            intents: vec![first, second],
        };

        assert_eq!(expect_delete_intents(sm.apply(command.clone()).unwrap()).created, 2);
        assert_eq!(expect_delete_intents(sm.apply(command).unwrap()).created, 2);
        assert_eq!(
            storage.get_delete_intent(5).unwrap().unwrap().block_id,
            existing.block_id
        );
        assert_eq!(
            storage.get_delete_intent(6).unwrap().unwrap().block_id,
            BlockId::new(data_handle_id, BlockIndex::new(1))
        );
        assert_eq!(
            storage.get_delete_intent(7).unwrap().unwrap().block_id,
            BlockId::new(data_handle_id, BlockIndex::new(2))
        );
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 2);
    }

    #[test]
    fn update_delete_intent_status_is_raft_authoritative_and_replay_stable() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let intent = make_delete_intent(9090, BlockId::new(DataHandleId::new(909), BlockIndex::new(0)));
        storage.put_delete_intent(&intent).unwrap();
        let command = Command::UpdateDeleteIntentStatus {
            dedup: dedup_for_test(909),
            intent_id: 9090,
            status: crate::state::DeleteIntentStatus::Completed,
            finished_at_ms: Some(1234),
            error_msg: None,
        };

        let result = expect_delete_intent_status(sm.apply(command.clone()).unwrap());
        assert_eq!(result.intent_id, 9090);
        assert!(matches!(result.status, crate::state::DeleteIntentStatus::Completed));
        assert!(matches!(
            storage.get_delete_intent(9090).unwrap().unwrap().status,
            crate::state::DeleteIntentStatus::Completed
        ));
        assert_eq!(
            storage.get_delete_intent(9090).unwrap().unwrap().finished_at_ms,
            Some(1234)
        );

        expect_delete_intent_status(sm.apply(command).unwrap());
        assert_eq!(
            storage.get_delete_intent(9090).unwrap().unwrap().finished_at_ms,
            Some(1234)
        );

        let mismatch = Command::UpdateDeleteIntentStatus {
            dedup: dedup_for_test(909),
            intent_id: 9090,
            status: crate::state::DeleteIntentStatus::Failed,
            finished_at_ms: Some(9999),
            error_msg: Some("different".to_string()),
        };
        assert!(matches!(sm.apply(mismatch), Err(MetadataError::InvalidArgument(_))));
        assert!(matches!(
            storage.get_delete_intent(9090).unwrap().unwrap().status,
            crate::state::DeleteIntentStatus::Completed
        ));
    }

    #[test]
    fn update_delete_intent_status_rejects_missing_and_invalid_transition_without_half_write() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let missing = Command::UpdateDeleteIntentStatus {
            dedup: dedup_for_test(910),
            intent_id: 9100,
            status: crate::state::DeleteIntentStatus::Completed,
            finished_at_ms: Some(1),
            error_msg: None,
        };
        assert!(matches!(sm.apply(missing), Err(MetadataError::NotFound(_))));

        let mut completed = make_delete_intent(9101, BlockId::new(DataHandleId::new(910), BlockIndex::new(0)));
        completed.status = crate::state::DeleteIntentStatus::Completed;
        completed.finished_at_ms = Some(10);
        storage.put_delete_intent(&completed).unwrap();
        let invalid = Command::UpdateDeleteIntentStatus {
            dedup: dedup_for_test(911),
            intent_id: 9101,
            status: crate::state::DeleteIntentStatus::Failed,
            finished_at_ms: Some(11),
            error_msg: Some("late failure".to_string()),
        };

        assert!(matches!(sm.apply(invalid), Err(MetadataError::InvalidArgument(_))));
        let stored = storage.get_delete_intent(9101).unwrap().unwrap();
        assert!(matches!(stored.status, crate::state::DeleteIntentStatus::Completed));
        assert_eq!(stored.finished_at_ms, Some(10));
        assert_eq!(stored.last_error_msg, None);
    }

    #[test]
    fn truncate_shrink_within_extent_updates_inode_layout_applied_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle_id = DataHandleId::new(91);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(910);
        install_file_with_extents(
            &storage,
            InodeId::new(909),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(block_id, 0, 1024)],
            1024,
        );
        storage.put_block_ref_count(block_id, 1).unwrap();

        let dedup = dedup_for_test(91);
        expect_fs_ok(
            sm.apply(Command::Truncate {
                dedup: dedup.clone(),
                inode_id,
                new_size: 512,
                lease_id: lease_id_for_inode_epoch(inode_id, 2),
                lease_epoch: 2,
            })
            .unwrap(),
        );

        let inode = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(inode.attrs.size, 512);
        match inode.data {
            InodeData::File {
                extents,
                lease_epoch,
                file_version,
            } => {
                let mut expected_extent = extent(block_id, 0, 512);
                expected_extent.file_version = Some(1);
                assert_eq!(extents, vec![expected_extent]);
                assert_eq!(file_version, Some(1));
                assert_eq!(lease_epoch, Some(2));
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
        assert_eq!(storage.get_layout(inode_id).unwrap(), FileLayout::new(4096, 4096, 1));
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn truncate_drops_full_blocks_creates_intent_and_replay_does_not_double_decrement() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle_id = DataHandleId::new(92);
        let kept_block = BlockId::new(data_handle_id, BlockIndex::new(0));
        let dropped_block = BlockId::new(data_handle_id, BlockIndex::new(1));
        let inode_id = InodeId::new(920);
        install_file_with_extents(
            &storage,
            InodeId::new(919),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(kept_block, 0, 4096), extent(dropped_block, 4096, 4096)],
            8192,
        );
        storage.put_block_ref_count(kept_block, 1).unwrap();
        storage.put_block_ref_count(dropped_block, 1).unwrap();

        let dedup = dedup_for_test(92);
        let command = Command::Truncate {
            dedup: dedup.clone(),
            inode_id,
            new_size: 4096,
            lease_id: lease_id_for_inode_epoch(inode_id, 2),
            lease_epoch: 2,
        };

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        assert_eq!(storage.get_block_ref_count(kept_block).unwrap(), Some(1));
        assert_eq!(storage.get_block_ref_count(dropped_block).unwrap(), None);
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 1);

        expect_fs_ok(sm.apply(command).unwrap());
        assert_eq!(storage.get_block_ref_count(kept_block).unwrap(), Some(1));
        assert_eq!(storage.get_block_ref_count(dropped_block).unwrap(), None);
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 1);
    }

    #[test]
    fn truncate_same_block_kept_and_dropped_does_not_release_kept_reference() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle_id = DataHandleId::new(925);
        let shared_block = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(9250);
        install_file_with_extents(
            &storage,
            InodeId::new(9249),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(shared_block, 0, 4096), extent(shared_block, 4096, 4096)],
            8192,
        );
        storage.put_block_ref_count(shared_block, 1).unwrap();

        expect_fs_ok(
            sm.apply(Command::Truncate {
                dedup: dedup_for_test(925),
                inode_id,
                new_size: 4096,
                lease_id: lease_id_for_inode_epoch(inode_id, 2),
                lease_epoch: 2,
            })
            .unwrap(),
        );

        assert_eq!(storage.get_block_ref_count(shared_block).unwrap(), Some(1));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
        let inode = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(inode.attrs.size, 4096);
        match inode.data {
            InodeData::File { extents, .. } => {
                let mut expected_extent = extent(shared_block, 0, 4096);
                expected_extent.file_version = Some(1);
                assert_eq!(extents, vec![expected_extent]);
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
        assert_eq!(storage.get_layout(inode_id).unwrap(), FileLayout::new(4096, 4096, 1));
    }

    #[test]
    fn truncate_multiple_zero_ref_blocks_allocates_stable_intent_ids_and_replay_is_noop() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle_id = DataHandleId::new(926);
        let first_block = BlockId::new(data_handle_id, BlockIndex::new(1));
        let second_block = BlockId::new(data_handle_id, BlockIndex::new(2));
        let inode_id = InodeId::new(9260);
        install_file_with_extents(
            &storage,
            InodeId::new(9259),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(first_block, 0, 4096), extent(second_block, 4096, 4096)],
            8192,
        );
        storage.put_block_ref_count(first_block, 1).unwrap();
        storage.put_block_ref_count(second_block, 1).unwrap();

        let command = Command::Truncate {
            dedup: dedup_for_test(926),
            inode_id,
            new_size: 0,
            lease_id: lease_id_for_inode_epoch(inode_id, 2),
            lease_epoch: 2,
        };
        expect_fs_ok(sm.apply(command.clone()).unwrap());
        expect_fs_ok(sm.apply(command).unwrap());

        let mut intents = storage.list_pending_delete_intents(10, u64::MAX).unwrap();
        intents.sort_by_key(|intent| intent.intent_id);
        assert_eq!(intents.len(), 2);
        assert_eq!(intents[0].intent_id, 1);
        assert_eq!(intents[0].block_id, first_block);
        assert_eq!(intents[1].intent_id, 2);
        assert_eq!(intents[1].block_id, second_block);
        assert_eq!(storage.get_block_ref_count(first_block).unwrap(), None);
        assert_eq!(storage.get_block_ref_count(second_block).unwrap(), None);
    }

    #[test]
    fn truncate_delete_intent_allocator_missing_bumps_above_existing_intent_id() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let existing_block = BlockId::new(DataHandleId::new(927), BlockIndex::new(0));
        let existing = make_delete_intent(1, existing_block);
        storage.put_delete_intent(&existing).unwrap();
        assert_eq!(read_next_delete_intent_id(&storage), None);

        let data_handle_id = DataHandleId::new(928);
        let dropped_block = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(9280);
        install_file_with_extents(
            &storage,
            InodeId::new(9279),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(dropped_block, 0, 4096)],
            4096,
        );
        storage.put_block_ref_count(dropped_block, 1).unwrap();

        let dedup = dedup_for_test(928);
        let command = Command::Truncate {
            dedup: dedup.clone(),
            inode_id,
            new_size: 0,
            lease_id: lease_id_for_inode_epoch(inode_id, 2),
            lease_epoch: 2,
        };

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        let stored_existing = storage.get_delete_intent(1).unwrap().unwrap();
        assert_eq!(stored_existing.block_id, existing_block);
        let new_intent = storage.get_delete_intent(2).unwrap().unwrap();
        assert_eq!(new_intent.block_id, dropped_block);
        assert_eq!(read_next_delete_intent_id(&storage), Some(3));
        assert_eq!(storage.get_block_ref_count(dropped_block).unwrap(), None);
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 0);
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());

        expect_fs_ok(sm.apply(command).unwrap());
        assert_eq!(storage.get_delete_intent(1).unwrap().unwrap().block_id, existing_block);
        assert_eq!(storage.get_delete_intent(2).unwrap().unwrap().block_id, dropped_block);
        assert_eq!(read_next_delete_intent_id(&storage), Some(3));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 2);
    }

    #[test]
    fn truncate_delete_intent_allocator_lagging_bumps_above_existing_intent_id() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        write_next_delete_intent_id(&storage, 1);
        let existing_block = BlockId::new(DataHandleId::new(929), BlockIndex::new(10));
        let mut existing = make_delete_intent(10, existing_block);
        existing.status = crate::state::DeleteIntentStatus::Completed;
        existing.finished_at_ms = Some(100);
        storage.put_delete_intent(&existing).unwrap();

        let data_handle_id = DataHandleId::new(930);
        let dropped_block = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(9300);
        install_file_with_extents(
            &storage,
            InodeId::new(9299),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(dropped_block, 0, 4096)],
            4096,
        );
        storage.put_block_ref_count(dropped_block, 1).unwrap();

        let command = Command::Truncate {
            dedup: dedup_for_test(930),
            inode_id,
            new_size: 0,
            lease_id: lease_id_for_inode_epoch(inode_id, 2),
            lease_epoch: 2,
        };

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        let stored_existing = storage.get_delete_intent(10).unwrap().unwrap();
        assert_eq!(stored_existing.block_id, existing_block);
        assert!(matches!(
            stored_existing.status,
            crate::state::DeleteIntentStatus::Completed
        ));
        assert_eq!(storage.get_delete_intent(11).unwrap().unwrap().block_id, dropped_block);
        assert_eq!(read_next_delete_intent_id(&storage), Some(12));
        assert_eq!(storage.get_block_ref_count(dropped_block).unwrap(), None);

        expect_fs_ok(sm.apply(command).unwrap());
        assert_eq!(storage.get_delete_intent(10).unwrap().unwrap().block_id, existing_block);
        assert_eq!(storage.get_delete_intent(11).unwrap().unwrap().block_id, dropped_block);
        assert_eq!(read_next_delete_intent_id(&storage), Some(12));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 1);
    }

    #[test]
    fn truncate_delete_intent_allocator_ahead_does_not_rewind() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        write_next_delete_intent_id(&storage, 100);
        let existing_block = BlockId::new(DataHandleId::new(931), BlockIndex::new(10));
        storage
            .put_delete_intent(&make_delete_intent(10, existing_block))
            .unwrap();

        let data_handle_id = DataHandleId::new(932);
        let dropped_block = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(9320);
        install_file_with_extents(
            &storage,
            InodeId::new(9319),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(dropped_block, 0, 4096)],
            4096,
        );
        storage.put_block_ref_count(dropped_block, 1).unwrap();

        expect_fs_ok(
            sm.apply(Command::Truncate {
                dedup: dedup_for_test(932),
                inode_id,
                new_size: 0,
                lease_id: lease_id_for_inode_epoch(inode_id, 2),
                lease_epoch: 2,
            })
            .unwrap(),
        );

        assert_eq!(storage.get_delete_intent(10).unwrap().unwrap().block_id, existing_block);
        assert_eq!(storage.get_delete_intent(100).unwrap().unwrap().block_id, dropped_block);
        assert_eq!(read_next_delete_intent_id(&storage), Some(101));
    }

    #[test]
    fn truncate_rejects_data_handle_mismatch_and_missing_refcount_without_half_commit() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_data_handle = DataHandleId::new(93);
        let wrong_data_handle = DataHandleId::new(94);
        let mismatched_block = BlockId::new(wrong_data_handle, BlockIndex::new(0));
        let inode_id = InodeId::new(930);
        install_file_with_extents(
            &storage,
            InodeId::new(929),
            "file",
            inode_id,
            inode_data_handle,
            vec![extent(mismatched_block, 0, 4096)],
            4096,
        );

        expect_fs_errno(
            sm.apply(Command::Truncate {
                dedup: dedup_for_test(93),
                inode_id,
                new_size: 0,
                lease_id: lease_id_for_inode_epoch(inode_id, 2),
                lease_epoch: 2,
            })
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 4096);
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);

        let correct_block = BlockId::new(inode_data_handle, BlockIndex::new(1));
        let missing_ref_inode = InodeId::new(931);
        install_file_with_extents(
            &storage,
            InodeId::new(929),
            "missing-ref",
            missing_ref_inode,
            inode_data_handle,
            vec![extent(correct_block, 0, 4096)],
            4096,
        );

        expect_fs_errno(
            sm.apply(Command::Truncate {
                dedup: dedup_for_test(94),
                inode_id: missing_ref_inode,
                new_size: 0,
                lease_id: lease_id_for_inode_epoch(missing_ref_inode, 2),
                lease_epoch: 2,
            })
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(missing_ref_inode).unwrap().unwrap().attrs.size, 4096);
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
    }

    #[test]
    fn truncate_grow_remains_not_supported_and_same_size_is_stable_noop() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle_id = DataHandleId::new(95);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(950);
        install_file_with_extents(
            &storage,
            InodeId::new(949),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(block_id, 0, 1024)],
            1024,
        );
        storage.put_block_ref_count(block_id, 1).unwrap();

        expect_fs_errno(
            sm.apply(Command::Truncate {
                dedup: dedup_for_test(95),
                inode_id,
                new_size: 2048,
                lease_id: lease_id_for_inode_epoch(inode_id, 2),
                lease_epoch: 2,
            })
            .unwrap(),
            FsErrorCode::ENotsup,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 1024);

        let dedup = dedup_for_test(96);
        expect_fs_ok(
            sm.apply(Command::Truncate {
                dedup: dedup.clone(),
                inode_id,
                new_size: 1024,
                lease_id: lease_id_for_inode_epoch(inode_id, 2),
                lease_epoch: 2,
            })
            .unwrap(),
        );
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());

        let mismatch = sm.apply(Command::Truncate {
            dedup,
            inode_id,
            new_size: 512,
            lease_id: lease_id_for_inode_epoch(inode_id, 2),
            lease_epoch: 2,
        });
        assert!(matches!(mismatch, Err(MetadataError::InvalidArgument(_))));
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
    }

    #[test]
    fn truncate_rejects_invalid_lease_identity_and_epoch_without_half_commit() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle_id = DataHandleId::new(958);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(9580);
        install_file_with_extents(
            &storage,
            InodeId::new(9579),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(block_id, 0, 4096)],
            4096,
        );
        storage.put_block_ref_count(block_id, 1).unwrap();

        expect_fs_errno(
            sm.apply(Command::Truncate {
                dedup: dedup_for_test(958),
                inode_id,
                new_size: 0,
                lease_id: types::ids::LeaseId::new(1),
                lease_epoch: 2,
            })
            .unwrap(),
            FsErrorCode::EAcces,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 4096);
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);

        expect_fs_errno(
            sm.apply(Command::Truncate {
                dedup: dedup_for_test(959),
                inode_id,
                new_size: 0,
                lease_id: lease_id_for_inode_epoch(inode_id, 1),
                lease_epoch: 1,
            })
            .unwrap(),
            FsErrorCode::EAcces,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 4096);
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
    }

    #[test]
    fn lease_commands_reapply_return_original_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle = DataHandleId::new(77);
        let inode_id = InodeId::new(770);
        let block_id = BlockId::new(data_handle, BlockIndex::new(0));
        storage.put_data_handle_owner(data_handle, inode_id).unwrap();
        storage
            .put_block(&BlockMetaState {
                block_id,
                inode_id,
                data_handle_id: data_handle,
                state: BlockState::Open,
                placement: BlockPlacement {
                    primary: WorkerId::new(1),
                    replicas: Vec::new(),
                },
                committed_length: 0,
            })
            .unwrap();

        let acquire = Command::AcquireLease {
            dedup: dedup_for_test(77),
            block_id,
            client_id: ClientId::new(77),
            epoch: 1,
            expires_at_ms: 1000,
        };
        let first = expect_lease_acquired(sm.apply(acquire.clone()).unwrap());
        let second = expect_lease_acquired(sm.apply(acquire).unwrap());
        assert_eq!(second.lease.owner, first.lease.owner);
        assert_eq!(second.lease.epoch, first.lease.epoch);

        let release = Command::ReleaseLease {
            dedup: dedup_for_test(78),
            block_id,
        };
        expect_lease_released(sm.apply(release.clone()).unwrap());
        expect_lease_released(sm.apply(release).unwrap());
        assert!(storage.get_lease(block_id).unwrap().is_none());
    }

    #[test]
    fn rename_overwrites_empty_file() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let source = sm
            .apply(Command::Create {
                dedup: dedup_for_test(38),
                parent_inode_id,
                name: "source".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap();
        let source_inode_id = match source {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected apply response: {:?}", other),
        };

        let target = sm
            .apply(Command::Create {
                dedup: dedup_for_test(39),
                parent_inode_id,
                name: "target".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(8192, 8192, 1),
            })
            .unwrap();
        let target_inode_id = match target {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected apply response: {:?}", other),
        };
        let target_inode = storage.get_inode(target_inode_id).unwrap().unwrap();
        let target_handle = target_inode.current_data_handle_id;
        let source_handle = storage
            .get_inode(source_inode_id)
            .unwrap()
            .unwrap()
            .current_data_handle_id;

        expect_fs_ok(
            sm.apply(Command::Rename {
                dedup: dedup_for_test(40),
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
            })
            .unwrap(),
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "source").unwrap(), None);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(source_inode_id)
        );
        assert!(storage.get_inode(source_inode_id).unwrap().is_some());
        assert!(storage.get_inode(target_inode_id).unwrap().is_none());
        assert_eq!(
            storage.get_inode_by_data_handle(source_handle).unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(storage.get_inode_by_data_handle(target_handle).unwrap(), None);
        assert!(storage.get_layout(target_inode_id).is_err());
    }

    #[test]
    fn rename_overwrites_file_with_extents() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(110);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        let source = expect_fs_ok(
            sm.apply(Command::Create {
                dedup: dedup_for_test(110),
                parent_inode_id,
                name: "source".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap(),
        );
        let source_inode_id = source.inode_id.unwrap();
        let target_inode_id = InodeId::new(1111);
        let target_handle = DataHandleId::new(112);
        let block_id = BlockId::new(target_handle, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "target",
            target_inode_id,
            target_handle,
            vec![extent(block_id, 0, 128)],
            128,
        );
        storage.put_block_ref_count(block_id, 1).unwrap();

        expect_fs_ok(
            sm.apply(Command::Rename {
                dedup: dedup_for_test(111),
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
            })
            .unwrap(),
        );

        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(source_inode_id)
        );
        assert!(storage.get_inode(target_inode_id).unwrap().is_none());
        assert!(storage.get_layout(target_inode_id).is_err());
        assert_eq!(storage.get_inode_by_data_handle(target_handle).unwrap(), None);
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), None);
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 1);
    }

    #[test]
    fn rename_overwrite_releases_old_blocks() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(120);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        expect_fs_ok(
            sm.apply(Command::Create {
                dedup: dedup_for_test(120),
                parent_inode_id,
                name: "source".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap(),
        );
        let target_inode_id = InodeId::new(1221);
        let target_handle = DataHandleId::new(122);
        let block_id = BlockId::new(target_handle, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "target",
            target_inode_id,
            target_handle,
            vec![extent(block_id, 0, 128)],
            128,
        );
        storage.put_block_ref_count(block_id, 2).unwrap();

        expect_fs_ok(
            sm.apply(Command::Rename {
                dedup: dedup_for_test(121),
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
            })
            .unwrap(),
        );

        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
    }

    #[test]
    fn rename_overwrite_creates_delete_intents() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(130);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        expect_fs_ok(
            sm.apply(Command::Create {
                dedup: dedup_for_test(130),
                parent_inode_id,
                name: "source".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap(),
        );
        let target_inode_id = InodeId::new(1331);
        let target_handle = DataHandleId::new(132);
        let block_id = BlockId::new(target_handle, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "target",
            target_inode_id,
            target_handle,
            vec![extent(block_id, 0, 128)],
            128,
        );
        storage.put_block_ref_count(block_id, 1).unwrap();

        expect_fs_ok(
            sm.apply(Command::Rename {
                dedup: dedup_for_test(131),
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
            })
            .unwrap(),
        );

        let intents = storage.list_pending_delete_intents(10, u64::MAX).unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].block_id, block_id);
    }

    #[test]
    fn rename_rejects_non_empty_directory_target() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(140);
        let source_dir_id = InodeId::new(141);
        let target_dir_id = InodeId::new(142);
        let child_inode_id = InodeId::new(143);
        let mount_id = MountId::new(1);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_dir(source_dir_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_dir(target_dir_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_file(
                child_inode_id,
                FileAttrs::new(),
                mount_id,
                DataHandleId::new(143),
            ))
            .unwrap();
        storage.put_dentry(parent_inode_id, "source", source_dir_id).unwrap();
        storage.put_dentry(parent_inode_id, "target", target_dir_id).unwrap();
        storage.put_dentry(target_dir_id, "child", child_inode_id).unwrap();

        expect_fs_errno(
            sm.apply(Command::Rename {
                dedup: dedup_for_test(140),
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
            })
            .unwrap(),
            FsErrorCode::ENotEmpty,
        );

        assert_eq!(
            storage.get_dentry(parent_inode_id, "source").unwrap(),
            Some(source_dir_id)
        );
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(target_dir_id)
        );
        assert_eq!(
            storage.get_dentry(target_dir_id, "child").unwrap(),
            Some(child_inode_id)
        );
    }

    #[test]
    fn rename_overwrite_is_dedup_safe() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(150);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        let source = expect_fs_ok(
            sm.apply(Command::Create {
                dedup: dedup_for_test(150),
                parent_inode_id,
                name: "source".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap(),
        );
        let source_inode_id = source.inode_id.unwrap();
        let target = expect_fs_ok(
            sm.apply(Command::Create {
                dedup: dedup_for_test(151),
                parent_inode_id,
                name: "target".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(8192, 8192, 1),
            })
            .unwrap(),
        );
        let target_inode_id = target.inode_id.unwrap();

        let dedup = dedup_for_test(152);
        let command = Command::Rename {
            dedup: dedup.clone(),
            src_parent_inode_id: parent_inode_id,
            src_name: "source".to_string(),
            dst_parent_inode_id: parent_inode_id,
            dst_name: "target".to_string(),
            flags: 0,
        };

        let first = expect_fs_ok(sm.apply(command.clone()).unwrap());
        let second = expect_fs_ok(sm.apply(command).unwrap());

        assert_eq!(second, first);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(source_inode_id)
        );
        assert!(storage.get_inode(target_inode_id).unwrap().is_none());
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn rename_replay_does_not_double_release_blocks() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(160);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        expect_fs_ok(
            sm.apply(Command::Create {
                dedup: dedup_for_test(160),
                parent_inode_id,
                name: "source".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap(),
        );
        let target_inode_id = InodeId::new(1661);
        let target_handle = DataHandleId::new(162);
        let block_id = BlockId::new(target_handle, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "target",
            target_inode_id,
            target_handle,
            vec![extent(block_id, 0, 128)],
            128,
        );
        storage.put_block_ref_count(block_id, 2).unwrap();

        let command = Command::Rename {
            dedup: dedup_for_test(161),
            src_parent_inode_id: parent_inode_id,
            src_name: "source".to_string(),
            dst_parent_inode_id: parent_inode_id,
            dst_name: "target".to_string(),
            flags: 0,
        };

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));

        expect_fs_ok(sm.apply(command).unwrap());
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
    }

    #[test]
    fn rename_reusing_call_id_for_different_target_is_rejected() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(170);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        let source = expect_fs_ok(
            sm.apply(Command::Create {
                dedup: dedup_for_test(170),
                parent_inode_id,
                name: "source".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap(),
        );
        let source_inode_id = source.inode_id.unwrap();

        let dedup = dedup_for_test(171);
        expect_fs_ok(
            sm.apply(Command::Rename {
                dedup: dedup.clone(),
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "first".to_string(),
                flags: 0,
            })
            .unwrap(),
        );

        let mismatch = sm
            .apply(Command::Rename {
                dedup,
                src_parent_inode_id: parent_inode_id,
                src_name: "first".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "second".to_string(),
                flags: 0,
            })
            .unwrap_err();

        assert!(matches!(mismatch, MetadataError::InvalidArgument(_)));
        assert_eq!(
            storage.get_dentry(parent_inode_id, "first").unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(storage.get_dentry(parent_inode_id, "second").unwrap(), None);
    }

    #[test]
    fn create_allocates_distinct_inode_ids() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let first = sm
            .apply(Command::Create {
                dedup: dedup_for_test(30),
                parent_inode_id,
                name: "first".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap();
        let second = sm
            .apply(Command::Create {
                dedup: dedup_for_test(31),
                parent_inode_id,
                name: "second".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap();

        let first_inode_id = match first {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected response: {:?}", other),
        };
        let second_inode_id = match second {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected response: {:?}", other),
        };
        assert_ne!(first_inode_id, second_inode_id);
    }

    #[test]
    fn create_continues_inode_allocator_after_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("state_machine_inode_allocator");
        let parent_inode_id = InodeId::new(100);
        let first_inode_id = {
            let storage = Arc::new(RocksDBStorage::open(&db_path).unwrap());
            storage
                .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
                .unwrap();
            let mount_table = Arc::new(MountTable::new());
            let sm = AppRaftStateMachine::new(Arc::clone(&storage), mount_table);
            let response = sm
                .apply(Command::Create {
                    dedup: dedup_for_test(32),
                    parent_inode_id,
                    name: "before-reopen".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                })
                .unwrap();
            match response {
                AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
                other => panic!("unexpected response: {:?}", other),
            }
        };

        let storage = Arc::new(RocksDBStorage::open(&db_path).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), mount_table);
        let response = sm
            .apply(Command::Create {
                dedup: dedup_for_test(33),
                parent_inode_id,
                name: "after-reopen".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap();
        let second_inode_id = match response {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected response: {:?}", other),
        };

        assert_ne!(first_inode_id, second_inode_id);
        assert!(second_inode_id.as_raw() > first_inode_id.as_raw());
    }

    #[test]
    fn close_write_extents_must_use_inode_data_handle() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(7);
        let data_handle_id = DataHandleId::new(99);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        sm.apply(Command::CloseWrite {
            dedup: dedup_for_test(34),
            inode_id,
            extents: vec![types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 11,
                file_version: None,
                block_stamp: None,
            }],
            final_size: 11,
            lease_id: types::ids::LeaseId::new(1),
            open_epoch: 1,
            lease_epoch: 1,
            commit_mode: FileCommitMode::Append,
        })
        .unwrap();
        let updated = storage.get_inode(inode_id).unwrap().unwrap();
        match updated.data {
            types::fs::InodeData::File { extents, .. } => {
                assert_eq!(extents[0].block_id.data_handle_id, data_handle_id)
            }
            other => panic!("unexpected inode data: {:?}", other),
        }

        let mismatch = sm.apply(Command::CloseWrite {
            dedup: dedup_for_test(35),
            inode_id,
            extents: vec![types::fs::Extent {
                file_offset: 11,
                // Intentional invalid fixture: extents must use inode.current_data_handle_id.
                block_id: BlockId::new(DataHandleId::new(inode_id.as_raw()), BlockIndex::new(1)),
                block_offset: 0,
                len: 1,
                file_version: None,
                block_stamp: None,
            }],
            final_size: 12,
            lease_id: types::ids::LeaseId::new(1),
            open_epoch: 1,
            lease_epoch: 2,
            commit_mode: FileCommitMode::Append,
        });
        assert!(matches!(
            mismatch,
            Ok(AppDataResponse::Fs(FsCommandResult::Err(FsErrnoResult {
                errno: FsErrorCode::EInval,
                ..
            })))
        ));
    }

    #[test]
    fn apply_rejects_duplicate_blocks() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(73);
        let data_handle_id = DataHandleId::new(173);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_errno(
            sm.apply(close_write_command(
                dedup_for_test(93),
                inode_id,
                vec![
                    close_extent(data_handle_id, 0, 0, 64),
                    close_extent(data_handle_id, 0, 64, 64),
                ],
                128,
                FileCommitMode::Append,
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
    }

    #[test]
    fn apply_rejects_overlapping_ranges() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(74);
        let data_handle_id = DataHandleId::new(174);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_errno(
            sm.apply(close_write_command(
                dedup_for_test(94),
                inode_id,
                vec![
                    close_extent(data_handle_id, 0, 0, 64),
                    close_extent(data_handle_id, 1, 32, 64),
                ],
                96,
                FileCommitMode::Append,
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
    }

    #[test]
    fn apply_rejects_zero_length_block() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(75);
        let data_handle_id = DataHandleId::new(175);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_errno(
            sm.apply(close_write_command(
                dedup_for_test(95),
                inode_id,
                vec![close_extent(data_handle_id, 0, 0, 0)],
                0,
                FileCommitMode::Append,
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
    }

    #[test]
    fn apply_rejects_bad_final_size() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(76);
        let data_handle_id = DataHandleId::new(176);
        let old_extent = close_extent(data_handle_id, 0, 0, 64);
        let mut attrs = FileAttrs::new();
        attrs.size = 64;
        let mut inode = Inode::new_file(inode_id, attrs, MountId::new(1), data_handle_id);
        if let InodeData::File { extents, .. } = &mut inode.data {
            extents.push(old_extent);
        }
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_errno(
            sm.apply(close_write_command(
                dedup_for_test(96),
                inode_id,
                vec![close_extent(data_handle_id, 1, 64, 64)],
                200,
                FileCommitMode::Append,
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
    }

    #[test]
    fn apply_replace_removes_old_layout() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(77);
        let data_handle_id = DataHandleId::new(177);
        let old_block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let new_block_id = BlockId::new(data_handle_id, BlockIndex::new(1));
        let mut attrs = FileAttrs::new();
        attrs.size = 64;
        let mut inode = Inode::new_file(inode_id, attrs, MountId::new(1), data_handle_id);
        if let InodeData::File { extents, .. } = &mut inode.data {
            extents.push(close_extent(data_handle_id, 0, 0, 64));
        }
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();
        storage.put_block_ref_count(old_block_id, 1).unwrap();

        expect_fs_ok(
            sm.apply(close_write_command(
                dedup_for_test(97),
                inode_id,
                vec![close_extent(data_handle_id, 1, 0, 32)],
                32,
                FileCommitMode::Replace,
            ))
            .unwrap(),
        );

        let updated = storage.get_inode(inode_id).unwrap().unwrap();
        match updated.data {
            InodeData::File { extents, .. } => {
                assert_eq!(extents.len(), 1);
                assert_eq!(extents[0].block_id, new_block_id);
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
        assert_eq!(storage.get_block_ref_count(old_block_id).unwrap(), None);
        assert_eq!(storage.get_block_ref_count(new_block_id).unwrap(), Some(1));
    }

    #[test]
    fn apply_append_keeps_old_layout() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(78);
        let data_handle_id = DataHandleId::new(178);
        let old_block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let new_block_id = BlockId::new(data_handle_id, BlockIndex::new(1));
        let mut attrs = FileAttrs::new();
        attrs.size = 64;
        let mut inode = Inode::new_file(inode_id, attrs, MountId::new(1), data_handle_id);
        if let InodeData::File { extents, .. } = &mut inode.data {
            extents.push(close_extent(data_handle_id, 0, 0, 64));
        }
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();
        storage.put_block_ref_count(old_block_id, 1).unwrap();

        expect_fs_ok(
            sm.apply(close_write_command(
                dedup_for_test(98),
                inode_id,
                vec![close_extent(data_handle_id, 1, 64, 32)],
                96,
                FileCommitMode::Append,
            ))
            .unwrap(),
        );

        let updated = storage.get_inode(inode_id).unwrap().unwrap();
        match updated.data {
            InodeData::File { extents, .. } => {
                assert_eq!(extents.len(), 2);
                assert_eq!(extents[0].block_id, old_block_id);
                assert_eq!(extents[1].block_id, new_block_id);
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
        assert_eq!(storage.get_block_ref_count(old_block_id).unwrap(), Some(1));
        assert_eq!(storage.get_block_ref_count(new_block_id).unwrap(), Some(1));
    }

    #[test]
    fn apply_rejects_append_offset_not_current_size() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(79);
        let data_handle_id = DataHandleId::new(179);
        let old_extent = close_extent(data_handle_id, 0, 0, 64);
        let mut attrs = FileAttrs::new();
        attrs.size = 64;
        let mut inode = Inode::new_file(inode_id, attrs, MountId::new(1), data_handle_id);
        if let InodeData::File { extents, .. } = &mut inode.data {
            extents.push(old_extent);
        }
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_errno(
            sm.apply(close_write_command(
                dedup_for_test(99),
                inode_id,
                vec![close_extent(data_handle_id, 1, 32, 32)],
                64,
                FileCommitMode::Append,
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
    }

    #[test]
    fn dedup_rejects_commit_mode_mismatch() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(80);
        let data_handle_id = DataHandleId::new(180);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let dedup = dedup_for_test(100);
        let extents = vec![close_extent(data_handle_id, 0, 0, 64)];
        expect_fs_ok(
            sm.apply(close_write_command(
                dedup.clone(),
                inode_id,
                extents.clone(),
                64,
                FileCommitMode::Replace,
            ))
            .unwrap(),
        );
        let mismatch = sm
            .apply(close_write_command(
                dedup,
                inode_id,
                extents,
                64,
                FileCommitMode::Append,
            ))
            .expect_err("same call_id with different commit mode must be rejected");
        assert!(matches!(mismatch, MetadataError::InvalidArgument(_)));
    }

    #[test]
    fn apply_replace_releases_old_blocks() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(70);
        let data_handle_id = DataHandleId::new(1700);
        let old_block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let new_block_id = BlockId::new(data_handle_id, BlockIndex::new(1));
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        inode.attrs.size = 64;
        if let types::fs::InodeData::File { extents, .. } = &mut inode.data {
            extents.push(types::fs::Extent {
                file_offset: 0,
                block_id: old_block_id,
                block_offset: 0,
                len: 64,
                file_version: None,
                block_stamp: None,
            });
        }
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();
        storage.put_block_ref_count(old_block_id, 1).unwrap();

        expect_fs_ok(
            sm.apply(Command::CloseWrite {
                dedup: dedup_for_test(36),
                inode_id,
                extents: vec![types::fs::Extent {
                    file_offset: 0,
                    block_id: new_block_id,
                    block_offset: 0,
                    len: 32,
                    file_version: None,
                    block_stamp: None,
                }],
                final_size: 32,
                lease_id: types::ids::LeaseId::new(1),
                open_epoch: 1,
                lease_epoch: 2,
                commit_mode: FileCommitMode::Replace,
            })
            .unwrap(),
        );

        let updated = storage.get_inode(inode_id).unwrap().unwrap();
        match updated.data {
            types::fs::InodeData::File { extents, .. } => {
                assert_eq!(extents.len(), 1);
                assert_eq!(extents[0].block_id, new_block_id);
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
        assert_eq!(storage.get_block_ref_count(old_block_id).unwrap(), None);
        assert_eq!(storage.get_block_ref_count(new_block_id).unwrap(), Some(1));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 1);
    }

    #[test]
    fn close_write_success_replay_returns_original_result_without_reapplying_mutation() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(70);
        let data_handle_id = DataHandleId::new(170);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        let layout = FileLayout::new(4096, 4096, 1);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, layout).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let dedup = dedup_for_test(90);
        let command = Command::CloseWrite {
            dedup: dedup.clone(),
            inode_id,
            extents: vec![types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 64,
                file_version: None,
                block_stamp: None,
            }],
            final_size: 64,
            lease_id: types::ids::LeaseId::new(1),
            open_epoch: 1,
            lease_epoch: 3,
            commit_mode: FileCommitMode::Append,
        };

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        let first_inode = storage.get_inode(inode_id).unwrap().unwrap();
        let first_result = match storage.get_applied_result(&dedup).unwrap().unwrap().result {
            AppDataResponse::Fs(result) => result,
            other => panic!("unexpected applied result: {:?}", other),
        };

        expect_fs_ok(sm.apply(command).unwrap());
        let replayed_inode = storage.get_inode(inode_id).unwrap().unwrap();
        let replayed_result = match storage.get_applied_result(&dedup).unwrap().unwrap().result {
            AppDataResponse::Fs(result) => result,
            other => panic!("unexpected applied result: {:?}", other),
        };

        assert_eq!(first_result, replayed_result);
        assert_eq!(replayed_inode, first_inode);
        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
    }

    #[test]
    fn close_write_extent_data_handle_mismatch_persists_error_without_half_commit() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(71);
        let data_handle_id = DataHandleId::new(171);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        let layout = FileLayout::new(4096, 4096, 1);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, layout).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let bad_block_id = BlockId::new(DataHandleId::new(999), BlockIndex::new(0));
        let command = Command::CloseWrite {
            dedup: dedup_for_test(91),
            inode_id,
            extents: vec![types::fs::Extent {
                file_offset: 0,
                block_id: bad_block_id,
                block_offset: 0,
                len: 64,
                file_version: None,
                block_stamp: None,
            }],
            final_size: 64,
            lease_id: types::ids::LeaseId::new(1),
            open_epoch: 1,
            lease_epoch: 3,
            commit_mode: FileCommitMode::Append,
        };

        expect_fs_errno(sm.apply(command.clone()).unwrap(), FsErrorCode::EInval);
        expect_fs_errno(sm.apply(command).unwrap(), FsErrorCode::EInval);

        let stored = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(stored, inode);
        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
        assert_eq!(storage.get_block_ref_count(bad_block_id).unwrap(), None);
    }

    #[test]
    fn close_write_fingerprint_mismatch_does_not_reapply_mutation() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let inode_id = InodeId::new(72);
        let data_handle_id = DataHandleId::new(172);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        let layout = FileLayout::new(4096, 4096, 1);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, layout).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let dedup = dedup_for_test(92);
        let first_block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let first = Command::CloseWrite {
            dedup: dedup.clone(),
            inode_id,
            extents: vec![types::fs::Extent {
                file_offset: 0,
                block_id: first_block_id,
                block_offset: 0,
                len: 64,
                file_version: None,
                block_stamp: None,
            }],
            final_size: 64,
            lease_id: types::ids::LeaseId::new(1),
            open_epoch: 1,
            lease_epoch: 3,
            commit_mode: FileCommitMode::Append,
        };
        let second_block_id = BlockId::new(data_handle_id, BlockIndex::new(1));
        let mismatch = Command::CloseWrite {
            dedup,
            inode_id,
            extents: vec![types::fs::Extent {
                file_offset: 64,
                block_id: second_block_id,
                block_offset: 0,
                len: 64,
                file_version: None,
                block_stamp: None,
            }],
            final_size: 128,
            lease_id: types::ids::LeaseId::new(1),
            open_epoch: 1,
            lease_epoch: 3,
            commit_mode: FileCommitMode::Append,
        };

        expect_fs_ok(sm.apply(first).unwrap());
        let first_inode = storage.get_inode(inode_id).unwrap().unwrap();
        let err = sm.apply(mismatch).unwrap_err();

        assert!(matches!(err, MetadataError::InvalidArgument(_)));
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), first_inode);
        assert_eq!(storage.get_block_ref_count(first_block_id).unwrap(), Some(1));
        assert_eq!(storage.get_block_ref_count(second_block_id).unwrap(), None);
    }
    #[test]
    #[ignore = "pending identity-pivot follow-ups"]
    fn allocate_block_validates_handle_owner() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle = DataHandleId::new(1);
        let inode_id = InodeId::new(2);
        storage.put_data_handle_owner(data_handle, inode_id).unwrap();

        let block_id = BlockId::new(data_handle, BlockIndex::new(0));
        let placement = BlockPlacement {
            primary: WorkerId::new(1),
            replicas: vec![],
        };

        // Success path
        sm.apply(Command::AllocateBlock {
            dedup: crate::raft::types::DedupKey::new(ClientId::new(11), CallId::new()),
            inode_id,
            block_id,
            placement: placement.clone(),
        })
        .unwrap();

        // Mismatch should fail
        let err = sm
            .apply(Command::AllocateBlock {
                dedup: crate::raft::types::DedupKey::new(ClientId::new(12), CallId::new()),
                inode_id: InodeId::new(999),
                block_id,
                placement,
            })
            .unwrap_err();
        assert!(matches!(err, MetadataError::InvalidArgument(_)));
    }

    #[test]
    fn acquire_lease_validates_handle_owner() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let data_handle = DataHandleId::new(42);
        let inode_id = InodeId::new(100);
        storage.put_data_handle_owner(data_handle, inode_id).unwrap();

        let block_id = BlockId::new(data_handle, BlockIndex::new(0));
        let placement = BlockPlacement {
            primary: WorkerId::new(1),
            replicas: Vec::new(),
        };
        let block_meta = BlockMetaState {
            block_id,
            inode_id,
            data_handle_id: data_handle,
            state: BlockState::Open,
            placement: placement.clone(),
            committed_length: 0,
        };
        storage.put_block(&block_meta).unwrap();

        let lease_raw = sm
            .apply(Command::AcquireLease {
                dedup: dedup_for_test(20),
                block_id,
                client_id: ClientId::new(9),
                epoch: 1,
                expires_at_ms: 9999,
            })
            .unwrap();
        let lease: LeaseState = match lease_raw {
            AppDataResponse::Lease(LeaseCommandResult::Acquired(lease)) => lease,
            other => panic!("unexpected lease response: {:?}", other),
        };
        assert_eq!(lease.block_id, block_id);
        assert_eq!(lease.lease.owner, ClientId::new(9));

        // Remap the handle to a different inode to trigger validation failure.
        storage
            .put_data_handle_owner(data_handle, InodeId::new(999))
            .expect("should update mapping for test");
        let err = sm
            .apply(Command::AcquireLease {
                dedup: dedup_for_test(21),
                block_id,
                client_id: ClientId::new(9),
                epoch: 2,
                expires_at_ms: 19999,
            })
            .unwrap_err();
        assert!(matches!(err, MetadataError::InvalidArgument(_)));
    }

    #[test]
    fn dedup_fingerprint_mismatch_does_not_apply_mutation_or_reapply_mutation() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let dedup = dedup_for_test(45);
        let first = Command::Create {
            dedup: dedup.clone(),
            parent_inode_id,
            name: "first".to_string(),
            attrs: FileAttrs::new(),
            layout: FileLayout::new(4096, 4096, 1),
        };
        let mismatch = Command::Create {
            dedup,
            parent_inode_id,
            name: "second".to_string(),
            attrs: FileAttrs::new(),
            layout: FileLayout::new(4096, 4096, 1),
        };

        sm.apply(first).unwrap();
        let err = sm.apply(mismatch).unwrap_err();

        assert!(matches!(err, MetadataError::InvalidArgument(_)));
        assert_eq!(storage.get_dentry(parent_inode_id, "second").unwrap(), None);
    }

    #[test]
    fn unlink_empty_file_deletes_namespace_data_owner_and_replays_without_mutating_again() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        let mut parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let inode_id = InodeId::new(11);
        let data_handle_id = DataHandleId::new(12);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), parent.mount_id, data_handle_id);
        storage.put_inode(&parent).unwrap();
        parent.attrs.update_mtime_ctime(1);
        storage
            .create_file_atomic(parent_inode_id, "file", &inode, &parent, FileLayout::new(4096, 4096, 1))
            .unwrap();

        let dedup = dedup_for_test(80);
        let command = Command::Unlink {
            dedup: dedup.clone(),
            parent_inode_id,
            name: "file".to_string(),
        };

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert_eq!(storage.get_inode_by_data_handle(data_handle_id).unwrap(), None);
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());

        expect_fs_ok(sm.apply(command).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
    }

    #[test]
    fn unlink_file_with_extents_deletes_layout_owner_refcount_and_creates_intent_once() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(20);
        let inode_id = InodeId::new(21);
        let data_handle_id = DataHandleId::new(22);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "file",
            inode_id,
            data_handle_id,
            vec![extent(block_id, 0, 128)],
            128,
        );
        storage.put_block_ref_count(block_id, 1).unwrap();

        let command = Command::Unlink {
            dedup: dedup_for_test(81),
            parent_inode_id,
            name: "file".to_string(),
        };

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert!(storage.get_layout(inode_id).is_err());
        assert_eq!(storage.get_inode_by_data_handle(data_handle_id).unwrap(), None);
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), None);
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 1);

        expect_fs_ok(sm.apply(command).unwrap());
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), None);
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 1);
    }

    #[test]
    fn unlink_file_with_shared_extent_only_decrements_refcount() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(50);
        let inode_id = InodeId::new(51);
        let data_handle_id = DataHandleId::new(52);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "file",
            inode_id,
            data_handle_id,
            vec![extent(block_id, 0, 128)],
            128,
        );
        storage.put_block_ref_count(block_id, 2).unwrap();

        expect_fs_ok(
            sm.apply(Command::Unlink {
                dedup: dedup_for_test(97),
                parent_inode_id,
                name: "file".to_string(),
            })
            .unwrap(),
        );

        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
    }

    #[test]
    fn unlink_file_with_missing_refcount_returns_error_without_half_delete() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(60);
        let inode_id = InodeId::new(61);
        let data_handle_id = DataHandleId::new(62);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "file",
            inode_id,
            data_handle_id,
            vec![extent(block_id, 0, 128)],
            128,
        );

        expect_fs_errno(
            sm.apply(Command::Unlink {
                dedup: dedup_for_test(98),
                parent_inode_id,
                name: "file".to_string(),
            })
            .unwrap(),
            FsErrorCode::EInval,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert!(storage.get_inode(inode_id).unwrap().is_some());
        assert_eq!(
            storage.get_inode_by_data_handle(data_handle_id).unwrap(),
            Some(inode_id)
        );
        assert_eq!(storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
    }

    #[test]
    fn rmdir_empty_dir_deletes_namespace_and_replays_without_mutating_again() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(30);
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let inode_id = InodeId::new(31);
        let inode = Inode::new_dir(inode_id, FileAttrs::new(), parent.mount_id);
        storage.put_inode(&parent).unwrap();
        storage.put_inode(&inode).unwrap();
        storage.put_dentry(parent_inode_id, "dir", inode_id).unwrap();

        let dedup = dedup_for_test(82);
        let command = Command::Rmdir {
            dedup: dedup.clone(),
            parent_inode_id,
            name: "dir".to_string(),
        };

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());

        expect_fs_ok(sm.apply(command).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
    }

    #[test]
    fn rmdir_non_empty_dir_returns_directory_not_empty_and_preserves_namespace() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(40);
        let dir_inode_id = InodeId::new(41);
        let child_inode_id = InodeId::new(42);
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let dir_inode = Inode::new_dir(dir_inode_id, FileAttrs::new(), parent.mount_id);
        let child_inode = Inode::new_file(child_inode_id, FileAttrs::new(), parent.mount_id, DataHandleId::new(42));
        storage.put_inode(&parent).unwrap();
        storage.put_inode(&dir_inode).unwrap();
        storage.put_inode(&child_inode).unwrap();
        storage.put_dentry(parent_inode_id, "dir", dir_inode_id).unwrap();
        storage.put_dentry(dir_inode_id, "child", child_inode_id).unwrap();

        let dedup = dedup_for_test(83);
        let command = Command::Rmdir {
            dedup: dedup.clone(),
            parent_inode_id,
            name: "dir".to_string(),
        };

        expect_fs_errno(sm.apply(command.clone()).unwrap(), FsErrorCode::ENotEmpty);
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(dir_inode_id));
        assert!(storage.get_inode(dir_inode_id).unwrap().is_some());
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());

        expect_fs_errno(sm.apply(command).unwrap(), FsErrorCode::ENotEmpty);
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(dir_inode_id));
    }
}
