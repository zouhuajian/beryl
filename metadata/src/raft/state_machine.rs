// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft state machine implementation.
//!
//! Applies commands to the state machine and maintains consistency.

use crate::error::{to_canonical_fs, MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::raft::command::Command;
use crate::raft::storage::{AppliedResult, RenameAtomicUpdate, RocksDBStorage};
use crate::raft::types::{
    AppDataResponse, BlockCommandResult, CommandFingerprint, DedupKey, DeleteIntentsResult, FsCommandResult,
    FsErrnoResult, FsOkResult, LeaseCommandResult, MountCommandResult, ShardGroupInfo, WorkerCommandResult,
};
use crate::state::{BlockMetaState, LeaseState, RouteEpoch};
use crate::worker::{HealthStatus, WorkerDescriptor, WorkerInfo};
use parking_lot::RwLock;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;
use types::block::{BlockPlacement, BlockState};
use types::fs::{FileAttrs, FsErrorCode, Inode, InodeId, InodeKind};
use types::ids::{BlockId, ClientId, DataHandleId, MountId, ShardGroupId, ShardId, WorkerId};
use types::layout::FileLayout;
use types::lease::{FencingToken, Lease};

fn meta_err_to_fs_errno(err: &MetadataError) -> Option<FsErrorCode> {
    match to_canonical_fs(err.clone()).code {
        Some(common::error::canonical::ErrorCode::FsErrno(errno)) => Some(errno),
        _ => None,
    }
}

const RENAME_OVERWRITE_CLEANUP_UNIMPLEMENTED: &str = "rename overwrite target cleanup is not implemented yet";

/// Raft state machine.
pub struct AppRaftStateMachine {
    storage: Arc<RocksDBStorage>,
    mount_table: Arc<MountTable>,
    _next_mount_id: Arc<RwLock<u64>>,
    applied_seq: Arc<RwLock<u64>>,
}

impl AppRaftStateMachine {
    pub fn new(storage: Arc<RocksDBStorage>, mount_table: Arc<MountTable>) -> Self {
        Self {
            storage,
            mount_table,
            _next_mount_id: Arc::new(RwLock::new(1)),
            applied_seq: Arc::new(RwLock::new(0)),
        }
    }

    /// Apply a command to the state machine.
    pub fn apply(&self, command: Command, seq: u64) -> MetadataResult<AppDataResponse> {
        let dedup_key = command.dedup_key().clone();
        let fingerprint = command.fingerprint();

        // Dedup hit returns the persisted replay record without re-running the mutation.
        if let Some(applied) = self.storage.get_applied_result(&dedup_key)? {
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
            self.storage.put_applied_seq(seq)?;
            *self.applied_seq.write() = seq;
            return Ok(applied.result);
        }

        // Apply command
        let result = match command {
            Command::UpdateCommittedLength { .. } => {
                return Err(MetadataError::InvalidArgument(
                    "Legacy file-based commands are no longer supported. Use inode-based commands instead.".to_string(),
                ));
            }
            Command::AllocateBlock {
                inode_id,
                block_id,
                placement,
                ..
            } => AppDataResponse::Block(BlockCommandResult::Allocated(
                self.apply_allocate_block(inode_id, block_id, placement)?,
            )),
            Command::CommitBlock { block_id, token, .. } => {
                AppDataResponse::Block(self.apply_commit_block(block_id, token)?)
            }
            Command::UpdateBlockState { block_id, state, .. } => {
                AppDataResponse::Block(self.apply_update_block_state(block_id, state)?)
            }
            Command::AcquireLease {
                block_id,
                client_id,
                epoch,
                expires_at_ms,
                ..
            } => {
                let result =
                    self.apply_acquire_lease(block_id, client_id, epoch, expires_at_ms, &dedup_key, fingerprint, seq)?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Lease(result));
            }
            Command::ReleaseLease { block_id, .. } => {
                let result = self.apply_release_lease(block_id, &dedup_key, fingerprint, seq)?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Lease(result));
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
                    seq,
                )?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Mount(result));
            }
            Command::DeleteMount { mount_id, .. } => {
                let result = self.apply_delete_mount(mount_id, &dedup_key, fingerprint, seq)?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Mount(result));
            }
            Command::AddShardGroup {
                shard_group_id,
                shard_ids,
                initial_members,
                ..
            } => {
                let result = self.apply_add_shard_group(
                    shard_group_id,
                    shard_ids,
                    initial_members,
                    &dedup_key,
                    fingerprint,
                    seq,
                )?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::ShardGroup(result));
            }
            Command::UpsertWorkerDescriptor {
                worker_id,
                address,
                net_transport_kind,
                worker_epoch,
                fault_domain,
                ..
            } => {
                let result = self.apply_upsert_worker_descriptor(
                    worker_id,
                    address,
                    net_transport_kind,
                    worker_epoch,
                    fault_domain,
                    &dedup_key,
                    fingerprint,
                    seq,
                )?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Worker(result));
            }
            Command::CreateDeleteIntents { intents, .. } => {
                AppDataResponse::DeleteIntents(self.apply_create_delete_intents(intents)?)
            }
            Command::Mkdir {
                parent_inode_id,
                name,
                attrs,
                ..
            } => {
                // Create/mkdir/rename persist namespace mutation, dedup result, and applied_seq together.
                let result = self.apply_mkdir(parent_inode_id, name, attrs, &dedup_key, fingerprint, seq)?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Fs(result));
            }
            Command::Create {
                parent_inode_id,
                name,
                attrs,
                layout,
                ..
            } => {
                let result = self.apply_create(parent_inode_id, name, attrs, layout, &dedup_key, fingerprint, seq)?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Fs(result));
            }
            Command::Unlink {
                parent_inode_id, name, ..
            } => AppDataResponse::Fs(self.apply_unlink(parent_inode_id, name)),
            Command::Rmdir {
                parent_inode_id, name, ..
            } => AppDataResponse::Fs(self.apply_rmdir(parent_inode_id, name)),
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
                    seq,
                )?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Fs(result));
            }
            Command::SetAttr {
                inode_id, mask, attrs, ..
            } => {
                let result = self.apply_set_attr(inode_id, mask, attrs, &dedup_key, fingerprint, seq)?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Fs(result));
            }
            Command::CloseWrite {
                inode_id,
                extents,
                final_size,
                lease_id,
                open_epoch,
                lease_epoch,
                ..
            } => AppDataResponse::Fs(self.apply_close_write(
                inode_id,
                extents,
                final_size,
                lease_id,
                open_epoch,
                lease_epoch,
            )),
            Command::Truncate {
                inode_id,
                new_size,
                lease_id,
                lease_epoch,
                ..
            } => AppDataResponse::Fs(self.apply_truncate(inode_id, new_size, lease_id, lease_epoch)),
            Command::SetXattr {
                inode_id,
                name,
                value,
                create,
                replace,
                ..
            } => {
                let result =
                    self.apply_set_xattr(inode_id, name, value, create, replace, &dedup_key, fingerprint, seq)?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Fs(result));
            }
            Command::RemoveXattr { inode_id, name, .. } => {
                let result = self.apply_remove_xattr(inode_id, name, &dedup_key, fingerprint, seq)?;
                *self.applied_seq.write() = seq;
                return Ok(AppDataResponse::Fs(result));
            }
        };

        // Store applied result for idempotency
        let applied_result = Self::make_applied_result(seq, fingerprint, result.clone());
        self.storage.put_applied_result(&dedup_key, applied_result)?;

        // Update applied sequence (persist + in-memory).
        // TODO: Remaining complex/legacy commands still need apply-level atomicity follow-up.
        self.storage.put_applied_seq(seq)?;
        *self.applied_seq.write() = seq;

        Ok(result)
    }

    fn make_applied_result(seq: u64, fingerprint: CommandFingerprint, result: AppDataResponse) -> AppliedResult {
        AppliedResult {
            seq,
            fingerprint,
            result,
            created_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            size_bytes: 0,
        }
    }

    fn apply_allocate_block(
        &self,
        inode_id: InodeId,
        block_id: BlockId,
        placement: BlockPlacement,
    ) -> MetadataResult<BlockMetaState> {
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

        self.storage.put_block(&block_meta)?;

        Ok(block_meta)
    }

    fn apply_commit_block(&self, block_id: BlockId, token: FencingToken) -> MetadataResult<BlockCommandResult> {
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
        self.storage.put_block(&block_meta)?;

        Ok(BlockCommandResult::Committed)
    }

    fn apply_update_block_state(&self, block_id: BlockId, state: BlockState) -> MetadataResult<BlockCommandResult> {
        let mut block_meta = self
            .storage
            .get_block(block_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Block not found: {:?}", block_id)))?;

        block_meta.state = state;
        self.storage.put_block(&block_meta)?;

        Ok(BlockCommandResult::StateUpdated)
    }

    fn apply_acquire_lease(
        &self,
        block_id: BlockId,
        client_id: ClientId,
        epoch: u64,
        expires_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        seq: u64,
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
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Lease(result.clone()));
        if let LeaseCommandResult::Acquired(lease_state) = &result {
            self.storage
                .acquire_lease_with_apply_result_atomic(lease_state, dedup_key, applied_result, seq)?;
        }

        Ok(result)
    }

    fn apply_release_lease(
        &self,
        block_id: BlockId,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        seq: u64,
    ) -> MetadataResult<LeaseCommandResult> {
        let result = LeaseCommandResult::Released;
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Lease(result.clone()));
        self.storage
            .release_lease_with_apply_result_atomic(block_id, dedup_key, applied_result, seq)?;
        Ok(result)
    }

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
        seq: u64,
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
                let applied_result =
                    Self::make_applied_result(seq, fingerprint, AppDataResponse::Mount(result.clone()));
                self.storage
                    .put_apply_result_and_seq_atomic(dedup_key, applied_result, seq)?;
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
                let applied_result =
                    Self::make_applied_result(seq, fingerprint, AppDataResponse::Mount(result.clone()));
                self.storage
                    .put_apply_result_and_seq_atomic(dedup_key, applied_result, seq)?;
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
                let now_ms = self.now_ms();
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
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Mount(result.clone()));
        self.storage.create_mount_with_apply_result_atomic(
            &entry,
            root_inode_to_create.as_ref(),
            new_version,
            new_route_epoch,
            dedup_key,
            applied_result,
            seq,
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
        seq: u64,
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
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Mount(result.clone()));
        self.storage.delete_mount_with_apply_result_atomic(
            mount_id,
            mount_version + 1,
            new_route_epoch,
            dedup_key,
            applied_result,
            seq,
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
        seq: u64,
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
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::ShardGroup(info.clone()));
        self.storage
            .add_shard_group_with_apply_result_atomic(&info, &shard_ids, dedup_key, applied_result, seq)?;

        Ok(info)
    }

    fn apply_upsert_worker_descriptor(
        &self,
        worker_id: WorkerId,
        address: String,
        net_transport_kind: i32,
        worker_epoch: u64,
        fault_domain: Option<String>,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        seq: u64,
    ) -> MetadataResult<WorkerCommandResult> {
        // Create worker descriptor (only authoritative fields)
        let descriptor = WorkerDescriptor {
            worker_id,
            address,
            net_transport_kind,
            worker_epoch,
            fault_domain,
        };

        // Store worker descriptor in RocksDB (only descriptor, no runtime)
        // For backward compatibility, we still use WorkerInfo structure but only with descriptor fields
        let worker_info = WorkerInfo {
            worker_id: descriptor.worker_id,
            address: descriptor.address.clone(),
            net_transport_kind: descriptor.net_transport_kind,
            worker_epoch: descriptor.worker_epoch,
            capacity_total: 0, // Runtime fields set to defaults
            capacity_used: 0,
            capacity_available: 0,
            active_reads: 0,
            active_writes: 0,
            health: HealthStatus::Healthy,
            last_heartbeat: 0,
            fault_domain: descriptor.fault_domain.clone(),
        };

        let result = WorkerCommandResult::Upserted(worker_id);
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Worker(result.clone()));
        self.storage
            .upsert_worker_descriptor_with_apply_result_atomic(&worker_info, dedup_key, applied_result, seq)?;

        Ok(result)
    }

    fn apply_create_delete_intents(
        &self,
        intents: Vec<crate::state::DeleteIntent>,
    ) -> MetadataResult<DeleteIntentsResult> {
        let intent_count = intents.len();
        // Persist all intents to RocksDB
        // Ensure status is Pending for newly created intents
        for mut intent in intents {
            // Newly created intents should always be Pending
            intent.status = crate::state::DeleteIntentStatus::Pending;
            intent.finished_at_ms = None;
            intent.last_error_msg = None;
            self.storage.put_delete_intent(&intent)?;
        }

        // Return count of created intents
        Ok(DeleteIntentsResult {
            created: intent_count as u64,
        })
    }

    /// Get applied sequence number.
    pub fn applied_seq(&self) -> u64 {
        *self.applied_seq.read()
    }

    /// Restore applied sequence (used after snapshot install/restart).
    pub fn set_applied_seq(&self, seq: u64) {
        *self.applied_seq.write() = seq;
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

    /// Helper: Get current time in milliseconds.
    fn now_ms(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
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
        seq: u64,
    ) -> MetadataResult<FsCommandResult> {
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage
            .put_apply_result_and_seq_atomic(dedup_key, applied_result, seq)?;
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
        seq: u64,
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
            let now_ms = self.now_ms();

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
            })
            .map(|ok| (inode, updated_parent, ok))
        })();

        let (inode, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint, seq)
            }
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.create_dir_with_apply_result_atomic(
            parent_inode_id,
            &name,
            &inode,
            &updated_parent,
            dedup_key,
            applied_result,
            seq,
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
        seq: u64,
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
            let now_ms = self.now_ms();

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
            })
            .map(|ok| (inode, updated_parent, ok))
        })();

        let (inode, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint, seq)
            }
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.create_file_with_apply_result_atomic(
            parent_inode_id,
            &name,
            &inode,
            &updated_parent,
            layout,
            dedup_key,
            applied_result,
            seq,
        )?;
        Ok(result)
    }

    /// Apply Unlink command.
    fn apply_unlink(&self, parent_inode_id: InodeId, name: String) -> FsCommandResult {
        let result: MetadataResult<FsOkResult> = (|| {
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

            let now_ms = self.now_ms();

            // Delete dentry
            self.storage.delete_dentry(parent_inode_id, &name)?;

            // Update parent directory mtime/ctime
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(now_ms);
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;
            self.storage.put_inode(&updated_parent)?;

            // Collect block_ids from child inode extents before deletion
            let removed_block_ids = match &child_inode.data {
                types::fs::InodeData::File { extents, .. } => {
                    let mut block_ids = std::collections::HashSet::new();
                    for extent in extents {
                        block_ids.insert(extent.block_id);
                    }
                    block_ids
                }
                _ => std::collections::HashSet::new(),
            };

            // TODO: Mark child inode as tombstone (simple deletion)
            // TODO: In production, would mark tombstone and let GC handle block deletion
            self.storage.delete_inode(child_inode_id)?;

            // Update block reference counts (decrement for removed blocks)
            let now_ms = self.now_ms();
            let mut gc_intents = Vec::new();
            for block_id in removed_block_ids {
                let (_new_count, reached_zero) = self.storage.decrement_block_ref_count(block_id)?;
                if reached_zero {
                    // Generate GC intent (will be written to CF_GC_INTENTS)
                    let intent_id = self.storage.generate_intent_id()?;
                    let intent = crate::state::DeleteIntent {
                        intent_id,
                        block_id,
                        reason: crate::state::DeleteIntentReason::Gc,
                        created_at_ms: now_ms,
                        not_before_ms: now_ms, // No grace period for unlink
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
                    };
                    gc_intents.push(intent);
                }
            }

            // Store GC intents (if any)
            for intent in &gc_intents {
                self.storage.put_delete_intent(intent)?;
            }

            Ok(FsOkResult::default())
        })();

        Self::fs_command_result(result)
    }

    /// Apply Rmdir command.
    fn apply_rmdir(&self, parent_inode_id: InodeId, name: String) -> FsCommandResult {
        let result: MetadataResult<FsOkResult> = (|| {
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

            let now_ms = self.now_ms();

            // Delete dentry
            self.storage.delete_dentry(parent_inode_id, &name)?;

            // Update parent directory mtime/ctime
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(now_ms);
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;
            self.storage.put_inode(&updated_parent)?;

            // Delete child inode
            self.storage.delete_inode(child_inode_id)?;

            Ok(FsOkResult::default())
        })();

        Self::fs_command_result(result)
    }

    /// Apply Rename command (atomic within mount).
    fn apply_rename(
        &self,
        src_parent_inode_id: InodeId,
        src_name: String,
        dst_parent_inode_id: InodeId,
        dst_name: String,
        flags: u32,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        seq: u64,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(
            InodeId,
            Option<InodeId>,
            Option<Inode>,
            Option<Inode>,
            Inode,
            FsOkResult,
        )> = (|| {
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

            let mut overwritten_inode_id = None;

            // Check if destination exists
            if let Some(dst_inode_id) = self.storage.get_dentry(dst_parent_inode_id, &dst_name)? {
                // NOREPLACE flag set -> fail when destination exists
                if flags & 0x1 != 0 {
                    return Err(MetadataError::AlreadyExists(format!(
                        "Destination exists and RENAME_NOREPLACE set: {}",
                        dst_name
                    )));
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
                self.validate_rename_overwrite_target_is_safe(&dst_inode)?;
                overwritten_inode_id = Some(dst_inode_id);
            }

            let now_ms = self.now_ms();

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

            Ok((
                src_inode_id,
                overwritten_inode_id,
                updated_src_parent,
                updated_dst_parent,
                updated_src_inode,
                FsOkResult::default(),
            ))
        })();

        let (src_inode_id, overwritten_inode_id, updated_src_parent, updated_dst_parent, updated_src_inode, ok) =
            match prepared {
                Ok(prepared) => prepared,
                Err(err) => {
                    return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint, seq)
                }
            };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.rename_with_apply_result_atomic(
            RenameAtomicUpdate {
                src_parent_inode_id,
                src_name: &src_name,
                dst_parent_inode_id,
                dst_name: &dst_name,
                src_inode_id,
                overwritten_inode_id,
                updated_src_parent: updated_src_parent.as_ref(),
                updated_dst_parent: updated_dst_parent.as_ref(),
                updated_src_inode: &updated_src_inode,
            },
            dedup_key,
            applied_result,
            seq,
        )?;

        Ok(result)
    }

    fn validate_rename_overwrite_target_is_safe(&self, dst_inode: &Inode) -> MetadataResult<()> {
        // TODO: Short-term behavior rejects complex overwrite targets to avoid orphaning target
        // layout/data_handle/block metadata. Future overwrite cleanup must batch target inode/dentry
        // removal with target layout, data_handle_owner, block refcount, and delete-intent updates.
        if !dst_inode.kind.is_dir() {
            return Err(MetadataError::InvalidArgument(format!(
                "{} for inode {}",
                RENAME_OVERWRITE_CLEANUP_UNIMPLEMENTED, dst_inode.inode_id
            )));
        }
        if dst_inode.current_data_handle_id.as_raw() != 0 {
            return Err(MetadataError::InvalidArgument(format!(
                "{} for inode {} with data_handle_id {}",
                RENAME_OVERWRITE_CLEANUP_UNIMPLEMENTED, dst_inode.inode_id, dst_inode.current_data_handle_id
            )));
        }
        match self.storage.get_layout(dst_inode.inode_id) {
            Ok(_) => Err(MetadataError::InvalidArgument(format!(
                "{} for inode {} with layout",
                RENAME_OVERWRITE_CLEANUP_UNIMPLEMENTED, dst_inode.inode_id
            ))),
            Err(MetadataError::NotFound(_)) => Ok(()),
            Err(err) => Err(err),
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
        seq: u64,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<Inode> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            let now_ms = self.now_ms();

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

            Ok(inode)
        })();

        let inode = match prepared {
            Ok(inode) => inode,
            Err(err) => {
                return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint, seq)
            }
        };
        let result = FsCommandResult::Ok(FsOkResult::default());
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage
            .put_inode_with_apply_result_atomic(&inode, dedup_key, applied_result, seq)?;
        Ok(result)
    }

    /// Apply CloseWrite command.
    fn apply_close_write(
        &self,
        inode_id: InodeId,
        extents: Vec<types::fs::Extent>,
        final_size: u64,
        _lease_id: types::ids::LeaseId,
        _open_epoch: u64,
        lease_epoch: u64,
    ) -> FsCommandResult {
        let result: MetadataResult<FsOkResult> = (|| {
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

            let expected_data_handle_id = inode.current_data_handle_id;
            if expected_data_handle_id.as_raw() == 0 {
                return Err(MetadataError::Internal(format!(
                    "File inode {} is missing current_data_handle_id",
                    inode_id
                )));
            }

            for extent in &extents {
                if extent.block_id.data_handle_id != expected_data_handle_id {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent block data_handle_id {} does not match inode {} current_data_handle_id {}",
                        extent.block_id.data_handle_id, inode_id, expected_data_handle_id
                    )));
                }
            }

            let now_ms = self.now_ms();

            // Update inode: append extents and update size/mtime/ctime/lease_epoch
            match &mut inode.data {
                types::fs::InodeData::File {
                    extents: existing_extents,
                    lease_epoch: stored_lease_epoch,
                    ..
                } => {
                    // Append new extents
                    existing_extents.extend(extents.clone());
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

            // Update block reference counts (increment for new extents)
            // Note: Idempotency is handled at the apply() level via request_id check.
            // If this command was already applied, we skip refcount updates (extents already appended).
            // Collect unique block_ids from new extents (per inode, count once per block_id)
            let mut unique_block_ids = std::collections::HashSet::new();
            for extent in &extents {
                unique_block_ids.insert(extent.block_id);
            }

            // Increment refcount for each unique block_id (in same WriteBatch via storage)
            // This is safe because apply() already checks idempotency via request_id
            for block_id in unique_block_ids {
                self.storage.increment_block_ref_count(block_id)?;
            }

            // Store updated inode
            self.storage.put_inode(&inode)?;

            Ok(FsOkResult::default())
        })();

        Self::fs_command_result(result)
    }

    /// Apply Truncate command.
    fn apply_truncate(
        &self,
        inode_id: InodeId,
        new_size: u64,
        _lease_id: types::ids::LeaseId,
        lease_epoch: u64,
    ) -> FsCommandResult {
        let result: MetadataResult<FsOkResult> = (|| {
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

            let current_size = inode.attrs.size;
            if new_size > current_size {
                return Err(MetadataError::InvalidArgument(format!(
                    "Truncate grow not supported: current_size={}, new_size={}",
                    current_size, new_size
                )));
            }

            if new_size == current_size {
                // No-op
                return Ok(FsOkResult::default());
            }

            let now_ms = self.now_ms();

            // Truncate extents: keep extents that are fully before new_size, truncate or remove others
            let removed_block_ids = match &mut inode.data {
                types::fs::InodeData::File {
                    extents,
                    lease_epoch: stored_lease_epoch,
                    ..
                } => {
                    let old_extents = extents.clone();
                    let mut new_extents = Vec::new();
                    let mut removed_block_ids = std::collections::HashSet::new();

                    for extent in old_extents.iter() {
                        let extent_end = extent.file_offset + extent.len;
                        if extent_end <= new_size {
                            // Extent is fully before new_size, keep it
                            new_extents.push(extent.clone());
                        } else if extent.file_offset < new_size {
                            // Extent crosses new_size boundary, truncate it
                            let truncated_len = new_size - extent.file_offset;
                            if truncated_len > 0 {
                                let mut truncated_extent = extent.clone();
                                truncated_extent.len = truncated_len;
                                new_extents.push(truncated_extent);
                            }
                            // If truncated to 0, mark block as removed
                            if truncated_len == 0 {
                                removed_block_ids.insert(extent.block_id);
                            }
                        } else {
                            // Extent is fully after new_size, remove it
                            removed_block_ids.insert(extent.block_id);
                        }
                    }

                    // Also check if any kept extents have reduced length (partial truncation)
                    // For simplicity, we only mark fully removed extents as removed
                    // Partial truncation doesn't change block_id reference

                    *extents = new_extents;
                    *stored_lease_epoch = Some(lease_epoch);
                    removed_block_ids
                }
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode data is not File: {}",
                        inode_id
                    )));
                }
            };

            // Update block reference counts (decrement for removed blocks)
            // Collect unique block_ids from removed extents (per inode, count once per block_id)
            let mut gc_intents = Vec::new();
            for block_id in removed_block_ids {
                let (_new_count, reached_zero) = self.storage.decrement_block_ref_count(block_id)?;
                if reached_zero {
                    // Generate GC intent (will be written to CF_GC_INTENTS)
                    let intent_id = self.storage.generate_intent_id()?;
                    let intent = crate::state::DeleteIntent {
                        intent_id,
                        block_id,
                        reason: crate::state::DeleteIntentReason::Gc,
                        created_at_ms: now_ms,
                        not_before_ms: now_ms, // No grace period for truncate
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
                    };
                    gc_intents.push(intent);
                }
            }

            // Update file size and timestamps
            inode.attrs.size = new_size;
            inode.attrs.update_mtime_ctime(now_ms);

            // Store updated inode
            self.storage.put_inode(&inode)?;

            // Store GC intents (if any)
            for intent in &gc_intents {
                self.storage.put_delete_intent(intent)?;
            }

            Ok(FsOkResult::default())
        })();

        Self::fs_command_result(result)
    }

    /// Apply set xattr command.
    fn apply_set_xattr(
        &self,
        inode_id: InodeId,
        name: String,
        value: Vec<u8>,
        create: bool,
        replace: bool,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        seq: u64,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<Inode> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            let exists = inode.xattrs.contains_key(&name);
            if create && exists {
                return Err(MetadataError::AlreadyExists(format!("xattr already exists: {}", name)));
            }
            if replace && !exists {
                return Err(MetadataError::NotFound(format!("xattr not found: {}", name)));
            }

            inode.xattrs.insert(name, value);
            let now_ms = self.now_ms();
            inode.attrs.update_ctime(now_ms);
            Ok(inode)
        })();

        let inode = match prepared {
            Ok(inode) => inode,
            Err(err) => {
                return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint, seq)
            }
        };
        let result = FsCommandResult::Ok(FsOkResult::default());
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage
            .put_inode_with_apply_result_atomic(&inode, dedup_key, applied_result, seq)?;
        Ok(result)
    }

    /// Apply remove xattr command.
    fn apply_remove_xattr(
        &self,
        inode_id: InodeId,
        name: String,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        seq: u64,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<Inode> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            if inode.xattrs.remove(&name).is_none() {
                return Err(MetadataError::NotFound(format!("xattr not found: {}", name)));
            }
            let now_ms = self.now_ms();
            inode.attrs.update_ctime(now_ms);
            Ok(inode)
        })();

        let inode = match prepared {
            Ok(inode) => inode,
            Err(err) => {
                return self.persist_fs_apply_result(Self::fs_command_result(Err(err)), dedup_key, fingerprint, seq)
            }
        };
        let result = FsCommandResult::Ok(FsOkResult::default());
        let applied_result = Self::make_applied_result(seq, fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage
            .put_inode_with_apply_result_atomic(&inode, dedup_key, applied_result, seq)?;
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
        let res = sm.apply(cmd, 1);
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
        let res = sm.apply(cmd, 2);
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
        sm.apply(cmd, 3).unwrap();

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

        let raw = sm.apply(cmd, 1).unwrap();
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
    fn create_reapply_returns_original_success_result_and_applied_seq() {
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

        let first = expect_fs_ok(sm.apply(cmd.clone(), 7).unwrap());
        assert_eq!(storage.get_applied_seq().unwrap(), Some(7));
        assert_eq!(sm.applied_seq(), 7);

        let second = expect_fs_ok(sm.apply(cmd, 8).unwrap());
        assert_eq!(second, first);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(8));
        assert_eq!(sm.applied_seq(), 8);

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
            .apply(
                Command::Mkdir {
                    dedup: dedup_for_test(29),
                    parent_inode_id,
                    name: "dir".to_string(),
                    attrs: FileAttrs::new(),
                },
                1,
            )
            .unwrap();
        let inode_id = match raw {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.expect("inode id should be returned"),
            other => panic!("unexpected apply response: {:?}", other),
        };

        assert!(storage.get_inode(inode_id).unwrap().unwrap().kind.is_dir());
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(inode_id));
    }

    #[test]
    fn mkdir_reapply_returns_original_success_result_and_applied_seq() {
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

        let first = expect_fs_ok(sm.apply(cmd.clone(), 9).unwrap());
        assert_eq!(storage.get_applied_seq().unwrap(), Some(9));
        assert_eq!(sm.applied_seq(), 9);

        let second = expect_fs_ok(sm.apply(cmd, 10).unwrap());
        assert_eq!(second, first);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(10));
        assert_eq!(sm.applied_seq(), 10);

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
            .apply(
                Command::Create {
                    dedup: dedup_for_test(36),
                    parent_inode_id,
                    name: "old".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
                1,
            )
            .unwrap();
        let inode_id = match created {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected apply response: {:?}", other),
        };

        sm.apply(
            Command::Rename {
                dedup: dedup_for_test(37),
                src_parent_inode_id: parent_inode_id,
                src_name: "old".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "new".to_string(),
                flags: 0,
            },
            2,
        )
        .unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "old").unwrap(), None);
        assert_eq!(storage.get_dentry(parent_inode_id, "new").unwrap(), Some(inode_id));
        assert!(storage.get_inode(inode_id).unwrap().is_some());
    }

    #[test]
    fn rename_reapply_returns_original_success_result_and_applied_seq() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let created = expect_fs_ok(
            sm.apply(
                Command::Create {
                    dedup: dedup_for_test(43),
                    parent_inode_id,
                    name: "old".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
                1,
            )
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

        let first = expect_fs_ok(sm.apply(cmd.clone(), 2).unwrap());
        assert_eq!(first, FsOkResult::default());
        assert_eq!(storage.get_applied_seq().unwrap(), Some(2));
        assert_eq!(sm.applied_seq(), 2);

        let second = expect_fs_ok(sm.apply(cmd, 3).unwrap());
        assert_eq!(second, first);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(3));
        assert_eq!(sm.applied_seq(), 3);
        assert_eq!(storage.get_dentry(parent_inode_id, "old").unwrap(), None);
        assert_eq!(storage.get_dentry(parent_inode_id, "new").unwrap(), Some(inode_id));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn attrs_and_xattrs_reapply_return_original_result_and_applied_seq() {
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
        let first = expect_fs_ok(sm.apply(set_attr.clone(), 11).unwrap());
        let ctime_after_first = storage.get_inode(inode_id).unwrap().unwrap().attrs.ctime_ms;
        let second = expect_fs_ok(sm.apply(set_attr, 12).unwrap());
        assert_eq!(second, first);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(12));
        assert_eq!(sm.applied_seq(), 12);
        let stored = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(stored.attrs.uid, 123);
        assert_eq!(stored.attrs.ctime_ms, ctime_after_first);

        let set_xattr = Command::SetXattr {
            dedup: dedup_for_test(71),
            inode_id,
            name: "user.key".to_string(),
            value: b"value".to_vec(),
            create: true,
            replace: false,
        };
        let first = expect_fs_ok(sm.apply(set_xattr.clone(), 13).unwrap());
        let second = expect_fs_ok(sm.apply(set_xattr, 14).unwrap());
        assert_eq!(second, first);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(14));
        assert_eq!(
            storage.get_inode(inode_id).unwrap().unwrap().xattrs.get("user.key"),
            Some(&b"value".to_vec())
        );

        let remove_xattr = Command::RemoveXattr {
            dedup: dedup_for_test(72),
            inode_id,
            name: "user.key".to_string(),
        };
        let first = expect_fs_ok(sm.apply(remove_xattr.clone(), 15).unwrap());
        let second = expect_fs_ok(sm.apply(remove_xattr, 16).unwrap());
        assert_eq!(second, first);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(16));
        assert!(!storage
            .get_inode(inode_id)
            .unwrap()
            .unwrap()
            .xattrs
            .contains_key("user.key"));
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
        let first = expect_mount_upserted(sm.apply(create_mount.clone(), 17).unwrap());
        let second = expect_mount_upserted(sm.apply(create_mount, 18).unwrap());
        assert_eq!(second.mount_id, first.mount_id);
        assert_eq!(second.config_version, first.config_version);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(18));
        assert_eq!(sm.applied_seq(), 18);
        assert_eq!(
            mount_table.get_mount(mount_id).unwrap().unwrap().mount_prefix,
            first.mount_prefix
        );
        assert_eq!(mount_table.list_mounts().len(), 1);

        let delete_mount = Command::DeleteMount {
            dedup: dedup_for_test(74),
            mount_id,
        };
        expect_mount_deleted(sm.apply(delete_mount.clone(), 19).unwrap());
        expect_mount_deleted(sm.apply(delete_mount, 20).unwrap());
        assert_eq!(storage.get_applied_seq().unwrap(), Some(20));
        assert_eq!(sm.applied_seq(), 20);
        assert!(storage.get_mount(mount_id).unwrap().is_none());
        assert!(mount_table.get_mount(mount_id).unwrap().is_none());
    }

    #[test]
    fn shard_group_reapply_returns_original_result_and_applied_seq() {
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

        let first = expect_shard_group(sm.apply(cmd.clone(), 21).unwrap());
        let second = expect_shard_group(sm.apply(cmd, 22).unwrap());
        assert_eq!(second, first);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(22));
        assert_eq!(sm.applied_seq(), 22);
        assert_eq!(
            storage.get_shard_routing(ShardId::new(750)).unwrap(),
            Some(ShardGroupId::new(75))
        );
    }

    #[test]
    fn worker_descriptor_reapply_returns_original_result_and_applied_seq() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let worker_id = WorkerId::new(76);
        let cmd = Command::UpsertWorkerDescriptor {
            dedup: dedup_for_test(76),
            worker_id,
            address: "127.0.0.1:17076".to_string(),
            net_transport_kind: 1,
            worker_epoch: 3,
            fault_domain: Some("rack-a".to_string()),
        };

        assert_eq!(expect_worker_upserted(sm.apply(cmd.clone(), 23).unwrap()), worker_id);
        assert_eq!(expect_worker_upserted(sm.apply(cmd, 24).unwrap()), worker_id);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(24));
        assert_eq!(sm.applied_seq(), 24);
        let stored = storage.get_worker(worker_id).unwrap().unwrap();
        assert_eq!(stored.address, "127.0.0.1:17076");
        assert_eq!(stored.worker_epoch, 3);
    }

    #[test]
    fn lease_commands_reapply_return_original_result_and_applied_seq() {
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
        let first = expect_lease_acquired(sm.apply(acquire.clone(), 25).unwrap());
        let second = expect_lease_acquired(sm.apply(acquire, 26).unwrap());
        assert_eq!(second.lease.owner, first.lease.owner);
        assert_eq!(second.lease.epoch, first.lease.epoch);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(26));
        assert_eq!(sm.applied_seq(), 26);

        let release = Command::ReleaseLease {
            dedup: dedup_for_test(78),
            block_id,
        };
        expect_lease_released(sm.apply(release.clone(), 27).unwrap());
        expect_lease_released(sm.apply(release, 28).unwrap());
        assert_eq!(storage.get_applied_seq().unwrap(), Some(28));
        assert_eq!(sm.applied_seq(), 28);
        assert!(storage.get_lease(block_id).unwrap().is_none());
    }

    #[test]
    fn rename_overwrite_file_with_data_state_is_rejected_without_cleanup() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let source = sm
            .apply(
                Command::Create {
                    dedup: dedup_for_test(38),
                    parent_inode_id,
                    name: "source".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
                1,
            )
            .unwrap();
        let source_inode_id = match source {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected apply response: {:?}", other),
        };

        let target = sm
            .apply(
                Command::Create {
                    dedup: dedup_for_test(39),
                    parent_inode_id,
                    name: "target".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(8192, 8192, 1),
                },
                2,
            )
            .unwrap();
        let target_inode_id = match target {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected apply response: {:?}", other),
        };
        let target_inode = storage.get_inode(target_inode_id).unwrap().unwrap();
        let target_handle = target_inode.current_data_handle_id;

        let rejected = sm
            .apply(
                Command::Rename {
                    dedup: dedup_for_test(40),
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "target".to_string(),
                    flags: 0,
                },
                3,
            )
            .unwrap();

        match rejected {
            AppDataResponse::Fs(FsCommandResult::Err(err)) => {
                assert_eq!(err.errno, FsErrorCode::EInval);
                assert!(err
                    .message
                    .contains("rename overwrite target cleanup is not implemented yet"));
            }
            other => panic!("unexpected apply response: {:?}", other),
        }

        assert_eq!(
            storage.get_dentry(parent_inode_id, "source").unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(target_inode_id)
        );
        assert!(storage.get_inode(target_inode_id).unwrap().is_some());
        assert_eq!(
            storage.get_layout(target_inode_id).unwrap(),
            FileLayout::new(8192, 8192, 1)
        );
        assert_eq!(
            storage.get_inode_by_data_handle(target_handle).unwrap(),
            Some(target_inode_id)
        );
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
            .apply(
                Command::Create {
                    dedup: dedup_for_test(30),
                    parent_inode_id,
                    name: "first".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
                1,
            )
            .unwrap();
        let second = sm
            .apply(
                Command::Create {
                    dedup: dedup_for_test(31),
                    parent_inode_id,
                    name: "second".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
                2,
            )
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
                .apply(
                    Command::Create {
                        dedup: dedup_for_test(32),
                        parent_inode_id,
                        name: "before-reopen".to_string(),
                        attrs: FileAttrs::new(),
                        layout: FileLayout::new(4096, 4096, 1),
                    },
                    1,
                )
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
            .apply(
                Command::Create {
                    dedup: dedup_for_test(33),
                    parent_inode_id,
                    name: "after-reopen".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
                2,
            )
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
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        sm.apply(
            Command::CloseWrite {
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
            },
            1,
        )
        .unwrap();
        let updated = storage.get_inode(inode_id).unwrap().unwrap();
        match updated.data {
            types::fs::InodeData::File { extents, .. } => {
                assert_eq!(extents[0].block_id.data_handle_id, data_handle_id)
            }
            other => panic!("unexpected inode data: {:?}", other),
        }

        let mismatch = sm.apply(
            Command::CloseWrite {
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
            },
            2,
        );
        assert!(matches!(
            mismatch,
            Ok(AppDataResponse::Fs(FsCommandResult::Err(FsErrnoResult {
                errno: FsErrorCode::EInval,
                ..
            })))
        ));
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
        sm.apply(
            Command::AllocateBlock {
                dedup: crate::raft::types::DedupKey::new(ClientId::new(11), CallId::new()),
                inode_id,
                block_id,
                placement: placement.clone(),
            },
            1,
        )
        .unwrap();

        // Mismatch should fail
        let err = sm
            .apply(
                Command::AllocateBlock {
                    dedup: crate::raft::types::DedupKey::new(ClientId::new(12), CallId::new()),
                    inode_id: InodeId::new(999),
                    block_id,
                    placement,
                },
                2,
            )
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
            .apply(
                Command::AcquireLease {
                    dedup: dedup_for_test(20),
                    block_id,
                    client_id: ClientId::new(9),
                    epoch: 1,
                    expires_at_ms: 9999,
                },
                1,
            )
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
            .apply(
                Command::AcquireLease {
                    dedup: dedup_for_test(21),
                    block_id,
                    client_id: ClientId::new(9),
                    epoch: 2,
                    expires_at_ms: 19999,
                },
                2,
            )
            .unwrap_err();
        assert!(matches!(err, MetadataError::InvalidArgument(_)));
    }

    #[test]
    fn dedup_fingerprint_mismatch_does_not_apply_mutation_or_advance_seq() {
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

        sm.apply(first, 7).unwrap();
        let err = sm.apply(mismatch, 8).unwrap_err();

        assert!(matches!(err, MetadataError::InvalidArgument(_)));
        assert_eq!(storage.get_dentry(parent_inode_id, "second").unwrap(), None);
        assert_eq!(storage.get_applied_seq().unwrap(), Some(7));
        assert_eq!(sm.applied_seq(), 7);
    }
}
