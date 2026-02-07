// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft state machine implementation.
//!
//! Applies commands to the state machine and maintains consistency.

use crate::error::{to_canonical_fs, MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::raft::command::Command;
use crate::raft::storage::{AppliedResult, RocksDBStorage};
use crate::raft::types::{
    AppDataResponse, BlockCommandResult, DeleteIntentsResult, FsCommandResult, FsErrnoResult, FsOkResult,
    LeaseCommandResult, MountCommandResult, ShardGroupInfo, WorkerCommandResult,
};
use crate::state::{BlockMetaState, LayoutVersion, LeaseState};
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

/// Raft state machine.
pub struct AppRaftStateMachine {
    storage: Arc<RocksDBStorage>,
    mount_table: Arc<MountTable>,
    next_mount_id: Arc<RwLock<u64>>,
    next_inode_id: Arc<RwLock<u64>>,
    applied_seq: Arc<RwLock<u64>>,
}

impl AppRaftStateMachine {
    pub fn new(storage: Arc<RocksDBStorage>, mount_table: Arc<MountTable>) -> Self {
        Self {
            storage,
            mount_table,
            next_mount_id: Arc::new(RwLock::new(1)),
            next_inode_id: Arc::new(RwLock::new(1)),
            applied_seq: Arc::new(RwLock::new(0)),
        }
    }

    /// Apply a command to the state machine.
    pub fn apply(&self, command: Command, seq: u64) -> MetadataResult<AppDataResponse> {
        let dedup_key = command.dedup_key().clone();
        let fingerprint = command.fingerprint();

        // Check idempotency
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
            } => AppDataResponse::Lease(self.apply_acquire_lease(block_id, client_id, epoch, expires_at_ms)?),
            Command::ReleaseLease { block_id, .. } => AppDataResponse::Lease(self.apply_release_lease(block_id)?),
            Command::CreateMount {
                mount_id,
                mount_prefix,
                mount_kind,
                ufs_uri,
                data_io_policy,
                namespace_owner_group_id,
                root_inode_id,
                ..
            } => AppDataResponse::Mount(self.apply_create_mount(
                mount_id,
                mount_prefix,
                mount_kind,
                ufs_uri,
                data_io_policy,
                namespace_owner_group_id,
                root_inode_id,
            )?),
            Command::DeleteMount { mount_id, .. } => AppDataResponse::Mount(self.apply_delete_mount(mount_id)?),
            Command::IncrementLayoutVersion { .. } => {
                AppDataResponse::LayoutVersion(self.apply_increment_layout_version()?)
            }
            Command::AddShardGroup {
                shard_group_id,
                shard_ids,
                initial_members,
                ..
            } => AppDataResponse::ShardGroup(self.apply_add_shard_group(shard_group_id, shard_ids, initial_members)?),
            Command::UpsertWorkerDescriptor {
                worker_id,
                address,
                net_transport_kind,
                worker_epoch,
                fault_domain,
                ..
            } => AppDataResponse::Worker(self.apply_upsert_worker_descriptor(
                worker_id,
                address,
                net_transport_kind,
                worker_epoch,
                fault_domain,
            )?),
            Command::CreateDeleteIntents { intents, .. } => {
                AppDataResponse::DeleteIntents(self.apply_create_delete_intents(intents)?)
            }
            Command::Mkdir {
                parent_inode_id,
                name,
                attrs,
                ..
            } => AppDataResponse::Fs(self.apply_mkdir(parent_inode_id, name, attrs)),
            Command::Create {
                parent_inode_id,
                name,
                attrs,
                layout,
                ..
            } => AppDataResponse::Fs(self.apply_create(parent_inode_id, name, attrs, layout)),
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
            } => AppDataResponse::Fs(self.apply_rename(
                src_parent_inode_id,
                src_name,
                dst_parent_inode_id,
                dst_name,
                flags,
            )),
            Command::SetAttr {
                inode_id, mask, attrs, ..
            } => AppDataResponse::Fs(self.apply_set_attr(inode_id, mask, attrs)),
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
            } => AppDataResponse::Fs(self.apply_set_xattr(inode_id, name, value, create, replace)),
            Command::RemoveXattr { inode_id, name, .. } => AppDataResponse::Fs(self.apply_remove_xattr(inode_id, name)),
        };

        // Store applied result for idempotency
        let applied_result = AppliedResult {
            seq,
            fingerprint,
            result: result.clone(),
            created_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            size_bytes: 0,
        };
        self.storage.put_applied_result(&dedup_key, applied_result)?;

        // Update applied sequence (persist + in-memory)
        self.storage.put_applied_seq(seq)?;
        *self.applied_seq.write() = seq;

        Ok(result)
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

        self.storage.put_lease(&lease_state)?;

        Ok(LeaseCommandResult::Acquired(lease_state))
    }

    fn apply_release_lease(&self, block_id: BlockId) -> MetadataResult<LeaseCommandResult> {
        self.storage.delete_lease(block_id)?;
        Ok(LeaseCommandResult::Released)
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
                return Ok(MountCommandResult::Upserted(existing));
            }
            return Err(MetadataError::AlreadyExists(format!(
                "Mount prefix already exists: {}",
                mount_prefix
            )));
        }

        if let Some(existing) = self.storage.get_mount(mount_id)? {
            if existing.mount_prefix == mount_prefix {
                return Ok(MountCommandResult::Upserted(existing));
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
        let root_inode = self.storage.get_inode(root_inode_id)?;
        let root_inode = match root_inode {
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
                self.storage.put_inode(&inode)?;
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

        // Store mount entry to RocksDB (source of truth)
        self.storage.put_mount(&entry)?;

        // Increment mount version
        self.storage.put_mount_version(new_version)?;

        // Synchronize in-memory MountTable (must succeed after RocksDB write)
        self.mount_table
            .upsert(entry.clone())
            .map_err(|e| MetadataError::Internal(format!("Failed to update MountTable after RocksDB write: {}", e)))?;

        Ok(MountCommandResult::Upserted(entry))
    }

    fn apply_delete_mount(&self, mount_id: MountId) -> MetadataResult<MountCommandResult> {
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

        // Delete mount entry from RocksDB (source of truth)
        self.storage.delete_mount(mount_id)?;

        // Increment mount version
        let mount_version = self.storage.get_mount_version()?;
        self.storage.put_mount_version(mount_version + 1)?;

        // Synchronize in-memory MountTable (must succeed after RocksDB delete)
        self.mount_table
            .remove(mount_id)
            .map_err(|e| MetadataError::Internal(format!("Failed to update MountTable after RocksDB delete: {}", e)))?;

        Ok(MountCommandResult::Deleted)
    }

    fn apply_increment_layout_version(&self) -> MetadataResult<LayoutVersion> {
        let current = self.storage.get_layout_version()?;
        let new_version = LayoutVersion::new(current.as_u64() + 1);
        self.storage.put_layout_version(new_version)?;

        Ok(new_version)
    }

    fn apply_add_shard_group(
        &self,
        shard_group_id: ShardGroupId,
        shard_ids: Vec<ShardId>,
        initial_members: Vec<u64>,
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

        self.storage.put_shard_group(&info)?;

        // Persist shard to group routing mappings
        for shard_id in &shard_ids {
            self.storage.put_shard_routing(*shard_id, shard_group_id)?;
        }

        Ok(info)
    }

    fn apply_upsert_worker_descriptor(
        &self,
        worker_id: WorkerId,
        address: String,
        net_transport_kind: i32,
        worker_epoch: u64,
        fault_domain: Option<String>,
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

        // Store worker descriptor (only authoritative fields)
        self.storage.put_worker(&worker_info)?;

        Ok(WorkerCommandResult::Upserted(worker_id))
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

    /// Helper: Generate next inode ID.
    fn next_inode_id(&self) -> InodeId {
        let mut next_id = self.next_inode_id.write();
        let inode_id = InodeId::new(*next_id);
        *next_id += 1;
        inode_id
    }

    fn next_data_handle_id(&self) -> MetadataResult<DataHandleId> {
        // Data-plane identities are allocated durably via RocksDB meta key.
        self.storage.get_and_increment_data_handle_id()
    }

    /// Apply Mkdir command.
    fn apply_mkdir(&self, parent_inode_id: InodeId, name: String, mut attrs: FileAttrs) -> FsCommandResult {
        let result: MetadataResult<FsOkResult> = (|| {
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
            let inode_id = self.next_inode_id();
            let now_ms = self.now_ms();

            // Initialize attrs
            attrs.update_timestamps(now_ms);
            attrs.nlink = 1; // Directory starts with 1 link (self)

            // Create directory inode (inherit mount_id from parent)
            let inode = Inode::new_dir(inode_id, attrs, parent_inode.mount_id);

            // Use WriteBatch for atomicity: write inode + dentry
            self.storage.put_inode(&inode)?;
            self.storage.put_dentry(parent_inode_id, &name, inode_id)?;

            // Update parent directory mtime/ctime
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(now_ms);
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;
            self.storage.put_inode(&updated_parent)?;

            Ok(FsOkResult {
                inode_id: Some(inode_id),
                data_handle_id: None,
            })
        })();

        Self::fs_command_result(result)
    }

    /// Apply Create command.
    fn apply_create(
        &self,
        parent_inode_id: InodeId,
        name: String,
        mut attrs: FileAttrs,
        layout: FileLayout,
    ) -> FsCommandResult {
        let result: MetadataResult<FsOkResult> = (|| {
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
            let inode_id = self.next_inode_id();
            let data_handle_id = self.next_data_handle_id()?;
            let now_ms = self.now_ms();

            // Initialize attrs
            attrs.update_timestamps(now_ms);
            attrs.nlink = 1;

            // Create file inode (inherit mount_id from parent) with a freshly allocated data handle.
            let inode = Inode::new_file(inode_id, attrs, parent_inode.mount_id, data_handle_id);

            // Persist data_handle_id -> inode mapping for routing and recovery.
            self.storage.put_data_handle_owner(data_handle_id, inode_id)?;
            self.storage.put_layout(inode_id, layout)?;

            // Store inode and dentry
            self.storage.put_inode(&inode)?;
            self.storage.put_dentry(parent_inode_id, &name, inode_id)?;

            // Update parent directory mtime/ctime
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(now_ms);
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;
            self.storage.put_inode(&updated_parent)?;

            Ok(FsOkResult {
                inode_id: Some(inode_id),
                data_handle_id: Some(data_handle_id),
            })
        })();

        Self::fs_command_result(result)
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
    ) -> FsCommandResult {
        let result: MetadataResult<FsOkResult> = (|| {
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
                    // Delete destination directory
                    self.storage.delete_inode(dst_inode_id)?;
                } else {
                    if dst_inode.kind.is_dir() {
                        return Err(MetadataError::IsDir("Cannot overwrite directory with file".to_string()));
                    }
                    // Delete destination file
                    self.storage.delete_inode(dst_inode_id)?;
                }
                // Delete destination dentry
                self.storage.delete_dentry(dst_parent_inode_id, &dst_name)?;
            }

            let now_ms = self.now_ms();

            // Atomic rename: delete source dentry, create destination dentry
            self.storage.delete_dentry(src_parent_inode_id, &src_name)?;
            self.storage.put_dentry(dst_parent_inode_id, &dst_name, src_inode_id)?;

            // Update parent directories mtime/ctime
            if src_parent_inode_id != dst_parent_inode_id {
                // Different parents - update both
                let src_parent = self
                    .storage
                    .get_inode(src_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Source parent disappeared".to_string()))?;
                let mut src_attrs = src_parent.attrs.clone();
                src_attrs.update_mtime_ctime(now_ms);
                let mut updated_src_parent = src_parent.clone();
                updated_src_parent.attrs = src_attrs;
                self.storage.put_inode(&updated_src_parent)?;
                let dst_parent = self
                    .storage
                    .get_inode(dst_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Destination parent disappeared".to_string()))?;
                let mut dst_attrs = dst_parent.attrs.clone();
                dst_attrs.update_mtime_ctime(now_ms);
                let mut updated_dst_parent = dst_parent.clone();
                updated_dst_parent.attrs = dst_attrs;
                self.storage.put_inode(&updated_dst_parent)?;
            } else {
                let parent = self
                    .storage
                    .get_inode(src_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Parent disappeared".to_string()))?;
                let mut attrs = parent.attrs.clone();
                attrs.update_mtime_ctime(now_ms);
                let mut updated_parent = parent.clone();
                updated_parent.attrs = attrs;
                self.storage.put_inode(&updated_parent)?;
            }

            // Update source inode ctime
            let mut src_attrs = src_inode.attrs.clone();
            src_attrs.update_ctime(now_ms);
            let mut updated_src_inode = src_inode.clone();
            updated_src_inode.attrs = src_attrs;
            self.storage.put_inode(&updated_src_inode)?;

            Ok(FsOkResult::default())
        })();

        Self::fs_command_result(result)
    }

    /// Apply SetAttr command.
    fn apply_set_attr(&self, inode_id: InodeId, mask: u32, new_attrs: FileAttrs) -> FsCommandResult {
        let result: MetadataResult<FsOkResult> = (|| {
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

            self.storage.put_inode(&inode)?;

            Ok(FsOkResult::default())
        })();

        Self::fs_command_result(result)
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
    ) -> FsCommandResult {
        let result: MetadataResult<FsOkResult> = (|| {
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
            self.storage.put_inode(&inode)?;
            Ok(FsOkResult::default())
        })();

        Self::fs_command_result(result)
    }

    /// Apply remove xattr command.
    fn apply_remove_xattr(&self, inode_id: InodeId, name: String) -> FsCommandResult {
        let result: MetadataResult<FsOkResult> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            if inode.xattrs.remove(&name).is_none() {
                return Err(MetadataError::NotFound(format!("xattr not found: {}", name)));
            }
            let now_ms = self.now_ms();
            inode.attrs.update_ctime(now_ms);
            self.storage.put_inode(&inode)?;
            Ok(FsOkResult::default())
        })();

        Self::fs_command_result(result)
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
}
