// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! State storage abstraction for metadata service.
//!
//! This module defines the state machine interface that will be replaced

mod memory;
mod raft_store;

pub use memory::MemoryStateStore;
pub use raft_store::RaftStateStore;

use crate::error::MetadataResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use types::block::{BlockPlacement, BlockState};
use types::ids::{BlockId, ClientId, DataHandleId};
use types::lease::Lease;

/// Layout version / epoch for consistency.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LayoutVersion(u64);

impl LayoutVersion {
    pub fn new(version: u64) -> Self {
        Self(version)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Block metadata stored in state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockMetaState {
    pub block_id: BlockId,
    pub inode_id: types::fs::InodeId,
    pub data_handle_id: DataHandleId,
    pub state: BlockState,
    pub placement: BlockPlacement,
    pub committed_length: u64,
}

/// Lease state stored in state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseState {
    pub block_id: BlockId,
    pub lease: Lease,
}

/// Delete intent reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DeleteIntentReason {
    /// GC: block is unreferenced and eligible for deletion.
    Gc,
    /// Orphan: block exists on worker but not in metadata.
    Orphan,
    /// Lease: lease expired and block should be deleted.
    Lease,
    /// Manual: manual deletion request.
    Manual,
    /// OverRep: block has more replicas than desired (over-replicated cleanup).
    OverRep,
}

/// Delete intent execution status (persisted in RocksDB).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DeleteIntentStatus {
    /// Pending: not yet started execution.
    Pending,
    /// InFlight: command sent, waiting for ack.
    InFlight,
    /// Completed: all required acks received.
    Completed,
    /// Failed: non-retryable failure.
    Failed,
}

impl Default for DeleteIntentStatus {
    fn default() -> Self {
        DeleteIntentStatus::Pending
    }
}

/// Delete intent for block deletion.
///
/// This is an authoritative, recoverable, low-frequency intent that is persisted in Raft.
/// High-frequency execution progress should NOT be written to Raft.
/// Execution status (Completed/Failed) is persisted directly to RocksDB (not via Raft).
///
/// Enhanced with shard_group_id and guard_watermark for cross-group gate control.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteIntent {
    /// Unique intent ID (UUID or u64).
    pub intent_id: u64,
    /// Block ID to delete.
    pub block_id: BlockId,
    /// Reason for deletion.
    pub reason: DeleteIntentReason,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at_ms: u64,
    /// Not before timestamp (milliseconds since epoch).
    /// Intent should not be executed before this time (grace window).
    pub not_before_ms: u64,
    /// Shard group ID this intent belongs to.
    /// Required for cross-group gate control.
    #[serde(default)]
    pub shard_group_id: Option<types::ids::ShardGroupId>,
    /// Guard watermark (shard_group_id + state_id).
    /// Used for execution gating: only execute if the target shard group
    /// has applied at least up to guard_watermark.state_id.
    #[serde(default)]
    pub guard_watermark: Option<types::group_watermark::GroupWatermark>,
    /// Mount epoch at intent creation time (optional).
    /// Used for route consistency checking.
    #[serde(default)]
    pub mount_epoch: Option<types::group_watermark::MountEpoch>,
    /// Guard state ID (legacy, for backward compatibility).
    /// If guard_watermark is provided, this is ignored.
    pub guard_state_id: types::RaftLogId,
    /// Target workers (optional, can be empty for now).
    pub target_workers: Vec<types::ids::WorkerId>,
    /// Execution status (persisted in RocksDB, not via Raft).
    #[serde(default)]
    pub status: DeleteIntentStatus,
    /// Finished timestamp (milliseconds since epoch, None if not finished).
    #[serde(default)]
    pub finished_at_ms: Option<u64>,
    /// Last error message (for Failed status).
    #[serde(default)]
    pub last_error_msg: Option<String>,
}

/// State store trait.
/// TODO(state): replace with a Raft-backed state machine.
#[async_trait]
pub trait StateStore: Send + Sync {
    // NOTE: FileMeta and file-based operations have been removed.
    // All file metadata is now stored in inodes. Use the FS service (inode/dentry-based) instead.
    // Path is not authoritative storage. All path operations must go through the FS service (inode/dentry-based).
    // The old MetadataClientService is deprecated. Use MetadataInodeServiceProto instead.

    // NOTE: list_files has been removed. Path-based listing is not authoritative.
    // Use the FS service ReadDir operation (inode/dentry-based) instead.

    /// Get block metadata.
    async fn get_block(&self, block_id: BlockId) -> MetadataResult<Option<BlockMetaState>>;

    /// Create or update block metadata.
    async fn create_block(
        &self,
        inode_id: types::fs::InodeId,
        block_id: BlockId,
        placement: BlockPlacement,
    ) -> MetadataResult<BlockMetaState>;

    /// Update block state.
    async fn update_block_state(&self, block_id: BlockId, state: BlockState) -> MetadataResult<()>;

    /// Get lease for a block.
    async fn get_lease(&self, block_id: BlockId) -> MetadataResult<Option<LeaseState>>;

    /// Acquire or renew lease.
    async fn acquire_lease(
        &self,
        block_id: BlockId,
        client_id: ClientId,
        epoch: u64,
        expires_at_ms: u64,
    ) -> MetadataResult<LeaseState>;

    /// Release lease.
    async fn release_lease(&self, block_id: BlockId) -> MetadataResult<()>;

    /// Get inode by id.
    async fn get_inode(&self, inode_id: types::fs::InodeId) -> MetadataResult<Option<types::fs::Inode>>;

    /// Get layout for an inode (authoritative).
    async fn get_layout(&self, inode_id: types::fs::InodeId) -> MetadataResult<types::layout::FileLayout>;

    /// Get current layout version.
    async fn get_layout_version(&self) -> MetadataResult<LayoutVersion>;

    /// Increment layout version (for epoch updates).
    async fn increment_layout_version(&self) -> MetadataResult<LayoutVersion>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::ids::BlockIndex;

    #[tokio::test]
    async fn test_block_id_format() {
        let data_handle_id = DataHandleId::new(42);
        let block_index = BlockIndex::new(7);
        let block_id = BlockId::new(data_handle_id, block_index);

        assert_eq!(block_id.data_handle_id.as_raw(), 42);
        assert_eq!(block_id.index.as_raw(), 7);
        assert_eq!(format!("{}", block_id), "42:7");
    }

    #[tokio::test]
    async fn test_layout_version() {
        let v1 = LayoutVersion::new(1);
        let v2 = LayoutVersion::new(2);

        assert_eq!(v1.as_u64(), 1);
        assert_eq!(v2.as_u64(), 2);
        assert_ne!(v1, v2);
    }

    #[tokio::test]
    async fn test_validate_data_handle_owner() {
        use crate::raft::RocksDBStorage;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::open(dir.path()).unwrap();
        let dh1 = DataHandleId::new(1);
        let inode1 = types::fs::InodeId::new(10);
        storage.put_data_handle_owner(dh1, inode1).unwrap();

        // Success path
        let owner = storage.validate_data_handle_owner(dh1, None).unwrap();
        assert_eq!(owner, inode1);

        // Missing handle should return StaleState
        let missing = storage.validate_data_handle_owner(DataHandleId::new(99), None);
        assert!(missing.is_err());

        // Mismatch should return InvalidArgument
        let mismatch = storage.validate_data_handle_owner(dh1, Some(types::fs::InodeId::new(11)));
        assert!(mismatch.is_err());
    }
}
