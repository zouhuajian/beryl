// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft-based StateStore implementation.

use crate::error::{MetadataError, MetadataResult};
use crate::raft::{AppDataResponse, AppRaftNode, BlockCommandResult, Command, DedupKey, LeaseCommandResult};
use crate::state::{BlockMetaState, LayoutVersion, LeaseState, StateStore};
use async_trait::async_trait;
use std::sync::Arc;
use types::block::{BlockPlacement, BlockState};
use types::ids::BlockId;

/// Raft-based StateStore implementation.
pub struct RaftStateStore {
    raft_node: Arc<AppRaftNode>,
}

impl RaftStateStore {
    pub fn new(raft_node: Arc<AppRaftNode>) -> Self {
        Self { raft_node }
    }
}

#[async_trait]
impl StateStore for RaftStateStore {
    // NOTE: FileMeta and file-based operations have been removed.
    // All file metadata is now stored in inodes. Use the FS service (inode/dentry-based) instead.

    async fn get_block(&self, block_id: BlockId) -> MetadataResult<Option<BlockMetaState>> {
        // Read from state machine (leader-read)
        self.raft_node.read(false, |sm| sm.get_block(block_id)).await
    }

    async fn create_block(
        &self,
        inode_id: types::fs::InodeId,
        block_id: BlockId,
        placement: BlockPlacement,
    ) -> MetadataResult<BlockMetaState> {
        let command = Command::AllocateBlock {
            dedup: DedupKey::system(),
            inode_id,
            block_id,
            placement,
        };

        match self.raft_node.propose(command).await? {
            AppDataResponse::Block(BlockCommandResult::Allocated(meta)) => Ok(meta),
            other => Err(MetadataError::Internal(format!(
                "Unexpected response for AllocateBlock: {:?}",
                other
            ))),
        }
    }

    async fn update_block_state(&self, block_id: BlockId, state: BlockState) -> MetadataResult<()> {
        let command = Command::UpdateBlockState {
            dedup: DedupKey::system(),
            block_id,
            state,
        };

        self.raft_node.propose(command).await?;
        Ok(())
    }

    async fn get_lease(&self, block_id: BlockId) -> MetadataResult<Option<LeaseState>> {
        // Read from state machine storage (leader-read)
        self.raft_node.read(false, |sm| sm.storage().get_lease(block_id)).await
    }

    async fn acquire_lease(
        &self,
        block_id: BlockId,
        client_id: types::ids::ClientId,
        epoch: u64,
        expires_at_ms: u64,
    ) -> MetadataResult<LeaseState> {
        let command = Command::AcquireLease {
            dedup: DedupKey::system(),
            block_id,
            client_id,
            epoch,
            expires_at_ms,
        };

        match self.raft_node.propose(command).await? {
            AppDataResponse::Lease(LeaseCommandResult::Acquired(lease)) => Ok(lease),
            other => Err(MetadataError::Internal(format!(
                "Unexpected response for AcquireLease: {:?}",
                other
            ))),
        }
    }

    async fn release_lease(&self, block_id: BlockId) -> MetadataResult<()> {
        // Command::ReleaseLease doesn't need client_id or fencing_token
        let command = Command::ReleaseLease {
            dedup: DedupKey::system(),
            block_id,
        };

        match self.raft_node.propose(command).await? {
            AppDataResponse::Lease(LeaseCommandResult::Released) => Ok(()),
            other => Err(MetadataError::Internal(format!(
                "Unexpected response for ReleaseLease: {:?}",
                other
            ))),
        }
    }

    async fn get_inode(&self, inode_id: types::fs::InodeId) -> MetadataResult<Option<types::fs::Inode>> {
        self.raft_node.read(false, |sm| sm.storage().get_inode(inode_id)).await
    }

    async fn get_layout(&self, inode_id: types::fs::InodeId) -> MetadataResult<types::layout::FileLayout> {
        self.raft_node.read(false, |sm| sm.storage().get_layout(inode_id)).await
    }

    async fn get_layout_version(&self) -> MetadataResult<LayoutVersion> {
        // Read from state machine storage (leader-read)
        self.raft_node.read(false, |sm| sm.storage().get_layout_version()).await
    }

    async fn increment_layout_version(&self) -> MetadataResult<LayoutVersion> {
        // This is typically not called directly, but if needed, we can implement it
        // For now, just return current version
        self.get_layout_version().await
    }
}
