// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! In-memory state store implementation.
//!
//! This is a placeholder implementation using HashMap.
//! TODO(state): replace with Raft-backed state machine.

use super::{BlockMetaState, LeaseState, RouteEpoch, StateStore};
use crate::error::{MetadataError, MetadataResult};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use types::block::{BlockPlacement, BlockState};
use types::ids::{BlockId, ClientId};
use types::lease::Lease;

/// In-memory state store.
pub struct MemoryStateStore {
    blocks: Arc<RwLock<HashMap<BlockId, BlockMetaState>>>,
    leases: Arc<RwLock<HashMap<BlockId, LeaseState>>>,
    inodes: Arc<RwLock<HashMap<types::fs::InodeId, types::fs::Inode>>>,
    layouts: Arc<RwLock<HashMap<types::fs::InodeId, types::layout::FileLayout>>>,
    route_epoch: Arc<RwLock<RouteEpoch>>,
}

impl MemoryStateStore {
    /// Helper for tests: set inode and layout.
    pub fn put_inode_with_layout(&self, inode: types::fs::Inode, layout: types::layout::FileLayout) {
        let inode_id = inode.inode_id;
        self.inodes.write().insert(inode_id, inode);
        self.layouts.write().insert(inode_id, layout);
    }

    pub fn new() -> Self {
        Self {
            blocks: Arc::new(RwLock::new(HashMap::new())),
            leases: Arc::new(RwLock::new(HashMap::new())),
            inodes: Arc::new(RwLock::new(HashMap::new())),
            layouts: Arc::new(RwLock::new(HashMap::new())),
            route_epoch: Arc::new(RwLock::new(RouteEpoch::new(1))),
        }
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StateStore for MemoryStateStore {
    // NOTE: FileMeta and file-based operations have been removed.
    // All file metadata is now stored in inodes. Use the FS service (inode/dentry-based) instead.

    async fn get_block(&self, block_id: BlockId) -> MetadataResult<Option<BlockMetaState>> {
        let blocks = self.blocks.read();
        Ok(blocks.get(&block_id).cloned())
    }

    async fn create_block(
        &self,
        inode_id: types::fs::InodeId,
        block_id: BlockId,
        placement: BlockPlacement,
    ) -> MetadataResult<BlockMetaState> {
        let mut blocks = self.blocks.write();
        let block_meta = BlockMetaState {
            block_id,
            inode_id,
            data_handle_id: block_id.data_handle_id,
            state: BlockState::Open,
            placement,
            committed_length: 0,
        };
        blocks.insert(block_id, block_meta.clone());
        Ok(block_meta)
    }

    async fn update_block_state(&self, block_id: BlockId, state: BlockState) -> MetadataResult<()> {
        let mut blocks = self.blocks.write();
        if let Some(block_meta) = blocks.get_mut(&block_id) {
            block_meta.state = state;
            Ok(())
        } else {
            Err(MetadataError::NotFound(format!("Block not found: {:?}", block_id)))
        }
    }

    async fn get_lease(&self, block_id: BlockId) -> MetadataResult<Option<LeaseState>> {
        let leases = self.leases.read();
        Ok(leases.get(&block_id).cloned())
    }

    async fn acquire_lease(
        &self,
        block_id: BlockId,
        client_id: ClientId,
        epoch: u64,
        expires_at_ms: u64,
    ) -> MetadataResult<LeaseState> {
        let mut leases = self.leases.write();

        // Check existing lease
        if let Some(existing) = leases.get(&block_id) {
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

        leases.insert(block_id, lease_state.clone());
        Ok(lease_state)
    }

    async fn release_lease(&self, block_id: BlockId) -> MetadataResult<()> {
        let mut leases = self.leases.write();
        leases.remove(&block_id);
        Ok(())
    }

    async fn get_inode(&self, inode_id: types::fs::InodeId) -> MetadataResult<Option<types::fs::Inode>> {
        Ok(self.inodes.read().get(&inode_id).cloned())
    }

    async fn get_layout(&self, inode_id: types::fs::InodeId) -> MetadataResult<types::layout::FileLayout> {
        if let Some(layout) = self.layouts.read().get(&inode_id) {
            return Ok(*layout);
        }
        Err(MetadataError::NotFound(format!(
            "Layout not found for inode {}",
            inode_id
        )))
    }

    async fn get_route_epoch(&self) -> MetadataResult<RouteEpoch> {
        Ok(*self.route_epoch.read())
    }
}
