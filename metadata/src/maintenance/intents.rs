// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! DeleteIntent builder: unified intent creation with all required fields.
//!
//! This module provides DeleteIntentBuilder to ensure all required fields are properly filled.

use crate::error::MetadataResult;
use crate::maintenance::owner_group_for_block;
use crate::mount::MountTable;
use crate::raft::RocksDBStorage;
use crate::state::{DeleteIntent, DeleteIntentReason};
use std::sync::Arc;
use types::group_watermark::{GroupWatermark, MountEpoch};
use types::ids::{BlockId, ShardGroupId, WorkerId};
use types::RaftLogId;

/// Builder for creating DeleteIntent with all required fields.
pub struct DeleteIntentBuilder {
    mount_table: Arc<MountTable>,
    storage: Arc<RocksDBStorage>,
}

impl DeleteIntentBuilder {
    pub fn new(mount_table: Arc<MountTable>, storage: Arc<RocksDBStorage>) -> Self {
        Self { mount_table, storage }
    }

    /// Build a DeleteIntent with all required fields.
    ///
    /// Returns error if router is not available or resolution fails (fail-closed).
    pub fn build(
        &self,
        intent_id: u64,
        block_id: BlockId,
        reason: DeleteIntentReason,
        created_at_ms: u64,
        not_before_ms: u64,
        guard_state_id: RaftLogId,
        target_workers: Vec<WorkerId>,
    ) -> MetadataResult<DeleteIntent> {
        // Resolve shard_group_id from block_id (TODO-1)
        let shard_group_id = self.resolve_shard_group_id(block_id)?;

        // Build guard_watermark
        let guard_watermark = GroupWatermark::new(shard_group_id, guard_state_id);

        // Get mount_epoch from mount_table
        let mount_epoch = MountEpoch::new(self.mount_table.version());

        Ok(DeleteIntent {
            intent_id,
            block_id,
            reason,
            created_at_ms,
            not_before_ms,
            shard_group_id: Some(shard_group_id),
            guard_watermark: Some(guard_watermark),
            mount_epoch: Some(mount_epoch),
            guard_state_id,
            target_workers,
            status: crate::state::DeleteIntentStatus::Pending,
            finished_at_ms: None,
            last_error_msg: None,
        })
    }

    /// Resolve shard_group_id from block_id using inode->mount owner.
    fn resolve_shard_group_id(&self, block_id: BlockId) -> MetadataResult<ShardGroupId> {
        owner_group_for_block(&self.storage, &self.mount_table, block_id)
    }
}
