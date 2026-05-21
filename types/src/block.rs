// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use crate::fs::InodeId;
use crate::ids::{BlockId, DataHandleId, WorkerId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockState {
    /// Writable by the current lease owner.
    Open,
    /// No more writes; readers can rely on committed_length.
    Sealed,
    /// Optional: aborted/incomplete block.
    Aborted,
    /// Block has been deleted (tombstone state, can be cleaned up after TTL).
    Deleted,
    /// Block has been compacted/merged (tombstone state, can be cleaned up after TTL).
    Compacted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockPlacement {
    pub primary: WorkerId,
    pub replicas: Vec<WorkerId>,
}

impl BlockPlacement {
    #[inline]
    pub fn all_workers(&self) -> impl Iterator<Item = WorkerId> + '_ {
        core::iter::once(self.primary).chain(self.replicas.iter().copied())
    }
}

/// Strongly-consistent block metadata kept in Meta (Raft state machine).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    pub block_id: BlockId,
    /// Namespace identity that owns this block (for routing/ownership checks).
    pub inode_id: InodeId,
    /// Data handle that owns this block (matches block_id.data_handle_id).
    pub data_handle_id: DataHandleId,
    /// Block ordinal within the data handle (duplicate of block_id.index for quick scans).
    pub block_index: u32,
    pub start_offset: u64, // file offset within the data handle layout
    pub state: BlockState,
    pub placement: BlockPlacement,
    /// File-visible committed length boundary (either per-file or per-block; choose one).
    pub committed_length: u64,
}
