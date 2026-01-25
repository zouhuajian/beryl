// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use crate::chunk::ChunkBitmap;
use crate::fs::InodeId;
use crate::ids::{BlockId, DataHandleId, ShardGroupId, WorkerId};
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

/// Worker-local block state (more granular than metadata BlockState).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalBlockState {
    /// Block is being written (has at least one chunk in Writing state).
    Writing,
    /// All chunks committed, block is complete.
    Committed,
    /// Block has uncommitted writes (dirty).
    Dirty,
    /// Block is clean (all committed, no pending writes).
    Clean,
    /// Block can be evicted.
    Evictable,
    /// Block is being deleted.
    Deleting,
    /// Block has been deleted (tombstone state, can be cleaned up after TTL).
    Deleted,
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

/// Worker-local block metadata (authoritative on worker).
///
/// This is the local view of a block, including chunk presence bitmap,
/// local state, and access tracking for eviction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalBlockMeta {
    pub block_id: BlockId,
    pub group_id: ShardGroupId,
    /// Local state (more granular than metadata BlockState).
    pub state: LocalBlockState,
    /// Chunk presence bitmap (which chunks are committed locally).
    pub chunk_bitmap: ChunkBitmap,
    /// Committed length within this block (bytes).
    pub committed_length: u64,
    /// Total size of this block (may be less than block_size for last block).
    pub total_size: u64,
    /// Placement information (replica info, placeholder for MVP).
    pub placement: Option<BlockPlacement>,
    /// Last access time (for eviction/LRU).
    pub last_access: Option<u64>, // Unix timestamp in seconds
    /// File layout version (from metadata, used for version checking).
    /// If None, falls back to committed_length for version checking.
    pub layout_version: Option<u64>,
    /// Block-level layout epoch; changes on route/content/commit changes.
    /// Used for client cache validation and worker fast validation.
    pub block_stamp: u64,
}

impl LocalBlockMeta {
    /// Create a new LocalBlockMeta with empty chunk bitmap.
    pub fn new(block_id: BlockId, group_id: ShardGroupId, chunk_bitmap: ChunkBitmap) -> Self {
        Self {
            block_id,
            group_id,
            state: LocalBlockState::Writing,
            chunk_bitmap,
            committed_length: 0,
            total_size: 0,
            placement: None,
            last_access: None,
            layout_version: None,
            block_stamp: 0, // TODO: implement proper block_stamp tracking
        }
    }

    /// Check if a chunk is present (committed).
    pub fn has_chunk(&self, chunk_idx: u32) -> bool {
        self.chunk_bitmap.test(chunk_idx)
    }

    /// Mark a chunk as committed.
    pub fn mark_chunk_committed(&mut self, chunk_idx: u32, chunk_size: u32) {
        self.chunk_bitmap.set(chunk_idx);
        self.committed_length += chunk_size as u64;
        if self.committed_length > self.total_size {
            self.total_size = self.committed_length;
        }
    }

    /// Check if block is complete (all expected chunks are present).
    pub fn is_complete(&self, expected_chunks: u32) -> bool {
        // Check if all chunks from 0 to expected_chunks-1 are present
        for i in 0..expected_chunks {
            if !self.has_chunk(i) {
                return false;
            }
        }
        true
    }

    /// Update last access time to now.
    pub fn touch(&mut self) {
        use std::time::{SystemTime, UNIX_EPOCH};
        self.last_access = SystemTime::now().duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs());
    }
}
