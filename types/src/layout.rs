// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use crate::chunk::{ByteRange, ChunkRef, ChunkSlice};
use crate::ids::{BlockId, BlockIndex, ChunkIndex, DataHandleId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Fixed layout parameters for a file (stable once created).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileLayout {
    pub block_size: u32, // bytes
    pub chunk_size: u32, // bytes
    pub replication: u8,
}

impl FileLayout {
    pub const fn new(block_size: u32, chunk_size: u32, replication: u8) -> Self {
        Self {
            block_size,
            chunk_size,
            replication,
        }
    }

    #[inline]
    pub fn chunks_per_block(&self) -> u32 {
        // Allow the last block to be partial while keeping per-block chunk counts based on the full block size for bitmap sizing.
        self.block_size.div_ceil(self.chunk_size)
    }

    /// Calculate block index from file offset.
    #[inline]
    pub fn block_index_from_offset(&self, offset: u64) -> BlockIndex {
        BlockIndex((offset / self.block_size as u64) as u32)
    }

    /// Calculate the start offset of a block within a file.
    #[inline]
    pub fn block_start_offset(&self, block_index: BlockIndex) -> u64 {
        block_index.as_raw() as u64 * self.block_size as u64
    }

    /// Calculate chunk index from offset within a block.
    #[inline]
    pub fn chunk_index_from_offset_in_block(&self, offset_in_block: u64) -> ChunkIndex {
        ChunkIndex((offset_in_block / self.chunk_size as u64) as u32)
    }

    /// Calculate the start offset of a chunk within a block.
    #[inline]
    pub fn chunk_start_offset_in_block(&self, chunk_index: ChunkIndex) -> u32 {
        chunk_index.as_raw() * self.chunk_size
    }

    /// Split a file range into chunk slices.
    ///
    /// Returns a vector of `ChunkSlice` that covers the given range.
    /// Each slice specifies which chunk and what portion of it to read.
    pub fn split_range_to_chunk_slices(&self, data_handle_id: DataHandleId, range: ByteRange) -> Vec<ChunkSlice> {
        let mut slices = Vec::new();
        let mut current_offset = range.offset;
        let end_offset = range.offset + range.len as u64;

        while current_offset < end_offset {
            let block_index = self.block_index_from_offset(current_offset);
            let block_start = self.block_start_offset(block_index);
            let offset_in_block = current_offset - block_start;

            let chunk_index = self.chunk_index_from_offset_in_block(offset_in_block);
            let chunk_start_in_block = self.chunk_start_offset_in_block(chunk_index);
            let offset_in_chunk = (offset_in_block - chunk_start_in_block as u64) as u32;

            // Calculate how much we can read from this chunk
            let chunk_end_in_block = chunk_start_in_block + self.chunk_size;
            let remaining_in_chunk =
                (chunk_end_in_block as u64 - (block_start + offset_in_block)).min(end_offset - current_offset) as u32;

            let chunk_ref = ChunkRef::new(BlockId::new(data_handle_id, block_index), chunk_index.as_raw());

            slices.push(ChunkSlice {
                chunk: chunk_ref,
                offset_in_chunk,
                len: remaining_in_chunk,
            });

            current_offset += remaining_in_chunk as u64;
        }

        slices
    }

    /// Convert a range within a block to chunk indices.
    ///
    /// Returns a vector of (chunk_index, offset_in_chunk, len) tuples.
    /// This is the unified conversion function that should be reused by all read/write paths.
    ///
    /// # Arguments
    /// * `_block_id` - The block ID (for future use, e.g., validation)
    /// * `offset` - Offset within the block (0-based)
    /// * `len` - Length in bytes
    ///
    /// # Returns
    /// Vector of (chunk_index, offset_in_chunk, len) tuples covering the range.
    pub fn range_to_chunks(&self, _block_id: BlockId, offset: u32, len: u32) -> Vec<(ChunkIndex, u32, u32)> {
        let mut chunks = Vec::new();
        let mut current_offset = offset as u64;
        let end_offset = offset as u64 + len as u64;

        while current_offset < end_offset {
            let chunk_index = self.chunk_index_from_offset_in_block(current_offset);
            let chunk_start = self.chunk_start_offset_in_block(chunk_index) as u64;
            let offset_in_chunk = (current_offset - chunk_start) as u32;

            // Calculate how much we can read from this chunk
            let chunk_end = chunk_start + self.chunk_size as u64;
            let remaining_in_chunk = (chunk_end - current_offset).min(end_offset - current_offset) as u32;

            chunks.push((chunk_index, offset_in_chunk, remaining_in_chunk));

            current_offset += remaining_in_chunk as u64;
        }

        chunks
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PlacementPolicy {
    /// Meta decides primary + replicas; client reads nearest/primary first.
    Default,
    /// Prefer local rack/zone if you have topology support.
    RackAware,
}

impl core::str::FromStr for PlacementPolicy {
    type Err = PlacementPolicyParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "default" | "Default" => Ok(Self::Default),
            "rack" | "rackaware" | "RackAware" => Ok(Self::RackAware),
            _ => Err(PlacementPolicyParseError),
        }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("invalid placement policy")]
pub struct PlacementPolicyParseError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_per_block_handles_div_ceil_boundaries() {
        assert_eq!(FileLayout::new(0, 4096, 1).chunks_per_block(), 0);
        assert_eq!(FileLayout::new(4096, 4096, 1).chunks_per_block(), 1);
        assert_eq!(FileLayout::new(8192, 4096, 1).chunks_per_block(), 2);
        assert_eq!(FileLayout::new(8193, 4096, 1).chunks_per_block(), 3);
        assert_eq!(FileLayout::new(1, 4096, 1).chunks_per_block(), 1);
        assert_eq!(FileLayout::new(5 * 4096 + 1, 4096, 1).chunks_per_block(), 6);
    }
}
