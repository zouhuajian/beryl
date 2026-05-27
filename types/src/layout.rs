// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use crate::chunk::{ByteRange, ChunkRef, ChunkSlice};
use crate::ids::{BlockId, BlockIndex, ChunkIndex, DataHandleId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Vecton block data/meta interpretation format selected by metadata.
///
/// This is not a worker StoreBackend or IoEngine. A worker may execute the same
/// block format on filesystem, mmap, SPDK, or another local engine, but metadata
/// only sees the stable format capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlockFormatId(u32);

impl BlockFormatId {
    /// Complete effective block file format used by the current worker store.
    pub const FULL_EFFECTIVE: Self = Self(1);

    /// Block format metadata assigns to newly created files.
    pub const CURRENT_FOR_NEW_FILE: Self = Self::FULL_EFFECTIVE;

    /// Return the raw format identifier.
    #[inline]
    pub const fn as_raw(self) -> u32 {
        self.0
    }

    /// Decode a persisted or wire block format identifier.
    pub fn from_raw(value: u32) -> Result<Self, BlockFormatIdError> {
        match value {
            1 => Ok(Self::FULL_EFFECTIVE),
            other => Err(BlockFormatIdError { raw: other }),
        }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown block_format_id {raw}")]
pub struct BlockFormatIdError {
    pub raw: u32,
}

/// Metadata-owned logical layout for a file version or data handle.
///
/// `block_size`, `chunk_size`, `replication`, and `block_format_id` are chosen
/// by metadata and then carried to worker writes. Current active writes require
/// `replication == 1`; larger target counts are reserved for durable
/// multi-replica write support.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileLayout {
    pub block_size: u32, // bytes
    pub chunk_size: u32, // bytes
    pub block_format_id: BlockFormatId,
    pub replication: u8,
}

impl FileLayout {
    /// Construct a layout for a newly created file using the current block format.
    pub const fn new(block_size: u32, chunk_size: u32, replication: u8) -> Self {
        Self::with_block_format(block_size, chunk_size, replication, BlockFormatId::CURRENT_FOR_NEW_FILE)
    }

    pub const fn with_block_format(
        block_size: u32,
        chunk_size: u32,
        replication: u8,
        block_format_id: BlockFormatId,
    ) -> Self {
        Self {
            block_size,
            chunk_size,
            block_format_id,
            replication,
        }
    }

    pub fn validate(&self) -> Result<(), FileLayoutError> {
        if self.block_size == 0 {
            return Err(FileLayoutError::ZeroBlockSize);
        }
        if self.chunk_size == 0 {
            return Err(FileLayoutError::ZeroChunkSize);
        }
        if self.chunk_size > self.block_size {
            return Err(FileLayoutError::ChunkLargerThanBlock);
        }
        if self.replication == 0 {
            return Err(FileLayoutError::ZeroReplication);
        }
        BlockFormatId::from_raw(self.block_format_id.as_raw()).map_err(FileLayoutError::UnknownBlockFormat)?;
        Ok(())
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

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum FileLayoutError {
    #[error("block_size must be non-zero")]
    ZeroBlockSize,
    #[error("chunk_size must be non-zero")]
    ZeroChunkSize,
    #[error("chunk_size must not exceed block_size")]
    ChunkLargerThanBlock,
    #[error("replication must be at least one")]
    ZeroReplication,
    #[error("{0}")]
    UnknownBlockFormat(BlockFormatIdError),
}

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
