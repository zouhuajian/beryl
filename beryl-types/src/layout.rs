// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use crate::chunk::{ByteRange, ChunkRef, ChunkSlice};
use crate::ids::{BlockId, BlockIndex, ChunkIndex, DataHandleId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Beryl block data/meta interpretation format selected by metadata.
///
/// This is not a worker StoreBackend or IoEngine. A worker may execute the same
/// block format on filesystem, mmap, SPDK, or another local engine, but metadata
/// only sees the stable format capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
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
/// by metadata for new blocks of this file version and then carried to worker
/// writes. `block_format_id` is not a worker StoreBackend or IoEngine. Current
/// FULL_EFFECTIVE writes require `block_size` to be a multiple of `chunk_size`
/// and `replication == 1`; larger target counts are reserved for durable
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
        BlockShape::new(
            self.block_format_id,
            u64::from(self.block_size),
            self.chunk_size,
            u64::from(self.block_size),
        )
        .map_err(FileLayoutError::from_block_shape_error)?;
        if self.replication == 0 {
            return Err(FileLayoutError::ZeroReplication);
        }
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
    /// * `_block_id` - The block ID, currently unused by chunk slicing.
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

/// Validated shape of one metadata-authorized block.
///
/// This carries only block layout fields that are persisted in worker block
/// metadata or sent across the data path. It does not validate ownership,
/// stamps, worker run ids, write stream sequence, or file-version state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockShape {
    pub block_format_id: BlockFormatId,
    pub block_size: u64,
    pub chunk_size: u32,
    pub effective_len: u64,
}

impl BlockShape {
    pub fn new(
        block_format_id: BlockFormatId,
        block_size: u64,
        chunk_size: u32,
        effective_len: u64,
    ) -> Result<Self, BlockShapeError> {
        validate_block_layout_parts(block_format_id, block_size, chunk_size)?;
        Self::validate_effective_len(block_size, effective_len)?;
        Ok(Self {
            block_format_id,
            block_size,
            chunk_size,
            effective_len,
        })
    }

    pub fn for_effective_len(layout: &FileLayout, effective_len: u64) -> Result<Self, BlockShapeError> {
        Self::new(
            layout.block_format_id,
            u64::from(layout.block_size),
            layout.chunk_size,
            effective_len,
        )
    }

    pub fn validate_effective_len(block_size: u64, effective_len: u64) -> Result<(), BlockShapeError> {
        if effective_len == 0 {
            return Err(BlockShapeError::ZeroEffectiveLen);
        }
        if effective_len > block_size {
            return Err(BlockShapeError::EffectiveLenExceedsBlock);
        }
        Ok(())
    }
}

fn validate_block_layout_parts(
    block_format_id: BlockFormatId,
    block_size: u64,
    chunk_size: u32,
) -> Result<(), BlockShapeError> {
    if block_size == 0 {
        return Err(BlockShapeError::ZeroBlockSize);
    }
    if chunk_size == 0 {
        return Err(BlockShapeError::ZeroChunkSize);
    }
    if u64::from(chunk_size) > block_size {
        return Err(BlockShapeError::ChunkLargerThanBlock);
    }
    if !block_size.is_multiple_of(u64::from(chunk_size)) {
        return Err(BlockShapeError::BlockSizeNotChunkAligned);
    }
    BlockFormatId::from_raw(block_format_id.as_raw()).map_err(BlockShapeError::UnknownBlockFormat)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum BlockShapeError {
    #[error("block_size must be non-zero")]
    ZeroBlockSize,
    #[error("chunk_size must be non-zero")]
    ZeroChunkSize,
    #[error("chunk_size must not exceed block_size")]
    ChunkLargerThanBlock,
    #[error("block_size must be a multiple of chunk_size")]
    BlockSizeNotChunkAligned,
    #[error("{0}")]
    UnknownBlockFormat(BlockFormatIdError),
    #[error("effective_len must be non-zero")]
    ZeroEffectiveLen,
    #[error("effective_len must not exceed block_size")]
    EffectiveLenExceedsBlock,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum FileLayoutError {
    #[error("block_size must be non-zero")]
    ZeroBlockSize,
    #[error("chunk_size must be non-zero")]
    ZeroChunkSize,
    #[error("chunk_size must not exceed block_size")]
    ChunkLargerThanBlock,
    #[error("block_size must be a multiple of chunk_size")]
    BlockSizeNotChunkAligned,
    #[error("replication must be at least one")]
    ZeroReplication,
    #[error("{0}")]
    UnknownBlockFormat(BlockFormatIdError),
}

impl FileLayoutError {
    fn from_block_shape_error(err: BlockShapeError) -> Self {
        match err {
            BlockShapeError::ZeroBlockSize => Self::ZeroBlockSize,
            BlockShapeError::ZeroChunkSize => Self::ZeroChunkSize,
            BlockShapeError::ChunkLargerThanBlock => Self::ChunkLargerThanBlock,
            BlockShapeError::BlockSizeNotChunkAligned => Self::BlockSizeNotChunkAligned,
            BlockShapeError::UnknownBlockFormat(err) => Self::UnknownBlockFormat(err),
            BlockShapeError::ZeroEffectiveLen | BlockShapeError::EffectiveLenExceedsBlock => {
                unreachable!("FileLayout validates block shape with effective_len=block_size")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_per_block_uses_valid_layouts() {
        assert_eq!(FileLayout::new(4096, 4096, 1).chunks_per_block(), 1);
        assert_eq!(FileLayout::new(8192, 4096, 1).chunks_per_block(), 2);
        assert_eq!(FileLayout::new(6 * 4096, 4096, 1).chunks_per_block(), 6);
    }

    #[test]
    fn block_shape_accepts_full_and_tail_effective_lengths() {
        let full =
            BlockShape::new(BlockFormatId::FULL_EFFECTIVE, 4096, 1024, 4096).expect("full block shape must pass");
        assert_eq!(full.block_format_id, BlockFormatId::FULL_EFFECTIVE);
        assert_eq!(full.block_size, 4096);
        assert_eq!(full.chunk_size, 1024);
        assert_eq!(full.effective_len, 4096);

        let layout = FileLayout::new(4096, 1024, 1);
        let tail = BlockShape::for_effective_len(&layout, 3072).expect("tail block shape must pass");
        assert_eq!(tail.effective_len, 3072);
    }

    #[test]
    fn block_shape_rejects_invalid_size_chunk_and_effective_length() {
        let cases = [
            (
                BlockShape::new(BlockFormatId::FULL_EFFECTIVE, 0, 1024, 1),
                BlockShapeError::ZeroBlockSize,
            ),
            (
                BlockShape::new(BlockFormatId::FULL_EFFECTIVE, 4096, 0, 1),
                BlockShapeError::ZeroChunkSize,
            ),
            (
                BlockShape::new(BlockFormatId::FULL_EFFECTIVE, 1024, 4096, 1),
                BlockShapeError::ChunkLargerThanBlock,
            ),
            (
                BlockShape::new(BlockFormatId::FULL_EFFECTIVE, 4097, 1024, 1),
                BlockShapeError::BlockSizeNotChunkAligned,
            ),
            (
                BlockShape::new(BlockFormatId::FULL_EFFECTIVE, 4096, 1024, 0),
                BlockShapeError::ZeroEffectiveLen,
            ),
            (
                BlockShape::new(BlockFormatId::FULL_EFFECTIVE, 4096, 1024, 4097),
                BlockShapeError::EffectiveLenExceedsBlock,
            ),
        ];

        for (result, expected) in cases {
            assert_eq!(result.expect_err("invalid shape must fail"), expected);
        }
    }
}
