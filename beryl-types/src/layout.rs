// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Largest logical block size accepted by the current Beryl block format.
///
/// This is a persisted domain invariant, not a client buffering or transport
/// policy. Lowering it requires an explicit metadata and worker-format
/// migration for blocks created under the previous ceiling.
pub const MAX_BLOCK_SIZE: u32 = 1024 * 1024 * 1024;

/// Storage integrity unit fixed by the `DURABLE_PREFIX` block format.
const DURABLE_PREFIX_STORAGE_CHUNK_SIZE: u32 = 4 * 1024 * 1024;

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
    /// Durable-prefix block format with persisted writer fencing and atomic checkpoints.
    pub const DURABLE_PREFIX: Self = Self(2);

    /// Block format metadata assigns to newly created files.
    pub const CURRENT_FOR_NEW_FILE: Self = Self::DURABLE_PREFIX;

    /// Return the raw format identifier.
    #[inline]
    pub const fn as_raw(self) -> u32 {
        self.0
    }

    /// Decode a persisted or wire block format identifier.
    pub fn from_raw(value: u32) -> Result<Self, BlockFormatIdError> {
        match value {
            2 => Ok(Self::DURABLE_PREFIX),
            other => Err(BlockFormatIdError { raw: other }),
        }
    }

    /// Return the storage integrity unit size in bytes, or an error for an unknown format.
    pub fn storage_chunk_size(self) -> Result<u32, BlockFormatIdError> {
        match self {
            Self::DURABLE_PREFIX => Ok(DURABLE_PREFIX_STORAGE_CHUNK_SIZE),
            other => Err(BlockFormatIdError { raw: other.as_raw() }),
        }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown block_format_id {raw}")]
pub struct BlockFormatIdError {
    pub raw: u32,
}

/// Metadata-owned logical layout for a file inode.
///
/// Metadata selects this immutable layout when creating the file and persists
/// it with the inode. Field order is also part of the binary encoding of
/// Metadata CreateFile replay records. Worker-local format parameters are derived from
/// `block_format_id`; replica execution is not part of the current product.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileLayout {
    pub block_size: u32, // bytes
    pub block_format_id: BlockFormatId,
}

impl FileLayout {
    /// Construct a layout for a newly created file using the current block format.
    pub const fn new(block_size: u32) -> Self {
        Self::with_block_format(block_size, BlockFormatId::CURRENT_FOR_NEW_FILE)
    }

    pub const fn with_block_format(block_size: u32, block_format_id: BlockFormatId) -> Self {
        Self {
            block_size,
            block_format_id,
        }
    }

    pub fn validate(&self) -> Result<(), FileLayoutError> {
        BlockFormatId::from_raw(self.block_format_id.as_raw()).map_err(FileLayoutError::UnknownBlockFormat)?;
        if self.block_size == 0 {
            return Err(FileLayoutError::ZeroBlockSize);
        }
        if self.block_size > MAX_BLOCK_SIZE {
            return Err(FileLayoutError::BlockTooLarge {
                actual: u64::from(self.block_size),
                maximum: u64::from(MAX_BLOCK_SIZE),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum FileLayoutError {
    #[error("block_size must be non-zero")]
    ZeroBlockSize,
    #[error("block_size {actual} exceeds maximum {maximum}")]
    BlockTooLarge { actual: u64, maximum: u64 },
    #[error("{0}")]
    UnknownBlockFormat(BlockFormatIdError),
}

/// Validated shape of one metadata-authorized block.
///
/// This carries only block layout fields that are persisted in worker block
/// metadata or sent across the data path. It does not validate ownership,
/// worker run ids, write stream ordering, or file content generation.
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
        let chunk_size = layout
            .block_format_id
            .storage_chunk_size()
            .map_err(BlockShapeError::UnknownBlockFormat)?;
        Self::new(
            layout.block_format_id,
            u64::from(layout.block_size),
            chunk_size,
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
    if block_size > u64::from(MAX_BLOCK_SIZE) {
        return Err(BlockShapeError::BlockTooLarge {
            actual: block_size,
            maximum: u64::from(MAX_BLOCK_SIZE),
        });
    }
    let expected_chunk_size = block_format_id
        .storage_chunk_size()
        .map_err(BlockShapeError::UnknownBlockFormat)?;
    if chunk_size != expected_chunk_size {
        return Err(BlockShapeError::StorageChunkSizeMismatch {
            expected: expected_chunk_size,
            got: chunk_size,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum BlockShapeError {
    #[error("block_size must be non-zero")]
    ZeroBlockSize,
    #[error("block_size {actual} exceeds maximum {maximum}")]
    BlockTooLarge { actual: u64, maximum: u64 },
    #[error("storage_chunk_size mismatch: expected {expected}, got {got}")]
    StorageChunkSizeMismatch { expected: u32, got: u32 },
    #[error("{0}")]
    UnknownBlockFormat(BlockFormatIdError),
    #[error("effective_len must be non-zero")]
    ZeroEffectiveLen,
    #[error("effective_len must not exceed block_size")]
    EffectiveLenExceedsBlock,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_shape_rejects_invalid_size_format_and_effective_length() {
        let chunk_size = BlockFormatId::DURABLE_PREFIX.storage_chunk_size().unwrap();
        assert_eq!(chunk_size, 4 * 1024 * 1024);
        let cases = [
            (
                BlockShape::new(BlockFormatId::DURABLE_PREFIX, 0, chunk_size, 1),
                BlockShapeError::ZeroBlockSize,
            ),
            (
                BlockShape::new(
                    BlockFormatId::DURABLE_PREFIX,
                    u64::from(MAX_BLOCK_SIZE) + 1,
                    chunk_size,
                    u64::from(MAX_BLOCK_SIZE) + 1,
                ),
                BlockShapeError::BlockTooLarge {
                    actual: u64::from(MAX_BLOCK_SIZE) + 1,
                    maximum: u64::from(MAX_BLOCK_SIZE),
                },
            ),
            (
                BlockShape::new(BlockFormatId::DURABLE_PREFIX, 4096, chunk_size - 1, 1),
                BlockShapeError::StorageChunkSizeMismatch {
                    expected: chunk_size,
                    got: chunk_size - 1,
                },
            ),
            (
                BlockShape::new(BlockFormatId::DURABLE_PREFIX, 4096, chunk_size, 0),
                BlockShapeError::ZeroEffectiveLen,
            ),
            (
                BlockShape::new(BlockFormatId::DURABLE_PREFIX, 4096, chunk_size, 4097),
                BlockShapeError::EffectiveLenExceedsBlock,
            ),
        ];

        for (result, expected) in cases {
            assert_eq!(result.expect_err("invalid shape must fail"), expected);
        }
    }

    #[test]
    fn file_layout_preserves_size_boundaries() {
        FileLayout::new(MAX_BLOCK_SIZE)
            .validate()
            .expect("maximum supported layout must pass");
        assert_eq!(FileLayout::new(0).validate(), Err(FileLayoutError::ZeroBlockSize));
        assert_eq!(
            FileLayout::new(MAX_BLOCK_SIZE + 1).validate(),
            Err(FileLayoutError::BlockTooLarge {
                actual: u64::from(MAX_BLOCK_SIZE) + 1,
                maximum: u64::from(MAX_BLOCK_SIZE),
            })
        );
    }

    #[test]
    fn deserialized_unknown_format_is_rejected_by_chunk_size_and_layout() {
        let block_format_id = serde_json::from_str::<BlockFormatId>("99").unwrap();
        assert_eq!(
            block_format_id.storage_chunk_size(),
            Err(BlockFormatIdError { raw: 99 })
        );
        for block_size in [0, MAX_BLOCK_SIZE + 1] {
            assert_eq!(
                FileLayout::with_block_format(block_size, block_format_id).validate(),
                Err(FileLayoutError::UnknownBlockFormat(BlockFormatIdError { raw: 99 }))
            );
        }
    }
}
