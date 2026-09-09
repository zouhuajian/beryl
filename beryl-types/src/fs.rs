// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared filesystem attributes, write modes, and content generations.
//!
//! Metadata owns persisted inode state; these domain values remain independent
//! of transport (gRPC/proto) and storage (RocksDB) layers.

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};
use thiserror::Error;

/// Maximum logical block capacity, independent of transport buffering and local encoding.
/// Lowering this persisted limit requires an explicit data migration.
pub const MAX_BLOCK_SIZE: u32 = 1024 * 1024 * 1024;

/// Validate a persisted or wire capacity before narrowing it to the inode's u32 field.
/// This does not validate readable lengths, checkpoints, or writer authority.
pub fn validate_block_size(block_size: u64) -> std::result::Result<(), BlockSizeError> {
    if block_size == 0 {
        return Err(BlockSizeError::ZeroBlockSize);
    }
    if block_size > u64::from(MAX_BLOCK_SIZE) {
        return Err(BlockSizeError::BlockTooLarge {
            actual: block_size,
            maximum: u64::from(MAX_BLOCK_SIZE),
        });
    }
    Ok(())
}

/// Invalid logical capacity received from configuration, metadata, or a Worker request.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum BlockSizeError {
    #[error("block_size must be non-zero")]
    ZeroBlockSize,
    #[error("block_size {actual} exceeds maximum {maximum}")]
    BlockTooLarge { actual: u64, maximum: u64 },
}

/// Validate a non-empty block prefix against its capacity.
///
/// The caller must validate the block capacity separately. Zero-length durable
/// checkpoints are valid storage state and do not use this check.
pub fn validate_effective_len(block_size: u64, effective_len: u64) -> std::result::Result<(), BlockLengthError> {
    if effective_len == 0 {
        return Err(BlockLengthError::ZeroEffectiveLen);
    }
    if effective_len > block_size {
        return Err(BlockLengthError::EffectiveLenExceedsBlock);
    }
    Ok(())
}

/// A nonempty read or completed write prefix falls outside its block capacity.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum BlockLengthError {
    #[error("effective_len must be non-zero")]
    ZeroEffectiveLen,
    #[error("effective_len must not exceed block_size")]
    EffectiveLenExceedsBlock,
}

/// Largest number of blocks stored in one file inode by the inline layout.
///
/// This fixed ceiling bounds replicated publication and inode serialization.
/// Files that need more blocks require paged block storage rather than a
/// larger inline vector.
pub const MAX_FILE_BLOCKS: usize = 10_000;

/// Payload-free namespace type tag, independent of inode storage layout.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    /// Regular file.
    File,
    /// Directory.
    Dir,
}

impl FileType {
    /// Returns true if this is a directory.
    #[inline]
    pub fn is_dir(self) -> bool {
        matches!(self, FileType::Dir)
    }

    /// Returns true if this is a file.
    #[inline]
    pub fn is_file(self) -> bool {
        matches!(self, FileType::File)
    }
}

/// Change counter for the currently visible content of one inode.
///
/// Compare generations only within the same inode. Metadata advances this value
/// when visible content changes; zero is the initial generation. It does not
/// identify retained historical data, a writer lease, or a physical block.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentGeneration(u64);

impl ContentGeneration {
    /// Wraps a generation from persisted state or a protocol boundary.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the scalar used by storage and wire encodings.
    pub const fn as_raw(self) -> u64 {
        self.0
    }

    /// Returns the next generation, or None on exhaustion; never wraps.
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl Display for ContentGeneration {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.0.fmt(f)
    }
}

/// Write behavior selected when opening a file session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Replace the currently visible file contents when publication succeeds.
    Overwrite,
    /// Append after the currently visible file contents.
    Append,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::LeaseEpoch;

    #[test]
    fn block_capacity_and_nonempty_prefix_boundaries() {
        for capacity in [1, u64::from(MAX_BLOCK_SIZE)] {
            validate_block_size(capacity).unwrap();
            for len in [1, capacity] {
                validate_effective_len(capacity, len).unwrap();
            }
            assert_eq!(
                validate_effective_len(capacity, 0),
                Err(BlockLengthError::ZeroEffectiveLen)
            );
            assert_eq!(
                validate_effective_len(capacity, capacity + 1),
                Err(BlockLengthError::EffectiveLenExceedsBlock)
            );
        }
        assert_eq!(validate_block_size(0), Err(BlockSizeError::ZeroBlockSize));
        for actual in [u64::from(MAX_BLOCK_SIZE) + 1, u64::MAX] {
            assert_eq!(
                validate_block_size(actual),
                Err(BlockSizeError::BlockTooLarge {
                    actual,
                    maximum: u64::from(MAX_BLOCK_SIZE),
                })
            );
        }
    }

    #[test]
    fn file_counters_preserve_scalar_encoding_and_never_wrap() {
        for raw in [0, 1, u64::MAX - 1, u64::MAX] {
            let generation = ContentGeneration::new(raw);
            let epoch = LeaseEpoch::new(raw);
            let encoded = raw.to_string();
            assert_eq!(serde_json::to_string(&generation).unwrap(), encoded);
            assert_eq!(serde_json::to_string(&epoch).unwrap(), encoded);
            assert_eq!(serde_json::from_str::<ContentGeneration>(&encoded).unwrap(), generation);
            assert_eq!(serde_json::from_str::<LeaseEpoch>(&encoded).unwrap(), epoch);
            assert_eq!(
                generation.checked_next().map(ContentGeneration::as_raw),
                raw.checked_add(1)
            );
            assert_eq!(epoch.checked_next().map(LeaseEpoch::as_raw), raw.checked_add(1));
        }
    }
}
