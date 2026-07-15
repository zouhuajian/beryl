// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use crate::ids::BlockId;
use crate::layout::FileLayout;
use bytes::Bytes;
use core::fmt;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkRef {
    pub block_id: BlockId,
    pub chunk_idx: u32,
}

impl ChunkRef {
    #[inline]
    pub const fn new(block_id: BlockId, chunk_idx: u32) -> Self {
        Self { block_id, chunk_idx }
    }
}

impl fmt::Display for ChunkRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.block_id, self.chunk_idx)
    }
}

/// Byte range within a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ByteRange {
    pub offset: u64,
    pub len: u32,
}

/// Slice inside a chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkSlice {
    pub chunk: ChunkRef,
    pub offset_in_chunk: u32,
    pub len: u32,
}

/// A compact presence summary for chunks within a block.
/// - Meta should store only this summary (weakly-consistent).
/// - Workers store the authoritative per-chunk index locally.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkBitmap {
    /// bits[i] stores 64 chunks.
    pub bits: Vec<u64>,
}

impl ChunkBitmap {
    pub fn with_capacity_for(layout: &FileLayout) -> Self {
        let n = layout.chunks_per_block() as usize;
        let words = n.div_ceil(64);
        Self { bits: vec![0; words] }
    }

    #[inline]
    pub fn set(&mut self, chunk_idx: u32) {
        let i = chunk_idx as usize;
        let w = i / 64;
        let b = i % 64;
        if w < self.bits.len() {
            self.bits[w] |= 1u64 << b;
        }
    }

    #[inline]
    pub fn test(&self, chunk_idx: u32) -> bool {
        let i = chunk_idx as usize;
        let w = i / 64;
        let b = i % 64;
        self.bits.get(w).map(|v| (v >> b) & 1 == 1).unwrap_or(false)
    }
}

/// Data payload wrapper used at the domain layer (transport-agnostic).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkData {
    pub slice: ChunkSlice,
    pub data: Bytes,
    pub checksum32: u32, // optional; CRC32C recommended in impl layer
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout_with_chunks(chunks: u32) -> FileLayout {
        FileLayout::new(chunks * 4096, 4096, 1)
    }

    #[test]
    fn chunk_bitmap_sizes_words_by_chunks_per_block_boundaries() {
        assert_eq!(ChunkBitmap::with_capacity_for(&layout_with_chunks(0)).bits.len(), 0);
        assert_eq!(ChunkBitmap::with_capacity_for(&layout_with_chunks(1)).bits.len(), 1);
        assert_eq!(ChunkBitmap::with_capacity_for(&layout_with_chunks(64)).bits.len(), 1);
        assert_eq!(ChunkBitmap::with_capacity_for(&layout_with_chunks(65)).bits.len(), 2);
    }
}
