// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Local block storage boundary.

use bytes::Bytes;
use types::ids::BlockId;

use crate::data::core::WorkerCoreResult;
use crate::error::WorkerError;

/// Placeholder for worker-local block storage.
///
/// The concrete file-backed store will own persisted block metadata, byte IO,
/// checksum state, and valid-bitmaps. This type intentionally performs no IO.
#[derive(Clone, Debug)]
pub struct BlockStore {
    /// Worker-local StorageChunk size.
    /// This is the IO/checksum/valid-bitmap granularity, not a transport frame size.
    chunk_size: u32,
}

impl BlockStore {
    pub const fn new(chunk_size: u32) -> Self {
        Self { chunk_size }
    }

    pub const fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// Read at a block-local offset.
    pub async fn read_at(&self, _block_id: BlockId, _offset: u64, _len: u32) -> WorkerCoreResult<Bytes> {
        Err(Self::not_implemented("BlockStore::read_at"))
    }

    /// Write at a block-local offset.
    pub async fn write_at(&self, _block_id: BlockId, _offset: u64, _data: Bytes) -> WorkerCoreResult<()> {
        Err(Self::not_implemented("BlockStore::write_at"))
    }

    /// Persist pending local data for a block.
    pub async fn sync_block(&self, _block_id: BlockId) -> WorkerCoreResult<u64> {
        Err(Self::not_implemented("BlockStore::sync_block"))
    }

    fn not_implemented(operation: &'static str) -> WorkerError {
        WorkerError::Unimplemented(format!("{operation} is not implemented"))
    }
}
