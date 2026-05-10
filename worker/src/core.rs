// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker core domain types and data-plane boundaries.

use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use types::chunk::ByteRange;
use types::ids::{BlockId, ChunkIndex, StreamId};
use types::lease::FencingToken;

use crate::error::WorkerError;

pub type WorkerCoreResult<T> = Result<T, WorkerError>;

/// Stream operation mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamMode {
    Read,
    Write,
}

/// Stream context established by a successful open operation.
#[derive(Clone, Debug)]
pub struct StreamContext {
    pub stream_id: StreamId,
    pub block_id: BlockId,
    pub mode: StreamMode,
    /// Transport payload size for stream frames, not the worker-local StorageChunk size.
    pub frame_size: u32,
    pub window_bytes: u32,
    /// Worker-observed logical block stamp for this stream context.
    pub block_stamp: u64,
    /// Block-local readable committed prefix, not the sum of ready chunks.
    pub committed_length: u64,
    pub byte_range: Option<ByteRange>,
    pub last_activity: Instant,
}

/// Open-read request in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOpenRequest {
    pub block_id: BlockId,
    /// Block-local byte range; callers must not pass file-level offsets.
    pub byte_range: ByteRange,
    /// Client-observed block stamp. Zero means stamp validation is skipped.
    pub block_stamp: u64,
    /// Requested transport payload size, not the worker-local StorageChunk size.
    pub frame_size: u32,
}

/// Open-read result in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOpenResult {
    pub stream_id: StreamId,
    pub frame_size: u32,
    pub window_bytes: u32,
    /// Current worker-observed logical block stamp.
    pub block_stamp: u64,
    /// Block-local readable committed prefix, not the sum of ready chunks.
    pub committed_length: u64,
    pub chunk_size: u32,
}

/// Open-write request in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOpenRequest {
    pub block_id: BlockId,
    pub token: FencingToken,
    /// Client-observed block stamp. Zero means stamp validation is skipped.
    pub block_stamp: u64,
    /// Requested transport payload size, not the worker-local StorageChunk size.
    pub frame_size: u32,
}

/// Open-write result in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOpenResult {
    pub stream_id: StreamId,
    pub frame_size: u32,
    pub window_bytes: u32,
    /// Current worker-observed logical block stamp.
    pub block_stamp: u64,
    /// Block-local readable committed prefix, not the sum of ready chunks.
    pub committed_length: u64,
    pub chunk_size: u32,
}

/// Transport payload returned by a read stream; this is not a StorageChunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadFrame {
    pub offset_in_block: u64,
    pub data: Bytes,
    pub checksum32: u32,
    pub eos: bool,
}

/// Transport payload accepted by a write stream; this is not a StorageChunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteFrame {
    pub stream_id: StreamId,
    pub seq: u64,
    pub offset_in_block: u64,
    pub data: Bytes,
    pub checksum32: u32,
}

/// Commit request for a write stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitWriteRequest {
    pub stream_id: StreamId,
    pub block_id: BlockId,
    pub token: FencingToken,
    pub commit_seq: u64,
    /// Readable prefix after commit.
    pub committed_length: u64,
    pub require_sync: bool,
}

/// Commit result for a write stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitWriteResult {
    /// Readable prefix after commit.
    pub committed_length: u64,
    pub block_stamp: u64,
    /// Highest byte offset known to be persisted by the worker.
    pub persisted_through: u64,
}

/// Abort request for a write stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortWriteRequest {
    pub stream_id: StreamId,
    pub block_id: BlockId,
    pub token: FencingToken,
    pub reason: String,
}

/// Abort result for a write stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortWriteResult {
    pub aborted: bool,
}

/// Block-local slice inside a worker-local StorageChunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageChunkSlice {
    pub chunk_index: ChunkIndex,
    pub offset_in_chunk: u32,
    pub len: u32,
}

/// Unique entry point for mapping block-local byte ranges to StorageChunk slices.
pub struct RangeMapper;

impl RangeMapper {
    pub fn map_range(byte_range: ByteRange, chunk_size: u32) -> WorkerCoreResult<Vec<StorageChunkSlice>> {
        if chunk_size == 0 {
            return Err(WorkerError::InvalidArgument(
                "chunk_size must be greater than zero".to_string(),
            ));
        }

        if byte_range.len == 0 {
            return Ok(Vec::new());
        }

        let chunk_size_u64 = chunk_size as u64;
        let first_offset = byte_range.offset % chunk_size_u64;
        let estimated = (first_offset + byte_range.len as u64).div_ceil(chunk_size_u64) as usize;
        let mut slices = Vec::with_capacity(estimated);
        let mut current_offset = byte_range.offset;
        let mut remaining = byte_range.len;

        while remaining > 0 {
            let raw_chunk_index = current_offset / chunk_size_u64;
            let chunk_index = u32::try_from(raw_chunk_index)
                .map_err(|_| WorkerError::InvalidArgument("chunk index exceeds u32".to_string()))?;
            let offset_in_chunk = (current_offset % chunk_size_u64) as u32;
            let available = chunk_size - offset_in_chunk;
            let len = remaining.min(available);

            slices.push(StorageChunkSlice {
                chunk_index: ChunkIndex::new(chunk_index),
                offset_in_chunk,
                len,
            });

            remaining -= len;
            current_offset = current_offset
                .checked_add(len as u64)
                .ok_or_else(|| WorkerError::InvalidArgument("byte range offset overflow".to_string()))?;
        }

        Ok(slices)
    }
}

/// Minimal block manager boundary needed by the stream data-plane core.
#[async_trait]
pub trait BlockManagerCore: Send + Sync {
    async fn open_read(&self, req: ReadOpenRequest) -> WorkerCoreResult<ReadOpenResult>;
    async fn open_write(&self, req: WriteOpenRequest) -> WorkerCoreResult<WriteOpenResult>;
    async fn commit_write(&self, req: CommitWriteRequest) -> WorkerCoreResult<CommitWriteResult>;
}

/// Minimal block storage boundary. Offsets are block-local byte offsets.
#[async_trait]
pub trait BlockStoreCore: Send + Sync {
    async fn read_at(&self, block_id: BlockId, offset: u64, len: u32) -> WorkerCoreResult<Bytes>;
    async fn write_at(&self, block_id: BlockId, offset: u64, data: Bytes) -> WorkerCoreResult<()>;
    async fn sync_block(&self, block_id: BlockId) -> WorkerCoreResult<u64>;
}

/// Data-plane core entry point used by service adapters.
#[derive(Clone, Debug)]
pub struct WorkerCore {
    chunk_size: u32,
}

impl WorkerCore {
    pub const fn new(chunk_size: u32) -> Self {
        Self { chunk_size }
    }

    pub const fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    pub async fn open_read(&self, _req: ReadOpenRequest) -> WorkerCoreResult<ReadOpenResult> {
        Err(Self::not_implemented("OpenReadStream"))
    }

    pub async fn open_write(&self, _req: WriteOpenRequest) -> WorkerCoreResult<WriteOpenResult> {
        Err(Self::not_implemented("OpenWriteStream"))
    }

    pub async fn commit_write(&self, _req: CommitWriteRequest) -> WorkerCoreResult<CommitWriteResult> {
        Err(Self::not_implemented("CommitWrite"))
    }

    pub async fn abort_write(&self, _req: AbortWriteRequest) -> WorkerCoreResult<AbortWriteResult> {
        Err(Self::not_implemented("AbortWrite"))
    }

    pub async fn read_frame(&self, _stream_id: StreamId, _max_bytes: u32) -> WorkerCoreResult<ReadFrame> {
        Err(Self::not_implemented("ReadStream"))
    }

    pub async fn write_frame(&self, _frame: WriteFrame) -> WorkerCoreResult<()> {
        Err(Self::not_implemented("WriteStream"))
    }

    fn not_implemented(operation: &'static str) -> WorkerError {
        WorkerError::Unimplemented(format!("{operation} worker core is not implemented"))
    }
}

#[async_trait]
impl BlockManagerCore for WorkerCore {
    async fn open_read(&self, req: ReadOpenRequest) -> WorkerCoreResult<ReadOpenResult> {
        WorkerCore::open_read(self, req).await
    }

    async fn open_write(&self, req: WriteOpenRequest) -> WorkerCoreResult<WriteOpenResult> {
        WorkerCore::open_write(self, req).await
    }

    async fn commit_write(&self, req: CommitWriteRequest) -> WorkerCoreResult<CommitWriteResult> {
        WorkerCore::commit_write(self, req).await
    }
}
