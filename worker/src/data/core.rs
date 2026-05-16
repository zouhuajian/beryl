// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker core domain types and data-plane facade.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use types::chunk::ByteRange;
use types::ids::{BlockId, ChunkIndex, ShardGroupId, StreamId};
use types::lease::FencingToken;

use crate::config::WorkerConfig;
use crate::error::WorkerError;
use crate::runtime::block::BlockManager;
use crate::runtime::stream::{StreamManager, StreamState};
use crate::store::block::{FullBlockFileStore, FullBlockFileStoreConfig, LocalBlockStore};

pub type WorkerCoreResult<T> = Result<T, WorkerError>;

/// Stream operation mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamMode {
    Read,
    Write,
}

/// Stream context established at open time.
#[derive(Clone, Debug)]
pub struct StreamContext {
    pub stream_id: StreamId,
    pub group_id: ShardGroupId,
    pub block_id: BlockId,
    pub mode: StreamMode,
    /// First block-local byte offset in this stream.
    pub start_offset: u64,
    /// Exclusive block-local byte offset where this stream stops.
    pub end_offset: u64,
    /// Transport frame payload size negotiated at stream open.
    /// This controls network batching and does not define StorageChunk size.
    pub frame_size: u32,
    /// Per-stream application-level in-flight byte window.
    /// This is independent from protocol-native flow control.
    pub window_bytes: u32,
    /// Logical block stamp used for direct read/write validation.
    /// It changes on logical commit or block metadata changes, not on ordinary reads.
    pub block_stamp: u64,
    /// Block-local readable committed prefix length.
    /// This is not the sum of ready chunks.
    pub committed_length: u64,
    /// Final Ready block length read from local metadata.
    pub effective_block_len: u64,
    /// Fencing token bound during write open. Read streams do not carry one.
    pub fencing_token: Option<FencingToken>,
}

/// Open-read request in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOpenRequest {
    pub group_id: ShardGroupId,
    pub block_id: BlockId,
    /// Block-local byte range. The offset is relative to block_id, not to the file.
    pub byte_range: ByteRange,
    /// Logical block stamp used for direct read validation.
    /// 0 means stamp validation is skipped.
    pub block_stamp: u64,
    /// Requested transport frame payload size, not the worker-local StorageChunk size.
    pub frame_size: u32,
}

/// Open-read result in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOpenResult {
    pub stream_id: StreamId,
    /// Transport frame payload size negotiated at stream open.
    /// This controls network batching and does not define StorageChunk size.
    pub frame_size: u32,
    /// Per-stream application-level in-flight byte window.
    /// This is independent from protocol-native flow control.
    pub window_bytes: u32,
    /// Logical block stamp used for direct read validation.
    pub block_stamp: u64,
    /// Block-local readable committed prefix length.
    /// This is not the sum of ready chunks.
    pub committed_length: u64,
    /// Worker-local StorageChunk size.
    /// This is the IO/checksum/valid-bitmap granularity, not a transport frame size.
    pub chunk_size: u32,
}

/// Open-write request in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOpenRequest {
    pub block_id: BlockId,
    pub token: FencingToken,
    /// Logical block stamp used for direct write validation.
    /// 0 means stamp validation is skipped.
    pub block_stamp: u64,
    /// Requested transport frame payload size, not the worker-local StorageChunk size.
    pub frame_size: u32,
}

/// Open-write result in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOpenResult {
    pub stream_id: StreamId,
    /// Transport frame payload size negotiated at stream open.
    pub frame_size: u32,
    /// Per-stream application-level in-flight byte window.
    pub window_bytes: u32,
    /// Logical block stamp used for direct write validation.
    pub block_stamp: u64,
    /// Block-local readable committed prefix length.
    /// This is not the sum of ready chunks.
    pub committed_length: u64,
    /// Worker-local StorageChunk size.
    /// This is the IO/checksum/valid-bitmap granularity, not a transport frame size.
    pub chunk_size: u32,
}

/// Transport payload returned by a read stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadFrame {
    pub offset_in_block: u64,
    pub data: Bytes,
    pub checksum32: u32,
    pub eos: bool,
}

/// Transport payload accepted by a write stream.
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
    /// Block-local readable committed prefix length.
    /// This is not the sum of ready chunks.
    pub committed_length: u64,
    pub require_sync: bool,
}

/// Commit result for a write stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitWriteResult {
    /// Block-local readable committed prefix length.
    pub committed_length: u64,
    /// Logical block stamp after commit.
    pub block_stamp: u64,
    /// Highest block-local byte offset known to be persisted by the worker.
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

/// A worker-local slice within a StorageChunk.
/// This is an execution granularity inside block operations, not a repair task unit.
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

/// Data-plane core entry point used by service adapters.
#[derive(Clone)]
pub struct WorkerCore {
    stream_manager: Arc<StreamManager>,
    block_manager: Arc<BlockManager>,
    block_store: Arc<dyn LocalBlockStore + Send + Sync>,
    default_chunk_size: u32,
    next_stream_seq: Arc<AtomicU64>,
}

impl WorkerCore {
    pub fn new(chunk_size: u32) -> Self {
        Self::with_options(
            chunk_size,
            BlockManager::DEFAULT_FRAME_SIZE,
            BlockManager::MAX_FRAME_SIZE,
            BlockManager::DEFAULT_WINDOW_BYTES,
            Duration::from_secs(60),
            WorkerConfig::default().storage_root,
        )
    }

    pub fn with_options(
        chunk_size: u32,
        default_frame_size: u32,
        max_frame_size: u32,
        window_bytes: u32,
        stream_idle_timeout: Duration,
        storage_root: PathBuf,
    ) -> Self {
        let block_store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(storage_root)));
        Self::with_local_store(
            chunk_size,
            default_frame_size,
            max_frame_size,
            window_bytes,
            stream_idle_timeout,
            block_store,
        )
    }

    pub fn with_local_store(
        chunk_size: u32,
        default_frame_size: u32,
        max_frame_size: u32,
        window_bytes: u32,
        stream_idle_timeout: Duration,
        block_store: Arc<dyn LocalBlockStore + Send + Sync>,
    ) -> Self {
        let block_manager = Arc::new(BlockManager::new(default_frame_size, max_frame_size, window_bytes));
        Self {
            stream_manager: Arc::new(StreamManager::new(stream_idle_timeout)),
            block_manager,
            block_store,
            default_chunk_size: chunk_size,
            next_stream_seq: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn chunk_size(&self) -> u32 {
        self.default_chunk_size
    }

    pub fn default_frame_size(&self) -> u32 {
        self.block_manager.default_frame_size()
    }

    pub fn max_frame_size(&self) -> u32 {
        self.block_manager.max_frame_size()
    }

    pub fn window_bytes(&self) -> u32 {
        self.block_manager.window_bytes()
    }

    pub fn stream_manager(&self) -> Arc<StreamManager> {
        Arc::clone(&self.stream_manager)
    }

    pub async fn open_read(&self, req: ReadOpenRequest) -> WorkerCoreResult<ReadOpenResult> {
        let frame_size = self.negotiate_frame_size(req.frame_size)?;
        let snapshot = self.block_manager.validate_read(self.block_store.as_ref(), &req)?;
        let stream_id = self.next_stream_id()?;
        let end_offset = req
            .byte_range
            .offset
            .checked_add(u64::from(req.byte_range.len))
            .ok_or_else(|| WorkerError::InvalidArgument("byte range offset overflow".to_string()))?;

        let context = StreamContext {
            stream_id,
            group_id: snapshot.group_id,
            block_id: snapshot.block_id,
            mode: StreamMode::Read,
            start_offset: req.byte_range.offset,
            end_offset,
            frame_size,
            window_bytes: self.window_bytes(),
            block_stamp: snapshot.block_stamp,
            committed_length: snapshot.effective_block_len,
            effective_block_len: snapshot.effective_block_len,
            fencing_token: None,
        };
        self.stream_manager.register(StreamState::new(context)).await;

        Ok(ReadOpenResult {
            stream_id,
            frame_size,
            window_bytes: self.window_bytes(),
            block_stamp: snapshot.block_stamp,
            committed_length: snapshot.effective_block_len,
            chunk_size: snapshot.chunk_size,
        })
    }

    pub async fn open_write(&self, req: WriteOpenRequest) -> WorkerCoreResult<WriteOpenResult> {
        self.block_manager.open_write(req).await
    }

    pub async fn commit_write(&self, req: CommitWriteRequest) -> WorkerCoreResult<CommitWriteResult> {
        self.block_manager.commit_write(req).await
    }

    pub async fn abort_write(&self, req: AbortWriteRequest) -> WorkerCoreResult<AbortWriteResult> {
        self.block_manager.abort_write(req).await
    }

    pub async fn read_frame(&self, stream_id: StreamId, max_bytes: u32) -> WorkerCoreResult<Vec<ReadFrame>> {
        self.read_stream(stream_id, max_bytes).await
    }

    pub async fn read_stream(&self, stream_id: StreamId, max_bytes: u32) -> WorkerCoreResult<Vec<ReadFrame>> {
        let state = self
            .stream_manager
            .get(stream_id)
            .await
            .ok_or_else(|| WorkerError::NotFound(format!("read stream not found: stream_id={stream_id}")))?;
        if state.context.mode != StreamMode::Read {
            return Err(WorkerError::InvalidArgument(format!(
                "stream is not a read stream: stream_id={stream_id}"
            )));
        }
        if state.cursor >= state.context.end_offset {
            self.stream_manager.remove(stream_id).await;
            return Ok(Vec::new());
        }

        let frame_budget = if max_bytes == 0 {
            state.context.frame_size
        } else {
            max_bytes.min(state.context.frame_size)
        };
        if frame_budget == 0 {
            return Err(WorkerError::InvalidArgument(
                "read stream frame budget must be greater than zero".to_string(),
            ));
        }

        let remaining = state.context.end_offset - state.cursor;
        let read_len = remaining.min(u64::from(frame_budget));
        let data = self
            .block_store
            .read_at(state.context.group_id, state.context.block_id, state.cursor, read_len)?;
        let next_cursor = state
            .cursor
            .checked_add(
                u64::try_from(data.len())
                    .map_err(|_| WorkerError::InvalidArgument("read frame length does not fit in u64".to_string()))?,
            )
            .ok_or_else(|| WorkerError::InvalidArgument("read cursor overflow".to_string()))?;
        let eos = next_cursor >= state.context.end_offset;
        let frame = ReadFrame {
            offset_in_block: state.cursor,
            data,
            checksum32: 0,
            eos,
        };
        if eos {
            self.stream_manager.remove(stream_id).await;
        } else {
            self.stream_manager.update_cursor(stream_id, next_cursor).await;
        }
        Ok(vec![frame])
    }

    pub async fn write_frame(&self, frame: WriteFrame) -> WorkerCoreResult<()> {
        self.write_stream(frame).await
    }

    pub async fn write_stream(&self, _frame: WriteFrame) -> WorkerCoreResult<()> {
        Err(Self::not_implemented("WriteStream"))
    }

    fn not_implemented(operation: &'static str) -> WorkerError {
        WorkerError::Unimplemented(format!("{operation} worker core is not implemented"))
    }

    fn negotiate_frame_size(&self, requested_frame_size: u32) -> WorkerCoreResult<u32> {
        let mut frame_size = if requested_frame_size == 0 {
            self.default_frame_size()
        } else {
            requested_frame_size
        };
        frame_size = frame_size.min(self.max_frame_size());
        if frame_size == 0 {
            return Err(WorkerError::InvalidArgument(
                "frame_size must be greater than zero after negotiation".to_string(),
            ));
        }
        Ok(frame_size)
    }

    fn next_stream_id(&self) -> WorkerCoreResult<StreamId> {
        let seq = self.next_stream_seq.fetch_add(1, Ordering::Relaxed);
        if seq == u64::MAX {
            return Err(WorkerError::ResourceExhausted(
                "stream id sequence exhausted".to_string(),
            ));
        }
        Ok(StreamId::new(u128::from(seq)))
    }
}
