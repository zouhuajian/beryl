// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker core domain types and data-plane facade.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_types::chunk::ByteRange;
use beryl_types::ids::{BlockId, ChunkIndex, StreamId};
use beryl_types::layout::{BlockFormatId, BlockShape, BlockShapeError};
use beryl_types::lease::FencingToken;
use beryl_types::{GroupName, Tier, WorkerRunId};
use bytes::Bytes;

use crate::error::WorkerError;
use crate::observe;
use crate::runtime::block::BlockManager;
use crate::runtime::stream::{StreamManager, StreamState};
use crate::store::block::{
    BlockState, ChecksumKind, CreateStagingBlockRequest, FullBlockFileStore, FullBlockFileStoreConfig, LocalBlockStore,
    PublishReadyRequest, SyncReadyBlockRequest,
};

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
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub mode: StreamMode,
    pub worker_run_id: WorkerRunId,
    /// First block-local byte offset in this stream.
    pub start_offset: u64,
    /// Exclusive block-local byte offset where this stream stops.
    pub end_offset: u64,
    /// Transport frame payload size negotiated at stream open.
    /// This controls network batching and does not define StorageChunk size.
    pub frame_size: u32,
    /// Logical block stamp used for direct read/write validation.
    /// It changes on logical commit or block metadata changes, not on ordinary reads.
    pub block_stamp: u64,
    pub block_format_id: BlockFormatId,
    pub block_size: u64,
    pub chunk_size: u32,
    /// Block-local readable committed prefix length.
    /// This is not the sum of ready chunks.
    pub committed_length: u64,
    /// Block-local valid length for reads, or the full write bound before commit.
    ///
    /// Write commits publish their final valid length through
    /// `CommitWriteRequest.effective_len`.
    pub effective_len: u64,
    /// Fencing token bound during write open. Read streams do not carry one.
    pub fencing_token: Option<FencingToken>,
}

/// Open-read request in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOpenRequest {
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub worker_run_id: WorkerRunId,
    /// Block-local byte range. The offset is relative to block_id, not to the file.
    pub byte_range: ByteRange,
    /// Logical block stamp used for direct read validation.
    /// Normal client reads must use a non-zero metadata-authoritative stamp.
    /// Public worker read opens reject 0 before local block metadata lookup.
    pub block_stamp: u64,
    pub block_format_id: BlockFormatId,
    pub block_size: u64,
    pub chunk_size: u32,
    pub effective_len: u64,
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
    /// Logical block stamp used for direct read validation.
    pub block_stamp: u64,
    /// Block-local readable committed prefix length.
    /// This is not the sum of ready chunks.
    pub committed_length: u64,
}

/// Open-write request in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOpenRequest {
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub worker_run_id: WorkerRunId,
    pub token: FencingToken,
    /// Logical block stamp used for direct write validation.
    /// Supplied by metadata for this block write plan.
    pub block_stamp: u64,
    /// Requested transport frame payload size, not the worker-local StorageChunk size.
    pub frame_size: u32,
    /// Full logical block size from the persisted FileLayout.
    ///
    /// The worker persists this value in BlockMeta.format.block_size. Tail or
    /// bounded valid length is carried later by CommitWrite.effective_len.
    pub block_size: u64,
    /// Metadata-selected Beryl block data/meta interpretation format.
    pub block_format_id: BlockFormatId,
    pub chunk_size: u32,
    pub effective_len: u64,
    pub checksum_kind: ChecksumKind,
    pub tier: Tier,
}

/// Open-write result in worker core terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOpenResult {
    pub stream_id: StreamId,
    /// Transport frame payload size negotiated at stream open.
    pub frame_size: u32,
    /// Logical block stamp used for direct write validation.
    pub block_stamp: u64,
    /// Published effective length reported to the caller.
    /// For a newly opened staging block this is zero until CommitWrite publishes Ready metadata.
    pub committed_length: u64,
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

/// Result for one accepted or rejected write frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteFrameResult {
    pub accepted: bool,
    pub last_acked_seq: u64,
    /// Contiguous byte prefix written into the staging block.
    /// This is not readable until final metadata is published.
    pub written_through: u64,
}

/// Commit request for a write stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitWriteRequest {
    pub stream_id: StreamId,
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub worker_run_id: WorkerRunId,
    pub token: FencingToken,
    pub commit_seq: u64,
    /// Complete effective block length to publish.
    pub effective_len: u64,
    /// Metadata-assigned logical block stamp to persist at publish time.
    pub block_stamp: u64,
    pub block_format_id: BlockFormatId,
    pub block_size: u64,
    pub chunk_size: u32,
    pub require_sync: bool,
}

/// Commit result for a write stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitWriteResult {
    /// Complete effective block length published as Ready.
    pub effective_len: u64,
    /// Logical block stamp after commit.
    pub block_stamp: u64,
    /// Contiguous byte prefix written into the staging block.
    /// This is not readable until final metadata is published.
    pub written_through: u64,
}

/// Durable sync request for an already committed block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncCommittedBlockRequest {
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub worker_run_id: WorkerRunId,
    /// Metadata-authoritative block_stamp for the committed block version.
    pub block_stamp: u64,
    /// Complete committed block length expected by the metadata-visible prefix.
    pub expected_block_len: u64,
    pub block_format_id: BlockFormatId,
    pub block_size: u64,
    pub chunk_size: u32,
}

/// Durable sync result for an already committed block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncCommittedBlockResult {
    pub effective_len: u64,
    pub block_stamp: u64,
}

/// Abort request for a write stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortWriteRequest {
    pub stream_id: StreamId,
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub token: FencingToken,
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
    next_stream_seq: Arc<AtomicU64>,
}

impl WorkerCore {
    pub fn with_options(
        default_frame_size: u32,
        max_frame_size: u32,
        stream_idle_timeout: Duration,
        store_dir: PathBuf,
    ) -> Self {
        let block_store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(store_dir)));
        Self::with_local_store(default_frame_size, max_frame_size, stream_idle_timeout, block_store)
    }

    pub fn with_local_store(
        default_frame_size: u32,
        max_frame_size: u32,
        stream_idle_timeout: Duration,
        block_store: Arc<dyn LocalBlockStore + Send + Sync>,
    ) -> Self {
        let block_manager = Arc::new(BlockManager::new(default_frame_size, max_frame_size));
        Self {
            stream_manager: Arc::new(StreamManager::new(stream_idle_timeout)),
            block_manager,
            block_store,
            next_stream_seq: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn default_frame_size(&self) -> u32 {
        self.block_manager.default_frame_size()
    }

    pub fn max_frame_size(&self) -> u32 {
        self.block_manager.max_frame_size()
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
            group_name: snapshot.group_name,
            block_id: snapshot.block_id,
            mode: StreamMode::Read,
            worker_run_id: req.worker_run_id,
            start_offset: req.byte_range.offset,
            end_offset,
            frame_size,
            block_stamp: snapshot.block_stamp,
            block_format_id: snapshot.block_format_id,
            block_size: snapshot.block_size,
            chunk_size: snapshot.chunk_size,
            committed_length: snapshot.effective_len,
            effective_len: snapshot.effective_len,
            fencing_token: None,
        };
        self.stream_manager.register(StreamState::new(context)).await;

        Ok(ReadOpenResult {
            stream_id,
            frame_size,
            block_stamp: snapshot.block_stamp,
            committed_length: snapshot.effective_len,
        })
    }

    pub async fn open_write(&self, req: WriteOpenRequest) -> WorkerCoreResult<WriteOpenResult> {
        let group_name = req.group_name.clone();
        let block_id = req.block_id;
        let worker_run_id = req.worker_run_id;
        let data_handle_id = req.block_id.data_handle_id;
        let block_stamp = req.block_stamp;
        let result = async {
            let frame_size = self.negotiate_frame_size(req.frame_size)?;
            validate_write_open_request(&req)?;
            reject_existing_final_block(self.block_store.as_ref(), &req)?;
            let stream_id = self.next_stream_id()?;

            match self.block_store.create_staging_block(CreateStagingBlockRequest {
                group_name: req.group_name.clone(),
                block_id: req.block_id,
                block_size: req.block_size,
                block_format_id: req.block_format_id,
                chunk_size: req.chunk_size,
                checksum_kind: req.checksum_kind,
                tier: req.tier,
            }) {
                Ok(_) => tracing::info!(
                    target: "worker.block",
                    op = "CreateBlock",
                    result = "created",
                    error_code = "none",
                    group_id = %group_name,
                    block_id = %block_id,
                    data_handle_id = data_handle_id.as_raw(),
                    worker_run_id = %worker_run_id,
                    block_stamp,
                    "Block created"
                ),
                Err(error) => {
                    tracing::warn!(
                        target: "worker.block",
                        op = "CreateBlock",
                        result = "rejected",
                        error_code = observe::worker_error_kind(&error),
                        group_id = %group_name,
                        block_id = %block_id,
                        data_handle_id = data_handle_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        block_stamp,
                        "Block create rejected"
                    );
                    return Err(error);
                }
            }

            let context = StreamContext {
                stream_id,
                group_name: req.group_name,
                block_id: req.block_id,
                mode: StreamMode::Write,
                worker_run_id: req.worker_run_id,
                start_offset: 0,
                end_offset: req.effective_len,
                frame_size,
                block_stamp: req.block_stamp,
                block_format_id: req.block_format_id,
                block_size: req.block_size,
                chunk_size: req.chunk_size,
                committed_length: 0,
                effective_len: req.effective_len,
                fencing_token: Some(req.token),
            };
            self.stream_manager.register(StreamState::new(context)).await;

            Ok(WriteOpenResult {
                stream_id,
                frame_size,
                block_stamp: req.block_stamp,
                committed_length: 0,
            })
        }
        .await;
        match &result {
            Ok(opened) => tracing::info!(
                target: "worker.state",
                op = "OpenWrite",
                result = "accepted",
                error_code = "none",
                group_id = %group_name,
                block_id = %block_id,
                stream_id = %opened.stream_id,
                data_handle_id = data_handle_id.as_raw(),
                worker_run_id = %worker_run_id,
                block_stamp,
                committed_length = opened.committed_length,
                "OpenWrite accepted"
            ),
            Err(error) => tracing::warn!(
                target: "worker.state",
                op = "OpenWrite",
                result = "rejected",
                error_code = observe::worker_error_kind(error),
                group_id = %group_name,
                block_id = %block_id,
                data_handle_id = data_handle_id.as_raw(),
                worker_run_id = %worker_run_id,
                block_stamp,
                "OpenWrite rejected"
            ),
        }
        result
    }

    pub async fn commit_write(&self, req: CommitWriteRequest) -> WorkerCoreResult<CommitWriteResult> {
        let group_name = req.group_name.clone();
        let block_id = req.block_id;
        let stream_id = req.stream_id;
        let worker_run_id = req.worker_run_id;
        let data_handle_id = req.block_id.data_handle_id;
        let result = async {
            let state = self.write_state(req.stream_id).await?;
            validate_commit_request(&state, &req)?;

            // FullBlockFileStore publishes synchronously, so require_sync currently
            // selects the same conservative path as the default commit.
            let _require_sync = req.require_sync;
            let meta = match self.block_store.publish_ready(PublishReadyRequest {
                group_name: req.group_name,
                block_id: req.block_id,
                effective_len: req.effective_len,
                block_stamp: req.block_stamp,
            }) {
                Ok(meta) => {
                    tracing::info!(
                        target: "worker.block",
                        op = "publish_ready",
                        result = "completed",
                        error_code = "none",
                        group_id = %group_name,
                        block_id = %block_id,
                        stream_id = %stream_id,
                        data_handle_id = data_handle_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        committed_length = meta.source.effective_len,
                        ready_chunks = 1_u64,
                        corrupt_chunks = 0_u64,
                        block_stamp = meta.visibility.block_stamp,
                        "Block publish_ready completed"
                    );
                    meta
                }
                Err(error) => {
                    tracing::warn!(
                        target: "worker.block",
                        op = "publish_ready",
                        result = "rejected",
                        error_code = observe::worker_error_kind(&error),
                        group_id = %group_name,
                        block_id = %block_id,
                        stream_id = %stream_id,
                        data_handle_id = data_handle_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        "Block publish_ready rejected"
                    );
                    return Err(error);
                }
            };
            tracing::info!(
                target: "worker.state",
                op = "CommitWrite",
                result = "completed",
                error_code = "none",
                group_id = %group_name,
                block_id = %block_id,
                stream_id = %stream_id,
                data_handle_id = data_handle_id.as_raw(),
                worker_run_id = %worker_run_id,
                committed_length = meta.source.effective_len,
                bytes_written = meta.source.effective_len,
                block_stamp = meta.visibility.block_stamp,
                "CommitWrite completed"
            );
            self.stream_manager.remove(req.stream_id).await;

            Ok(CommitWriteResult {
                effective_len: meta.source.effective_len,
                block_stamp: meta.visibility.block_stamp,
                written_through: meta.source.effective_len,
            })
        }
        .await;
        if let Err(error) = &result {
            tracing::warn!(
                target: "worker.state",
                op = "CommitWrite",
                result = "rejected",
                error_code = observe::worker_error_kind(error),
                group_id = %group_name,
                block_id = %block_id,
                stream_id = %stream_id,
                data_handle_id = data_handle_id.as_raw(),
                worker_run_id = %worker_run_id,
                "CommitWrite rejected"
            );
        }
        result
    }

    pub async fn sync_committed_block(
        &self,
        req: SyncCommittedBlockRequest,
    ) -> WorkerCoreResult<SyncCommittedBlockResult> {
        validate_sync_committed_block_request(&req)?;
        let meta = match self.block_store.load_meta(&req.group_name, req.block_id) {
            Ok(meta) => meta,
            Err(WorkerError::NotFound(message)) => {
                return Err(WorkerError::RefreshMetadata {
                    kind: ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                    message: format!("local block is not available for durable sync: {message}"),
                });
            }
            Err(error) => return Err(error),
        };
        validate_sync_committed_block_meta(&req, &meta)?;
        let synced = self.block_store.sync_ready_block(SyncReadyBlockRequest {
            group_name: req.group_name.clone(),
            block_id: req.block_id,
        })?;
        validate_sync_committed_block_meta(&req, &synced)?;
        Ok(SyncCommittedBlockResult {
            effective_len: synced.source.effective_len,
            block_stamp: synced.visibility.block_stamp,
        })
    }

    pub async fn abort_write(&self, req: AbortWriteRequest) -> WorkerCoreResult<AbortWriteResult> {
        let state = self.write_state(req.stream_id).await?;
        validate_abort_request(&state, &req)?;
        self.stream_manager.remove(req.stream_id).await;
        self.block_store.abort_staging_block(&req.group_name, req.block_id)?;
        Ok(AbortWriteResult { aborted: true })
    }

    pub(crate) async fn abort_write_stream_after_error(&self, stream_id: StreamId) -> WorkerCoreResult<()> {
        let Some(state) = self.stream_manager.get(stream_id).await else {
            return Ok(());
        };
        if state.context.mode != StreamMode::Write {
            return Ok(());
        }
        self.stream_manager.remove(stream_id).await;
        self.block_store
            .abort_staging_block(&state.context.group_name, state.context.block_id)?;
        Ok(())
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
        let store_started = Instant::now();
        let data = match self.block_store.read_at(
            &state.context.group_name,
            state.context.block_id,
            state.cursor,
            read_len,
        ) {
            Ok(data) => {
                observe::record_store_io(
                    "read",
                    "ok",
                    "none",
                    data.len() as u64,
                    store_started.elapsed().as_secs_f64(),
                );
                data
            }
            Err(error) => {
                observe::record_store_io(
                    "read",
                    "error",
                    observe::worker_error_kind(&error),
                    0,
                    store_started.elapsed().as_secs_f64(),
                );
                self.stream_manager.remove(stream_id).await;
                return Err(error);
            }
        };
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

    pub async fn write_frame(&self, frame: WriteFrame) -> WorkerCoreResult<WriteFrameResult> {
        self.write_stream(frame).await
    }

    pub async fn write_stream(&self, frame: WriteFrame) -> WorkerCoreResult<WriteFrameResult> {
        let state = self.write_state(frame.stream_id).await?;
        let expected_seq = state
            .last_acked_seq
            .checked_add(1)
            .ok_or_else(|| WorkerError::InvalidArgument("write stream sequence overflow".to_string()))?;
        if frame.seq != expected_seq {
            return Ok(rejected_write_frame(&state));
        }
        if frame.offset_in_block != state.cursor {
            return Ok(rejected_write_frame(&state));
        }
        if frame.data.is_empty() {
            return Ok(rejected_write_frame(&state));
        }
        let len = u64::try_from(frame.data.len())
            .map_err(|_| WorkerError::InvalidArgument("write frame length does not fit in u64".to_string()))?;
        let written_through = frame
            .offset_in_block
            .checked_add(len)
            .ok_or_else(|| WorkerError::InvalidArgument("write frame offset overflow".to_string()))?;
        if written_through > state.context.end_offset {
            return Ok(rejected_write_frame(&state));
        }

        let store_started = Instant::now();
        match self.block_store.write_at(
            &state.context.group_name,
            state.context.block_id,
            frame.offset_in_block,
            frame.data,
        ) {
            Ok(()) => observe::record_store_io("write", "ok", "none", len, store_started.elapsed().as_secs_f64()),
            Err(error) => {
                observe::record_store_io(
                    "write",
                    "error",
                    observe::worker_error_kind(&error),
                    0,
                    store_started.elapsed().as_secs_f64(),
                );
                return Err(error);
            }
        }
        if !self
            .stream_manager
            .advance_write_progress(frame.stream_id, frame.seq, written_through)
            .await
        {
            return Err(WorkerError::Internal(format!(
                "write stream disappeared after local write: stream_id={}",
                frame.stream_id
            )));
        }
        Ok(WriteFrameResult {
            accepted: true,
            last_acked_seq: frame.seq,
            written_through,
        })
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

    async fn write_state(&self, stream_id: StreamId) -> WorkerCoreResult<StreamState> {
        let state = self
            .stream_manager
            .get(stream_id)
            .await
            .ok_or_else(|| WorkerError::NotFound(format!("write stream not found: stream_id={stream_id}")))?;
        if state.context.mode != StreamMode::Write {
            return Err(WorkerError::InvalidArgument(format!(
                "stream is not a write stream: stream_id={stream_id}"
            )));
        }
        Ok(state)
    }
}

fn validate_write_open_request(req: &WriteOpenRequest) -> WorkerCoreResult<()> {
    validate_fencing_token_shape(req.block_id, req.token)?;
    if req.block_stamp == 0 {
        return Err(WorkerError::InvalidArgument(
            "block_stamp must be metadata-assigned and non-zero".to_string(),
        ));
    }
    validate_block_shape(
        req.block_format_id,
        req.block_size,
        req.chunk_size,
        req.effective_len,
        req.checksum_kind,
    )?;
    Ok(())
}

fn validate_block_shape(
    block_format_id: BlockFormatId,
    block_size: u64,
    chunk_size: u32,
    effective_len: u64,
    checksum_kind: ChecksumKind,
) -> WorkerCoreResult<()> {
    BlockShape::new(block_format_id, block_size, chunk_size, effective_len)
        .map_err(|err| WorkerError::InvalidArgument(err.to_string()))?;
    if checksum_kind != ChecksumKind::None {
        return Err(WorkerError::InvalidArgument(
            "only checksum_kind None is supported".to_string(),
        ));
    }
    Ok(())
}

fn validate_fencing_token_shape(block_id: BlockId, token: FencingToken) -> WorkerCoreResult<()> {
    if token.block_id != block_id {
        return Err(WorkerError::Fencing(format!(
            "fencing token block_id does not match request block_id: token={}, request={}",
            token.block_id, block_id
        )));
    }
    if token.epoch == 0 {
        return Err(WorkerError::Fencing("fencing token epoch must be non-zero".to_string()));
    }
    if token.owner.is_zero() {
        return Err(WorkerError::Fencing("fencing token owner must be present".to_string()));
    }
    Ok(())
}

fn reject_existing_final_block(
    store: &(dyn LocalBlockStore + Send + Sync),
    req: &WriteOpenRequest,
) -> WorkerCoreResult<()> {
    match store.load_meta(&req.group_name, req.block_id) {
        Ok(meta) => {
            validate_existing_block_shape(req, &meta)?;
            match meta.visibility.block_state {
                BlockState::Ready | BlockState::Corrupt => Err(WorkerError::RefreshMetadata {
                    kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
                    message: format!(
                        "local block already has final metadata: group_name={}, block_id={}, state={:?}",
                        req.group_name, req.block_id, meta.visibility.block_state
                    ),
                }),
                BlockState::Loading => Err(WorkerError::Corrupt(
                    "loading block metadata is not valid final metadata".to_string(),
                )),
            }
        }
        Err(WorkerError::NotFound(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

fn validate_existing_block_shape(
    req: &WriteOpenRequest,
    meta: &crate::store::block::BlockMetaPayload,
) -> WorkerCoreResult<()> {
    if meta.visibility.block_stamp != req.block_stamp {
        return Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
            message: format!(
                "block stamp mismatch: group_name={}, block_id={}, requested={}, local={}",
                req.group_name, req.block_id, req.block_stamp, meta.visibility.block_stamp
            ),
        });
    }
    if meta.format.format_id != req.block_format_id
        || meta.format.block_size != req.block_size
        || meta.format.chunk_size != u64::from(req.chunk_size)
        || meta.source.effective_len != req.effective_len
        || meta.tier != req.tier
    {
        return Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
            message: format!(
                "block layout mismatch: group_name={}, block_id={}",
                req.group_name, req.block_id
            ),
        });
    }
    Ok(())
}

fn validate_commit_request(state: &StreamState, req: &CommitWriteRequest) -> WorkerCoreResult<()> {
    validate_stream_identity(state, &req.group_name, req.block_id)?;
    validate_matching_token(state, req.token)?;
    if req.commit_seq != state.last_acked_seq {
        return Err(WorkerError::InvalidArgument(format!(
            "commit_seq mismatch: requested={}, expected={}",
            req.commit_seq, state.last_acked_seq
        )));
    }
    if let Err(err) = BlockShape::validate_effective_len(state.context.end_offset, req.effective_len) {
        return Err(match err {
            BlockShapeError::ZeroEffectiveLen => {
                WorkerError::InvalidArgument("effective_len must be greater than zero".to_string())
            }
            BlockShapeError::EffectiveLenExceedsBlock => WorkerError::InvalidArgument(format!(
                "effective_len exceeds block_size: requested={}, block_size={}",
                req.effective_len, state.context.end_offset
            )),
            other => WorkerError::InvalidArgument(other.to_string()),
        });
    }
    if state.cursor != req.effective_len {
        return Err(WorkerError::InvalidArgument(format!(
            "write stream is incomplete: written_through={}, effective_len={}",
            state.cursor, req.effective_len
        )));
    }
    if req.block_stamp == 0 {
        return Err(WorkerError::InvalidArgument(
            "block_stamp must be metadata-assigned and non-zero".to_string(),
        ));
    }
    if req.block_stamp != state.context.block_stamp {
        return Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
            message: format!(
                "block_stamp mismatch between open and commit: open={}, commit={}",
                state.context.block_stamp, req.block_stamp
            ),
        });
    }
    if !req.worker_run_id.matches(state.context.worker_run_id)
        || req.block_format_id != state.context.block_format_id
        || req.block_size != state.context.block_size
        || req.chunk_size != state.context.chunk_size
    {
        return Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
            message: "commit block expectation does not match open write context".to_string(),
        });
    }
    Ok(())
}

fn validate_sync_committed_block_request(req: &SyncCommittedBlockRequest) -> WorkerCoreResult<()> {
    if req.block_stamp == 0 {
        return Err(WorkerError::InvalidArgument(
            "sync committed block requires non-zero block_stamp".to_string(),
        ));
    }
    validate_block_shape(
        req.block_format_id,
        req.block_size,
        req.chunk_size,
        req.expected_block_len,
        ChecksumKind::None,
    )?;
    Ok(())
}

fn validate_sync_committed_block_meta(
    req: &SyncCommittedBlockRequest,
    meta: &crate::store::block::BlockMetaPayload,
) -> WorkerCoreResult<()> {
    if meta.visibility.block_state != BlockState::Ready {
        return Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
            message: format!(
                "local block is not Ready for durable sync: group_name={}, block_id={}, state={:?}",
                req.group_name, req.block_id, meta.visibility.block_state
            ),
        });
    }
    if meta.visibility.block_stamp != req.block_stamp {
        return Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
            message: format!(
                "block stamp mismatch during durable sync: group_name={}, block_id={}, requested={}, local={}",
                req.group_name, req.block_id, req.block_stamp, meta.visibility.block_stamp
            ),
        });
    }
    if meta.source.effective_len != req.expected_block_len {
        return Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
            message: format!(
                "effective block length mismatch during durable sync: group_name={}, block_id={}, expected={}, local={}",
                req.group_name, req.block_id, req.expected_block_len, meta.source.effective_len
            ),
        });
    }
    if req.block_format_id != meta.format.format_id
        || req.block_size != meta.format.block_size
        || u64::from(req.chunk_size) != meta.format.chunk_size
    {
        return Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
            message: format!(
                "block layout mismatch during durable sync: group_name={}, block_id={}",
                req.group_name, req.block_id
            ),
        });
    }
    Ok(())
}

fn validate_abort_request(state: &StreamState, req: &AbortWriteRequest) -> WorkerCoreResult<()> {
    validate_stream_identity(state, &req.group_name, req.block_id)?;
    validate_matching_token(state, req.token)
}

fn validate_stream_identity(state: &StreamState, group_name: &GroupName, block_id: BlockId) -> WorkerCoreResult<()> {
    if &state.context.group_name != group_name {
        return Err(WorkerError::InvalidArgument(format!(
            "write stream group_name mismatch: stream={}, request={}",
            state.context.group_name, group_name
        )));
    }
    if state.context.block_id != block_id {
        return Err(WorkerError::InvalidArgument(format!(
            "write stream block_id mismatch: stream={}, request={}",
            state.context.block_id, block_id
        )));
    }
    Ok(())
}

fn validate_matching_token(state: &StreamState, token: FencingToken) -> WorkerCoreResult<()> {
    let expected = state
        .context
        .fencing_token
        .ok_or_else(|| WorkerError::InvalidArgument("write stream has no fencing token".to_string()))?;
    if token != expected {
        return Err(WorkerError::Fencing(
            "fencing token does not match write stream".to_string(),
        ));
    }
    Ok(())
}

fn rejected_write_frame(state: &StreamState) -> WriteFrameResult {
    WriteFrameResult {
        accepted: false,
        last_acked_seq: state.last_acked_seq,
        written_through: state.written_through,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
    use beryl_types::chunk::ByteRange;
    use beryl_types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, StreamId};
    use beryl_types::layout::BlockFormatId;
    use beryl_types::lease::FencingToken;
    use beryl_types::{GroupName, Tier, WorkerRunId};
    use bytes::Bytes;
    use tempfile::TempDir;

    use crate::data::core::{
        AbortWriteRequest, CommitWriteRequest, RangeMapper, ReadOpenRequest, StreamContext, StreamMode,
        SyncCommittedBlockRequest, WorkerCore, WorkerCoreResult, WriteFrame, WriteOpenRequest,
    };
    use crate::error::WorkerError;
    use crate::runtime::stream::StreamState;
    use crate::store::block::{
        ChecksumKind, CreateStagingBlockRequest, FullBlockFileStore, FullBlockFileStoreConfig, PublishReadyRequest,
    };

    const BLOCK_SIZE: u64 = 4096;
    const CHUNK_SIZE: u32 = 1024;
    const BLOCK_STAMP: u64 = 55;

    fn block_id() -> BlockId {
        BlockId::new(DataHandleId::new(7), BlockIndex::new(3))
    }

    fn group_name() -> GroupName {
        GroupName::parse("root").expect("test group name is valid")
    }

    fn stream_id() -> StreamId {
        StreamId::new((1u128 << 64) | 42)
    }

    fn token() -> FencingToken {
        FencingToken::new(block_id(), ClientId::new(9), 11)
    }

    fn assert_refresh_metadata<T: std::fmt::Debug>(result: WorkerCoreResult<T>, expected_kind: ErrorKind) {
        let error = result.expect_err("operation should need refresh");
        match error {
            WorkerError::RefreshMetadata { kind, .. } => assert_eq!(kind, expected_kind),
            other => panic!("expected RefreshMetadata, got {other:?}"),
        }
    }

    fn assert_invalid_argument<T: std::fmt::Debug>(result: WorkerCoreResult<T>) {
        match result.expect_err("operation should fail") {
            WorkerError::InvalidArgument(_) => {}
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    fn assert_not_found<T: std::fmt::Debug>(result: WorkerCoreResult<T>) {
        match result.expect_err("operation should fail") {
            WorkerError::NotFound(_) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    fn test_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
    }

    fn write_open_request() -> WriteOpenRequest {
        WriteOpenRequest {
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: test_worker_run_id(),
            token: token(),
            block_stamp: BLOCK_STAMP,
            frame_size: 8192,
            block_size: BLOCK_SIZE,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
            checksum_kind: ChecksumKind::None,
            tier: Tier::Hdd,
        }
    }

    fn commit_write_request() -> CommitWriteRequest {
        CommitWriteRequest {
            stream_id: stream_id(),
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: test_worker_run_id(),
            token: token(),
            commit_seq: 8,
            effective_len: 4096,
            block_stamp: BLOCK_STAMP,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            require_sync: true,
        }
    }

    fn abort_write_request() -> AbortWriteRequest {
        AbortWriteRequest {
            stream_id: stream_id(),
            group_name: group_name(),
            block_id: block_id(),
            token: token(),
        }
    }

    fn sync_committed_block_request(block_stamp: u64, expected_block_len: u64) -> SyncCommittedBlockRequest {
        SyncCommittedBlockRequest {
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: test_worker_run_id(),
            block_stamp,
            expected_block_len,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        }
    }

    fn stream_context() -> StreamContext {
        StreamContext {
            stream_id: stream_id(),
            group_name: group_name(),
            block_id: block_id(),
            mode: StreamMode::Read,
            start_offset: 0,
            end_offset: 4096,
            frame_size: 8192,
            block_stamp: 17,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            committed_length: 4096,
            effective_len: 4096,
            worker_run_id: test_worker_run_id(),
            fencing_token: None,
        }
    }

    pub(super) fn payload() -> Bytes {
        Bytes::from((0..BLOCK_SIZE).map(|idx| (idx % 251) as u8).collect::<Vec<_>>())
    }

    fn core_with_store(default_frame_size: u32, max_frame_size: u32) -> (TempDir, Arc<FullBlockFileStore>, WorkerCore) {
        let temp = TempDir::new().expect("tempdir");
        let store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
            temp.path().to_path_buf(),
        )));
        let core = WorkerCore::with_local_store(
            default_frame_size,
            max_frame_size,
            Duration::from_secs(60),
            store.clone(),
        );
        (temp, store, core)
    }

    fn publish_ready_block(store: &FullBlockFileStore, data: Bytes, block_stamp: u64) {
        store
            .create_staging_block(CreateStagingBlockRequest {
                group_name: group_name(),
                block_id: block_id(),
                block_size: BLOCK_SIZE,
                block_format_id: BlockFormatId::FULL_EFFECTIVE,
                chunk_size: CHUNK_SIZE,
                checksum_kind: ChecksumKind::None,
                tier: Tier::Hdd,
            })
            .expect("create staging block");
        store
            .write_at(&group_name(), block_id(), 0, data.clone())
            .expect("write staging block");
        store
            .publish_ready(PublishReadyRequest {
                group_name: group_name(),
                block_id: block_id(),
                effective_len: data.len() as u64,
                block_stamp,
            })
            .expect("publish ready block");
    }

    fn read_open_request_for(offset: u64, len: u32, block_stamp: u64, frame_size: u32) -> ReadOpenRequest {
        read_open_request_for_len(offset, len, block_stamp, BLOCK_SIZE, frame_size)
    }

    fn read_open_request_for_len(
        offset: u64,
        len: u32,
        block_stamp: u64,
        effective_len: u64,
        frame_size: u32,
    ) -> ReadOpenRequest {
        ReadOpenRequest {
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: test_worker_run_id(),
            byte_range: ByteRange { offset, len },
            block_stamp,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len,
            frame_size,
        }
    }

    async fn collect_core_read(core: &WorkerCore, stream_id: StreamId, max_bytes: u32) -> Bytes {
        let mut out = Vec::new();
        loop {
            let frames = core.read_stream(stream_id, max_bytes).await.expect("read stream");
            let Some(frame) = frames.into_iter().next() else {
                break;
            };
            let eos = frame.eos;
            out.extend_from_slice(&frame.data);
            if eos {
                break;
            }
        }
        Bytes::from(out)
    }

    fn write_stream_context() -> StreamContext {
        StreamContext {
            mode: StreamMode::Write,
            fencing_token: Some(token()),
            ..stream_context()
        }
    }

    #[test]
    fn range_mapper_covers_chunk_boundaries() {
        let cases = [
            ("inside one chunk", 100, 200, vec![(0, 100, 200)]),
            ("across two chunks", 900, 300, vec![(0, 900, 124), (1, 0, 176)]),
            ("at chunk boundary", 1024, 100, vec![(1, 0, 100)]),
            ("empty range", 512, 0, vec![]),
            (
                "non-aligned across three chunks",
                1537,
                2000,
                vec![(1, 513, 511), (2, 0, 1024), (3, 0, 465)],
            ),
        ];

        for (case, offset, len, expected) in cases {
            let actual = RangeMapper::map_range(ByteRange { offset, len }, 1024)
                .unwrap()
                .into_iter()
                .map(|slice| (slice.chunk_index.as_raw(), slice.offset_in_chunk, slice.len))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "case {case}");
        }
    }

    #[tokio::test]
    async fn open_write_creates_staging_stream() {
        let (_temp, store, core) = core_with_store(512, 2048);

        let result = core.open_write(write_open_request()).await.expect("open write");

        assert_eq!(result.frame_size, 2048);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
        assert_eq!(result.committed_length, 0);

        let paths = store.paths(&group_name(), block_id());
        assert!(paths.staging_data_path.exists());
        assert!(paths.staging_meta_path.exists());
        assert!(!paths.meta_path.exists());
        assert_not_found(store.read_at(&group_name(), block_id(), 0, 1));

        let state = core
            .stream_manager()
            .get(result.stream_id)
            .await
            .expect("write stream registered");
        assert_eq!(state.context.group_name, group_name());
        assert_eq!(state.context.block_id, block_id());
        assert_eq!(state.context.mode, StreamMode::Write);
        assert_eq!(state.context.end_offset, BLOCK_SIZE);
        assert_eq!(state.cursor, 0);
        assert_eq!(state.last_acked_seq, 0);
        assert_eq!(state.written_through, 0);
    }

    #[tokio::test]
    async fn open_write_rejects_invalid_metadata_shape_before_staging() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let paths = store.paths(&group_name(), block_id());

        let mut zero_stamp = write_open_request();
        zero_stamp.block_stamp = 0;
        assert_invalid_argument(core.open_write(zero_stamp).await);

        let mut non_aligned = write_open_request();
        non_aligned.chunk_size = 1000;
        assert_invalid_argument(core.open_write(non_aligned).await);

        let mut over_len = write_open_request();
        over_len.effective_len = BLOCK_SIZE + 1;
        assert_invalid_argument(core.open_write(over_len).await);

        assert!(!paths.staging_data_path.exists());
        assert!(!paths.staging_meta_path.exists());
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_write_rejects_invalid_fencing_token() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let mut req = write_open_request();
        req.token = FencingToken::new(block_id(), ClientId::new(9), 0);

        match core.open_write(req).await.expect_err("zero epoch must be rejected") {
            WorkerError::Fencing(message) => assert!(message.contains("epoch")),
            other => panic!("expected Fencing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn open_write_rejects_ready_block_conflicts() {
        for (case, stored_stamp, block_size, expected) in [
            (
                "already ready",
                BLOCK_STAMP,
                BLOCK_SIZE,
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
            ),
            (
                "shape mismatch",
                BLOCK_STAMP,
                BLOCK_SIZE * 2,
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
            ),
            (
                "stamp mismatch",
                BLOCK_STAMP + 1,
                BLOCK_SIZE,
                ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
            ),
        ] {
            let (_temp, store, core) = core_with_store(512, 2048);
            publish_ready_block(&store, payload(), stored_stamp);
            let mut request = write_open_request();
            request.block_size = block_size;

            assert_refresh_metadata(core.open_write(request).await, expected);
            assert_eq!(
                core.stream_manager().active_count().await,
                0,
                "case {case} must not register a stream"
            );
        }
    }

    #[tokio::test]
    async fn write_stream_writes_staging_data_and_advances_state() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = Bytes::from_static(b"abcd");

        let result = core
            .write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: 1,
                offset_in_block: 0,
                data: data.clone(),
                checksum32: 0,
            })
            .await
            .expect("write frame");

        assert!(result.accepted);
        assert_eq!(result.last_acked_seq, 1);
        assert_eq!(result.written_through, data.len() as u64);
        let state = core.stream_manager().get(open.stream_id).await.expect("stream state");
        assert_eq!(state.cursor, data.len() as u64);
        assert_eq!(state.last_acked_seq, 1);
        assert_eq!(state.written_through, data.len() as u64);
        assert!(!store.paths(&group_name(), block_id()).meta_path.exists());
    }

    #[tokio::test]
    async fn write_stream_rejects_sequence_and_offset_gaps() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");

        for (case, seq, offset_in_block) in [("sequence", 2, 0), ("offset", 1, 1)] {
            let result = core
                .write_stream(WriteFrame {
                    stream_id: open.stream_id,
                    seq,
                    offset_in_block,
                    data: Bytes::from_static(b"abcd"),
                    checksum32: 0,
                })
                .await
                .unwrap_or_else(|error| panic!("{case} gap should return a negative acknowledgement: {error}"));

            assert!(!result.accepted, "{case} gap must be rejected");
            assert_eq!(result.last_acked_seq, 0, "{case} gap");
            assert_eq!(result.written_through, 0, "{case} gap");
            assert_eq!(
                core.stream_manager().get(open.stream_id).await.expect("stream").cursor,
                0,
                "{case} gap must not advance the cursor"
            );
        }
    }

    #[tokio::test]
    async fn write_stream_rejects_read_stream() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let open = core
            .open_read(read_open_request_for(0, 4, BLOCK_STAMP, 512))
            .await
            .expect("open read");

        match core
            .write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: 1,
                offset_in_block: 0,
                data: Bytes::from_static(b"abcd"),
                checksum32: 0,
            })
            .await
            .expect_err("read stream must reject writes")
        {
            WorkerError::InvalidArgument(message) => assert!(message.contains("not a write stream")),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn commit_write_publishes_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: data.slice(0..2048),
            checksum32: 0,
        })
        .await
        .expect("first frame");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 2,
            offset_in_block: 2048,
            data: data.slice(2048..4096),
            checksum32: 0,
        })
        .await
        .expect("second frame");

        let result = core
            .commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 2,
                effective_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await
            .expect("commit write");

        assert_eq!(result.effective_len, BLOCK_SIZE);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
        assert_eq!(result.written_through, BLOCK_SIZE);
        let meta = store.load_meta(&group_name(), block_id()).expect("ready meta");
        assert_eq!(meta.visibility.block_state, crate::store::block::BlockState::Ready);
        assert_eq!(meta.visibility.block_stamp, BLOCK_STAMP);
        assert_eq!(store.read_at(&group_name(), block_id(), 0, BLOCK_SIZE).unwrap(), data);
    }

    #[tokio::test]
    async fn multichunk_write_commit_and_read_returns_exact_effective_bytes() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let effective_len = 3073;
        let data = payload().slice(0..effective_len as usize);
        let mut open_req = write_open_request();
        open_req.effective_len = effective_len;
        let open = core.open_write(open_req).await.expect("open write");

        let chunks = [
            data.slice(0..700),
            data.slice(700..1536),
            data.slice(1536..2500),
            data.slice(2500..effective_len as usize),
        ];
        let mut offset = 0u64;
        for (idx, chunk) in chunks.into_iter().enumerate() {
            core.write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: (idx + 1) as u64,
                offset_in_block: offset,
                data: chunk.clone(),
                checksum32: 0,
            })
            .await
            .expect("write chunk");
            offset += chunk.len() as u64;
        }

        let result = core
            .commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 4,
                effective_len,
                ..commit_write_request()
            })
            .await
            .expect("commit write");

        assert_eq!(result.effective_len, effective_len);
        assert_eq!(result.written_through, effective_len);
        let meta = store.load_meta(&group_name(), block_id()).expect("ready meta");
        assert_eq!(meta.source.effective_len, effective_len);
        assert_eq!(
            store.read_at(&group_name(), block_id(), 0, effective_len).unwrap(),
            data
        );

        let open_read = core
            .open_read(read_open_request_for_len(
                0,
                effective_len as u32,
                BLOCK_STAMP,
                effective_len,
                600,
            ))
            .await
            .expect("open read");
        assert_eq!(collect_core_read(&core, open_read.stream_id, 600).await, data);

        let eof_read = core
            .open_read(read_open_request_for_len(
                effective_len,
                0,
                BLOCK_STAMP,
                effective_len,
                600,
            ))
            .await
            .expect("open eof read");
        assert!(collect_core_read(&core, eof_read.stream_id, 600).await.is_empty());
        assert_invalid_argument(
            core.open_read(read_open_request_for_len(
                effective_len,
                1,
                BLOCK_STAMP,
                effective_len,
                600,
            ))
            .await,
        );
    }

    #[tokio::test]
    async fn commit_write_accepts_non_chunk_aligned_tail_and_persists_full_block_shape() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let effective_len = u64::from(CHUNK_SIZE) + 1;
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from(vec![7; effective_len as usize]),
            checksum32: 0,
        })
        .await
        .expect("tail frame");

        let result = core
            .commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len,
                ..commit_write_request()
            })
            .await
            .expect("tail commit");

        assert_eq!(result.effective_len, effective_len);
        assert_eq!(result.written_through, effective_len);
        let meta = store.load_meta(&group_name(), block_id()).expect("ready meta");
        assert_eq!(meta.format.block_size, BLOCK_SIZE);
        assert_eq!(meta.source.effective_len, effective_len);
    }

    #[tokio::test]
    async fn commit_write_rejects_effective_len_larger_than_block_size() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");

        assert_invalid_argument(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 0,
                effective_len: BLOCK_SIZE + 1,
                ..commit_write_request()
            })
            .await,
        );
    }

    #[tokio::test]
    async fn commit_write_rejects_layout_mismatch_against_open_request() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let mut open_req = write_open_request();
        open_req.effective_len = 4;
        let open = core.open_write(open_req).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"abcd"),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        assert_refresh_metadata(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len: 4,
                chunk_size: CHUNK_SIZE * 2,
                ..commit_write_request()
            })
            .await,
            ErrorKind::Metadata(MetadataErrorKind::StaleState),
        );
    }

    #[tokio::test]
    async fn commit_write_rejects_incomplete_block() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"abcd"),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        assert_invalid_argument(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await,
        );
    }

    #[tokio::test]
    async fn commit_write_rejects_token_mismatch() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data,
            checksum32: 0,
        })
        .await
        .expect("write frame");

        match core
            .commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                token: FencingToken::new(block_id(), ClientId::new(99), 11),
                commit_seq: 1,
                effective_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await
            .expect_err("token mismatch must be rejected")
        {
            WorkerError::Fencing(message) => assert!(message.contains("token")),
            other => panic!("expected Fencing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn duplicate_commit_fails_without_republishing_or_corrupting_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: data.clone(),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_len: BLOCK_SIZE,
            ..commit_write_request()
        })
        .await
        .expect("first commit");
        assert!(core.stream_manager().get(open.stream_id).await.is_none());
        assert_not_found(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await,
        );

        let scanned = store.scan_group_blocks(&group_name()).expect("scan group");
        assert_eq!(scanned.len(), 1);
        assert_eq!(
            scanned[0].visibility.block_state,
            crate::store::block::BlockState::Ready
        );
        assert_eq!(store.read_at(&group_name(), block_id(), 0, BLOCK_SIZE).unwrap(), data);
    }

    #[tokio::test]
    async fn sync_committed_block_succeeds_after_terminal_commit_without_stream() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: payload(),
            checksum32: 0,
        })
        .await
        .expect("write frame");
        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_len: BLOCK_SIZE,
            require_sync: false,
            ..commit_write_request()
        })
        .await
        .expect("visibility commit");
        assert!(core.stream_manager().get(open.stream_id).await.is_none());

        let result = core
            .sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
            .await
            .expect("sync committed block");

        assert_eq!(result.effective_len, BLOCK_SIZE);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
    }

    #[tokio::test]
    async fn sync_committed_block_rejects_missing_wrong_generation_and_uncommitted_block() {
        let (_temp, _store, core) = core_with_store(512, 2048);
        assert_refresh_metadata(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
                .await,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        );

        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: payload(),
            checksum32: 0,
        })
        .await
        .expect("write frame");
        assert_refresh_metadata(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
                .await,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        );

        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_len: BLOCK_SIZE,
            ..commit_write_request()
        })
        .await
        .expect("commit write");
        assert_refresh_metadata(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP + 1, BLOCK_SIZE))
                .await,
            ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
        );
        assert_refresh_metadata(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE - 1))
                .await,
            ErrorKind::Metadata(MetadataErrorKind::StaleState),
        );
    }

    #[tokio::test]
    async fn sync_committed_block_rejects_block_layout_mismatch() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(store.as_ref(), payload(), BLOCK_STAMP);

        let mut block_size_mismatch = sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE);
        block_size_mismatch.block_size = BLOCK_SIZE * 2;
        assert_refresh_metadata(
            core.sync_committed_block(block_size_mismatch).await,
            ErrorKind::Metadata(MetadataErrorKind::StaleState),
        );

        let mut chunk_size_mismatch = sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE);
        chunk_size_mismatch.chunk_size = CHUNK_SIZE * 2;
        assert_refresh_metadata(
            core.sync_committed_block(chunk_size_mismatch).await,
            ErrorKind::Metadata(MetadataErrorKind::StaleState),
        );
    }

    #[tokio::test]
    async fn repeated_sync_committed_block_is_idempotent() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(store.as_ref(), payload(), BLOCK_STAMP);

        let first = core
            .sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
            .await
            .expect("first sync");
        let second = core
            .sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
            .await
            .expect("second sync");

        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn abort_discards_partial_write_and_keeps_no_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"partial"),
            checksum32: 0,
        })
        .await
        .expect("partial frame");

        let result = core
            .abort_write(AbortWriteRequest {
                stream_id: open.stream_id,
                ..abort_write_request()
            })
            .await
            .expect("abort write");

        assert!(result.aborted);
        assert!(core.stream_manager().get(open.stream_id).await.is_none());
        let paths = store.paths(&group_name(), block_id());
        assert!(!paths.staging_data_path.exists());
        assert!(!paths.staging_meta_path.exists());
        assert!(!paths.meta_path.exists());
        assert_not_found(store.read_at(&group_name(), block_id(), 0, 1));
        assert_refresh_metadata(
            core.open_read(read_open_request_for(0, 1, BLOCK_STAMP, 512)).await,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        );
        assert!(store.scan_group_blocks(&group_name()).expect("scan group").is_empty());

        assert_not_found(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len: 7,
                ..commit_write_request()
            })
            .await,
        );
        assert_not_found(
            core.abort_write(AbortWriteRequest {
                stream_id: open.stream_id,
                ..abort_write_request()
            })
            .await,
        );
    }

    #[tokio::test]
    async fn abort_after_successful_commit_does_not_damage_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: data.clone(),
            checksum32: 0,
        })
        .await
        .expect("write frame");
        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_len: BLOCK_SIZE,
            ..commit_write_request()
        })
        .await
        .expect("commit write");

        assert_not_found(
            core.abort_write(AbortWriteRequest {
                stream_id: open.stream_id,
                ..abort_write_request()
            })
            .await,
        );

        let scanned = store.scan_group_blocks(&group_name()).expect("scan group");
        assert_eq!(scanned.len(), 1);
        assert_eq!(store.read_at(&group_name(), block_id(), 0, BLOCK_SIZE).unwrap(), data);
    }

    #[tokio::test]
    async fn recover_after_uncommitted_write_is_not_readable() {
        let (temp, _store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"abcd"),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        let recovered_store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(temp.path().to_path_buf()));
        assert_not_found(recovered_store.recover_block(&group_name(), block_id()));
        assert_not_found(recovered_store.read_at(&group_name(), block_id(), 0, 1));
    }

    #[tokio::test]
    async fn incomplete_staging_write_is_ignored_by_ready_block_scan() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"partial"),
            checksum32: 0,
        })
        .await
        .expect("partial frame");

        let paths = store.paths(&group_name(), block_id());
        assert!(paths.staging_data_path.exists());
        assert!(paths.staging_meta_path.exists());
        assert!(!paths.meta_path.exists());
        assert!(store.scan_group_blocks(&group_name()).expect("scan group").is_empty());
    }

    #[tokio::test]
    async fn open_read_ready_block_succeeds() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        let result = core
            .open_read(read_open_request_for(128, 1024, BLOCK_STAMP, 0))
            .await
            .expect("open read");

        assert_eq!(result.frame_size, 512);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
        assert_eq!(result.committed_length, BLOCK_SIZE);

        let state = core
            .stream_manager()
            .get(result.stream_id)
            .await
            .expect("read stream registered");
        assert_eq!(state.context.group_name, group_name());
        assert_eq!(state.context.block_id, block_id());
        assert_eq!(state.context.mode, StreamMode::Read);
        assert_eq!(state.context.start_offset, 128);
        assert_eq!(state.context.end_offset, 1152);
        assert_eq!(state.cursor, 128);
        assert_eq!(state.context.effective_len, BLOCK_SIZE);
    }

    #[tokio::test]
    async fn worker_core_uses_configured_store_dir() {
        let custom_dir = TempDir::new().expect("custom store dir");
        let other_dir = TempDir::new().expect("other store dir");
        let store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(custom_dir.path().to_path_buf()));
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        let core = WorkerCore::with_options(512, 2048, Duration::from_secs(60), custom_dir.path().to_path_buf());

        let result = core
            .open_read(read_open_request_for(0, 8, BLOCK_STAMP, 512))
            .await
            .expect("open read from configured store dir");
        assert!(core.stream_manager().get(result.stream_id).await.is_some());

        let paths = store.paths(&group_name(), block_id());
        assert!(paths.data_path.starts_with(custom_dir.path()));
        assert!(paths.meta_path.starts_with(custom_dir.path()));
        assert!(
            paths.data_path.exists(),
            "ready block data must exist under custom store dir"
        );
        assert!(
            paths.meta_path.exists(),
            "ready block metadata must exist under custom store dir"
        );

        let other_store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(other_dir.path().to_path_buf()));
        let other_paths = other_store.paths(&group_name(), block_id());
        assert!(
            !other_paths.data_path.exists(),
            "ready block data must not be created under other store dir"
        );
        assert!(
            !other_paths.meta_path.exists(),
            "ready block metadata must not be created under other store dir"
        );
    }

    #[tokio::test]
    async fn open_read_rejects_invalid_ready_block_requests() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        let mut block_size_mismatch = read_open_request_for(0, 1024, BLOCK_STAMP, 512);
        block_size_mismatch.block_size = BLOCK_SIZE * 2;
        let mut chunk_size_mismatch = read_open_request_for(0, 1024, BLOCK_STAMP, 512);
        chunk_size_mismatch.chunk_size = CHUNK_SIZE * 2;
        let cases = [
            (
                "stamp mismatch",
                read_open_request_for(0, 1024, BLOCK_STAMP + 1, 512),
                Some(ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch)),
            ),
            (
                "block size mismatch",
                block_size_mismatch,
                Some(ErrorKind::Metadata(MetadataErrorKind::StaleState)),
            ),
            (
                "chunk size mismatch",
                chunk_size_mismatch,
                Some(ErrorKind::Metadata(MetadataErrorKind::StaleState)),
            ),
            ("zero block stamp", read_open_request_for(0, 1024, 0, 512), None),
            (
                "out of bounds range",
                read_open_request_for(4090, 16, BLOCK_STAMP, 512),
                None,
            ),
        ];

        for (case, request, refresh_error) in cases {
            let result = core.open_read(request).await;
            if let Some(expected) = refresh_error {
                assert_refresh_metadata(result, expected);
            } else {
                assert_invalid_argument(result);
            }
            assert_eq!(
                core.stream_manager().active_count().await,
                0,
                "case {case} must not register a stream"
            );
        }
    }

    #[tokio::test]
    async fn open_read_rejects_missing_block() {
        let (_temp, _store, core) = core_with_store(512, 2048);

        assert_refresh_metadata(
            core.open_read(read_open_request_for(0, 1024, BLOCK_STAMP, 512)).await,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        );
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn read_stream_advances_cursor_and_respects_max_bytes() {
        let (_temp, store, core) = core_with_store(8, 16);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let open = core
            .open_read(read_open_request_for(0, 8, BLOCK_STAMP, 8))
            .await
            .expect("open read");

        let first = core.read_stream(open.stream_id, 3).await.expect("first read");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].data, data.slice(0..3));
        assert!(!first[0].eos);
        assert_eq!(
            core.stream_manager().get(open.stream_id).await.expect("stream").cursor,
            3
        );

        let second = core.read_stream(open.stream_id, 4).await.expect("second read");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].data, data.slice(3..7));
        assert!(!second[0].eos);
        assert_eq!(
            core.stream_manager().get(open.stream_id).await.expect("stream").cursor,
            7
        );

        let third = core.read_stream(open.stream_id, 4).await.expect("third read");
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].data, data.slice(7..8));
        assert!(third[0].eos);
        assert!(core.stream_manager().get(open.stream_id).await.is_none());
    }

    #[tokio::test]
    async fn read_stream_offset_length_and_eof_boundaries_are_exact() {
        let (_temp, store, core) = core_with_store(513, 2048);
        let effective_len = u64::from(CHUNK_SIZE) * 2 + 17;
        let data = payload().slice(0..effective_len as usize);
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);

        let full = core
            .open_read(read_open_request_for_len(
                0,
                effective_len as u32,
                BLOCK_STAMP,
                effective_len,
                513,
            ))
            .await
            .expect("open full read");
        assert_eq!(collect_core_read(&core, full.stream_id, 513).await, data);

        let nonzero = core
            .open_read(read_open_request_for_len(17, 100, BLOCK_STAMP, effective_len, 64))
            .await
            .expect("open nonzero read");
        assert_eq!(
            collect_core_read(&core, nonzero.stream_id, 64).await,
            data.slice(17..117)
        );

        let short = core
            .open_read(read_open_request_for_len(100, 3, BLOCK_STAMP, effective_len, 64))
            .await
            .expect("open short read");
        assert_eq!(
            collect_core_read(&core, short.stream_id, 64).await,
            data.slice(100..103)
        );

        let boundary_offset = u64::from(CHUNK_SIZE) - 3;
        let across_chunk = core
            .open_read(read_open_request_for_len(
                boundary_offset,
                10,
                BLOCK_STAMP,
                effective_len,
                4,
            ))
            .await
            .expect("open chunk boundary read");
        assert_eq!(
            collect_core_read(&core, across_chunk.stream_id, 4).await,
            data.slice(boundary_offset as usize..boundary_offset as usize + 10)
        );

        let eof = core
            .open_read(read_open_request_for_len(
                effective_len,
                0,
                BLOCK_STAMP,
                effective_len,
                64,
            ))
            .await
            .expect("open eof read");
        assert!(collect_core_read(&core, eof.stream_id, 64).await.is_empty());

        assert_invalid_argument(
            core.open_read(read_open_request_for_len(
                effective_len,
                1,
                BLOCK_STAMP,
                effective_len,
                64,
            ))
            .await,
        );
        assert_invalid_argument(
            core.open_read(read_open_request_for_len(
                effective_len - 1,
                2,
                BLOCK_STAMP,
                effective_len,
                64,
            ))
            .await,
        );
    }

    #[tokio::test]
    async fn read_stream_rejects_missing_and_write_streams() {
        let (_temp, _store, core) = core_with_store(8, 16);

        assert_not_found(core.read_stream(stream_id(), 1024).await);
        let state = StreamState::new(write_stream_context());
        core.stream_manager().register(state).await;

        match core
            .read_stream(stream_id(), 1024)
            .await
            .expect_err("write stream must not be readable")
        {
            WorkerError::InvalidArgument(message) => assert!(message.contains("not a read stream")),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
        assert_eq!(core.stream_manager().get(stream_id()).await.expect("stream").cursor, 0);
    }
}
