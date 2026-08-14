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
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;

use crate::error::WorkerError;
use crate::observe;
use crate::runtime::block::{BlockManager, ReadPin, ReclaimingBlock};
use crate::runtime::stream::{StreamAccessError, StreamManager, StreamOperation, StreamState};
use crate::store::block::{
    BlockState, ChecksumKind, CreateStagingBlockRequest, FullBlockFileStore, FullBlockFileStoreConfig, LocalBlockStore,
    PublishReadyRequest, ReclaimBlockRequest, ReclaimBlockResult, SyncReadyBlockRequest,
};

pub type WorkerCoreResult<T> = Result<T, WorkerError>;

const STREAM_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
const MAX_STREAM_RETIREMENTS_PER_PASS: usize = 64;

/// Stable metadata-authorized facts for one open write stream.
#[derive(Clone, Debug)]
pub struct WriteStreamContext {
    pub stream_id: StreamId,
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub worker_run_id: WorkerRunId,
    /// Exclusive block-local byte offset where writes must stop.
    pub end_offset: u64,
    /// Metadata-authoritative block stamp bound at write open.
    pub block_stamp: u64,
    pub block_format_id: BlockFormatId,
    pub block_size: u64,
    pub chunk_size: u32,
    /// Fencing token that every later write operation must match.
    pub fencing_token: FencingToken,
}

/// Metadata-authorized block-local range requested by one read RPC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadBlockRequest {
    pub(crate) group_name: GroupName,
    pub(crate) block_id: BlockId,
    /// Block-local byte range. The offset is relative to block_id, not to the file.
    pub(crate) byte_range: ByteRange,
    /// Logical block stamp used for direct read validation.
    /// Normal client reads must use a non-zero metadata-authoritative stamp.
    /// The `ReadBlock` RPC rejects 0 before local block metadata lookup.
    pub(crate) block_stamp: u64,
    pub(crate) block_format_id: BlockFormatId,
    pub(crate) block_size: u64,
    pub(crate) chunk_size: u32,
    pub(crate) effective_len: u64,
    /// Requested transport frame payload size, not the worker-local StorageChunk size.
    pub(crate) frame_size: u32,
}

/// Live state for one `ReadBlock` response stream.
///
/// The pin spans the response stream and is cloned into every blocking read so
/// cancellation cannot let reclamation pass an unfinished filesystem access.
#[derive(Debug)]
pub(crate) struct ActiveBlockRead {
    group_name: GroupName,
    block_id: BlockId,
    next_offset: u64,
    end_offset: u64,
    frame_size: u32,
    read_pin: ReadPin,
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
    stream_id_prefix: u128,
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
        Self::with_local_store_for_run(
            default_frame_size,
            max_frame_size,
            stream_idle_timeout,
            WorkerRunId::new(),
            block_store,
        )
    }

    /// Builds a Worker core whose opaque stream IDs are scoped to one process run.
    ///
    /// The run-derived prefix prevents stale high-frequency frames, which carry
    /// only a stream ID, from matching a stream created after Worker restart.
    pub fn with_local_store_for_run(
        default_frame_size: u32,
        max_frame_size: u32,
        stream_idle_timeout: Duration,
        worker_run_id: WorkerRunId,
        block_store: Arc<dyn LocalBlockStore + Send + Sync>,
    ) -> Self {
        let block_manager = Arc::new(BlockManager::new(default_frame_size, max_frame_size));
        Self {
            stream_manager: Arc::new(StreamManager::new(stream_idle_timeout)),
            block_manager,
            block_store,
            next_stream_seq: Arc::new(AtomicU64::new(1)),
            stream_id_prefix: worker_run_id.as_uuid().as_u128() & !u128::from(u32::MAX),
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

    /// Validates one metadata-authorized range and binds its pin to an RPC-owned read.
    pub(crate) async fn begin_block_read(&self, req: ReadBlockRequest) -> WorkerCoreResult<ActiveBlockRead> {
        let frame_size = self.negotiate_frame_size(req.frame_size)?;
        self.block_manager.validate_read_request(&req)?;
        let read_pin = self.block_manager.pin_read(&req.group_name, req.block_id)?;
        let validation_pin = read_pin.clone();
        let block_manager = Arc::clone(&self.block_manager);
        let block_store = Arc::clone(&self.block_store);
        let validation_request = req.clone();
        tokio::task::spawn_blocking(move || {
            let _pin = validation_pin;
            block_manager.validate_read(block_store.as_ref(), &validation_request)
        })
        .await
        .map_err(|error| WorkerError::Internal(format!("block read validation task failed: {error}")))??;
        let end_offset = req
            .byte_range
            .offset
            .checked_add(u64::from(req.byte_range.len))
            .ok_or_else(|| WorkerError::InvalidArgument("byte range offset overflow".to_string()))?;
        Ok(ActiveBlockRead {
            group_name: req.group_name,
            block_id: req.block_id,
            next_offset: req.byte_range.offset,
            end_offset,
            frame_size,
            read_pin,
        })
    }

    /// Reclaims one metadata-authorized Ready block version from local storage.
    ///
    /// The exact stamp is checked before reader exclusion and again before the
    /// durable marker is published. New readers are rejected once reclaiming
    /// starts, existing read RPCs drain through RAII pins, and any filesystem
    /// error leaves the block reclaiming for an idempotent retry.
    pub async fn reclaim_block(&self, req: ReclaimBlockRequest) -> WorkerCoreResult<ReclaimBlockResult> {
        self.block_store.inspect_reclaim_block(&req)?;
        let permit = self
            .block_manager
            .begin_reclaim(&req.group_name, req.block_id, req.expected_block_stamp)
            .await?;
        let result = self.block_store.reclaim_block(&req)?;
        permit.complete();
        Ok(result)
    }

    /// Lists exact block versions currently excluded from new readers.
    pub(crate) fn reclaiming_blocks(&self, group_name: &GroupName) -> Vec<ReclaimingBlock> {
        self.block_manager.reclaiming_blocks(group_name)
    }

    /// Waits until runtime reclamation completion may change a block report.
    pub(crate) async fn wait_for_block_report_change(&self) {
        self.block_manager.wait_for_block_report_change().await;
    }

    /// Open a staging stream bounded by Metadata's full block capacity.
    ///
    /// The final valid length is intentionally unknown here and is fixed by
    /// `commit_write` after the worker verifies the written cursor exactly.
    pub async fn open_write(&self, req: WriteOpenRequest) -> WorkerCoreResult<WriteOpenResult> {
        let group_name = req.group_name.clone();
        let block_id = req.block_id;
        let worker_run_id = req.worker_run_id;
        let inode_id = req.block_id.inode_id;
        let block_stamp = req.block_stamp;
        let result = async {
            let frame_size = self.negotiate_frame_size(req.frame_size)?;
            validate_write_open_request(&req)?;
            reject_existing_final_block(self.block_store.as_ref(), &req)?;
            let stream_id = self.next_stream_id()?;
            let context = WriteStreamContext {
                stream_id,
                group_name: req.group_name.clone(),
                block_id: req.block_id,
                worker_run_id: req.worker_run_id,
                end_offset: req.block_size,
                block_stamp: req.block_stamp,
                block_format_id: req.block_format_id,
                block_size: req.block_size,
                chunk_size: req.chunk_size,
                fencing_token: req.token,
            };
            let stream_state = StreamState::new(context);

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
                    inode_id = inode_id.as_raw(),
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
                        inode_id = inode_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        block_stamp,
                        "Block create rejected"
                    );
                    if let Err(cleanup_error) = self.block_store.abort_staging_block(&req.group_name, req.block_id) {
                        tracing::warn!(
                            target: "worker.block",
                            op = "AbortFailedOpen",
                            group_id = %group_name,
                            block_id = %block_id,
                            error_code = observe::worker_error_kind(&cleanup_error),
                            error = %cleanup_error,
                            "Failed OpenWrite retained a Retiring stream for cleanup retry"
                        );
                        if !self.stream_manager.register_write(stream_state) {
                            return Err(WorkerError::Internal(format!(
                                "duplicate stream identity generated while retaining failed open: stream_id={stream_id}"
                            )));
                        }
                        self.stream_manager.request_write_retirement(stream_id);
                    }
                    return Err(error);
                }
            }

            if !self.stream_manager.register_write(stream_state) {
                self.block_store.abort_staging_block(&req.group_name, req.block_id)?;
                return Err(WorkerError::Internal(format!(
                    "duplicate stream identity generated: stream_id={stream_id}"
                )));
            }

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
                inode_id = inode_id.as_raw(),
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
                inode_id = inode_id.as_raw(),
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
        let inode_id = req.block_id.inode_id;
        let result = async {
            let operation = self.write_operation(req.stream_id).await?;
            validate_commit_request(&operation, &req)?;
            operation.mark_retiring();

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
                        inode_id = inode_id.as_raw(),
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
                        inode_id = inode_id.as_raw(),
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
                inode_id = inode_id.as_raw(),
                worker_run_id = %worker_run_id,
                committed_length = meta.source.effective_len,
                bytes_written = meta.source.effective_len,
                block_stamp = meta.visibility.block_stamp,
                "CommitWrite completed"
            );
            self.stream_manager.complete_retirement(req.stream_id, &operation);

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
                inode_id = inode_id.as_raw(),
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

    /// Idempotently aborts one write stream and its unpublished local block.
    ///
    /// A missing stream is already terminal and succeeds. Existing streams are
    /// identity- and token-checked before entering Retiring. Filesystem failure
    /// keeps the Retiring entry so process-owned cleanup can retry exactly.
    pub async fn abort_write(&self, req: AbortWriteRequest) -> WorkerCoreResult<AbortWriteResult> {
        let operation = match self.stream_manager.begin_operation(req.stream_id).await {
            Ok(operation) => operation,
            Err(StreamAccessError::Missing) => return Ok(AbortWriteResult { aborted: true }),
            Err(StreamAccessError::Retiring) => match self.stream_manager.begin_write_retirement(req.stream_id).await {
                Ok(operation) => operation,
                Err(StreamAccessError::Missing) => return Ok(AbortWriteResult { aborted: true }),
                Err(error) => return Err(stream_access_error("write", req.stream_id, error)),
            },
        };
        validate_abort_request(&operation, &req)?;
        operation.mark_retiring();
        self.block_store.abort_staging_block(&req.group_name, req.block_id)?;
        self.stream_manager.complete_retirement(req.stream_id, &operation);
        Ok(AbortWriteResult { aborted: true })
    }

    /// Retires only a write stream after transport or protocol failure.
    ///
    /// Missing and non-write identities are no-ops; local abort failure keeps
    /// the write Retiring for the process-owned maintenance loop.
    pub(crate) async fn abort_write_stream_after_error(&self, stream_id: StreamId) -> WorkerCoreResult<()> {
        let operation = match self.stream_manager.begin_write_retirement(stream_id).await {
            Ok(operation) => operation,
            Err(StreamAccessError::Missing) => return Ok(()),
            Err(StreamAccessError::Retiring) => {
                unreachable!("begin_write_retirement accepts a Retiring write stream")
            }
        };
        self.block_store
            .abort_staging_block(&operation.context.group_name, operation.context.block_id)?;
        self.stream_manager.complete_retirement(stream_id, &operation);
        Ok(())
    }

    /// Runs bounded idle retirement until process shutdown begins.
    pub async fn run_idle_stream_cleanup(&self, shutdown: CancellationToken) {
        let mut interval = tokio::time::interval(STREAM_CLEANUP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = interval.tick() => {
                    self.cleanup_stream_batch(false).await;
                }
            }
        }
    }

    /// Retires all streams until they are gone or the process deadline expires.
    ///
    /// A timed-out drain leaves unpublished files for the startup recovery path;
    /// it never reports those resources as successfully released.
    pub async fn drain_streams_until(&self, deadline: TokioInstant) -> bool {
        loop {
            self.cleanup_stream_batch(true).await;
            if self.stream_manager.active_count().await == 0 {
                return false;
            }
            if TokioInstant::now() >= deadline {
                return true;
            }
            let retry_at = (TokioInstant::now() + Duration::from_millis(10)).min(deadline);
            tokio::time::sleep_until(retry_at).await;
        }
    }

    /// Completes at most one bounded batch of drained stream cleanup.
    async fn cleanup_stream_batch(&self, drain: bool) -> usize {
        let candidates = self
            .stream_manager
            .take_cleanup_batch(MAX_STREAM_RETIREMENTS_PER_PASS, drain);
        let mut completed = 0usize;
        for stream_id in candidates {
            let operation = match self.stream_manager.try_begin_retirement(stream_id) {
                Ok(Some(operation)) => operation,
                Ok(None) | Err(StreamAccessError::Missing) => continue,
                Err(StreamAccessError::Retiring) => unreachable!("retirement accepts a Retiring stream"),
            };
            let cleanup = self
                .block_store
                .abort_staging_block(&operation.context.group_name, operation.context.block_id);
            match cleanup {
                Ok(()) => {
                    completed += usize::from(self.stream_manager.complete_retirement(stream_id, &operation));
                }
                Err(error) => tracing::warn!(
                    target: "worker.state",
                    op = "RetireStream",
                    stream_id = %stream_id,
                    group_id = %operation.context.group_name,
                    block_id = %operation.context.block_id,
                    error_code = observe::worker_error_kind(&error),
                    error = %error,
                    "Stream retirement retained local resources for retry"
                ),
            }
        }
        completed
    }

    /// Reads the next exact chunk without executing filesystem work on Tokio workers.
    pub(crate) async fn read_block_chunk(&self, read: &mut ActiveBlockRead) -> WorkerCoreResult<Option<Bytes>> {
        if read.next_offset >= read.end_offset {
            return Ok(None);
        }
        let remaining = read.end_offset - read.next_offset;
        let read_len = remaining.min(u64::from(read.frame_size));
        let expected_len = usize::try_from(read_len)
            .map_err(|_| WorkerError::InvalidArgument("read length does not fit in usize".to_string()))?;
        let block_store = Arc::clone(&self.block_store);
        let group_name = read.group_name.clone();
        let block_id = read.block_id;
        let offset = read.next_offset;
        let io_pin = read.read_pin.clone();
        let store_started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            let _pin = io_pin;
            block_store.read_at(&group_name, block_id, offset, read_len)
        })
        .await
        .map_err(|error| WorkerError::Internal(format!("block read task failed: {error}")))?;
        let data = match result {
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
                return Err(error);
            }
        };
        if data.len() != expected_len {
            return Err(WorkerError::Corrupt(format!(
                "block read returned {} bytes, expected {expected_len}",
                data.len()
            )));
        }
        read.next_offset = read
            .next_offset
            .checked_add(
                u64::try_from(data.len())
                    .map_err(|_| WorkerError::InvalidArgument("read chunk length does not fit in u64".to_string()))?,
            )
            .ok_or_else(|| WorkerError::InvalidArgument("read cursor overflow".to_string()))?;
        Ok(Some(data))
    }

    pub async fn write_frame(&self, frame: WriteFrame) -> WorkerCoreResult<WriteFrameResult> {
        self.write_stream(frame).await
    }

    pub async fn write_stream(&self, frame: WriteFrame) -> WorkerCoreResult<WriteFrameResult> {
        let mut operation = self.write_operation(frame.stream_id).await?;
        let expected_seq = operation
            .last_acked_seq
            .checked_add(1)
            .ok_or_else(|| WorkerError::InvalidArgument("write stream sequence overflow".to_string()))?;
        if frame.seq != expected_seq {
            return Ok(rejected_write_frame(&operation));
        }
        if frame.offset_in_block != operation.cursor {
            return Ok(rejected_write_frame(&operation));
        }
        if frame.data.is_empty() {
            return Ok(rejected_write_frame(&operation));
        }
        let len = u64::try_from(frame.data.len())
            .map_err(|_| WorkerError::InvalidArgument("write frame length does not fit in u64".to_string()))?;
        let written_through = frame
            .offset_in_block
            .checked_add(len)
            .ok_or_else(|| WorkerError::InvalidArgument("write frame offset overflow".to_string()))?;
        if written_through > operation.context.end_offset {
            return Ok(rejected_write_frame(&operation));
        }

        let store_started = Instant::now();
        match self.block_store.write_at(
            &operation.context.group_name,
            operation.context.block_id,
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
        operation.cursor = written_through;
        operation.last_acked_seq = frame.seq;
        operation.written_through = written_through;
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
        if seq > u64::from(u32::MAX) {
            return Err(WorkerError::ResourceExhausted(
                "stream id sequence exhausted".to_string(),
            ));
        }
        Ok(StreamId::new(self.stream_id_prefix | u128::from(seq)))
    }

    async fn write_operation(&self, stream_id: StreamId) -> WorkerCoreResult<StreamOperation> {
        let operation = self
            .stream_manager
            .begin_operation(stream_id)
            .await
            .map_err(|error| stream_access_error("write", stream_id, error))?;
        Ok(operation)
    }
}

fn stream_access_error(mode: &str, stream_id: StreamId, error: StreamAccessError) -> WorkerError {
    match error {
        StreamAccessError::Missing => WorkerError::NotFound(format!("{mode} stream not found: stream_id={stream_id}")),
        StreamAccessError::Retiring => {
            WorkerError::NotFound(format!("{mode} stream is retiring: stream_id={stream_id}"))
        }
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
        req.block_size,
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
    if token != state.context.fencing_token {
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
    use std::collections::BTreeMap;
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;

    use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
    use beryl_types::chunk::ByteRange;
    use beryl_types::ids::{BlockId, BlockIndex, ClientId, InodeId, StreamId};
    use beryl_types::layout::BlockFormatId;
    use beryl_types::lease::FencingToken;
    use beryl_types::{GroupName, Tier, WorkerRunId};
    use bytes::Bytes;
    use tempfile::TempDir;

    use crate::config::StoreDirConfig;
    use crate::data::core::{
        AbortWriteRequest, ActiveBlockRead, CommitWriteRequest, RangeMapper, ReadBlockRequest,
        SyncCommittedBlockRequest, WorkerCore, WorkerCoreResult, WriteFrame, WriteOpenRequest,
    };
    use crate::error::WorkerError;
    use crate::store::block::{
        BlockMetaPayload, ChecksumKind, CreateStagingBlockRequest, FullBlockFileStore, FullBlockFileStoreConfig,
        LocalBlockStore, PublishReadyRequest, ReclaimBlockRequest, ReclaimBlockResult, ReclaimBlockState,
        RecoveredBlock, StoreResult, SyncReadyBlockRequest,
    };
    use crate::store::dirs::StoreDirs;

    const BLOCK_SIZE: u64 = 4096;
    const CHUNK_SIZE: u32 = 1024;
    const BLOCK_STAMP: u64 = 55;

    fn block_id() -> BlockId {
        BlockId::new(InodeId::new(7), BlockIndex::new(3))
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

    struct BlockingReadStore {
        inner: Arc<FullBlockFileStore>,
        started: Mutex<Option<mpsc::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl LocalBlockStore for BlockingReadStore {
        fn create_staging_block(&self, req: CreateStagingBlockRequest) -> StoreResult<BlockMetaPayload> {
            self.inner.create_staging_block(req)
        }

        fn write_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()> {
            self.inner.write_at(group_name, block_id, offset, data)
        }

        fn publish_ready(&self, req: PublishReadyRequest) -> StoreResult<BlockMetaPayload> {
            self.inner.publish_ready(req)
        }

        fn read_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes> {
            if let Some(started) = self.started.lock().expect("read started sender").take() {
                started.send(()).expect("report blocking read start");
                self.release
                    .lock()
                    .expect("read release receiver")
                    .recv()
                    .expect("release read");
            }
            self.inner.read_at(group_name, block_id, offset, len)
        }

        fn load_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
            self.inner.load_meta(group_name, block_id)
        }

        fn sync_ready_block(&self, req: SyncReadyBlockRequest) -> StoreResult<BlockMetaPayload> {
            self.inner.sync_ready_block(req)
        }

        fn recover_block(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<RecoveredBlock> {
            self.inner.recover_block(group_name, block_id)
        }

        fn inspect_reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState> {
            self.inner.inspect_reclaim_block(req)
        }

        fn reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockResult> {
            self.inner.reclaim_block(req)
        }

        fn abort_staging_block(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<()> {
            self.inner.abort_staging_block(group_name, block_id)
        }
    }

    #[test]
    fn stream_ids_are_scoped_to_the_worker_process_run() {
        let temp = TempDir::new().expect("tempdir");
        let store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
            temp.path().to_path_buf(),
        )));
        let first = WorkerCore::with_local_store_for_run(
            512,
            2048,
            Duration::from_secs(60),
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            store.clone(),
        );
        let second = WorkerCore::with_local_store_for_run(
            512,
            2048,
            Duration::from_secs(60),
            "650e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            store,
        );

        assert_ne!(first.next_stream_id().unwrap(), second.next_stream_id().unwrap());
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

    fn read_block_request(offset: u64, len: u32, block_stamp: u64, frame_size: u32) -> ReadBlockRequest {
        read_block_request_for_len(offset, len, block_stamp, BLOCK_SIZE, frame_size)
    }

    fn read_block_request_for_len(
        offset: u64,
        len: u32,
        block_stamp: u64,
        effective_len: u64,
        frame_size: u32,
    ) -> ReadBlockRequest {
        ReadBlockRequest {
            group_name: group_name(),
            block_id: block_id(),
            byte_range: ByteRange { offset, len },
            block_stamp,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len,
            frame_size,
        }
    }

    async fn collect_core_read(core: &WorkerCore, mut read: ActiveBlockRead) -> Bytes {
        let mut out = Vec::new();
        while let Some(chunk) = core.read_block_chunk(&mut read).await.expect("read block chunk") {
            out.extend_from_slice(&chunk);
        }
        Bytes::from(out)
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
        let open = core.open_write(write_open_request()).await.expect("open write");

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

        let read = core
            .begin_block_read(read_block_request_for_len(
                0,
                effective_len as u32,
                BLOCK_STAMP,
                effective_len,
                600,
            ))
            .await
            .expect("begin block read");
        assert_eq!(collect_core_read(&core, read).await, data);

        let eof_read = core
            .begin_block_read(read_block_request_for_len(
                effective_len,
                0,
                BLOCK_STAMP,
                effective_len,
                600,
            ))
            .await
            .expect("begin eof read");
        assert!(collect_core_read(&core, eof_read).await.is_empty());
        assert_invalid_argument(
            core.begin_block_read(read_block_request_for_len(
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
            core.begin_block_read(read_block_request(0, 1, BLOCK_STAMP, 512)).await,
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
        assert!(
            core.abort_write(AbortWriteRequest {
                stream_id: open.stream_id,
                ..abort_write_request()
            })
            .await
            .expect("repeated abort")
            .aborted
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

        assert!(
            core.abort_write(AbortWriteRequest {
                stream_id: open.stream_id,
                ..abort_write_request()
            })
            .await
            .expect("abort after commit")
            .aborted
        );

        let scanned = store.scan_group_blocks(&group_name()).expect("scan group");
        assert_eq!(scanned.len(), 1);
        assert_eq!(store.read_at(&group_name(), block_id(), 0, BLOCK_SIZE).unwrap(), data);
    }

    #[tokio::test]
    async fn rejected_abort_does_not_retire_the_open_stream() {
        let (_temp, store, core) = core_with_store(512, 2048);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let mut wrong_abort = abort_write_request();
        wrong_abort.stream_id = open.stream_id;
        wrong_abort.token = FencingToken::new(block_id(), ClientId::new(99), 11);

        assert!(matches!(
            core.abort_write(wrong_abort).await,
            Err(WorkerError::Fencing(_))
        ));
        assert!(core.stream_manager().get(open.stream_id).await.is_some());

        core.abort_write(AbortWriteRequest {
            stream_id: open.stream_id,
            ..abort_write_request()
        })
        .await
        .expect("valid abort after rejection");
        let paths = store.paths(&group_name(), block_id());
        assert!(!paths.staging_data_path.exists());
        assert!(!paths.staging_meta_path.exists());
    }

    #[tokio::test]
    async fn concurrent_abort_is_idempotent_across_exact_removal() {
        let temp = TempDir::new().expect("tempdir");
        let store = Arc::new(
            StoreDirs::open(
                BTreeMap::from([(
                    "hdd0".to_string(),
                    StoreDirConfig {
                        path: temp.path().join("hdd0"),
                        tier: Tier::Hdd,
                        capacity_bytes: 64 * 1024,
                    },
                )]),
                0,
                30_000,
            )
            .expect("open store dirs"),
        );
        let core = Arc::new(WorkerCore::with_local_store(
            512,
            2048,
            Duration::from_secs(60),
            store.clone(),
        ));
        let open = core.open_write(write_open_request()).await.expect("open write");
        let terminal = core
            .stream_manager()
            .begin_operation(open.stream_id)
            .await
            .expect("hold first terminal operation");
        let second_core = Arc::clone(&core);
        let second = tokio::spawn(async move {
            second_core
                .abort_write(AbortWriteRequest {
                    stream_id: open.stream_id,
                    ..abort_write_request()
                })
                .await
        });
        tokio::task::yield_now().await;

        terminal.mark_retiring();
        store
            .abort_staging_block(&terminal.context.group_name, terminal.context.block_id)
            .expect("first abort local cleanup");
        assert!(core.stream_manager().complete_retirement(open.stream_id, &terminal));
        drop(terminal);

        assert!(
            second
                .await
                .expect("second abort task")
                .expect("idempotent second abort")
                .aborted
        );
        assert_eq!(store.report().expect("store report").pending_bytes, 0);
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn shutdown_drain_aborts_open_write_before_deadline() {
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

        let forced = core
            .drain_streams_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;

        assert!(!forced);
        assert_eq!(core.stream_manager().active_count().await, 0);
        let paths = store.paths(&group_name(), block_id());
        assert!(!paths.staging_data_path.exists());
        assert!(!paths.staging_meta_path.exists());
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
    async fn reclaim_waits_for_read_rpc_lifetime_before_deleting() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let core = Arc::new(core);
        let read = core
            .begin_block_read(read_block_request(0, BLOCK_SIZE as u32, BLOCK_STAMP, 512))
            .await
            .expect("begin pinned read");
        let reclaim_core = Arc::clone(&core);
        let reclaim = tokio::spawn(async move {
            reclaim_core
                .reclaim_block(ReclaimBlockRequest {
                    group_name: group_name(),
                    block_id: block_id(),
                    expected_block_stamp: BLOCK_STAMP,
                })
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match core.begin_block_read(read_block_request(0, 1, BLOCK_STAMP, 512)).await {
                    Err(WorkerError::RefreshMetadata {
                        kind: ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                        ..
                    }) => break,
                    Ok(extra) => drop(extra),
                    Err(other) => panic!("unexpected read-open result while reclaim starts: {other:?}"),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reclaim should reject new readers");
        assert!(!reclaim.is_finished(), "reclaim must wait for the active stream");

        assert_eq!(collect_core_read(&core, read).await, payload());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), reclaim)
                .await
                .expect("reclaim should finish after EOS")
                .expect("reclaim task")
                .expect("reclaim result"),
            ReclaimBlockResult::Deleted {
                effective_len: BLOCK_SIZE
            }
        );
        let paths = store.paths(&group_name(), block_id());
        assert!(!paths.data_path.exists());
        assert!(!paths.meta_path.exists());
        assert!(!paths.deleting_marker_path.exists());
    }

    #[tokio::test]
    async fn cancelled_read_keeps_pin_until_blocking_io_exits() {
        let temp = TempDir::new().expect("tempdir");
        let inner = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
            temp.path().to_path_buf(),
        )));
        publish_ready_block(&inner, payload(), BLOCK_STAMP);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let store: Arc<dyn LocalBlockStore + Send + Sync> = Arc::new(BlockingReadStore {
            inner,
            started: Mutex::new(Some(started_tx)),
            release: Mutex::new(release_rx),
        });
        let core = Arc::new(WorkerCore::with_local_store(512, 2048, Duration::from_secs(60), store));
        let mut read = core
            .begin_block_read(read_block_request(0, 8, BLOCK_STAMP, 512))
            .await
            .expect("begin read");
        let read_core = Arc::clone(&core);
        let read_task = tokio::spawn(async move { read_core.read_block_chunk(&mut read).await });
        tokio::task::spawn_blocking(move || started_rx.recv().expect("blocking read started"))
            .await
            .expect("wait for blocking read");
        read_task.abort();
        assert!(read_task.await.expect_err("read task cancelled").is_cancelled());

        let reclaim_core = Arc::clone(&core);
        let reclaim = tokio::spawn(async move {
            reclaim_core
                .reclaim_block(ReclaimBlockRequest {
                    group_name: group_name(),
                    block_id: block_id(),
                    expected_block_stamp: BLOCK_STAMP,
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match core.begin_block_read(read_block_request(0, 1, BLOCK_STAMP, 512)).await {
                    Err(WorkerError::RefreshMetadata {
                        kind: ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                        ..
                    }) => break,
                    Ok(extra) => drop(extra),
                    Err(other) => panic!("unexpected read result while reclaim starts: {other:?}"),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reclaim starts");
        assert!(!reclaim.is_finished(), "reclaim passed a cancelled blocking read");

        release_tx.send(()).expect("release blocking read");
        assert!(tokio::time::timeout(Duration::from_secs(1), reclaim)
            .await
            .expect("reclaim completes after blocking IO")
            .expect("reclaim task")
            .is_ok());
    }

    #[tokio::test]
    async fn reclaim_stamp_mismatch_keeps_ready_block_readable() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        assert_refresh_metadata(
            core.reclaim_block(ReclaimBlockRequest {
                group_name: group_name(),
                block_id: block_id(),
                expected_block_stamp: BLOCK_STAMP + 1,
            })
            .await,
            ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
        );
        let read = core
            .begin_block_read(read_block_request(0, 8, BLOCK_STAMP, 512))
            .await
            .expect("stamp mismatch must not fence valid reads");
        assert_eq!(collect_core_read(&core, read).await, payload().slice(0..8));
    }

    #[tokio::test]
    async fn worker_core_uses_configured_store_dir() {
        let custom_dir = TempDir::new().expect("custom store dir");
        let other_dir = TempDir::new().expect("other store dir");
        let store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(custom_dir.path().to_path_buf()));
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        let core = WorkerCore::with_options(512, 2048, Duration::from_secs(60), custom_dir.path().to_path_buf());

        let read = core
            .begin_block_read(read_block_request(0, 8, BLOCK_STAMP, 512))
            .await
            .expect("begin read from configured store dir");
        assert_eq!(collect_core_read(&core, read).await, payload().slice(0..8));

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
    async fn read_block_rejects_invalid_authority_and_ranges() {
        let (_temp, store, core) = core_with_store(512, 2048);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        let mut block_size_mismatch = read_block_request(0, 1024, BLOCK_STAMP, 512);
        block_size_mismatch.block_size = BLOCK_SIZE * 2;
        let mut chunk_size_mismatch = read_block_request(0, 1024, BLOCK_STAMP, 512);
        chunk_size_mismatch.chunk_size = CHUNK_SIZE * 2;
        let cases = [
            (
                "stamp mismatch",
                read_block_request(0, 1024, BLOCK_STAMP + 1, 512),
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
            ("zero block stamp", read_block_request(0, 1024, 0, 512), None),
            (
                "out of bounds range",
                read_block_request(4090, 16, BLOCK_STAMP, 512),
                None,
            ),
        ];

        for (case, request, refresh_error) in cases {
            let result = core.begin_block_read(request).await;
            if let Some(expected) = refresh_error {
                assert_refresh_metadata(result, expected);
            } else {
                assert_invalid_argument(result);
            }
            let _ = case;
        }
    }

    #[tokio::test]
    async fn read_block_rejects_missing_block() {
        let (_temp, _store, core) = core_with_store(512, 2048);

        assert_refresh_metadata(
            core.begin_block_read(read_block_request(0, 1024, BLOCK_STAMP, 512))
                .await,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        );
    }

    #[tokio::test]
    async fn read_block_emits_bounded_chunks_and_implicit_eof() {
        let (_temp, store, core) = core_with_store(8, 16);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let mut read = core
            .begin_block_read(read_block_request(2, 8, BLOCK_STAMP, 3))
            .await
            .expect("begin read");

        assert_eq!(core.read_block_chunk(&mut read).await.unwrap(), Some(data.slice(2..5)));
        assert_eq!(core.read_block_chunk(&mut read).await.unwrap(), Some(data.slice(5..8)));
        assert_eq!(core.read_block_chunk(&mut read).await.unwrap(), Some(data.slice(8..10)));
        assert_eq!(core.read_block_chunk(&mut read).await.unwrap(), None);
    }

    #[tokio::test]
    async fn read_block_range_and_eof_boundaries_are_exact() {
        let (_temp, store, core) = core_with_store(513, 2048);
        let effective_len = u64::from(CHUNK_SIZE) * 2 + 17;
        let data = payload().slice(0..effective_len as usize);
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);

        let boundary_offset = u64::from(CHUNK_SIZE) - 3;
        for (offset, len, frame_size) in [(17, 100, 64), (boundary_offset, 10, 4), (effective_len, 0, 64)] {
            let read = core
                .begin_block_read(read_block_request_for_len(
                    offset,
                    len,
                    BLOCK_STAMP,
                    effective_len,
                    frame_size,
                ))
                .await
                .expect("begin valid range");
            assert_eq!(
                collect_core_read(&core, read).await,
                data.slice(offset as usize..offset as usize + len as usize)
            );
        }

        for (offset, len) in [(effective_len, 1), (effective_len - 1, 2)] {
            assert_invalid_argument(
                core.begin_block_read(read_block_request_for_len(offset, len, BLOCK_STAMP, effective_len, 64))
                    .await,
            );
        }
    }
}
