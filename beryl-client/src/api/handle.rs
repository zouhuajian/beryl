// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public reader and writer handles.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use bytes::Bytes;
use tokio::sync::Mutex;

use crate::client_inner::{
    is_unknown_session_barrier_outcome, mark_session_after_metadata_error, metric_labels, refresh_hint_from_error,
    ClientInner,
};
use crate::error::{
    invalid_response, read_buffer_reservation_failed, side_effect_response_body_mismatch, ClientError, ClientResult,
};
use crate::metadata::ReadSnapshot;
use crate::metrics::ClientMetric;
use crate::planner;
use crate::runtime::{classify_error, ErrorClass, OperationContext, OperationDeadline};
use crate::session::write_session::WriteSession;
use crate::worker::BlockWrite;

/// A reader for an immutable file snapshot opened through the filesystem client.
#[derive(Clone)]
pub struct FileReader {
    /// Shared client owner used to refresh metadata and access Workers.
    inner: Arc<ClientInner>,
    snapshot: ReadSnapshot,
}

impl FileReader {
    /// Creates a public reader from a validated immutable Metadata snapshot.
    pub(crate) fn new(inner: Arc<ClientInner>, snapshot: ReadSnapshot) -> Self {
        Self { inner, snapshot }
    }

    /// Returns the namespace path used to open this file snapshot.
    pub fn path(&self) -> &str {
        self.snapshot.path()
    }

    /// Returns the file size observed when this reader was opened.
    pub fn size_hint(&self) -> u64 {
        self.snapshot.size_hint()
    }

    /// Reads a configured bounded range from the opened file snapshot.
    pub async fn read_at(&self, offset: u64, len: u32) -> ClientResult<Bytes> {
        self.read_at_with_deadline(offset, len, self.inner.metadata.operation_deadline())
            .await
    }

    async fn read_at_with_deadline(&self, offset: u64, len: u32, deadline: OperationDeadline) -> ClientResult<Bytes> {
        validate_read_request_size(len, self.inner.config.read.max_request_bytes)?;
        let Some(requested_range) = planner::requested_range(offset, len, self.snapshot.size_hint())? else {
            return Ok(Bytes::new());
        };
        let content_revision = self.snapshot.content_revision();
        let inode_id = self.snapshot.inode_id();
        let operation = OperationContext::new_named(
            self.inner.metadata.client_id(),
            self.inner.metadata.client_name(),
            "Read",
            Some(self.snapshot.path().to_string()),
            deadline,
        )?;
        for attempt_index in 0..self.inner.config.retry.max_attempts() {
            let layout = self
                .inner
                .metadata
                .read_layout_for_inode(
                    self.snapshot.path(),
                    inode_id,
                    requested_range.file_offset,
                    requested_range.len,
                    operation.deadline().clone(),
                )
                .await?;
            let (group_name, block_reads) =
                planner::plan_block_reads_from_layout(inode_id, Some(content_revision), requested_range, &layout)?;
            let ctx = self.inner.data_context(&operation, attempt_index as u32);
            match self
                .inner
                .worker_rpc_with_timeout(
                    &operation,
                    self.inner.worker.read_block_ranges(ctx, group_name, &block_reads),
                )
                .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(err) => {
                    let class = classify_error(&err);
                    self.inner.record_error_metric("Read", "worker", &class);
                    let has_next = attempt_index + 1 < self.inner.config.retry.max_attempts();
                    match class.clone() {
                        ErrorClass::RefreshMetadata(reason) if has_next && should_replan_after_worker_error(&err) => {
                            self.inner.metadata.record_data_refresh(
                                &operation,
                                reason,
                                &refresh_hint_from_error(&err),
                            )?;
                            self.inner.record_metric(
                                ClientMetric::RetryAttempt,
                                metric_labels("Read", "worker").with_error_class(class.label()),
                            );
                        }
                        ErrorClass::RefreshMetadata(_) => return Err(err),
                        ErrorClass::RetryableTransport | ErrorClass::ServerRetry if has_next => {
                            self.inner.record_metric(
                                ClientMetric::RetryAttempt,
                                metric_labels("Read", "worker").with_error_class(class.label()),
                            );
                            self.inner.sleep_before_retry(attempt_index, &operation).await?;
                        }
                        ErrorClass::RetryableTransport | ErrorClass::ServerRetry => {
                            self.inner.record_metric(
                                ClientMetric::RetryExhausted,
                                metric_labels("Read", "worker").with_error_class(class.label()),
                            );
                            return Err(err);
                        }
                        ErrorClass::UnknownOutcome => {
                            self.inner.record_metric(
                                ClientMetric::UnknownOutcome,
                                metric_labels("Read", "worker")
                                    .with_error_class(class.label())
                                    .with_outcome("unknown"),
                            );
                            return Err(err);
                        }
                        _ => return Err(err),
                    }
                }
            }
        }
        unreachable!("read attempt loop always returns on the final attempt")
    }

    /// Reads the entire opened file snapshot when it fits the configured
    /// owned-buffer limit.
    pub async fn read_all(&self) -> ClientResult<Bytes> {
        let size = self.snapshot.size_hint();
        if size == 0 {
            return Ok(Bytes::new());
        }
        validate_read_all_size(size, self.inner.config.read.max_buffered_bytes)?;
        let capacity = usize::try_from(size)
            .map_err(|_| ClientError::InvalidArgument("file is too large to read into one buffer".to_string()))?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(capacity)
            .map_err(|error| read_buffer_reservation_failed("read_all", capacity, error))?;
        let mut offset = 0u64;
        let deadline = self.inner.metadata.operation_deadline();
        while offset < size {
            let len = (size - offset).min(u64::from(self.inner.config.read.max_request_bytes)) as u32;
            let bytes = self.read_at_with_deadline(offset, len, deadline.clone()).await?;
            ensure_exact_read(offset, len, &bytes)?;
            output.extend_from_slice(&bytes);
            offset += u64::from(len);
        }
        Ok(Bytes::from(output))
    }

    /// Reads exactly `len` bytes from `offset`, failing if the file snapshot ends first.
    pub async fn read_exact_at(&self, offset: u64, len: u32) -> ClientResult<Bytes> {
        let bytes = self
            .read_at_with_deadline(offset, len, self.inner.metadata.operation_deadline())
            .await?;
        ensure_exact_read(offset, len, &bytes)?;
        Ok(bytes)
    }
}

fn ensure_exact_read(offset: u64, len: u32, bytes: &Bytes) -> ClientResult<()> {
    if bytes.len() != len as usize {
        return Err(ClientError::InvalidArgument(format!(
            "read_exact_at requested {} bytes at offset {} but read {} bytes",
            len,
            offset,
            bytes.len()
        )));
    }
    Ok(())
}

impl fmt::Debug for FileReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileReader")
            .field("path", &self.path())
            .field("size_hint", &self.size_hint())
            .finish()
    }
}

/// Returns true when a worker read failure requires a fresh metadata layout.
fn should_replan_after_worker_error(err: &ClientError) -> bool {
    matches!(
        classify_error(err),
        ErrorClass::RefreshMetadata(
            ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch)
                | ErrorKind::Worker(WorkerErrorKind::RunMismatch | WorkerErrorKind::BlockStampMismatch)
        )
    )
}

/// A writer for a sequential write session created through the filesystem client.
pub struct FileWriter {
    /// Shared client owner used to publish metadata barriers and access Workers.
    inner: Arc<ClientInner>,
    handle: WriteHandle,
    /// The sole acknowledged Worker RPC for the block at the current cursor.
    block_write: Option<BlockWrite>,
}

impl FileWriter {
    /// Creates a public writer that owns the handle around one Metadata session.
    pub(crate) fn new(inner: Arc<ClientInner>, session: WriteSession) -> Self {
        Self {
            inner,
            handle: WriteHandle::new(session),
            block_write: None,
        }
    }

    /// Returns the namespace path associated with this write session.
    pub fn path(&self) -> &str {
        self.handle.path()
    }

    /// Returns the next sequential write offset for this writer.
    pub fn cursor(&self) -> u64 {
        self.handle.write_cursor()
    }

    /// Writes all supplied bytes at the current sequential cursor.
    pub async fn write_all(&mut self, data: Bytes) -> ClientResult<()> {
        let deadline = self.inner.metadata.operation_deadline();
        let session_ref = self.handle.write_session();
        let mut session = session_ref.lock().await;
        if let Some(block) = self.block_write.as_mut() {
            if let Err(error) = self.inner.check_block_write(&mut session, block, &deadline).await {
                let _ = self.cancel_block_write(&deadline).await;
                return Err(error);
            }
        }
        self.renew_lease_if_needed(&mut session, deadline.clone()).await?;
        session.ensure_open_for_write()?;
        if data.is_empty() {
            return Ok(());
        }

        let mut offset = 0usize;
        while offset < data.len() {
            if self.block_write.is_none() {
                self.block_write = Some(self.inner.open_block_write(&mut session, deadline.clone()).await?);
            }
            let block = self.block_write.as_mut().expect("block write was just opened");
            let remaining = usize::try_from(block.remaining()).unwrap_or(usize::MAX);
            let frame_len = (data.len() - offset)
                .min(remaining)
                .min(beryl_proto::DEFAULT_WORKER_DATA_FRAME_SIZE);
            let frame = Bytes::copy_from_slice(&data[offset..offset + frame_len]);
            if let Err(error) = self
                .inner
                .write_block_frame(&mut session, block, frame, &deadline)
                .await
            {
                let _ = self.cancel_block_write(&deadline).await;
                return Err(error);
            }
            offset += frame_len;
            if self.block_write.as_ref().is_some_and(|block| block.remaining() == 0) {
                let block = self.block_write.take().expect("full block write is present");
                self.inner.finish_block_write(&mut session, block, &deadline).await?;
            }
        }
        self.handle.store_write_cursor(session.cursor());
        Ok(())
    }

    /// Publishes the written prefix for visibility while keeping the writer open.
    pub async fn sync_write_visibility(&mut self) -> ClientResult<()> {
        self.sync_write_barrier().await
    }

    /// Publishes the written prefix for durability while keeping the writer open.
    pub async fn sync_write_durability(&mut self) -> ClientResult<()> {
        self.sync_write_barrier().await
    }

    /// Renews the writer lease while keeping the write session open.
    pub async fn renew_lease(&mut self) -> ClientResult<()> {
        let deadline = self.inner.metadata.operation_deadline();
        let session_ref = self.handle.write_session();
        let mut session = session_ref.lock().await;
        self.renew_lease_locked(&mut session, deadline).await
    }

    async fn renew_lease_if_needed(
        &mut self,
        session: &mut WriteSession,
        deadline: OperationDeadline,
    ) -> ClientResult<()> {
        let config = &self.inner.config.write_lease;
        if !config.auto_renew || !session.should_renew_lease(config.renew_before_expiry_ms)? {
            return Ok(());
        }
        self.renew_lease_locked(session, deadline).await
    }

    async fn renew_lease_locked(
        &mut self,
        session: &mut WriteSession,
        deadline: OperationDeadline,
    ) -> ClientResult<()> {
        session.ensure_open_for_renew()?;
        let path = session.path().to_string();
        let write_handle = session.write_handle();
        self.inner.record_metric(
            ClientMetric::LeaseRenewAttempt,
            metric_labels("RenewLease", "metadata").with_outcome("attempt"),
        );
        match self.inner.metadata.renew_lease(&path, write_handle, deadline).await {
            Ok(response) => {
                let expires_at_ms = valid_write_session_expiry("RenewLease", response.expires_at_ms)?;
                let block_lease_update = self
                    .block_write
                    .as_ref()
                    .map(|block| block.update_lease_expiry(expires_at_ms))
                    .transpose();
                session.update_expires_at_ms(expires_at_ms);
                self.inner.record_metric(
                    ClientMetric::LeaseRenewSuccess,
                    metric_labels("RenewLease", "metadata").with_outcome("success"),
                );
                if let Err(error) = block_lease_update {
                    session.mark_unknown_outcome();
                    return Err(error);
                }
                Ok(())
            }
            Err(err) => {
                mark_session_after_metadata_error(session, &err);
                let class = classify_error(&err);
                self.inner.record_error_metric("RenewLease", "metadata", &class);
                self.inner.record_metric(
                    ClientMetric::LeaseRenewFailure,
                    metric_labels("RenewLease", "metadata")
                        .with_error_class(class.label())
                        .with_outcome("failure"),
                );
                Err(err)
            }
        }
    }

    /// Closes the writer and commits the final file metadata.
    pub async fn close(&mut self) -> ClientResult<()> {
        let deadline = self.inner.metadata.operation_deadline();
        let session_ref = self.handle.write_session();
        let mut session = session_ref.lock().await;
        self.renew_lease_if_needed(&mut session, deadline.clone()).await?;
        session.ensure_open_for_close()?;
        let path = session.path().to_string();
        self.finish_pending_block(&mut session, &deadline).await?;
        let final_size = session.cursor();
        let committed_blocks = self.inner.committed_blocks_for_barrier(&session);

        let retrying_unknown_commit = session.is_commit_unknown();
        let plan = session.prepare_commit_file(
            self.inner.metadata.client_id(),
            self.inner.metadata.client_name(),
            committed_blocks,
            final_size,
            deadline,
        )?;
        if retrying_unknown_commit {
            self.inner.record_metric(
                ClientMetric::CommitUnknownRetry,
                metric_labels("CommitFile", "metadata").with_outcome("retry"),
            );
        }
        match self.inner.metadata.commit_file(plan).await {
            Ok(response) => {
                if let Err(error) = validate_commit_file_size(response.committed_size, final_size) {
                    session.mark_commit_unknown();
                    self.inner.record_metric(
                        ClientMetric::UnknownOutcome,
                        metric_labels("CommitFile", "metadata").with_outcome("unknown"),
                    );
                    return Err(error);
                }
                session.mark_closed();
                Ok(())
            }
            Err(err) if is_unknown_session_barrier_outcome(&err) => {
                session.mark_commit_unknown();
                self.inner.record_metric(
                    ClientMetric::UnknownOutcome,
                    metric_labels("CommitFile", "metadata").with_outcome("unknown"),
                );
                Err(ClientError::UnknownOutcome(format!(
                    "CommitFile outcome is unknown for path {}: {}",
                    path, err
                )))
            }
            Err(err) => {
                mark_session_after_metadata_error(&mut session, &err);
                let class = classify_error(&err);
                self.inner.record_error_metric("CommitFile", "metadata", &class);
                Err(err)
            }
        }
    }

    /// Aborts this writer's open write session and reports cleanup failures.
    pub async fn abort(&mut self) -> ClientResult<()> {
        let deadline = self.inner.metadata.operation_deadline();
        let session_ref = self.handle.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_open_for_abort()?;
        self.cancel_block_write(&deadline).await?;
        let plan = session.prepare_abort_cleanup(
            self.inner.metadata.client_id(),
            self.inner.metadata.client_name(),
            deadline,
        )?;
        self.inner.record_metric(
            ClientMetric::AbortAttempt,
            metric_labels("AbortFileWrite", "metadata").with_outcome("attempt"),
        );
        if let Err(err) = self
            .inner
            .metadata
            .abort_file_write(plan.metadata_operation(), plan.metadata_write_handle())
            .await
        {
            session.mark_abort_unknown();
            let normalized = self.inner.normalize_outcome_error("AbortFileWrite", "metadata", err);
            let metric = if matches!(normalized, ClientError::UnknownOutcome(_)) {
                ClientMetric::AbortUnknown
            } else {
                ClientMetric::AbortFailure
            };
            self.inner.record_metric(
                metric,
                metric_labels("AbortFileWrite", "metadata").with_outcome("unknown"),
            );
            return Err(normalized);
        }
        session.mark_aborted();
        self.inner.record_metric(
            ClientMetric::AbortSuccess,
            metric_labels("AbortFileWrite", "metadata").with_outcome("success"),
        );
        Ok(())
    }

    /// Flushes worker data to the requested level and publishes the metadata sync barrier.
    async fn sync_write_barrier(&mut self) -> ClientResult<()> {
        let deadline = self.inner.metadata.operation_deadline();
        let session_ref = self.handle.write_session();
        let mut session = session_ref.lock().await;
        self.renew_lease_if_needed(&mut session, deadline.clone()).await?;
        session.ensure_open_for_barrier()?;
        let path = session.path().to_string();
        self.finish_pending_block(&mut session, &deadline).await?;
        let target_size = session.cursor();
        let committed_blocks = self.inner.committed_blocks_for_barrier(&session);
        match self
            .inner
            .metadata
            .sync_write(&session, committed_blocks, target_size, deadline)
            .await
        {
            Ok(response) => {
                let content_revision = match validate_sync_write_response(&response, target_size) {
                    Ok(content_revision) => content_revision,
                    Err(error) => {
                        session.mark_unknown_outcome();
                        self.inner.record_metric(
                            ClientMetric::UnknownOutcome,
                            metric_labels("SyncWrite", "metadata").with_outcome("unknown"),
                        );
                        return Err(error);
                    }
                };
                session.update_published_state(content_revision, target_size);
                self.handle.store_write_cursor(session.cursor());
                Ok(())
            }
            Err(err) => {
                let class = classify_error(&err);
                if is_unknown_session_barrier_outcome(&err) {
                    session.mark_unknown_outcome();
                    self.inner.record_metric(
                        ClientMetric::UnknownOutcome,
                        metric_labels("SyncWrite", "metadata").with_outcome("unknown"),
                    );
                    return Err(ClientError::UnknownOutcome(format!(
                        "SyncWrite outcome is unknown for path {}: {}",
                        path, err
                    )));
                }
                mark_session_after_metadata_error(&mut session, &err);
                self.inner.record_error_metric("SyncWrite", "metadata", &class);
                Err(err)
            }
        }
    }

    /// Half-closes the current partial block and waits for Worker Ready before a
    /// Metadata visibility or commit barrier.
    async fn finish_pending_block(
        &mut self,
        session: &mut WriteSession,
        deadline: &OperationDeadline,
    ) -> ClientResult<()> {
        if let Some(block) = self.block_write.take() {
            self.inner.finish_block_write(session, block, deadline).await?;
        }
        Ok(())
    }

    /// Cancels an unfinished block RPC before abandoning its request stream.
    async fn cancel_block_write(&mut self, deadline: &OperationDeadline) -> ClientResult<()> {
        if let Some(block) = self.block_write.take() {
            self.inner.cancel_block_write(block, deadline).await?;
        }
        Ok(())
    }
}

impl fmt::Debug for FileWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileWriter")
            .field("path", &self.path())
            .field("cursor", &self.cursor())
            .finish()
    }
}

/// Accepts a successful commit body only when it proves the exact frozen
/// publication intent.
fn validate_commit_file_size(committed_size: u64, final_size: u64) -> ClientResult<()> {
    if committed_size != final_size {
        return Err(side_effect_response_body_mismatch(
            "CommitFile",
            format!(
                "committed_size {} does not equal final_size {}",
                committed_size, final_size
            ),
        ));
    }
    Ok(())
}

/// Validates the complete state needed to advance a writer after SyncWrite.
fn validate_sync_write_response(
    response: &beryl_proto::metadata::SyncWriteResponseProto,
    target_size: u64,
) -> ClientResult<u64> {
    if response.synced_size != target_size {
        return Err(side_effect_response_body_mismatch(
            "SyncWrite",
            format!(
                "synced_size {} does not equal target_size {}",
                response.synced_size, target_size
            ),
        ));
    }
    response
        .content_revision
        .ok_or_else(|| side_effect_response_body_mismatch("SyncWrite", "content_revision missing"))
}

/// Rejects a successful lease response that cannot identify a live expiry.
fn valid_write_session_expiry(operation: &'static str, expires_at_ms: u64) -> ClientResult<u64> {
    if expires_at_ms == 0 {
        return Err(invalid_response(operation, "expires_at_ms must be non-zero"));
    }
    Ok(expires_at_ms)
}

/// Rejects a positioned read before planning or RPC when its declared result
/// could exceed the configured owned-buffer limit.
fn validate_read_request_size(requested_bytes: u32, max_bytes: u32) -> ClientResult<()> {
    if requested_bytes > max_bytes {
        return Err(ClientError::InvalidArgument(format!(
            "read request size {requested_bytes} exceeds configured maximum {max_bytes}"
        )));
    }
    Ok(())
}

/// Rejects a whole-file convenience read before allocation or RPC.
fn validate_read_all_size(file_size: u64, max_bytes: u64) -> ClientResult<()> {
    if file_size > max_bytes {
        return Err(ClientError::InvalidArgument(format!(
            "file size {file_size} exceeds configured read_all maximum {max_bytes}"
        )));
    }
    Ok(())
}

/// Shared mutable wrapper retained until `FileWriter` directly owns its
/// session in PR 7.
pub(crate) struct WriteHandle {
    path: String,
    write_session: Arc<Mutex<WriteSession>>,
    write_cursor: Arc<AtomicU64>,
}

impl WriteHandle {
    /// Wraps one Metadata-created session without introducing another source
    /// of path or cursor truth at construction.
    pub(crate) fn new(session: WriteSession) -> Self {
        let path = session.path().to_string();
        let cursor = session.cursor();
        Self {
            path,
            write_session: Arc::new(Mutex::new(session)),
            write_cursor: Arc::new(AtomicU64::new(cursor)),
        }
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn write_session(&self) -> Arc<Mutex<WriteSession>> {
        Arc::clone(&self.write_session)
    }

    pub(crate) fn write_cursor(&self) -> u64 {
        self.write_cursor.load(Ordering::SeqCst)
    }

    pub(crate) fn store_write_cursor(&self, cursor: u64) {
        self.write_cursor.store(cursor, Ordering::SeqCst);
    }
}

impl fmt::Debug for WriteHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteHandle")
            .field("path", &self.path())
            .field("cursor", &self.write_cursor())
            .finish()
    }
}
