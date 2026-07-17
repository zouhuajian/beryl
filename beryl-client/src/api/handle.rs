// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public reader and writer handles.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_proto::metadata::WriteSyncModeProto;
use beryl_types::DataHandleId;
use bytes::{Bytes, BytesMut};
use tokio::sync::Mutex;

use crate::error::{invalid_response, ClientError, ClientResult};
use crate::metrics::ClientMetric;
use crate::planner;
use crate::runtime::{
    classify_error, is_unknown_session_barrier_outcome, mark_session_after_metadata_error, metric_labels,
    refresh_hint_from_error, ClientRuntime, ErrorClass, OperationContext, OperationDeadline,
};
use crate::session::write_session::{WorkerCommitLevel, WriteSession};

const MAX_CONVENIENCE_READ_CHUNK: u32 = 8 * 1024 * 1024;

/// A reader for an immutable file snapshot opened through the filesystem client.
#[derive(Clone)]
pub struct FileReader {
    /// Shared runtime used to refresh metadata and access workers for this handle.
    runtime: Arc<ClientRuntime>,
    handle: ReadHandle,
}

impl FileReader {
    pub(crate) fn new(runtime: Arc<ClientRuntime>, handle: ReadHandle) -> Self {
        Self { runtime, handle }
    }

    /// Returns the namespace path used to open this file snapshot.
    pub fn path(&self) -> &str {
        self.handle.path()
    }

    /// Returns the file size observed when this reader was opened.
    pub fn size_hint(&self) -> u64 {
        self.handle.size_hint()
    }

    /// Reads a range from the opened file snapshot.
    pub async fn read_at(&self, offset: u64, len: u32) -> ClientResult<Bytes> {
        self.read_at_with_deadline(offset, len, self.runtime.executor.operation_deadline())
            .await
    }

    async fn read_at_with_deadline(&self, offset: u64, len: u32, deadline: OperationDeadline) -> ClientResult<Bytes> {
        let Some(requested_range) = planner::requested_range(offset, len, self.handle.size_hint())? else {
            return Ok(Bytes::new());
        };
        let content_revision = self.handle.content_revision();
        let data_handle_id = self.handle.data_handle_id();
        let operation = OperationContext::new_named(
            self.runtime.executor.client_id(),
            self.runtime.executor.client_name(),
            "Read",
            Some(self.handle.path().to_string()),
            deadline,
        )?;
        for attempt_index in 0..self.runtime.config.retry.max_attempts() {
            let layout = self
                .runtime
                .executor
                .read_layout_for_data_handle(
                    self.handle.path(),
                    data_handle_id,
                    requested_range.file_offset,
                    requested_range.len,
                    operation.deadline().clone(),
                )
                .await?;
            let (group_name, block_reads) = planner::plan_block_reads_from_layout(
                data_handle_id,
                Some(content_revision),
                requested_range,
                &layout,
            )?;
            let ctx = self.runtime.data_context(&operation, attempt_index as u32);
            match self
                .runtime
                .worker_rpc_with_timeout(
                    &operation,
                    self.runtime.data_plane.read_block_ranges(ctx, group_name, &block_reads),
                )
                .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(err) => {
                    let class = classify_error(&err);
                    self.runtime.record_error_metric("Read", "worker", &class);
                    let has_next = attempt_index + 1 < self.runtime.config.retry.max_attempts();
                    match class.clone() {
                        ErrorClass::RefreshMetadata(reason) if has_next && should_replan_after_worker_error(&err) => {
                            self.runtime.executor.record_data_refresh(
                                &operation,
                                reason,
                                &refresh_hint_from_error(&err),
                            )?;
                            self.runtime.record_metric(
                                ClientMetric::RetryAttempt,
                                metric_labels("Read", "worker").with_error_class(class.label()),
                            );
                        }
                        ErrorClass::RefreshMetadata(_) => return Err(err),
                        ErrorClass::RetryableTransport | ErrorClass::ServerRetry if has_next => {
                            self.runtime.record_metric(
                                ClientMetric::RetryAttempt,
                                metric_labels("Read", "worker").with_error_class(class.label()),
                            );
                            self.runtime.sleep_before_retry(attempt_index, &operation).await?;
                        }
                        ErrorClass::RetryableTransport | ErrorClass::ServerRetry => {
                            self.runtime.record_metric(
                                ClientMetric::RetryExhausted,
                                metric_labels("Read", "worker").with_error_class(class.label()),
                            );
                            return Err(err);
                        }
                        ErrorClass::UnknownOutcome => {
                            self.runtime.record_metric(
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

    /// Reads the entire opened file snapshot into one buffer.
    pub async fn read_all(&self) -> ClientResult<Bytes> {
        let size = self.handle.size_hint();
        if size == 0 {
            return Ok(Bytes::new());
        }
        let capacity = usize::try_from(size)
            .map_err(|_| ClientError::InvalidArgument("file is too large to read into one buffer".to_string()))?;
        let mut output = BytesMut::with_capacity(capacity);
        let mut offset = 0u64;
        let deadline = self.runtime.executor.operation_deadline();
        while offset < size {
            let len = (size - offset).min(u64::from(MAX_CONVENIENCE_READ_CHUNK)) as u32;
            let bytes = self.read_at_with_deadline(offset, len, deadline.clone()).await?;
            ensure_exact_read(offset, len, &bytes)?;
            output.extend_from_slice(&bytes);
            offset += u64::from(len);
        }
        Ok(output.freeze())
    }

    /// Reads exactly `len` bytes from `offset`, failing if the file snapshot ends first.
    pub async fn read_exact_at(&self, offset: u64, len: u32) -> ClientResult<Bytes> {
        let bytes = self
            .read_at_with_deadline(offset, len, self.runtime.executor.operation_deadline())
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
    /// Shared runtime used to publish metadata barriers and access workers.
    runtime: Arc<ClientRuntime>,
    handle: WriteHandle,
}

impl FileWriter {
    pub(crate) fn new(runtime: Arc<ClientRuntime>, handle: WriteHandle) -> Self {
        Self { runtime, handle }
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
        let deadline = self.runtime.executor.operation_deadline();
        let session_ref = self.handle.write_session();
        let mut session = session_ref.lock().await;
        self.renew_lease_if_needed(&mut session, deadline.clone()).await?;
        session.ensure_open_for_write()?;
        if data.is_empty() {
            return Ok(());
        }

        let blocks = buffer_write(&mut session, data)?;
        for block in blocks {
            self.runtime.write_block(&mut session, block, deadline.clone()).await?;
        }
        self.handle.store_write_cursor(session.cursor());
        Ok(())
    }

    /// Publishes the written prefix for visibility while keeping the writer open.
    pub async fn sync_write_visibility(&mut self) -> ClientResult<()> {
        self.sync_write_barrier(WriteSyncModeProto::WriteSyncModeVisibility)
            .await
    }

    /// Publishes the written prefix for durability while keeping the writer open.
    pub async fn sync_write_durability(&mut self) -> ClientResult<()> {
        self.sync_write_barrier(WriteSyncModeProto::WriteSyncModeDurability)
            .await
    }

    /// Renews the writer lease while keeping the write session open.
    pub async fn renew_lease(&mut self) -> ClientResult<()> {
        let deadline = self.runtime.executor.operation_deadline();
        let session_ref = self.handle.write_session();
        let mut session = session_ref.lock().await;
        self.renew_lease_locked(&mut session, deadline).await
    }

    async fn renew_lease_if_needed(&self, session: &mut WriteSession, deadline: OperationDeadline) -> ClientResult<()> {
        let config = &self.runtime.config.write_lease;
        if !config.auto_renew || !session.should_renew_lease(config.renew_before_expiry_ms)? {
            return Ok(());
        }
        self.renew_lease_locked(session, deadline).await
    }

    async fn renew_lease_locked(&self, session: &mut WriteSession, deadline: OperationDeadline) -> ClientResult<()> {
        session.ensure_open_for_renew()?;
        let path = session.path().to_string();
        let write_handle = session.write_handle();
        self.runtime.record_metric(
            ClientMetric::LeaseRenewAttempt,
            metric_labels("RenewLease", "metadata").with_outcome("attempt"),
        );
        match self.runtime.executor.renew_lease(&path, write_handle, deadline).await {
            Ok(response) => {
                let expires_at_ms = valid_write_session_expiry("RenewLease", response.expires_at_ms)?;
                session.update_expires_at_ms(expires_at_ms);
                self.runtime.record_metric(
                    ClientMetric::LeaseRenewSuccess,
                    metric_labels("RenewLease", "metadata").with_outcome("success"),
                );
                Ok(())
            }
            Err(err) => {
                mark_session_after_metadata_error(session, &err);
                let class = classify_error(&err);
                self.runtime.record_error_metric("RenewLease", "metadata", &class);
                self.runtime.record_metric(
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
        let deadline = self.runtime.executor.operation_deadline();
        let session_ref = self.handle.write_session();
        let mut session = session_ref.lock().await;
        self.renew_lease_if_needed(&mut session, deadline.clone()).await?;
        session.ensure_open_for_close()?;
        let path = session.path().to_string();
        self.flush_pending_bytes(&mut session, deadline.clone()).await?;
        let final_size = session.cursor();
        let committed_blocks = self
            .runtime
            .commit_pending_blocks_for_barrier(&mut session, WorkerCommitLevel::CLOSE_REQUIRED, deadline.clone())
            .await?;

        let retrying_unknown_commit = session.is_commit_unknown();
        let plan = session.prepare_commit_file(
            self.runtime.executor.client_id(),
            self.runtime.executor.client_name(),
            committed_blocks,
            final_size,
            deadline,
        )?;
        if retrying_unknown_commit {
            self.runtime.record_metric(
                ClientMetric::CommitUnknownRetry,
                metric_labels("CommitFile", "metadata").with_outcome("retry"),
            );
        }
        match self.runtime.executor.commit_file(plan).await {
            Ok(response) => {
                validate_commit_file_size(response.committed_size, final_size)?;
                session.mark_closed(response.content_revision);
                Ok(())
            }
            Err(err) if is_unknown_session_barrier_outcome(&err) => {
                session.mark_commit_unknown();
                self.runtime.record_metric(
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
                self.runtime.record_error_metric("CommitFile", "metadata", &class);
                Err(err)
            }
        }
    }

    /// Aborts this writer's open write session and reports cleanup failures.
    pub async fn abort(&mut self) -> ClientResult<()> {
        let session_ref = self.handle.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_open_for_abort()?;
        session.discard_buffered_bytes();
        let plan = session.prepare_abort_cleanup(
            self.runtime.executor.client_id(),
            self.runtime.executor.client_name(),
            self.runtime.executor.operation_deadline(),
        )?;
        let mut abort_error = None;
        self.runtime.record_metric(
            ClientMetric::AbortAttempt,
            metric_labels("AbortFileWrite", "metadata").with_outcome("attempt"),
        );
        for cleanup in plan.worker_cleanups() {
            let operation = cleanup.operation();
            let ctx = self.runtime.data_context(&operation, 0);
            if let Err(err) = self
                .runtime
                .worker_rpc_with_timeout(
                    &operation,
                    self.runtime
                        .data_plane
                        .abort_block_write(ctx, cleanup.block_write_handle()),
                )
                .await
            {
                abort_error.get_or_insert(err);
            }
        }
        if let Err(err) = self
            .runtime
            .executor
            .abort_file_write(plan.metadata_operation(), plan.metadata_write_handle())
            .await
        {
            abort_error.get_or_insert(self.runtime.normalize_outcome_error("AbortFileWrite", "metadata", err));
        }
        match abort_error {
            Some(err) => {
                session.mark_abort_unknown();
                let normalized = self.runtime.normalize_outcome_error("AbortWrite", "worker", err);
                let metric = if matches!(normalized, ClientError::UnknownOutcome(_)) {
                    ClientMetric::AbortUnknown
                } else {
                    ClientMetric::AbortFailure
                };
                self.runtime
                    .record_metric(metric, metric_labels("AbortWrite", "worker").with_outcome("unknown"));
                Err(normalized)
            }
            None => {
                session.mark_aborted();
                self.runtime.record_metric(
                    ClientMetric::AbortSuccess,
                    metric_labels("AbortFileWrite", "metadata").with_outcome("success"),
                );
                Ok(())
            }
        }
    }

    /// Flushes worker data to the requested level and publishes the metadata sync barrier.
    async fn sync_write_barrier(&mut self, mode: WriteSyncModeProto) -> ClientResult<()> {
        let deadline = self.runtime.executor.operation_deadline();
        let session_ref = self.handle.write_session();
        let mut session = session_ref.lock().await;
        self.renew_lease_if_needed(&mut session, deadline.clone()).await?;
        session.ensure_open_for_barrier()?;
        let path = session.path().to_string();
        self.flush_pending_bytes(&mut session, deadline.clone()).await?;
        let target_size = session.cursor();
        let required_level = sync_write_required_commit_level(mode)?;
        let committed_blocks = self
            .runtime
            .commit_pending_blocks_for_barrier(&mut session, required_level, deadline.clone())
            .await?;
        match self
            .runtime
            .executor
            .sync_write(&session, committed_blocks, target_size, mode, deadline)
            .await
        {
            Ok(response) => {
                validate_sync_write_size(response.synced_size, target_size)?;
                let content_revision = response.content_revision.ok_or_else(|| {
                    ClientError::Metadata("SyncWriteResponseProto.content_revision missing".to_string())
                })?;
                session.update_published_state(content_revision, target_size);
                self.handle.store_write_cursor(session.cursor());
                Ok(())
            }
            Err(err) => {
                let class = classify_error(&err);
                if is_unknown_session_barrier_outcome(&err) {
                    session.mark_unknown_outcome();
                    self.runtime.record_metric(
                        ClientMetric::UnknownOutcome,
                        metric_labels("SyncWrite", "metadata").with_outcome("unknown"),
                    );
                    return Err(ClientError::UnknownOutcome(format!(
                        "SyncWrite outcome is unknown for path {}: {}",
                        path, err
                    )));
                }
                mark_session_after_metadata_error(&mut session, &err);
                self.runtime.record_error_metric("SyncWrite", "metadata", &class);
                Err(err)
            }
        }
    }

    /// Writes the buffered tail block, if the session currently has one.
    async fn flush_pending_bytes(&self, session: &mut WriteSession, deadline: OperationDeadline) -> ClientResult<()> {
        if let Some(block) = session.take_buffered_tail() {
            self.runtime.write_block(session, block, deadline).await?;
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

/// Buffers incoming bytes and returns complete blocks ready for worker writes.
fn buffer_write(session: &mut WriteSession, data: Bytes) -> ClientResult<Vec<Bytes>> {
    let mut blocks = Vec::new();
    let mut offset = 0usize;
    let block_size = session.block_size_usize();
    while offset < data.len() {
        if session.buffered_len() == 0 && data.len() - offset >= block_size {
            let end = offset + block_size;
            session.advance_cursor(block_size)?;
            blocks.push(data.slice(offset..end));
            offset = end;
            continue;
        }

        let needed = block_size - session.buffered_len();
        let len = needed.min(data.len() - offset);
        session.buffer_bytes(&data[offset..offset + len])?;
        offset += len;
        if let Some(block) = session.take_full_buffered_block() {
            blocks.push(block);
        }
    }
    Ok(blocks)
}

/// Maps a public sync mode to the worker commit level required before metadata publication.
fn sync_write_required_commit_level(mode: WriteSyncModeProto) -> ClientResult<WorkerCommitLevel> {
    match mode {
        WriteSyncModeProto::WriteSyncModeDurability => Ok(WorkerCommitLevel::Durable),
        WriteSyncModeProto::WriteSyncModeVisibility => Ok(WorkerCommitLevel::Visible),
        WriteSyncModeProto::WriteSyncModeUnspecified => Err(ClientError::InvalidArgument(
            "SyncWrite mode must be visibility or durability".to_string(),
        )),
    }
}

fn validate_commit_file_size(committed_size: u64, final_size: u64) -> ClientResult<()> {
    if committed_size < final_size {
        return Err(invalid_response(
            "CommitFile",
            format!(
                "committed_size {} is smaller than final_size {}",
                committed_size, final_size
            ),
        ));
    }
    Ok(())
}

fn validate_sync_write_size(synced_size: u64, target_size: u64) -> ClientResult<()> {
    if synced_size < target_size {
        return Err(invalid_response(
            "SyncWrite",
            format!(
                "synced_size {} is smaller than target_size {}",
                synced_size, target_size
            ),
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct ReadHandle {
    path: String,
    data_handle_id: DataHandleId,
    content_revision: u64,
    file_size: u64,
}

impl ReadHandle {
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn size_hint(&self) -> u64 {
        self.file_size
    }

    pub(crate) fn new(path: String, data_handle_id: DataHandleId, content_revision: u64, file_size: u64) -> Self {
        Self {
            path,
            data_handle_id,
            content_revision,
            file_size,
        }
    }

    pub(crate) fn data_handle_id(&self) -> DataHandleId {
        self.data_handle_id
    }

    pub(crate) fn content_revision(&self) -> u64 {
        self.content_revision
    }
}

impl fmt::Debug for ReadHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadHandle")
            .field("path", &self.path())
            .field("size_hint", &self.size_hint())
            .finish()
    }
}

pub(crate) struct WriteHandle {
    path: String,
    write_session: Arc<Mutex<WriteSession>>,
    write_cursor: Arc<AtomicU64>,
}

impl WriteHandle {
    pub(crate) fn new(path: String, base_size: u64, session: WriteSession) -> Self {
        Self {
            path,
            write_session: Arc::new(Mutex::new(session)),
            write_cursor: Arc::new(AtomicU64::new(base_size)),
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

pub(crate) fn valid_write_session_expiry(operation: &'static str, expires_at_ms: u64) -> ClientResult<u64> {
    if expires_at_ms == 0 {
        return Err(invalid_response(operation, "expires_at_ms must be non-zero"));
    }
    Ok(expires_at_ms)
}

impl fmt::Debug for WriteHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteHandle")
            .field("path", &self.path())
            .field("cursor", &self.write_cursor())
            .finish()
    }
}
