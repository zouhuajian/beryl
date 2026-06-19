// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public reader and writer handles.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use proto::metadata::WriteSyncModeProto;
use tokio::sync::Mutex;
use types::DataHandleId;

use super::runtime::{
    is_unknown_session_barrier_outcome, mark_session_after_metadata_error, metric_labels, refresh_hint_from_error,
    ClientRuntime,
};
use crate::error::{invalid_response, ClientError, ClientResult};
use crate::metrics::ClientMetric;
use crate::planner;
use crate::runtime::{ErrorClass, ErrorClassifier, OperationContext, OperationIdentity, OperationKind, RefreshReason};
use crate::session::write_session::{WorkerCommitLevel, WriteSession};

/// A reader for an immutable file snapshot opened through the filesystem client.
#[derive(Clone)]
pub struct FileReader {
    /// Shared runtime used to refresh metadata and access workers for this handle.
    runtime: Arc<ClientRuntime>,
    inner: ReadHandle,
}

impl FileReader {
    pub(crate) fn new(runtime: Arc<ClientRuntime>, inner: ReadHandle) -> Self {
        Self { runtime, inner }
    }

    /// Returns the namespace path used to open this file snapshot.
    pub fn path(&self) -> &str {
        self.inner.path()
    }

    /// Returns the file size observed when this reader was opened.
    pub fn size_hint(&self) -> u64 {
        self.inner.size_hint()
    }

    /// Reads a range from the opened file snapshot.
    pub async fn read_at(&self, offset: u64, len: u32) -> ClientResult<Bytes> {
        let Some(requested_range) = planner::requested_range(offset, len, self.inner.size_hint())? else {
            return Ok(Bytes::new());
        };
        let file_version = self.inner.file_version();
        let data_handle_id = self.inner.data_handle_id();
        let operation = OperationContext::new_named(
            self.runtime.executor.client_id(),
            self.runtime.executor.client_name(),
            OperationKind::WorkerReadData,
            "Read",
            OperationIdentity::path(self.inner.path().to_string()),
        )?;
        let mut retry_used = 0usize;
        let mut refresh_used = 0usize;
        let retry_budget = self.runtime.config.retry.max_retry_attempts();
        let refresh_budget = self.runtime.config.refresh.max_refresh_attempts;
        let mut attempt = 0u32;
        loop {
            let layout = self
                .runtime
                .executor
                .read_layout_for_data_handle(
                    self.inner.path(),
                    data_handle_id,
                    requested_range.file_offset,
                    requested_range.len,
                )
                .await?;
            let (group_name, block_reads) =
                planner::plan_block_reads_from_layout(data_handle_id, Some(file_version), requested_range, &layout)?;
            let ctx = self.runtime.data_context(&operation, attempt);
            match self
                .runtime
                .worker_rpc_with_timeout(
                    "Read",
                    OperationKind::WorkerReadData,
                    self.runtime.data_plane.read_block_ranges(ctx, group_name, &block_reads),
                )
                .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(err) => {
                    let class = ErrorClassifier.classify_error(&err);
                    self.runtime
                        .record_error_metric("Read", OperationKind::WorkerReadData, &class);
                    match class.clone() {
                        ErrorClass::NeedRefresh(RefreshReason::Unknown) => return Err(err),
                        ErrorClass::NeedRefresh(reason) if should_replan_after_worker_error(&err) => {
                            if refresh_budget.saturating_sub(refresh_used) == 0 {
                                self.runtime.record_metric(
                                    ClientMetric::RefreshExhausted,
                                    metric_labels("Read", OperationKind::WorkerReadData)
                                        .with_refresh_reason(reason.label()),
                                );
                                return Err(ClientError::Worker(format!(
                                    "read refresh budget exhausted for {}",
                                    reason.label()
                                )));
                            }
                            if retry_budget.saturating_sub(retry_used) == 0 {
                                self.runtime.record_metric(
                                    ClientMetric::RetryExhausted,
                                    metric_labels("Read", OperationKind::WorkerReadData)
                                        .with_error_class(class.label()),
                                );
                                return Err(err);
                            }
                            self.runtime.executor.record_data_refresh(
                                &operation,
                                reason,
                                &refresh_hint_from_error(&err),
                            )?;
                            self.runtime.record_refresh_metric(
                                "Read",
                                OperationKind::WorkerReadData,
                                reason,
                                "refresh",
                            );
                            retry_used += 1;
                            refresh_used += 1;
                            attempt = attempt.saturating_add(1);
                        }
                        ErrorClass::NeedRefresh(_) => return Err(err),
                        ErrorClass::RetryableTransport => {
                            if retry_budget.saturating_sub(retry_used) == 0 {
                                self.runtime.record_metric(
                                    ClientMetric::RetryExhausted,
                                    metric_labels("Read", OperationKind::WorkerReadData)
                                        .with_error_class(class.label()),
                                );
                                return Err(err);
                            }
                            let retry_index = retry_used;
                            retry_used += 1;
                            self.runtime.record_metric(
                                ClientMetric::RetryAttempt,
                                metric_labels("Read", OperationKind::WorkerReadData).with_error_class(class.label()),
                            );
                            self.runtime
                                .sleep_before_retry(retry_index, "Read", OperationKind::WorkerReadData)
                                .await;
                            attempt = attempt.saturating_add(1);
                        }
                        ErrorClass::UnknownOutcome => {
                            self.runtime.record_metric(
                                ClientMetric::UnknownOutcome,
                                metric_labels("Read", OperationKind::WorkerReadData)
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
    }
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
        ErrorClassifier.classify_error(err),
        ErrorClass::NeedRefresh(
            RefreshReason::RouteEpochMismatch | RefreshReason::WorkerRunMismatch | RefreshReason::BlockStampMismatch
        )
    )
}

/// A writer for a sequential write session created through the filesystem client.
pub struct FileWriter {
    /// Shared runtime used to publish metadata barriers and access workers.
    runtime: Arc<ClientRuntime>,
    inner: WriteHandle,
}

impl FileWriter {
    pub(crate) fn new(runtime: Arc<ClientRuntime>, inner: WriteHandle) -> Self {
        Self { runtime, inner }
    }

    /// Returns the namespace path associated with this write session.
    pub fn path(&self) -> &str {
        self.inner.path()
    }

    /// Returns the next sequential write offset for this writer.
    pub fn cursor(&self) -> u64 {
        self.inner.write_cursor()
    }

    /// Writes all supplied bytes at the current sequential cursor.
    pub async fn write_all(&mut self, data: Bytes) -> ClientResult<()> {
        let session_ref = self.inner.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_open_for_write()?;
        if data.is_empty() {
            return Ok(());
        }

        let blocks = buffer_write(&mut session, data)?;
        for block in blocks {
            self.runtime.write_block(&mut session, block).await?;
        }
        self.inner.store_write_cursor(session.cursor());
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
        let session_ref = self.inner.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_open_for_renew()?;
        let path = session.path().to_string();
        let session_identity = session.session_identity();
        let write_handle = session.write_handle();
        self.runtime.record_metric(
            ClientMetric::LeaseRenewAttempt,
            metric_labels("RenewLease", OperationKind::MetadataSessionBarrier).with_outcome("attempt"),
        );
        match self
            .runtime
            .executor
            .renew_lease(&path, session_identity, write_handle)
            .await
        {
            Ok(response) => {
                let expires_at_ms = valid_write_session_expiry("RenewLease", response.expires_at_ms)?;
                session.update_expires_at_ms(expires_at_ms);
                self.runtime.record_metric(
                    ClientMetric::LeaseRenewSuccess,
                    metric_labels("RenewLease", OperationKind::MetadataSessionBarrier).with_outcome("success"),
                );
                Ok(())
            }
            Err(err) => {
                mark_session_after_metadata_error(&mut session, &err);
                let class = ErrorClassifier.classify_error(&err);
                self.runtime
                    .record_error_metric("RenewLease", OperationKind::MetadataSessionBarrier, &class);
                self.runtime.record_metric(
                    ClientMetric::LeaseRenewFailure,
                    metric_labels("RenewLease", OperationKind::MetadataSessionBarrier)
                        .with_error_class(class.label())
                        .with_outcome("failure"),
                );
                Err(err)
            }
        }
    }

    /// Closes the writer and commits the final file metadata.
    pub async fn close(&mut self) -> ClientResult<()> {
        let session_ref = self.inner.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_close_allowed()?;
        let path = session.path().to_string();
        self.flush_pending_bytes(&mut session).await?;
        let final_size = session.cursor();
        let committed_blocks = self
            .runtime
            .commit_pending_blocks_for_barrier(&mut session, WorkerCommitLevel::CLOSE_REQUIRED)
            .await?;

        let retrying_unknown_commit = session.is_commit_unknown();
        let plan = session.prepare_commit_file(
            self.runtime.executor.client_id(),
            self.runtime.executor.client_name(),
            committed_blocks,
            final_size,
        )?;
        if retrying_unknown_commit {
            self.runtime.record_metric(
                ClientMetric::CommitUnknownRetry,
                metric_labels("CommitFile", OperationKind::MetadataSessionBarrier).with_outcome("retry"),
            );
        }
        match self.runtime.executor.commit_file(plan).await {
            Ok(response) => {
                validate_commit_file_size(response.committed_size, final_size)?;
                session.mark_closed(response.file_version);
                Ok(())
            }
            Err(err) if is_unknown_session_barrier_outcome(&err) => {
                session.mark_commit_unknown();
                self.runtime.record_metric(
                    ClientMetric::UnknownOutcome,
                    metric_labels("CommitFile", OperationKind::MetadataSessionBarrier).with_outcome("unknown"),
                );
                Err(ClientError::UnknownOutcome(format!(
                    "CommitFile outcome is unknown for path {}: {}",
                    path, err
                )))
            }
            Err(err) => {
                mark_session_after_metadata_error(&mut session, &err);
                let class = ErrorClassifier.classify_error(&err);
                self.runtime
                    .record_error_metric("CommitFile", OperationKind::MetadataSessionBarrier, &class);
                Err(err)
            }
        }
    }

    /// Aborts this writer's open write session and reports cleanup failures.
    pub async fn abort(&mut self) -> ClientResult<()> {
        let session_ref = self.inner.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_open_for_abort()?;
        session.discard_buffered_bytes();
        let plan =
            session.prepare_abort_cleanup(self.runtime.executor.client_id(), self.runtime.executor.client_name())?;
        let mut abort_error = None;
        self.runtime.record_metric(
            ClientMetric::AbortAttempt,
            metric_labels("AbortFileWrite", OperationKind::CleanupBestEffort).with_outcome("attempt"),
        );
        for cleanup in plan.worker_cleanups() {
            let operation = cleanup.operation();
            let ctx = self.runtime.data_context(&operation, 0);
            if let Err(err) = self
                .runtime
                .worker_rpc_with_timeout(
                    "AbortWrite",
                    OperationKind::CleanupBestEffort,
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
            abort_error.get_or_insert(self.runtime.normalize_outcome_error(
                "AbortFileWrite",
                OperationKind::CleanupBestEffort,
                err,
            ));
        }
        match abort_error {
            Some(err) => {
                session.mark_abort_unknown();
                let normalized =
                    self.runtime
                        .normalize_outcome_error("AbortWrite", OperationKind::CleanupBestEffort, err);
                let metric = if matches!(normalized, ClientError::UnknownOutcome(_)) {
                    ClientMetric::AbortUnknown
                } else {
                    ClientMetric::AbortFailure
                };
                self.runtime.record_metric(
                    metric,
                    metric_labels("AbortWrite", OperationKind::CleanupBestEffort).with_outcome("unknown"),
                );
                Err(normalized)
            }
            None => {
                session.mark_aborted();
                self.runtime.record_metric(
                    ClientMetric::AbortSuccess,
                    metric_labels("AbortFileWrite", OperationKind::CleanupBestEffort).with_outcome("success"),
                );
                Ok(())
            }
        }
    }

    /// Flushes worker data to the requested level and publishes the metadata sync barrier.
    async fn sync_write_barrier(&mut self, mode: WriteSyncModeProto) -> ClientResult<()> {
        let session_ref = self.inner.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_open_for_barrier()?;
        let path = session.path().to_string();
        self.flush_pending_bytes(&mut session).await?;
        let target_size = session.cursor();
        let required_level = sync_write_required_commit_level(mode)?;
        let committed_blocks = self
            .runtime
            .commit_pending_blocks_for_barrier(&mut session, required_level)
            .await?;
        match self
            .runtime
            .executor
            .sync_write(
                &session,
                self.inner.data_handle_id(),
                committed_blocks,
                target_size,
                mode,
            )
            .await
        {
            Ok(response) => {
                validate_sync_write_size(response.synced_size, target_size)?;
                self.inner.store_write_cursor(session.cursor());
                Ok(())
            }
            Err(err) => {
                let class = ErrorClassifier.classify_error(&err);
                if is_unknown_session_barrier_outcome(&err) {
                    session.mark_unknown_outcome();
                    self.runtime.record_metric(
                        ClientMetric::UnknownOutcome,
                        metric_labels("SyncWrite", OperationKind::MetadataSessionBarrier).with_outcome("unknown"),
                    );
                    return Err(ClientError::UnknownOutcome(format!(
                        "SyncWrite outcome is unknown for path {}: {}",
                        path, err
                    )));
                }
                mark_session_after_metadata_error(&mut session, &err);
                self.runtime
                    .record_error_metric("SyncWrite", OperationKind::MetadataSessionBarrier, &class);
                Err(err)
            }
        }
    }

    /// Writes the buffered tail block, if the session currently has one.
    async fn flush_pending_bytes(&self, session: &mut WriteSession) -> ClientResult<()> {
        if let Some(block) = session.take_buffered_tail() {
            self.runtime.write_block(session, block).await?;
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
    file_version: u64,
    file_size: u64,
}

impl ReadHandle {
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn size_hint(&self) -> u64 {
        self.file_size
    }

    pub(crate) fn new(path: String, data_handle_id: DataHandleId, file_version: u64, file_size: u64) -> Self {
        Self {
            path,
            data_handle_id,
            file_version,
            file_size,
        }
    }

    pub(crate) fn data_handle_id(&self) -> DataHandleId {
        self.data_handle_id
    }

    pub(crate) fn file_version(&self) -> u64 {
        self.file_version
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
    data_handle_id: DataHandleId,
    write_session: Arc<Mutex<WriteSession>>,
    write_cursor: Arc<AtomicU64>,
}

impl WriteHandle {
    pub(crate) fn new(path: String, data_handle_id: DataHandleId, base_size: u64, session: WriteSession) -> Self {
        Self {
            path,
            data_handle_id,
            write_session: Arc::new(Mutex::new(session)),
            write_cursor: Arc::new(AtomicU64::new(base_size)),
        }
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn data_handle_id(&self) -> DataHandleId {
        self.data_handle_id
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
