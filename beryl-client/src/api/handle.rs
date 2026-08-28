// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public writer handle.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;

use crate::client_inner::{
    is_unknown_session_barrier_outcome, mark_session_after_metadata_error, metric_labels, ClientInner,
};
use crate::error::ClientResult;
use crate::metrics::ClientMetric;
use crate::runtime::OperationDeadline;
use crate::session::write_session::WriteSession;
use crate::worker::BlockWrite;

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
            Ok(expires_at_ms) => {
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
                self.inner.record_error_metric("RenewLease", "metadata", &err);
                self.inner.record_metric(
                    ClientMetric::LeaseRenewFailure,
                    metric_labels("RenewLease", "metadata")
                        .with_error_class(err.classification_label())
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
            Ok(_) => {
                session.mark_closed();
                Ok(())
            }
            Err(err) if is_unknown_session_barrier_outcome(&err) => {
                session.mark_commit_unknown();
                self.inner.record_metric(
                    ClientMetric::UnknownOutcome,
                    metric_labels("CommitFile", "metadata").with_outcome("unknown"),
                );
                let message = format!("CommitFile outcome is unknown for path {path}: {err}");
                Err(err.with_unknown_outcome_name("CommitFile", message))
            }
            Err(err) => {
                mark_session_after_metadata_error(&mut session, &err);
                self.inner.record_error_metric("CommitFile", "metadata", &err);
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
            let metric = if normalized.is_outcome_unknown() {
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
            Ok(content_revision) => {
                session.update_published_state(content_revision, target_size);
                self.handle.store_write_cursor(session.cursor());
                Ok(())
            }
            Err(err) => {
                if is_unknown_session_barrier_outcome(&err) {
                    session.mark_unknown_outcome();
                    self.inner.record_metric(
                        ClientMetric::UnknownOutcome,
                        metric_labels("SyncWrite", "metadata").with_outcome("unknown"),
                    );
                    let message = format!("SyncWrite outcome is unknown for path {path}: {err}");
                    return Err(err.with_unknown_outcome_name("SyncWrite", message));
                }
                mark_session_after_metadata_error(&mut session, &err);
                self.inner.record_error_metric("SyncWrite", "metadata", &err);
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
