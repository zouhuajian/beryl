// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public writer handle.

use crate::client_inner::{
    is_unknown_session_barrier_outcome, mark_session_after_metadata_error, metric_labels, ClientInner,
};
use crate::error::ClientResult;
use crate::metrics::ClientMetric;
use crate::runtime::OperationDeadline;
use crate::session::write_session::WriteSession;
use crate::worker::BlockWrite;
use bytes::Bytes;
use std::fmt::{Debug, Formatter, Result};
use std::sync::Arc;

/// A uniquely owned sequential write session created through the filesystem client.
///
/// Dropping an open writer cancels its active Worker stream locally. Metadata
/// retains authority over the write session until an explicit close or abort,
/// or until its lease expires.
#[must_use = "dropping a FileWriter leaves the metadata session to lease expiry; call close or abort explicitly"]
pub struct FileWriter {
    /// Shared client owner used to publish metadata barriers and access Workers.
    inner: Arc<ClientInner>,
    /// Sole mutable owner of Metadata session identity and lifecycle state.
    session: WriteSession,
    /// The sole acknowledged Worker RPC for the block at the current cursor.
    active_block: Option<BlockWrite>,
}

impl FileWriter {
    /// Creates the sole public owner of one Metadata write session.
    pub(crate) fn new(inner: Arc<ClientInner>, session: WriteSession) -> Self {
        Self {
            inner,
            session,
            active_block: None,
        }
    }

    /// Returns the namespace path associated with this write session.
    pub fn path(&self) -> &str {
        self.session.path()
    }

    /// Returns the next sequential write offset for this writer.
    pub fn cursor(&self) -> u64 {
        self.session.cursor()
    }

    /// Writes all supplied bytes at the current sequential cursor.
    pub async fn write_all(&mut self, data: Bytes) -> ClientResult<()> {
        let deadline = self.inner.metadata.operation_deadline();
        if let Some(block) = self.active_block.as_mut() {
            if let Err(error) = self.inner.check_block_write(&mut self.session, block, &deadline).await {
                let _ = self.cancel_block_write(&deadline).await;
                return Err(error);
            }
        }
        self.renew_lease_if_needed(deadline.clone()).await?;
        self.session.ensure_open_for_write()?;
        if data.is_empty() {
            return Ok(());
        }

        let mut offset = 0usize;
        while offset < data.len() {
            if self.active_block.is_none() {
                self.active_block = Some(self.inner.open_block_write(&mut self.session, deadline.clone()).await?);
            }
            let block = self.active_block.as_mut().expect("block write was just opened");
            let remaining = usize::try_from(block.remaining()).unwrap_or(usize::MAX);
            let frame_len = (data.len() - offset)
                .min(remaining)
                .min(beryl_proto::DEFAULT_WORKER_DATA_FRAME_SIZE);
            let frame = Bytes::copy_from_slice(&data[offset..offset + frame_len]);
            if let Err(error) = self
                .inner
                .write_block_frame(&mut self.session, block, frame, &deadline)
                .await
            {
                let _ = self.cancel_block_write(&deadline).await;
                return Err(error);
            }
            offset += frame_len;
            if self.active_block.as_ref().is_some_and(|block| block.remaining() == 0) {
                let block = self.active_block.take().expect("full block write is present");
                self.inner
                    .finish_block_write(&mut self.session, block, &deadline)
                    .await?;
            }
        }
        Ok(())
    }

    /// Makes all accepted bytes durable and visible while keeping the writer open.
    ///
    /// If the result is unknown, call `sync` again on the same writer. The
    /// retry reuses the original operation identity and exact publication
    /// payload; all other writer operations remain blocked until it resolves.
    pub async fn sync(&mut self) -> ClientResult<()> {
        let deadline = self.inner.metadata.operation_deadline();
        self.renew_lease_if_needed(deadline.clone()).await?;
        self.session.ensure_open_for_sync()?;
        let path = self.session.path().to_string();
        self.finish_pending_block(&deadline).await?;
        let target_size = self.session.cursor();
        let committed_blocks = self.inner.committed_blocks_for_barrier(&self.session);
        let plan = self.session.prepare_sync_write(
            self.inner.metadata.client_id(),
            self.inner.metadata.client_name(),
            committed_blocks,
            target_size,
            deadline,
        )?;
        match self.inner.metadata.sync_write(plan).await {
            Ok(generation) => {
                self.session.mark_sync_completed(generation, target_size)?;
                Ok(())
            }
            Err(err) if is_unknown_session_barrier_outcome(&err) => {
                self.inner.record_metric(
                    ClientMetric::UnknownOutcome,
                    metric_labels("SyncWrite", "metadata").with_outcome("unknown"),
                );
                let message = format!("SyncWrite outcome is unknown for path {path}: {err}");
                Err(err.with_unknown_outcome_name("SyncWrite", message))
            }
            Err(err) => {
                mark_session_after_metadata_error(&mut self.session, &err);
                self.inner.record_error_metric("SyncWrite", "metadata", &err);
                Err(err)
            }
        }
    }

    /// Renews the writer lease while keeping the write session open.
    pub async fn renew_lease(&mut self) -> ClientResult<()> {
        let deadline = self.inner.metadata.operation_deadline();
        self.renew_lease_locked(deadline).await
    }

    async fn renew_lease_if_needed(&mut self, deadline: OperationDeadline) -> ClientResult<()> {
        let config = &self.inner.config;
        if !config.automatic_lease_renewal() || !self.session.should_renew_lease(config.lease_renewal_threshold_ms())? {
            return Ok(());
        }
        self.renew_lease_locked(deadline).await
    }

    async fn renew_lease_locked(&mut self, deadline: OperationDeadline) -> ClientResult<()> {
        self.session.ensure_open_for_renew()?;
        let path = self.session.path().to_string();
        let write_handle = self.session.write_handle();
        self.inner.record_metric(
            ClientMetric::LeaseRenewAttempt,
            metric_labels("RenewLease", "metadata").with_outcome("attempt"),
        );
        match self.inner.metadata.renew_lease(&path, write_handle, deadline).await {
            Ok(expires_at_ms) => {
                let block_lease_update = self
                    .active_block
                    .as_ref()
                    .map(|block| block.update_lease_expiry(expires_at_ms))
                    .transpose();
                self.session.update_expires_at_ms(expires_at_ms);
                self.inner.record_metric(
                    ClientMetric::LeaseRenewSuccess,
                    metric_labels("RenewLease", "metadata").with_outcome("success"),
                );
                if let Err(error) = block_lease_update {
                    self.session.mark_unknown_outcome();
                    return Err(error);
                }
                Ok(())
            }
            Err(err) => {
                mark_session_after_metadata_error(&mut self.session, &err);
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
        self.renew_lease_if_needed(deadline.clone()).await?;
        self.session.ensure_open_for_close()?;
        let path = self.session.path().to_string();
        self.finish_pending_block(&deadline).await?;
        let final_size = self.session.cursor();
        let committed_blocks = self.inner.committed_blocks_for_barrier(&self.session);

        let retrying_unknown_commit = self.session.is_commit_unknown();
        let plan = self.session.prepare_commit_file(
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
                self.session.mark_closed();
                Ok(())
            }
            Err(err) if is_unknown_session_barrier_outcome(&err) => {
                self.session.mark_commit_unknown();
                self.inner.record_metric(
                    ClientMetric::UnknownOutcome,
                    metric_labels("CommitFile", "metadata").with_outcome("unknown"),
                );
                let message = format!("CommitFile outcome is unknown for path {path}: {err}");
                Err(err.with_unknown_outcome_name("CommitFile", message))
            }
            Err(err) => {
                mark_session_after_metadata_error(&mut self.session, &err);
                self.inner.record_error_metric("CommitFile", "metadata", &err);
                Err(err)
            }
        }
    }

    /// Aborts this writer's open write session and reports cleanup failures.
    pub async fn abort(&mut self) -> ClientResult<()> {
        let deadline = self.inner.metadata.operation_deadline();
        self.session.ensure_open_for_abort()?;
        self.cancel_block_write(&deadline).await?;
        let plan = self.session.prepare_abort_cleanup(
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
            self.session.mark_abort_unknown();
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
        self.session.mark_aborted();
        self.inner.record_metric(
            ClientMetric::AbortSuccess,
            metric_labels("AbortFileWrite", "metadata").with_outcome("success"),
        );
        Ok(())
    }

    /// Half-closes the current partial block and waits for Worker Ready before a
    /// Metadata visibility or commit barrier.
    async fn finish_pending_block(&mut self, deadline: &OperationDeadline) -> ClientResult<()> {
        if let Some(block) = self.active_block.take() {
            self.inner
                .finish_block_write(&mut self.session, block, deadline)
                .await?;
        }
        Ok(())
    }

    /// Cancels an unfinished block RPC before abandoning its request stream.
    async fn cancel_block_write(&mut self, deadline: &OperationDeadline) -> ClientResult<()> {
        if let Some(block) = self.active_block.take() {
            self.inner.cancel_block_write(block, deadline).await?;
        }
        Ok(())
    }
}

impl Debug for FileWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("FileWriter")
            .field("path", &self.path())
            .field("cursor", &self.cursor())
            .finish()
    }
}
