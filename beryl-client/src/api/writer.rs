// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Sequential file writes, Worker durability, and Metadata publication.

use crate::client_inner::{metric_labels, ClientInner};
use crate::error::{side_effect_response_body_mismatch, ClientError, ClientErrorKind, ClientResult};
use crate::metrics::{self, ClientMetric};
use crate::runtime::{is_definite_worker_capacity_rejection, Operation, OperationContext, OperationDeadline};
use crate::session::WriteSession;
use crate::worker::{BlockWrite, BlockWriteInput};
use beryl_common::error::rpc::RecoveryAction;
use bytes::Bytes;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc::OwnedPermit;

/// A uniquely owned sequential file writer with native [`ClientResult`] methods.
///
/// Writes accept bytes into a bounded Worker request stream. [`Self::position`]
/// counts accepted bytes, including progress before failure or cancellation.
/// [`Self::flush`] confirms Worker durability, [`Self::sync`] also publishes
/// visibility, and [`Self::close`] publishes the final contents and ends the lease.
///
/// Cancelling pending write preparation, renewal, or a Worker checkpoint makes
/// this writer unusable for further writes or publication. Drop it and leave
/// cleanup to lease expiry. Once a Metadata sync, commit, or abort request has
/// been frozen, cancellation retains that identity and payload for an explicit
/// retry of the same method. A retry gets a new bounded wait budget.
///
/// Each write step and each flush, sync, or close has a bounded deadline.
/// Callers control the total timeout of `write_all`.
/// Dropping a writer cancels local Worker activity without committing or aborting
/// Metadata authority. The current transport requires a Tokio runtime.
///
/// ```no_run
/// use beryl_client::{ClientResult, FsClient};
///
/// # async fn example(client: &FsClient) -> ClientResult<()> {
/// let mut writer = client.create("/file").await?;
/// writer.write_all(b"visible prefix").await?;
/// writer.sync().await?;
/// writer.write_all(b" and suffix").await?;
/// writer.close().await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "dropping an open writer does not commit it"]
pub struct FileWriter {
    inner: Arc<ClientInner>,
    session: WriteSession,
    active_block: Option<BlockWrite>,
}

impl FileWriter {
    pub(crate) fn new(inner: Arc<ClientInner>, session: WriteSession) -> Self {
        Self {
            inner,
            session,
            active_block: None,
        }
    }

    /// Returns the namespace path used to create or append this file.
    pub fn path(&self) -> &str {
        self.session.path()
    }

    /// Returns the offset after all accepted bytes, independent of durability
    /// and Metadata visibility.
    pub fn position(&self) -> u64 {
        self.session.position()
    }

    /// Accepts a prefix of `data`. An empty slice returns zero without IO.
    /// Bytes are accepted only after block preparation completes.
    pub async fn write(&mut self, data: &[u8]) -> ClientResult<usize> {
        async {
            if data.is_empty() {
                return Ok(0);
            }
            let deadline = self.inner.metadata.operation_deadline();
            let cancellation = CancelWrite::new(self);
            let result = cancellation.writer.prepare_block(deadline).await;
            let permit = cancellation.complete(result)?;

            // No await separates accepting bytes from recording their position.
            let block = self.active_block.as_mut().expect("prepared block");
            let len = data
                .len()
                .min(block.remaining() as usize)
                .min(beryl_proto::DEFAULT_WORKER_DATA_FRAME_SIZE);
            let next_position = self
                .session
                .position()
                .checked_add(len as u64)
                .ok_or_else(|| ClientError::invalid_argument("write position overflow"))?;
            if let Err(error) = block.write_reserved(permit, Bytes::copy_from_slice(&data[..len])) {
                mark_session_after_write_error(&mut self.session, &error);
                self.active_block.take();
                return Err(self.inner.normalize_outcome_error("WriteBlock", "worker", error));
            }
            self.session.set_position(next_position);
            Ok(len)
        }
        .await
        .inspect_err(|error| metrics::record_unknown_outcome("WriteBlock", "worker", error))
    }

    /// Accepts all bytes through sequential writes. On error or cancellation,
    /// `position()` includes the accepted prefix; do not replay it blindly.
    pub async fn write_all(&mut self, mut data: &[u8]) -> ClientResult<()> {
        while !data.is_empty() {
            let len = self.write(data).await?;
            data = &data[len..];
        }
        Ok(())
    }

    /// Confirms Worker durability without publishing data. Empty flush does not
    /// allocate a block. Flush after a successful close is a no-op.
    pub async fn flush(&mut self) -> ClientResult<()> {
        async {
            if self.session.is_closed() {
                return Ok(());
            }
            let deadline = self.inner.metadata.operation_deadline();
            self.renew_lease_if_needed(deadline.clone()).await?;
            self.session.ensure_open_for_write()?;
            self.finish_block(&deadline).await
        }
        .await
        .inspect_err(|error| metrics::record_unknown_outcome("WriteBlock", "worker", error))
    }

    /// Renews the open lease. Idle writers are not renewed in the background.
    pub async fn renew_lease(&mut self) -> ClientResult<()> {
        let deadline = self.inner.metadata.operation_deadline();
        self.renew_lease_with_deadline(deadline)
            .await
            .inspect_err(|error| metrics::record_unknown_outcome("RenewLease", "metadata", error))
    }

    /// Makes all accepted bytes durable and visible while keeping the writer open.
    ///
    /// If the Metadata publication result is unknown, retry on the same writer. The
    /// retry reuses the original operation identity and exact publication
    /// payload; all other writer operations remain blocked until it resolves.
    pub async fn sync(&mut self) -> ClientResult<()> {
        async {
            let deadline = self.inner.metadata.operation_deadline();
            self.renew_lease_if_needed(deadline.clone()).await?;
            self.session.ensure_open_for_sync()?;
            let path = self.session.path().to_string();
            self.finish_block(&deadline).await?;
            let target_len = self.session.position();
            let plan = self.session.prepare_sync_write(
                self.inner.metadata.client_id(),
                self.inner.metadata.client_name(),
                deadline,
            );
            match self.inner.metadata.sync_write(plan).await {
                Ok(generation) => {
                    self.session.mark_sync_completed(generation, target_len);
                    Ok(())
                }
                Err(err) if err.is_outcome_unknown() => {
                    let message = format!("SyncWrite outcome is unknown for path {path}: {err}");
                    Err(err.with_unknown_outcome_name("SyncWrite", message))
                }
                Err(err) => {
                    mark_session_after_metadata_error(&mut self.session, &err);
                    metrics::record_error("SyncWrite", "metadata", &err);
                    Err(err)
                }
            }
        }
        .await
        .inspect_err(|error| metrics::record_unknown_outcome("SyncWrite", "metadata", error))
    }

    /// Publishes the final contents and ends the lease. Successful close is
    /// idempotent. Retry an unknown Metadata commit with this same writer.
    pub async fn close(&mut self) -> ClientResult<()> {
        async {
            let deadline = self.inner.metadata.operation_deadline();
            if self.session.is_closed() {
                return Ok(());
            }
            self.renew_lease_if_needed(deadline.clone()).await?;
            self.session.ensure_open_for_close()?;
            let path = self.session.path().to_string();
            self.finish_block(&deadline).await?;

            let retrying_unknown_commit = self.session.is_commit_pending();
            let plan = self.session.prepare_commit_file(
                self.inner.metadata.client_id(),
                self.inner.metadata.client_name(),
                deadline,
            );
            if retrying_unknown_commit {
                metrics::record(
                    ClientMetric::CommitUnknownRetry,
                    metric_labels("CommitFile", "metadata").with_outcome("retry"),
                );
            }
            match self.inner.metadata.commit_file(plan).await {
                Ok(()) => {
                    self.session.mark_closed();
                    Ok(())
                }
                Err(err)
                    if retrying_unknown_commit
                        || err.is_outcome_unknown()
                        || err.kind() == ClientErrorKind::Internal =>
                {
                    // A later fence or missing receipt cannot establish the outcome
                    // of the original attempt, including a cancelled close future.
                    // Internal failures also lack proof that Raft did not apply.
                    let message = format!("CommitFile outcome is unknown for path {path}: {err}");
                    Err(err.with_unknown_outcome_name("CommitFile", message))
                }
                Err(err) => {
                    mark_session_after_metadata_error(&mut self.session, &err);
                    metrics::record_error("CommitFile", "metadata", &err);
                    Err(err)
                }
            }
        }
        .await
        .inspect_err(|error| metrics::record_unknown_outcome("CommitFile", "metadata", error))
    }

    /// Aborts this writer's open write session and reports cleanup failures.
    pub async fn abort(&mut self) -> ClientResult<()> {
        async {
            let deadline = self.inner.metadata.operation_deadline();
            self.session.ensure_open_for_abort()?;
            let operation = self.session.prepare_abort(
                self.inner.metadata.client_id(),
                self.inner.metadata.client_name(),
                deadline.clone(),
            );
            self.cancel_block_write(&deadline).await?;
            metrics::record(
                ClientMetric::AbortAttempt,
                metric_labels("AbortFileWrite", "metadata").with_outcome("attempt"),
            );
            if let Err(err) = self
                .inner
                .metadata
                .abort_file_write(operation, self.session.write_handle())
                .await
            {
                let normalized = self.inner.normalize_outcome_error("AbortFileWrite", "metadata", err);
                let metric = if normalized.is_outcome_unknown() {
                    ClientMetric::AbortUnknown
                } else {
                    ClientMetric::AbortFailure
                };
                metrics::record(
                    metric,
                    metric_labels("AbortFileWrite", "metadata").with_outcome("unknown"),
                );
                return Err(normalized);
            }
            self.session.mark_aborted();
            metrics::record(
                ClientMetric::AbortSuccess,
                metric_labels("AbortFileWrite", "metadata").with_outcome("success"),
            );
            Ok(())
        }
        .await
        .inspect_err(|error| metrics::record_unknown_outcome("AbortFileWrite", "metadata", error))
    }

    async fn prepare_block(&mut self, deadline: OperationDeadline) -> ClientResult<OwnedPermit<BlockWriteInput>> {
        if let Some(block) = self.active_block.as_mut() {
            if let Err(error) = self
                .inner
                .worker_write_step_with_timeout(&deadline, block.check_open())
                .await
            {
                mark_session_after_write_error(&mut self.session, &error);
                let _ = self.cancel_block_write(&deadline).await;
                return Err(self.inner.normalize_outcome_error("WriteBlock", "worker", error));
            }
        }
        self.renew_lease_if_needed(deadline.clone()).await?;
        self.session.ensure_open_for_write()?;
        if self.active_block.as_ref().is_some_and(|block| block.remaining() == 0) {
            self.finish_block(&deadline).await?;
        }
        if self.active_block.is_none() {
            self.active_block = Some(self.open_block_write(deadline.clone()).await?);
        }
        let block = self.active_block.as_mut().expect("opened block");
        match self
            .inner
            .worker_write_step_with_timeout(&deadline, block.reserve())
            .await
        {
            Ok(permit) => Ok(permit),
            Err(error) => {
                mark_session_after_write_error(&mut self.session, &error);
                let _ = self.cancel_block_write(&deadline).await;
                Err(self.inner.normalize_outcome_error("WriteBlock", "worker", error))
            }
        }
    }

    /// Reopens the partial tail or allocates a new block, then crosses the Worker acknowledgement boundary.
    /// Only an explicit capacity rejection before side effects permits a retry.
    async fn open_block_write(&mut self, deadline: OperationDeadline) -> ClientResult<BlockWrite> {
        let (allocate_block_operation, allocate_block) = if let Some((group_name, block)) = self.session.reusable_tail()
        {
            (
                OperationContext::new_named(
                    self.inner.metadata.client_id(),
                    self.inner.metadata.client_name(),
                    Operation::WriteBlock,
                    deadline.clone(),
                ),
                crate::metadata::model::AllocateBlockResult { group_name, block },
            )
        } else {
            match self
                .inner
                .metadata
                .allocate_block(
                    self.session.write_handle(),
                    self.session.previous_block_id(),
                    deadline.clone(),
                )
                .await
            {
                Ok(allocate_block) => allocate_block,
                Err(err) => {
                    mark_session_after_write_error(&mut self.session, &err);
                    return Err(self.inner.normalize_outcome_error("AllocateBlock", "metadata", err));
                }
            }
        };
        if let Err(err) = self.session.validate_target(&allocate_block.block) {
            self.session.mark_unknown_outcome();
            metrics::record(
                ClientMetric::WorkerResponseBodyMismatch,
                metric_labels("AllocateBlock", "metadata").with_outcome("unknown"),
            );
            return Err(side_effect_response_body_mismatch("AllocateBlock", err)
                .with_operation_context(&allocate_block_operation));
        }
        self.session.record_write_group(allocate_block.group_name.clone());
        let operation = OperationContext::new_named(
            self.inner.metadata.client_id(),
            self.inner.metadata.client_name(),
            Operation::WriteBlock,
            deadline,
        );
        let lease_expires_at_ms = self.session.expires_at_ms();
        for attempt_index in 0..self.inner.config.max_attempts() {
            let ctx = operation.clone();
            match self
                .inner
                .worker_rpc_with_timeout(
                    &operation,
                    self.inner.worker.open_write_block(
                        ctx,
                        allocate_block.group_name.clone(),
                        allocate_block.block.clone(),
                        lease_expires_at_ms,
                    ),
                )
                .await
            {
                Ok(block) => return Ok(block),
                Err(err) if is_definite_worker_capacity_rejection(&err) => {
                    let has_next = attempt_index + 1 < self.inner.config.max_attempts();
                    if !has_next {
                        return Err(err.with_operation_context(&operation));
                    }
                    metrics::record(
                        ClientMetric::RetryAttempt,
                        metric_labels("WriteBlock", "worker").with_error_class("server_retry"),
                    );
                    self.inner.sleep_before_retry(attempt_index, &operation).await?;
                }
                Err(err) => {
                    mark_session_after_write_error(&mut self.session, &err);
                    return Err(self.inner.normalize_outcome_error("WriteBlock", "worker", err));
                }
            }
        }
        unreachable!("client retry configuration requires at least one attempt")
    }

    /// Waits for local block cancellation only within the current public
    /// operation; the detached completion task retains its lease bound.
    async fn cancel_block(&self, block: BlockWrite, deadline: &OperationDeadline) -> ClientResult<()> {
        if block.cancel(deadline.remaining()).await {
            return Ok(());
        }
        self.inner.record_worker_timeout("WriteBlock");
        Err(ClientError::from(tonic::Status::deadline_exceeded(
            "WriteBlock cancellation timed out",
        )))
    }

    async fn renew_lease_if_needed(&mut self, deadline: OperationDeadline) -> ClientResult<()> {
        let config = &self.inner.config;
        if !config.automatic_lease_renewal() || !self.session.should_renew_lease(config.lease_renewal_threshold_ms())? {
            return Ok(());
        }
        self.renew_lease_with_deadline(deadline).await
    }

    async fn renew_lease_with_deadline(&mut self, deadline: OperationDeadline) -> ClientResult<()> {
        self.session.ensure_open_for_renew()?;
        let write_handle = self.session.write_handle();
        metrics::record(
            ClientMetric::LeaseRenewAttempt,
            metric_labels("RenewLease", "metadata").with_outcome("attempt"),
        );
        let cancellation = CancelWrite::new(self);
        let result = cancellation
            .writer
            .inner
            .metadata
            .renew_lease(write_handle, deadline)
            .await;
        let result = cancellation.complete(result);
        match result {
            Ok(expires_at_ms) => {
                let block_lease_update = self
                    .active_block
                    .as_ref()
                    .map(|block| block.update_lease_expiry(expires_at_ms))
                    .transpose();
                self.session.update_expires_at_ms(expires_at_ms);
                metrics::record(
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
                metrics::record_error("RenewLease", "metadata", &err);
                metrics::record(
                    ClientMetric::LeaseRenewFailure,
                    metric_labels("RenewLease", "metadata")
                        .with_error_class(err.classification_label())
                        .with_outcome("failure"),
                );
                Err(err)
            }
        }
    }

    /// Records a durability checkpoint only after normal Worker completion.
    async fn finish_block(&mut self, deadline: &OperationDeadline) -> ClientResult<()> {
        let Some(block) = self.active_block.take() else {
            return Ok(());
        };
        let cancellation = CancelWrite::new(self);
        if !block.has_data() {
            let result = cancellation.writer.cancel_block(block, deadline).await;
            return cancellation.complete(result);
        }
        let result = cancellation
            .writer
            .inner
            .worker_write_step_with_timeout(deadline, block.finish())
            .await;
        match cancellation.complete(result) {
            Ok((target, written_len)) => {
                self.session.push_ready_block(target, written_len);
                Ok(())
            }
            Err(error) => {
                mark_session_after_write_error(&mut self.session, &error);
                Err(self.inner.normalize_outcome_error("WriteBlock", "worker", error))
            }
        }
    }

    /// Cancels an unfinished block RPC before abandoning its request stream.
    async fn cancel_block_write(&mut self, deadline: &OperationDeadline) -> ClientResult<()> {
        if let Some(block) = self.active_block.take() {
            self.cancel_block(block, deadline).await?;
        }
        Ok(())
    }
}

impl fmt::Debug for FileWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileWriter")
            .field("path", &self.path())
            .field("position", &self.position())
            .finish()
    }
}

// A cancelled ordinary IO cannot establish what the peer accepted. Publications
// use their frozen WriteSession plans instead and must never be erased here.
struct CancelWrite<'a> {
    writer: &'a mut FileWriter,
    completed: bool,
}

impl<'a> CancelWrite<'a> {
    fn new(writer: &'a mut FileWriter) -> Self {
        Self {
            writer,
            completed: false,
        }
    }

    fn complete<T>(mut self, result: T) -> T {
        self.completed = true;
        result
    }
}

impl Drop for CancelWrite<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.writer.session.mark_unknown_outcome();
            self.writer.active_block.take();
        }
    }
}

/// Marks a write session after a metadata session-level failure.
fn mark_session_after_metadata_error(session: &mut WriteSession, err: &ClientError) {
    if err.is_outcome_unknown() {
        session.mark_unknown_outcome();
        return;
    }
    match err.kind() {
        ClientErrorKind::SessionExpired => session.mark_session_expired(),
        ClientErrorKind::Fenced | ClientErrorKind::SessionInvalid => session.mark_session_invalid(),
        _ => {}
    }
    if matches!(
        err.remote_error().map(|error| &error.recovery),
        Some(RecoveryAction::RefreshMetadata { .. })
    ) {
        session.mark_session_invalid();
    }
}

/// Marks a write session after a worker write or add-block failure.
fn mark_session_after_write_error(session: &mut WriteSession, err: &ClientError) {
    if err.is_outcome_unknown() || err.is_retryable_transport() || err.is_invalid_success_response() {
        session.mark_unknown_outcome();
    } else if err.kind() == ClientErrorKind::SessionExpired
        && !matches!(
            err.remote_error().map(|error| &error.recovery),
            Some(RecoveryAction::RefreshMetadata { .. })
        )
    {
        session.mark_session_expired();
    } else {
        session.mark_session_invalid();
    }
}
