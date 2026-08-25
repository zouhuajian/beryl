// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared client runtime state used by filesystem facade and open handles.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::config::ClientConfig;
use crate::data::{BlockWrite, WorkerDataPlane};
use crate::error::side_effect_response_body_mismatch;
use crate::error::{ClientError, ClientResult};
use crate::metadata::MetadataGateway;
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics};
use crate::rpc_error::{ClientAction, RefreshHint};
use crate::runtime::{
    classify_error, is_definite_worker_capacity_rejection, AttemptContext, ClientIdentity, ErrorClass,
    MetadataExecutor, MetadataTargets, OperationContext, OperationDeadline,
};
use crate::session::write_session::{ReadyBlock, WriteSession};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use bytes::Bytes;

/// Shared concrete runtime state for the filesystem facade and open file handles.
pub(crate) struct ClientRuntime {
    /// Immutable client configuration used by metadata and data-plane attempts.
    pub(crate) config: ClientConfig,
    /// Metadata RPC executor with bounded retry, refresh, and deadline handling.
    pub(crate) executor: MetadataExecutor,
    /// Worker data-plane adapter used after metadata returns validated targets.
    pub(crate) data_plane: WorkerDataPlane,
    /// Metrics sink shared by facade and open handles.
    pub(crate) metrics: Arc<dyn ClientMetrics>,
}

impl ClientRuntime {
    /// Builds the shared runtime from injected metadata, worker, and metrics dependencies.
    pub(crate) fn new(
        config: ClientConfig,
        gateway: Arc<dyn MetadataGateway>,
        metadata_targets: MetadataTargets,
        data_plane: WorkerDataPlane,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        config.read.validate()?;
        let identity = ClientIdentity::generate(config.client_name.clone())?;
        let executor = MetadataExecutor::new(identity, gateway, metadata_targets, &config, Arc::clone(&metrics))?;
        Ok(Self {
            config,
            executor,
            data_plane,
            metrics,
        })
    }

    /// Allocates one metadata block, then opens its Worker stream through the
    /// staging acknowledgement boundary. Only a marked capacity rejection
    /// before Worker side effects is retried; all attempts reuse one target.
    pub(crate) async fn open_block_write(
        &self,
        session: &mut WriteSession,
        deadline: OperationDeadline,
    ) -> ClientResult<BlockWrite> {
        let add_block = match self
            .executor
            .add_block(
                session.path(),
                session.write_handle(),
                session.previous_block_id(),
                deadline.clone(),
            )
            .await
        {
            Ok(add_block) => add_block,
            Err(err) => {
                mark_session_after_write_error(session, &err);
                return Err(self.normalize_outcome_error("AddBlock", "metadata", err));
            }
        };
        if let Err(err) = session.validate_target(&add_block.target) {
            session.mark_unknown_outcome();
            self.record_metric(
                ClientMetric::WorkerResponseBodyMismatch,
                metric_labels("AddBlock", "metadata").with_outcome("unknown"),
            );
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels("AddBlock", "metadata").with_outcome("unknown"),
            );
            return Err(side_effect_response_body_mismatch("AddBlock", err));
        }
        let operation = worker_write_context(
            self.executor.client_id(),
            self.executor.client_name(),
            "WriteBlock",
            session.path(),
            deadline,
        )?;
        let lease_expires_at_ms = session.expires_at_ms()?;
        for attempt_index in 0..self.config.retry.max_attempts() {
            let ctx = self.data_context(&operation, attempt_index as u32);
            match self
                .worker_rpc_with_timeout(
                    &operation,
                    self.data_plane.open_write_block(
                        ctx,
                        add_block.group_name.clone(),
                        add_block.target.clone(),
                        lease_expires_at_ms,
                    ),
                )
                .await
            {
                Ok(block) => return Ok(block),
                Err(err) if is_definite_worker_capacity_rejection(&err) => {
                    let class = ErrorClass::ServerRetry;
                    let has_next = attempt_index + 1 < self.config.retry.max_attempts();
                    if !has_next {
                        return Err(err);
                    }
                    self.record_metric(
                        ClientMetric::RetryAttempt,
                        metric_labels("WriteBlock", "worker").with_error_class(class.label()),
                    );
                    if self.sleep_before_retry(attempt_index, &operation).await.is_err() {
                        return Err(err);
                    }
                }
                Err(err) => {
                    mark_session_after_write_error(session, &err);
                    return Err(self.normalize_outcome_error("WriteBlock", "worker", err));
                }
            }
        }
        unreachable!("client retry configuration requires at least one attempt")
    }

    /// Sends one frame on an acknowledged block RPC under the current public
    /// write call's deadline, then advances the session cursor.
    pub(crate) async fn write_block_frame(
        &self,
        session: &mut WriteSession,
        block: &mut BlockWrite,
        data: Bytes,
        deadline: &OperationDeadline,
    ) -> ClientResult<()> {
        let len = data.len();
        session
            .cursor()
            .checked_add(len as u64)
            .ok_or_else(|| ClientError::InvalidArgument("write cursor overflow".to_string()))?;
        match self.worker_write_step_with_timeout(deadline, block.write(data)).await {
            Ok(()) => session.advance_cursor(len),
            Err(err) => {
                mark_session_after_write_error(session, &err);
                Err(self.normalize_outcome_error("WriteBlock", "worker", err))
            }
        }
    }

    /// Half-closes one block request stream and records the block as Ready only
    /// after the Worker response stream ends normally.
    pub(crate) async fn finish_block_write(
        &self,
        session: &mut WriteSession,
        block: BlockWrite,
        deadline: &OperationDeadline,
    ) -> ClientResult<()> {
        match self.worker_write_step_with_timeout(deadline, block.finish()).await {
            Ok((target, written_len)) => {
                if let Err(err) = session.push_ready_block(target, written_len) {
                    session.mark_session_invalid();
                    return Err(err);
                }
                Ok(())
            }
            Err(err) => {
                mark_session_after_write_error(session, &err);
                Err(self.normalize_outcome_error("WriteBlock", "worker", err))
            }
        }
    }

    /// Observes a terminal Worker result that arrived between public writer
    /// calls before sending more bytes on the same block RPC.
    pub(crate) async fn check_block_write(
        &self,
        session: &mut WriteSession,
        block: &mut BlockWrite,
        deadline: &OperationDeadline,
    ) -> ClientResult<()> {
        match self.worker_write_step_with_timeout(deadline, block.check_open()).await {
            Ok(()) => Ok(()),
            Err(err) => {
                mark_session_after_write_error(session, &err);
                Err(self.normalize_outcome_error("WriteBlock", "worker", err))
            }
        }
    }

    /// Waits for local block cancellation only within the current public
    /// operation; the detached completion task retains its lease bound.
    pub(crate) async fn cancel_block_write(&self, block: BlockWrite, deadline: &OperationDeadline) -> ClientResult<()> {
        if block.cancel(deadline.remaining()).await {
            return Ok(());
        }
        self.record_worker_timeout("WriteBlock");
        Err(timeout_error("worker", "WriteBlock cancellation"))
    }

    /// Converts durable Worker blocks into the Metadata visibility-barrier shape.
    pub(crate) fn committed_blocks_for_barrier(&self, session: &WriteSession) -> Vec<beryl_types::CommittedBlock> {
        session.ready_blocks().iter().map(committed_block_from_ready).collect()
    }

    /// Builds a data-plane attempt context under the public operation deadline.
    pub(crate) fn data_context(&self, operation: &OperationContext, attempt: u32) -> AttemptContext {
        AttemptContext::for_data(operation, attempt)
    }

    /// Runs a worker RPC under the shared public operation deadline.
    pub(crate) async fn worker_rpc_with_timeout<T, Fut>(
        &self,
        operation: &OperationContext,
        future: Fut,
    ) -> ClientResult<T>
    where
        Fut: Future<Output = ClientResult<T>>,
    {
        let timeout = operation.deadline().remaining();
        if timeout.is_zero() {
            self.record_worker_timeout(operation.operation_name());
            return Err(timeout_error("worker", operation.operation_name()));
        }
        match tokio::time::timeout(timeout, future).await {
            Ok(result) => result,
            Err(_) => {
                self.record_worker_timeout(operation.operation_name());
                Err(timeout_error("worker", operation.operation_name()))
            }
        }
    }

    /// Bounds one send, status check, or finish step without imposing a fixed
    /// timeout on the multi-call lifetime of the underlying streaming RPC.
    async fn worker_write_step_with_timeout<T, Fut>(&self, deadline: &OperationDeadline, future: Fut) -> ClientResult<T>
    where
        Fut: Future<Output = ClientResult<T>>,
    {
        let timeout = deadline.remaining();
        if timeout.is_zero() {
            self.record_worker_timeout("WriteBlock");
            return Err(timeout_error("worker", "WriteBlock"));
        }
        match tokio::time::timeout(timeout, future).await {
            Ok(result) => result,
            Err(_) => {
                self.record_worker_timeout("WriteBlock");
                Err(timeout_error("worker", "WriteBlock"))
            }
        }
    }

    /// Sleeps before a worker retry without exceeding the public deadline.
    pub(crate) async fn sleep_before_retry(
        &self,
        retry_index: usize,
        operation: &OperationContext,
    ) -> ClientResult<()> {
        let delay = fixed_backoff_delay(retry_index);
        let remaining = operation.deadline().remaining();
        if remaining.is_zero() || delay >= remaining {
            self.record_worker_timeout(operation.operation_name());
            return Err(timeout_error("worker", operation.operation_name()));
        }
        tokio::time::sleep(delay).await;
        Ok(())
    }

    /// Records error-class metrics for client-recognized protocol and session failures.
    pub(crate) fn record_error_metric(&self, operation: &'static str, target_plane: &'static str, class: &ErrorClass) {
        let metric = match class {
            ErrorClass::InvalidHeader => Some(ClientMetric::InvalidHeader),
            ErrorClass::UnknownOutcome => Some(ClientMetric::UnknownOutcome),
            ErrorClass::Fencing => Some(ClientMetric::FencingMismatch),
            ErrorClass::SessionInvalid => Some(ClientMetric::SessionInvalid),
            ErrorClass::SessionExpired => Some(ClientMetric::SessionExpired),
            ErrorClass::Unsupported => Some(ClientMetric::UnsupportedOperation),
            _ => None,
        };
        if let Some(metric) = metric {
            self.record_metric(
                metric,
                metric_labels(operation, target_plane).with_error_class(class.label()),
            );
        }
    }

    /// Maps transport or malformed-response uncertainty into an unknown-outcome client error.
    pub(crate) fn normalize_outcome_error(
        &self,
        operation: &'static str,
        target_plane: &'static str,
        err: ClientError,
    ) -> ClientError {
        if matches!(err, ClientError::UnknownOutcome(_)) {
            return err;
        }
        let class = classify_error(&err);
        self.record_error_metric(operation, target_plane, &class);
        let normalized = map_outcome_error(operation, err);
        if matches!(normalized, ClientError::UnknownOutcome(_)) {
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels(operation, target_plane)
                    .with_error_class(classify_error(&normalized).label())
                    .with_outcome("unknown"),
            );
        }
        normalized
    }

    fn record_worker_timeout(&self, operation: &'static str) {
        self.record_metric(
            ClientMetric::RpcTimeout,
            metric_labels(operation, "worker")
                .with_error_class(ErrorClass::RetryableTransport.label())
                .with_outcome("timeout"),
        );
    }

    /// Emits one metric event through the configured metrics sink.
    pub(crate) fn record_metric(&self, metric: ClientMetric, labels: ClientMetricLabels) {
        self.metrics.record(ClientMetricEvent::new(metric, labels));
    }
}

/// Builds the standard metric label set for one client operation.
pub(crate) fn metric_labels(operation: &'static str, target_plane: &'static str) -> ClientMetricLabels {
    ClientMetricLabels::default().with_operation(operation, target_plane)
}

/// Extracts a structured refresh hint from action errors when one is available.
pub(crate) fn refresh_hint_from_error(err: &ClientError) -> RefreshHint {
    match err {
        ClientError::Action(action) => match action.action() {
            ClientAction::Refresh { hint, .. } => hint.as_ref().clone(),
            _ => RefreshHint::default(),
        },
        _ => RefreshHint::default(),
    }
}

/// Returns true when a metadata session barrier has an unknown result.
pub(crate) fn is_unknown_session_barrier_outcome(err: &ClientError) -> bool {
    matches!(err, ClientError::UnknownOutcome(_)) || matches!(classify_error(err), ErrorClass::RetryableTransport)
}

/// Marks a write session after a metadata session-level failure.
pub(crate) fn mark_session_after_metadata_error(session: &mut WriteSession, err: &ClientError) {
    match classify_error(err) {
        ErrorClass::SessionExpired => session.mark_session_expired(),
        ErrorClass::Fencing | ErrorClass::SessionInvalid | ErrorClass::RefreshMetadata(_) => {
            session.mark_session_invalid()
        }
        _ => {}
    }
}

/// Converts a worker timeout into the standard transport-style client error.
fn timeout_error(target_plane: &str, operation: &str) -> ClientError {
    ClientError::from(tonic::Status::deadline_exceeded(format!(
        "{target_plane} {operation} exceeded the public operation deadline"
    )))
}

/// Creates the stable operation identity used for worker write attempts.
fn worker_write_context(
    client_id: beryl_types::ClientId,
    client_name: &str,
    operation_name: &'static str,
    path: &str,
    deadline: OperationDeadline,
) -> ClientResult<OperationContext> {
    OperationContext::new_named(client_id, client_name, operation_name, Some(path.to_string()), deadline)
}

/// Converts a durable Worker block into the Metadata committed-block shape.
fn committed_block_from_ready(block: &ReadyBlock) -> beryl_types::CommittedBlock {
    let target = block.target();
    beryl_types::CommittedBlock {
        block_id: target.block_id,
        file_offset: target.file_offset,
        len: block.written_len(),
    }
}

/// Marks a write session after a worker write or add-block failure.
fn mark_session_after_write_error(session: &mut WriteSession, err: &ClientError) {
    if has_uncertain_write_effect(err) {
        session.mark_unknown_outcome();
    } else if is_session_or_fencing_error(err) || is_write_refresh_error(err) {
        mark_session_after_metadata_error(session, err);
    } else {
        session.mark_session_invalid();
    }
}

/// Returns true when a failure leaves worker write side effects uncertain.
fn has_uncertain_write_effect(err: &ClientError) -> bool {
    matches!(err, ClientError::UnknownOutcome(_))
        || matches!(
            classify_error(err),
            ErrorClass::RetryableTransport | ErrorClass::InvalidHeader
        )
}

/// Returns true when the error invalidates or expires the write session.
fn is_session_or_fencing_error(err: &ClientError) -> bool {
    matches!(
        classify_error(err),
        ErrorClass::Fencing | ErrorClass::SessionInvalid | ErrorClass::SessionExpired
    )
}

/// Returns true when a write-path metadata refresh cause invalidates the current session.
fn is_write_refresh_error(err: &ClientError) -> bool {
    matches!(
        classify_error(err),
        ErrorClass::RefreshMetadata(
            ErrorKind::Metadata(
                MetadataErrorKind::RouteEpochMismatch
                    | MetadataErrorKind::OwnerGroupMismatch
                    | MetadataErrorKind::StaleState
            ) | ErrorKind::Worker(WorkerErrorKind::RunMismatch | WorkerErrorKind::BlockStampMismatch)
        )
    )
}

/// Normalizes uncertain transport and header failures into unknown outcomes.
fn map_outcome_error(operation: &str, err: ClientError) -> ClientError {
    match classify_error(&err) {
        ErrorClass::RetryableTransport => {
            ClientError::UnknownOutcome(format!("{operation} outcome is unknown after transport failure: {err}"))
        }
        ErrorClass::InvalidHeader => ClientError::UnknownOutcome(format!(
            "{operation} outcome is unknown after malformed OK response: {err}"
        )),
        _ => err,
    }
}

fn fixed_backoff_delay(retry_index: usize) -> Duration {
    const INITIAL_MS: u64 = 100;
    const MAX_MS: u64 = 2_000;
    let shift = retry_index.min(20) as u32;
    Duration::from_millis(INITIAL_MS.saturating_mul(1u64 << shift).min(MAX_MS))
}
