// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Shared client runtime state used by filesystem facade and open handles.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::canonical::{ClientAction, RefreshHint};
use crate::config::ClientConfig;
use crate::data::{WorkerBlockSyncResult, WorkerCommitResult, WorkerDataPlane};
use crate::error::side_effect_response_body_mismatch;
use crate::error::{ClientError, ClientResult};
use crate::metadata::MetadataGateway;
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics};
use crate::runtime::MetadataTargets;
use crate::runtime::OperationExecutor;
use crate::runtime::{
    AttemptContext, BackoffPolicy, BackoffSleeper, ClientIdentity, ErrorClass, ErrorClassifier, OperationContext,
    OperationIdentity, OperationKind, OperationRuntime, RefreshReason,
};
use crate::session::write_session::{PendingBlock, WorkerCommitLevel, WriteSession};
use bytes::Bytes;

/// Shared concrete runtime state for the filesystem facade and open file handles.
pub(crate) struct ClientRuntime {
    /// Immutable client configuration used by metadata and data-plane attempts.
    pub(crate) config: ClientConfig,
    /// Metadata RPC executor with retry, refresh, and replay policy.
    pub(crate) executor: OperationExecutor,
    /// Worker data-plane adapter used after metadata returns validated targets.
    pub(crate) data_plane: WorkerDataPlane,
    /// Backoff policy shared by handle-level data-plane retries.
    pub(crate) backoff: BackoffPolicy,
    /// Sleeper implementation used for scheduled retry delays.
    pub(crate) sleeper: Arc<dyn BackoffSleeper>,
    /// Metrics sink shared by facade and open handles.
    pub(crate) metrics: Arc<dyn ClientMetrics>,
}

impl ClientRuntime {
    /// Builds the shared runtime from injected metadata, worker, sleep, and metrics dependencies.
    pub(crate) fn new(
        config: ClientConfig,
        gateway: Arc<dyn MetadataGateway>,
        metadata_targets: MetadataTargets,
        data_plane: WorkerDataPlane,
        sleeper: Arc<dyn BackoffSleeper>,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        let identity = ClientIdentity::generate(config.client_name.clone())?;
        let executor = OperationExecutor::with_runtime(
            identity,
            gateway,
            metadata_targets,
            OperationRuntime {
                retry: config.retry.clone(),
                refresh: config.refresh.clone(),
                backoff: config.backoff.clone(),
                sleeper: Arc::clone(&sleeper),
                metrics: Arc::clone(&metrics),
            },
        )?;
        let backoff = BackoffPolicy::from_config(&config.backoff);
        Ok(Self {
            config,
            executor,
            data_plane,
            backoff,
            sleeper,
            metrics,
        })
    }

    /// Allocates one metadata block and writes its payload to the selected worker.
    pub(crate) async fn write_block(&self, session: &mut WriteSession, data: Bytes) -> ClientResult<()> {
        let block_len = data.len() as u64;
        let add_block = match self
            .executor
            .add_block(
                session.path(),
                session.session_identity(),
                session.write_handle(),
                block_len,
            )
            .await
        {
            Ok(add_block) => add_block,
            Err(err) => {
                mark_session_after_write_error(session, &err);
                return Err(self.normalize_outcome_error("AddBlock", OperationKind::MetadataMutation, err));
            }
        };
        if let Err(err) = session.validate_target(&add_block.target, block_len) {
            session.mark_unknown_outcome();
            self.record_metric(
                ClientMetric::WorkerResponseBodyMismatch,
                metric_labels("AddBlock", OperationKind::MetadataMutation).with_outcome("unknown"),
            );
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels("AddBlock", OperationKind::MetadataMutation).with_outcome("unknown"),
            );
            return Err(side_effect_response_body_mismatch("AddBlock", err));
        }
        let operation = worker_write_context(
            self.executor.client_id(),
            self.executor.client_name(),
            "OpenWriteStream",
            session.path(),
            &session.session_identity(),
        )?;
        let ctx = self.data_context(&operation, 0);
        let block_write_handle = match self
            .worker_rpc_with_timeout(
                "OpenWriteStream",
                OperationKind::WorkerWriteData,
                self.data_plane
                    .open_block_write(ctx, add_block.group_name.clone(), add_block.target.clone()),
            )
            .await
        {
            Ok(block_write_handle) => block_write_handle,
            Err(err) => {
                mark_session_after_write_error(session, &err);
                return Err(self.normalize_outcome_error("OpenWriteStream", OperationKind::WorkerWriteData, err));
            }
        };
        let response = match self
            .worker_rpc_with_timeout(
                "WriteStream",
                OperationKind::WorkerWriteData,
                self.data_plane.write_block_bytes(&block_write_handle, data),
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                mark_session_after_write_error(session, &err);
                return Err(self.normalize_outcome_error("WriteStream", OperationKind::WorkerWriteData, err));
            }
        };
        if response.written_through != block_len {
            session.mark_unknown_outcome();
            self.record_metric(
                ClientMetric::WorkerResponseBodyMismatch,
                metric_labels("WriteStream", OperationKind::WorkerWriteData).with_outcome("unknown"),
            );
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels("WriteStream", OperationKind::WorkerWriteData).with_outcome("unknown"),
            );
            return Err(ClientError::UnknownOutcome(format!(
                "worker WriteStream written_through mismatch: expected {}, got {}",
                block_len, response.written_through
            )));
        }
        if let Err(err) =
            session.push_pending_block(add_block.target, block_write_handle, block_len, response.last_acked_seq)
        {
            session.mark_session_invalid();
            return Err(err);
        }
        Ok(())
    }

    /// Commit pending worker blocks to the level required by the next metadata barrier.
    pub(crate) async fn commit_pending_blocks_for_barrier(
        &self,
        session: &mut WriteSession,
        required_level: WorkerCommitLevel,
    ) -> ClientResult<Vec<types::CommittedBlock>> {
        let worker_path = session.path().to_string();
        let worker_session_identity = session.session_identity();
        let mut committed_blocks = Vec::with_capacity(session.pending_blocks_mut().len());
        for pending in session.pending_blocks_mut() {
            if pending.worker_commit_level().satisfies(required_level) {
                committed_blocks.push(committed_block_from_pending(pending));
                continue;
            }

            match (pending.worker_commit_level(), required_level) {
                (WorkerCommitLevel::Uncommitted, WorkerCommitLevel::Visible | WorkerCommitLevel::Durable) => {
                    let require_sync = required_level.requires_sync();
                    let operation = worker_write_context(
                        self.executor.client_id(),
                        self.executor.client_name(),
                        "CommitWrite",
                        &worker_path,
                        &worker_session_identity,
                    )?;
                    let ctx = self.data_context(&operation, 0);
                    let commit_result = match self
                        .worker_rpc_with_timeout(
                            "CommitWrite",
                            OperationKind::WorkerWriteData,
                            self.data_plane.commit_block_write(
                                ctx,
                                pending.block_write_handle(),
                                pending.written_len(),
                                pending.commit_seq(),
                                require_sync,
                            ),
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(err) => {
                            mark_session_after_write_error(session, &err);
                            return Err(self.normalize_outcome_error(
                                "CommitWrite",
                                OperationKind::WorkerWriteData,
                                err,
                            ));
                        }
                    };
                    if let Err(err) = validate_worker_commit_result(pending, commit_result) {
                        session.mark_unknown_outcome();
                        self.record_metric(
                            ClientMetric::WorkerResponseBodyMismatch,
                            metric_labels("CommitWrite", OperationKind::WorkerWriteData).with_outcome("unknown"),
                        );
                        self.record_metric(
                            ClientMetric::UnknownOutcome,
                            metric_labels("CommitWrite", OperationKind::WorkerWriteData).with_outcome("unknown"),
                        );
                        return Err(err);
                    }
                    pending.mark_worker_committed(require_sync);
                }
                (WorkerCommitLevel::Visible, WorkerCommitLevel::Durable) => {
                    let operation = worker_write_context(
                        self.executor.client_id(),
                        self.executor.client_name(),
                        "SyncCommittedBlock",
                        &worker_path,
                        &worker_session_identity,
                    )?;
                    let ctx = self.data_context(&operation, 0);
                    let sync_result = match self
                        .worker_rpc_with_timeout(
                            "SyncCommittedBlock",
                            OperationKind::WorkerWriteData,
                            self.data_plane.sync_committed_block(
                                ctx,
                                pending.block_write_handle(),
                                pending.written_len(),
                            ),
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(err) => {
                            mark_session_after_block_sync_error(session, &err);
                            return Err(self.normalize_outcome_error(
                                "SyncCommittedBlock",
                                OperationKind::WorkerWriteData,
                                err,
                            ));
                        }
                    };
                    if let Err(err) = validate_worker_block_sync_result(pending, sync_result) {
                        session.mark_unknown_outcome();
                        self.record_metric(
                            ClientMetric::WorkerResponseBodyMismatch,
                            metric_labels("SyncCommittedBlock", OperationKind::WorkerWriteData).with_outcome("unknown"),
                        );
                        self.record_metric(
                            ClientMetric::UnknownOutcome,
                            metric_labels("SyncCommittedBlock", OperationKind::WorkerWriteData).with_outcome("unknown"),
                        );
                        return Err(err);
                    }
                    pending.mark_worker_committed(true);
                }
                (WorkerCommitLevel::Visible, WorkerCommitLevel::Visible)
                | (WorkerCommitLevel::Durable, WorkerCommitLevel::Visible | WorkerCommitLevel::Durable)
                | (WorkerCommitLevel::Uncommitted, WorkerCommitLevel::Uncommitted)
                | (WorkerCommitLevel::Visible | WorkerCommitLevel::Durable, WorkerCommitLevel::Uncommitted) => {}
            }
            committed_blocks.push(committed_block_from_pending(pending));
        }
        Ok(committed_blocks)
    }

    /// Builds a data-plane attempt context with the configured operation timeout.
    pub(crate) fn data_context(&self, operation: &OperationContext, attempt: u32) -> AttemptContext {
        AttemptContext::for_data(operation, attempt).with_operation_timeout_ms(self.config.retry.operation_timeout_ms)
    }

    /// Runs a worker RPC under the shared operation timeout policy.
    pub(crate) async fn worker_rpc_with_timeout<T, Fut>(
        &self,
        operation: &'static str,
        kind: OperationKind,
        future: Fut,
    ) -> ClientResult<T>
    where
        Fut: Future<Output = ClientResult<T>>,
    {
        let Some(timeout) = self.operation_timeout_duration() else {
            return future.await;
        };
        match tokio::time::timeout(timeout, future).await {
            Ok(result) => result,
            Err(_) => {
                self.record_metric(
                    ClientMetric::RpcTimeout,
                    metric_labels(operation, kind)
                        .with_error_class(ErrorClass::RetryableTransport.label())
                        .with_outcome("timeout"),
                );
                Err(timeout_error(kind.target_plane(), operation, timeout))
            }
        }
    }

    /// Returns the configured per-operation timeout, if one is set.
    fn operation_timeout_duration(&self) -> Option<Duration> {
        self.config.retry.operation_timeout_ms.map(Duration::from_millis)
    }

    /// Sleeps before a retry and records the scheduled backoff metric.
    pub(crate) async fn sleep_before_retry(&self, retry_index: usize, operation: &'static str, kind: OperationKind) {
        self.record_metric(
            ClientMetric::BackoffDelay,
            metric_labels(operation, kind).with_outcome("scheduled"),
        );
        self.sleeper.sleep(self.backoff.delay_for_retry(retry_index)).await;
    }

    /// Records refresh decision and refresh reason metrics with a shared label shape.
    pub(crate) fn record_refresh_metric(
        &self,
        operation: &'static str,
        kind: OperationKind,
        reason: RefreshReason,
        outcome: &'static str,
    ) {
        let labels = metric_labels(operation, kind)
            .with_refresh_reason(reason.label())
            .with_outcome(outcome);
        self.record_metric(ClientMetric::RefreshDecision, labels.clone());
        self.record_metric(ClientMetric::RefreshReason, labels);
    }

    /// Records error-class metrics for client-recognized protocol and session failures.
    pub(crate) fn record_error_metric(&self, operation: &'static str, kind: OperationKind, class: &ErrorClass) {
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
            self.record_metric(metric, metric_labels(operation, kind).with_error_class(class.label()));
        }
    }

    /// Maps transport or malformed-response uncertainty into an unknown-outcome client error.
    pub(crate) fn normalize_outcome_error(
        &self,
        operation: &'static str,
        kind: OperationKind,
        err: ClientError,
    ) -> ClientError {
        let class = ErrorClassifier.classify_error(&err);
        self.record_error_metric(operation, kind, &class);
        let normalized = map_outcome_error(operation, err);
        if matches!(normalized, ClientError::UnknownOutcome(_)) {
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels(operation, kind)
                    .with_error_class(ErrorClassifier.classify_error(&normalized).label())
                    .with_outcome("unknown"),
            );
        }
        normalized
    }

    /// Emits one metric event through the configured metrics sink.
    pub(crate) fn record_metric(&self, metric: ClientMetric, labels: ClientMetricLabels) {
        self.metrics.record(ClientMetricEvent::new(metric, labels));
    }
}

/// Builds the standard metric label set for one client operation.
pub(crate) fn metric_labels(operation: &'static str, kind: OperationKind) -> ClientMetricLabels {
    ClientMetricLabels::default().with_operation(kind.label(), operation, kind.target_plane())
}

/// Extracts a structured refresh hint from action errors when one is available.
pub(crate) fn refresh_hint_from_error(err: &ClientError) -> RefreshHint {
    match err {
        ClientError::Action(action) => match action.as_ref() {
            ClientAction::Refresh { hint, .. } => hint.as_ref().clone(),
            _ => RefreshHint::default(),
        },
        _ => RefreshHint::default(),
    }
}

/// Returns true when a metadata session barrier has an unknown result.
pub(crate) fn is_unknown_session_barrier_outcome(err: &ClientError) -> bool {
    matches!(err, ClientError::UnknownOutcome(_))
        || matches!(ErrorClassifier.classify_error(err), ErrorClass::RetryableTransport)
}

/// Marks a write session after a metadata session-level failure.
pub(crate) fn mark_session_after_metadata_error(session: &mut WriteSession, err: &ClientError) {
    match ErrorClassifier.classify_error(err) {
        ErrorClass::SessionExpired => session.mark_session_expired(),
        ErrorClass::Fencing | ErrorClass::SessionInvalid | ErrorClass::NeedRefresh(_) => session.mark_session_invalid(),
        _ => {}
    }
}

/// Converts a worker timeout into the standard transport-style client error.
fn timeout_error(target_plane: &str, operation: &str, timeout: Duration) -> ClientError {
    ClientError::from(tonic::Status::deadline_exceeded(format!(
        "{target_plane} {operation} timed out after {}ms",
        timeout.as_millis()
    )))
}

/// Creates the stable operation identity used for worker write attempts.
fn worker_write_context(
    client_id: types::ClientId,
    client_name: &str,
    operation_name: &str,
    path: &str,
    session_identity: &str,
) -> ClientResult<OperationContext> {
    OperationContext::new_named(
        client_id,
        client_name,
        OperationKind::WorkerWriteData,
        operation_name,
        OperationIdentity::session(path, session_identity),
    )
}

/// Converts a pending worker block into the metadata committed-block shape.
fn committed_block_from_pending(pending: &PendingBlock) -> types::CommittedBlock {
    let target = pending.target();
    types::CommittedBlock {
        block_id: target.block_id,
        file_offset: target.file_offset,
        len: pending.written_len(),
        checksum: None,
    }
}

fn validate_worker_commit_result(pending: &PendingBlock, result: WorkerCommitResult) -> ClientResult<()> {
    let expected_len = pending.written_len();
    if result.effective_len != expected_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!("effective_len expected {}, got {}", expected_len, result.effective_len),
        ));
    }
    if result.written_through != expected_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!(
                "written_through expected {}, got {}",
                expected_len, result.written_through
            ),
        ));
    }
    let expected_stamp = pending.target().block_stamp;
    if result.block_stamp != expected_stamp {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!("block_stamp expected {}, got {}", expected_stamp, result.block_stamp),
        ));
    }
    Ok(())
}

fn validate_worker_block_sync_result(pending: &PendingBlock, result: WorkerBlockSyncResult) -> ClientResult<()> {
    let expected_len = pending.written_len();
    if result.effective_len != expected_len {
        return Err(side_effect_response_body_mismatch(
            "SyncCommittedBlock",
            format!("effective_len expected {}, got {}", expected_len, result.effective_len),
        ));
    }
    let expected_stamp = pending.target().block_stamp;
    if result.block_stamp != expected_stamp {
        return Err(side_effect_response_body_mismatch(
            "SyncCommittedBlock",
            format!("block_stamp expected {}, got {}", expected_stamp, result.block_stamp),
        ));
    }
    Ok(())
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

/// Marks a write session after a durable block-sync failure.
fn mark_session_after_block_sync_error(session: &mut WriteSession, err: &ClientError) {
    if is_session_or_fencing_error(err) || is_write_refresh_error(err) {
        mark_session_after_metadata_error(session, err);
    } else {
        session.mark_unknown_outcome();
    }
}

/// Returns true when a failure leaves worker write side effects uncertain.
fn has_uncertain_write_effect(err: &ClientError) -> bool {
    matches!(err, ClientError::UnknownOutcome(_))
        || matches!(
            ErrorClassifier.classify_error(err),
            ErrorClass::RetryableTransport | ErrorClass::InvalidHeader
        )
}

/// Returns true when the error invalidates or expires the write session.
fn is_session_or_fencing_error(err: &ClientError) -> bool {
    matches!(
        ErrorClassifier.classify_error(err),
        ErrorClass::Fencing | ErrorClass::SessionInvalid | ErrorClass::SessionExpired
    )
}

/// Returns true when a write-path refresh reason invalidates the current session.
fn is_write_refresh_error(err: &ClientError) -> bool {
    matches!(
        ErrorClassifier.classify_error(err),
        ErrorClass::NeedRefresh(
            RefreshReason::RouteEpochMismatch
                | RefreshReason::WorkerRunMismatch
                | RefreshReason::BlockStampMismatch
                | RefreshReason::Unknown
        )
    )
}

/// Normalizes uncertain transport and header failures into unknown outcomes.
fn map_outcome_error(operation: &str, err: ClientError) -> ClientError {
    match ErrorClassifier.classify_error(&err) {
        ErrorClass::RetryableTransport => {
            ClientError::UnknownOutcome(format!("{operation} outcome is unknown after transport failure: {err}"))
        }
        ErrorClass::InvalidHeader => ClientError::UnknownOutcome(format!(
            "{operation} outcome is unknown after malformed OK response: {err}"
        )),
        _ => err,
    }
}
