// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared client ownership and cross-plane orchestration.

use std::future::Future;
use std::sync::Arc;

use crate::config::ClientConfig;
use crate::error::{ClientError, ClientErrorKind, ClientResult, RefreshHint};
use crate::metadata::{GrpcMetadataTransport, MetadataClient};
use crate::metrics::{self, ClientMetric, ClientMetricLabels};
use crate::runtime::retry::backoff_delay;
use crate::runtime::{ClientIdentity, MetadataTargets, OperationContext, OperationDeadline};
use crate::worker::WorkerClient;

/// Shared owner for client configuration, Metadata orchestration, and Worker IO
/// used by the filesystem facade and open handles.
pub(crate) struct ClientInner {
    /// Immutable client configuration used by metadata and data-plane attempts.
    pub(crate) config: ClientConfig,
    /// Metadata client with bounded retry, refresh, and deadline handling.
    pub(crate) metadata: MetadataClient,
    /// Worker client used only after Metadata returns validated targets.
    pub(crate) worker: WorkerClient,
}

impl ClientInner {
    /// Builds the production owner and both concrete transports from validated
    /// client configuration.
    pub(crate) fn from_config(config: ClientConfig) -> ClientResult<Self> {
        config.validate()?;
        let metadata_targets = MetadataTargets::from_config(&config)?;
        let metadata_transport = Arc::new(GrpcMetadataTransport::new_lazy_with_config(&config));
        let worker = WorkerClient::from_config(&config);
        let identity = ClientIdentity::generate(config.client_name().to_string())?;
        let metadata = MetadataClient::new(identity, metadata_transport, metadata_targets, &config);
        Ok(Self {
            config,
            metadata,
            worker,
        })
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
            return Err(timeout_error("worker", operation.operation_name()).with_operation_context(operation));
        }
        match tokio::time::timeout(timeout, future).await {
            Ok(result) => result.map_err(|error| error.with_operation_context(operation)),
            Err(_) => {
                self.record_worker_timeout(operation.operation_name());
                Err(timeout_error("worker", operation.operation_name()).with_operation_context(operation))
            }
        }
    }

    /// Bounds one send, status check, or finish step without imposing a fixed
    /// timeout on the multi-call lifetime of the underlying streaming RPC.
    pub(crate) async fn worker_write_step_with_timeout<T, Fut>(
        &self,
        deadline: &OperationDeadline,
        future: Fut,
    ) -> ClientResult<T>
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
        let delay = backoff_delay(retry_index);
        let remaining = operation.deadline().remaining();
        if remaining.is_zero() || delay >= remaining {
            self.record_worker_timeout(operation.operation_name());
            return Err(timeout_error("worker", operation.operation_name()).with_operation_context(operation));
        }
        tokio::time::sleep(delay).await;
        Ok(())
    }

    /// Records metrics for client-recognized protocol and session failures.
    pub(crate) fn record_error_metric(&self, operation: &'static str, target_plane: &'static str, error: &ClientError) {
        let metric = if error.is_outcome_unknown() {
            Some(ClientMetric::UnknownOutcome)
        } else {
            match error.kind() {
                ClientErrorKind::InvalidResponse => Some(ClientMetric::InvalidHeader),
                ClientErrorKind::Fenced => Some(ClientMetric::FencingMismatch),
                ClientErrorKind::SessionInvalid => Some(ClientMetric::SessionInvalid),
                ClientErrorKind::SessionExpired => Some(ClientMetric::SessionExpired),
                ClientErrorKind::Unsupported => Some(ClientMetric::UnsupportedOperation),
                _ => None,
            }
        };
        if let Some(metric) = metric {
            self.record_metric(
                metric,
                metric_labels(operation, target_plane).with_error_class(error.classification_label()),
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
        if err.is_outcome_unknown() {
            return err;
        }
        self.record_error_metric(operation, target_plane, &err);
        let normalized = map_outcome_error(operation, err);
        if normalized.is_outcome_unknown() {
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels(operation, target_plane)
                    .with_error_class("unknown_outcome")
                    .with_outcome("unknown"),
            );
        }
        normalized
    }

    pub(crate) fn record_worker_timeout(&self, operation: &'static str) {
        self.record_metric(
            ClientMetric::RpcTimeout,
            metric_labels(operation, "worker")
                .with_error_class("retryable_transport")
                .with_outcome("timeout"),
        );
    }

    /// Emits one low-cardinality counter through the process-wide recorder.
    pub(crate) fn record_metric(&self, metric: ClientMetric, labels: ClientMetricLabels) {
        metrics::record(metric, labels);
    }
}

/// Builds the standard metric label set for one client operation.
pub(crate) fn metric_labels(operation: &'static str, target_plane: &'static str) -> ClientMetricLabels {
    ClientMetricLabels::default().with_operation(operation, target_plane)
}

/// Extracts a structured refresh hint from action errors when one is available.
pub(crate) fn refresh_hint_from_error(err: &ClientError) -> RefreshHint {
    err.refresh_hint().cloned().unwrap_or_default()
}

/// Converts a worker timeout into the standard transport-style client error.
fn timeout_error(target_plane: &str, operation: &str) -> ClientError {
    ClientError::from(tonic::Status::deadline_exceeded(format!(
        "{target_plane} {operation} exceeded the public operation deadline"
    )))
}

/// Normalizes uncertain transport and header failures into unknown outcomes.
fn map_outcome_error(operation: &'static str, err: ClientError) -> ClientError {
    if err.is_retryable_transport() {
        let message = format!("{operation} outcome is unknown after transport failure: {err}");
        return err.with_unknown_outcome_name(operation, message);
    }
    if err.is_invalid_success_response() {
        let message = format!("{operation} outcome is unknown after malformed OK response: {err}");
        return err.with_unknown_outcome_name(operation, message);
    }
    err
}
