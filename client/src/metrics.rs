// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client metrics and observability.

use std::fmt;

/// Client runtime metric event kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientMetric {
    /// Retry attempt started.
    RetryAttempt,
    /// Retry budget exhausted.
    RetryExhausted,
    /// Refresh decision selected for a classified error.
    RefreshDecision,
    /// Refresh reason observed.
    RefreshReason,
    /// Refresh budget exhausted.
    RefreshExhausted,
    /// Unknown outcome observed.
    UnknownOutcome,
    /// Fencing mismatch observed.
    FencingMismatch,
    /// Session expired observed.
    SessionExpired,
    /// Session invalid observed.
    SessionInvalid,
    /// Lease renewal attempt started.
    LeaseRenewAttempt,
    /// Lease renewal succeeded.
    LeaseRenewSuccess,
    /// Lease renewal failed.
    LeaseRenewFailure,
    /// CommitFile unknown-outcome retry attempted.
    CommitUnknownRetry,
    /// Invalid response header observed.
    InvalidHeader,
    /// Worker response body mismatch observed.
    WorkerResponseBodyMismatch,
    /// Abort cleanup attempt started.
    AbortAttempt,
    /// Abort cleanup completed.
    AbortSuccess,
    /// Abort cleanup failed with a known error.
    AbortFailure,
    /// Abort cleanup outcome is unknown.
    AbortUnknown,
    /// Unsupported operation observed.
    UnsupportedOperation,
    /// Session barrier replay denied.
    SessionBarrierReplayDenied,
    /// Unsafe replay denied.
    UnsafeReplayDenied,
    /// Backoff delay selected.
    BackoffDelay,
    /// Metadata channel pool hit.
    MetadataChannelPoolHit,
    /// Metadata channel pool miss.
    MetadataChannelPoolMiss,
    /// Worker channel pool hit.
    WorkerChannelPoolHit,
    /// Worker channel pool miss.
    WorkerChannelPoolMiss,
    /// Channel pool connection setup failed.
    ChannelPoolConnectError,
    /// Precise cache invalidation was used.
    CachePreciseInvalidation,
    /// Client-side RPC timeout fired.
    RpcTimeout,
}

/// Low-cardinality labels for client metric events.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClientMetricLabels {
    /// Logical operation kind.
    pub(crate) operation_kind: Option<&'static str>,
    /// Stable operation name.
    pub(crate) operation_name: Option<String>,
    /// Classified error class.
    pub(crate) error_class: Option<&'static str>,
    /// Refresh reason.
    pub(crate) refresh_reason: Option<&'static str>,
    /// Target plane.
    pub(crate) target_plane: Option<&'static str>,
    /// Cache or pool name.
    pub(crate) cache: Option<&'static str>,
    /// Cache or pool reason.
    pub(crate) reason: Option<&'static str>,
    /// Outcome.
    pub(crate) outcome: Option<&'static str>,
}

impl ClientMetricLabels {
    /// Attach operation identity labels.
    pub(crate) fn with_operation(
        mut self,
        operation_kind: &'static str,
        operation_name: impl Into<String>,
        target_plane: &'static str,
    ) -> Self {
        self.operation_kind = Some(operation_kind);
        self.operation_name = Some(operation_name.into());
        self.target_plane = Some(target_plane);
        self
    }

    /// Attach error class label.
    pub(crate) fn with_error_class(mut self, error_class: &'static str) -> Self {
        self.error_class = Some(error_class);
        self
    }

    /// Attach refresh reason label.
    pub(crate) fn with_refresh_reason(mut self, refresh_reason: &'static str) -> Self {
        self.refresh_reason = Some(refresh_reason);
        self
    }

    /// Attach outcome label.
    pub(crate) fn with_outcome(mut self, outcome: &'static str) -> Self {
        self.outcome = Some(outcome);
        self
    }

    /// Attach a cache label.
    pub(crate) fn with_cache(mut self, cache: &'static str) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Attach a target plane label without an operation kind.
    pub(crate) fn with_target_plane(mut self, target_plane: &'static str) -> Self {
        self.target_plane = Some(target_plane);
        self
    }

    /// Attach a stable operation label without changing the operation kind.
    pub(crate) fn with_operation_name(mut self, operation_name: &'static str) -> Self {
        self.operation_name = Some(operation_name.to_string());
        self
    }

    /// Return true if no label value contains a sensitive or high-cardinality marker.
    pub(crate) fn has_only_safe_values(&self) -> bool {
        let values = [
            self.operation_name.as_deref(),
            self.operation_kind,
            self.error_class,
            self.refresh_reason,
            self.target_plane,
            self.cache,
            self.reason,
            self.outcome,
        ];
        values.into_iter().flatten().all(|value| {
            !value.contains('/')
                && !value.contains("://")
                && !value.contains("127.")
                && !value.contains("localhost")
                && !value.contains("token")
        })
    }
}

/// One client metric event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClientMetricEvent {
    /// Metric kind.
    pub(crate) metric: ClientMetric,
    /// Low-cardinality labels.
    pub(crate) labels: ClientMetricLabels,
}

impl ClientMetricEvent {
    /// Create a metric event.
    pub(crate) fn new(metric: ClientMetric, labels: ClientMetricLabels) -> Self {
        debug_assert!(labels.has_only_safe_values());
        Self { metric, labels }
    }
}

/// Client metrics recorder.
pub(crate) trait ClientMetrics: Send + Sync + fmt::Debug {
    /// Record one client metric event.
    fn record(&self, event: ClientMetricEvent);
}

/// No-op client metrics recorder.
#[derive(Debug, Default)]
pub(crate) struct NoopClientMetrics;

impl ClientMetrics for NoopClientMetrics {
    fn record(&self, _event: ClientMetricEvent) {}
}
