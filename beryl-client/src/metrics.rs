// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Private client observability emitted through the workspace metrics facade.

/// One low-cardinality client event counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientMetric {
    RetryAttempt,
    RetryExhausted,
    UnknownOutcome,
    FencingMismatch,
    SessionExpired,
    SessionInvalid,
    LeaseRenewAttempt,
    LeaseRenewSuccess,
    LeaseRenewFailure,
    CommitUnknownRetry,
    InvalidHeader,
    WorkerResponseBodyMismatch,
    AbortAttempt,
    AbortSuccess,
    AbortFailure,
    AbortUnknown,
    UnsupportedOperation,
    MetadataChannelPoolHit,
    MetadataChannelPoolMiss,
    WorkerChannelPoolHit,
    WorkerChannelPoolMiss,
    ChannelBuildError,
    CachePreciseInvalidation,
    RpcTimeout,
}

impl ClientMetric {
    /// Returns the stable metric name registered with the global recorder.
    const fn name(self) -> &'static str {
        match self {
            Self::RetryAttempt => "beryl_client_retry_attempts_total",
            Self::RetryExhausted => "beryl_client_retry_exhausted_total",
            Self::UnknownOutcome => "beryl_client_unknown_outcomes_total",
            Self::FencingMismatch => "beryl_client_fencing_mismatches_total",
            Self::SessionExpired => "beryl_client_session_expired_total",
            Self::SessionInvalid => "beryl_client_session_invalid_total",
            Self::LeaseRenewAttempt => "beryl_client_lease_renew_attempts_total",
            Self::LeaseRenewSuccess => "beryl_client_lease_renew_success_total",
            Self::LeaseRenewFailure => "beryl_client_lease_renew_failures_total",
            Self::CommitUnknownRetry => "beryl_client_commit_unknown_retries_total",
            Self::InvalidHeader => "beryl_client_invalid_headers_total",
            Self::WorkerResponseBodyMismatch => "beryl_client_worker_response_mismatches_total",
            Self::AbortAttempt => "beryl_client_abort_attempts_total",
            Self::AbortSuccess => "beryl_client_abort_success_total",
            Self::AbortFailure => "beryl_client_abort_failures_total",
            Self::AbortUnknown => "beryl_client_abort_unknown_total",
            Self::UnsupportedOperation => "beryl_client_unsupported_operations_total",
            Self::MetadataChannelPoolHit => "beryl_client_metadata_channel_pool_hits_total",
            Self::MetadataChannelPoolMiss => "beryl_client_metadata_channel_pool_misses_total",
            Self::WorkerChannelPoolHit => "beryl_client_worker_channel_pool_hits_total",
            Self::WorkerChannelPoolMiss => "beryl_client_worker_channel_pool_misses_total",
            Self::ChannelBuildError => "beryl_client_channel_build_errors_total",
            Self::CachePreciseInvalidation => "beryl_client_cache_invalidations_total",
            Self::RpcTimeout => "beryl_client_rpc_timeouts_total",
        }
    }
}

/// Validated low-cardinality labels attached to one client metric.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClientMetricLabels {
    operation_name: Option<&'static str>,
    error_class: Option<&'static str>,
    target_plane: Option<&'static str>,
    cache: Option<&'static str>,
    outcome: Option<&'static str>,
}

impl ClientMetricLabels {
    /// Attaches a stable operation and its target plane.
    pub(crate) fn with_operation(mut self, operation_name: &'static str, target_plane: &'static str) -> Self {
        self.operation_name = Some(operation_name);
        self.target_plane = Some(target_plane);
        self
    }

    /// Attaches a stable error classification.
    pub(crate) fn with_error_class(mut self, error_class: &'static str) -> Self {
        self.error_class = Some(error_class);
        self
    }

    /// Attaches a stable outcome classification.
    pub(crate) fn with_outcome(mut self, outcome: &'static str) -> Self {
        self.outcome = Some(outcome);
        self
    }

    /// Attaches a bounded cache or pool name.
    pub(crate) fn with_cache(mut self, cache: &'static str) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Attaches the Metadata or Worker target plane.
    pub(crate) fn with_target_plane(mut self, target_plane: &'static str) -> Self {
        self.target_plane = Some(target_plane);
        self
    }

    /// Attaches a stable operation name without changing the target plane.
    pub(crate) fn with_operation_name(mut self, operation_name: &'static str) -> Self {
        self.operation_name = Some(operation_name);
        self
    }

    /// Returns true when labels contain no paths, endpoints, or credential-like values.
    fn has_only_safe_values(&self) -> bool {
        let values = [
            self.operation_name,
            self.error_class,
            self.target_plane,
            self.cache,
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

/// Emits one counter through the process-wide metrics facade.
pub(crate) fn record(metric: ClientMetric, labels: ClientMetricLabels) {
    debug_assert!(labels.has_only_safe_values());
    let mut metric_labels = Vec::with_capacity(5);
    if let Some(value) = labels.operation_name {
        metric_labels.push(metrics::Label::new("operation", value));
    }
    if let Some(value) = labels.error_class {
        metric_labels.push(metrics::Label::new("error_class", value));
    }
    if let Some(value) = labels.target_plane {
        metric_labels.push(metrics::Label::new("target_plane", value));
    }
    if let Some(value) = labels.cache {
        metric_labels.push(metrics::Label::new("cache", value));
    }
    if let Some(value) = labels.outcome {
        metric_labels.push(metrics::Label::new("outcome", value));
    }
    metrics::counter!(metric.name(), metric_labels).increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_labels_detect_high_cardinality_values() {
        assert!(ClientMetricLabels::default()
            .with_operation("ReadBlock", "worker")
            .with_error_class("retryable_transport")
            .with_cache("channel_pool")
            .with_outcome("retry")
            .has_only_safe_values());
        assert!(!ClientMetricLabels::default()
            .with_operation("/user/path", "worker")
            .has_only_safe_values());
    }
}
