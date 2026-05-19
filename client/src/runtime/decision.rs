// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Executor decisions after error classification.

use crate::runtime::classify::{ErrorClass, RefreshReason};
use crate::runtime::policy::{OperationKind, ReplaySafety};

/// Explicit retry decision selected for a classified operation failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    /// Retry the same operation after bounded backoff.
    Retry,
    /// Refresh authoritative state, then retry if replay is safe.
    RefreshThenRetry,
    /// Return the classified error to the caller.
    ReturnError,
    /// Return an unknown-outcome error without hiding it as transport retry.
    UnknownOutcome,
    /// Deny replay because the operation lacks required stable identity.
    DenyUnsafeReplay,
}

/// Inputs used by the retry decision table.
#[derive(Clone, Debug)]
pub(crate) struct RetryDecisionInput {
    /// Logical operation kind.
    pub(crate) operation_kind: OperationKind,
    /// Stable low-cardinality operation name.
    pub(crate) operation_name: &'static str,
    /// Attempt number for the current logical operation.
    pub(crate) attempt_number: u32,
    /// Retry budget remaining before this decision.
    pub(crate) retry_budget_remaining: usize,
    /// Refresh budget remaining before this decision.
    pub(crate) refresh_budget_remaining: usize,
    /// Classified error.
    pub(crate) error_class: ErrorClass,
    /// Refresh reason when the class is refreshable.
    pub(crate) refresh_reason: Option<RefreshReason>,
    /// Replay safety required by the operation.
    pub(crate) replay_safety: ReplaySafety,
    /// Whether side effects may have reached the server or worker.
    pub(crate) side_effects_may_have_occurred: bool,
    /// Whether call id and fingerprint are stable for replay.
    pub(crate) has_stable_call_id_and_fingerprint: bool,
    /// Whether session identity is stable for session barriers.
    pub(crate) has_stable_session_identity: bool,
    /// Whether public read bytes already escaped to the caller.
    pub(crate) public_bytes_returned: bool,
    /// Whether the operation outcome is already classified as unknown.
    pub(crate) outcome_unknown: bool,
}

impl RetryDecision {
    /// Decide retry behavior from explicit operation facts.
    pub(crate) fn from_input(input: RetryDecisionInput) -> Self {
        let _ = (
            input.operation_kind,
            input.operation_name,
            input.attempt_number,
            input.refresh_reason,
        );
        if input.public_bytes_returned {
            return Self::ReturnError;
        }
        if input.outcome_unknown || input.error_class == ErrorClass::UnknownOutcome {
            return Self::UnknownOutcome;
        }
        if input.error_class == ErrorClass::InvalidHeader
            && input.side_effects_may_have_occurred
            && input.operation_kind == OperationKind::WorkerWriteData
        {
            return Self::UnknownOutcome;
        }
        if !has_required_replay_identity(&input) {
            return Self::DenyUnsafeReplay;
        }
        match input.error_class {
            ErrorClass::RetryableTransport if input.retry_budget_remaining > 0 => Self::Retry,
            ErrorClass::RetryableTransport => Self::ReturnError,
            ErrorClass::NeedRefresh(reason)
                if reason != RefreshReason::Unknown
                    && input.retry_budget_remaining > 0
                    && input.refresh_budget_remaining > 0 =>
            {
                Self::RefreshThenRetry
            }
            ErrorClass::NeedRefresh(_) => Self::ReturnError,
            ErrorClass::UnknownOutcome => Self::UnknownOutcome,
            ErrorClass::Fatal
            | ErrorClass::InvalidArgument
            | ErrorClass::InvalidHeader
            | ErrorClass::PermissionDenied
            | ErrorClass::Unsupported
            | ErrorClass::Fencing
            | ErrorClass::SessionInvalid
            | ErrorClass::SessionExpired => Self::ReturnError,
        }
    }

    /// Low-cardinality label for metrics.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::RefreshThenRetry => "refresh_then_retry",
            Self::ReturnError => "return_error",
            Self::UnknownOutcome => "unknown_outcome",
            Self::DenyUnsafeReplay => "deny_unsafe_replay",
        }
    }
}

fn has_required_replay_identity(input: &RetryDecisionInput) -> bool {
    match input.replay_safety {
        ReplaySafety::Idempotent => true,
        ReplaySafety::StableCallId | ReplaySafety::BestEffortCleanup => input.has_stable_call_id_and_fingerprint,
        ReplaySafety::StableSession => input.has_stable_call_id_and_fingerprint && input.has_stable_session_identity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::classify::{ErrorClass, RefreshReason};
    use crate::runtime::policy::{OperationKind, ReplaySafety};

    fn input(kind: OperationKind, class: ErrorClass) -> RetryDecisionInput {
        RetryDecisionInput {
            operation_kind: kind,
            operation_name: "Operation",
            attempt_number: 0,
            retry_budget_remaining: 1,
            refresh_budget_remaining: 1,
            error_class: class,
            refresh_reason: None,
            replay_safety: ReplaySafety::Idempotent,
            side_effects_may_have_occurred: false,
            has_stable_call_id_and_fingerprint: true,
            has_stable_session_identity: true,
            public_bytes_returned: false,
            outcome_unknown: false,
        }
    }

    #[test]
    fn metadata_read_transport_retry_requires_remaining_budget() {
        let decision = RetryDecision::from_input(input(OperationKind::MetadataRead, ErrorClass::RetryableTransport));
        assert_eq!(decision, RetryDecision::Retry);

        let mut exhausted = input(OperationKind::MetadataRead, ErrorClass::RetryableTransport);
        exhausted.retry_budget_remaining = 0;
        assert_eq!(RetryDecision::from_input(exhausted), RetryDecision::ReturnError);
    }

    #[test]
    fn metadata_mutation_retry_requires_stable_identity() {
        let mut unstable = input(OperationKind::MetadataMutation, ErrorClass::RetryableTransport);
        unstable.replay_safety = ReplaySafety::StableCallId;
        unstable.has_stable_call_id_and_fingerprint = false;

        assert_eq!(RetryDecision::from_input(unstable), RetryDecision::DenyUnsafeReplay);
    }

    #[test]
    fn session_barrier_retry_requires_session_identity() {
        let mut barrier = input(OperationKind::MetadataSessionBarrier, ErrorClass::RetryableTransport);
        barrier.replay_safety = ReplaySafety::StableSession;
        barrier.has_stable_session_identity = false;

        assert_eq!(RetryDecision::from_input(barrier), RetryDecision::DenyUnsafeReplay);
    }

    #[test]
    fn refresh_then_retry_requires_refresh_budget_and_known_reason() {
        let mut stale = input(
            OperationKind::MetadataRead,
            ErrorClass::NeedRefresh(RefreshReason::StaleState),
        );
        stale.refresh_reason = Some(RefreshReason::StaleState);
        assert_eq!(
            RetryDecision::from_input(stale.clone()),
            RetryDecision::RefreshThenRetry
        );

        let mut exhausted = stale.clone();
        exhausted.refresh_budget_remaining = 0;
        assert_eq!(RetryDecision::from_input(exhausted), RetryDecision::ReturnError);

        let mut unknown = input(
            OperationKind::MetadataRead,
            ErrorClass::NeedRefresh(RefreshReason::Unknown),
        );
        unknown.refresh_reason = Some(RefreshReason::Unknown);
        assert_eq!(RetryDecision::from_input(unknown), RetryDecision::ReturnError);
    }

    #[test]
    fn worker_write_unknown_outcome_is_first_class_and_not_retryable() {
        let mut commit_write = input(OperationKind::WorkerWriteData, ErrorClass::UnknownOutcome);
        commit_write.side_effects_may_have_occurred = true;
        commit_write.outcome_unknown = true;

        assert_eq!(RetryDecision::from_input(commit_write), RetryDecision::UnknownOutcome);
    }

    #[test]
    fn unsupported_fencing_and_invalid_header_side_effects_do_not_retry() {
        assert_eq!(
            RetryDecision::from_input(input(OperationKind::MetadataRead, ErrorClass::Unsupported)),
            RetryDecision::ReturnError
        );
        assert_eq!(
            RetryDecision::from_input(input(OperationKind::WorkerWriteData, ErrorClass::Fencing)),
            RetryDecision::ReturnError
        );

        let mut invalid_header = input(OperationKind::WorkerWriteData, ErrorClass::InvalidHeader);
        invalid_header.side_effects_may_have_occurred = true;
        assert_eq!(RetryDecision::from_input(invalid_header), RetryDecision::UnknownOutcome);
    }

    #[test]
    fn public_read_does_not_retry_after_bytes_reached_caller() {
        let mut read = input(OperationKind::WorkerReadData, ErrorClass::RetryableTransport);
        read.public_bytes_returned = true;

        assert_eq!(RetryDecision::from_input(read), RetryDecision::ReturnError);
    }
}
