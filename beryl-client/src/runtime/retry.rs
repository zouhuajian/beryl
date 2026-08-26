// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Pure retry decisions from operation safety and validated failure evidence.

use beryl_common::error::rpc::{ErrorKind, RecoveryAction};

use crate::error::ClientError;

use super::RetrySafety;

/// Action authorized for the next attempt of the same immutable operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    Retry,
    RefreshMetadata(ErrorKind),
    Return,
}

/// Decides whether the same call identity and payload may be attempted again.
pub(crate) fn retry_decision(error: &ClientError, safety: RetrySafety) -> RetryDecision {
    if let Some(remote) = error.remote_error() {
        return match remote.recovery {
            RecoveryAction::Retry { .. } => RetryDecision::Retry,
            RecoveryAction::RefreshMetadata { .. } => RetryDecision::RefreshMetadata(remote.kind),
            RecoveryAction::ReopenWriteSession { .. }
            | RecoveryAction::RegisterWorker
            | RecoveryAction::SendFullBlockReport
            | RecoveryAction::Fail => RetryDecision::Return,
        };
    }
    if !error.is_retryable_transport() {
        return RetryDecision::Return;
    }
    if error.is_definitely_before_side_effect() || safety != RetrySafety::NonReplayableMutation {
        RetryDecision::Retry
    } else {
        RetryDecision::Return
    }
}

/// Returns true when a mutation may have completed before transport failure.
pub(crate) fn transport_outcome_is_ambiguous(error: &ClientError, safety: RetrySafety) -> bool {
    safety != RetrySafety::ReadOnly && error.is_transport_failure() && !error.is_definitely_before_side_effect()
}

/// Returns true only for a server-marked Worker capacity rejection before IO.
pub(crate) fn is_definite_worker_capacity_rejection(error: &ClientError) -> bool {
    error.transport_code() == Some(tonic::Code::ResourceExhausted) && error.is_definitely_before_side_effect()
}
