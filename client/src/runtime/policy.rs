// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Replay safety rules for client operations.

use crate::error::{ClientError, ClientResult};
use crate::runtime::context::{OperationContext, OperationFingerprint};

/// Logical operation category used by replay policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum OperationKind {
    /// Idempotent metadata read.
    MetadataRead,
    /// Metadata mutation guarded by stable call id and server-side dedup.
    MetadataMutation,
    /// Metadata write-session barrier.
    MetadataSessionBarrier,
    /// Worker read data RPC.
    WorkerReadData,
    /// Worker write data RPC.
    WorkerWriteData,
    /// Best-effort cleanup.
    CleanupBestEffort,
}

impl OperationKind {
    /// Low-cardinality label for metrics.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::MetadataRead => "metadata_read",
            Self::MetadataMutation => "metadata_mutation",
            Self::MetadataSessionBarrier => "metadata_session_barrier",
            Self::WorkerReadData => "worker_read",
            Self::WorkerWriteData => "worker_write",
            Self::CleanupBestEffort => "cleanup_best_effort",
        }
    }

    /// Target plane label for metrics.
    pub(crate) fn target_plane(self) -> &'static str {
        match self {
            Self::MetadataRead | Self::MetadataMutation | Self::MetadataSessionBarrier => "metadata",
            Self::WorkerReadData | Self::WorkerWriteData => "worker",
            Self::CleanupBestEffort => "local",
        }
    }
}

/// Evidence required before replaying an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplaySafety {
    /// Replay is safe because the operation is an idempotent read.
    Idempotent,
    /// Replay requires a stable logical call id.
    StableCallId,
    /// Replay requires stable write-session identity.
    StableSession,
    /// Replay is cleanup-only and must not affect a newer session.
    BestEffortCleanup,
}

/// Return the replay safety requirement for an operation kind.
pub(crate) fn replay_safety_for(kind: OperationKind) -> ReplaySafety {
    match kind {
        OperationKind::MetadataRead | OperationKind::WorkerReadData => ReplaySafety::Idempotent,
        OperationKind::MetadataMutation => ReplaySafety::StableCallId,
        OperationKind::MetadataSessionBarrier | OperationKind::WorkerWriteData => ReplaySafety::StableSession,
        OperationKind::CleanupBestEffort => ReplaySafety::BestEffortCleanup,
    }
}

/// Reject unsafe mutation or session replay before an executor retries.
pub(crate) fn ensure_replay_allowed(
    operation: &OperationContext,
    observed_fingerprint: Option<OperationFingerprint>,
) -> ClientResult<()> {
    let safety = operation.replay_safety();
    let Some(observed_fingerprint) = observed_fingerprint else {
        return Err(ClientError::Unsupported(format!(
            "replay for {:?} requires {safety:?}",
            operation.kind()
        )));
    };
    if observed_fingerprint != operation.operation_fingerprint() {
        return Err(ClientError::Unsupported(format!(
            "replay for {:?} denied: operation fingerprint changed",
            operation.kind()
        )));
    }
    match safety {
        ReplaySafety::Idempotent | ReplaySafety::StableCallId => Ok(()),
        ReplaySafety::BestEffortCleanup => Ok(()),
        ReplaySafety::StableSession if operation.has_session_identity() => Ok(()),
        ReplaySafety::StableSession => Err(ClientError::Unsupported(format!(
            "replay for {:?} requires StableSession",
            operation.kind()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::executor::{OperationContext, OperationIdentity};
    use types::ClientId;

    #[test]
    fn metadata_mutation_replay_requires_stable_call_id() {
        let mutation = OperationContext::new(
            ClientId::new(7),
            OperationKind::MetadataMutation,
            "Delete",
            OperationIdentity::path("/alpha"),
        )
        .expect("mutation context");

        let err = ensure_replay_allowed(&mutation, None).expect_err("mutation replay without call id must fail");

        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("StableCallId")));
        assert!(ensure_replay_allowed(&mutation, Some(mutation.operation_fingerprint())).is_ok());
    }

    #[test]
    fn metadata_mutation_replay_with_changed_fingerprint_is_denied() {
        let mutation = OperationContext::new(
            ClientId::new(7),
            OperationKind::MetadataMutation,
            "Delete",
            OperationIdentity::path("/alpha"),
        )
        .expect("mutation context");
        let changed = OperationIdentity::path("/beta").fingerprint(OperationKind::MetadataMutation, "Delete");

        let err = ensure_replay_allowed(&mutation, Some(changed)).expect_err("changed mutation identity must fail");

        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("operation fingerprint")));
    }

    #[test]
    fn metadata_session_barrier_replay_requires_stable_fingerprint_and_session_identity() {
        let barrier = OperationContext::new(
            ClientId::new(7),
            OperationKind::MetadataSessionBarrier,
            "CommitFile",
            OperationIdentity::path("/alpha"),
        )
        .expect("barrier context");
        let err = ensure_replay_allowed(&barrier, Some(barrier.operation_fingerprint()))
            .expect_err("session replay without session identity must fail");

        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("StableSession")));
        let with_session = OperationContext::new(
            ClientId::new(7),
            OperationKind::MetadataSessionBarrier,
            "CommitFile",
            OperationIdentity::session("/alpha", "write-handle-1"),
        )
        .expect("barrier context");
        assert!(ensure_replay_allowed(&with_session, Some(with_session.operation_fingerprint())).is_ok());
    }

    #[test]
    fn metadata_session_barrier_replay_with_changed_fingerprint_is_denied() {
        let barrier = OperationContext::new(
            ClientId::new(7),
            OperationKind::MetadataSessionBarrier,
            "CommitFile",
            OperationIdentity::session("/alpha", "write-handle-1").with_detail("final_size=5"),
        )
        .expect("barrier context");
        let changed = OperationIdentity::session("/alpha", "write-handle-1")
            .with_detail("final_size=6")
            .fingerprint(OperationKind::MetadataSessionBarrier, "CommitFile");

        let err =
            ensure_replay_allowed(&barrier, Some(changed)).expect_err("changed session barrier fingerprint must fail");

        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("operation fingerprint")));
    }

    #[test]
    fn cleanup_replay_is_best_effort_but_still_requires_call_id() {
        assert_eq!(
            replay_safety_for(OperationKind::CleanupBestEffort),
            ReplaySafety::BestEffortCleanup
        );
        let cleanup = OperationContext::new(
            ClientId::new(7),
            OperationKind::CleanupBestEffort,
            "AbortFileWrite",
            OperationIdentity::path("/alpha"),
        )
        .expect("cleanup context");
        let err = ensure_replay_allowed(&cleanup, None).expect_err("cleanup replay without call id must fail");

        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("BestEffortCleanup")));
        assert!(ensure_replay_allowed(&cleanup, Some(cleanup.operation_fingerprint())).is_ok());
    }
}
