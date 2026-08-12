// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata service error types.
//!
//! This module defines unified error types for the metadata service,
//! with proper mapping to proto status codes and retry semantics.

use beryl_common::error::rpc::{
    ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, RefreshHint, RpcErrorDetail, WorkerErrorKind,
};
use beryl_types::ids::MountId;
use thiserror::Error;

/// Metadata service error.
#[derive(Debug, Error, Clone)]
pub enum MetadataError {
    /// Resource not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Already exists (e.g., file already exists).
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// Invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Not a directory.
    #[error("not a directory: {0}")]
    NotDir(String),

    /// Is a directory.
    #[error("is a directory: {0}")]
    IsDir(String),

    /// Directory not empty.
    #[error("directory not empty: {0}")]
    DirectoryNotEmpty(String),

    /// Cross-mount rename.
    #[error("cross-mount rename not allowed: {0}")]
    CrossMountRename(String),

    /// Permission denied.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Operation not supported.
    #[error("operation not supported: {0}")]
    NotSupported(String),

    /// Resource busy.
    #[error("resource busy: {0}")]
    Busy(String),

    /// Same stable WorkerId is still live from a different endpoint.
    #[error("active worker conflict: {0}")]
    ActiveWorkerConflict(String),

    /// Resource temporarily unavailable.
    #[error("resource temporarily unavailable: {0}")]
    Again(String),

    /// A request exceeds a stable Metadata resource boundary.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    /// Transient leader-local write-session capacity exhaustion.
    #[error("write-session limit exceeded: {0}")]
    WriteSessionLimitExceeded(String),

    /// Lease fenced: expected epoch >= {expected}, got {got}.
    #[error("lease fenced: expected epoch >= {expected}, got {got}")]
    LeaseFenced { expected: u64, got: u64 },

    /// Leader changed (retryable).
    #[error("leader changed: {0}")]
    LeaderChanged(String),

    /// Epoch mismatch (retryable).
    #[error("epoch mismatch: expected {expected}, got {got}")]
    EpochMismatch { expected: u64, got: u64 },

    /// Mount epoch mismatch (retryable).
    #[error("mount epoch mismatch: expected {expected}, got {got} (mount_id={mount_id:?})")]
    MountEpochMismatch {
        expected: u64,
        got: u64,
        mount_id: Option<MountId>,
    },

    /// Routing stale (retryable).
    #[error("routing stale: {0}")]
    RoutingStale(String),

    /// Stale state: follower last_applied < requested state_id (retryable).
    #[error("stale state: {0}")]
    StaleState(String),

    /// Worker must publish a new full block report before deltas can continue.
    #[error("full report required: {0}")]
    FullReportRequired(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),

    /// Service unavailable (e.g., not ready).
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),
}

/// Map one metadata failure into the shared RPC fact and recovery model.
///
/// This is the only metadata-to-RPC mapping entry point. Filesystem handlers
/// use the same domain facts as other metadata handlers; a POSIX adapter, if
/// added later, must translate those facts at its own boundary.
pub fn to_rpc_error(err: MetadataError) -> RpcErrorDetail {
    match map_shared_rpc_error(err) {
        Ok(rpc_error) => rpc_error,
        Err(err) => map_rpc_application_error(err),
    }
}

/// Result type for metadata operations.
pub type MetadataResult<T> = Result<T, MetadataError>;

fn map_shared_rpc_error(err: MetadataError) -> Result<RpcErrorDetail, MetadataError> {
    match err {
        MetadataError::LeaderChanged(msg) => Ok(RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::NotLeader),
            RefreshHint::default(),
            msg,
        )),
        MetadataError::EpochMismatch { expected, got } => Ok(RpcErrorDetail::reopen_write_session(
            ErrorKind::Metadata(MetadataErrorKind::EpochMismatch),
            RefreshHint::default(),
            format!("epoch mismatch: expected {}, got {}", expected, got),
        )),
        MetadataError::MountEpochMismatch {
            expected,
            got,
            mount_id,
        } => Ok(RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch),
            RefreshHint::default(),
            format!(
                "mount epoch mismatch: expected {}, got {} (mount_id={:?})",
                expected, got, mount_id
            ),
        )),
        MetadataError::RoutingStale(msg) => Ok(RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch),
            RefreshHint::default(),
            msg,
        )),
        MetadataError::StaleState(msg) => Ok(RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::StaleState),
            RefreshHint::default(),
            msg,
        )),
        MetadataError::FullReportRequired(msg) => Ok(RpcErrorDetail::send_full_block_report(
            ErrorKind::Worker(WorkerErrorKind::FullReportRequired),
            msg,
        )),
        MetadataError::LeaseFenced { expected, got } => Ok(RpcErrorDetail::reopen_write_session(
            ErrorKind::Metadata(MetadataErrorKind::Fencing),
            RefreshHint::default(),
            format!("lease fenced: expected >= {}, got {}", expected, got),
        )),
        MetadataError::ServiceUnavailable(msg) => Ok(RpcErrorDetail::retry(
            ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
            Some(1000),
            format!("service unavailable: {}", msg),
        )),
        MetadataError::WriteSessionLimitExceeded(msg) => Ok(RpcErrorDetail::retry(
            ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted),
            None,
            format!("write-session limit exceeded: {msg}"),
        )),
        MetadataError::ResourceExhausted(msg) => Ok(RpcErrorDetail::fail(
            ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted),
            format!("resource exhausted: {}", msg),
        )),
        other => Err(other),
    }
}

fn map_rpc_application_error(err: MetadataError) -> RpcErrorDetail {
    match err {
        MetadataError::NotFound(msg) => RpcErrorDetail::fail(
            ErrorKind::Metadata(MetadataErrorKind::NotFound),
            format!("not found: {}", msg),
        ),
        MetadataError::AlreadyExists(msg) => RpcErrorDetail::fail(
            ErrorKind::Metadata(MetadataErrorKind::AlreadyExists),
            format!("already exists: {}", msg),
        ),
        MetadataError::InvalidArgument(msg) => RpcErrorDetail::fail(
            ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument),
            format!("invalid argument: {}", msg),
        ),
        MetadataError::NotDir(msg) => RpcErrorDetail::fail(
            ErrorKind::Metadata(MetadataErrorKind::NotDirectory),
            format!("not a directory: {}", msg),
        ),
        MetadataError::IsDir(msg) => RpcErrorDetail::fail(
            ErrorKind::Metadata(MetadataErrorKind::IsDirectory),
            format!("is a directory: {}", msg),
        ),
        MetadataError::DirectoryNotEmpty(msg) => RpcErrorDetail::fail(
            ErrorKind::Metadata(MetadataErrorKind::DirectoryNotEmpty),
            format!("directory not empty: {}", msg),
        ),
        MetadataError::CrossMountRename(msg) => RpcErrorDetail::fail(
            ErrorKind::Metadata(MetadataErrorKind::CrossMountRename),
            format!("cross-mount rename not allowed: {}", msg),
        ),
        MetadataError::PermissionDenied(msg) => RpcErrorDetail::fail(
            ErrorKind::Protocol(ProtocolErrorKind::PermissionDenied),
            format!("permission denied: {}", msg),
        ),
        MetadataError::NotSupported(msg) => RpcErrorDetail::fail(
            ErrorKind::Protocol(ProtocolErrorKind::Unsupported),
            format!("operation not supported: {}", msg),
        ),
        MetadataError::Busy(msg) => RpcErrorDetail::fail(
            ErrorKind::Metadata(MetadataErrorKind::Busy),
            format!("resource busy: {}", msg),
        ),
        MetadataError::ActiveWorkerConflict(msg) => RpcErrorDetail::fail(
            ErrorKind::Metadata(MetadataErrorKind::Conflict),
            format!("active worker conflict: {}", msg),
        ),
        MetadataError::Again(msg) => RpcErrorDetail::retry(
            ErrorKind::Metadata(MetadataErrorKind::Conflict),
            None,
            format!("resource temporarily unavailable: {}", msg),
        ),
        MetadataError::Internal(msg) => RpcErrorDetail::fail(
            ErrorKind::Internal(InternalErrorKind::Internal),
            format!("internal error: {}", msg),
        ),
        MetadataError::LeaderChanged(_)
        | MetadataError::EpochMismatch { .. }
        | MetadataError::MountEpochMismatch { .. }
        | MetadataError::RoutingStale(_)
        | MetadataError::StaleState(_)
        | MetadataError::FullReportRequired(_)
        | MetadataError::LeaseFenced { .. }
        | MetadataError::ResourceExhausted(_)
        | MetadataError::WriteSessionLimitExceeded(_)
        | MetadataError::ServiceUnavailable(_) => unreachable!("shared metadata errors must be mapped earlier"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_common::error::rpc::RecoveryAction;

    #[test]
    fn test_to_rpc_error_kind_coverage() {
        let cases = vec![
            (
                MetadataError::NotFound("x".to_string()),
                ErrorKind::Metadata(MetadataErrorKind::NotFound),
                "not found: x".to_string(),
            ),
            (
                MetadataError::AlreadyExists("x".to_string()),
                ErrorKind::Metadata(MetadataErrorKind::AlreadyExists),
                "already exists: x".to_string(),
            ),
            (
                MetadataError::InvalidArgument("x".to_string()),
                ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument),
                "invalid argument: x".to_string(),
            ),
            (
                MetadataError::NotDir("x".to_string()),
                ErrorKind::Metadata(MetadataErrorKind::NotDirectory),
                "not a directory: x".to_string(),
            ),
            (
                MetadataError::IsDir("x".to_string()),
                ErrorKind::Metadata(MetadataErrorKind::IsDirectory),
                "is a directory: x".to_string(),
            ),
            (
                MetadataError::DirectoryNotEmpty("x".to_string()),
                ErrorKind::Metadata(MetadataErrorKind::DirectoryNotEmpty),
                "directory not empty: x".to_string(),
            ),
            (
                MetadataError::CrossMountRename("x".to_string()),
                ErrorKind::Metadata(MetadataErrorKind::CrossMountRename),
                "cross-mount rename not allowed: x".to_string(),
            ),
            (
                MetadataError::PermissionDenied("x".to_string()),
                ErrorKind::Protocol(ProtocolErrorKind::PermissionDenied),
                "permission denied: x".to_string(),
            ),
            (
                MetadataError::NotSupported("x".to_string()),
                ErrorKind::Protocol(ProtocolErrorKind::Unsupported),
                "operation not supported: x".to_string(),
            ),
            (
                MetadataError::Busy("x".to_string()),
                ErrorKind::Metadata(MetadataErrorKind::Busy),
                "resource busy: x".to_string(),
            ),
            (
                MetadataError::ActiveWorkerConflict("x".to_string()),
                ErrorKind::Metadata(MetadataErrorKind::Conflict),
                "active worker conflict: x".to_string(),
            ),
            (
                MetadataError::Again("x".to_string()),
                ErrorKind::Metadata(MetadataErrorKind::Conflict),
                "resource temporarily unavailable: x".to_string(),
            ),
            (
                MetadataError::Internal("x".to_string()),
                ErrorKind::Internal(InternalErrorKind::Internal),
                "internal error: x".to_string(),
            ),
        ];

        for (input, expected_kind, expected_message) in cases {
            let rpc_error = to_rpc_error(input);
            assert_eq!(rpc_error.kind, expected_kind);
            assert_eq!(rpc_error.message, expected_message);
        }
    }

    #[test]
    fn shared_retryable_and_refresh_failures_preserve_recovery() {
        let leader = to_rpc_error(MetadataError::LeaderChanged("leader changed".to_string()));
        assert!(matches!(leader.recovery, RecoveryAction::RefreshMetadata { .. }));

        let epoch = to_rpc_error(MetadataError::EpochMismatch { expected: 7, got: 5 });
        assert!(matches!(epoch.recovery, RecoveryAction::ReopenWriteSession { .. }));

        let stale = to_rpc_error(MetadataError::StaleState("stale state".to_string()));
        assert!(matches!(stale.recovery, RecoveryAction::RefreshMetadata { .. }));

        let again = to_rpc_error(MetadataError::Again("try again".to_string()));
        assert!(matches!(again.recovery, RecoveryAction::Retry { after_ms: None }));

        let unavailable = to_rpc_error(MetadataError::ServiceUnavailable("node warming up".to_string()));
        assert!(matches!(
            unavailable.recovery,
            RecoveryAction::Retry { after_ms: Some(1000) }
        ));
    }

    #[test]
    fn resource_exhausted_is_a_non_retryable_metadata_failure() {
        let error = to_rpc_error(MetadataError::ResourceExhausted("request exceeds limit".to_string()));

        assert_eq!(error.kind, ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted));
        assert_eq!(error.recovery, RecoveryAction::Fail);
    }

    #[test]
    fn write_session_limit_exceeded_is_retryable_resource_exhaustion() {
        let error = to_rpc_error(MetadataError::WriteSessionLimitExceeded(
            "global limit 1024 reached".to_string(),
        ));

        assert_eq!(error.kind, ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted));
        assert_eq!(error.recovery, RecoveryAction::Retry { after_ms: None });
    }
}
