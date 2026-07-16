// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata service error types.
//!
//! This module defines unified error types for the metadata service,
//! with proper mapping to proto status codes and retry semantics.

use beryl_common::error::rpc::{
    ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, RefreshHint, RpcErrorDetail, WorkerErrorKind,
};
use beryl_types::fs::FsErrorCode;
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

/// Authoritative metadata -> rpc_error mapping for non-filesystem handlers.
pub fn to_rpc_error(err: MetadataError) -> RpcErrorDetail {
    match map_shared_rpc_error(err) {
        Ok(rpc_error) => rpc_error,
        Err(err) => map_rpc_application_error(err),
    }
}

/// Result type for metadata operations.
pub type MetadataResult<T> = Result<T, MetadataError>;

/// Authoritative FS error mapping entrypoint.
///
/// This is the single place that maps `MetadataError` into FS errno-backed
/// RPC errors for filesystem-facing handlers.
pub fn to_fs_error_detail(err: MetadataError) -> RpcErrorDetail {
    match map_shared_rpc_error(err) {
        Ok(rpc_error) => rpc_error,
        Err(err) => map_fs_fatal_rpc_error(err),
    }
}

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
            ErrorKind::Internal(InternalErrorKind::ResourceExhausted),
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
        | MetadataError::ServiceUnavailable(_) => unreachable!("shared metadata errors must be mapped earlier"),
    }
}

fn map_fs_fatal_rpc_error(err: MetadataError) -> RpcErrorDetail {
    match err {
        MetadataError::NotFound(msg) => RpcErrorDetail::fs(FsErrorCode::ENoEnt, msg),
        MetadataError::AlreadyExists(msg) => RpcErrorDetail::fs(FsErrorCode::EExist, msg),
        MetadataError::InvalidArgument(msg) => RpcErrorDetail::fs(FsErrorCode::EInval, msg),
        MetadataError::NotDir(msg) => RpcErrorDetail::fs(FsErrorCode::ENotDir, msg),
        MetadataError::IsDir(msg) => RpcErrorDetail::fs(FsErrorCode::EIsDir, msg),
        MetadataError::DirectoryNotEmpty(msg) => RpcErrorDetail::fs(FsErrorCode::ENotEmpty, msg),
        MetadataError::CrossMountRename(msg) => RpcErrorDetail::fs(FsErrorCode::EXDev, msg),
        MetadataError::PermissionDenied(msg) => RpcErrorDetail::fs(FsErrorCode::EAcces, msg),
        MetadataError::NotSupported(msg) => RpcErrorDetail::fs(FsErrorCode::ENotsup, msg),
        MetadataError::Busy(msg) => RpcErrorDetail::fs(FsErrorCode::EBusy, msg),
        MetadataError::ActiveWorkerConflict(msg) => RpcErrorDetail::fs(FsErrorCode::EBusy, msg),
        MetadataError::Again(msg) => RpcErrorDetail::fs(FsErrorCode::EAgain, msg),
        MetadataError::Internal(msg) => RpcErrorDetail::fs(FsErrorCode::EInval, msg),
        MetadataError::LeaderChanged(_)
        | MetadataError::EpochMismatch { .. }
        | MetadataError::MountEpochMismatch { .. }
        | MetadataError::RoutingStale(_)
        | MetadataError::StaleState(_)
        | MetadataError::FullReportRequired(_)
        | MetadataError::LeaseFenced { .. }
        | MetadataError::ServiceUnavailable(_) => unreachable!("shared metadata errors must be mapped earlier"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_common::error::rpc::RecoveryAction;
    use beryl_types::fs::FsErrorCode;

    #[test]
    fn test_to_fs_error_detail_errno_coverage() {
        let cases = vec![
            (MetadataError::NotFound("x".to_string()), FsErrorCode::ENoEnt),
            (MetadataError::AlreadyExists("x".to_string()), FsErrorCode::EExist),
            (MetadataError::InvalidArgument("x".to_string()), FsErrorCode::EInval),
            (MetadataError::NotDir("x".to_string()), FsErrorCode::ENotDir),
            (MetadataError::IsDir("x".to_string()), FsErrorCode::EIsDir),
            (
                MetadataError::DirectoryNotEmpty("x".to_string()),
                FsErrorCode::ENotEmpty,
            ),
            (MetadataError::CrossMountRename("x".to_string()), FsErrorCode::EXDev),
            (MetadataError::PermissionDenied("x".to_string()), FsErrorCode::EAcces),
            (MetadataError::NotSupported("x".to_string()), FsErrorCode::ENotsup),
            (MetadataError::Busy("x".to_string()), FsErrorCode::EBusy),
            (MetadataError::ActiveWorkerConflict("x".to_string()), FsErrorCode::EBusy),
            (MetadataError::Again("x".to_string()), FsErrorCode::EAgain),
        ];

        for (input, expected_errno) in cases {
            let rpc_error = to_fs_error_detail(input);
            assert_eq!(rpc_error.kind, ErrorKind::Fs(expected_errno));
            assert_eq!(rpc_error.recovery, RecoveryAction::Fail);
        }
    }

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
                ErrorKind::Internal(InternalErrorKind::ResourceExhausted),
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
    fn test_shared_retryable_and_refresh_mapping_matches_between_rpc_and_fs() {
        let cases = vec![
            MetadataError::LeaderChanged("leader changed".to_string()),
            MetadataError::EpochMismatch { expected: 7, got: 5 },
            MetadataError::MountEpochMismatch {
                expected: 9,
                got: 8,
                mount_id: Some(MountId::new(11)),
            },
            MetadataError::RoutingStale("routing stale".to_string()),
            MetadataError::StaleState("stale state".to_string()),
            MetadataError::LeaseFenced { expected: 13, got: 12 },
            MetadataError::ServiceUnavailable("node warming up".to_string()),
        ];

        for input in cases {
            let rpc = to_rpc_error(input.clone());
            let fs = to_fs_error_detail(input);
            assert_eq!(rpc.kind, fs.kind);
            assert_eq!(rpc.recovery, fs.recovery);
            assert_eq!(rpc.message, fs.message);
        }
    }
}
