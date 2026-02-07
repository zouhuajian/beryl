// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service error types.
//!
//! This module defines unified error types for the metadata service,
//! with proper mapping to proto status codes and retry semantics.

use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::RpcErrorCode;
use thiserror::Error;
use types::fs::FsErrorCode;
use types::ids::MountId;

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

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),

    /// Service unavailable (e.g., not ready).
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),
}

impl MetadataError {
    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::LeaderChanged(_)
                | Self::EpochMismatch { .. }
                | Self::MountEpochMismatch { .. }
                | Self::RoutingStale(_)
                | Self::StaleState(_)
        )
    }
}

impl From<MetadataError> for CanonicalError {
    fn from(err: MetadataError) -> Self {
        match err {
            MetadataError::LeaderChanged(msg) => {
                CanonicalError::need_refresh(RpcErrorCode::NotLeader, RefreshReason::NotLeader, msg)
            }
            MetadataError::EpochMismatch { expected, got } => CanonicalError::need_refresh(
                RpcErrorCode::EpochMismatch,
                RefreshReason::EpochMismatch,
                format!("epoch mismatch: expected {}, got {}", expected, got),
            ),
            MetadataError::MountEpochMismatch {
                expected,
                got,
                mount_id,
            } => CanonicalError::need_refresh(
                RpcErrorCode::MountEpochMismatch,
                RefreshReason::MountEpochMismatch,
                format!(
                    "mount epoch mismatch: expected {}, got {} (mount_id={:?})",
                    expected, got, mount_id
                ),
            ),
            MetadataError::RoutingStale(msg) => {
                CanonicalError::need_refresh(RpcErrorCode::ShardMoved, RefreshReason::RouteEpochMismatch, msg)
            }
            MetadataError::StaleState(msg) => {
                CanonicalError::need_refresh(RpcErrorCode::StaleState, RefreshReason::StaleState, msg)
            }
            MetadataError::LeaseFenced { expected, got } => CanonicalError::need_refresh(
                RpcErrorCode::Fencing,
                RefreshReason::Fencing,
                format!("lease fenced: expected >= {}, got {}", expected, got),
            ),
            MetadataError::NotFound(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("not found: {}", msg),
            },
            MetadataError::AlreadyExists(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("already exists: {}", msg),
            },
            MetadataError::InvalidArgument(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("invalid argument: {}", msg),
            },
            MetadataError::NotDir(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("not a directory: {}", msg),
            },
            MetadataError::IsDir(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("is a directory: {}", msg),
            },
            MetadataError::DirectoryNotEmpty(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("directory not empty: {}", msg),
            },
            MetadataError::CrossMountRename(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("cross-mount rename not allowed: {}", msg),
            },
            MetadataError::PermissionDenied(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("permission denied: {}", msg),
            },
            MetadataError::NotSupported(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("operation not supported: {}", msg),
            },
            MetadataError::Busy(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("resource busy: {}", msg),
            },
            MetadataError::Again(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("resource temporarily unavailable: {}", msg),
            },
            MetadataError::Internal(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("internal error: {}", msg),
            },
            MetadataError::ServiceUnavailable(msg) => CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                Some(1000), // Default retry after 1s
                format!("service unavailable: {}", msg),
            ),
        }
    }
}

/// Result type for metadata operations.
pub type MetadataResult<T> = Result<T, MetadataError>;

/// Authoritative FS canonicalization entrypoint.
///
/// This is the single place that maps `MetadataError` into FS errno-backed
/// canonical errors for filesystem-facing handlers.
pub fn to_canonical_fs(err: MetadataError) -> CanonicalError {
    match err {
        MetadataError::LeaderChanged(msg) => {
            CanonicalError::need_refresh(RpcErrorCode::NotLeader, RefreshReason::NotLeader, msg)
        }
        MetadataError::EpochMismatch { expected, got } => CanonicalError::need_refresh(
            RpcErrorCode::EpochMismatch,
            RefreshReason::EpochMismatch,
            format!("epoch mismatch: expected {}, got {}", expected, got),
        ),
        MetadataError::MountEpochMismatch {
            expected,
            got,
            mount_id,
        } => CanonicalError::need_refresh(
            RpcErrorCode::MountEpochMismatch,
            RefreshReason::MountEpochMismatch,
            format!(
                "mount epoch mismatch: expected {}, got {} (mount_id={:?})",
                expected, got, mount_id
            ),
        ),
        MetadataError::RoutingStale(msg) => {
            CanonicalError::need_refresh(RpcErrorCode::ShardMoved, RefreshReason::RouteEpochMismatch, msg)
        }
        MetadataError::StaleState(msg) => {
            CanonicalError::need_refresh(RpcErrorCode::StaleState, RefreshReason::StaleState, msg)
        }
        MetadataError::LeaseFenced { expected, got } => CanonicalError::need_refresh(
            RpcErrorCode::Fencing,
            RefreshReason::Fencing,
            format!("lease fenced: expected >= {}, got {}", expected, got),
        ),
        MetadataError::ServiceUnavailable(msg) => CanonicalError::retryable(
            RpcErrorCode::NodeUnavailable,
            Some(1000),
            format!("service unavailable: {}", msg),
        ),
        MetadataError::NotFound(msg) => CanonicalError::fatal_fs(FsErrorCode::ENoEnt, msg),
        MetadataError::AlreadyExists(msg) => CanonicalError::fatal_fs(FsErrorCode::EExist, msg),
        MetadataError::InvalidArgument(msg) => CanonicalError::fatal_fs(FsErrorCode::EInval, msg),
        MetadataError::NotDir(msg) => CanonicalError::fatal_fs(FsErrorCode::ENotDir, msg),
        MetadataError::IsDir(msg) => CanonicalError::fatal_fs(FsErrorCode::EIsDir, msg),
        MetadataError::DirectoryNotEmpty(msg) => CanonicalError::fatal_fs(FsErrorCode::ENotEmpty, msg),
        MetadataError::CrossMountRename(msg) => CanonicalError::fatal_fs(FsErrorCode::EXDev, msg),
        MetadataError::PermissionDenied(msg) => CanonicalError::fatal_fs(FsErrorCode::EAcces, msg),
        MetadataError::NotSupported(msg) => CanonicalError::fatal_fs(FsErrorCode::ENotsup, msg),
        MetadataError::Busy(msg) => CanonicalError::fatal_fs(FsErrorCode::EBusy, msg),
        MetadataError::Again(msg) => CanonicalError::fatal_fs(FsErrorCode::EAgain, msg),
        MetadataError::Internal(msg) => CanonicalError::fatal_fs(FsErrorCode::EInval, msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::error::canonical::ErrorCode as CanonicalCode;
    use types::fs::FsErrorCode;

    #[test]
    fn test_error_retryable() {
        assert!(MetadataError::LeaderChanged("test".to_string()).is_retryable());
        assert!(MetadataError::EpochMismatch { expected: 1, got: 0 }.is_retryable());
        assert!(MetadataError::MountEpochMismatch {
            expected: 2,
            got: 1,
            mount_id: None
        }
        .is_retryable());
        assert!(MetadataError::RoutingStale("test".to_string()).is_retryable());
        assert!(!MetadataError::NotFound("test".to_string()).is_retryable());
        assert!(!MetadataError::LeaseFenced { expected: 1, got: 0 }.is_retryable());
    }

    #[test]
    fn test_to_canonical_fs_errno_coverage() {
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
            (MetadataError::Again("x".to_string()), FsErrorCode::EAgain),
        ];

        for (input, expected_errno) in cases {
            let canonical = to_canonical_fs(input);
            assert_eq!(canonical.class, ErrorClass::Fatal);
            assert!(matches!(
                canonical.code,
                Some(CanonicalCode::FsErrno(errno)) if errno == expected_errno
            ));
        }
    }
}
