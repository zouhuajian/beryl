// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker error types and gRPC status mapping.
//!
//! This module provides unified error handling for the worker, mapping internal errors
//! to gRPC Status codes with retry information and details.

use common::error::rpc::{
    ErrorKind, InternalErrorKind, MetadataErrorKind, RefreshHint, RpcErrorDetail, WorkerErrorKind,
};
use thiserror::Error;
use tonic::Status;
use types::fs::FsErrorCode;

/// Worker error types.
#[derive(Error, Debug, Clone)]
pub enum WorkerError {
    /// Leader changed (raft group leader election).
    #[error("Leader changed: {0}")]
    LeaderChanged(String),

    /// Chunk conflict (e.g., concurrent write to same chunk).
    #[error("Chunk conflict: {0}")]
    ChunkConflict(String),

    /// Resource exhausted (e.g., disk full, quota exceeded).
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    /// Disk I/O error.
    #[error("Disk I/O error: {0}")]
    DiskError(String),

    /// Operation timeout.
    #[error("Operation timeout: {0}")]
    Timeout(String),

    /// Operation cancelled.
    #[error("Operation cancelled: {0}")]
    Cancelled(String),

    /// Service is temporarily unavailable.
    #[error("Unavailable: {0}")]
    Unavailable(String),

    /// Invalid argument.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Not found.
    #[error("Not found: {0}")]
    NotFound(String),

    /// Persisted local data or metadata is malformed.
    #[error("Corrupt: {0}")]
    Corrupt(String),

    /// Caller must refresh routing or block placement/state before retrying.
    #[error("Refresh metadata ({kind:?}): {message}")]
    RefreshMetadata { kind: ErrorKind, message: String },

    /// Fencing token is missing, malformed, or does not match the active writer.
    #[error("Fencing: {0}")]
    Fencing(String),

    /// Permission denied.
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    /// Operation is part of the defined contract but has no execution path yet.
    #[error("Unimplemented: {0}")]
    Unimplemented(String),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Error metadata for retry and observability.
#[derive(Clone, Debug)]
pub struct ErrorMetadata {
    /// Suggested retry delay hint for retryable errors.
    pub retry_after_ms: Option<u64>,
}

impl WorkerError {
    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            WorkerError::LeaderChanged(_)
                | WorkerError::Timeout(_)
                | WorkerError::ResourceExhausted(_)
                | WorkerError::Unavailable(_)
        )
    }

    /// Get error metadata.
    pub fn metadata(&self) -> ErrorMetadata {
        let retry_after_ms = match self {
            WorkerError::LeaderChanged(_) => Some(500),
            WorkerError::Timeout(_) => Some(100),
            WorkerError::ResourceExhausted(_) => Some(5000),
            WorkerError::Unavailable(_) => Some(500),
            WorkerError::ChunkConflict(_)
            | WorkerError::DiskError(_)
            | WorkerError::Cancelled(_)
            | WorkerError::InvalidArgument(_)
            | WorkerError::NotFound(_)
            | WorkerError::Corrupt(_)
            | WorkerError::RefreshMetadata { .. }
            | WorkerError::Fencing(_)
            | WorkerError::PermissionDenied(_)
            | WorkerError::Unimplemented(_)
            | WorkerError::Internal(_) => None,
        };

        ErrorMetadata { retry_after_ms }
    }

    /// Convert to gRPC Status (without modifying proto).
    pub fn to_status(&self) -> Status {
        let code = self.to_grpc_code();
        let message = self.to_string();
        Status::new(code, message)
    }

    /// Convert to gRPC status code.
    fn to_grpc_code(&self) -> tonic::Code {
        match self {
            WorkerError::LeaderChanged(_) => tonic::Code::Unavailable,
            WorkerError::ChunkConflict(_) => tonic::Code::FailedPrecondition,
            WorkerError::ResourceExhausted(_) => tonic::Code::ResourceExhausted,
            WorkerError::DiskError(_) => tonic::Code::Internal,
            WorkerError::Timeout(_) => tonic::Code::DeadlineExceeded,
            WorkerError::Cancelled(_) => tonic::Code::Cancelled,
            WorkerError::Unavailable(_) => tonic::Code::Unavailable,
            WorkerError::InvalidArgument(_) => tonic::Code::InvalidArgument,
            WorkerError::NotFound(_) => tonic::Code::NotFound,
            WorkerError::Corrupt(_) => tonic::Code::DataLoss,
            WorkerError::RefreshMetadata { .. } => tonic::Code::FailedPrecondition,
            WorkerError::Fencing(_) => tonic::Code::FailedPrecondition,
            WorkerError::PermissionDenied(_) => tonic::Code::PermissionDenied,
            WorkerError::Unimplemented(_) => tonic::Code::Unimplemented,
            WorkerError::Internal(_) => tonic::Code::Internal,
        }
    }
}

impl From<WorkerError> for Status {
    fn from(err: WorkerError) -> Self {
        err.to_status()
    }
}

impl From<anyhow::Error> for WorkerError {
    fn from(err: anyhow::Error) -> Self {
        WorkerError::Internal(err.to_string())
    }
}

impl From<std::io::Error> for WorkerError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => WorkerError::NotFound(err.to_string()),
            std::io::ErrorKind::PermissionDenied => WorkerError::PermissionDenied(err.to_string()),
            std::io::ErrorKind::TimedOut => WorkerError::Timeout(err.to_string()),
            _ => WorkerError::DiskError(err.to_string()),
        }
    }
}

impl From<WorkerError> for RpcErrorDetail {
    fn from(err: WorkerError) -> Self {
        let metadata = err.metadata();
        match err {
            WorkerError::LeaderChanged(msg) => RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                RefreshHint::default(),
                msg,
            ),
            WorkerError::Timeout(msg) => RpcErrorDetail::retry(
                ErrorKind::Worker(WorkerErrorKind::Timeout),
                metadata.retry_after_ms,
                msg,
            ),
            WorkerError::ResourceExhausted(msg) => RpcErrorDetail::retry(
                ErrorKind::Worker(WorkerErrorKind::ResourceExhausted),
                metadata.retry_after_ms,
                msg,
            ),
            WorkerError::Unavailable(msg) => RpcErrorDetail::retry(
                ErrorKind::Worker(WorkerErrorKind::NodeUnavailable),
                metadata.retry_after_ms,
                msg,
            ),
            WorkerError::ChunkConflict(msg) => RpcErrorDetail::fail(
                ErrorKind::Worker(WorkerErrorKind::Conflict),
                format!("chunk conflict: {}", msg),
            ),
            WorkerError::DiskError(msg) => RpcErrorDetail::fail(
                ErrorKind::Worker(WorkerErrorKind::Io),
                format!("disk I/O error: {}", msg),
            ),
            WorkerError::Cancelled(msg) => RpcErrorDetail::fail(
                ErrorKind::Worker(WorkerErrorKind::Cancelled),
                format!("operation cancelled: {}", msg),
            ),
            WorkerError::InvalidArgument(msg) => {
                RpcErrorDetail::fs(FsErrorCode::EInval, format!("invalid argument: {}", msg))
            }
            WorkerError::NotFound(msg) => RpcErrorDetail::fs(FsErrorCode::ENoEnt, format!("not found: {}", msg)),
            WorkerError::Corrupt(msg) => {
                RpcErrorDetail::fail(ErrorKind::Worker(WorkerErrorKind::Corrupt), format!("corrupt: {}", msg))
            }
            WorkerError::RefreshMetadata { kind, message } => {
                RpcErrorDetail::refresh_metadata(kind, RefreshHint::default(), message)
            }
            WorkerError::Fencing(msg) => {
                RpcErrorDetail::fail(ErrorKind::Worker(WorkerErrorKind::Fencing), format!("fencing: {}", msg))
            }
            WorkerError::PermissionDenied(msg) => {
                RpcErrorDetail::fs(FsErrorCode::EAcces, format!("permission denied: {}", msg))
            }
            WorkerError::Unimplemented(msg) => {
                RpcErrorDetail::fs(FsErrorCode::ENotImpl, format!("unimplemented: {}", msg))
            }
            WorkerError::Internal(msg) => RpcErrorDetail::fail(
                ErrorKind::Internal(InternalErrorKind::Internal),
                format!("internal error: {}", msg),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retryable_errors() {
        assert!(WorkerError::LeaderChanged("test".to_string()).is_retryable());
        assert!(WorkerError::Timeout("test".to_string()).is_retryable());
        assert!(WorkerError::Unavailable("test".to_string()).is_retryable());
        assert!(!WorkerError::ChunkConflict("test".to_string()).is_retryable());
        assert!(!WorkerError::DiskError("test".to_string()).is_retryable());
    }

    #[test]
    fn test_error_metadata() {
        let err = WorkerError::LeaderChanged("leader moved".to_string());
        let metadata = err.metadata();
        assert_eq!(metadata.retry_after_ms, Some(500));

        let disk = WorkerError::ResourceExhausted("disk full".to_string()).metadata();
        let quota = WorkerError::ResourceExhausted("quota exhausted".to_string()).metadata();
        assert_eq!(disk.retry_after_ms, Some(5000));
        assert_eq!(quota.retry_after_ms, Some(5000));
    }

    #[test]
    fn test_to_status() {
        let err = WorkerError::NotFound("chunk not found".to_string());
        let status = err.to_status();
        assert_eq!(status.code(), tonic::Code::NotFound);

        let err = WorkerError::LeaderChanged("not leader".to_string());
        let status = err.to_status();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(!status.message().contains("retry_after_ms"));
    }
}
