// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker error types and gRPC status mapping.
//!
//! This module provides unified error handling for the worker, mapping internal errors
//! to gRPC Status codes with retry information and details.

use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::RpcErrorCode;
use thiserror::Error;
use tonic::Status;
use types::ids::ShardGroupId;

/// Worker error types.
#[derive(Error, Debug, Clone)]
pub enum WorkerError {
    /// UFS (underlying file system) is unstable or unavailable.
    #[error("UFS unstable: {0}")]
    UfsUnstable(String),

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

    /// Invalid argument.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Not found.
    #[error("Not found: {0}")]
    NotFound(String),

    /// Permission denied.
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Error metadata for retry and observability.
#[derive(Clone, Debug)]
pub struct ErrorMetadata {
    /// Whether this error is retryable.
    pub retryable: bool,
    /// Suggested retry after duration (if retryable).
    pub retry_after_ms: Option<u64>,
    /// Limit type (e.g., "rate_limit", "capacity_limit").
    pub limit_type: Option<String>,
    /// Leader hint (if leader changed).
    pub leader_hint: Option<u64>,
    /// Group ID (if applicable).
    pub group_id: Option<ShardGroupId>,
    /// Additional context.
    pub context: std::collections::HashMap<String, String>,
}

impl WorkerError {
    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            WorkerError::UfsUnstable(_)
                | WorkerError::LeaderChanged(_)
                | WorkerError::Timeout(_)
                | WorkerError::ResourceExhausted(_)
        )
    }

    /// Get error metadata.
    pub fn metadata(&self) -> ErrorMetadata {
        let (retryable, retry_after_ms, limit_type, leader_hint) = match self {
            WorkerError::UfsUnstable(_) => (true, Some(1000), None, None),
            WorkerError::LeaderChanged(_) => (true, Some(500), None, None),
            WorkerError::Timeout(_) => (true, Some(100), None, None),
            WorkerError::ResourceExhausted(msg) => {
                let limit_type = if msg.contains("disk") || msg.contains("capacity") {
                    Some("capacity_limit".to_string())
                } else if msg.contains("rate") || msg.contains("quota") {
                    Some("rate_limit".to_string())
                } else {
                    None
                };
                (true, Some(5000), limit_type, None)
            }
            WorkerError::ChunkConflict(_) => (false, None, None, None),
            WorkerError::DiskError(_) => (false, None, None, None),
            WorkerError::Cancelled(_) => (false, None, None, None),
            WorkerError::InvalidArgument(_) => (false, None, None, None),
            WorkerError::NotFound(_) => (false, None, None, None),
            WorkerError::PermissionDenied(_) => (false, None, None, None),
            WorkerError::Internal(_) => (false, None, None, None),
        };

        ErrorMetadata {
            retryable,
            retry_after_ms,
            limit_type,
            leader_hint,
            group_id: None,
            context: std::collections::HashMap::new(),
        }
    }

    /// Convert to gRPC Status (without modifying proto).
    pub fn to_status(&self) -> Status {
        let metadata = self.metadata();
        let code = self.to_grpc_code();
        let message = self.to_string();
        let message_clone = message.clone();

        let mut status = Status::new(code, message);

        // Add retry information to status details if possible
        // Note: We can't modify proto, but we can encode retry hints in the message
        // or use status details (if proto supports it via extensions)
        if metadata.retryable {
            let retry_hint = if let Some(retry_after_ms) = metadata.retry_after_ms {
                format!("retry_after_ms: {}", retry_after_ms)
            } else {
                "retryable".to_string()
            };
            // For now, append to message (in production, use status details if available)
            let enhanced_message = format!("{} [{}]", message_clone, retry_hint);
            status = Status::new(code, enhanced_message);
        }

        status
    }

    /// Convert to gRPC status code.
    fn to_grpc_code(&self) -> tonic::Code {
        match self {
            WorkerError::UfsUnstable(_) => tonic::Code::Unavailable,
            WorkerError::LeaderChanged(_) => tonic::Code::Unavailable,
            WorkerError::ChunkConflict(_) => tonic::Code::FailedPrecondition,
            WorkerError::ResourceExhausted(_) => tonic::Code::ResourceExhausted,
            WorkerError::DiskError(_) => tonic::Code::Internal,
            WorkerError::Timeout(_) => tonic::Code::DeadlineExceeded,
            WorkerError::Cancelled(_) => tonic::Code::Cancelled,
            WorkerError::InvalidArgument(_) => tonic::Code::InvalidArgument,
            WorkerError::NotFound(_) => tonic::Code::NotFound,
            WorkerError::PermissionDenied(_) => tonic::Code::PermissionDenied,
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
        let msg = err.to_string();
        if msg.contains("timeout") || msg.contains("deadline") {
            WorkerError::Timeout(msg)
        } else if msg.contains("not found") || msg.contains("NotFound") {
            WorkerError::NotFound(msg)
        } else if msg.contains("permission") || msg.contains("Permission") {
            WorkerError::PermissionDenied(msg)
        } else if msg.contains("disk") || msg.contains("I/O") || msg.contains("io") {
            WorkerError::DiskError(msg)
        } else {
            WorkerError::Internal(msg)
        }
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

impl From<WorkerError> for CanonicalError {
    fn from(err: WorkerError) -> Self {
        let metadata = err.metadata();
        match err {
            WorkerError::LeaderChanged(msg) => {
                CanonicalError::need_refresh(RpcErrorCode::NotLeader, RefreshReason::NotLeader, msg)
            }
            WorkerError::UfsUnstable(msg) => {
                CanonicalError::retryable(RpcErrorCode::NodeUnavailable, metadata.retry_after_ms, msg)
            }
            WorkerError::Timeout(msg) => {
                CanonicalError::retryable(RpcErrorCode::Application, metadata.retry_after_ms, msg)
            }
            WorkerError::ResourceExhausted(msg) => {
                CanonicalError::retryable(RpcErrorCode::Application, metadata.retry_after_ms, msg)
            }
            WorkerError::ChunkConflict(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("chunk conflict: {}", msg),
                refresh_hint: None,
            },
            WorkerError::DiskError(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("disk I/O error: {}", msg),
                refresh_hint: None,
            },
            WorkerError::Cancelled(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("operation cancelled: {}", msg),
                refresh_hint: None,
            },
            WorkerError::InvalidArgument(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("invalid argument: {}", msg),
                refresh_hint: None,
            },
            WorkerError::NotFound(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("not found: {}", msg),
                refresh_hint: None,
            },
            WorkerError::PermissionDenied(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::PermissionDenied)),
                reason: None,
                retry_after_ms: None,
                message: format!("permission denied: {}", msg),
                refresh_hint: None,
            },
            WorkerError::Internal(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("internal error: {}", msg),
                refresh_hint: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retryable_errors() {
        assert!(WorkerError::UfsUnstable("test".to_string()).is_retryable());
        assert!(WorkerError::LeaderChanged("test".to_string()).is_retryable());
        assert!(WorkerError::Timeout("test".to_string()).is_retryable());
        assert!(!WorkerError::ChunkConflict("test".to_string()).is_retryable());
        assert!(!WorkerError::DiskError("test".to_string()).is_retryable());
    }

    #[test]
    fn test_error_metadata() {
        let err = WorkerError::UfsUnstable("test".to_string());
        let metadata = err.metadata();
        assert!(metadata.retryable);
        assert_eq!(metadata.retry_after_ms, Some(1000));

        let err = WorkerError::ResourceExhausted("disk full".to_string());
        let metadata = err.metadata();
        assert!(metadata.retryable);
        assert_eq!(metadata.limit_type, Some("capacity_limit".to_string()));
    }

    #[test]
    fn test_to_status() {
        let err = WorkerError::NotFound("chunk not found".to_string());
        let status = err.to_status();
        assert_eq!(status.code(), tonic::Code::NotFound);

        let err = WorkerError::UfsUnstable("UFS unavailable".to_string());
        let status = err.to_status();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("retry_after_ms"));
    }
}
