// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service error types.
//!
//! This module defines unified error types for the metadata service,
//! with proper mapping to proto status codes and retry semantics.

use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::RpcErrorCode;
use thiserror::Error;
use tonic::{Code, Status};
// Removed unused imports: BlockId, DataHandleId, ClientId

/// Metadata service error.
#[derive(Debug, Error)]
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

    /// Lease fenced: expected epoch >= {expected}, got {got}.
    #[error("lease fenced: expected epoch >= {expected}, got {got}")]
    LeaseFenced { expected: u64, got: u64 },

    /// Leader changed (retryable).
    #[error("leader changed: {0}")]
    LeaderChanged(String),

    /// Epoch mismatch (retryable).
    #[error("epoch mismatch: expected {expected}, got {got}")]
    EpochMismatch { expected: u64, got: u64 },

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
            Self::LeaderChanged(_) | Self::EpochMismatch { .. } | Self::RoutingStale(_) | Self::StaleState(_)
        )
    }

    /// Convert to tonic Status.
    pub fn to_status(&self) -> Status {
        match self {
            Self::NotFound(_) => Status::new(Code::NotFound, self.to_string()),
            Self::AlreadyExists(_) => Status::new(Code::AlreadyExists, self.to_string()),
            Self::InvalidArgument(_) => Status::new(Code::InvalidArgument, self.to_string()),
            Self::LeaseFenced { .. } => Status::new(Code::FailedPrecondition, self.to_string()),
            Self::LeaderChanged(_) => Status::new(Code::Unavailable, self.to_string()),
            Self::EpochMismatch { .. } => Status::new(Code::Aborted, self.to_string()),
            Self::RoutingStale(_) => Status::new(Code::Unavailable, self.to_string()),
            Self::StaleState(_) => Status::new(Code::FailedPrecondition, self.to_string()),
            Self::Internal(_) => Status::new(Code::Internal, self.to_string()),
            Self::ServiceUnavailable(_) => Status::new(Code::Unavailable, self.to_string()),
        }
    }
}

impl From<MetadataError> for Status {
    fn from(err: MetadataError) -> Self {
        err.to_status()
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
            MetadataError::RoutingStale(msg) => {
                CanonicalError::need_refresh(RpcErrorCode::ShardMoved, RefreshReason::RouteEpochMismatch, msg)
            }
            MetadataError::StaleState(msg) => {
                CanonicalError::need_refresh(RpcErrorCode::StaleState, RefreshReason::StaleState, msg)
            }
            MetadataError::LeaseFenced { expected, got } => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing)),
                reason: Some(RefreshReason::Fencing),
                retry_after_ms: None,
                message: format!("lease fenced: expected >= {}, got {}", expected, got),
            },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_retryable() {
        assert!(MetadataError::LeaderChanged("test".to_string()).is_retryable());
        assert!(MetadataError::EpochMismatch { expected: 1, got: 0 }.is_retryable());
        assert!(MetadataError::RoutingStale("test".to_string()).is_retryable());
        assert!(!MetadataError::NotFound("test".to_string()).is_retryable());
        assert!(!MetadataError::LeaseFenced { expected: 1, got: 0 }.is_retryable());
    }

    #[test]
    fn test_error_to_status() {
        let err = MetadataError::NotFound("file".to_string());
        let status = err.to_status();
        assert_eq!(status.code(), Code::NotFound);

        let err = MetadataError::LeaseFenced { expected: 2, got: 1 };
        let status = err.to_status();
        assert_eq!(status.code(), Code::FailedPrecondition);

        let err = MetadataError::LeaderChanged("test".to_string());
        let status = err.to_status();
        assert_eq!(status.code(), Code::Unavailable);
    }
}
