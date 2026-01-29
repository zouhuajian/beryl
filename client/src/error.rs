// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client error types and error code mapping.

use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::RpcErrorCode;
use common::{CommonError, CommonErrorCode as CommonErrorCode};
use proto::common::RpcErrorCodeProto as ProtoErrorCode;
use thiserror::Error;

/// Client-specific error type.
#[derive(Error, Debug)]
pub enum ClientError {
    /// Common error (wrapped).
    #[error("Common error: {0}")]
    Common(#[from] CommonError),

    /// Metadata service error.
    #[error("Metadata error: {0}")]
    Metadata(String),

    /// Worker service error.
    #[error("Worker error: {0}")]
    Worker(String),

    /// Cache error.
    #[error("Cache error: {0}")]
    Cache(String),

    /// Routing error.
    #[error("Routing error: {0}")]
    Routing(String),

    /// Version mismatch (cache invalidation trigger).
    #[error("Version mismatch: expected {expected}, got {actual}")]
    VersionMismatch {
        /// Expected version.
        expected: u64,
        /// Actual version.
        actual: u64,
    },

    /// Route epoch mismatch.
    #[error("Route epoch mismatch: expected {expected}, got {actual}")]
    RouteEpochMismatch {
        /// Expected route epoch.
        expected: u64,
        /// Actual route epoch.
        actual: u64,
    },

    /// Stale metadata (need refresh).
    #[error("Stale metadata: {0}")]
    StaleMeta(String),

    /// Not leader error (with leader hint).
    #[error("Not leader: {0:?}")]
    NotLeader(Option<u64>),

    /// Resource moved.
    #[error("Resource moved: {0}")]
    Moved(String),

    /// Unimplemented feature.
    #[error("Unimplemented: {0}")]
    Unimplemented(String),

    /// Need refresh (for follower read).
    #[error("Need refresh: {0}")]
    NeedRefresh(String),
}

/// Result type alias for client operations.
pub type ClientResult<T> = Result<T, ClientError>;

impl ClientError {
    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            ClientError::Common(e) => e.is_retryable(),
            ClientError::Metadata(_) => true,
            ClientError::Worker(_) => true,
            ClientError::Routing(_) => true,
            ClientError::NotLeader(_) => true,
            ClientError::RouteEpochMismatch { .. } => true,
            ClientError::StaleMeta(_) => true,
            ClientError::NeedRefresh(_) => true,
            ClientError::VersionMismatch { .. } => false, // Cache invalidation, not retry
            ClientError::Cache(_) => false,
            ClientError::Moved(_) => false,
            ClientError::Unimplemented(_) => false,
        }
    }

    /// Check if this error requires cache invalidation.
    pub fn requires_cache_invalidation(&self) -> bool {
        matches!(
            self,
            ClientError::VersionMismatch { .. }
                | ClientError::RouteEpochMismatch { .. }
                | ClientError::StaleMeta(_)
                | ClientError::Moved(_)
        )
    }

    /// Get the leader ID if this is a NotLeader error.
    pub fn leader_id(&self) -> Option<u64> {
        match self {
            ClientError::NotLeader(id) => *id,
            _ => None,
        }
    }
}

/// Convert proto ErrorCode to ClientError.
/// Note: This is a placeholder - actual proto ErrorCode enum variants may differ.
/// TODO: Update after proto code generation to match actual variant names.
impl From<ProtoErrorCode> for ClientError {
    fn from(code: ProtoErrorCode) -> Self {
        // Use i32 value for matching since variant names may differ
        let code_value = code as i32;
        match code_value {
            11 => ClientError::NotLeader(None),
            12 => ClientError::RouteEpochMismatch { expected: 0, actual: 0 },
            13 => ClientError::StaleMeta("Stale metadata".to_string()),
            14 => ClientError::VersionMismatch { expected: 0, actual: 0 },
            15 => ClientError::Moved("Resource moved".to_string()),
            16 => ClientError::Unimplemented("Feature not implemented".to_string()),
            17 => ClientError::NeedRefresh("Need refresh".to_string()),
            1 => ClientError::Common(CommonError::new(CommonErrorCode::Timeout, "Timeout")),
            2 => ClientError::Common(CommonError::new(CommonErrorCode::Unavailable, "Unavailable")),
            3 => ClientError::Common(CommonError::new(CommonErrorCode::Throttled, "Throttled")),
            4 => ClientError::Common(CommonError::new(CommonErrorCode::NotFound, "Not found")),
            5 => ClientError::Common(CommonError::new(CommonErrorCode::PermissionDenied, "Permission denied")),
            6 => ClientError::Common(CommonError::new(CommonErrorCode::InvalidArgument, "Invalid argument")),
            7 => ClientError::Common(CommonError::new(CommonErrorCode::Io, "IO error")),
            8 => ClientError::Common(CommonError::new(CommonErrorCode::Internal, "Internal error")),
            _ => ClientError::Common(CommonError::new(
                CommonErrorCode::Internal,
                format!("Unknown error code: {}", code_value),
            )),
        }
    }
}

/// Convert tonic::Status to ClientError.
impl From<tonic::Status> for ClientError {
    fn from(status: tonic::Status) -> Self {
        // Try to extract error code from status details
        // TODO: Parse error details from status if needed

        // Map gRPC status codes
        match status.code() {
            tonic::Code::NotFound => ClientError::Common(CommonError::new(CommonErrorCode::NotFound, status.message())),
            tonic::Code::PermissionDenied => {
                ClientError::Common(CommonError::new(CommonErrorCode::PermissionDenied, status.message()))
            }
            tonic::Code::InvalidArgument => {
                ClientError::Common(CommonError::new(CommonErrorCode::InvalidArgument, status.message()))
            }
            tonic::Code::Unavailable => {
                ClientError::Common(CommonError::new(CommonErrorCode::Unavailable, status.message()))
            }
            tonic::Code::DeadlineExceeded => {
                ClientError::Common(CommonError::new(CommonErrorCode::Timeout, status.message()))
            }
            tonic::Code::Unimplemented => ClientError::Unimplemented(status.message().to_string()),
            _ => ClientError::Common(CommonError::new(CommonErrorCode::Internal, status.message())),
        }
    }
}

impl From<ClientError> for CanonicalError {
    fn from(err: ClientError) -> Self {
        match err {
            ClientError::NotLeader(leader_id) => {
                let msg = format!("not leader: {:?}", leader_id);
                CanonicalError::need_refresh(RpcErrorCode::NotLeader, RefreshReason::NotLeader, msg)
            }
            ClientError::RouteEpochMismatch { expected, actual } => CanonicalError::need_refresh(
                RpcErrorCode::ShardMoved,
                RefreshReason::RouteEpochMismatch,
                format!("route epoch mismatch: expected {}, got {}", expected, actual),
            ),
            ClientError::StaleMeta(msg) => {
                CanonicalError::need_refresh(RpcErrorCode::StaleState, RefreshReason::StaleState, msg)
            }
            ClientError::NeedRefresh(msg) => {
                CanonicalError::need_refresh(RpcErrorCode::StaleState, RefreshReason::Unknown, msg)
            }
            ClientError::Moved(msg) => {
                CanonicalError::need_refresh(RpcErrorCode::ShardMoved, RefreshReason::Moved, msg)
            }
            ClientError::VersionMismatch { expected, actual } => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("version mismatch: expected {}, got {}", expected, actual),
            },
            ClientError::Common(common_err) => {
                let is_retryable = common_err.is_retryable();
                CanonicalError {
                    class: if is_retryable {
                        ErrorClass::Retryable
                    } else {
                        ErrorClass::Fatal
                    },
                    code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                    reason: None,
                    retry_after_ms: if is_retryable { Some(1000) } else { None },
                    message: common_err.to_string(),
                }
            }
            ClientError::Metadata(msg) | ClientError::Worker(msg) | ClientError::Routing(msg) => {
                CanonicalError::retryable(RpcErrorCode::Application, Some(1000), msg)
            }
            ClientError::Cache(msg) | ClientError::Unimplemented(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: msg,
            },
        }
    }
}
