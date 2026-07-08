// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client error types and error code mapping.

use crate::rpc_error::ClientAction;
use common::error::rpc::{
    ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction, RefreshHint, RpcErrorDetail,
};
use common::{CommonError, CommonErrorCode};
use thiserror::Error;
use types::fs::FsErrorCode;

/// Opaque structured action error derived from RPC header validation.
#[derive(Clone)]
pub struct ClientActionError {
    action: Box<ClientAction>,
}

impl ClientActionError {
    pub(crate) fn new(action: ClientAction) -> Self {
        Self {
            action: Box::new(action),
        }
    }

    pub(crate) fn into_action(self) -> ClientAction {
        *self.action
    }

    pub(crate) fn action(&self) -> &ClientAction {
        self.action.as_ref()
    }

    /// Return the RPC error carried by this action, when one exists.
    pub fn rpc_error(&self) -> Option<&RpcErrorDetail> {
        self.action.rpc_error()
    }

    /// Return the RPC error kind, when this action carries a RPC error.
    pub fn kind(&self) -> Option<ErrorKind> {
        self.rpc_error().map(|rpc_error| rpc_error.kind)
    }

    /// Return the rpc_error recovery action, when this action carries a RPC error.
    pub fn recovery(&self) -> Option<&RecoveryAction> {
        self.rpc_error().map(|rpc_error| &rpc_error.recovery)
    }

    /// Return the RPC error message, when this action carries one.
    pub fn message(&self) -> Option<&str> {
        self.rpc_error().map(|rpc_error| rpc_error.message.as_str())
    }

    /// Return the retry-after delay in milliseconds, when this action carries one.
    pub fn retry_after_ms(&self) -> Option<u64> {
        self.rpc_error().and_then(|rpc_error| match rpc_error.recovery {
            RecoveryAction::Retry { after_ms } => after_ms,
            _ => None,
        })
    }

    /// Return whether the action is retryable under client retry policy.
    pub fn is_retryable(&self) -> bool {
        match self.action.as_ref() {
            ClientAction::Refresh { rpc_error, .. } => matches!(
                rpc_error.recovery,
                RecoveryAction::RefreshMetadata { .. }
                    | RecoveryAction::RegisterWorker
                    | RecoveryAction::SendFullBlockReport
            ),
            ClientAction::Retry { .. } => true,
            ClientAction::TransportFail { status } => {
                matches!(
                    status.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
                )
            }
            ClientAction::Fail { .. } => false,
        }
    }

    /// Return whether the action requires refreshing client metadata state.
    pub fn is_refresh_required(&self) -> bool {
        matches!(self.action.as_ref(), ClientAction::Refresh { .. })
    }
}

impl std::fmt::Debug for ClientActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClientActionError")
    }
}

impl std::fmt::Display for ClientActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.action.as_ref() {
            ClientAction::Refresh { reason, rpc_error, .. } => write!(
                f,
                "Client action error: kind={:?}, recovery={:?}, reason={:?}, message={}",
                rpc_error.kind, rpc_error.recovery, reason, rpc_error.message
            ),
            ClientAction::Retry {
                retry_after_ms_hint,
                rpc_error,
            } => write!(
                f,
                "Client action error: kind={:?}, recovery={:?}, retry_after_ms_hint={:?}, message={}",
                rpc_error.kind, rpc_error.recovery, retry_after_ms_hint, rpc_error.message
            ),
            ClientAction::Fail { rpc_error } => write!(
                f,
                "Client action error: kind={:?}, recovery={:?}, message={}",
                rpc_error.kind, rpc_error.recovery, rpc_error.message
            ),
            ClientAction::TransportFail { status } => write!(
                f,
                "Client action error: transport code={:?}, message={}",
                status.code(),
                status.message()
            ),
        }
    }
}

/// Client-specific error type.
#[derive(Clone, Error, Debug)]
pub enum ClientError {
    /// Common error (wrapped).
    #[error("Common error: {0}")]
    Common(#[from] CommonError),

    /// Metadata service error.
    #[error("Metadata error: {0}")]
    Metadata(String),

    /// Client configuration error.
    #[error("Config error: {0}")]
    Config(String),

    /// Invalid client argument or server-advertised protocol value.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Stale public file handle.
    #[error("Stale handle: {reason}")]
    StaleHandle {
        /// Reason the handle is stale.
        reason: String,
    },

    /// Invalid metadata layout returned for a read.
    #[error("Invalid layout: {0}")]
    InvalidLayout(String),

    /// Protocol-violating success response from metadata or worker.
    #[error("Invalid response from {operation}: {reason}")]
    InvalidResponse {
        /// Operation that returned the invalid response.
        operation: &'static str,
        /// Machine-readable enough reason for rejecting the response.
        reason: String,
    },

    /// Worker service error.
    #[error("Worker error: {0}")]
    Worker(String),

    /// Operation result may have committed, but the client cannot prove it.
    #[error("Unknown outcome: {0}")]
    UnknownOutcome(String),

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

    /// Unsupported operation or protocol for the current implementation.
    #[error("Unsupported: {0}")]
    Unsupported(String),

    /// Unsupported operation for the current MVP contract.
    #[error("Not supported: {0}")]
    NotSupported(String),

    /// Structured action error derived from rpc header validation.
    #[error("{0}")]
    Action(ClientActionError),
}

/// Result type alias for client operations.
pub type ClientResult<T> = Result<T, ClientError>;

pub(crate) fn side_effect_response_body_mismatch(operation: &str, detail: impl std::fmt::Display) -> ClientError {
    ClientError::UnknownOutcome(format!("{operation} response body mismatch after OK header: {detail}"))
}

pub(crate) fn invalid_response(operation: &'static str, reason: impl Into<String>) -> ClientError {
    ClientError::InvalidResponse {
        operation,
        reason: reason.into(),
    }
}

impl ClientError {
    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            ClientError::Common(e) => e.is_retryable(),
            ClientError::Metadata(_) => false,
            ClientError::Worker(_) => false,
            ClientError::UnknownOutcome(_) => false,
            ClientError::Routing(_) => false,
            ClientError::NotLeader(_) => true,
            ClientError::RouteEpochMismatch { .. } => true,
            ClientError::StaleMeta(_) => true,
            ClientError::Action(action) => match action.action() {
                ClientAction::Refresh { rpc_error, .. } => matches!(
                    rpc_error.recovery,
                    RecoveryAction::RefreshMetadata { .. }
                        | RecoveryAction::RegisterWorker
                        | RecoveryAction::SendFullBlockReport
                ),
                ClientAction::Retry { .. } => true,
                ClientAction::TransportFail { status } => {
                    matches!(
                        status.code(),
                        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
                    )
                }
                ClientAction::Fail { .. } => false,
            },
            ClientError::VersionMismatch { .. } => false, // Cache invalidation, not retry
            ClientError::Cache(_) => false,
            ClientError::Moved(_) => false,
            ClientError::Unimplemented(_) => false,
            ClientError::Unsupported(_) => false,
            ClientError::NotSupported(_) => false,
            ClientError::Config(_) => false,
            ClientError::InvalidArgument(_) => false,
            ClientError::InvalidResponse { .. } => false,
            ClientError::StaleHandle { .. } => false,
            ClientError::InvalidLayout(_) => false,
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
        ) || matches!(
            self,
            ClientError::Action(action) if matches!(action.action(), ClientAction::Refresh { .. })
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

/// Convert tonic::Status to ClientError.
impl From<tonic::Status> for ClientError {
    fn from(status: tonic::Status) -> Self {
        ClientError::from(ClientAction::TransportFail {
            status: Box::new(status),
        })
    }
}

impl From<ClientError> for RpcErrorDetail {
    fn from(err: ClientError) -> Self {
        match err {
            ClientError::NotLeader(leader_id) => {
                let msg = format!("not leader: {:?}", leader_id);
                RpcErrorDetail::refresh_metadata(
                    ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                    RefreshHint::default(),
                    msg,
                )
            }
            ClientError::RouteEpochMismatch { expected, actual } => RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch),
                RefreshHint::default(),
                format!("route epoch mismatch: expected {}, got {}", expected, actual),
            ),
            ClientError::StaleMeta(msg) => RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
                RefreshHint::default(),
                msg,
            ),
            ClientError::Moved(msg) => RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch),
                RefreshHint::default(),
                msg,
            ),
            ClientError::VersionMismatch { expected, actual } => RpcErrorDetail::fail(
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
                format!("version mismatch: expected {}, got {}", expected, actual),
            ),
            ClientError::Action(action) => match action.into_action() {
                ClientAction::Refresh { rpc_error, .. }
                | ClientAction::Retry { rpc_error, .. }
                | ClientAction::Fail { rpc_error } => *rpc_error,
                ClientAction::TransportFail { status } => RpcErrorDetail::fail(
                    ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
                    format!("transport status {:?}: {}", status.code(), status.message()),
                ),
            },
            ClientError::Common(common_err) => rpc_error_from_common_error(common_err),
            ClientError::Metadata(msg) | ClientError::Worker(msg) | ClientError::Routing(msg) => {
                RpcErrorDetail::retry(ErrorKind::Internal(InternalErrorKind::NodeUnavailable), Some(1000), msg)
            }
            ClientError::InvalidArgument(msg) | ClientError::InvalidLayout(msg) => {
                RpcErrorDetail::fail(ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument), msg)
            }
            ClientError::Unimplemented(msg) | ClientError::Unsupported(msg) | ClientError::NotSupported(msg) => {
                RpcErrorDetail::fail(ErrorKind::Protocol(ProtocolErrorKind::Unsupported), msg)
            }
            ClientError::Cache(msg) | ClientError::Config(msg) | ClientError::UnknownOutcome(msg) => {
                RpcErrorDetail::fail(ErrorKind::Internal(InternalErrorKind::Internal), msg)
            }
            ClientError::InvalidResponse { operation, reason } => RpcErrorDetail::fail(
                ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader),
                format!("invalid response from {operation}: {reason}"),
            ),
            ClientError::StaleHandle { reason } => {
                RpcErrorDetail::fail(ErrorKind::Metadata(MetadataErrorKind::StaleState), reason)
            }
        }
    }
}

fn rpc_error_from_common_error(err: CommonError) -> RpcErrorDetail {
    match err.code {
        CommonErrorCode::Timeout => RpcErrorDetail::retry(
            ErrorKind::Internal(InternalErrorKind::Timeout),
            Some(1000),
            err.to_string(),
        ),
        CommonErrorCode::Unavailable => RpcErrorDetail::retry(
            ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
            Some(1000),
            err.to_string(),
        ),
        CommonErrorCode::Throttled | CommonErrorCode::Overloaded => RpcErrorDetail::retry(
            ErrorKind::Internal(InternalErrorKind::ResourceExhausted),
            Some(1000),
            err.to_string(),
        ),
        CommonErrorCode::NotFound => {
            RpcErrorDetail::fail(ErrorKind::Metadata(MetadataErrorKind::NotFound), err.to_string())
        }
        CommonErrorCode::PermissionDenied => RpcErrorDetail::fs(FsErrorCode::EAcces, err.to_string()),
        CommonErrorCode::InvalidArgument => {
            RpcErrorDetail::fail(ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument), err.to_string())
        }
        CommonErrorCode::Io | CommonErrorCode::Internal => {
            RpcErrorDetail::fail(ErrorKind::Internal(InternalErrorKind::Internal), err.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_error::{ClientAction, RefreshHint};
    use crate::runtime::MetadataRefreshCause;
    use common::error::rpc::RefreshHint as RpcRefreshHint;

    #[test]
    fn action_error_exposes_rpc_diagnostics() {
        let rpc_error = RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch),
            RpcRefreshHint::default(),
            "route epoch is stale",
        );
        let err = ClientActionError::new(ClientAction::Refresh {
            reason: MetadataRefreshCause::RouteEpochMismatch,
            hint: Box::new(RefreshHint::default()),
            rpc_error: Box::new(rpc_error.clone()),
        });

        assert_eq!(err.rpc_error().unwrap().message, rpc_error.message);
        assert_eq!(
            err.kind(),
            Some(ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch))
        );
        assert_eq!(err.recovery(), Some(&rpc_error.recovery));
        assert_eq!(err.message(), Some("route epoch is stale"));
        assert_eq!(err.retry_after_ms(), None);
        assert!(err.is_retryable());
        assert!(err.is_refresh_required());

        let displayed = ClientError::Action(err).to_string();
        assert!(displayed.contains("RefreshMetadata"));
        assert!(displayed.contains("RouteEpochMismatch"));
        assert!(displayed.contains("route epoch is stale"));
    }

    #[test]
    fn client_action_roundtrip_preserves_machine_semantics_without_message_classification() {
        let refresh_rpc_error = RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::NotLeader),
            RpcRefreshHint::default(),
            "fatal transport text",
        );
        let refresh = ClientError::from(ClientAction::Refresh {
            reason: MetadataRefreshCause::NotLeader,
            hint: Box::new(RefreshHint::default()),
            rpc_error: Box::new(refresh_rpc_error),
        });
        match refresh {
            ClientError::Action(action) => match action.action() {
                ClientAction::Refresh { reason, rpc_error, .. } => {
                    assert_eq!(*reason, MetadataRefreshCause::NotLeader);
                    assert_eq!(rpc_error.kind, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
                    assert!(matches!(rpc_error.recovery, RecoveryAction::RefreshMetadata { .. }));
                }
                other => panic!("expected refresh action, got {other:?}"),
            },
            other => panic!("expected action error, got {other:?}"),
        }

        let retry_rpc_error = RpcErrorDetail::retry(
            ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
            Some(25),
            "please refresh",
        );
        let retry = ClientError::from(ClientAction::Retry {
            retry_after_ms_hint: Some(25),
            rpc_error: Box::new(retry_rpc_error),
        });
        match retry {
            ClientError::Action(action) => match action.action() {
                ClientAction::Retry {
                    retry_after_ms_hint,
                    rpc_error,
                } => {
                    assert_eq!(*retry_after_ms_hint, Some(25));
                    assert_eq!(rpc_error.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
                    assert_eq!(rpc_error.recovery, RecoveryAction::Retry { after_ms: Some(25) });
                }
                other => panic!("expected retry action, got {other:?}"),
            },
            other => panic!("expected action error, got {other:?}"),
        }

        let fail_rpc_error =
            RpcErrorDetail::fail(ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument), "retry later");
        let fail = ClientError::from(ClientAction::Fail {
            rpc_error: Box::new(fail_rpc_error),
        });
        match fail {
            ClientError::Action(action) => match action.action() {
                ClientAction::Fail { rpc_error } => {
                    assert_eq!(rpc_error.kind, ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument));
                    assert_eq!(rpc_error.recovery, RecoveryAction::Fail);
                }
                other => panic!("expected fail action, got {other:?}"),
            },
            other => panic!("expected action error, got {other:?}"),
        }

        let transport = ClientError::from(tonic::Status::unavailable("not leader"));
        match transport {
            ClientError::Action(action) => match action.action() {
                ClientAction::TransportFail { status } => {
                    assert_eq!(status.code(), tonic::Code::Unavailable);
                    assert_eq!(status.message(), "not leader");
                }
                other => panic!("expected transport action, got {other:?}"),
            },
            other => panic!("expected action error, got {other:?}"),
        }
    }
}
