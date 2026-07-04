// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client error types and error code mapping.

use crate::canonical::ClientAction;
use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::RpcErrorCode;
use common::CommonError;
use thiserror::Error;

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

    /// Return the canonical error carried by this action, when one exists.
    pub fn canonical(&self) -> Option<&CanonicalError> {
        self.action.canonical()
    }

    /// Return the canonical error class, when this action carries a canonical error.
    pub fn class(&self) -> Option<ErrorClass> {
        self.canonical().map(|canonical| canonical.class)
    }

    /// Return the canonical error code, when this action carries one.
    pub fn code(&self) -> Option<&CanonicalErrorCode> {
        self.canonical().and_then(|canonical| canonical.code.as_ref())
    }

    /// Return the canonical refresh reason, when this action carries one.
    pub fn reason(&self) -> Option<RefreshReason> {
        self.canonical().and_then(|canonical| canonical.reason)
    }

    /// Return the canonical error message, when this action carries one.
    pub fn message(&self) -> Option<&str> {
        self.canonical().map(|canonical| canonical.message.as_str())
    }

    /// Return the retry-after delay in milliseconds, when this action carries one.
    pub fn retry_after_ms(&self) -> Option<u64> {
        self.canonical().and_then(|canonical| canonical.retry_after_ms)
    }

    /// Return whether the action is retryable under client retry policy.
    pub fn is_retryable(&self) -> bool {
        match self.action.as_ref() {
            ClientAction::Refresh {
                reason:
                    RefreshReason::Fencing
                    | RefreshReason::EpochMismatch
                    | RefreshReason::SessionInvalid
                    | RefreshReason::SessionExpired,
                ..
            } => false,
            ClientAction::Retry { .. } | ClientAction::Refresh { .. } => true,
            ClientAction::TransportFail { status } => {
                matches!(
                    status.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
                )
            }
            ClientAction::Fail { .. } | ClientAction::Ok => false,
        }
    }

    /// Return whether the action requires refreshing client metadata state.
    pub fn is_refresh_required(&self) -> bool {
        matches!(self.action.as_ref(), ClientAction::Refresh { .. })
    }
}

impl AsRef<ClientAction> for ClientActionError {
    fn as_ref(&self) -> &ClientAction {
        self.action.as_ref()
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
            ClientAction::Refresh { reason, canonical, .. } => write!(
                f,
                "Client action error: class={:?}, code={:?}, reason={:?}, message={}",
                canonical.class, canonical.code, reason, canonical.message
            ),
            ClientAction::Retry {
                retry_after_ms_hint,
                canonical,
            } => write!(
                f,
                "Client action error: class={:?}, code={:?}, retry_after_ms_hint={:?}, message={}",
                canonical.class, canonical.code, retry_after_ms_hint, canonical.message
            ),
            ClientAction::Fail { canonical } => write!(
                f,
                "Client action error: class={:?}, code={:?}, reason={:?}, message={}",
                canonical.class, canonical.code, canonical.reason, canonical.message
            ),
            ClientAction::TransportFail { status } => write!(
                f,
                "Client action error: transport code={:?}, message={}",
                status.code(),
                status.message()
            ),
            ClientAction::Ok => f.write_str("Client action error: ok action"),
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

    /// Structured action error derived from canonical/header validation.
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
            ClientError::Action(action) => match action.as_ref() {
                ClientAction::Refresh {
                    reason:
                        common::error::canonical::RefreshReason::Fencing
                        | common::error::canonical::RefreshReason::EpochMismatch
                        | common::error::canonical::RefreshReason::SessionInvalid
                        | common::error::canonical::RefreshReason::SessionExpired,
                    ..
                } => false,
                ClientAction::Retry { .. } | ClientAction::Refresh { .. } => true,
                ClientAction::TransportFail { status } => {
                    matches!(
                        status.code(),
                        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
                    )
                }
                ClientAction::Fail { .. } | ClientAction::Ok => false,
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
            ClientError::Action(action) if matches!(action.as_ref(), ClientAction::Refresh { .. })
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
            ClientError::Moved(msg) => {
                CanonicalError::need_refresh(RpcErrorCode::ShardMoved, RefreshReason::Moved, msg)
            }
            ClientError::VersionMismatch { expected, actual } => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("version mismatch: expected {}, got {}", expected, actual),
                refresh_hint: None,
            },
            ClientError::Action(action) => match action.into_action() {
                ClientAction::Ok => CanonicalError::ok("ok"),
                ClientAction::Refresh { canonical, .. }
                | ClientAction::Retry { canonical, .. }
                | ClientAction::Fail { canonical } => *canonical,
                ClientAction::TransportFail { status } => CanonicalError {
                    class: ErrorClass::Fatal,
                    code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                    reason: None,
                    retry_after_ms: None,
                    message: format!("transport status {:?}: {}", status.code(), status.message()),
                    refresh_hint: None,
                },
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
                    refresh_hint: None,
                }
            }
            ClientError::Metadata(msg) | ClientError::Worker(msg) | ClientError::Routing(msg) => {
                CanonicalError::retryable(RpcErrorCode::Application, Some(1000), msg)
            }
            ClientError::Cache(msg)
            | ClientError::Config(msg)
            | ClientError::InvalidArgument(msg)
            | ClientError::InvalidLayout(msg)
            | ClientError::UnknownOutcome(msg)
            | ClientError::Unimplemented(msg)
            | ClientError::Unsupported(msg)
            | ClientError::NotSupported(msg) => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: msg,
                refresh_hint: None,
            },
            ClientError::InvalidResponse { operation, reason } => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: format!("invalid response from {operation}: {reason}"),
                refresh_hint: None,
            },
            ClientError::StaleHandle { reason } => CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: reason,
                refresh_hint: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::{ClientAction, RefreshHint};

    #[test]
    fn action_error_exposes_canonical_diagnostics() {
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::ShardMoved,
            RefreshReason::RouteEpochMismatch,
            "route epoch is stale",
        );
        let err = ClientActionError::new(ClientAction::Refresh {
            reason: RefreshReason::RouteEpochMismatch,
            hint: Box::new(RefreshHint::default()),
            canonical: Box::new(canonical.clone()),
        });

        assert_eq!(err.canonical().unwrap().message, canonical.message);
        assert_eq!(err.class(), Some(ErrorClass::NeedRefresh));
        assert_eq!(err.code(), canonical.code.as_ref());
        assert_eq!(err.reason(), Some(RefreshReason::RouteEpochMismatch));
        assert_eq!(err.message(), Some("route epoch is stale"));
        assert_eq!(err.retry_after_ms(), None);
        assert!(err.is_retryable());
        assert!(err.is_refresh_required());

        let displayed = ClientError::Action(err).to_string();
        assert!(displayed.contains("NeedRefresh"));
        assert!(displayed.contains("ShardMoved"));
        assert!(displayed.contains("RouteEpochMismatch"));
        assert!(displayed.contains("route epoch is stale"));
    }

    #[test]
    fn client_action_roundtrip_preserves_machine_semantics_without_message_classification() {
        let refresh_canonical = CanonicalError::need_refresh(
            RpcErrorCode::NotLeader,
            RefreshReason::NotLeader,
            "fatal transport text",
        );
        let refresh = ClientError::from(ClientAction::Refresh {
            reason: RefreshReason::NotLeader,
            hint: Box::new(RefreshHint::default()),
            canonical: Box::new(refresh_canonical),
        });
        match refresh {
            ClientError::Action(action) => match action.as_ref() {
                ClientAction::Refresh { reason, canonical, .. } => {
                    assert_eq!(*reason, RefreshReason::NotLeader);
                    assert_eq!(canonical.class, ErrorClass::NeedRefresh);
                    assert_eq!(canonical.reason, Some(RefreshReason::NotLeader));
                }
                other => panic!("expected refresh action, got {other:?}"),
            },
            other => panic!("expected action error, got {other:?}"),
        }

        let retry_canonical = CanonicalError::retryable(RpcErrorCode::NodeUnavailable, Some(25), "please refresh");
        let retry = ClientError::from(ClientAction::Retry {
            retry_after_ms_hint: Some(25),
            canonical: Box::new(retry_canonical),
        });
        match retry {
            ClientError::Action(action) => match action.as_ref() {
                ClientAction::Retry {
                    retry_after_ms_hint,
                    canonical,
                } => {
                    assert_eq!(*retry_after_ms_hint, Some(25));
                    assert_eq!(canonical.class, ErrorClass::Retryable);
                    assert_eq!(canonical.retry_after_ms, Some(25));
                }
                other => panic!("expected retry action, got {other:?}"),
            },
            other => panic!("expected action error, got {other:?}"),
        }

        let fail_canonical = CanonicalError {
            class: ErrorClass::Fatal,
            code: Some(common::error::canonical::ErrorCode::RpcCode(
                RpcErrorCode::InvalidArgument,
            )),
            reason: None,
            retry_after_ms: None,
            message: "retry later".to_string(),
            refresh_hint: None,
        };
        let fail = ClientError::from(ClientAction::Fail {
            canonical: Box::new(fail_canonical),
        });
        match fail {
            ClientError::Action(action) => match action.as_ref() {
                ClientAction::Fail { canonical } => {
                    assert_eq!(canonical.class, ErrorClass::Fatal);
                    assert_eq!(
                        canonical.code,
                        Some(common::error::canonical::ErrorCode::RpcCode(
                            RpcErrorCode::InvalidArgument
                        ))
                    );
                }
                other => panic!("expected fail action, got {other:?}"),
            },
            other => panic!("expected action error, got {other:?}"),
        }

        let transport = ClientError::from(tonic::Status::unavailable("not leader"));
        match transport {
            ClientError::Action(action) => match action.as_ref() {
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
