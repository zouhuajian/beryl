// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client error types and error code mapping.

use crate::canonical::ClientAction;
use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::RpcErrorCode;
use common::{CommonError, CommonErrorCode};
use proto::common::RpcErrorCodeProto as ProtoErrorCode;
use thiserror::Error;

/// Opaque structured action error derived from RPC header validation.
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

/// Client-specific error type.
#[derive(Error, Debug)]
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
    #[error("Client action error")]
    Action(ClientActionError),
}

/// Result type alias for client operations.
pub type ClientResult<T> = Result<T, ClientError>;

pub(crate) fn side_effect_response_body_mismatch(operation: &str, detail: impl std::fmt::Display) -> ClientError {
    ClientError::UnknownOutcome(format!("{operation} response body mismatch after OK header: {detail}"))
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
            17 => ClientError::from(ClientAction::Refresh {
                reason: RefreshReason::StaleState,
                hint: Box::default(),
                canonical: Box::new(CanonicalError::need_refresh(
                    RpcErrorCode::StaleState,
                    RefreshReason::StaleState,
                    "Need refresh",
                )),
            }),
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
