// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Error classifier entry point.

use crate::error::ClientError;
use crate::rpc_error::ClientAction;
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction, RpcErrorDetail};
use beryl_common::header::{HEADER_PRE_HANDLER_REJECTION, PRE_HANDLER_REJECTION_RPC_CONCURRENCY};

/// Runtime error classification used by the metadata executor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ErrorClass {
    /// Unrecoverable error.
    Fatal,
    /// Retryable transport/framework failure.
    RetryableTransport,
    /// Server explicitly asked the client to retry.
    ServerRetry,
    /// Structured refresh is needed before replay.
    RefreshMetadata(ErrorKind),
    /// Local or server-side invalid argument.
    InvalidArgument,
    /// Malformed successful RPC header.
    InvalidHeader,
    /// Side-effecting operation outcome cannot be proven.
    UnknownOutcome,
    /// Permission denied.
    PermissionDenied,
    /// Unsupported operation.
    Unsupported,
    /// Fencing or writer-token mismatch.
    Fencing,
    /// Write session is no longer valid.
    SessionInvalid,
    /// Write session lease expired.
    SessionExpired,
}

impl ErrorClass {
    /// Low-cardinality label for metrics.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Fatal => "fatal",
            Self::RetryableTransport => "retryable_transport",
            Self::ServerRetry => "server_retry",
            Self::RefreshMetadata(_) => "refresh_metadata",
            Self::InvalidArgument => "invalid_argument",
            Self::InvalidHeader => "invalid_header",
            Self::UnknownOutcome => "unknown_outcome",
            Self::PermissionDenied => "permission_denied",
            Self::Unsupported => "unsupported",
            Self::Fencing => "fencing",
            Self::SessionInvalid => "session_invalid",
            Self::SessionExpired => "session_expired",
        }
    }
}

/// Classify a client error without string matching.
pub(crate) fn classify_error(err: &ClientError) -> ErrorClass {
    match err {
        ClientError::InvalidArgument(_) | ClientError::InvalidLayout(_) => ErrorClass::InvalidArgument,
        ClientError::InvalidResponse { .. } => ErrorClass::Fatal,
        ClientError::Unsupported(_) | ClientError::NotSupported(_) | ClientError::Unimplemented(_) => {
            ErrorClass::Unsupported
        }
        ClientError::Action(action) => classify_action(action.action()),
        ClientError::Common(common) if common.is_retryable() => ErrorClass::RetryableTransport,
        ClientError::UnknownOutcome(_) => ErrorClass::UnknownOutcome,
        ClientError::Metadata(_) | ClientError::Worker(_) | ClientError::Routing(_) => ErrorClass::Fatal,
        ClientError::NotLeader(_) => ErrorClass::RefreshMetadata(ErrorKind::Metadata(MetadataErrorKind::NotLeader)),
        ClientError::RouteEpochMismatch { .. } => {
            ErrorClass::RefreshMetadata(ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch))
        }
        ClientError::StaleMeta(_) => ErrorClass::RefreshMetadata(ErrorKind::Metadata(MetadataErrorKind::StaleState)),
        ClientError::Moved(_) => ErrorClass::Fatal,
        ClientError::Common(_)
        | ClientError::Cache(_)
        | ClientError::Config(_)
        | ClientError::StaleHandle { .. }
        | ClientError::VersionMismatch { .. } => ErrorClass::Fatal,
    }
}

fn classify_action(action: &ClientAction) -> ErrorClass {
    match action {
        ClientAction::TransportFail { status } if is_pre_handler_concurrency_rejection(status) => {
            ErrorClass::ServerRetry
        }
        ClientAction::TransportFail { status } if is_retryable_transport(status) => ErrorClass::RetryableTransport,
        ClientAction::TransportFail { .. } => ErrorClass::Fatal,
        ClientAction::Retry { .. } => ErrorClass::ServerRetry,
        ClientAction::Refresh { rpc_error, .. } => classify_refresh_action(rpc_error),
        ClientAction::Fail { rpc_error } => classify_fail_action(rpc_error),
    }
}

/// Identifies a definite pre-handler rejection that is safe to replay.
///
/// Unmarked resource exhaustion remains a transport failure because its
/// side-effect outcome cannot be inferred from the status code alone.
fn is_pre_handler_concurrency_rejection(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::ResourceExhausted
        && status
            .metadata()
            .get(HEADER_PRE_HANDLER_REJECTION)
            .and_then(|value| value.to_str().ok())
            == Some(PRE_HANDLER_REJECTION_RPC_CONCURRENCY)
}

fn classify_refresh_action(rpc_error: &RpcErrorDetail) -> ErrorClass {
    match (&rpc_error.recovery, rpc_error.kind) {
        (RecoveryAction::ReopenWriteSession { .. }, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid)) => {
            ErrorClass::SessionInvalid
        }
        (RecoveryAction::ReopenWriteSession { .. }, ErrorKind::Metadata(MetadataErrorKind::SessionExpired)) => {
            ErrorClass::SessionExpired
        }
        (
            RecoveryAction::ReopenWriteSession { .. },
            ErrorKind::Metadata(MetadataErrorKind::Fencing) | ErrorKind::Metadata(MetadataErrorKind::EpochMismatch),
        ) => ErrorClass::Fencing,
        (
            _,
            ErrorKind::Metadata(MetadataErrorKind::Fencing) | ErrorKind::Metadata(MetadataErrorKind::EpochMismatch),
        ) => ErrorClass::Fencing,
        _ => ErrorClass::RefreshMetadata(rpc_error.kind),
    }
}

fn classify_fail_action(rpc_error: &RpcErrorDetail) -> ErrorClass {
    match rpc_error.kind {
        ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader) => ErrorClass::InvalidHeader,
        ErrorKind::Metadata(MetadataErrorKind::Fencing) | ErrorKind::Metadata(MetadataErrorKind::EpochMismatch) => {
            ErrorClass::Fencing
        }
        ErrorKind::Protocol(ProtocolErrorKind::PermissionDenied) => ErrorClass::PermissionDenied,
        ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument) => ErrorClass::InvalidArgument,
        ErrorKind::Protocol(ProtocolErrorKind::Unsupported) => ErrorClass::Unsupported,
        _ => ErrorClass::Fatal,
    }
}

fn is_retryable_transport(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
    )
}
