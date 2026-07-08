// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Error classifier entry point.

use crate::error::ClientError;
use crate::rpc_error::ClientAction;
use common::error::rpc::{ErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction, RpcErrorDetail};
use types::fs::FsErrorCode;

/// Metadata refresh cause used by the runtime executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetadataRefreshCause {
    /// Metadata leader changed or the target is not leader.
    NotLeader,
    /// Path or mount owner group changed.
    OwnerGroupMismatch,
    /// Mount epoch changed.
    MountEpochMismatch,
    /// Metadata state watermark is stale.
    StaleState,
    /// Route/layout epoch changed.
    RouteEpochMismatch,
    /// Worker process run changed while WorkerId and endpoint may be reused.
    WorkerRunMismatch,
    /// Worker reported that the metadata-provided block stamp is stale.
    BlockStampMismatch,
    /// Refresh was requested without enough structured detail.
    Unknown,
}

impl MetadataRefreshCause {
    /// Low-cardinality label for metrics.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::NotLeader => "not_leader",
            Self::OwnerGroupMismatch => "owner_group_mismatch",
            Self::MountEpochMismatch => "mount_epoch_mismatch",
            Self::StaleState => "stale_state",
            Self::RouteEpochMismatch => "route_epoch_mismatch",
            Self::WorkerRunMismatch => "worker_run_mismatch",
            Self::BlockStampMismatch => "block_stamp_mismatch",
            Self::Unknown => "unknown",
        }
    }
}

/// Runtime error classification used by [`crate::runtime::OperationExecutor`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ErrorClass {
    /// Unrecoverable error.
    Fatal,
    /// Retryable transport/framework failure.
    RetryableTransport,
    /// Structured refresh is needed before replay.
    RefreshMetadata(MetadataRefreshCause),
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

/// Classifies transport, metadata rpc_error, and worker RPC errors.
#[derive(Clone, Debug, Default)]
pub(crate) struct ErrorClassifier;

impl ErrorClassifier {
    /// Classify a client error without string matching.
    pub(crate) fn classify_error(&self, err: &ClientError) -> ErrorClass {
        match err {
            ClientError::InvalidArgument(_) | ClientError::InvalidLayout(_) => ErrorClass::InvalidArgument,
            ClientError::InvalidResponse { .. } => ErrorClass::Fatal,
            ClientError::Unsupported(_) | ClientError::NotSupported(_) | ClientError::Unimplemented(_) => {
                ErrorClass::Unsupported
            }
            ClientError::Action(action) => self.classify_action(action.action()),
            ClientError::Common(common) if common.is_retryable() => ErrorClass::RetryableTransport,
            ClientError::UnknownOutcome(_) => ErrorClass::UnknownOutcome,
            ClientError::Metadata(_) | ClientError::Worker(_) | ClientError::Routing(_) => ErrorClass::Fatal,
            ClientError::NotLeader(_) => ErrorClass::RefreshMetadata(MetadataRefreshCause::NotLeader),
            ClientError::RouteEpochMismatch { .. } => {
                ErrorClass::RefreshMetadata(MetadataRefreshCause::RouteEpochMismatch)
            }
            ClientError::StaleMeta(_) => ErrorClass::RefreshMetadata(MetadataRefreshCause::StaleState),
            ClientError::Moved(_) => ErrorClass::RefreshMetadata(MetadataRefreshCause::Unknown),
            ClientError::Common(_)
            | ClientError::Cache(_)
            | ClientError::Config(_)
            | ClientError::StaleHandle { .. }
            | ClientError::VersionMismatch { .. } => ErrorClass::Fatal,
        }
    }

    fn classify_action(&self, action: &ClientAction) -> ErrorClass {
        match action {
            ClientAction::TransportFail { status } if is_retryable_transport(status) => ErrorClass::RetryableTransport,
            ClientAction::TransportFail { .. } => ErrorClass::Fatal,
            ClientAction::Retry { .. } => ErrorClass::RetryableTransport,
            ClientAction::Refresh { reason, rpc_error, .. } => classify_refresh_action(*reason, rpc_error),
            ClientAction::Fail { rpc_error } => classify_fail_action(rpc_error),
        }
    }
}

fn classify_refresh_action(reason: MetadataRefreshCause, rpc_error: &RpcErrorDetail) -> ErrorClass {
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
        _ => ErrorClass::RefreshMetadata(reason),
    }
}

fn classify_fail_action(rpc_error: &RpcErrorDetail) -> ErrorClass {
    match rpc_error.kind {
        ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader) => ErrorClass::InvalidHeader,
        ErrorKind::Metadata(MetadataErrorKind::Fencing) | ErrorKind::Metadata(MetadataErrorKind::EpochMismatch) => {
            ErrorClass::Fencing
        }
        ErrorKind::Protocol(ProtocolErrorKind::PermissionDenied)
        | ErrorKind::Fs(FsErrorCode::EPerm | FsErrorCode::EAcces) => ErrorClass::PermissionDenied,
        ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument) | ErrorKind::Fs(FsErrorCode::EInval) => {
            ErrorClass::InvalidArgument
        }
        ErrorKind::Protocol(ProtocolErrorKind::Unsupported)
        | ErrorKind::Fs(FsErrorCode::ENotsup | FsErrorCode::ENotImpl) => ErrorClass::Unsupported,
        _ => ErrorClass::Fatal,
    }
}

fn is_retryable_transport(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ClientError;
    use crate::rpc_error::ClientAction;
    use common::error::rpc::{ErrorKind, RecoveryAction, RefreshHint as RpcRefreshHint, RpcErrorDetail};

    #[test]
    fn refresh_action_uses_runtime_reason_for_refresh_class() {
        let rpc_error = RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch),
            RpcRefreshHint::default(),
            "owner moved",
        );
        let err = ClientError::from(ClientAction::Refresh {
            reason: MetadataRefreshCause::OwnerGroupMismatch,
            hint: Box::default(),
            rpc_error: Box::new(rpc_error),
        });

        assert_eq!(
            ErrorClassifier.classify_error(&err),
            ErrorClass::RefreshMetadata(MetadataRefreshCause::OwnerGroupMismatch)
        );
    }

    #[test]
    fn non_ok_tonic_status_remains_transport_failure() {
        let err = ClientError::from(tonic::Status::unavailable("metadata unavailable"));

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::RetryableTransport);
    }

    #[test]
    fn invalid_response_classifies_as_fatal_protocol_failure() {
        let err = ClientError::InvalidResponse {
            operation: "GetStatus",
            reason: "missing attrs".to_string(),
        };

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::Fatal);
    }

    #[test]
    fn reopen_session_action_classifies_session_expired() {
        let rpc_error = RpcErrorDetail::reopen_write_session(
            ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
            RpcRefreshHint::default(),
            "expired",
        );
        let err = ClientError::from(ClientAction::Refresh {
            reason: MetadataRefreshCause::Unknown,
            hint: Box::default(),
            rpc_error: Box::new(rpc_error),
        });

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::SessionExpired);
    }

    #[test]
    fn fatal_fencing_kind_is_typed_and_not_transport_retryable() {
        let err = ClientError::from(ClientAction::Fail {
            rpc_error: Box::new(RpcErrorDetail::fail(
                ErrorKind::Metadata(MetadataErrorKind::Fencing),
                "fencing mismatch",
            )),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::Fencing);
        assert_ne!(classified, ErrorClass::RetryableTransport);
    }

    #[test]
    fn invalid_header_kind_is_typed_and_not_transport_retryable() {
        let err = ClientError::from(ClientAction::Fail {
            rpc_error: Box::new(RpcErrorDetail::fail(
                ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader),
                "malformed OK response",
            )),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::InvalidHeader);
        assert_ne!(classified, ErrorClass::RetryableTransport);
    }

    #[test]
    fn fail_session_kind_is_not_a_session_control_signal() {
        let err = ClientError::from(ClientAction::Fail {
            rpc_error: Box::new(RpcErrorDetail::new(
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                RecoveryAction::Fail,
                "fatal session-shaped error",
            )),
        });

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::Fatal);
    }
}
