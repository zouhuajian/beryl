// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Error classifier entry point.

use crate::canonical::ClientAction;
use crate::error::ClientError;
use common::error::canonical::RefreshReason as CanonicalRefreshReason;

/// Refresh reason used by the runtime executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshReason {
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

impl RefreshReason {
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
pub enum ErrorClass {
    /// Unrecoverable error.
    Fatal,
    /// Retryable transport/framework failure.
    RetryableTransport,
    /// Structured refresh is needed before replay.
    NeedRefresh(RefreshReason),
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
            Self::NeedRefresh(_) => "need_refresh",
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

/// Classifies transport, metadata canonical, and worker canonical errors.
#[derive(Clone, Debug, Default)]
pub struct ErrorClassifier;

impl ErrorClassifier {
    /// Classify a client error without string matching.
    pub fn classify_error(&self, err: &ClientError) -> ErrorClass {
        match err {
            ClientError::InvalidArgument(_) | ClientError::InvalidLayout(_) => ErrorClass::InvalidArgument,
            ClientError::InvalidResponse { .. } => ErrorClass::Fatal,
            ClientError::Unsupported(_) | ClientError::NotSupported(_) | ClientError::Unimplemented(_) => {
                ErrorClass::Unsupported
            }
            ClientError::Action(action) => self.classify_action(action.as_ref()),
            ClientError::Common(common) if common.is_retryable() => ErrorClass::RetryableTransport,
            ClientError::UnknownOutcome(_) => ErrorClass::UnknownOutcome,
            ClientError::Metadata(_) | ClientError::Worker(_) | ClientError::Routing(_) => ErrorClass::Fatal,
            ClientError::NotLeader(_) => ErrorClass::NeedRefresh(RefreshReason::NotLeader),
            ClientError::RouteEpochMismatch { .. } => ErrorClass::NeedRefresh(RefreshReason::RouteEpochMismatch),
            ClientError::StaleMeta(_) => ErrorClass::NeedRefresh(RefreshReason::StaleState),
            ClientError::Moved(_) => ErrorClass::NeedRefresh(RefreshReason::Unknown),
            ClientError::Common(_)
            | ClientError::Cache(_)
            | ClientError::Config(_)
            | ClientError::StaleHandle { .. }
            | ClientError::VersionMismatch { .. } => ErrorClass::Fatal,
        }
    }

    fn classify_action(&self, action: &ClientAction) -> ErrorClass {
        match action {
            ClientAction::Ok => ErrorClass::Fatal,
            ClientAction::TransportFail { status } if is_retryable_transport(status) => ErrorClass::RetryableTransport,
            ClientAction::TransportFail { .. } => ErrorClass::Fatal,
            ClientAction::Retry { .. } => ErrorClass::RetryableTransport,
            ClientAction::Refresh { reason, hint, .. } => classify_refresh_reason(*reason, hint.group_name.as_ref()),
            ClientAction::Fail { canonical } => {
                use common::error::canonical::ErrorCode;
                use types::fs::FsErrorCode;
                match canonical.code.as_ref() {
                    Some(ErrorCode::RpcCode(common::header::RpcErrorCode::PermissionDenied)) => {
                        ErrorClass::PermissionDenied
                    }
                    Some(ErrorCode::RpcCode(common::header::RpcErrorCode::InvalidHeader)) => ErrorClass::InvalidHeader,
                    Some(ErrorCode::RpcCode(common::header::RpcErrorCode::Fencing)) => ErrorClass::Fencing,
                    Some(ErrorCode::FsErrno(FsErrorCode::EPerm | FsErrorCode::EAcces)) => ErrorClass::PermissionDenied,
                    Some(ErrorCode::FsErrno(FsErrorCode::EInval)) => ErrorClass::InvalidArgument,
                    Some(ErrorCode::FsErrno(FsErrorCode::ENotsup | FsErrorCode::ENotImpl)) => ErrorClass::Unsupported,
                    _ => ErrorClass::Fatal,
                }
            }
        }
    }
}

fn classify_refresh_reason(reason: CanonicalRefreshReason, _group_hint: Option<&types::GroupName>) -> ErrorClass {
    match reason {
        CanonicalRefreshReason::Fencing | CanonicalRefreshReason::EpochMismatch => ErrorClass::Fencing,
        CanonicalRefreshReason::SessionInvalid => ErrorClass::SessionInvalid,
        CanonicalRefreshReason::SessionExpired => ErrorClass::SessionExpired,
        other => ErrorClass::NeedRefresh(refresh_reason_from_canonical(other)),
    }
}

fn refresh_reason_from_canonical(reason: CanonicalRefreshReason) -> RefreshReason {
    match reason {
        CanonicalRefreshReason::NotLeader => RefreshReason::NotLeader,
        CanonicalRefreshReason::OwnerGroupMismatch => RefreshReason::OwnerGroupMismatch,
        CanonicalRefreshReason::Moved => RefreshReason::Unknown,
        CanonicalRefreshReason::StaleState => RefreshReason::StaleState,
        CanonicalRefreshReason::MountEpochMismatch => RefreshReason::MountEpochMismatch,
        CanonicalRefreshReason::RouteEpochMismatch => RefreshReason::RouteEpochMismatch,
        CanonicalRefreshReason::WorkerRunMismatch => RefreshReason::WorkerRunMismatch,
        CanonicalRefreshReason::GroupMismatch
        | CanonicalRefreshReason::NeedRegister
        | CanonicalRefreshReason::FullReportRequired => RefreshReason::Unknown,
        CanonicalRefreshReason::BlockStampMismatch => RefreshReason::BlockStampMismatch,
        CanonicalRefreshReason::Unknown => RefreshReason::Unknown,
        CanonicalRefreshReason::Fencing
        | CanonicalRefreshReason::EpochMismatch
        | CanonicalRefreshReason::SessionInvalid
        | CanonicalRefreshReason::SessionExpired => RefreshReason::Unknown,
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
    use crate::canonical::{ClientAction, RefreshHint};
    use crate::error::ClientError;
    use common::error::canonical::{CanonicalError, RefreshHint as CanonicalRefreshHint};
    use common::header::RpcErrorCode;
    use types::GroupName;

    #[test]
    fn owner_group_mismatch_reason_classifies_as_owner_group_mismatch() {
        let canonical = CanonicalError::need_refresh_with_hint(
            RpcErrorCode::ShardMoved,
            common::error::canonical::RefreshReason::OwnerGroupMismatch,
            CanonicalRefreshHint {
                group_name: Some("analytics".to_string()),
                ..CanonicalRefreshHint::default()
            },
            "owner moved",
        );
        let err = ClientError::from(ClientAction::Refresh {
            reason: common::error::canonical::RefreshReason::OwnerGroupMismatch,
            hint: Box::new(RefreshHint {
                group_name: Some(GroupName::parse("analytics").unwrap()),
                ..RefreshHint::default()
            }),
            canonical: Box::new(canonical),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::NeedRefresh(RefreshReason::OwnerGroupMismatch));
    }

    #[test]
    fn generic_moved_no_longer_infers_owner_group_from_hint() {
        let canonical = CanonicalError::need_refresh_with_hint(
            RpcErrorCode::ShardMoved,
            common::error::canonical::RefreshReason::Moved,
            CanonicalRefreshHint {
                group_name: Some("analytics".to_string()),
                ..CanonicalRefreshHint::default()
            },
            "resource moved",
        );
        let err = ClientError::from(ClientAction::Refresh {
            reason: common::error::canonical::RefreshReason::Moved,
            hint: Box::new(RefreshHint {
                group_name: Some(GroupName::parse("analytics").unwrap()),
                ..RefreshHint::default()
            }),
            canonical: Box::new(canonical),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::NeedRefresh(RefreshReason::Unknown));
    }

    #[test]
    fn need_refresh_without_structured_reason_is_conservative() {
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::Application,
            common::error::canonical::RefreshReason::Unknown,
            "unknown refresh",
        );
        let err = ClientError::from(ClientAction::Refresh {
            reason: common::error::canonical::RefreshReason::Unknown,
            hint: Box::default(),
            canonical: Box::new(canonical),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::NeedRefresh(RefreshReason::Unknown));
    }

    #[test]
    fn block_stamp_mismatch_classifies_as_typed_refresh_reason() {
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::BlockStampMismatch,
            common::error::canonical::RefreshReason::BlockStampMismatch,
            "block stamp mismatch",
        );
        let err = ClientError::from(ClientAction::Refresh {
            reason: common::error::canonical::RefreshReason::BlockStampMismatch,
            hint: Box::default(),
            canonical: Box::new(canonical),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::NeedRefresh(RefreshReason::BlockStampMismatch));
    }

    #[test]
    fn worker_run_mismatch_classifies_as_typed_refresh_reason() {
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::WorkerRunMismatch,
            common::error::canonical::RefreshReason::WorkerRunMismatch,
            "worker run mismatch",
        );
        let err = ClientError::from(ClientAction::Refresh {
            reason: common::error::canonical::RefreshReason::WorkerRunMismatch,
            hint: Box::default(),
            canonical: Box::new(canonical),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::NeedRefresh(RefreshReason::WorkerRunMismatch));
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
    fn fencing_refresh_is_typed_and_not_transport_retryable() {
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::Fencing,
            common::error::canonical::RefreshReason::Fencing,
            "fencing mismatch",
        );
        let err = ClientError::from(ClientAction::Refresh {
            reason: common::error::canonical::RefreshReason::Fencing,
            hint: Box::default(),
            canonical: Box::new(canonical),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::Fencing);
    }

    #[test]
    fn fatal_fencing_rpc_code_is_typed_and_not_transport_retryable() {
        let err = ClientError::from(ClientAction::Fail {
            canonical: Box::new(CanonicalError {
                class: common::error::canonical::ErrorClass::Fatal,
                code: Some(common::error::canonical::ErrorCode::RpcCode(RpcErrorCode::Fencing)),
                reason: None,
                retry_after_ms: None,
                message: "fencing mismatch".to_string(),
                refresh_hint: None,
            }),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::Fencing);
        assert_ne!(classified, ErrorClass::RetryableTransport);
    }

    #[test]
    fn invalid_header_rpc_code_is_typed_and_not_transport_retryable() {
        let err = ClientError::from(ClientAction::Fail {
            canonical: Box::new(CanonicalError {
                class: common::error::canonical::ErrorClass::Fatal,
                code: Some(common::error::canonical::ErrorCode::RpcCode(
                    RpcErrorCode::InvalidHeader,
                )),
                reason: None,
                retry_after_ms: None,
                message: "malformed OK response".to_string(),
                refresh_hint: None,
            }),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::InvalidHeader);
        assert_ne!(classified, ErrorClass::RetryableTransport);
    }

    #[test]
    fn session_invalid_refresh_is_typed_and_not_transport_retryable() {
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::Application,
            common::error::canonical::RefreshReason::SessionInvalid,
            "session invalid",
        );
        let err = ClientError::from(ClientAction::Refresh {
            reason: common::error::canonical::RefreshReason::SessionInvalid,
            hint: Box::default(),
            canonical: Box::new(canonical),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::SessionInvalid);
    }

    #[test]
    fn session_expired_refresh_is_typed_and_not_transport_retryable() {
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::Application,
            common::error::canonical::RefreshReason::SessionExpired,
            "session expired",
        );
        let err = ClientError::from(ClientAction::Refresh {
            reason: common::error::canonical::RefreshReason::SessionExpired,
            hint: Box::default(),
            canonical: Box::new(canonical),
        });

        let classified = ErrorClassifier.classify_error(&err);

        assert_eq!(classified, ErrorClass::SessionExpired);
    }
}
