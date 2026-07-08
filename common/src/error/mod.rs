// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Common error types and utilities.

use crate::header::RequestHeader;
use std::error::Error as StdError;
use std::fmt;

pub mod rpc {
    //! RPC header error model for Vecton.
    //!
    //! The model has two independent axes:
    //! - `ErrorKind`: the fact that failed.
    //! - `RecoveryAction`: what a caller should do next.
    //!
    //! Human-readable `message` and structured `detail` are diagnostic only.
    //! Machine control flow must branch on `kind` and `recovery`.

    use serde::{Deserialize, Serialize};
    use types::fs::FsErrorCode;

    /// Stable, machine-readable failure fact.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum ErrorKind {
        Fs(FsErrorCode),
        Metadata(MetadataErrorKind),
        Worker(WorkerErrorKind),
        Ufs(UfsErrorKind),
        Protocol(ProtocolErrorKind),
        Internal(InternalErrorKind),
    }

    /// Metadata-domain failure fact.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum MetadataErrorKind {
        NotFound,
        AlreadyExists,
        NotDirectory,
        IsDirectory,
        DirectoryNotEmpty,
        CrossMountRename,
        Busy,
        Conflict,
        NotLeader,
        StaleState,
        MountEpochMismatch,
        RouteEpochMismatch,
        OwnerGroupMismatch,
        GroupMismatch,
        Fencing,
        SessionInvalid,
        SessionExpired,
        EpochMismatch,
        ResourceExhausted,
    }

    /// Worker-domain failure fact.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum WorkerErrorKind {
        NotRegistered,
        RunMismatch,
        DescriptorMismatch,
        FullReportRequired,
        BlockLocationUnavailable,
        BlockStampMismatch,
        NodeUnavailable,
        Timeout,
        ResourceExhausted,
        Conflict,
        Corrupt,
        Fencing,
        Cancelled,
        Io,
    }

    /// UFS-domain failure fact.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum UfsErrorKind {
        NotFound,
        PermissionDenied,
        Unsupported,
        NotImplemented,
        InvalidSpec,
        InvalidPath,
        UnexpectedEof,
        Backend,
        Overloaded,
        Timeout,
    }

    /// Protocol and request-shape failure fact.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum ProtocolErrorKind {
        InvalidHeader,
        InvalidArgument,
        PermissionDenied,
        Unsupported,
        Cancelled,
        Corrupt,
    }

    /// Internal or infrastructure failure fact.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum InternalErrorKind {
        NodeUnavailable,
        Timeout,
        ResourceExhausted,
        Cancelled,
        Corrupt,
        Internal,
    }

    /// Caller recovery strategy. This is deliberately smaller than `ErrorKind`.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum RecoveryAction {
        Fail,
        Retry { after_ms: Option<u64> },
        RefreshMetadata { hint: RefreshHint },
        ReopenWriteSession { hint: RefreshHint },
        RegisterWorker,
        SendFullBlockReport,
    }

    /// Worker endpoint hint used in RPC refresh hints.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WorkerEndpointHint {
        pub worker_id: u64,
        pub endpoint: String,
        pub worker_net_protocol: i32,
    }

    /// Structured refresh hints attached to RPC errors.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RefreshHint {
        pub leader_endpoint: Option<String>,
        pub group_name: Option<String>,
        pub mount_epoch: Option<u64>,
        pub mount_prefix: Option<String>,
        pub route_epoch: Option<u64>,
        pub worker_endpoints: Vec<WorkerEndpointHint>,
        pub worker_resolve_required: bool,
    }

    /// Diagnostic structured data. This must not drive control flow.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ErrorDetail {
        pub fields: Vec<ErrorDetailField>,
    }

    /// One diagnostic field in `ErrorDetail`.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ErrorDetailField {
        pub key: String,
        pub value: String,
    }

    /// RPC error model for Vecton.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct RpcErrorDetail {
        pub kind: ErrorKind,
        pub recovery: RecoveryAction,
        pub message: String,
        pub detail: ErrorDetail,
    }

    impl RpcErrorDetail {
        pub fn new(kind: ErrorKind, recovery: RecoveryAction, message: impl Into<String>) -> Self {
            Self {
                kind,
                recovery,
                message: message.into(),
                detail: ErrorDetail::default(),
            }
        }

        pub fn fs(errno: FsErrorCode, message: impl Into<String>) -> Self {
            Self::new(ErrorKind::Fs(errno), RecoveryAction::Fail, message)
        }

        pub fn fail(kind: ErrorKind, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::Fail, message)
        }

        pub fn retry(kind: ErrorKind, after_ms: Option<u64>, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::Retry { after_ms }, message)
        }

        pub fn refresh_metadata(kind: ErrorKind, hint: RefreshHint, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::RefreshMetadata { hint }, message)
        }

        pub fn reopen_write_session(kind: ErrorKind, hint: RefreshHint, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::ReopenWriteSession { hint }, message)
        }

        pub fn register_worker(kind: ErrorKind, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::RegisterWorker, message)
        }

        pub fn send_full_block_report(kind: ErrorKind, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::SendFullBlockReport, message)
        }

        pub fn with_detail_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
            self.detail.fields.push(ErrorDetailField {
                key: key.into(),
                value: value.into(),
            });
            self
        }
    }
}

/// Error codes for common error classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CommonErrorCode {
    /// Operation timed out.
    Timeout,
    /// Service is unavailable.
    Unavailable,
    /// Request was throttled/rate-limited.
    Throttled,
    /// Service is overloaded (too many concurrent requests).
    Overloaded,
    /// Resource not found.
    NotFound,
    /// Permission denied.
    PermissionDenied,
    /// Invalid argument.
    InvalidArgument,
    /// I/O error.
    Io,
    /// Internal error.
    Internal,
}

impl CommonErrorCode {
    /// Check if this error code is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            CommonErrorCode::Timeout
                | CommonErrorCode::Unavailable
                | CommonErrorCode::Throttled
                | CommonErrorCode::Overloaded
        )
    }
}

impl fmt::Display for CommonErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommonErrorCode::Timeout => write!(f, "Timeout"),
            CommonErrorCode::Unavailable => write!(f, "Unavailable"),
            CommonErrorCode::Throttled => write!(f, "Throttled"),
            CommonErrorCode::Overloaded => write!(f, "Overloaded"),
            CommonErrorCode::NotFound => write!(f, "NotFound"),
            CommonErrorCode::PermissionDenied => write!(f, "PermissionDenied"),
            CommonErrorCode::InvalidArgument => write!(f, "InvalidArgument"),
            CommonErrorCode::Io => write!(f, "Io"),
            CommonErrorCode::Internal => write!(f, "Internal"),
        }
    }
}

/// Metadata attached to errors for debugging and observability.
#[derive(Clone, Debug, Default)]
pub struct ErrorMeta {
    /// Call ID associated with this error.
    pub call_id: Option<types::CallId>,
    /// Operation name (e.g., "ufs.read", "worker.rpc").
    pub op: Option<String>,
    /// Peer endpoint or identifier.
    pub peer: Option<String>,
    /// Whether this error is retryable.
    pub retryable: bool,
}

/// Common error type used across all modules.
#[derive(Debug)]
pub struct CommonError {
    /// Error code.
    pub code: CommonErrorCode,
    /// Human-readable error message.
    pub message: String,
    /// Source error (if any).
    pub source: Option<Box<dyn StdError + Send + Sync>>,
    /// Error metadata.
    pub meta: ErrorMeta,
}

impl Clone for CommonError {
    fn clone(&self) -> Self {
        Self {
            code: self.code,
            message: self.message.clone(),
            // We can't clone the source error, so just create a simple error with the message
            source: None,
            meta: self.meta.clone(),
        }
    }
}

impl CommonError {
    /// Create a new CommonError.
    pub fn new(code: CommonErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
            meta: ErrorMeta {
                retryable: code.is_retryable(),
                ..Default::default()
            },
        }
    }

    /// Set the source error.
    pub fn with_source(mut self, source: Box<dyn StdError + Send + Sync>) -> Self {
        self.source = Some(source);
        self
    }

    /// Set error metadata from RequestHeader.
    pub fn with_ctx(mut self, ctx: &RequestHeader) -> Self {
        self.meta.call_id = Some(ctx.client.call_id);
        self
    }

    /// Set the operation name.
    pub fn with_op(mut self, op: impl Into<String>) -> Self {
        self.meta.op = Some(op.into());
        self
    }

    /// Set the peer identifier.
    pub fn with_peer(mut self, peer: impl Into<String>) -> Self {
        self.meta.peer = Some(peer.into());
        self
    }

    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        self.meta.retryable
    }
}

impl fmt::Display for CommonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)?;
        if let Some(ref op) = self.meta.op {
            write!(f, " (op: {})", op)?;
        }
        if let Some(ref peer) = self.meta.peer {
            write!(f, " (peer: {})", peer)?;
        }
        if let Some(ref call_id) = self.meta.call_id {
            write!(f, " (call_id: {})", call_id)?;
        }
        Ok(())
    }
}

impl StdError for CommonError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_ref().map(|e| e.as_ref() as _)
    }
}

/// Extension trait for Result to add context.
pub trait ResultExt<T> {
    /// Add RequestHeader to the error.
    fn with_ctx(self, ctx: &RequestHeader) -> Result<T, CommonError>;

    /// Add operation name to the error.
    fn with_op(self, op: &str) -> Result<T, CommonError>;

    /// Add peer identifier to the error.
    fn with_peer(self, peer: &str) -> Result<T, CommonError>;
}

impl<T, E> ResultExt<T> for Result<T, E>
where
    E: Into<CommonError>,
{
    fn with_ctx(self, ctx: &RequestHeader) -> Result<T, CommonError> {
        self.map_err(|e| e.into().with_ctx(ctx))
    }

    fn with_op(self, op: &str) -> Result<T, CommonError> {
        self.map_err(|e| e.into().with_op(op))
    }

    fn with_peer(self, peer: &str) -> Result<T, CommonError> {
        self.map_err(|e| e.into().with_peer(peer))
    }
}

impl From<CommonErrorCode> for CommonError {
    fn from(code: CommonErrorCode) -> Self {
        CommonError::new(code, code.to_string())
    }
}

#[cfg(test)]
#[path = "rpc_tests.rs"]
mod rpc_tests;

#[cfg(test)]
mod tests {
    use super::rpc::{ErrorKind, InternalErrorKind, MetadataErrorKind, RecoveryAction, RefreshHint, RpcErrorDetail};
    use types::fs::FsErrorCode;

    #[test]
    fn rpc_fs_error_fails() {
        let err = RpcErrorDetail::fs(FsErrorCode::ENoEnt, "no such file");
        assert_eq!(err.kind, ErrorKind::Fs(FsErrorCode::ENoEnt));
        assert_eq!(err.recovery, RecoveryAction::Fail);
    }

    #[test]
    fn rpc_refresh_carries_hint() {
        let hint = RefreshHint {
            group_name: Some("root".to_string()),
            ..RefreshHint::default()
        };
        let err = RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::NotLeader),
            hint,
            "leader changed",
        );
        assert_eq!(err.kind, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
        assert!(matches!(err.recovery, RecoveryAction::RefreshMetadata { .. }));
    }

    #[test]
    fn rpc_retry_keeps_backoff_in_recovery() {
        let err = RpcErrorDetail::retry(
            ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
            Some(1000),
            "unavailable",
        );
        assert_eq!(err.recovery, RecoveryAction::Retry { after_ms: Some(1000) });
    }
}
