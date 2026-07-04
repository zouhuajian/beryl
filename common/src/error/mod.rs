// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Common error types and utilities.

use crate::header::RequestHeader;
use std::error::Error as StdError;
use std::fmt;

pub mod canonical {
    //! Canonical, protocol-independent error model for vecton.
    //!
    //! This sits *above* module-local error types (MetadataError, WorkerError,
    //! ClientError, FsErrorCode, RpcErrorCode) and *below* the
    //! concrete protobuf headers. The goal is:
    //!
    //! - Provide a single semantic classification:
    //!   - `ErrorClass`: `Ok` / `NeedRefresh` / `Retryable` / `Fatal`
    //!   - `RefreshReason`: fine‑grained refresh reasons (leader change,
    //!     epoch/route mismatch, fencing, block stamp mismatch, etc.)
    //! - Provide a single place for:
    //!   - FS errno vs RPC error code (`ErrorCode`)
    //!   - `retry_after_ms` source of truth
    //! - Enforce invariants *in Rust* even before protobuf headers are fully
    //!   converged.
    //!
    //! NOTE:
    //! - This module MUST NOT depend on generated protobuf types.
    //! - All conversions between module errors and `CanonicalError` should live
    //!   in the respective owner crates (metadata/worker/client).

    use crate::header::RpcErrorCode;
    use serde::{Deserialize, Serialize};
    use types::fs::FsErrorCode;

    /// Coarse-grained error class used for client behaviour.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum ErrorClass {
        /// No error.
        Ok,
        /// Client must refresh routing / mount / state before retrying.
        NeedRefresh,
        /// Temporary failure, can retry (possibly with backoff).
        Retryable,
        /// Unrecoverable from client perspective.
        Fatal,
    }

    /// Reason for `ErrorClass::NeedRefresh` (or closely related routing/state
    /// issues). This enum intentionally mirrors – but does not *depend on* –
    /// current RpcErrorCode / data-plane enums so that we can evolve proto
    /// independently.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum RefreshReason {
        /// Unknown or unclassified refresh hint.
        Unknown,
        /// Not leader for the requested group.
        NotLeader,
        /// Path or mount owner group mismatch.
        OwnerGroupMismatch,
        /// Resource relocated, but the producer cannot classify the refresh more specifically.
        Moved,
        /// Follower state is behind requested state watermark.
        StaleState,
        /// Mount epoch mismatch (client vs server).
        MountEpochMismatch,
        /// Route epoch mismatch.
        RouteEpochMismatch,
        /// Request targeted a metadata group this server does not serve.
        GroupMismatch,
        /// Worker must run startup registration for the target group.
        NeedRegister,
        /// Worker process-run identity no longer matches metadata live state.
        WorkerRunMismatch,
        /// Worker must send a new full block report before deltas continue.
        FullReportRequired,
        /// Metadata or worker cannot provide a usable location for a visible block.
        BlockLocationUnavailable,
        /// Block stamp mismatch (data-plane).
        BlockStampMismatch,
        /// Fencing / lease fenced.
        Fencing,
        /// Generic epoch mismatch not further classified.
        EpochMismatch,
        /// Write session is invalid and must be reopened.
        SessionInvalid,
        /// Write session lease/session has expired and must be reopened.
        SessionExpired,
    }

    /// Worker endpoint hint used in canonical refresh hints.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WorkerEndpointHint {
        pub worker_id: u64,
        pub endpoint: String,
        pub worker_net_protocol: i32,
    }

    /// Structured refresh hints attached to canonical errors.
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

    /// Error code namespace – either a filesystem errno or an RPC‑level code.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum ErrorCode {
        FsErrno(FsErrorCode),
        RpcCode(RpcErrorCode),
    }

    /// Canonical error model for vecton.
    ///
    /// Invariants (enforced via `debug_assert!` and constructor helpers):
    ///
    /// 1. `class == ErrorClass::Ok`  => `code/reason/retry_after_ms` must be `None`.
    /// 2. `class == ErrorClass::NeedRefresh` => `reason.is_some()`; `code`
    ///    SHOULD be `Some(ErrorCode::RpcCode(_))`.
    /// 3. `class == ErrorClass::Retryable`   => `retry_after_ms` is optional;
    ///    `code` SHOULD be `Some(ErrorCode::RpcCode(_))`.
    /// 4. FS errno errors default to `class == Fatal` unless explicitly marked retriable/refreshable.
    #[derive(Clone, Debug)]
    pub struct CanonicalError {
        pub class: ErrorClass,
        pub code: Option<ErrorCode>,
        pub reason: Option<RefreshReason>,
        pub retry_after_ms: Option<u64>,
        pub message: String,
        pub refresh_hint: Option<RefreshHint>,
    }

    impl CanonicalError {
        /// Construct an `Ok` error (mainly for completeness / tests).
        pub fn ok(message: impl Into<String>) -> Self {
            let err = Self {
                class: ErrorClass::Ok,
                code: None,
                reason: None,
                retry_after_ms: None,
                message: message.into(),
                refresh_hint: None,
            };
            err.debug_validate();
            err
        }

        /// Construct a fatal FS errno error.
        pub fn fatal_fs(errno: FsErrorCode, message: impl Into<String>) -> Self {
            let err = Self {
                class: ErrorClass::Fatal,
                code: Some(ErrorCode::FsErrno(errno)),
                reason: None,
                retry_after_ms: None,
                message: message.into(),
                refresh_hint: None,
            };
            err.debug_validate();
            err
        }

        /// Construct a NEED_REFRESH error from an RPC code + refresh reason.
        pub fn need_refresh(code: RpcErrorCode, reason: RefreshReason, message: impl Into<String>) -> Self {
            let err = Self {
                class: ErrorClass::NeedRefresh,
                code: Some(ErrorCode::RpcCode(code)),
                reason: Some(reason),
                retry_after_ms: None,
                message: message.into(),
                refresh_hint: None,
            };
            err.debug_validate();
            err
        }

        /// Construct a NEED_REFRESH error from an RPC code + refresh reason + structured hint.
        pub fn need_refresh_with_hint(
            code: RpcErrorCode,
            reason: RefreshReason,
            hint: RefreshHint,
            message: impl Into<String>,
        ) -> Self {
            let err = Self {
                class: ErrorClass::NeedRefresh,
                code: Some(ErrorCode::RpcCode(code)),
                reason: Some(reason),
                retry_after_ms: None,
                message: message.into(),
                refresh_hint: Some(hint),
            };
            err.debug_validate();
            err
        }

        /// Construct a RETRYABLE error from an RPC code and optional backoff.
        pub fn retryable(code: RpcErrorCode, retry_after_ms: Option<u64>, message: impl Into<String>) -> Self {
            let err = Self {
                class: ErrorClass::Retryable,
                code: Some(ErrorCode::RpcCode(code)),
                reason: None,
                retry_after_ms,
                message: message.into(),
                refresh_hint: None,
            };
            err.debug_validate();
            err
        }

        /// Attach structured refresh hint to an existing canonical error.
        pub fn with_refresh_hint(mut self, hint: RefreshHint) -> Self {
            self.refresh_hint = Some(hint);
            self
        }

        /// Best-effort invariant checks; only compiled in debug/profile builds.
        fn debug_validate(&self) {
            match self.class {
                ErrorClass::Ok => {
                    debug_assert!(
                        self.code.is_none() && self.reason.is_none() && self.retry_after_ms.is_none(),
                        "CanonicalError invariant violated: Ok must not carry code/reason/retry_after_ms: {:?}",
                        self
                    );
                }
                ErrorClass::NeedRefresh => {
                    debug_assert!(
                        self.reason.is_some(),
                        "CanonicalError invariant violated: NeedRefresh must have reason: {:?}",
                        self
                    );
                }
                ErrorClass::Retryable => {
                    // No strict invariant beyond class at the moment.
                }
                ErrorClass::Fatal => {
                    // Fs errno defaults to Fatal by convention; no strict rule.
                }
            }
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
#[path = "canonical_tests.rs"]
mod canonical_tests;

#[cfg(test)]
mod tests {
    use super::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalCode, RefreshReason};
    use types::fs::FsErrorCode;

    #[test]
    fn canonical_ok_invariant() {
        let err = CanonicalError::ok("ok");
        assert_eq!(err.class, ErrorClass::Ok);
        assert!(err.code.is_none());
        assert!(err.reason.is_none());
        assert!(err.retry_after_ms.is_none());
    }

    #[test]
    fn canonical_need_refresh_has_reason() {
        let err = CanonicalError::need_refresh(
            crate::header::RpcErrorCode::NotLeader,
            RefreshReason::NotLeader,
            "leader changed",
        );
        assert_eq!(err.class, ErrorClass::NeedRefresh);
        assert!(err.reason.is_some());
    }

    #[test]
    fn canonical_fatal_fs_sets_errno() {
        let err = CanonicalError::fatal_fs(FsErrorCode::ENoEnt, "no such file");
        assert_eq!(err.class, ErrorClass::Fatal);
        match err.code.unwrap() {
            CanonicalCode::FsErrno(code) => assert_eq!(code, FsErrorCode::ENoEnt),
            _ => panic!("expected FsErrno"),
        }
    }
}
