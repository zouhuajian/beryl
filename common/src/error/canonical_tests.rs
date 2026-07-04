// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Table-driven tests for canonical error model invariants.
//!
//! These tests verify the invariants of CanonicalError:
//! - Ok must not carry error fields
//! - NeedRefresh must have reason
//! - Retryable's retry_after_ms is carried as a server hint
//! - Code oneof is mutually exclusive

#[cfg(test)]
mod tests {
    use super::super::canonical::{CanonicalError, ErrorClass, ErrorCode, RefreshReason};
    use crate::header::RpcErrorCode;
    use types::fs::FsErrorCode;

    // ============================================================================
    // Invariant Tests
    // ============================================================================

    #[test]
    fn test_invariant_ok_no_error_fields() {
        let err = CanonicalError::ok("success");
        assert_eq!(err.class, ErrorClass::Ok);
        assert!(err.code.is_none());
        assert!(err.reason.is_none());
        assert!(err.retry_after_ms.is_none());
    }

    #[test]
    fn test_invariant_need_refresh_has_reason() {
        let err = CanonicalError::need_refresh(RpcErrorCode::NotLeader, RefreshReason::NotLeader, "not leader");
        assert_eq!(err.class, ErrorClass::NeedRefresh);
        assert!(err.reason.is_some());
        assert_eq!(err.reason.unwrap(), RefreshReason::NotLeader);
    }

    #[test]
    fn test_invariant_code_oneof_mutually_exclusive_fs() {
        // FS errno errors should have FsErrno code, not RpcCode
        let err = CanonicalError::fatal_fs(FsErrorCode::ENoEnt, "not found");
        assert_eq!(err.class, ErrorClass::Fatal);
        assert!(matches!(err.code, Some(ErrorCode::FsErrno(FsErrorCode::ENoEnt))));
        assert!(!matches!(err.code, Some(ErrorCode::RpcCode(_))));
    }

    #[test]
    fn test_invariant_code_oneof_mutually_exclusive_rpc() {
        // RPC errors should have RpcCode, not FsErrno
        let err = CanonicalError::retryable(RpcErrorCode::NodeUnavailable, None, "unavailable");
        assert_eq!(err.class, ErrorClass::Retryable);
        assert!(matches!(
            err.code,
            Some(ErrorCode::RpcCode(RpcErrorCode::NodeUnavailable))
        ));
        assert!(!matches!(err.code, Some(ErrorCode::FsErrno(_))));
    }

    #[test]
    fn test_invariant_retryable_retry_after_ms_source() {
        // retry_after_ms should only be carried as CanonicalError.retry_after_ms
        let err1 = CanonicalError::retryable(RpcErrorCode::NodeUnavailable, Some(2000), "unavailable");
        assert_eq!(err1.retry_after_ms, Some(2000));

        let err2 = CanonicalError::retryable(RpcErrorCode::NodeUnavailable, None, "unavailable");
        assert_eq!(err2.retry_after_ms, None);

        let err3 = CanonicalError::retryable(RpcErrorCode::NodeUnavailable, Some(0), "unavailable");
        assert_eq!(err3.retry_after_ms, Some(0));
    }

    #[test]
    fn test_invariant_fatal_fs_sets_errno() {
        let err = CanonicalError::fatal_fs(FsErrorCode::ENoEnt, "not found");
        assert_eq!(err.class, ErrorClass::Fatal);
        assert!(matches!(err.code, Some(ErrorCode::FsErrno(FsErrorCode::ENoEnt))));
        assert!(err.reason.is_none());
        assert!(err.retry_after_ms.is_none());
    }

    #[test]
    fn test_invariant_need_refresh_all_reasons() {
        // Test all refresh reasons are properly set
        let reasons = vec![
            RefreshReason::NotLeader,
            RefreshReason::OwnerGroupMismatch,
            RefreshReason::Moved,
            RefreshReason::StaleState,
            RefreshReason::MountEpochMismatch,
            RefreshReason::RouteEpochMismatch,
            RefreshReason::BlockLocationUnavailable,
            RefreshReason::BlockStampMismatch,
            RefreshReason::Fencing,
            RefreshReason::EpochMismatch,
        ];

        for reason in reasons {
            let err = CanonicalError::need_refresh(RpcErrorCode::Application, reason, "test");
            assert_eq!(err.class, ErrorClass::NeedRefresh);
            assert_eq!(err.reason, Some(reason));
        }
    }

    #[test]
    fn test_invariant_retryable_optional_retry_after() {
        // Retryable errors can carry retry_after_ms as a hint or omit it
        let err_with = CanonicalError::retryable(RpcErrorCode::NodeUnavailable, Some(1000), "test");
        assert_eq!(err_with.retry_after_ms, Some(1000));

        let err_without = CanonicalError::retryable(RpcErrorCode::NodeUnavailable, None, "test");
        assert_eq!(err_without.retry_after_ms, None);
    }

    #[test]
    fn test_invariant_fatal_fs_errno_coverage() {
        // Test various FS errno codes
        let errnos = vec![
            FsErrorCode::ENoEnt,
            FsErrorCode::EExist,
            FsErrorCode::ENotEmpty,
            FsErrorCode::EXDev,
            FsErrorCode::EPerm,
            FsErrorCode::EInval,
        ];

        for errno in errnos {
            let err = CanonicalError::fatal_fs(errno, "test");
            assert_eq!(err.class, ErrorClass::Fatal);
            assert!(matches!(err.code, Some(ErrorCode::FsErrno(_))));
        }
    }
}
