// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

#[cfg(test)]
mod tests {
    use super::super::rpc::{
        ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction, RefreshHint,
        RpcErrorDetail, WorkerErrorKind,
    };
    use beryl_types::fs::FsErrorCode;

    #[test]
    fn fs_error_records_fact_without_recovery_coupling() {
        let err = RpcErrorDetail::fs(FsErrorCode::ENoEnt, "missing inode");

        assert_eq!(err.kind, ErrorKind::Fs(FsErrorCode::ENoEnt));
        assert_eq!(err.recovery, RecoveryAction::Fail);
        assert_eq!(err.message, "missing inode");
    }

    #[test]
    fn same_kind_can_use_different_recovery_actions() {
        let hint = RefreshHint {
            route_epoch: Some(7),
            ..RefreshHint::default()
        };
        let kind = ErrorKind::Worker(WorkerErrorKind::RunMismatch);
        let refresh = RpcErrorDetail::refresh_metadata(kind, hint.clone(), "stale worker");
        let reopen = RpcErrorDetail::reopen_write_session(kind, hint, "writer fenced");

        assert_eq!(refresh.kind, kind);
        assert!(matches!(refresh.recovery, RecoveryAction::RefreshMetadata { .. }));
        assert_eq!(reopen.kind, kind);
        assert!(matches!(reopen.recovery, RecoveryAction::ReopenWriteSession { .. }));
    }

    #[test]
    fn worker_control_recovery_is_explicit() {
        let register =
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::NotRegistered), "register first");
        let full_report = RpcErrorDetail::send_full_block_report(
            ErrorKind::Worker(WorkerErrorKind::FullReportRequired),
            "full report required",
        );

        assert_eq!(register.recovery, RecoveryAction::RegisterWorker);
        assert_eq!(full_report.recovery, RecoveryAction::SendFullBlockReport);
    }

    #[test]
    fn retry_keeps_backoff_as_recovery_data() {
        let err = RpcErrorDetail::retry(
            ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
            Some(250),
            "metadata unavailable",
        );

        assert_eq!(err.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
        assert_eq!(err.recovery, RecoveryAction::Retry { after_ms: Some(250) });
    }

    #[test]
    fn error_kind_is_domain_layered() {
        let metadata = ErrorKind::Metadata(MetadataErrorKind::NotLeader);
        let worker = ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch);
        let protocol = ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument);

        assert_ne!(metadata, worker);
        assert_ne!(worker, protocol);
    }
}
