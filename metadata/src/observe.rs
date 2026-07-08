// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata-owned metrics emitted through the shared recorder.

use crate::error::MetadataError;
use common::error::rpc::{
    ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, RpcErrorDetail, UfsErrorKind, WorkerErrorKind,
};
use types::fs::FsErrorCode;

pub(crate) const METADATA_UP: &str = "metadata_up";
pub(crate) const METADATA_BUILD_INFO: &str = "metadata_build_info";
pub(crate) const METADATA_ROOT_READY: &str = "metadata_root_ready";
pub(crate) const METADATA_RAFT_ROLE: &str = "metadata_raft_role";
pub(crate) const METADATA_RAFT_TERM: &str = "metadata_raft_term";
pub(crate) const METADATA_RAFT_LAST_APPLIED_INDEX: &str = "metadata_raft_last_applied_index";
pub(crate) const METADATA_RAFT_COMMITTED_INDEX: &str = "metadata_raft_committed_index";
pub(crate) const METADATA_RAFT_PROPOSALS_TOTAL: &str = "metadata_raft_proposals_total";
pub(crate) const METADATA_RAFT_PROPOSE_DURATION_SECONDS: &str = "metadata_raft_propose_duration_seconds";
pub(crate) const METADATA_RAFT_APPLY_TOTAL: &str = "metadata_raft_apply_total";
pub(crate) const METADATA_RAFT_APPLY_DURATION_SECONDS: &str = "metadata_raft_apply_duration_seconds";
pub(crate) const METADATA_RPC_REQUESTS_TOTAL: &str = "metadata_rpc_requests_total";
pub(crate) const METADATA_RPC_REQUEST_DURATION_SECONDS: &str = "metadata_rpc_request_duration_seconds";
pub(crate) const METADATA_FS_OPS_TOTAL: &str = "metadata_fs_ops_total";
pub(crate) const METADATA_FS_OP_DURATION_SECONDS: &str = "metadata_fs_op_duration_seconds";
pub(crate) const METADATA_WORKER_LIVE: &str = "metadata_worker_live";
pub(crate) const METADATA_WORKER_REGISTERED_TOTAL: &str = "metadata_worker_registered_total";
pub(crate) const METADATA_WORKER_REGISTRATION_DURATION_SECONDS: &str = "metadata_worker_registration_duration_seconds";
pub(crate) const METADATA_WORKER_HEARTBEAT_TOTAL: &str = "metadata_worker_heartbeat_total";
pub(crate) const METADATA_WORKER_HEARTBEAT_DURATION_SECONDS: &str = "metadata_worker_heartbeat_duration_seconds";
pub(crate) const METADATA_WORKER_HEARTBEAT_LAG_SECONDS: &str = "metadata_worker_heartbeat_lag_seconds";
pub(crate) const METADATA_WORKER_BLOCK_REPORT_TOTAL: &str = "metadata_worker_block_report_total";
pub(crate) const METADATA_WORKER_BLOCK_REPORT_DURATION_SECONDS: &str = "metadata_worker_block_report_duration_seconds";
pub(crate) const METADATA_WORKER_BLOCK_REPORT_BLOCKS_TOTAL: &str = "metadata_worker_block_report_blocks_total";
pub(crate) const METADATA_MAINTENANCE_ORPHAN_QUEUE_DEPTH: &str = "metadata_maintenance_orphan_queue_depth";
pub(crate) const METADATA_MAINTENANCE_ORPHAN_CLEANUP_TOTAL: &str = "metadata_maintenance_orphan_cleanup_total";
pub(crate) const METADATA_DELETE_QUEUE_DEPTH: &str = "metadata_delete_queue_depth";
pub(crate) const METADATA_DELETE_TASKS_TOTAL: &str = "metadata_delete_tasks_total";
pub(crate) const METADATA_REPAIR_QUEUE_DEPTH: &str = "metadata_repair_queue_depth";
pub(crate) const METADATA_REPAIR_ATTEMPTS_TOTAL: &str = "metadata_repair_attempts_total";

pub(crate) fn record_metadata_started(service: &str, version: &str) {
    metrics::gauge!(METADATA_UP).set(1.0);
    metrics::gauge!(
        METADATA_BUILD_INFO,
        "service" => service.to_string(),
        "version" => version.to_string()
    )
    .set(1.0);
}

pub(crate) fn record_root_ready(ready: bool) {
    metrics::gauge!(METADATA_ROOT_READY).set(if ready { 1.0 } else { 0.0 });
}

pub(crate) fn record_raft_role(role: &str) {
    for known_role in ["leader", "follower", "candidate", "learner", "shutdown", "unknown"] {
        metrics::gauge!(METADATA_RAFT_ROLE, "role" => known_role).set(if known_role == role { 1.0 } else { 0.0 });
    }
}

pub(crate) fn record_raft_term(term: u64) {
    metrics::gauge!(METADATA_RAFT_TERM).set(term as f64);
}

pub(crate) fn record_raft_indexes(last_applied: Option<u64>, committed: Option<u64>) {
    if let Some(last_applied) = last_applied {
        metrics::gauge!(METADATA_RAFT_LAST_APPLIED_INDEX).set(last_applied as f64);
    }
    if let Some(committed) = committed {
        metrics::gauge!(METADATA_RAFT_COMMITTED_INDEX).set(committed as f64);
    }
}

pub(crate) fn record_raft_proposal(status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_RAFT_PROPOSALS_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_RAFT_PROPOSE_DURATION_SECONDS,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_raft_apply(status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_RAFT_APPLY_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_RAFT_APPLY_DURATION_SECONDS,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_rpc_request(service: &str, method: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_RPC_REQUESTS_TOTAL,
        "service" => service.to_string(),
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_RPC_REQUEST_DURATION_SECONDS,
        "service" => service.to_string(),
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_fs_op(operation: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_FS_OPS_TOTAL,
        "operation" => operation.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_FS_OP_DURATION_SECONDS,
        "operation" => operation.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn set_worker_live(count: usize) {
    metrics::gauge!(METADATA_WORKER_LIVE).set(count as f64);
}

pub(crate) fn record_worker_registration(status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_WORKER_REGISTERED_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_WORKER_REGISTRATION_DURATION_SECONDS,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_worker_heartbeat(status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_WORKER_HEARTBEAT_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_WORKER_HEARTBEAT_DURATION_SECONDS,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_worker_heartbeat_lag(lag_seconds: f64) {
    metrics::histogram!(METADATA_WORKER_HEARTBEAT_LAG_SECONDS).record(lag_seconds);
}

pub(crate) fn record_worker_block_report(kind: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_WORKER_BLOCK_REPORT_TOTAL,
        "kind" => kind.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_WORKER_BLOCK_REPORT_DURATION_SECONDS,
        "kind" => kind.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_worker_block_report_blocks(change: &str, count: usize) {
    if count == 0 {
        return;
    }
    metrics::counter!(METADATA_WORKER_BLOCK_REPORT_BLOCKS_TOTAL, "change" => change.to_string())
        .increment(count as u64);
}

pub(crate) fn set_orphan_queue_depth(depth: usize) {
    metrics::gauge!(METADATA_MAINTENANCE_ORPHAN_QUEUE_DEPTH).set(depth as f64);
}

pub(crate) fn record_orphan_cleanup(status: &str, reason: &str) {
    metrics::counter!(
        METADATA_MAINTENANCE_ORPHAN_CLEANUP_TOTAL,
        "status" => status.to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
}

pub(crate) fn set_delete_queue_depth(depth: usize) {
    metrics::gauge!(METADATA_DELETE_QUEUE_DEPTH).set(depth as f64);
}

pub(crate) fn record_delete_task(status: &str, error_kind: &str) {
    metrics::counter!(
        METADATA_DELETE_TASKS_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
}

pub(crate) fn set_repair_queue_depth(depth: usize) {
    metrics::gauge!(METADATA_REPAIR_QUEUE_DEPTH).set(depth as f64);
}

pub(crate) fn record_repair_attempt(status: &str, error_kind: &str) {
    metrics::counter!(
        METADATA_REPAIR_ATTEMPTS_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
}

pub(crate) fn metadata_error_kind(error: &MetadataError) -> &'static str {
    match error {
        MetadataError::NotFound(_) => "not_found",
        MetadataError::AlreadyExists(_) => "already_exists",
        MetadataError::InvalidArgument(_) => "invalid_argument",
        MetadataError::NotDir(_) => "not_dir",
        MetadataError::IsDir(_) => "is_dir",
        MetadataError::DirectoryNotEmpty(_) => "directory_not_empty",
        MetadataError::CrossMountRename(_) => "cross_mount_rename",
        MetadataError::PermissionDenied(_) => "permission_denied",
        MetadataError::NotSupported(_) => "not_supported",
        MetadataError::Busy(_) => "busy",
        MetadataError::ActiveWorkerConflict(_) => "active_worker_conflict",
        MetadataError::Again(_) => "again",
        MetadataError::LeaseFenced { .. } => "lease_fenced",
        MetadataError::LeaderChanged(_) => "not_leader",
        MetadataError::EpochMismatch { .. } => "epoch_mismatch",
        MetadataError::MountEpochMismatch { .. } => "mount_epoch_mismatch",
        MetadataError::RoutingStale(_) => "route_epoch_mismatch",
        MetadataError::StaleState(_) => "stale_state",
        MetadataError::FullReportRequired(_) => "full_report_required",
        MetadataError::Internal(_) => "internal",
        MetadataError::ServiceUnavailable(_) => "unavailable",
    }
}

pub(crate) fn rpc_error_kind(error: &RpcErrorDetail) -> &'static str {
    error_kind_label(error.kind)
}

pub(crate) fn fs_errno_kind(errno: FsErrorCode) -> &'static str {
    match errno {
        FsErrorCode::Ok => "none",
        FsErrorCode::ENoEnt => "enoent",
        FsErrorCode::EExist => "eexist",
        FsErrorCode::ENotEmpty => "enotempty",
        FsErrorCode::ENotDir => "enotdir",
        FsErrorCode::EIsDir => "eisdir",
        FsErrorCode::EXDev => "exdev",
        FsErrorCode::EPerm => "eperm",
        FsErrorCode::EAcces => "eacces",
        FsErrorCode::EInval => "einval",
        FsErrorCode::ENotsup => "enotsup",
        FsErrorCode::ENotImpl => "enotimpl",
        FsErrorCode::EAgain => "eagain",
        FsErrorCode::EBusy => "ebusy",
    }
}

fn error_kind_label(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Fs(errno) => fs_errno_kind(errno),
        ErrorKind::Ufs(kind) => ufs_error_kind(kind),
        ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader) => "invalid_header",
        ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument) => "invalid_argument",
        ErrorKind::Protocol(ProtocolErrorKind::PermissionDenied) => "permission_denied",
        ErrorKind::Protocol(ProtocolErrorKind::Unsupported) => "unsupported",
        ErrorKind::Metadata(MetadataErrorKind::NotFound) => "not_found",
        ErrorKind::Metadata(MetadataErrorKind::AlreadyExists) => "already_exists",
        ErrorKind::Metadata(MetadataErrorKind::NotDirectory) => "not_directory",
        ErrorKind::Metadata(MetadataErrorKind::IsDirectory) => "is_directory",
        ErrorKind::Metadata(MetadataErrorKind::DirectoryNotEmpty) => "directory_not_empty",
        ErrorKind::Metadata(MetadataErrorKind::CrossMountRename) => "cross_mount_rename",
        ErrorKind::Metadata(MetadataErrorKind::Busy) => "busy",
        ErrorKind::Metadata(MetadataErrorKind::Conflict) => "conflict",
        ErrorKind::Metadata(MetadataErrorKind::NotLeader) => "not_leader",
        ErrorKind::Metadata(MetadataErrorKind::StaleState) => "stale_state",
        ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch) => "mount_epoch_mismatch",
        ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch) => "route_epoch_mismatch",
        ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch) => "owner_group_mismatch",
        ErrorKind::Metadata(MetadataErrorKind::GroupMismatch) => "group_mismatch",
        ErrorKind::Worker(WorkerErrorKind::NotRegistered) => "worker_not_registered",
        ErrorKind::Worker(WorkerErrorKind::RunMismatch) => "worker_run_mismatch",
        ErrorKind::Worker(WorkerErrorKind::DescriptorMismatch) => "worker_descriptor_mismatch",
        ErrorKind::Worker(WorkerErrorKind::FullReportRequired) => "full_report_required",
        ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable) => "block_location_unavailable",
        ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch) => "block_stamp_mismatch",
        ErrorKind::Metadata(MetadataErrorKind::Fencing) => "fencing",
        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid) => "session_invalid",
        ErrorKind::Metadata(MetadataErrorKind::SessionExpired) => "session_expired",
        ErrorKind::Metadata(MetadataErrorKind::EpochMismatch) => "epoch_mismatch",
        ErrorKind::Internal(InternalErrorKind::NodeUnavailable) => "node_unavailable",
        ErrorKind::Internal(InternalErrorKind::Timeout) => "timeout",
        ErrorKind::Internal(InternalErrorKind::ResourceExhausted) => "resource_exhausted",
        ErrorKind::Internal(InternalErrorKind::Cancelled) => "cancelled",
        ErrorKind::Internal(InternalErrorKind::Corrupt) => "corrupt",
        ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted) => "resource_exhausted",
        ErrorKind::Worker(kind) => worker_error_kind(kind),
        ErrorKind::Protocol(kind) => protocol_error_kind(kind),
        ErrorKind::Internal(_) => "internal",
    }
}

fn worker_error_kind(kind: WorkerErrorKind) -> &'static str {
    match kind {
        WorkerErrorKind::NotRegistered => "worker_not_registered",
        WorkerErrorKind::RunMismatch => "worker_run_mismatch",
        WorkerErrorKind::DescriptorMismatch => "worker_descriptor_mismatch",
        WorkerErrorKind::FullReportRequired => "full_report_required",
        WorkerErrorKind::BlockLocationUnavailable => "block_location_unavailable",
        WorkerErrorKind::BlockStampMismatch => "block_stamp_mismatch",
        WorkerErrorKind::NodeUnavailable => "worker_node_unavailable",
        WorkerErrorKind::Timeout => "worker_timeout",
        WorkerErrorKind::ResourceExhausted => "worker_resource_exhausted",
        WorkerErrorKind::Conflict => "worker_conflict",
        WorkerErrorKind::Corrupt => "worker_corrupt",
        WorkerErrorKind::Fencing => "worker_fencing",
        WorkerErrorKind::Cancelled => "worker_cancelled",
        WorkerErrorKind::Io => "worker_io",
    }
}

fn protocol_error_kind(kind: ProtocolErrorKind) -> &'static str {
    match kind {
        ProtocolErrorKind::InvalidHeader => "invalid_header",
        ProtocolErrorKind::InvalidArgument => "invalid_argument",
        ProtocolErrorKind::PermissionDenied => "permission_denied",
        ProtocolErrorKind::Unsupported => "unsupported",
        ProtocolErrorKind::Cancelled => "cancelled",
        ProtocolErrorKind::Corrupt => "corrupt",
    }
}

fn ufs_error_kind(kind: UfsErrorKind) -> &'static str {
    match kind {
        UfsErrorKind::NotFound => "ufs_not_found",
        UfsErrorKind::PermissionDenied => "ufs_permission_denied",
        UfsErrorKind::Unsupported => "ufs_unsupported",
        UfsErrorKind::NotImplemented => "ufs_not_implemented",
        UfsErrorKind::InvalidSpec => "ufs_invalid_spec",
        UfsErrorKind::InvalidPath => "ufs_invalid_path",
        UfsErrorKind::UnexpectedEof => "ufs_unexpected_eof",
        UfsErrorKind::Backend => "ufs_backend",
        UfsErrorKind::Overloaded => "ufs_overloaded",
        UfsErrorKind::Timeout => "ufs_timeout",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata_metric_contract_names() -> [&'static str; 30] {
        [
            METADATA_UP,
            METADATA_BUILD_INFO,
            METADATA_ROOT_READY,
            METADATA_RAFT_ROLE,
            METADATA_RAFT_TERM,
            METADATA_RAFT_LAST_APPLIED_INDEX,
            METADATA_RAFT_COMMITTED_INDEX,
            METADATA_RAFT_PROPOSALS_TOTAL,
            METADATA_RAFT_PROPOSE_DURATION_SECONDS,
            METADATA_RAFT_APPLY_TOTAL,
            METADATA_RAFT_APPLY_DURATION_SECONDS,
            METADATA_RPC_REQUESTS_TOTAL,
            METADATA_RPC_REQUEST_DURATION_SECONDS,
            METADATA_FS_OPS_TOTAL,
            METADATA_FS_OP_DURATION_SECONDS,
            METADATA_WORKER_LIVE,
            METADATA_WORKER_REGISTERED_TOTAL,
            METADATA_WORKER_REGISTRATION_DURATION_SECONDS,
            METADATA_WORKER_HEARTBEAT_TOTAL,
            METADATA_WORKER_HEARTBEAT_DURATION_SECONDS,
            METADATA_WORKER_HEARTBEAT_LAG_SECONDS,
            METADATA_WORKER_BLOCK_REPORT_TOTAL,
            METADATA_WORKER_BLOCK_REPORT_DURATION_SECONDS,
            METADATA_WORKER_BLOCK_REPORT_BLOCKS_TOTAL,
            METADATA_MAINTENANCE_ORPHAN_QUEUE_DEPTH,
            METADATA_MAINTENANCE_ORPHAN_CLEANUP_TOTAL,
            METADATA_DELETE_QUEUE_DEPTH,
            METADATA_DELETE_TASKS_TOTAL,
            METADATA_REPAIR_QUEUE_DEPTH,
            METADATA_REPAIR_ATTEMPTS_TOTAL,
        ]
    }

    fn metadata_metric_label_contract_names() -> [&'static str; 10] {
        [
            "service",
            "version",
            "role",
            "status",
            "error_kind",
            "method",
            "operation",
            "kind",
            "change",
            "reason",
        ]
    }

    #[test]
    fn metric_names_match_observability_contract() {
        let names = metadata_metric_contract_names();
        let expected = [
            "metadata_up",
            "metadata_build_info",
            "metadata_root_ready",
            "metadata_raft_role",
            "metadata_raft_term",
            "metadata_raft_last_applied_index",
            "metadata_raft_committed_index",
            "metadata_raft_proposals_total",
            "metadata_raft_propose_duration_seconds",
            "metadata_raft_apply_total",
            "metadata_raft_apply_duration_seconds",
            "metadata_rpc_requests_total",
            "metadata_rpc_request_duration_seconds",
            "metadata_fs_ops_total",
            "metadata_fs_op_duration_seconds",
            "metadata_worker_live",
            "metadata_worker_registered_total",
            "metadata_worker_registration_duration_seconds",
            "metadata_worker_heartbeat_total",
            "metadata_worker_heartbeat_duration_seconds",
            "metadata_worker_heartbeat_lag_seconds",
            "metadata_worker_block_report_total",
            "metadata_worker_block_report_duration_seconds",
            "metadata_worker_block_report_blocks_total",
            "metadata_maintenance_orphan_queue_depth",
            "metadata_maintenance_orphan_cleanup_total",
            "metadata_delete_queue_depth",
            "metadata_delete_tasks_total",
            "metadata_repair_queue_depth",
            "metadata_repair_attempts_total",
        ];

        assert_eq!(names, expected);
        assert!(names.iter().all(|name| !name.starts_with(concat!("vecton", "_"))));
    }

    #[test]
    fn metric_label_names_are_low_cardinality() {
        let forbidden = [
            "path",
            "full_storage_dir",
            "block_id",
            "stream_id",
            concat!("request", "_", "id"),
            concat!("trace", "_", "id"),
            "span_id",
            "worker_run_id",
            "client_id",
            "user_id",
            "token",
            "secret",
            "authorization",
            "cookie",
            "credential",
            "error",
        ];

        for label in metadata_metric_label_contract_names() {
            assert!(!forbidden.contains(&label), "forbidden high-cardinality label {label}");
        }
    }

    #[test]
    fn gauge_metric_names_do_not_end_in_total() {
        for name in [
            METADATA_UP,
            METADATA_BUILD_INFO,
            METADATA_ROOT_READY,
            METADATA_RAFT_ROLE,
            METADATA_RAFT_TERM,
            METADATA_RAFT_LAST_APPLIED_INDEX,
            METADATA_RAFT_COMMITTED_INDEX,
            METADATA_WORKER_LIVE,
            METADATA_WORKER_HEARTBEAT_LAG_SECONDS,
            METADATA_MAINTENANCE_ORPHAN_QUEUE_DEPTH,
            METADATA_DELETE_QUEUE_DEPTH,
            METADATA_REPAIR_QUEUE_DEPTH,
        ] {
            assert!(
                !name.ends_with("_total"),
                "gauge metric {name} must not use _total suffix"
            );
        }
    }

    #[test]
    fn observe_helpers_emit_without_installed_recorder() {
        record_metadata_started("metadata", "0.0.0-test");
        record_root_ready(false);
        record_root_ready(true);
        record_raft_role("leader");
        record_raft_term(2);
        record_raft_indexes(Some(3), Some(4));
        record_raft_proposal("ok", "none", 0.001);
        record_raft_apply("ok", "none", 0.002);
        record_rpc_request("metadata_worker", "register_worker", "ok", "none", 0.003);
        record_fs_op("create_file", "ok", "none", 0.004);
        set_worker_live(1);
        record_worker_registration("ok", "none", 0.005);
        record_worker_heartbeat("ok", "none", 0.006);
        record_worker_heartbeat_lag(0.0);
        record_worker_block_report("full", "ok", "none", 0.007);
        record_worker_block_report_blocks("added", 1);
        set_orphan_queue_depth(0);
        record_orphan_cleanup("ok", "empty");
        set_delete_queue_depth(0);
        record_delete_task("ok", "none");
        set_repair_queue_depth(0);
        record_repair_attempt("ok", "none");
    }
}
