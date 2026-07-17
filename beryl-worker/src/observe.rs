// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-owned metrics emitted through the shared recorder.

use crate::error::WorkerError;
use crate::store::dirs::StoreReport;
use beryl_common::error::rpc::{ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, WorkerErrorKind};

pub(crate) const WORKER_UP: &str = "worker_up";
pub(crate) const WORKER_BUILD_INFO: &str = "worker_build_info";
pub(crate) const WORKER_REGISTERED: &str = "worker_registered";
pub(crate) const WORKER_METADATA_RPC_TOTAL: &str = "worker_metadata_rpc_total";
pub(crate) const WORKER_METADATA_RPC_DURATION_SECONDS: &str = "worker_metadata_rpc_duration_seconds";
pub(crate) const WORKER_HEARTBEAT_SENT_TOTAL: &str = "worker_heartbeat_sent_total";
pub(crate) const WORKER_BLOCK_REPORT_SENT_TOTAL: &str = "worker_block_report_sent_total";
pub(crate) const WORKER_BLOCK_REPORT_DURATION_SECONDS: &str = "worker_block_report_duration_seconds";
pub(crate) const WORKER_STORE_CAPACITY_BYTES: &str = "worker_store_capacity_bytes";
pub(crate) const WORKER_STORE_WRITABLE: &str = "worker_store_writable";
pub(crate) const WORKER_STORE_BLOCKS: &str = "worker_store_blocks";
pub(crate) const WORKER_STORE_IO_BYTES: &str = "worker_store_io_bytes";
pub(crate) const WORKER_STORE_IO_DURATION_SECONDS: &str = "worker_store_io_duration_seconds";
pub(crate) const WORKER_DATA_RPC_TOTAL: &str = "worker_data_rpc_total";
pub(crate) const WORKER_DATA_RPC_DURATION_SECONDS: &str = "worker_data_rpc_duration_seconds";
pub(crate) const WORKER_STREAM_OPEN_TOTAL: &str = "worker_stream_open_total";
pub(crate) const WORKER_STREAM_INFLIGHT: &str = "worker_stream_inflight";
pub(crate) const WORKER_STREAM_FRAME_BYTES: &str = "worker_stream_frame_bytes";
pub(crate) const WORKER_STREAM_FRAMES_TOTAL: &str = "worker_stream_frames_total";
pub(crate) const WORKER_STREAM_COMMIT_TOTAL: &str = "worker_stream_commit_total";
pub(crate) const WORKER_STREAM_ABORT_TOTAL: &str = "worker_stream_abort_total";

pub fn record_worker_started(service: &str, version: &str) {
    metrics::gauge!(WORKER_UP).set(1.0);
    metrics::gauge!(
        WORKER_BUILD_INFO,
        "service" => service.to_string(),
        "version" => version.to_string()
    )
    .set(1.0);
}

pub fn set_worker_registered(registered: bool) {
    metrics::gauge!(WORKER_REGISTERED).set(if registered { 1.0 } else { 0.0 });
}

pub(crate) fn record_metadata_rpc(method: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        WORKER_METADATA_RPC_TOTAL,
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        WORKER_METADATA_RPC_DURATION_SECONDS,
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_heartbeat_sent(status: &str, error_kind: &str) {
    metrics::counter!(
        WORKER_HEARTBEAT_SENT_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
}

pub(crate) fn record_block_report_sent(kind: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        WORKER_BLOCK_REPORT_SENT_TOTAL,
        "kind" => kind.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        WORKER_BLOCK_REPORT_DURATION_SECONDS,
        "kind" => kind.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_store_capacity(dir_id: &str, kind: &str, bytes: u64) {
    metrics::gauge!(
        WORKER_STORE_CAPACITY_BYTES,
        "dir_id" => dir_id.to_string(),
        "kind" => kind.to_string()
    )
    .set(bytes as f64);
}

pub(crate) fn record_store_writable(dir_id: &str, writable: bool) {
    metrics::gauge!(WORKER_STORE_WRITABLE, "dir_id" => dir_id.to_string()).set(if writable { 1.0 } else { 0.0 });
}

pub(crate) fn record_store_blocks(dir_id: &str, count: u64) {
    metrics::gauge!(WORKER_STORE_BLOCKS, "dir_id" => dir_id.to_string()).set(count as f64);
}

pub(crate) fn record_store_report(report: &StoreReport) {
    for dir in &report.dirs {
        record_store_capacity(&dir.id, "total", dir.capacity_bytes);
        record_store_capacity(&dir.id, "used", dir.used_bytes);
        record_store_capacity(&dir.id, "available", dir.free_bytes);
        record_store_writable(&dir.id, dir.writable);
        record_store_blocks(&dir.id, dir.block_count);
    }
}

pub(crate) fn record_store_io(operation: &str, status: &str, error_kind: &str, bytes: u64, duration_seconds: f64) {
    if bytes > 0 {
        metrics::counter!(WORKER_STORE_IO_BYTES, "operation" => operation.to_string()).increment(bytes);
    }
    metrics::histogram!(
        WORKER_STORE_IO_DURATION_SECONDS,
        "operation" => operation.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_data_rpc(method: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        WORKER_DATA_RPC_TOTAL,
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        WORKER_DATA_RPC_DURATION_SECONDS,
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_stream_open(mode: &str, status: &str, error_kind: &str) {
    metrics::counter!(
        WORKER_STREAM_OPEN_TOTAL,
        "mode" => mode.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
}

pub(crate) fn increment_stream_inflight(mode: &str) {
    metrics::gauge!(WORKER_STREAM_INFLIGHT, "mode" => mode.to_string()).increment(1.0);
}

pub(crate) fn decrement_stream_inflight(mode: &str) {
    metrics::gauge!(WORKER_STREAM_INFLIGHT, "mode" => mode.to_string()).decrement(1.0);
}

pub(crate) fn record_stream_frame(mode: &str, status: &str, error_kind: &str, bytes: u64) {
    if bytes > 0 {
        metrics::counter!(WORKER_STREAM_FRAME_BYTES, "mode" => mode.to_string()).increment(bytes);
    }
    metrics::counter!(
        WORKER_STREAM_FRAMES_TOTAL,
        "mode" => mode.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
}

pub(crate) fn record_stream_commit(status: &str, error_kind: &str) {
    metrics::counter!(
        WORKER_STREAM_COMMIT_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
}

pub(crate) fn record_stream_abort(status: &str, error_kind: &str) {
    metrics::counter!(
        WORKER_STREAM_ABORT_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
}

pub(crate) fn worker_error_kind(error: &WorkerError) -> &'static str {
    match error {
        WorkerError::LeaderChanged(_) => "not_leader",
        WorkerError::Timeout(_) => "timeout",
        WorkerError::Unavailable(_) => "unavailable",
        WorkerError::ChunkConflict(_) => "chunk_conflict",
        WorkerError::DiskError(_) => "disk_error",
        WorkerError::Cancelled(_) => "cancelled",
        WorkerError::InvalidArgument(_) => "invalid_argument",
        WorkerError::NotFound(_) => "not_found",
        WorkerError::Corrupt(_) => "corrupt",
        WorkerError::RefreshMetadata { kind, .. } => error_kind_label(*kind),
        WorkerError::Fencing(_) => "fencing",
        WorkerError::PermissionDenied(_) => "permission_denied",
        WorkerError::Unimplemented(_) => "unimplemented",
        WorkerError::Internal(_) => "internal",
        WorkerError::ResourceExhausted(_) => "resource_exhausted",
    }
}

fn error_kind_label(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Fs(_) => "fs",
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
        ErrorKind::Worker(kind) => rpc_worker_error_kind(kind),
        ErrorKind::Protocol(kind) => protocol_error_kind(kind),
        ErrorKind::Internal(_) => "internal",
    }
}

fn rpc_worker_error_kind(kind: WorkerErrorKind) -> &'static str {
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use metrics::{
        Counter, CounterFn, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };

    use super::*;

    fn worker_metric_contract_names() -> [&'static str; 21] {
        [
            WORKER_UP,
            WORKER_BUILD_INFO,
            WORKER_REGISTERED,
            WORKER_METADATA_RPC_TOTAL,
            WORKER_METADATA_RPC_DURATION_SECONDS,
            WORKER_HEARTBEAT_SENT_TOTAL,
            WORKER_BLOCK_REPORT_SENT_TOTAL,
            WORKER_BLOCK_REPORT_DURATION_SECONDS,
            WORKER_STORE_CAPACITY_BYTES,
            WORKER_STORE_WRITABLE,
            WORKER_STORE_BLOCKS,
            WORKER_STORE_IO_BYTES,
            WORKER_STORE_IO_DURATION_SECONDS,
            WORKER_DATA_RPC_TOTAL,
            WORKER_DATA_RPC_DURATION_SECONDS,
            WORKER_STREAM_OPEN_TOTAL,
            WORKER_STREAM_INFLIGHT,
            WORKER_STREAM_FRAME_BYTES,
            WORKER_STREAM_FRAMES_TOTAL,
            WORKER_STREAM_COMMIT_TOTAL,
            WORKER_STREAM_ABORT_TOTAL,
        ]
    }

    fn worker_metric_label_contract_names() -> [&'static str; 9] {
        [
            "service",
            "version",
            "method",
            "status",
            "error_kind",
            "kind",
            "operation",
            "mode",
            "dir_id",
        ]
    }

    #[test]
    fn metric_names_match_observability_contract() {
        let names = worker_metric_contract_names();
        let expected = [
            "worker_up",
            "worker_build_info",
            "worker_registered",
            "worker_metadata_rpc_total",
            "worker_metadata_rpc_duration_seconds",
            "worker_heartbeat_sent_total",
            "worker_block_report_sent_total",
            "worker_block_report_duration_seconds",
            "worker_store_capacity_bytes",
            "worker_store_writable",
            "worker_store_blocks",
            "worker_store_io_bytes",
            "worker_store_io_duration_seconds",
            "worker_data_rpc_total",
            "worker_data_rpc_duration_seconds",
            "worker_stream_open_total",
            "worker_stream_inflight",
            "worker_stream_frame_bytes",
            "worker_stream_frames_total",
            "worker_stream_commit_total",
            "worker_stream_abort_total",
        ];

        assert_eq!(names, expected);
        assert!(names.iter().all(|name| !name.starts_with(concat!("beryl", "_"))));
    }

    #[test]
    fn gauge_metric_names_do_not_end_in_total() {
        for name in [
            WORKER_UP,
            WORKER_BUILD_INFO,
            WORKER_REGISTERED,
            WORKER_STORE_CAPACITY_BYTES,
            WORKER_STORE_WRITABLE,
            WORKER_STORE_BLOCKS,
            WORKER_STREAM_INFLIGHT,
        ] {
            if name.ends_with("_build_info") {
                continue;
            }
            assert!(
                !name.ends_with("_total"),
                "gauge metric {name} must not use _total suffix"
            );
        }
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

        for label in worker_metric_label_contract_names() {
            assert!(!forbidden.contains(&label), "forbidden high-cardinality label {label}");
        }
    }

    #[test]
    fn observe_helpers_emit_without_installed_recorder() {
        record_worker_started("worker", "0.0.0-test");
        set_worker_registered(false);
        set_worker_registered(true);
        record_metadata_rpc("heartbeat", "ok", "none", 0.001);
        record_heartbeat_sent("ok", "none");
        record_block_report_sent("full", "ok", "none", 0.002);
        record_store_capacity("hdd0", "available", 1);
        record_store_writable("hdd0", true);
        record_store_blocks("hdd0", 1);
        record_store_io("read", "ok", "none", 1, 0.003);
        record_data_rpc("open_read_stream", "ok", "none", 0.004);
        record_stream_open("read", "ok", "none");
        increment_stream_inflight("read");
        decrement_stream_inflight("read");
        record_stream_frame("read", "ok", "none", 1);
        record_stream_commit("ok", "none");
        record_stream_abort("ok", "none");
    }

    #[test]
    fn byte_helpers_record_byte_values() {
        let recorder = ByteCounterRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            record_store_io("read", "ok", "none", 17, 0.003);
            record_stream_frame("write", "ok", "none", 23);
        });

        assert!(recorder.has_counter(WORKER_STORE_IO_BYTES, &[("operation", "read")], 17));
        assert!(recorder.has_counter(WORKER_STREAM_FRAME_BYTES, &[("mode", "write")], 23));
    }

    #[test]
    fn worker_registered_helper_sets_gauge_values() {
        let recorder = MetricRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            set_worker_registered(false);
            set_worker_registered(true);
        });

        assert_eq!(
            *recorder.gauges.lock().expect("gauge values poisoned"),
            vec![
                (WORKER_REGISTERED.to_string(), 0.0),
                (WORKER_REGISTERED.to_string(), 1.0)
            ]
        );
    }

    #[test]
    fn store_report_uses_capacity_kinds_and_separate_writable_metric() {
        let recorder = StoreMetricRecorder::default();
        let report = StoreReport {
            total_bytes: 100,
            used_bytes: 40,
            pending_bytes: 7,
            free_bytes: 60,
            tier_free: Vec::new(),
            dirs: vec![crate::store::dirs::StoreDirReport {
                id: "hdd0".to_string(),
                path: std::path::PathBuf::from("/tmp/beryl-worker/hdd0"),
                tier: beryl_types::Tier::Hdd,
                capacity_bytes: 100,
                used_bytes: 40,
                pending_bytes: 7,
                block_count: 3,
                fs_total_bytes: 120,
                fs_free_bytes: 60,
                free_bytes: 60,
                writable: false,
                error: Some("probe failed".to_string()),
            }],
        };

        metrics::with_local_recorder(&recorder, || {
            record_store_report(&report);
        });

        assert!(recorder.has_gauge(
            WORKER_STORE_CAPACITY_BYTES,
            &[("dir_id", "hdd0"), ("kind", "total")],
            100.0,
        ));
        assert!(recorder.has_gauge(
            WORKER_STORE_CAPACITY_BYTES,
            &[("dir_id", "hdd0"), ("kind", "used")],
            40.0,
        ));
        assert!(recorder.has_gauge(
            WORKER_STORE_CAPACITY_BYTES,
            &[("dir_id", "hdd0"), ("kind", "available")],
            60.0,
        ));
        assert!(recorder.has_gauge(WORKER_STORE_WRITABLE, &[("dir_id", "hdd0")], 0.0));
        assert!(!recorder.has_gauge(
            WORKER_STORE_CAPACITY_BYTES,
            &[("dir_id", "hdd0"), ("kind", "writable")],
            0.0,
        ));
        assert!(!recorder.has_label_name(WORKER_STORE_CAPACITY_BYTES, "state"));
    }

    #[derive(Clone, Debug)]
    struct RecordedGauge {
        name: String,
        labels: Vec<(String, String)>,
        value: f64,
    }

    #[derive(Clone, Debug)]
    struct RecordedCounter {
        name: String,
        labels: Vec<(String, String)>,
        value: u64,
    }

    impl RecordedCounter {
        fn from_key(key: &Key, value: u64) -> Self {
            Self {
                name: key.name().to_string(),
                labels: key
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect(),
                value,
            }
        }

        fn matches(&self, name: &str, labels: &[(&str, &str)], value: u64) -> bool {
            self.name == name
                && self.value == value
                && labels.iter().all(|(key, value)| {
                    self.labels
                        .iter()
                        .any(|(actual_key, actual_value)| actual_key == key && actual_value == value)
                })
        }
    }

    #[derive(Default)]
    struct ByteCounterRecorder {
        counters: Arc<Mutex<Vec<RecordedCounter>>>,
    }

    impl ByteCounterRecorder {
        fn has_counter(&self, name: &str, labels: &[(&str, &str)], value: u64) -> bool {
            self.counters
                .lock()
                .expect("byte counter values poisoned")
                .iter()
                .any(|counter| counter.matches(name, labels, value))
        }
    }

    impl Recorder for ByteCounterRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::from_arc(Arc::new(ByteCounter {
                key: key.clone(),
                counters: Arc::clone(&self.counters),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    struct ByteCounter {
        key: Key,
        counters: Arc<Mutex<Vec<RecordedCounter>>>,
    }

    impl CounterFn for ByteCounter {
        fn increment(&self, value: u64) {
            self.counters
                .lock()
                .expect("byte counter values poisoned")
                .push(RecordedCounter::from_key(&self.key, value));
        }

        fn absolute(&self, value: u64) {
            self.increment(value);
        }
    }

    #[derive(Default)]
    struct StoreMetricRecorder {
        gauges: Arc<Mutex<Vec<RecordedGauge>>>,
    }

    impl StoreMetricRecorder {
        fn has_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) -> bool {
            self.gauges
                .lock()
                .expect("store gauge values poisoned")
                .iter()
                .any(|gauge| {
                    gauge.name == name
                        && gauge.value == value
                        && labels.iter().all(|(key, value)| {
                            gauge
                                .labels
                                .iter()
                                .any(|(actual_key, actual_value)| actual_key == key && actual_value == value)
                        })
                })
        }

        fn has_label_name(&self, name: &str, label_name: &str) -> bool {
            self.gauges
                .lock()
                .expect("store gauge values poisoned")
                .iter()
                .filter(|gauge| gauge.name == name)
                .any(|gauge| gauge.labels.iter().any(|(key, _)| key == label_name))
        }
    }

    impl Recorder for StoreMetricRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::noop()
        }

        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            let labels = key
                .labels()
                .map(|label| (label.key().to_string(), label.value().to_string()))
                .collect();
            Gauge::from_arc(Arc::new(StoreGauge {
                name: key.name().to_string(),
                labels,
                values: Arc::clone(&self.gauges),
            }))
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    struct StoreGauge {
        name: String,
        labels: Vec<(String, String)>,
        values: Arc<Mutex<Vec<RecordedGauge>>>,
    }

    impl GaugeFn for StoreGauge {
        fn increment(&self, value: f64) {
            self.set(value);
        }

        fn decrement(&self, value: f64) {
            self.set(-value);
        }

        fn set(&self, value: f64) {
            self.values
                .lock()
                .expect("store gauge values poisoned")
                .push(RecordedGauge {
                    name: self.name.clone(),
                    labels: self.labels.clone(),
                    value,
                });
        }
    }

    #[derive(Default)]
    struct MetricRecorder {
        gauges: Arc<Mutex<Vec<(String, f64)>>>,
    }

    impl Recorder for MetricRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            assert_eq!(key.labels().count(), 0);
            Counter::noop()
        }

        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            assert_eq!(key.labels().count(), 0);
            Gauge::from_arc(Arc::new(TestGauge {
                name: key.name().to_string(),
                values: Arc::clone(&self.gauges),
            }))
        }

        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            assert_eq!(key.labels().count(), 0);
            Histogram::noop()
        }
    }

    struct TestGauge {
        name: String,
        values: Arc<Mutex<Vec<(String, f64)>>>,
    }

    impl GaugeFn for TestGauge {
        fn increment(&self, value: f64) {
            self.set(value);
        }

        fn decrement(&self, value: f64) {
            self.set(-value);
        }

        fn set(&self, value: f64) {
            self.values
                .lock()
                .expect("gauge values poisoned")
                .push((self.name.clone(), value));
        }
    }
}
