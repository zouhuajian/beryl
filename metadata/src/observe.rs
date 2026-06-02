// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata-owned metrics emitted through the shared recorder.

pub(crate) const METADATA_UP: &str = "metadata_up";
pub(crate) const METADATA_ROOT_READY: &str = "metadata_root_ready";
pub(crate) const METADATA_WORKER_REGISTERED_TOTAL: &str = "metadata_worker_registered_total";
pub(crate) const METADATA_WORKER_BLOCK_REPORT_PROCESSED_TOTAL: &str = "metadata_worker_block_report_processed_total";
pub(crate) const MAINTENANCE_ORPHAN_CLEANUP_SKIPPED_TOTAL: &str = "maintenance_orphan_cleanup_skipped_total";

pub(crate) fn record_metadata_started() {
    metrics::gauge!(METADATA_UP).set(1.0);
}

pub(crate) fn record_root_ready(ready: bool) {
    metrics::gauge!(METADATA_ROOT_READY).set(if ready { 1.0 } else { 0.0 });
}

pub(crate) fn record_worker_registered() {
    metrics::counter!(METADATA_WORKER_REGISTERED_TOTAL).increment(1);
}

pub(crate) fn record_worker_block_report_processed() {
    metrics::counter!(METADATA_WORKER_BLOCK_REPORT_PROCESSED_TOTAL).increment(1);
}

pub(crate) fn record_orphan_cleanup_skipped() {
    metrics::counter!(MAINTENANCE_ORPHAN_CLEANUP_SKIPPED_TOTAL).increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p0_metric_names_match_contract() {
        let names = [
            METADATA_UP,
            METADATA_ROOT_READY,
            METADATA_WORKER_REGISTERED_TOTAL,
            METADATA_WORKER_BLOCK_REPORT_PROCESSED_TOTAL,
            MAINTENANCE_ORPHAN_CLEANUP_SKIPPED_TOTAL,
        ];

        assert_eq!(METADATA_UP, "metadata_up");
        assert_eq!(METADATA_ROOT_READY, "metadata_root_ready");
        assert_eq!(METADATA_WORKER_REGISTERED_TOTAL, "metadata_worker_registered_total");
        assert_eq!(
            METADATA_WORKER_BLOCK_REPORT_PROCESSED_TOTAL,
            "metadata_worker_block_report_processed_total"
        );
        assert_eq!(
            MAINTENANCE_ORPHAN_CLEANUP_SKIPPED_TOTAL,
            "maintenance_orphan_cleanup_skipped_total"
        );
        assert!(names.iter().all(|name| !name.starts_with(concat!("vecton", "_"))));
    }

    #[test]
    fn observe_helpers_emit_without_installed_recorder() {
        record_metadata_started();
        record_root_ready(false);
        record_root_ready(true);
        record_worker_registered();
        record_worker_block_report_processed();
        record_orphan_cleanup_skipped();
    }
}
