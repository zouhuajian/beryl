// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Process-local metadata metrics.
//!
//! This module remains a thin shared registry for counters that are currently
//! updated across multiple metadata subsystems. Subsystem-owned metrics should
//! stay with their owner when they do not need this shared export surface.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

// Dedup metrics are updated by Raft storage/state-machine paths. They remain
// process-wide because dedup is an authority-wide apply concern.
pub(crate) static DEDUP_LOOKUP_HIT_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_LOOKUP_MISS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_LOOKUP_MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_EVICTED_TTL_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_EVICTED_SIZE_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_STORE_ENTRIES_GAUGE: AtomicU64 = AtomicU64::new(0);

// Current authz implementation only supports NONE; future ACL/Ranger metrics
// should be added with their implementation rather than exported as zeroes.
pub(crate) static AUTHZ_ALLOW_NONE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Metadata metrics with real runtime updaters and Prometheus export lines.
#[derive(Clone)]
pub struct MetadataMetrics {
    // Maintenance GC and gates.
    pub(crate) gc_cycles_total: Arc<AtomicU64>,
    pub(crate) gc_refcount_load_fail_total: Arc<AtomicU64>,
    pub(crate) gc_candidates: Arc<AtomicUsize>,
    pub(crate) gc_gate_state: Arc<AtomicUsize>,
    pub(crate) gc_skipped_total: Arc<AtomicU64>,
    pub(crate) gc_candidates_total: Arc<AtomicU64>,
    pub(crate) gc_actions_total: Arc<AtomicU64>,
    pub(crate) gc_refcount_reload_success_total: Arc<AtomicU64>,
    pub(crate) gc_refcount_reload_fail_total: Arc<AtomicU64>,

    // Lease and orphan maintenance gates.
    pub(crate) lease_gate_state: Arc<AtomicUsize>,
    pub(crate) lease_cleanup_skipped_total: Arc<AtomicU64>,
    pub(crate) lease_cleanup_actions_total: Arc<AtomicU64>,
    pub(crate) orphan_gate_state: Arc<AtomicUsize>,
    pub(crate) orphan_cleanup_skipped_total: Arc<AtomicU64>,
    pub(crate) orphan_cleanup_actions_total: Arc<AtomicU64>,

    // Block report convergence used by destructive maintenance gates.
    pub(crate) maintenance_blockreport_active_workers: Arc<AtomicUsize>,
    pub(crate) maintenance_blockreport_full_reported_workers: Arc<AtomicUsize>,
    pub(crate) maintenance_blockreport_ratio: Arc<AtomicUsize>,
    pub(crate) maintenance_blockreport_converged: Arc<AtomicUsize>,

    // Delete intent creation and execution.
    pub(crate) delete_intents_created_total: Arc<AtomicU64>,
    pub(crate) delete_intents_create_failed_total: Arc<AtomicU64>,
    pub(crate) maintenance_gc_created_intents_total: Arc<AtomicU64>,
    pub(crate) delete_intents_completed_total: Arc<AtomicU64>,
    pub(crate) delete_intents_retry_total: Arc<AtomicU64>,
    pub(crate) delete_intents_completed_by_ack_only_total: Arc<AtomicU64>,
    pub(crate) delete_intents_completed_by_reconcile_total: Arc<AtomicU64>,
    pub(crate) delete_executor_requests_total: Arc<AtomicU64>,
    pub(crate) delete_executor_requests_failed_total: Arc<AtomicU64>,

    // Over-replica cleanup.
    pub(crate) overrep_candidates_total: Arc<AtomicU64>,
    pub(crate) overrep_scanned_total: Arc<AtomicU64>,
    pub(crate) overrep_intents_created_total: Arc<AtomicU64>,
    pub(crate) overrep_skipped_conflict_total: Arc<AtomicU64>,
    pub(crate) overrep_skipped_gate_total: Arc<AtomicU64>,
    pub(crate) overrep_skipped_state_total: Arc<AtomicU64>,

    // Root readiness.
    pub(crate) root_ready: Arc<AtomicUsize>,
    pub(crate) root_wait_attempts: Arc<AtomicU64>,
    pub(crate) root_wait_elapsed_ms: Arc<AtomicU64>,

    // Filesystem routing and Raft append guardrails.
    pub(crate) fs_write_routed_total: Arc<AtomicU64>,
    pub(crate) fs_write_cross_mount_rename_exdev_total: Arc<AtomicU64>,
    pub(crate) fs_write_mount_epoch_mismatch_total: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_total: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_create: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_mkdir: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_unlink: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_directory_delete: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_rename: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_setattr: Arc<AtomicU64>,
}

impl MetadataMetrics {
    pub fn new() -> Self {
        Self {
            gc_cycles_total: Arc::new(AtomicU64::new(0)),
            gc_refcount_load_fail_total: Arc::new(AtomicU64::new(0)),
            gc_candidates: Arc::new(AtomicUsize::new(0)),
            gc_gate_state: Arc::new(AtomicUsize::new(0)),
            gc_skipped_total: Arc::new(AtomicU64::new(0)),
            gc_candidates_total: Arc::new(AtomicU64::new(0)),
            gc_actions_total: Arc::new(AtomicU64::new(0)),
            gc_refcount_reload_success_total: Arc::new(AtomicU64::new(0)),
            gc_refcount_reload_fail_total: Arc::new(AtomicU64::new(0)),
            lease_gate_state: Arc::new(AtomicUsize::new(0)),
            lease_cleanup_skipped_total: Arc::new(AtomicU64::new(0)),
            lease_cleanup_actions_total: Arc::new(AtomicU64::new(0)),
            orphan_gate_state: Arc::new(AtomicUsize::new(0)),
            orphan_cleanup_skipped_total: Arc::new(AtomicU64::new(0)),
            orphan_cleanup_actions_total: Arc::new(AtomicU64::new(0)),
            maintenance_blockreport_active_workers: Arc::new(AtomicUsize::new(0)),
            maintenance_blockreport_full_reported_workers: Arc::new(AtomicUsize::new(0)),
            maintenance_blockreport_ratio: Arc::new(AtomicUsize::new(0)),
            maintenance_blockreport_converged: Arc::new(AtomicUsize::new(0)),
            delete_intents_created_total: Arc::new(AtomicU64::new(0)),
            delete_intents_create_failed_total: Arc::new(AtomicU64::new(0)),
            maintenance_gc_created_intents_total: Arc::new(AtomicU64::new(0)),
            delete_intents_completed_total: Arc::new(AtomicU64::new(0)),
            delete_intents_retry_total: Arc::new(AtomicU64::new(0)),
            delete_intents_completed_by_ack_only_total: Arc::new(AtomicU64::new(0)),
            delete_intents_completed_by_reconcile_total: Arc::new(AtomicU64::new(0)),
            delete_executor_requests_total: Arc::new(AtomicU64::new(0)),
            delete_executor_requests_failed_total: Arc::new(AtomicU64::new(0)),
            overrep_candidates_total: Arc::new(AtomicU64::new(0)),
            overrep_scanned_total: Arc::new(AtomicU64::new(0)),
            overrep_intents_created_total: Arc::new(AtomicU64::new(0)),
            overrep_skipped_conflict_total: Arc::new(AtomicU64::new(0)),
            overrep_skipped_gate_total: Arc::new(AtomicU64::new(0)),
            overrep_skipped_state_total: Arc::new(AtomicU64::new(0)),
            root_ready: Arc::new(AtomicUsize::new(0)),
            root_wait_attempts: Arc::new(AtomicU64::new(0)),
            root_wait_elapsed_ms: Arc::new(AtomicU64::new(0)),
            fs_write_routed_total: Arc::new(AtomicU64::new(0)),
            fs_write_cross_mount_rename_exdev_total: Arc::new(AtomicU64::new(0)),
            fs_write_mount_epoch_mismatch_total: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_total: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_create: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_mkdir: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_unlink: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_directory_delete: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_rename: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_setattr: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Export metrics in the existing Prometheus text format.
    pub fn export_prometheus(&self) -> String {
        let mut metrics = Vec::new();

        push_u64(
            &mut metrics,
            "metadata_gc_cycles_total",
            "Total number of GC cycles",
            &self.gc_cycles_total,
        );
        push_u64(
            &mut metrics,
            "maintenance_gc_refcount_load_fail_total",
            "Total number of refcount load failures",
            &self.gc_refcount_load_fail_total,
        );
        push_usize(
            &mut metrics,
            "maintenance_gc_candidates",
            "Current number of GC candidates",
            &self.gc_candidates,
        );
        push_usize(
            &mut metrics,
            "maintenance_gc_gate_state",
            "GC gate state (1=ready, 0=not ready)",
            &self.gc_gate_state,
        );
        push_u64(
            &mut metrics,
            "maintenance_gc_skipped_total",
            "Total number of GC cycles skipped",
            &self.gc_skipped_total,
        );
        push_u64(
            &mut metrics,
            "maintenance_gc_candidates_total",
            "Total number of GC candidates collected",
            &self.gc_candidates_total,
        );
        push_u64(
            &mut metrics,
            "maintenance_gc_actions_total",
            "Total number of GC destructive actions",
            &self.gc_actions_total,
        );
        push_u64(
            &mut metrics,
            "maintenance_gc_refcount_reload_success_total",
            "Total number of successful refcount reloads",
            &self.gc_refcount_reload_success_total,
        );
        push_u64(
            &mut metrics,
            "maintenance_gc_refcount_reload_fail_total",
            "Total number of failed refcount reloads",
            &self.gc_refcount_reload_fail_total,
        );

        push_usize(
            &mut metrics,
            "maintenance_lease_gate_state",
            "Lease cleanup gate state (1=ready, 0=not ready)",
            &self.lease_gate_state,
        );
        push_u64(
            &mut metrics,
            "maintenance_lease_cleanup_skipped_total",
            "Total number of lease cleanup cycles skipped",
            &self.lease_cleanup_skipped_total,
        );
        push_u64(
            &mut metrics,
            "maintenance_lease_cleanup_actions_total",
            "Total number of lease cleanup actions",
            &self.lease_cleanup_actions_total,
        );
        push_usize(
            &mut metrics,
            "maintenance_orphan_gate_state",
            "Orphan cleanup gate state (1=ready, 0=not ready)",
            &self.orphan_gate_state,
        );
        push_u64(
            &mut metrics,
            "maintenance_orphan_cleanup_skipped_total",
            "Total number of orphan cleanup cycles skipped",
            &self.orphan_cleanup_skipped_total,
        );
        push_u64(
            &mut metrics,
            "maintenance_orphan_cleanup_actions_total",
            "Total number of orphan cleanup actions",
            &self.orphan_cleanup_actions_total,
        );

        push_usize(
            &mut metrics,
            "maintenance_blockreport_active_workers",
            "Number of active workers (heartbeat within TTL)",
            &self.maintenance_blockreport_active_workers,
        );
        push_usize(
            &mut metrics,
            "maintenance_blockreport_full_reported_workers",
            "Number of active workers with full report",
            &self.maintenance_blockreport_full_reported_workers,
        );
        push_usize(
            &mut metrics,
            "maintenance_blockreport_ratio",
            "Block report convergence ratio (ratio * 1000)",
            &self.maintenance_blockreport_ratio,
        );
        push_usize(
            &mut metrics,
            "maintenance_blockreport_converged",
            "Block report convergence status (1=converged, 0=not converged)",
            &self.maintenance_blockreport_converged,
        );

        push_u64(
            &mut metrics,
            "delete_intents_created_total",
            "Total number of delete intents created",
            &self.delete_intents_created_total,
        );
        push_u64(
            &mut metrics,
            "delete_intents_create_failed_total",
            "Total number of failed delete intent creations",
            &self.delete_intents_create_failed_total,
        );
        push_u64(
            &mut metrics,
            "maintenance_gc_created_intents_total",
            "Total number of delete intents created by GC",
            &self.maintenance_gc_created_intents_total,
        );
        push_u64(
            &mut metrics,
            "delete_intents_completed_total",
            "Total number of delete intents completed",
            &self.delete_intents_completed_total,
        );
        push_u64(
            &mut metrics,
            "delete_intents_completed_by_ack_only_total",
            "Total number of delete intents completed by ack only (reconcile pending)",
            &self.delete_intents_completed_by_ack_only_total,
        );
        push_u64(
            &mut metrics,
            "delete_intents_completed_by_reconcile_total",
            "Total number of delete intents completed by reconcile (ack + blockreport)",
            &self.delete_intents_completed_by_reconcile_total,
        );
        push_u64(
            &mut metrics,
            "delete_intents_retry_total",
            "Total number of delete intents retried",
            &self.delete_intents_retry_total,
        );
        push_u64(
            &mut metrics,
            "delete_executor_requests_total",
            "Total number of delete executor requests sent",
            &self.delete_executor_requests_total,
        );
        push_u64(
            &mut metrics,
            "delete_executor_requests_failed_total",
            "Total number of failed delete executor requests",
            &self.delete_executor_requests_failed_total,
        );

        push_u64(
            &mut metrics,
            "overrep_candidates_total",
            "Total number of over-replication candidates",
            &self.overrep_candidates_total,
        );
        push_u64(
            &mut metrics,
            "overrep_scanned_total",
            "Total number of over-replication scan candidates examined",
            &self.overrep_scanned_total,
        );
        push_u64(
            &mut metrics,
            "overrep_intents_created_total",
            "Total number of over-replication delete intents created",
            &self.overrep_intents_created_total,
        );
        push_u64(
            &mut metrics,
            "overrep_skipped_conflict_total",
            "Total number of over-replication candidates skipped by inflight conflict",
            &self.overrep_skipped_conflict_total,
        );
        push_u64(
            &mut metrics,
            "overrep_skipped_gate_total",
            "Total number of over-replication candidates skipped by destructive gate",
            &self.overrep_skipped_gate_total,
        );
        push_u64(
            &mut metrics,
            "overrep_skipped_state_total",
            "Total number of over-replication candidates skipped by metadata state",
            &self.overrep_skipped_state_total,
        );

        push_usize(
            &mut metrics,
            "metadata_root_ready",
            "Root mount readiness status (1=ready, 0=not ready)",
            &self.root_ready,
        );
        push_u64(
            &mut metrics,
            "metadata_root_wait_attempts",
            "Root mount readiness wait attempts",
            &self.root_wait_attempts,
        );
        push_u64(
            &mut metrics,
            "metadata_root_wait_elapsed_ms",
            "Root mount readiness elapsed time in ms",
            &self.root_wait_elapsed_ms,
        );

        push_u64(
            &mut metrics,
            "metadata_fs_write_routed_total",
            "Total number of FS write operations routed to mount namespace owner group",
            &self.fs_write_routed_total,
        );
        push_u64(
            &mut metrics,
            "metadata_fs_write_cross_mount_rename_exdev_total",
            "Total number of cross-mount rename operations rejected with EXDEV",
            &self.fs_write_cross_mount_rename_exdev_total,
        );
        push_u64(
            &mut metrics,
            "metadata_fs_write_mount_epoch_mismatch_total",
            "Total number of FS write operations rejected due to mount epoch mismatch",
            &self.fs_write_mount_epoch_mismatch_total,
        );
        push_u64(
            &mut metrics,
            "metadata_fs_raft_appends_total",
            "Total number of FS Raft log appends (all operations)",
            &self.fs_raft_appends_total,
        );
        push_u64(
            &mut metrics,
            "metadata_fs_raft_appends_create",
            "Total number of Create operations that wrote to Raft",
            &self.fs_raft_appends_create,
        );
        push_u64(
            &mut metrics,
            "metadata_fs_raft_appends_mkdir",
            "Total number of Mkdir operations that wrote to Raft",
            &self.fs_raft_appends_mkdir,
        );
        push_u64(
            &mut metrics,
            "metadata_fs_raft_appends_unlink",
            "Total number of Unlink operations that wrote to Raft",
            &self.fs_raft_appends_unlink,
        );
        push_u64(
            &mut metrics,
            "metadata_fs_raft_appends_directory_delete",
            "Total number of directory delete operations that wrote to Raft",
            &self.fs_raft_appends_directory_delete,
        );
        push_u64(
            &mut metrics,
            "metadata_fs_raft_appends_rename",
            "Total number of Rename operations that wrote to Raft",
            &self.fs_raft_appends_rename,
        );
        push_u64(
            &mut metrics,
            "metadata_fs_raft_appends_setattr",
            "Total number of SetAttr operations that wrote to Raft",
            &self.fs_raft_appends_setattr,
        );

        push_u64_static(
            &mut metrics,
            "metadata_authz_allow_none_total",
            "Total authz allows for None scheme",
            &AUTHZ_ALLOW_NONE_TOTAL,
        );

        metrics.join("\n")
    }
}

impl Default for MetadataMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn push_u64(metrics: &mut Vec<String>, name: &str, help: &str, value: &Arc<AtomicU64>) {
    push_u64_static(metrics, name, help, value.as_ref());
}

fn push_u64_static(metrics: &mut Vec<String>, name: &str, help: &str, value: &AtomicU64) {
    metrics.push(format!("# HELP {name} {help}"));
    metrics.push(format!("{name} {}", value.load(Ordering::Relaxed)));
}

fn push_usize(metrics: &mut Vec<String>, name: &str, help: &str, value: &Arc<AtomicUsize>) {
    metrics.push(format!("# HELP {name} {help}"));
    metrics.push(format!("{name} {}", value.load(Ordering::Relaxed)));
}
