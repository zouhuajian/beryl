// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Process-local metadata counters shared by metadata subsystems.
//!
//! The common observability layer owns exported metrics. This module only holds
//! in-process state used by metadata readiness, maintenance, and FsCore paths.

use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;

// Dedup metrics are updated by Raft storage/state-machine paths. They remain
// process-wide because dedup is an authority-wide apply concern.
pub(crate) static DEDUP_LOOKUP_HIT_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_LOOKUP_MISS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_LOOKUP_MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_EVICTED_TTL_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_EVICTED_SIZE_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_STORE_ENTRIES_GAUGE: AtomicU64 = AtomicU64::new(0);

/// Process-local metadata counters.
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
    pub(crate) delete_intents_completed_by_ack_only_total: Arc<AtomicU64>,
    pub(crate) delete_intents_completed_by_reconcile_total: Arc<AtomicU64>,
    pub(crate) delete_executor_requests_total: Arc<AtomicU64>,

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
            delete_intents_completed_by_ack_only_total: Arc::new(AtomicU64::new(0)),
            delete_intents_completed_by_reconcile_total: Arc::new(AtomicU64::new(0)),
            delete_executor_requests_total: Arc::new(AtomicU64::new(0)),
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
}

impl Default for MetadataMetrics {
    fn default() -> Self {
        Self::new()
    }
}
