// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metrics for metadata service.
//!
//! This module defines and exports key metrics for monitoring the metadata service.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

// Global dedup metrics (low cardinality, process-wide).
pub static DEDUP_LOOKUP_HIT_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static DEDUP_LOOKUP_MISS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static DEDUP_LOOKUP_MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static DEDUP_EVICTED_TTL_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static DEDUP_EVICTED_SIZE_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static DEDUP_STORE_ENTRIES_GAUGE: AtomicU64 = AtomicU64::new(0);

// Authz/cache metrics (low cardinality, process-wide).
pub static AUTHZ_GROUPS_CACHE_HIT_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_GROUPS_CACHE_MISS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_GROUPS_CACHE_EXPIRY_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_GROUPS_STALE_FALLBACK_USED_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_GROUPS_RESOLVER_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_PERM_CACHE_HIT_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_PERM_CACHE_MISS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_PERM_CACHE_INVALIDATE_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_ALLOW_RANGER_PATH_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_DENY_RANGER_PATH_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_ALLOW_ACL_INODE_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_DENY_ACL_INODE_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_ALLOW_NONE_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static AUTHZ_DENY_NONE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Metadata service metrics.
#[derive(Clone)]
pub struct MetadataMetrics {
    // Dedup metrics (low cardinality)
    pub dedup_lookup_hit_total: Arc<AtomicU64>,
    pub dedup_lookup_miss_total: Arc<AtomicU64>,
    pub dedup_lookup_mismatch_total: Arc<AtomicU64>,
    pub dedup_evicted_ttl_total: Arc<AtomicU64>,
    pub dedup_evicted_size_total: Arc<AtomicU64>,
    pub dedup_store_entries_gauge: Arc<AtomicU64>,
    // File operations
    pub files_total: Arc<AtomicUsize>,
    pub files_created_total: Arc<AtomicU64>,
    pub files_deleted_total: Arc<AtomicU64>,
    pub files_renamed_total: Arc<AtomicU64>,

    // Block operations
    pub blocks_total: Arc<AtomicUsize>,
    pub blocks_created_total: Arc<AtomicU64>,
    pub blocks_deleted_total: Arc<AtomicU64>,
    pub block_ref_counts_total: Arc<AtomicUsize>,

    // Lease operations
    pub leases_active: Arc<AtomicUsize>,
    pub leases_expired_total: Arc<AtomicU64>,
    pub leases_cleaned_total: Arc<AtomicU64>,
    // RenewLease metrics
    pub renew_lease_total: Arc<AtomicU64>,
    pub renew_lease_failed_total: Arc<AtomicU64>,
    // Lease cleanup metrics (enhanced)
    pub lease_cleanup_soft_expired_total: Arc<AtomicU64>,
    pub lease_cleanup_hard_expired_total: Arc<AtomicU64>,

    // UFS operations
    pub ufs_operations_total: Arc<AtomicU64>,
    pub ufs_operations_failed_total: Arc<AtomicU64>,
    pub ufs_cross_mount_renames_total: Arc<AtomicU64>,

    // Raft operations
    pub raft_commands_applied_total: Arc<AtomicU64>,
    pub raft_snapshots_created_total: Arc<AtomicU64>,
    pub raft_snapshots_restored_total: Arc<AtomicU64>,

    // Maintenance operations
    pub gc_cycles_total: Arc<AtomicU64>,
    pub gc_blocks_collected_total: Arc<AtomicU64>,
    pub repair_tasks_executed_total: Arc<AtomicU64>,
    pub orphan_blocks_processed_total: Arc<AtomicU64>,

    // GC readiness and state metrics
    pub gc_ready: Arc<AtomicUsize>, // 0 or 1 (deprecated, use gc_gate_state)
    pub gc_refcount_load_fail_total: Arc<AtomicU64>,
    pub gc_skipped_cycles_total: Arc<AtomicU64>,
    pub gc_candidates: Arc<AtomicUsize>,
    pub gc_swept_total: Arc<AtomicU64>,
    pub gc_rebuild_attempt_total: Arc<AtomicU64>,
    pub gc_rebuild_fail_total: Arc<AtomicU64>,
    // New gate-based metrics
    pub gc_gate_state: Arc<AtomicUsize>, // 0 or 1
    pub gc_skipped_total: Arc<AtomicU64>,
    pub gc_candidates_total: Arc<AtomicU64>,
    pub gc_actions_total: Arc<AtomicU64>,
    pub gc_refcount_reload_success_total: Arc<AtomicU64>,
    pub gc_refcount_reload_fail_total: Arc<AtomicU64>,
    // Lease cleanup metrics
    pub lease_gate_state: Arc<AtomicUsize>,
    pub lease_cleanup_skipped_total: Arc<AtomicU64>,
    pub lease_cleanup_actions_total: Arc<AtomicU64>,
    // Orphan cleanup metrics
    pub orphan_gate_state: Arc<AtomicUsize>,
    pub orphan_cleanup_skipped_total: Arc<AtomicU64>,
    pub orphan_cleanup_actions_total: Arc<AtomicU64>,

    // Block report convergence metrics (for maintenance safety gate)
    pub maintenance_blockreport_active_workers: Arc<AtomicUsize>,
    pub maintenance_blockreport_full_reported_workers: Arc<AtomicUsize>,
    pub maintenance_blockreport_ratio: Arc<AtomicUsize>, // ratio * 1000 (for precision)
    pub maintenance_blockreport_converged: Arc<AtomicUsize>, // 0 or 1

    // DeleteIntent metrics
    pub delete_intents_created_total: Arc<AtomicU64>,
    pub delete_intents_create_failed_total: Arc<AtomicU64>,
    pub maintenance_gc_created_intents_total: Arc<AtomicU64>,
    pub delete_intents_completed_total: Arc<AtomicU64>,
    pub delete_intents_retry_total: Arc<AtomicU64>,
    // DeleteIntent completion metrics broken down by path (ack + reconcile)
    pub delete_intents_completed_by_ack_only_total: Arc<AtomicU64>,
    pub delete_intents_completed_by_reconcile_total: Arc<AtomicU64>,
    // DeleteExecutor metrics
    pub delete_executor_inflight_total: Arc<AtomicUsize>,
    pub delete_executor_requests_total: Arc<AtomicU64>,
    pub delete_executor_requests_failed_total: Arc<AtomicU64>,

    // Full report lease metrics
    pub full_report_leases_inflight: Arc<AtomicUsize>,
    pub full_report_leases_available: Arc<AtomicUsize>,
    pub full_report_leases_waiting: Arc<AtomicUsize>, // Estimated: needs_full_sync but no lease
    pub full_report_granted_total: Arc<AtomicU64>,
    pub full_report_throttled_total: Arc<AtomicU64>,

    // Over-replication cleanup metrics
    pub overrep_candidates_total: Arc<AtomicU64>,
    pub overrep_scanned_total: Arc<AtomicU64>,
    pub overrep_intents_created_total: Arc<AtomicU64>,
    pub overrep_skipped_conflict_total: Arc<AtomicU64>,
    pub overrep_skipped_gate_total: Arc<AtomicU64>,
    pub overrep_skipped_state_total: Arc<AtomicU64>,

    // Request latencies (in microseconds)
    pub request_latency_us: Arc<AtomicU64>,     // Last request latency
    pub request_latency_p50_us: Arc<AtomicU64>, // P50 latency (simplified)
    pub request_latency_p99_us: Arc<AtomicU64>, // P99 latency (simplified)

    // Root readiness metrics
    pub root_ready: Arc<AtomicUsize>,         // 0 or 1
    pub root_wait_attempts: Arc<AtomicU64>,   // attempts until ready
    pub root_wait_elapsed_ms: Arc<AtomicU64>, // elapsed ms until ready

    // FS write routing metrics
    pub fs_write_routed_total: Arc<AtomicU64>, // Total FS write operations routed
    pub fs_write_cross_mount_rename_exdev_total: Arc<AtomicU64>, // Cross-mount rename EXDEV returns
    pub fs_write_mount_epoch_mismatch_total: Arc<AtomicU64>, // Mount epoch mismatch (NEED_REFRESH)

    // Raft write amplification guardrails
    // Counters for each FS write operation type that writes to Raft
    pub fs_raft_appends_total: Arc<AtomicU64>, // Total FS Raft appends (all ops)
    pub fs_raft_appends_create: Arc<AtomicU64>, // Create operations
    pub fs_raft_appends_mkdir: Arc<AtomicU64>, // Mkdir operations
    pub fs_raft_appends_unlink: Arc<AtomicU64>, // Unlink operations
    pub fs_raft_appends_rmdir: Arc<AtomicU64>, // Rmdir operations
    pub fs_raft_appends_rename: Arc<AtomicU64>, // Rename operations
    pub fs_raft_appends_setattr: Arc<AtomicU64>, // SetAttr operations
}

impl MetadataMetrics {
    pub fn new() -> Self {
        Self {
            files_total: Arc::new(AtomicUsize::new(0)),
            files_created_total: Arc::new(AtomicU64::new(0)),
            files_deleted_total: Arc::new(AtomicU64::new(0)),
            files_renamed_total: Arc::new(AtomicU64::new(0)),
            blocks_total: Arc::new(AtomicUsize::new(0)),
            blocks_created_total: Arc::new(AtomicU64::new(0)),
            blocks_deleted_total: Arc::new(AtomicU64::new(0)),
            block_ref_counts_total: Arc::new(AtomicUsize::new(0)),
            leases_active: Arc::new(AtomicUsize::new(0)),
            leases_expired_total: Arc::new(AtomicU64::new(0)),
            leases_cleaned_total: Arc::new(AtomicU64::new(0)),
            renew_lease_total: Arc::new(AtomicU64::new(0)),
            renew_lease_failed_total: Arc::new(AtomicU64::new(0)),
            lease_cleanup_soft_expired_total: Arc::new(AtomicU64::new(0)),
            lease_cleanup_hard_expired_total: Arc::new(AtomicU64::new(0)),
            ufs_operations_total: Arc::new(AtomicU64::new(0)),
            ufs_operations_failed_total: Arc::new(AtomicU64::new(0)),
            ufs_cross_mount_renames_total: Arc::new(AtomicU64::new(0)),
            raft_commands_applied_total: Arc::new(AtomicU64::new(0)),
            raft_snapshots_created_total: Arc::new(AtomicU64::new(0)),
            raft_snapshots_restored_total: Arc::new(AtomicU64::new(0)),
            gc_cycles_total: Arc::new(AtomicU64::new(0)),
            gc_blocks_collected_total: Arc::new(AtomicU64::new(0)),
            repair_tasks_executed_total: Arc::new(AtomicU64::new(0)),
            orphan_blocks_processed_total: Arc::new(AtomicU64::new(0)),
            gc_ready: Arc::new(AtomicUsize::new(0)),
            gc_refcount_load_fail_total: Arc::new(AtomicU64::new(0)),
            gc_skipped_cycles_total: Arc::new(AtomicU64::new(0)),
            gc_candidates: Arc::new(AtomicUsize::new(0)),
            gc_swept_total: Arc::new(AtomicU64::new(0)),
            gc_rebuild_attempt_total: Arc::new(AtomicU64::new(0)),
            gc_rebuild_fail_total: Arc::new(AtomicU64::new(0)),
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
            dedup_lookup_hit_total: Arc::new(AtomicU64::new(0)),
            dedup_lookup_miss_total: Arc::new(AtomicU64::new(0)),
            dedup_lookup_mismatch_total: Arc::new(AtomicU64::new(0)),
            dedup_evicted_ttl_total: Arc::new(AtomicU64::new(0)),
            dedup_evicted_size_total: Arc::new(AtomicU64::new(0)),
            dedup_store_entries_gauge: Arc::new(AtomicU64::new(0)),
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
            delete_executor_inflight_total: Arc::new(AtomicUsize::new(0)),
            delete_executor_requests_total: Arc::new(AtomicU64::new(0)),
            delete_executor_requests_failed_total: Arc::new(AtomicU64::new(0)),
            full_report_leases_inflight: Arc::new(AtomicUsize::new(0)),
            full_report_leases_available: Arc::new(AtomicUsize::new(0)),
            full_report_leases_waiting: Arc::new(AtomicUsize::new(0)),
            full_report_granted_total: Arc::new(AtomicU64::new(0)),
            full_report_throttled_total: Arc::new(AtomicU64::new(0)),
            overrep_candidates_total: Arc::new(AtomicU64::new(0)),
            overrep_scanned_total: Arc::new(AtomicU64::new(0)),
            overrep_intents_created_total: Arc::new(AtomicU64::new(0)),
            overrep_skipped_conflict_total: Arc::new(AtomicU64::new(0)),
            overrep_skipped_gate_total: Arc::new(AtomicU64::new(0)),
            overrep_skipped_state_total: Arc::new(AtomicU64::new(0)),
            request_latency_us: Arc::new(AtomicU64::new(0)),
            request_latency_p50_us: Arc::new(AtomicU64::new(0)),
            request_latency_p99_us: Arc::new(AtomicU64::new(0)),
            root_ready: Arc::new(AtomicUsize::new(0)),
            root_wait_attempts: Arc::new(AtomicU64::new(0)),
            root_wait_elapsed_ms: Arc::new(AtomicU64::new(0)),
            // FS write routing metrics
            fs_write_routed_total: Arc::new(AtomicU64::new(0)),
            fs_write_cross_mount_rename_exdev_total: Arc::new(AtomicU64::new(0)),
            fs_write_mount_epoch_mismatch_total: Arc::new(AtomicU64::new(0)),
            // Raft write amplification guardrails
            fs_raft_appends_total: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_create: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_mkdir: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_unlink: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_rmdir: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_rename: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_setattr: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Export metrics in Prometheus format.
    pub fn export_prometheus(&self) -> String {
        let mut metrics = Vec::new();

        // File metrics
        metrics.push("# HELP metadata_files_total Total number of files".to_string());
        metrics.push(format!(
            "metadata_files_total {}",
            self.files_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_files_created_total Total number of files created".to_string());
        metrics.push(format!(
            "metadata_files_created_total {}",
            self.files_created_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_files_deleted_total Total number of files deleted".to_string());
        metrics.push(format!(
            "metadata_files_deleted_total {}",
            self.files_deleted_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_files_renamed_total Total number of files renamed".to_string());
        metrics.push(format!(
            "metadata_files_renamed_total {}",
            self.files_renamed_total.load(Ordering::Relaxed)
        ));

        // Block metrics
        metrics.push("# HELP metadata_blocks_total Total number of blocks".to_string());
        metrics.push(format!(
            "metadata_blocks_total {}",
            self.blocks_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_blocks_created_total Total number of blocks created".to_string());
        metrics.push(format!(
            "metadata_blocks_created_total {}",
            self.blocks_created_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_blocks_deleted_total Total number of blocks deleted".to_string());
        metrics.push(format!(
            "metadata_blocks_deleted_total {}",
            self.blocks_deleted_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_block_ref_counts_total Total number of block reference counts".to_string());
        metrics.push(format!(
            "metadata_block_ref_counts_total {}",
            self.block_ref_counts_total.load(Ordering::Relaxed)
        ));

        // Lease metrics
        metrics.push("# HELP metadata_leases_active Current number of active leases".to_string());
        metrics.push(format!(
            "metadata_leases_active {}",
            self.leases_active.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_leases_expired_total Total number of expired leases".to_string());
        metrics.push(format!(
            "metadata_leases_expired_total {}",
            self.leases_expired_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_leases_cleaned_total Total number of leases cleaned".to_string());
        metrics.push(format!(
            "metadata_leases_cleaned_total {}",
            self.leases_cleaned_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_renew_lease_total Total number of lease renewals".to_string());
        metrics.push(format!(
            "metadata_renew_lease_total {}",
            self.renew_lease_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_renew_lease_failed_total Total number of failed lease renewals".to_string());
        metrics.push(format!(
            "metadata_renew_lease_failed_total {}",
            self.renew_lease_failed_total.load(Ordering::Relaxed)
        ));
        metrics
            .push("# HELP metadata_lease_cleanup_soft_expired_total Total number of soft-expired leases".to_string());
        metrics.push(format!(
            "metadata_lease_cleanup_soft_expired_total {}",
            self.lease_cleanup_soft_expired_total.load(Ordering::Relaxed)
        ));
        metrics
            .push("# HELP metadata_lease_cleanup_hard_expired_total Total number of hard-expired leases".to_string());
        metrics.push(format!(
            "metadata_lease_cleanup_hard_expired_total {}",
            self.lease_cleanup_hard_expired_total.load(Ordering::Relaxed)
        ));

        // UFS metrics
        metrics.push("# HELP metadata_ufs_operations_total Total number of UFS operations".to_string());
        metrics.push(format!(
            "metadata_ufs_operations_total {}",
            self.ufs_operations_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_ufs_operations_failed_total Total number of failed UFS operations".to_string());
        metrics.push(format!(
            "metadata_ufs_operations_failed_total {}",
            self.ufs_operations_failed_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_ufs_cross_mount_renames_total Total number of cross-mount renames".to_string());
        metrics.push(format!(
            "metadata_ufs_cross_mount_renames_total {}",
            self.ufs_cross_mount_renames_total.load(Ordering::Relaxed)
        ));

        // Raft metrics
        metrics.push("# HELP metadata_raft_commands_applied_total Total number of Raft commands applied".to_string());
        metrics.push(format!(
            "metadata_raft_commands_applied_total {}",
            self.raft_commands_applied_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_raft_snapshots_created_total Total number of Raft snapshots created".to_string());
        metrics.push(format!(
            "metadata_raft_snapshots_created_total {}",
            self.raft_snapshots_created_total.load(Ordering::Relaxed)
        ));
        metrics
            .push("# HELP metadata_raft_snapshots_restored_total Total number of Raft snapshots restored".to_string());
        metrics.push(format!(
            "metadata_raft_snapshots_restored_total {}",
            self.raft_snapshots_restored_total.load(Ordering::Relaxed)
        ));

        // Maintenance metrics
        metrics.push("# HELP metadata_gc_cycles_total Total number of GC cycles".to_string());
        metrics.push(format!(
            "metadata_gc_cycles_total {}",
            self.gc_cycles_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_gc_blocks_collected_total Total number of blocks collected by GC".to_string());
        metrics.push(format!(
            "metadata_gc_blocks_collected_total {}",
            self.gc_blocks_collected_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_repair_tasks_executed_total Total number of repair tasks executed".to_string());
        metrics.push(format!(
            "metadata_repair_tasks_executed_total {}",
            self.repair_tasks_executed_total.load(Ordering::Relaxed)
        ));
        metrics
            .push("# HELP metadata_orphan_blocks_processed_total Total number of orphan blocks processed".to_string());
        metrics.push(format!(
            "metadata_orphan_blocks_processed_total {}",
            self.orphan_blocks_processed_total.load(Ordering::Relaxed)
        ));

        // GC readiness and state metrics
        metrics.push("# HELP maintenance_gc_ready GC readiness status (1=ready, 0=not ready)".to_string());
        metrics.push(format!(
            "maintenance_gc_ready {}",
            self.gc_ready.load(Ordering::Relaxed)
        ));
        metrics
            .push("# HELP maintenance_gc_refcount_load_fail_total Total number of refcount load failures".to_string());
        metrics.push(format!(
            "maintenance_gc_refcount_load_fail_total {}",
            self.gc_refcount_load_fail_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP maintenance_gc_skipped_cycles_total Total number of GC cycles skipped".to_string());
        metrics.push(format!(
            "maintenance_gc_skipped_cycles_total {}",
            self.gc_skipped_cycles_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP maintenance_gc_candidates Current number of GC candidates".to_string());
        metrics.push(format!(
            "maintenance_gc_candidates {}",
            self.gc_candidates.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP maintenance_gc_swept_total Total number of blocks swept by GC".to_string());
        metrics.push(format!(
            "maintenance_gc_swept_total {}",
            self.gc_swept_total.load(Ordering::Relaxed)
        ));
        metrics
            .push("# HELP maintenance_gc_rebuild_attempt_total Total number of refcount rebuild attempts".to_string());
        metrics.push(format!(
            "maintenance_gc_rebuild_attempt_total {}",
            self.gc_rebuild_attempt_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP maintenance_gc_rebuild_fail_total Total number of refcount rebuild failures".to_string());
        metrics.push(format!(
            "maintenance_gc_rebuild_fail_total {}",
            self.gc_rebuild_fail_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP maintenance_gc_gate_state GC gate state (1=ready, 0=not ready)".to_string());
        metrics.push(format!(
            "maintenance_gc_gate_state {}",
            self.gc_gate_state.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP maintenance_gc_skipped_total Total number of GC cycles skipped".to_string());
        metrics.push(format!(
            "maintenance_gc_skipped_total {}",
            self.gc_skipped_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP maintenance_gc_candidates_total Total number of GC candidates collected".to_string());
        metrics.push(format!(
            "maintenance_gc_candidates_total {}",
            self.gc_candidates_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP maintenance_gc_actions_total Total number of GC destructive actions".to_string());
        metrics.push(format!(
            "maintenance_gc_actions_total {}",
            self.gc_actions_total.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP maintenance_gc_refcount_reload_success_total Total number of successful refcount reloads"
                .to_string(),
        );
        metrics.push(format!(
            "maintenance_gc_refcount_reload_success_total {}",
            self.gc_refcount_reload_success_total.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP maintenance_gc_refcount_reload_fail_total Total number of failed refcount reloads".to_string(),
        );
        metrics.push(format!(
            "maintenance_gc_refcount_reload_fail_total {}",
            self.gc_refcount_reload_fail_total.load(Ordering::Relaxed)
        ));

        // Lease cleanup metrics
        metrics.push("# HELP maintenance_lease_gate_state Lease cleanup gate state (1=ready, 0=not ready)".to_string());
        metrics.push(format!(
            "maintenance_lease_gate_state {}",
            self.lease_gate_state.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP maintenance_lease_cleanup_skipped_total Total number of lease cleanup cycles skipped".to_string(),
        );
        metrics.push(format!(
            "maintenance_lease_cleanup_skipped_total {}",
            self.lease_cleanup_skipped_total.load(Ordering::Relaxed)
        ));
        metrics
            .push("# HELP maintenance_lease_cleanup_actions_total Total number of lease cleanup actions".to_string());
        metrics.push(format!(
            "maintenance_lease_cleanup_actions_total {}",
            self.lease_cleanup_actions_total.load(Ordering::Relaxed)
        ));

        // Orphan cleanup metrics
        metrics
            .push("# HELP maintenance_orphan_gate_state Orphan cleanup gate state (1=ready, 0=not ready)".to_string());
        metrics.push(format!(
            "maintenance_orphan_gate_state {}",
            self.orphan_gate_state.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP maintenance_orphan_cleanup_skipped_total Total number of orphan cleanup cycles skipped".to_string(),
        );
        metrics.push(format!(
            "maintenance_orphan_cleanup_skipped_total {}",
            self.orphan_cleanup_skipped_total.load(Ordering::Relaxed)
        ));
        metrics
            .push("# HELP maintenance_orphan_cleanup_actions_total Total number of orphan cleanup actions".to_string());
        metrics.push(format!(
            "maintenance_orphan_cleanup_actions_total {}",
            self.orphan_cleanup_actions_total.load(Ordering::Relaxed)
        ));

        // Block report convergence metrics
        metrics.push(
            "# HELP maintenance_blockreport_active_workers Number of active workers (heartbeat within TTL)".to_string(),
        );
        metrics.push(format!(
            "maintenance_blockreport_active_workers {}",
            self.maintenance_blockreport_active_workers.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP maintenance_blockreport_full_reported_workers Number of active workers with full report"
                .to_string(),
        );
        metrics.push(format!(
            "maintenance_blockreport_full_reported_workers {}",
            self.maintenance_blockreport_full_reported_workers
                .load(Ordering::Relaxed)
        ));
        metrics.push("# HELP maintenance_blockreport_ratio Block report convergence ratio (ratio * 1000)".to_string());
        metrics.push(format!(
            "maintenance_blockreport_ratio {}",
            self.maintenance_blockreport_ratio.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP maintenance_blockreport_converged Block report convergence status (1=converged, 0=not converged)"
                .to_string(),
        );
        metrics.push(format!(
            "maintenance_blockreport_converged {}",
            self.maintenance_blockreport_converged.load(Ordering::Relaxed)
        ));

        // DeleteIntent metrics
        metrics.push("# HELP delete_intents_created_total Total number of delete intents created".to_string());
        metrics.push(format!(
            "delete_intents_created_total {}",
            self.delete_intents_created_total.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP delete_intents_create_failed_total Total number of failed delete intent creations".to_string(),
        );
        metrics.push(format!(
            "delete_intents_create_failed_total {}",
            self.delete_intents_create_failed_total.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP maintenance_gc_created_intents_total Total number of delete intents created by GC".to_string(),
        );
        metrics.push(format!(
            "maintenance_gc_created_intents_total {}",
            self.maintenance_gc_created_intents_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP delete_intents_completed_total Total number of delete intents completed".to_string());
        metrics.push(format!(
            "delete_intents_completed_total {}",
            self.delete_intents_completed_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP delete_intents_completed_by_ack_only_total Total number of delete intents completed by ack only (reconcile pending)".to_string());
        metrics.push(format!(
            "delete_intents_completed_by_ack_only_total {}",
            self.delete_intents_completed_by_ack_only_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP delete_intents_completed_by_reconcile_total Total number of delete intents completed by reconcile (ack + blockreport)".to_string());
        metrics.push(format!(
            "delete_intents_completed_by_reconcile_total {}",
            self.delete_intents_completed_by_reconcile_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP delete_intents_retry_total Total number of delete intents retried".to_string());
        metrics.push(format!(
            "delete_intents_retry_total {}",
            self.delete_intents_retry_total.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP delete_executor_inflight_total Current number of in-flight delete executor requests".to_string(),
        );
        metrics.push(format!(
            "delete_executor_inflight_total {}",
            self.delete_executor_inflight_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP delete_executor_requests_total Total number of delete executor requests sent".to_string());
        metrics.push(format!(
            "delete_executor_requests_total {}",
            self.delete_executor_requests_total.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP delete_executor_requests_failed_total Total number of failed delete executor requests".to_string(),
        );
        metrics.push(format!(
            "delete_executor_requests_failed_total {}",
            self.delete_executor_requests_failed_total.load(Ordering::Relaxed)
        ));

        // Full report lease metrics
        metrics.push("# HELP full_report_leases_inflight Current number of in-flight full report leases".to_string());
        metrics.push(format!(
            "full_report_leases_inflight {}",
            self.full_report_leases_inflight.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP full_report_leases_available Current number of available full report leases".to_string());
        metrics.push(format!(
            "full_report_leases_available {}",
            self.full_report_leases_available.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP full_report_leases_waiting Estimated number of workers waiting for full report leases".to_string(),
        );
        metrics.push(format!(
            "full_report_leases_waiting {}",
            self.full_report_leases_waiting.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP full_report_granted_total Total number of full report leases granted".to_string());
        metrics.push(format!(
            "full_report_granted_total {}",
            self.full_report_granted_total.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP full_report_throttled_total Total number of full report lease requests throttled".to_string(),
        );
        metrics.push(format!(
            "full_report_throttled_total {}",
            self.full_report_throttled_total.load(Ordering::Relaxed)
        ));

        // Latency metrics
        metrics.push("# HELP metadata_request_latency_us Last request latency in microseconds".to_string());
        metrics.push(format!(
            "metadata_request_latency_us {}",
            self.request_latency_us.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_request_latency_p50_us P50 request latency in microseconds".to_string());
        metrics.push(format!(
            "metadata_request_latency_p50_us {}",
            self.request_latency_p50_us.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_request_latency_p99_us P99 request latency in microseconds".to_string());
        metrics.push(format!(
            "metadata_request_latency_p99_us {}",
            self.request_latency_p99_us.load(Ordering::Relaxed)
        ));

        metrics.push("# HELP metadata_root_ready Root mount readiness status (1=ready, 0=not ready)".to_string());
        metrics.push(format!(
            "metadata_root_ready {}",
            self.root_ready.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_root_wait_attempts Root mount readiness wait attempts".to_string());
        metrics.push(format!(
            "metadata_root_wait_attempts {}",
            self.root_wait_attempts.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_root_wait_elapsed_ms Root mount readiness elapsed time in ms".to_string());
        metrics.push(format!(
            "metadata_root_wait_elapsed_ms {}",
            self.root_wait_elapsed_ms.load(Ordering::Relaxed)
        ));

        // FS write routing metrics
        metrics.push("# HELP metadata_fs_write_routed_total Total number of FS write operations routed to mount namespace owner group".to_string());
        metrics.push(format!(
            "metadata_fs_write_routed_total {}",
            self.fs_write_routed_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_fs_write_cross_mount_rename_exdev_total Total number of cross-mount rename operations rejected with EXDEV".to_string());
        metrics.push(format!(
            "metadata_fs_write_cross_mount_rename_exdev_total {}",
            self.fs_write_cross_mount_rename_exdev_total.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_fs_write_mount_epoch_mismatch_total Total number of FS write operations rejected due to mount epoch mismatch".to_string());
        metrics.push(format!(
            "metadata_fs_write_mount_epoch_mismatch_total {}",
            self.fs_write_mount_epoch_mismatch_total.load(Ordering::Relaxed)
        ));

        // Raft write amplification guardrails
        metrics.push(
            "# HELP metadata_fs_raft_appends_total Total number of FS Raft log appends (all operations)".to_string(),
        );
        metrics.push(format!(
            "metadata_fs_raft_appends_total {}",
            self.fs_raft_appends_total.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP metadata_fs_raft_appends_create Total number of Create operations that wrote to Raft".to_string(),
        );
        metrics.push(format!(
            "metadata_fs_raft_appends_create {}",
            self.fs_raft_appends_create.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP metadata_fs_raft_appends_mkdir Total number of Mkdir operations that wrote to Raft".to_string(),
        );
        metrics.push(format!(
            "metadata_fs_raft_appends_mkdir {}",
            self.fs_raft_appends_mkdir.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP metadata_fs_raft_appends_unlink Total number of Unlink operations that wrote to Raft".to_string(),
        );
        metrics.push(format!(
            "metadata_fs_raft_appends_unlink {}",
            self.fs_raft_appends_unlink.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP metadata_fs_raft_appends_rmdir Total number of Rmdir operations that wrote to Raft".to_string(),
        );
        metrics.push(format!(
            "metadata_fs_raft_appends_rmdir {}",
            self.fs_raft_appends_rmdir.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP metadata_fs_raft_appends_rename Total number of Rename operations that wrote to Raft".to_string(),
        );
        metrics.push(format!(
            "metadata_fs_raft_appends_rename {}",
            self.fs_raft_appends_rename.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP metadata_fs_raft_appends_setattr Total number of SetAttr operations that wrote to Raft".to_string(),
        );
        metrics.push(format!(
            "metadata_fs_raft_appends_setattr {}",
            self.fs_raft_appends_setattr.load(Ordering::Relaxed)
        ));

        // Authz/cache metrics
        metrics.push("# HELP metadata_authz_groups_cache_hit_total Total group cache hits in ACL authz".to_string());
        metrics.push(format!(
            "metadata_authz_groups_cache_hit_total {}",
            AUTHZ_GROUPS_CACHE_HIT_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_authz_groups_cache_miss_total Total group cache misses in ACL authz".to_string());
        metrics.push(format!(
            "metadata_authz_groups_cache_miss_total {}",
            AUTHZ_GROUPS_CACHE_MISS_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP metadata_authz_groups_cache_expiry_total Total group cache expiry events in ACL authz".to_string(),
        );
        metrics.push(format!(
            "metadata_authz_groups_cache_expiry_total {}",
            AUTHZ_GROUPS_CACHE_EXPIRY_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_authz_groups_stale_fallback_used_total Total stale group cache fallbacks used on resolver errors".to_string());
        metrics.push(format!(
            "metadata_authz_groups_stale_fallback_used_total {}",
            AUTHZ_GROUPS_STALE_FALLBACK_USED_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP metadata_authz_groups_resolver_error_total Total group resolver errors observed by ACL authz"
                .to_string(),
        );
        metrics.push(format!(
            "metadata_authz_groups_resolver_error_total {}",
            AUTHZ_GROUPS_RESOLVER_ERROR_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_authz_perm_cache_hit_total Total inode permission cache hits".to_string());
        metrics.push(format!(
            "metadata_authz_perm_cache_hit_total {}",
            AUTHZ_PERM_CACHE_HIT_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_authz_perm_cache_miss_total Total inode permission cache misses".to_string());
        metrics.push(format!(
            "metadata_authz_perm_cache_miss_total {}",
            AUTHZ_PERM_CACHE_MISS_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push(
            "# HELP metadata_authz_perm_cache_invalidate_total Total inode permission cache invalidations".to_string(),
        );
        metrics.push(format!(
            "metadata_authz_perm_cache_invalidate_total {}",
            AUTHZ_PERM_CACHE_INVALIDATE_TOTAL.load(Ordering::Relaxed)
        ));
        metrics
            .push("# HELP metadata_authz_allow_ranger_path_total Total authz allows for RangerPath scheme".to_string());
        metrics.push(format!(
            "metadata_authz_allow_ranger_path_total {}",
            AUTHZ_ALLOW_RANGER_PATH_TOTAL.load(Ordering::Relaxed)
        ));
        metrics
            .push("# HELP metadata_authz_deny_ranger_path_total Total authz denials for RangerPath scheme".to_string());
        metrics.push(format!(
            "metadata_authz_deny_ranger_path_total {}",
            AUTHZ_DENY_RANGER_PATH_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_authz_allow_acl_inode_total Total authz allows for AclInode scheme".to_string());
        metrics.push(format!(
            "metadata_authz_allow_acl_inode_total {}",
            AUTHZ_ALLOW_ACL_INODE_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_authz_deny_acl_inode_total Total authz denials for AclInode scheme".to_string());
        metrics.push(format!(
            "metadata_authz_deny_acl_inode_total {}",
            AUTHZ_DENY_ACL_INODE_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_authz_allow_none_total Total authz allows for None scheme".to_string());
        metrics.push(format!(
            "metadata_authz_allow_none_total {}",
            AUTHZ_ALLOW_NONE_TOTAL.load(Ordering::Relaxed)
        ));
        metrics.push("# HELP metadata_authz_deny_none_total Total authz denials for None scheme".to_string());
        metrics.push(format!(
            "metadata_authz_deny_none_total {}",
            AUTHZ_DENY_NONE_TOTAL.load(Ordering::Relaxed)
        ));

        metrics.join("\n")
    }
}

impl Default for MetadataMetrics {
    fn default() -> Self {
        Self::new()
    }
}
