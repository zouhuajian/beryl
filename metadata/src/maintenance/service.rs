// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Background maintenance tasks: GC, lease cleanup, and orphan block cleanup.
//!
//! This module implements fail-closed + loud + self-healing maintenance tasks.
//!
//! Note: This module is being refactored. Core services have been extracted to submodules:
//! - maintenance/gate.rs: TaskGate and gate logic
//! - maintenance/gc.rs: GcService
//! - maintenance/orphan.rs: OrphanBlockCleaner
//! - maintenance/lease_cleanup.rs: LeaseCleanupService

use super::gate::TaskGate;
use super::gc::{GcCandidate, GcService};
use super::lease_cleanup::LeaseCleanupService;
use super::lost_worker::{LostWorkerCleanupDeps, LostWorkerCleanupService};
use super::orphan::{OrphanBlockCleaner, PendingOrphan};
use super::repair::{OrphanQueue, RepairPlanner, RepairPolicy, RepairQueue};

use crate::destructive_gate::DestructiveGate;
use crate::inflight_registry::InflightRegistry;
use crate::metrics::MetadataMetrics;
use crate::raft::{AppRaftNode, RocksDBStorage};
use crate::worker::WorkerManager;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use types::ids::{BlockId, DataHandleId};

/// Active worker TTL in milliseconds (default: 3 minutes, or use heartbeat_timeout_sec * 1000).
pub const ACTIVE_TTL_MS: u64 = 180_000;

/// Reference count for blocks (data_handle_id -> block_id -> count).
type BlockRefCounts = HashMap<DataHandleId, HashMap<BlockId, u32>>;

/// Maintenance background task handles.
pub struct MaintenanceHandle {
    tasks: Vec<JoinHandle<()>>,
}

impl MaintenanceHandle {
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }
}

/// Maintenance service for background tasks.
pub struct MaintenanceService {
    raft_node: Arc<AppRaftNode>,
    storage: Arc<RocksDBStorage>,
    worker_manager: Arc<WorkerManager>,
    repair_queue: Arc<RepairQueue>,
    orphan_queue: Arc<OrphanQueue>,
    repair_planner: Arc<RepairPlanner>,
    repair_policy: RepairPolicy,
    block_ref_counts: Arc<RwLock<BlockRefCounts>>,
    metrics: Arc<MetadataMetrics>,
    // Gates for each task
    gc_gate: Arc<RwLock<TaskGate>>,
    lease_gate: Arc<RwLock<TaskGate>>,
    orphan_gate: Arc<RwLock<TaskGate>>,
    // GC candidate cache
    gc_candidates: Arc<RwLock<HashMap<BlockId, GcCandidate>>>,
    // Orphan pending cache
    orphan_pending: Arc<RwLock<HashMap<BlockId, PendingOrphan>>>,
    // Rate limiting for logging
    last_gc_log_ms: Arc<RwLock<u64>>,
    last_lease_log_ms: Arc<RwLock<u64>>,
    last_orphan_log_ms: Arc<RwLock<u64>>,
    // Intervals
    gc_interval_sec: u64,
    lease_cleanup_interval_sec: u64,
    orphan_cleanup_interval_sec: u64,
    lost_worker_cleanup_interval_sec: u64,
    rebalance_interval_sec: u64,
    timeout_check_interval_sec: u64,
    // Self-healing intervals
    refcount_reload_interval_sec: u64,
    // Unified destructive gate and inflight registry
    destructive_gate: Arc<DestructiveGate>,
    inflight_registry: Arc<InflightRegistry>,
    // Mount table for computing mount_epoch during destructive gate checks
    mount_table: Arc<crate::mount::MountTable>,
}

impl MaintenanceService {
    /// Create a new maintenance service.
    // Constructor mirrors maintenance runtime wiring; grouping dependencies would hide ownership.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        raft_node: Arc<AppRaftNode>,
        storage: Arc<RocksDBStorage>,
        worker_manager: Arc<WorkerManager>,
        repair_queue: Arc<RepairQueue>,
        orphan_queue: Arc<OrphanQueue>,
        repair_planner: Arc<RepairPlanner>,
        metrics: Arc<MetadataMetrics>,
        mount_table: Arc<crate::mount::MountTable>,
    ) -> Self {
        Self::new_with_inflight_registry(
            raft_node,
            storage,
            worker_manager,
            repair_queue,
            orphan_queue,
            repair_planner,
            metrics,
            None, // Will create default if None
            mount_table,
            RepairPolicy::default(),
        )
    }

    /// Create a new maintenance service with optional shared inflight registry.
    // Constructor mirrors maintenance runtime wiring; grouping dependencies would hide ownership.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_inflight_registry(
        raft_node: Arc<AppRaftNode>,
        storage: Arc<RocksDBStorage>,
        worker_manager: Arc<WorkerManager>,
        repair_queue: Arc<RepairQueue>,
        orphan_queue: Arc<OrphanQueue>,
        repair_planner: Arc<RepairPlanner>,
        metrics: Arc<MetadataMetrics>,
        inflight_registry: Option<Arc<InflightRegistry>>,
        mount_table: Arc<crate::mount::MountTable>,
        repair_policy: RepairPolicy,
    ) -> Self {
        // Create unified destructive gate
        let mount_table_for_gate = Arc::clone(&mount_table);
        let destructive_gate = Arc::new(DestructiveGate::new(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            mount_table_for_gate,
        ));

        // Use provided inflight registry or create default (default TTL: 5 minutes)
        let inflight_registry = inflight_registry.unwrap_or_else(|| Arc::new(InflightRegistry::new(5 * 60 * 1000)));

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Initialize gates
        let mut gc_gate = TaskGate::new();
        let lease_gate = TaskGate::new();
        let orphan_gate = TaskGate::new();

        // Load block reference counts from storage
        let mut block_ref_counts = HashMap::new();
        match storage.get_all_block_ref_counts() {
            Ok(ref_counts) => {
                // Preserve the legacy in-memory view from the global block refcount
                // store. Authoritative file-layout mutations update refcounts in
                // Raft apply batches; this cache must not be treated as authority.
                for (block_id, count) in ref_counts {
                    let data_handle_id = block_id.data_handle_id;
                    let file_refs = block_ref_counts.entry(data_handle_id).or_insert_with(HashMap::new);
                    file_refs.insert(block_id, count as u32);
                }
                info!(
                    count = block_ref_counts.len(),
                    "Loaded block reference counts from storage"
                );
                gc_gate.set_ready(now_ms);
            }
            Err(e) => {
                error!(
                    error = %e,
                    "CRITICAL: Failed to load block reference counts. GC will be degraded."
                );
                metrics.gc_refcount_load_fail_total.fetch_add(1, Ordering::Relaxed);
                gc_gate.set_degraded("refcount_load_failed".to_string(), e.to_string(), now_ms);
            }
        }

        Self {
            raft_node,
            storage,
            worker_manager,
            repair_queue,
            orphan_queue,
            repair_planner,
            repair_policy,
            block_ref_counts: Arc::new(RwLock::new(block_ref_counts)),
            metrics,
            gc_gate: Arc::new(RwLock::new(gc_gate)),
            lease_gate: Arc::new(RwLock::new(lease_gate)),
            orphan_gate: Arc::new(RwLock::new(orphan_gate)),
            gc_candidates: Arc::new(RwLock::new(HashMap::new())),
            orphan_pending: Arc::new(RwLock::new(HashMap::new())),
            last_gc_log_ms: Arc::new(RwLock::new(0)),
            last_lease_log_ms: Arc::new(RwLock::new(0)),
            last_orphan_log_ms: Arc::new(RwLock::new(0)),
            gc_interval_sec: 300,            // 5 minutes
            lease_cleanup_interval_sec: 60,  // 1 minute
            orphan_cleanup_interval_sec: 10, // 10 seconds
            lost_worker_cleanup_interval_sec: 30,
            rebalance_interval_sec: 300,      // 5 minutes
            timeout_check_interval_sec: 10,   // 10 seconds
            refcount_reload_interval_sec: 30, // 30 seconds for self-healing
            destructive_gate,
            inflight_registry,
            mount_table,
        }
    }

    /// Start all background maintenance tasks.
    pub fn start(&self) -> MaintenanceHandle {
        let mut tasks = Vec::with_capacity(8);

        // Start GC task with self-healing
        {
            let gc_service = Arc::new(GcService::new(
                Arc::clone(&self.raft_node),
                Arc::clone(&self.storage),
                Arc::clone(&self.worker_manager),
                Arc::clone(&self.block_ref_counts),
                Arc::clone(&self.gc_gate),
                Arc::clone(&self.metrics),
                Arc::clone(&self.gc_candidates),
                Arc::clone(&self.last_gc_log_ms),
                Arc::clone(&self.destructive_gate),
                Arc::clone(&self.inflight_registry),
                Arc::clone(&self.mount_table),
            ));

            // GC task
            let gc = Arc::clone(&gc_service);
            let raft_node = Arc::clone(&self.raft_node);
            let interval_sec = self.gc_interval_sec;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
                loop {
                    interval.tick().await;
                    if raft_node.is_leader() {
                        if let Err(e) = gc.run_gc().await {
                            error!(task = "gc", error = %e, "GC task failed");
                        }
                    }
                }
            }));

            // GC self-healing: reload refcounts
            let gc_reload = Arc::clone(&gc_service);
            let storage = Arc::clone(&self.storage);
            let block_ref_counts = Arc::clone(&self.block_ref_counts);
            let gc_gate = Arc::clone(&self.gc_gate);
            let metrics = Arc::clone(&self.metrics);
            let reload_interval = self.refcount_reload_interval_sec;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(reload_interval));
                loop {
                    interval.tick().await;
                    if let Err(e) = gc_reload
                        .reload_refcounts(&storage, &block_ref_counts, &gc_gate, &metrics)
                        .await
                    {
                        debug!(error = %e, "GC refcount reload attempt failed");
                    }
                }
            }));
        }

        // Start lease cleanup task with self-healing
        {
            let lease_service = Arc::new(LeaseCleanupService::new(
                Arc::clone(&self.raft_node),
                Arc::clone(&self.storage),
                Arc::clone(&self.worker_manager),
                Arc::clone(&self.lease_gate),
                Arc::clone(&self.metrics),
                Arc::clone(&self.last_lease_log_ms),
                Arc::new(RwLock::new(HashMap::new())),
                Arc::clone(&self.destructive_gate),
            ));

            let lease = Arc::clone(&lease_service);
            let raft_node = Arc::clone(&self.raft_node);
            let interval_sec = self.lease_cleanup_interval_sec;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
                loop {
                    interval.tick().await;
                    if raft_node.is_leader() {
                        if let Err(e) = lease.cleanup_expired_leases().await {
                            error!(task = "lease_cleanup", error = %e, "Lease cleanup task failed");
                        }
                    }
                }
            }));
        }

        // Start orphan cleanup task
        {
            let orphan_service = Arc::new(OrphanBlockCleaner::new(
                Arc::clone(&self.raft_node),
                Arc::clone(&self.storage),
                Arc::clone(&self.worker_manager),
                Arc::clone(&self.orphan_queue),
                Arc::clone(&self.orphan_gate),
                Arc::clone(&self.orphan_pending),
                Arc::clone(&self.metrics),
                Arc::clone(&self.last_orphan_log_ms),
                Arc::clone(&self.destructive_gate),
                Arc::clone(&self.mount_table),
            ));

            let orphan = Arc::clone(&orphan_service);
            let raft_node = Arc::clone(&self.raft_node);
            let interval_sec = self.orphan_cleanup_interval_sec;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
                loop {
                    interval.tick().await;
                    if raft_node.is_leader() {
                        if let Err(e) = orphan.run_once().await {
                            error!(task = "orphan_cleanup", error = %e, "Orphan cleanup task failed");
                        }
                    }
                }
            }));
        }

        // Start lost-worker cleanup task.
        {
            let lost_worker_service = Arc::new(LostWorkerCleanupService::new(LostWorkerCleanupDeps {
                raft_node: Arc::clone(&self.raft_node),
                worker_manager: Arc::clone(&self.worker_manager),
                repair_queue: Arc::clone(&self.repair_queue),
                repair_planner: Arc::clone(&self.repair_planner),
                repair_policy: self.repair_policy,
            }));

            let lost_worker = Arc::clone(&lost_worker_service);
            let interval_sec = self.lost_worker_cleanup_interval_sec;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
                loop {
                    interval.tick().await;
                    if let Err(e) = lost_worker.run_once().await {
                        error!(task = "lost_worker_cleanup", error = %e, "Lost-worker cleanup task failed");
                    }
                }
            }));
        }

        // Start rebalance task
        {
            let repair_planner: Arc<RepairPlanner> = Arc::clone(&self.repair_planner);
            let repair_queue: Arc<RepairQueue> = Arc::clone(&self.repair_queue);
            let worker_manager = Arc::clone(&self.worker_manager);
            let raft_node = Arc::clone(&self.raft_node);
            let interval_sec = self.rebalance_interval_sec;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
                loop {
                    interval.tick().await;
                    if raft_node.is_leader() {
                        let actions = repair_planner.plan_rebalance(&worker_manager);
                        for action in actions {
                            let block_id = action.block_id();
                            let task = action.into_task();
                            if let Err(e) = repair_queue.enqueue(task) {
                                error!(block_id = %block_id, error = %e, "Failed to enqueue rebalance task");
                            }
                        }
                    }
                }
            }));
        }

        // Start timeout requeue task
        {
            let repair_queue = Arc::clone(&self.repair_queue);
            let interval_sec = self.timeout_check_interval_sec;
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
                loop {
                    interval.tick().await;
                    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                    let timeout_count = repair_queue.requeue_timeouts(now_ms);
                    if timeout_count > 0 {
                        info!(timeout_count, "Requeued timed-out repair tasks");
                    }
                }
            }));
        }

        // Start over-replication cleanup task
        {
            use super::overrep::OverReplicaCleanupService;
            let overrep_candidates = Arc::new(RwLock::new(HashMap::new()));
            let overrep_service = Arc::new(OverReplicaCleanupService::new(
                Arc::clone(&self.raft_node),
                Arc::clone(&self.storage),
                Arc::clone(&self.worker_manager),
                Arc::clone(&self.metrics),
                Arc::clone(&overrep_candidates),
                Arc::clone(&self.destructive_gate),
                Arc::clone(&self.inflight_registry),
                Arc::clone(&self.mount_table),
                self.repair_policy,
            ));

            let overrep = Arc::clone(&overrep_service);
            let raft_node = Arc::clone(&self.raft_node);
            let interval_sec = 300; // 5 minutes (same as GC)
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
                loop {
                    interval.tick().await;
                    if raft_node.is_leader() {
                        if let Err(e) = overrep.run_once().await {
                            error!(task = "overrep_cleanup", error = %e, "OverRep cleanup task failed");
                        }
                    }
                }
            }));
        }

        info!("Maintenance service started with fail-closed gates");

        MaintenanceHandle { tasks }
    }
}

// GcService has been moved to maintenance/gc.rs

// LeaseCleanupService has been moved to maintenance/lease_cleanup.rs

// OrphanBlockCleaner has been moved to maintenance/orphan.rs
