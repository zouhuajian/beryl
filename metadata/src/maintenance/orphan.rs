// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Orphan block cleaner: processing orphan blocks with grace period.
//!
//! This module implements orphan detection with a pending cache followed by DeleteIntent creation.

use crate::destructive_gate::DestructiveGate;
use crate::error::{MetadataError, MetadataResult};
use crate::metrics::MetadataMetrics;
use crate::mount::MountTable;
use crate::raft::{AppRaftNode, RocksDBStorage};
use crate::state::DeleteIntentReason;
use crate::worker::{OrphanQueue, RepairQueue, WorkerManager};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
use types::ids::{BlockId, WorkerId};

use super::gate::{GateCheckResult, TaskGate};
use super::gc::BLOCKREPORT_CONVERGENCE_THRESHOLD;
use super::intents::DeleteIntentBuilder;

/// Pending orphan block (waiting for grace period or second confirmation).
#[derive(Debug, Clone)]
pub struct PendingOrphan {
    block_id: BlockId,
    worker_id: WorkerId,
    first_seen_ms: u64,
    last_seen_ms: u64,
    seen_count: u32,
}

impl PendingOrphan {
    fn new(block_id: BlockId, worker_id: WorkerId, now_ms: u64) -> Self {
        Self {
            block_id,
            worker_id,
            first_seen_ms: now_ms,
            last_seen_ms: now_ms,
            seen_count: 1,
        }
    }

    fn update(&mut self, now_ms: u64) {
        self.last_seen_ms = now_ms;
        self.seen_count += 1;
    }

    /// Check if orphan is eligible for evict.
    fn is_eligible_for_evict(&self, now_ms: u64) -> bool {
        const GRACE_PERIOD_MS: u64 = 10 * 60 * 1000; // 10 minutes
        const MIN_SEEN_COUNT: u32 = 2;

        (self.seen_count >= MIN_SEEN_COUNT) || (now_ms.saturating_sub(self.first_seen_ms) >= GRACE_PERIOD_MS)
    }
}

/// Orphan block cleaner for processing orphan blocks.
pub struct OrphanBlockCleaner {
    raft_node: Arc<AppRaftNode>,
    storage: Arc<RocksDBStorage>,
    worker_manager: Arc<WorkerManager>,
    repair_queue: Arc<RepairQueue>,
    orphan_queue: Arc<OrphanQueue>,
    orphan_gate: Arc<RwLock<TaskGate>>,
    orphan_pending: Arc<RwLock<HashMap<BlockId, PendingOrphan>>>,
    metrics: Arc<MetadataMetrics>,
    last_log_ms: Arc<RwLock<u64>>,
    /// Unified destructive gate
    destructive_gate: Arc<DestructiveGate>,
    /// Mount table for computing mount_epoch when convergence gating is enabled.
    mount_table: Arc<MountTable>,
}

impl OrphanBlockCleaner {
    /// Create a new OrphanBlockCleaner.
    pub fn new(
        raft_node: Arc<AppRaftNode>,
        storage: Arc<RocksDBStorage>,
        worker_manager: Arc<WorkerManager>,
        repair_queue: Arc<RepairQueue>,
        orphan_queue: Arc<OrphanQueue>,
        orphan_gate: Arc<RwLock<TaskGate>>,
        orphan_pending: Arc<RwLock<HashMap<BlockId, PendingOrphan>>>,
        metrics: Arc<MetadataMetrics>,
        last_log_ms: Arc<RwLock<u64>>,
        destructive_gate: Arc<DestructiveGate>,
        mount_table: Arc<MountTable>,
    ) -> Self {
        Self {
            raft_node,
            storage,
            worker_manager,
            repair_queue,
            orphan_queue,
            orphan_gate,
            orphan_pending,
            metrics,
            last_log_ms,
            destructive_gate,
            mount_table,
        }
    }

    /// Run one cleanup cycle.
    pub async fn run_once(&self) -> MetadataResult<()> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Check gate
        let gate_check = {
            let gate = self.orphan_gate.read();
            gate.check("orphan_cleanup", now_ms)
        };

        // Check if we're leader and state is stable
        let is_leader = self.raft_node.is_leader();
        if !is_leader {
            return Ok(()); // Only run on leader
        }

        let (is_ready, reason_opt) = match &gate_check {
            GateCheckResult::Ready => (true, None),
            GateCheckResult::Degraded { reason, .. } => (false, Some(reason.clone())),
            GateCheckResult::Blocked { reason } => (false, Some(reason.clone())),
        };

        if !is_ready {
            self.metrics
                .orphan_cleanup_skipped_total
                .fetch_add(1, Ordering::Relaxed);
            self.metrics.orphan_gate_state.store(0, Ordering::Relaxed);

            const LOG_INTERVAL_MS: u64 = 5 * 60 * 1000;
            {
                let mut last_log = self.last_log_ms.write();
                if now_ms.saturating_sub(*last_log) >= LOG_INTERVAL_MS {
                    let gate_state = match &gate_check {
                        GateCheckResult::Ready => "ready",
                        GateCheckResult::Degraded { .. } => "degraded",
                        GateCheckResult::Blocked { .. } => "blocked",
                    };
                    error!(
                        task = "orphan_cleanup",
                        gate_state,
                        reason = %reason_opt.as_deref().unwrap_or("unknown"),
                        "Orphan cleanup skipped: gate not ready"
                    );
                    *last_log = now_ms;
                }
            }
            return Ok(());
        }

        // Check block report convergence before evict actions
        let epoch = self.worker_manager.get_metadata_epoch();
        let active_ttl_ms = self.worker_manager.heartbeat_timeout_sec() * 1000;
        let snapshot = self.worker_manager.blockreport_convergence_snapshot(
            now_ms,
            active_ttl_ms,
            epoch,
            BLOCKREPORT_CONVERGENCE_THRESHOLD,
        );

        // Update metrics
        self.metrics
            .maintenance_blockreport_active_workers
            .store(snapshot.active_workers, Ordering::Relaxed);
        self.metrics
            .maintenance_blockreport_full_reported_workers
            .store(snapshot.full_reported_workers, Ordering::Relaxed);
        self.metrics
            .maintenance_blockreport_ratio
            .store((snapshot.ratio * 1000.0) as usize, Ordering::Relaxed);
        self.metrics
            .maintenance_blockreport_converged
            .store(if snapshot.converged { 1 } else { 0 }, Ordering::Relaxed);

        // Hard gate: block report must be converged before evict
        let allow_evict = is_ready && snapshot.converged;

        if !allow_evict {
            self.metrics
                .orphan_cleanup_skipped_total
                .fetch_add(1, Ordering::Relaxed);
            self.metrics.orphan_gate_state.store(0, Ordering::Relaxed);

            const LOG_INTERVAL_MS: u64 = 5 * 60 * 1000;
            {
                let mut last_log = self.last_log_ms.write();
                if now_ms.saturating_sub(*last_log) >= LOG_INTERVAL_MS {
                    let gate_state = match &gate_check {
                        GateCheckResult::Ready => "ready",
                        GateCheckResult::Degraded { .. } => "degraded",
                        GateCheckResult::Blocked { .. } => "blocked",
                    };
                    let reason = if !is_ready {
                        reason_opt.as_deref().unwrap_or("gate_not_ready")
                    } else {
                        "blockreport_not_converged"
                    };
                    warn!(
                        task = "orphan_cleanup",
                        gate_state,
                        reason,
                        active_workers = snapshot.active_workers,
                        full_reported_workers = snapshot.full_reported_workers,
                        ratio = snapshot.ratio,
                        threshold = BLOCKREPORT_CONVERGENCE_THRESHOLD,
                        epoch = epoch,
                        "Orphan evict blocked: gate not ready or block report not converged. Scan/pending allowed."
                    );
                    *last_log = now_ms;
                }
            }
            // Allow scan/pending accumulation, but skip evict
            return Ok(());
        }

        self.metrics.orphan_gate_state.store(1, Ordering::Relaxed);

        // Process orphan blocks with grace period
        self.process_orphan_blocks(now_ms).await?;

        Ok(())
    }

    /// Process orphan blocks from the queue with grace period.
    async fn process_orphan_blocks(&self, now_ms: u64) -> MetadataResult<()> {
        const MAX_ORPHANS_PER_CYCLE: usize = 10;

        // Create intent builder
        let intent_builder = DeleteIntentBuilder::new(Arc::clone(&self.mount_table), Arc::clone(&self.storage));

        for _ in 0..MAX_ORPHANS_PER_CYCLE {
            let (block_id, worker_id) = match self.orphan_queue.dequeue() {
                Some(orphan) => orphan,
                None => break,
            };

            // Check if block exists in metadata (fail-closed: Err must not be swallowed)
            let block_exists = match self
                .raft_node
                .read(false, |sm| {
                    // Fail-closed: return Err if storage read fails
                    sm.get_block(block_id)
                        .map_err(|e| MetadataError::Internal(format!("Failed to read block {}: {}", block_id, e)))
                })
                .await
            {
                Ok(block_opt) => block_opt,
                Err(e) => {
                    // Fail-closed: set gate to degraded and skip this orphan
                    {
                        let mut gate = self.orphan_gate.write();
                        gate.set_degraded("block_read_failed".to_string(), e.to_string(), now_ms);
                    }
                    self.metrics
                        .orphan_cleanup_skipped_total
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        task = "orphan_cleanup",
                        block_id = %block_id,
                        error = %e,
                        "Failed to read block, gate degraded, skipping orphan"
                    );
                    continue;
                }
            };

            if block_exists.is_none() {
                // Block doesn't exist - check pending cache
                let should_evict = {
                    let mut pending = self.orphan_pending.write();
                    if let Some(pending_orphan) = pending.get_mut(&block_id) {
                        // Update existing pending orphan
                        pending_orphan.update(now_ms);
                        pending_orphan.is_eligible_for_evict(now_ms)
                    } else {
                        // First time seeing this orphan
                        pending.insert(block_id, PendingOrphan::new(block_id, worker_id, now_ms));
                        false // Not eligible yet
                    }
                };

                if should_evict {
                    // Eligible for evict - create DeleteIntent instead of direct evict
                    // Check gate: must be leader, block report converged, and gate ready
                    let is_leader = self.raft_node.is_leader();
                    if !is_leader {
                        debug!(
                            task = "orphan_cleanup",
                            block_id = %block_id,
                            "Skipping orphan evict: not leader"
                        );
                        continue;
                    }

                    // Check block report convergence
                    let epoch = self.worker_manager.get_metadata_epoch();
                    let active_ttl_ms = self.worker_manager.heartbeat_timeout_sec() * 1000;
                    let snapshot = self.worker_manager.blockreport_convergence_snapshot(
                        now_ms,
                        active_ttl_ms,
                        epoch,
                        BLOCKREPORT_CONVERGENCE_THRESHOLD,
                    );

                    if !snapshot.converged {
                        debug!(
                            task = "orphan_cleanup",
                            block_id = %block_id,
                            "Skipping orphan evict: block report not converged"
                        );
                        continue;
                    }

                    // Get guard_state_id
                    let guard_state_id = match self.raft_node.get_last_applied_state_id() {
                        Some(sid) => sid,
                        None => {
                            warn!(
                                task = "orphan_cleanup",
                                block_id = %block_id,
                                "Failed to get guard_state_id, skipping"
                            );
                            continue;
                        }
                    };

                    // Create DeleteIntent
                    const GRACE_WINDOW_MS: u64 = 10 * 60 * 1000; // 10 minutes default grace window
                    let not_before_ms = now_ms + GRACE_WINDOW_MS;
                    let intent_id = now_ms * 1_000_000 + (block_id.data_handle_id.as_raw() % 1_000_000);

                    // Build intent using DeleteIntentBuilder
                    match intent_builder.build(
                        intent_id,
                        block_id,
                        DeleteIntentReason::Orphan,
                        now_ms,
                        not_before_ms,
                        guard_state_id,
                        vec![worker_id], // Include target worker for orphan
                    ) {
                        Ok(intent) => {
                            use crate::raft::Command;
                            use types::CallId;
                            let command = Command::CreateDeleteIntents {
                                request_id: CallId::new(),
                                intents: vec![intent],
                            };

                            match self.raft_node.propose(command).await {
                                Ok(_) => {
                                    info!(
                                        task = "orphan_cleanup",
                                        block_id = %block_id,
                                        worker_id = worker_id.as_raw(),
                                        "Created delete intent for orphan block"
                                    );
                                    self.metrics
                                        .orphan_cleanup_actions_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    self.metrics
                                        .delete_intents_created_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    // Remove from pending
                                    self.orphan_pending.write().remove(&block_id);
                                }
                                Err(e) => {
                                    warn!(
                                        task = "orphan_cleanup",
                                        block_id = %block_id,
                                        error = %e,
                                        "Failed to propose CreateDeleteIntents for orphan"
                                    );
                                    self.metrics
                                        .delete_intents_create_failed_total
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(e) => {
                            // Fail-closed: router missing or unable to resolve, skip creating DeleteIntent
                            warn!(
                                task = "orphan_cleanup",
                                block_id = %block_id,
                                error = %e,
                                "Failed to build DeleteIntent, skipping"
                            );
                            self.metrics
                                .orphan_cleanup_skipped_total
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
