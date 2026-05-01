// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Repair planner: converts system state anomalies into repair actions.
//!
//! RepairPlanner is a **pure planning layer**:
//! - It does NOT execute repair tasks (execution happens via worker heartbeat pull)
//! - It does NOT hold locks or write to storage
//! - It does NOT call raft
//! - It outputs RepairAction, which can be converted to RepairTask and enqueued by callers
//!
//! This separation allows:
//! - Easy testing of planning logic (pure functions)
//! - Flexible execution strategies (batch enqueue, rate limiting, etc.)
//! - Clear extension points for new action types (e.g., EvictReplica for excess replicas)

use super::actions::RepairAction;
use super::orphan::OrphanQueue;
use super::queue::RepairQueue;
use crate::error::MetadataResult;
use std::sync::Arc;
use tracing::info;
use types::ids::{BlockId, WorkerId};

/// Repair planner for converting system state anomalies into repair actions.
///
/// # Responsibilities
///
/// RepairPlanner is a **decision layer**, not an execution layer:
/// - It outputs RepairAction (planning), not RepairTask (execution)
/// - Callers are responsible for converting actions to tasks and enqueuing
/// - It does NOT interact with RepairQueue directly
/// - It does NOT interact with workers directly
///
/// # Inputs
///
/// - Replication factor checks (current locations vs target)
/// - Rebalance decisions (worker load analysis)
/// - orphan_queue: suspected orphan blocks (signals, not actions)
///
/// # Outputs
///
/// - RepairAction::Replicate (when replication factor not met)
/// - RepairAction::MoveCopy (when rebalancing needed)
/// - RepairAction::EvictReplica (for excess replicas - future extension)
///
/// Note: Evict tasks for orphan blocks are created by MaintenanceService after
/// secondary confirmation, not by RepairPlanner.
pub struct RepairPlanner {
    /// Orphan queue stores *suspected* orphan blocks reported by workers.
    ///
    /// # Role
    ///
    /// Items in this queue are **signals**, not repair actions:
    /// - Workers report blocks that exist on disk but not in metadata
    /// - These are candidates for cleanup, but require confirmation
    /// - This queue serves as input to repair decision logic
    ///
    /// # Processing Flow
    ///
    /// 1. Workers report suspected orphan blocks → added to orphan_queue
    /// 2. MaintenanceService periodically processes orphan_queue
    /// 3. For each (block_id, worker_id):
    ///    - Secondary confirmation: query metadata state again
    ///    - If confirmed orphan: enqueue RepairTask::Evict
    ///    - If false positive: discard
    ///
    /// RepairPlanner does NOT directly create Evict tasks from orphan_queue.
    /// The final decision and task creation happens in MaintenanceService.
    _orphan_queue: Arc<OrphanQueue>,
}

impl RepairPlanner {
    pub fn new(_repair_queue: Arc<RepairQueue>, orphan_queue: Arc<OrphanQueue>) -> Self {
        // Note: repair_queue parameter kept for backward compatibility but not used
        // Planner now outputs actions instead of enqueuing directly
        Self {
            _orphan_queue: orphan_queue,
        }
    }

    /// Plan replication actions for a block (pure planning, no side effects).
    ///
    /// Returns a list of RepairAction::Replicate actions that should be executed
    /// to meet the replication factor.
    ///
    /// # Leader-only
    /// This method should only be called by the leader node. Repair actions are only
    /// processed by the leader. Follower nodes should not call this.
    pub fn plan_replication(
        &self,
        block_id: BlockId,
        current_locations: &[WorkerId],
        replication_factor: u8,
        available_workers: &[WorkerId],
    ) -> Vec<RepairAction> {
        let current_count = current_locations.len() as u8;

        if current_count < replication_factor {
            // Under-replicated: need to add replicas
            let needed = replication_factor - current_count;

            // Select target workers (simple: pick from available, excluding current)
            let current_set: std::collections::HashSet<WorkerId> = current_locations.iter().copied().collect();
            let candidates: Vec<WorkerId> = available_workers
                .iter()
                .filter(|w| !current_set.contains(w))
                .take(needed as usize)
                .copied()
                .collect();

            // Create one action per target worker
            candidates
                .into_iter()
                .map(|target_worker| RepairAction::Replicate {
                    block_id,
                    src_workers: current_locations.to_vec(),
                    target_worker,
                    replication_factor: Some(replication_factor),
                    reason: Some(format!(
                        "Replication factor {} not met (current: {})",
                        replication_factor, current_count
                    )),
                })
                .collect()
        } else if current_count > replication_factor {
            // Over-replicated: need to remove excess replicas
            let excess = current_count - replication_factor;

            // Select workers to evict (simple: pick from current locations)
            // TODO: Add fault domain awareness and hotness-based selection
            let workers_to_evict: Vec<WorkerId> = current_locations.iter().take(excess as usize).copied().collect();

            // Create one EvictReplica action per excess worker
            workers_to_evict
                .into_iter()
                .map(|target_worker| RepairAction::EvictReplica {
                    block_id,
                    target_worker,
                    reason: format!(
                        "Excess replica removal: current={}, desired={}",
                        current_count, replication_factor
                    ),
                })
                .collect()
        } else {
            // Perfect replication: no action needed
            Vec::new()
        }
    }

    /// Plan rebalancing actions based on worker load (pure planning, no side effects).
    ///
    /// Returns a list of RepairAction::MoveCopy actions that should be executed
    /// to balance worker load.
    pub fn plan_rebalance(&self, worker_manager: &crate::worker::WorkerManager) -> Vec<RepairAction> {
        // Get all live workers with their load information
        let live_workers = worker_manager.list_live_workers();

        // Calculate load metrics for each worker
        let mut worker_loads: Vec<(WorkerId, f64)> = live_workers
            .iter()
            .filter_map(|&id| {
                worker_manager.get_worker(id).map(|w| {
                    // Calculate load as: (capacity_used / capacity_total) * 100
                    let capacity_ratio = if w.capacity_total > 0 {
                        w.capacity_used as f64 / w.capacity_total as f64
                    } else {
                        0.0
                    };
                    // Also factor in active I/O operations
                    let io_load = (w.active_reads + w.active_writes) as f64 / 1000.0; // Normalize
                    let total_load = capacity_ratio * 0.7 + io_load * 0.3; // Weighted
                    (id, total_load)
                })
            })
            .collect();

        if worker_loads.len() < 2 {
            // Need at least 2 workers for rebalancing
            return Vec::new();
        }

        // Sort by load
        worker_loads.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Find overloaded (top 20%) and underloaded (bottom 20%)
        let threshold_high = 0.8; // 80% load threshold
        let threshold_low = 0.3; // 30% load threshold

        let overloaded: Vec<(WorkerId, f64)> = worker_loads
            .iter()
            .rev()
            .take((worker_loads.len() as f64 * 0.2).ceil() as usize)
            .filter(|(_, load)| *load > threshold_high)
            .map(|(id, load)| (*id, *load))
            .collect();

        let underloaded: Vec<(WorkerId, f64)> = worker_loads
            .iter()
            .take((worker_loads.len() as f64 * 0.2).ceil() as usize)
            .filter(|(_, load)| *load < threshold_low)
            .map(|(id, load)| (*id, *load))
            .collect();

        if overloaded.is_empty() || underloaded.is_empty() {
            return Vec::new();
        }

        // Create move actions: move blocks from overloaded to underloaded workers
        // Limit to 10 rebalance actions per cycle to avoid overwhelming the system
        const MAX_REBALANCE_ACTIONS: usize = 10;
        let mut action_count = 0;
        let mut actions = Vec::new();

        for (from_worker, _) in &overloaded {
            if action_count >= MAX_REBALANCE_ACTIONS {
                break;
            }

            // Get blocks on this worker
            let blocks = worker_manager.get_worker_blocks(*from_worker);

            // Select a few blocks to move (prefer smaller blocks or less frequently accessed)
            // For simplicity, we'll move up to 5 blocks per overloaded worker
            let blocks_to_move: Vec<BlockId> = blocks.iter().take(5).copied().collect();

            // Select target worker (round-robin from underloaded)
            for (idx, block_id) in blocks_to_move.iter().enumerate() {
                if action_count >= MAX_REBALANCE_ACTIONS {
                    break;
                }

                let to_worker = underloaded[idx % underloaded.len()].0;

                actions.push(RepairAction::MoveCopy {
                    block_id: *block_id,
                    from_worker: *from_worker,
                    to_worker,
                });

                action_count += 1;
            }
        }

        actions
    }

    // Backward compatibility methods: these enqueue actions automatically
    // TODO: Remove these after migrating all callers to use plan_* methods

    /// Check replication and enqueue repair tasks (backward compatibility).
    ///
    /// # Deprecated
    /// Use `plan_replication()` instead and enqueue actions manually.
    #[deprecated(note = "Use plan_replication() instead")]
    pub fn check_replication(
        &self,
        block_id: BlockId,
        current_locations: &[WorkerId],
        replication_factor: u8,
        available_workers: &[WorkerId],
        repair_queue: &RepairQueue,
    ) -> MetadataResult<()> {
        let actions = self.plan_replication(block_id, current_locations, replication_factor, available_workers);

        for action in actions {
            let task = action.to_task();
            if let Err(e) = repair_queue.enqueue(task) {
                tracing::warn!(
                    block_id = %block_id,
                    error = %e,
                    "Failed to enqueue replication task"
                );
            } else {
                info!(
                    block_id = %block_id,
                    current_replicas = current_locations.len(),
                    target_replicas = replication_factor,
                    "Enqueued replication task"
                );
            }
        }

        Ok(())
    }

    /// Check load balance and enqueue rebalancing tasks (backward compatibility).
    ///
    /// # Deprecated
    /// Use `plan_rebalance()` instead and enqueue actions manually.
    #[deprecated(note = "Use plan_rebalance() instead")]
    pub fn check_rebalance(
        &self,
        worker_manager: &crate::worker::WorkerManager,
        repair_queue: &RepairQueue,
    ) -> MetadataResult<()> {
        let actions = self.plan_rebalance(worker_manager);

        for action in actions {
            let block_id = action.block_id();
            let task = action.to_task();
            if let Err(e) = repair_queue.enqueue(task) {
                tracing::warn!(
                    block_id = %block_id,
                    error = %e,
                    "Failed to enqueue rebalance task"
                );
            } else {
                info!(
                    block_id = %block_id,
                    "Enqueued rebalance task"
                );
            }
        }

        Ok(())
    }
}
