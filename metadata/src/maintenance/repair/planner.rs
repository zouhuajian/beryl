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
use std::sync::Arc;
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
/// - RepairAction::EvictReplica (for excess replicas)
///
/// Orphan/GC physical deletion is represented by DeleteIntent and consumed by
/// DeleteExecutor, not by RepairPlanner.
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
    /// RepairPlanner does NOT directly create delete work from orphan_queue.
    /// Confirmed orphan cleanup is converted to DeleteIntent by maintenance.
    _orphan_queue: Arc<OrphanQueue>,
}

impl RepairPlanner {
    pub fn new(orphan_queue: Arc<OrphanQueue>) -> Self {
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

    /// Plan rebalancing actions based on worker load.
    pub fn plan_rebalance(&self, _worker_manager: &crate::worker::WorkerManager) -> Vec<RepairAction> {
        // Rebalance planning currently emits no copy actions because source
        // and target routing must carry authoritative group identity.
        Vec::new()
    }
}
