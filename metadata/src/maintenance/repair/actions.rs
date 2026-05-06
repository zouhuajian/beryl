// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Repair actions: high-level repair operations that can be planned by RepairPlanner.
//!
//! RepairAction represents the "what to do" (planning layer), while RepairTask represents
//! the "how to execute" (execution layer). This separation allows:
//! - Planner to output actions without knowing about queue implementation
//! - Queue to handle task lifecycle without knowing about planning logic
//! - Easy extension for new action types (e.g., EvictReplica for excess replicas)

use super::types::RepairTask;
use types::ids::{BlockId, WorkerId};

/// Repair action: high-level operation planned by RepairPlanner.
///
/// Actions are converted to RepairTasks before being enqueued.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepairAction {
    /// Replicate block to a target worker.
    Replicate {
        block_id: BlockId,
        src_workers: Vec<WorkerId>,
        target_worker: WorkerId,
        replication_factor: Option<u8>,
        reason: Option<String>,
    },
    /// Move copy: copy block from source to target worker (copy then delete).
    MoveCopy {
        block_id: BlockId,
        from_worker: WorkerId,
        to_worker: WorkerId,
    },
    /// Evict an excess or move follow-up replica from a worker.
    EvictReplica {
        block_id: BlockId,
        target_worker: WorkerId,
        reason: String,
    },
}

impl RepairAction {
    /// Convert action to RepairTask for execution.
    pub fn into_task(self) -> RepairTask {
        match self {
            RepairAction::Replicate {
                block_id,
                src_workers,
                target_worker,
                replication_factor,
                reason,
            } => RepairTask::Replicate {
                block_id,
                src_workers,
                target_worker,
                replication_factor,
                reason,
            },
            RepairAction::MoveCopy {
                block_id,
                from_worker,
                to_worker,
            } => RepairTask::MoveCopy {
                block_id,
                from_worker,
                to_worker,
            },
            RepairAction::EvictReplica {
                block_id,
                target_worker,
                reason,
            } => RepairTask::EvictReplica {
                block_id,
                target_worker,
                reason,
            },
        }
    }

    /// Get block_id for this action.
    pub fn block_id(&self) -> BlockId {
        match self {
            RepairAction::Replicate { block_id, .. }
            | RepairAction::MoveCopy { block_id, .. }
            | RepairAction::EvictReplica { block_id, .. } => *block_id,
        }
    }

    /// Get target worker for this action (if applicable).
    pub fn target_worker(&self) -> Option<WorkerId> {
        match self {
            RepairAction::Replicate { target_worker, .. } => Some(*target_worker),
            RepairAction::MoveCopy { to_worker, .. } => Some(*to_worker),
            RepairAction::EvictReplica { target_worker, .. } => Some(*target_worker),
        }
    }
}
