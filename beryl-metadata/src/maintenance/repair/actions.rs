// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Repair actions: high-level repair operations that can be planned by RepairPlanner.
//!
//! RepairAction represents the "what to do" (planning layer), while RepairTask represents
//! the "how to execute" (execution layer). This separation allows:
//! - Planner to output actions without knowing about queue implementation
//! - Queue to handle task lifecycle without knowing about planning logic
//! - Easy extension for new action types (e.g., EvictReplica for excess replicas)

use super::types::RepairTask;
use beryl_types::ids::{BlockId, WorkerId};

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
    /// Evict an excess replica from a worker.
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
            RepairAction::Replicate { block_id, .. } | RepairAction::EvictReplica { block_id, .. } => *block_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};

    fn make_block_id(data_handle_id: u64, index: u32) -> BlockId {
        BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(index))
    }

    fn make_worker_id(id: u64) -> WorkerId {
        WorkerId::new(id)
    }

    #[test]
    fn test_action_into_task() {
        let block_id = make_block_id(1, 0);
        let target_worker = make_worker_id(2);
        let src_workers = vec![make_worker_id(1)];

        let action = RepairAction::Replicate {
            block_id,
            src_workers: src_workers.clone(),
            target_worker,
            replication_factor: Some(3),
            reason: Some("Test".to_string()),
        };

        let task = action.into_task();
        match task {
            RepairTask::Replicate {
                block_id: bid,
                src_workers: sw,
                target_worker: tw,
                replication_factor: rf,
                reason: r,
            } => {
                assert_eq!(bid, block_id);
                assert_eq!(sw, src_workers);
                assert_eq!(tw, target_worker);
                assert_eq!(rf, Some(3));
                assert_eq!(r, Some("Test".to_string()));
            }
            _ => panic!("Expected Replicate task"),
        }
    }
}
