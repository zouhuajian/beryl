// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for repair replica eviction queue behavior.

use metadata::maintenance::repair::{RepairQueue, RepairTask};
use std::sync::Arc;
use types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};

#[test]
fn test_replica_eviction_dedup_returns_existing_task_id() {
    let repair_queue = Arc::new(RepairQueue::with_config(1000, 3, 60_000, 1_000, 60_000, 10));

    let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));
    let target_worker = WorkerId::new(1);

    // Create first replica eviction task
    let task1 = RepairTask::EvictReplica {
        target_worker,
        block_id,
        reason: "excess replica cleanup".to_string(),
    };
    let task_id1 = repair_queue.enqueue(task1).unwrap();

    // Try to enqueue same task again (same block_id + target_worker)
    let task2 = RepairTask::EvictReplica {
        target_worker,
        block_id,
        reason: "move follow-up duplicate".to_string(),
    };
    let task_id2 = repair_queue.enqueue(task2).unwrap();

    // Should return same task_id (dedup)
    assert_eq!(task_id1, task_id2);

    // Check that only one task exists
    assert_eq!(repair_queue.len_pending(), 1);
}

#[test]
fn test_replica_eviction_same_block_different_worker_has_distinct_task() {
    let repair_queue = Arc::new(RepairQueue::with_config(1000, 3, 60_000, 1_000, 60_000, 10));

    let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));
    let target_worker = WorkerId::new(1);

    // Create and enqueue replica eviction task
    let task = RepairTask::EvictReplica {
        target_worker,
        block_id,
        reason: "excess replica cleanup".to_string(),
    };
    repair_queue.enqueue(task).unwrap();

    // Try to enqueue another task for same block (different worker should be allowed)
    let target_worker2 = WorkerId::new(2);
    let task2 = RepairTask::EvictReplica {
        target_worker: target_worker2,
        block_id,
        reason: "different target replica cleanup".to_string(),
    };
    // This should succeed (different target_worker = different dedup key)
    let task_id2 = repair_queue.enqueue(task2).unwrap();
    assert_ne!(task_id2.0, 0);
    assert_eq!(repair_queue.len_pending(), 2);
}
