// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for GC convergence: intent processing and deduplication.

use metadata::raft::RocksDBStorage;
use metadata::state::{DeleteIntent, DeleteIntentReason, DeleteIntentStatus};
use metadata::worker::{RepairQueue, RepairTask};
use std::sync::Arc;
use tempfile::TempDir;
use types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};

#[test]
fn test_gc_dedup_single_inject() {
    let temp_dir = TempDir::new().unwrap();
    let _storage = Arc::new(RocksDBStorage::open(temp_dir.path()).unwrap());
    let repair_queue = Arc::new(RepairQueue::with_config(1000, 3, 60_000, 1_000, 60_000, 10));

    let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));
    let target_worker = WorkerId::new(1);

    // Create first Evict task
    let task1 = RepairTask::Evict {
        target_worker,
        block_id,
        reason: "GC: test".to_string(),
    };
    let task_id1 = repair_queue.enqueue(task1).unwrap();

    // Try to enqueue same task again (same block_id + target_worker)
    let task2 = RepairTask::Evict {
        target_worker,
        block_id,
        reason: "GC: test2".to_string(),
    };
    let task_id2 = repair_queue.enqueue(task2).unwrap();

    // Should return same task_id (dedup)
    assert_eq!(task_id1, task_id2);

    // Check that only one task exists
    assert_eq!(repair_queue.len_pending(), 1);
}

#[test]
fn test_gc_skip_inflight() {
    let temp_dir = TempDir::new().unwrap();
    let _storage = Arc::new(RocksDBStorage::open(temp_dir.path()).unwrap());
    let repair_queue = Arc::new(RepairQueue::with_config(1000, 3, 60_000, 1_000, 60_000, 10));

    let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));
    let target_worker = WorkerId::new(1);

    // Create and enqueue Evict task
    let task = RepairTask::Evict {
        target_worker,
        block_id,
        reason: "GC: test".to_string(),
    };
    repair_queue.enqueue(task).unwrap();

    // Check that block is marked as in-flight
    assert!(repair_queue.is_block_inflight(block_id));

    // Try to enqueue another task for same block (different worker should be allowed)
    let target_worker2 = WorkerId::new(2);
    let task2 = RepairTask::Evict {
        target_worker: target_worker2,
        block_id,
        reason: "GC: test2".to_string(),
    };
    // This should succeed (different target_worker = different dedup key)
    let task_id2 = repair_queue.enqueue(task2).unwrap();
    assert_ne!(task_id2.0, 0);
}

#[test]
fn test_gc_intent_refcount_double_check() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(temp_dir.path()).unwrap());

    let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));

    // Create intent with refcount > 0
    storage.put_block_ref_count(block_id, 1).unwrap();
    let intent_id = 1;
    let intent = DeleteIntent {
        intent_id,
        block_id,
        reason: DeleteIntentReason::Gc,
        created_at_ms: 0,
        not_before_ms: 0,
        shard_group_id: None,
        guard_watermark: None,
        mount_epoch: None,
        guard_state_id: types::RaftLogId {
            term: 0,
            leader_node_id: 0,
            index: 0,
        },
        target_workers: Vec::new(),
        status: DeleteIntentStatus::Pending,
        finished_at_ms: None,
        last_error_msg: None,
    };
    storage.put_delete_intent(&intent).unwrap();

    // Verify intent is pending
    let pending = storage.list_pending_delete_intents(10, 0).unwrap();
    assert_eq!(pending.len(), 1);

    // Verify refcount > 0 (should skip processing)
    assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
}
