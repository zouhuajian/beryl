// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Repair module: queue, planner, and orphan management for block repair operations.
//!
//! This module is organized as follows:
//! - `types.rs`: Core types (RepairTaskId, RepairTask, RepairTaskState, etc.)
//! - `actions.rs`: RepairAction enum (planning layer output)
//! - `queue.rs`: RepairQueue (state machine, deduplication, retry)
//! - `planner.rs`: RepairPlanner (pure planning logic)
//! - `orphan.rs`: OrphanQueue (orphan block tracking)

mod actions;
mod orphan;
mod planner;
mod queue;
mod types;

// Re-export public types for backward compatibility
#[allow(unused_imports)]
pub use actions::RepairAction;
pub use orphan::{OrphanMetrics, OrphanQueue};
pub use planner::RepairPlanner;
pub use queue::RepairQueue;
#[allow(unused_imports)]
pub use types::{
    ErrorClass, RepairDedupKey, RepairTask, RepairTaskId, RepairTaskRecord, RepairTaskState, TaskAckStatus,
};

#[cfg(test)]
mod tests {
    use super::*;
    use ::types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};
    use std::sync::Arc;

    fn make_block_id(data_handle_id: u64, index: u32) -> BlockId {
        BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(index))
    }

    fn make_worker_id(id: u64) -> WorkerId {
        WorkerId::new(id)
    }

    #[test]
    fn test_enqueue_dedup_replicate() {
        let queue = RepairQueue::new(1000);
        let block_id = make_block_id(1, 0);
        let target_worker = make_worker_id(1);

        let task1 = RepairTask::Replicate {
            block_id,
            src_workers: vec![],
            target_worker,
            replication_factor: Some(3),
            reason: None,
        };

        let task2 = RepairTask::Replicate {
            block_id,
            src_workers: vec![],
            target_worker,
            replication_factor: Some(3),
            reason: None,
        };

        // First enqueue should succeed
        let id1 = queue.enqueue(task1).unwrap();
        assert!(id1.0 > 0);

        // Second enqueue with same dedup key should return existing ID
        let id2 = queue.enqueue(task2).unwrap();
        assert_eq!(id1, id2);

        // Should only have one pending task
        assert_eq!(queue.len_pending(), 1);
    }

    #[test]
    fn test_planner_plan_replication() {
        let orphan_queue = Arc::new(OrphanQueue::new(100));
        let repair_queue = Arc::new(RepairQueue::new(100));
        let planner = RepairPlanner::new(repair_queue, orphan_queue);

        let block_id = make_block_id(1, 0);
        let current_locations = vec![make_worker_id(1)];
        let replication_factor = 3;
        let available_workers = vec![make_worker_id(1), make_worker_id(2), make_worker_id(3)];

        let actions = planner.plan_replication(block_id, &current_locations, replication_factor, &available_workers);

        // Should plan 2 replicate actions (need 2 more replicas)
        assert_eq!(actions.len(), 2);
        for action in &actions {
            match action {
                RepairAction::Replicate {
                    block_id: bid,
                    target_worker,
                    replication_factor: rf,
                    ..
                } => {
                    assert_eq!(*bid, block_id);
                    assert_eq!(*rf, Some(replication_factor));
                    assert!(*target_worker != make_worker_id(1)); // Should not target current location
                }
                _ => panic!("Expected Replicate action"),
            }
        }
    }

    #[test]
    fn test_action_to_task() {
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

        let task = action.to_task();
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

    // Regression test for RepairQueue state machine ack path.
    #[test]
    fn test_queue_state_machine_ack_success() {
        let queue = RepairQueue::new(1000);
        let block_id = make_block_id(1, 0);
        let worker1 = make_worker_id(1);

        let task = RepairTask::Replicate {
            block_id,
            src_workers: vec![],
            target_worker: worker1,
            replication_factor: Some(3),
            reason: None,
        };

        let task_id = queue.enqueue(task).unwrap();
        assert_eq!(queue.len_pending(), 1);

        // Poll task
        let records = queue.poll_for_worker(worker1, 10);
        assert_eq!(records.len(), 1);
        let polled_id = records[0].id;
        assert_eq!(queue.len_pending(), 0);
        assert_eq!(queue.len_inflight(), 1);

        // Ack success
        let followup = queue
            .ack(polled_id, worker1, TaskAckStatus::Success, None, None)
            .unwrap();
        assert!(followup.is_none());

        // Task should be removed
        assert_eq!(queue.len_total(), 0);
        assert_eq!(queue.len_pending(), 0);
        assert_eq!(queue.len_inflight(), 0);
    }

    #[test]
    fn test_queue_state_machine_ack_retryable_backoff() {
        let queue = RepairQueue::with_config(1000, 3, 300_000, 1_000, 60_000, 4);
        let block_id = make_block_id(1, 0);
        let worker1 = make_worker_id(1);

        let task = RepairTask::Replicate {
            block_id,
            src_workers: vec![],
            target_worker: worker1,
            replication_factor: Some(3),
            reason: None,
        };

        let task_id = queue.enqueue(task).unwrap();
        let records = queue.poll_for_worker(worker1, 10);
        assert_eq!(records.len(), 1);
        let polled_id = records[0].id;

        // Ack with retryable error
        let _ = queue.ack(
            polled_id,
            worker1,
            TaskAckStatus::RetryableFailed,
            Some("Temporary error".to_string()),
            Some(ErrorClass::Retryable),
        );

        // Task should be back to pending with backoff
        assert_eq!(queue.len_pending(), 1);
        assert_eq!(queue.len_inflight(), 0);

        // Should not be pollable immediately (backoff not expired)
        let records2 = queue.poll_for_worker(worker1, 10);
        assert_eq!(records2.len(), 0);
    }

    #[test]
    fn test_queue_state_machine_timeout_requeue() {
        let queue = RepairQueue::with_config(1000, 3, 300_000, 1_000, 60_000, 4);
        let block_id = make_block_id(1, 0);
        let worker1 = make_worker_id(1);

        let task = RepairTask::Replicate {
            block_id,
            src_workers: vec![],
            target_worker: worker1,
            replication_factor: Some(3),
            reason: None,
        };

        let _task_id = queue.enqueue(task).unwrap();
        let records = queue.poll_for_worker(worker1, 10);
        assert_eq!(records.len(), 1);
        let polled_id = records[0].id;

        // Simulate timeout by calling requeue_timeouts with future time
        let future_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 400_000; // 400 seconds in future (exceeds 300s timeout)

        let timeout_count = queue.requeue_timeouts(future_ms);
        assert_eq!(timeout_count, 1);

        // Task should be back to pending
        assert_eq!(queue.len_pending(), 1);
        assert_eq!(queue.len_inflight(), 0);
    }

    // Regression test for deduplication key stability.
    #[test]
    fn test_dedup_same_key_returns_existing_id() {
        let queue = RepairQueue::new(1000);
        let block_id = make_block_id(1, 0);
        let worker1 = make_worker_id(1);

        let task1 = RepairTask::Replicate {
            block_id,
            src_workers: vec![],
            target_worker: worker1,
            replication_factor: Some(3),
            reason: Some("Reason 1".to_string()),
        };

        let task2 = RepairTask::Replicate {
            block_id,
            src_workers: vec![make_worker_id(2)], // Different src_workers
            target_worker: worker1,               // Same target_worker
            replication_factor: Some(5),          // Different replication_factor
            reason: Some("Reason 2".to_string()), // Different reason
        };

        let id1 = queue.enqueue(task1).unwrap();
        let id2 = queue.enqueue(task2).unwrap();

        // Should return same ID (dedup key is block_id + target_worker)
        assert_eq!(id1, id2);
        assert_eq!(queue.len_pending(), 1);
    }

    // Regression test for planner determinism.
    #[test]
    fn test_planner_stable_output() {
        let orphan_queue = Arc::new(OrphanQueue::new(100));
        let repair_queue = Arc::new(RepairQueue::new(100));
        let planner = RepairPlanner::new(repair_queue, orphan_queue);

        let block_id = make_block_id(1, 0);
        let current_locations = vec![make_worker_id(1)];
        let replication_factor = 3;
        let available_workers = vec![make_worker_id(1), make_worker_id(2), make_worker_id(3)];

        // Call multiple times, should get same result
        let actions1 = planner.plan_replication(block_id, &current_locations, replication_factor, &available_workers);
        let actions2 = planner.plan_replication(block_id, &current_locations, replication_factor, &available_workers);

        assert_eq!(actions1.len(), actions2.len());
        assert_eq!(actions1.len(), 2);

        // Actions should be identical (same target workers selected)
        for (a1, a2) in actions1.iter().zip(actions2.iter()) {
            match (a1, a2) {
                (
                    RepairAction::Replicate { target_worker: tw1, .. },
                    RepairAction::Replicate { target_worker: tw2, .. },
                ) => assert_eq!(tw1, tw2),
                _ => panic!("Expected Replicate actions"),
            }
        }
    }

    #[test]
    fn test_planner_overrep_evict_replicas() {
        // Verify over-replication scenario produces EvictReplica actions.
        let orphan_queue = Arc::new(OrphanQueue::new(100));
        let repair_queue = Arc::new(RepairQueue::new(100));
        let planner = RepairPlanner::new(repair_queue, orphan_queue);

        let block_id = make_block_id(1, 0);
        let current_locations = vec![
            make_worker_id(1),
            make_worker_id(2),
            make_worker_id(3),
            make_worker_id(4),
            make_worker_id(5),
        ]; // 5 replicas
        let replication_factor = 3; // desired 3

        let actions = planner.plan_replication(block_id, &current_locations, replication_factor, &current_locations);

        // Should return 2 EvictReplica actions (5 - 3 = 2 excess)
        assert_eq!(actions.len(), 2);
        for action in &actions {
            match action {
                RepairAction::EvictReplica {
                    block_id: bid, reason, ..
                } => {
                    assert_eq!(*bid, block_id);
                    assert!(reason.contains("Excess replica"));
                }
                _ => panic!("Expected EvictReplica action"),
            }
        }
    }

    #[test]
    fn test_orphan_queue_min_age() {
        use std::thread;
        use std::time::Duration;

        // Create queue with 100ms min_age
        let queue = OrphanQueue::with_config(100, 100);
        let block_id = make_block_id(1, 0);
        let worker1 = make_worker_id(1);

        // Add orphan
        queue.add(block_id, worker1);
        assert_eq!(queue.len(), 1);

        // Immediately try to dequeue - should return None (not past min_age)
        assert!(queue.dequeue().is_none());

        // Wait for min_age to pass
        thread::sleep(Duration::from_millis(150));

        // Now should be able to dequeue
        let result = queue.dequeue();
        assert!(result.is_some());
        let (bid, wid) = result.unwrap();
        assert_eq!(bid, block_id);
        assert_eq!(wid, worker1);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_orphan_queue_peek_oldest() {
        let queue = OrphanQueue::with_config(100, 0); // No min_age for this test
        let block_id1 = make_block_id(1, 0);
        let block_id2 = make_block_id(2, 0);
        let worker1 = make_worker_id(1);

        queue.add(block_id1, worker1);
        queue.add(block_id2, worker1);

        // Peek should return the oldest (first added)
        let peeked = queue.peek_oldest();
        assert!(peeked.is_some());
        let (bid, _) = peeked.unwrap();
        assert_eq!(bid, block_id1);

        // Queue should still have 2 items
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn test_orphan_queue_len_eligible() {
        use std::thread;
        use std::time::Duration;

        let queue = OrphanQueue::with_config(100, 100); // 100ms min_age
        let block_id1 = make_block_id(1, 0);
        let block_id2 = make_block_id(2, 0);
        let worker1 = make_worker_id(1);

        queue.add(block_id1, worker1);
        thread::sleep(Duration::from_millis(50));
        queue.add(block_id2, worker1);

        // Initially, no eligible orphans
        assert_eq!(queue.len_eligible(), 0);

        // Wait for first orphan to become eligible
        thread::sleep(Duration::from_millis(60));
        assert_eq!(queue.len_eligible(), 1);

        // Wait for second orphan to become eligible
        thread::sleep(Duration::from_millis(50));
        assert_eq!(queue.len_eligible(), 2);
    }
}
