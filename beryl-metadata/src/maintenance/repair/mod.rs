// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Internal repair queue and planner primitives.
//!
//! These types are maintenance internals for safety and cleanup. They are not
//! a complete productized repair or rebalance lifecycle.
//!
//! This module is organized as follows:
//! - `types.rs`: Core types (RepairTaskId, RepairTask, RepairTaskState, etc.)
//! - `actions.rs`: RepairAction enum (planning layer output)
//! - `queue.rs`: RepairQueue (state machine, deduplication, retry)
//! - `planner.rs`: RepairPlanner (pure planning logic)

mod actions;
mod metrics;
mod planner;
mod policy;
mod queue;
mod types;

pub use actions::RepairAction;
pub(crate) use metrics::RepairMetrics;
pub use planner::RepairPlanner;
pub use policy::RepairPolicy;
pub use queue::RepairQueue;
pub use types::{
    RepairDedupKey, RepairTask, RepairTaskId, RepairTaskRecord, RepairTaskState, TaskAckStatus, TaskFailureClass,
};

#[cfg(test)]
mod tests {
    use super::*;
    use ::beryl_types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};
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
        let planner = RepairPlanner::new();

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

        let _task_id = queue.enqueue(task).unwrap();
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

        let _task_id = queue.enqueue(task).unwrap();
        let records = queue.poll_for_worker(worker1, 10);
        assert_eq!(records.len(), 1);
        let polled_id = records[0].id;

        // Ack with retryable error
        let _ = queue.ack(
            polled_id,
            worker1,
            TaskAckStatus::RetryableFailed,
            Some("Temporary error".to_string()),
            Some(TaskFailureClass::Retryable),
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
        let _polled_id = records[0].id;

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
        let planner = RepairPlanner::new();

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
        let planner = RepairPlanner::new();

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
    fn replica_eviction_dedup_returns_existing_task_id() {
        let repair_queue = Arc::new(RepairQueue::with_config(1000, 3, 60_000, 1_000, 60_000, 10));

        let block_id = make_block_id(1, 0);
        let target_worker = make_worker_id(1);

        let task_id1 = repair_queue
            .enqueue(RepairTask::EvictReplica {
                target_worker,
                block_id,
                reason: "excess replica cleanup".to_string(),
            })
            .unwrap();

        let task_id2 = repair_queue
            .enqueue(RepairTask::EvictReplica {
                target_worker,
                block_id,
                reason: "move follow-up duplicate".to_string(),
            })
            .unwrap();

        assert_eq!(task_id1, task_id2);
        assert_eq!(repair_queue.len_pending(), 1);
    }

    #[test]
    fn replica_eviction_same_block_different_worker_has_distinct_task() {
        let repair_queue = Arc::new(RepairQueue::with_config(1000, 3, 60_000, 1_000, 60_000, 10));

        let block_id = make_block_id(1, 0);

        repair_queue
            .enqueue(RepairTask::EvictReplica {
                target_worker: make_worker_id(1),
                block_id,
                reason: "excess replica cleanup".to_string(),
            })
            .unwrap();

        let second_task_id = repair_queue
            .enqueue(RepairTask::EvictReplica {
                target_worker: make_worker_id(2),
                block_id,
                reason: "different target replica cleanup".to_string(),
            })
            .unwrap();

        assert_ne!(second_task_id.0, 0);
        assert_eq!(repair_queue.len_pending(), 2);
    }
}
