// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Lost-worker cleanup and affected-block repair scheduling.

use crate::error::MetadataResult;
use crate::maintenance::repair::{RepairPlanner, RepairPolicy, RepairQueue};
use crate::raft::AppRaftNode;
use crate::worker::WorkerManager;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{info, warn};

/// Dependencies for lost-worker cleanup.
pub struct LostWorkerCleanupDeps {
    pub raft_node: Arc<AppRaftNode>,
    pub worker_manager: Arc<WorkerManager>,
    pub repair_queue: Arc<RepairQueue>,
    pub repair_planner: Arc<RepairPlanner>,
    pub repair_policy: RepairPolicy,
}

/// Summary for one lost-worker cleanup scan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LostWorkerCleanupOutcome {
    pub removed_workers: usize,
    pub affected_blocks: usize,
    pub repair_tasks_enqueued: usize,
    pub enqueue_failures: usize,
    pub skipped_dead_workers: usize,
}

/// Scans worker soft-state for dead workers and schedules affected block repair.
pub struct LostWorkerCleanupService {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    repair_queue: Arc<RepairQueue>,
    repair_planner: Arc<RepairPlanner>,
    repair_policy: RepairPolicy,
}

impl LostWorkerCleanupService {
    pub fn new(deps: LostWorkerCleanupDeps) -> Self {
        Self {
            raft_node: deps.raft_node,
            worker_manager: deps.worker_manager,
            repair_queue: deps.repair_queue,
            repair_planner: deps.repair_planner,
            repair_policy: deps.repair_policy,
        }
    }

    pub async fn run_once(&self) -> MetadataResult<LostWorkerCleanupOutcome> {
        if !self.raft_node.is_leader() {
            return Ok(LostWorkerCleanupOutcome::default());
        }

        let live_workers = self.worker_manager.list_live_workers();
        let all_workers = self.worker_manager.list_all_workers();
        let live_set: HashSet<_> = live_workers.iter().copied().collect();
        let dead_workers: Vec<_> = all_workers
            .into_iter()
            .filter(|worker| !live_set.contains(worker))
            .collect();

        let mut outcome = LostWorkerCleanupOutcome::default();
        for dead_worker in dead_workers {
            info!(
                group_id = dead_worker.group_id.as_raw(),
                worker_id = dead_worker.worker_id.as_raw(),
                "Removing dead worker"
            );
            let affected_blocks = self
                .worker_manager
                .remove_dead_worker(dead_worker.group_id, dead_worker.worker_id);
            outcome.removed_workers += 1;
            outcome.affected_blocks += affected_blocks.len();

            let live_workers_after = self.worker_manager.list_live_workers_in_group(dead_worker.group_id);
            for block_id in affected_blocks {
                let current_locations = self.worker_manager.get_block_locations(dead_worker.group_id, block_id);
                let replication_factor = self.repair_policy.default_replication_factor;
                let actions = self.repair_planner.plan_replication(
                    block_id,
                    &current_locations,
                    replication_factor,
                    &live_workers_after,
                );
                for action in actions {
                    let task = action.into_task();
                    if let Err(e) = self.repair_queue.enqueue(task) {
                        outcome.enqueue_failures += 1;
                        warn!(
                            block_id = %block_id,
                            error = %e,
                            "Failed to enqueue replication task after worker removal"
                        );
                    } else {
                        outcome.repair_tasks_enqueued += 1;
                    }
                }
            }
        }

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use crate::maintenance::lost_worker::{LostWorkerCleanupDeps, LostWorkerCleanupService};
    use crate::maintenance::repair::{OrphanQueue, RepairPlanner, RepairPolicy, RepairQueue};
    use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    use crate::worker::{BlockReportBlock, BlockReportBlockState, HealthStatus, WorkerManager};
    use crate::MountTable;
    use std::sync::Arc;
    use tempfile::TempDir;
    use types::ids::{BlockId, BlockIndex, DataHandleId, ShardGroupId, WorkerId};
    use types::WorkerRunId;

    async fn test_raft(dir: &TempDir, leader: bool) -> Arc<AppRaftNode> {
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), mount_table));
        let peers = if leader {
            vec!["127.0.0.1:0".to_string()]
        } else {
            Vec::new()
        };
        let raft_config = crate::config::RaftConfig { node_id: 1, peers };
        let raft_node = Arc::new(AppRaftNode::new(1, storage, state_machine, &raft_config).await.unwrap());
        if leader {
            for _ in 0..100 {
                if raft_node.is_leader() {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            }
            assert!(raft_node.is_leader());
        } else {
            assert!(!raft_node.is_leader());
        }
        raft_node
    }

    fn worker_run_id(worker_id: WorkerId) -> WorkerRunId {
        format!("550e8400-e29b-41d4-a716-{:012x}", worker_id.as_raw())
            .parse()
            .expect("valid test WorkerRunId")
    }

    fn live_worker(manager: &WorkerManager, worker_id: WorkerId) {
        let group_id = ShardGroupId::new(1);
        let address = format!("127.0.0.1:{}", 9000 + worker_id.as_raw());
        let run_id = worker_run_id(worker_id);
        manager
            .register_worker(group_id, worker_id, address.clone(), 1, 100, None)
            .unwrap();
        manager
            .register_worker_run(group_id, worker_id, address.clone(), 1, run_id, None)
            .unwrap();
        manager
            .record_heartbeat(
                group_id,
                worker_id,
                run_id,
                1,
                &address,
                1,
                1_000,
                500,
                500,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();
    }

    fn report_block(block_id: BlockId) -> BlockReportBlock {
        BlockReportBlock {
            block_id,
            data_handle_id: block_id.data_handle_id.as_raw(),
            block_index: block_id.index.as_raw(),
            block_stamp: u64::from(block_id.index.as_raw()) + 1,
            effective_len: 4096,
            committed_length: 4096,
            block_state: BlockReportBlockState::Ready,
        }
    }

    fn publish_report(manager: &WorkerManager, worker_id: WorkerId, report_epoch: u64, blocks: Vec<BlockId>) {
        let group_id = ShardGroupId::new(1);
        let run_id = manager
            .get_registration(group_id, worker_id)
            .expect("worker registration")
            .worker_run_id;
        manager
            .receive_full_block_report(
                group_id,
                worker_id,
                run_id,
                report_epoch,
                0,
                true,
                blocks.into_iter().map(report_block).collect(),
            )
            .unwrap();
    }

    fn service(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        repair_queue: Arc<RepairQueue>,
        orphan_queue: Arc<OrphanQueue>,
    ) -> LostWorkerCleanupService {
        service_with_policy(
            raft_node,
            worker_manager,
            repair_queue,
            orphan_queue,
            RepairPolicy::default(),
        )
    }

    fn service_with_policy(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        repair_queue: Arc<RepairQueue>,
        orphan_queue: Arc<OrphanQueue>,
        repair_policy: RepairPolicy,
    ) -> LostWorkerCleanupService {
        let repair_planner = Arc::new(RepairPlanner::new(orphan_queue));
        LostWorkerCleanupService::new(LostWorkerCleanupDeps {
            raft_node,
            worker_manager,
            repair_queue,
            repair_planner,
            repair_policy,
        })
    }

    #[tokio::test]
    async fn dead_worker_removed_and_affected_blocks_planned() {
        let dir = TempDir::new().unwrap();
        let raft_node = test_raft(&dir, true).await;
        let worker_manager = Arc::new(WorkerManager::new(1));
        let repair_queue = Arc::new(RepairQueue::new(100));
        let orphan_queue = Arc::new(OrphanQueue::new(100));
        let source = WorkerId::new(1);
        let target_a = WorkerId::new(2);
        let target_b = WorkerId::new(3);
        let dead = WorkerId::new(4);
        let block_id = BlockId::new(DataHandleId::new(11), BlockIndex::new(0));
        live_worker(&worker_manager, dead);
        publish_report(&worker_manager, dead, 1, vec![block_id]);
        tokio::time::sleep(tokio::time::Duration::from_millis(1_100)).await;
        live_worker(&worker_manager, source);
        live_worker(&worker_manager, target_a);
        live_worker(&worker_manager, target_b);
        publish_report(&worker_manager, source, 1, vec![block_id]);

        let outcome = service(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::clone(&repair_queue),
            Arc::clone(&orphan_queue),
        )
        .run_once()
        .await
        .unwrap();

        assert_eq!(outcome.removed_workers, 1);
        assert_eq!(outcome.affected_blocks, 1);
        assert_eq!(outcome.repair_tasks_enqueued, 2);
        assert!(worker_manager.get_worker_blocks(ShardGroupId::new(1), dead).is_empty());
        assert_eq!(
            worker_manager.get_block_locations(ShardGroupId::new(1), block_id),
            vec![source]
        );
        assert_eq!(repair_queue.len_pending(), 2);
    }

    #[tokio::test]
    async fn no_dead_worker_is_noop() {
        let dir = TempDir::new().unwrap();
        let raft_node = test_raft(&dir, true).await;
        let worker_manager = Arc::new(WorkerManager::new(1));
        let repair_queue = Arc::new(RepairQueue::new(100));
        let orphan_queue = Arc::new(OrphanQueue::new(100));
        live_worker(&worker_manager, WorkerId::new(1));

        let outcome = service(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::clone(&repair_queue),
            Arc::clone(&orphan_queue),
        )
        .run_once()
        .await
        .unwrap();

        assert_eq!(outcome.removed_workers, 0);
        assert_eq!(outcome.affected_blocks, 0);
        assert_eq!(outcome.repair_tasks_enqueued, 0);
        assert_eq!(repair_queue.len_pending(), 0);
    }

    #[tokio::test]
    async fn dead_worker_cleanup_uses_repair_policy_default_replication_factor() {
        let dir = TempDir::new().unwrap();
        let raft_node = test_raft(&dir, true).await;
        let worker_manager = Arc::new(WorkerManager::new(1));
        let repair_queue = Arc::new(RepairQueue::new(100));
        let orphan_queue = Arc::new(OrphanQueue::new(100));
        let source = WorkerId::new(1);
        let target_a = WorkerId::new(2);
        let target_b = WorkerId::new(3);
        let dead = WorkerId::new(4);
        let block_id = BlockId::new(DataHandleId::new(13), BlockIndex::new(0));
        live_worker(&worker_manager, dead);
        publish_report(&worker_manager, dead, 1, vec![block_id]);
        tokio::time::sleep(tokio::time::Duration::from_millis(1_100)).await;
        live_worker(&worker_manager, source);
        live_worker(&worker_manager, target_a);
        live_worker(&worker_manager, target_b);
        publish_report(&worker_manager, source, 1, vec![block_id]);

        let outcome = service_with_policy(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::clone(&repair_queue),
            Arc::clone(&orphan_queue),
            RepairPolicy {
                default_replication_factor: 2,
            },
        )
        .run_once()
        .await
        .unwrap();

        assert_eq!(outcome.removed_workers, 1);
        assert_eq!(outcome.affected_blocks, 1);
        assert_eq!(outcome.repair_tasks_enqueued, 1);
        let mut records = repair_queue.poll_for_worker(target_a, 1);
        records.extend(repair_queue.poll_for_worker(target_b, 1));
        assert_eq!(records.len(), 1);
        match records.remove(0).task {
            crate::maintenance::repair::RepairTask::Replicate { replication_factor, .. } => {
                assert_eq!(replication_factor, Some(2))
            }
            other => panic!("expected replicate task, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn nonleader_lost_worker_cleanup_is_noop() {
        let dir = TempDir::new().unwrap();
        let raft_node = test_raft(&dir, false).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let repair_queue = Arc::new(RepairQueue::new(100));
        let orphan_queue = Arc::new(OrphanQueue::new(100));
        let dead = WorkerId::new(1);
        let block_id = BlockId::new(DataHandleId::new(12), BlockIndex::new(0));
        live_worker(&worker_manager, dead);
        publish_report(&worker_manager, dead, 1, vec![block_id]);

        let outcome = service(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::clone(&repair_queue),
            Arc::clone(&orphan_queue),
        )
        .run_once()
        .await
        .unwrap();

        assert_eq!(outcome.removed_workers, 0);
        assert_eq!(outcome.skipped_dead_workers, 0);
        assert_eq!(
            worker_manager.get_worker_blocks(ShardGroupId::new(1), dead),
            vec![block_id]
        );
        assert_eq!(repair_queue.len_pending(), 0);
    }
}
