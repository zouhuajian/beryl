// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Background namespace, block, and worker-state convergence.

use super::{BlockCleanupCoordinator, DetachedRootReclaimer};
use crate::raft::AppRaftNode;
use crate::session_registry::SessionRegistry;
use crate::worker::WorkerManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Maintenance background task handles.
pub struct MaintenanceHandle {
    shutdown: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl MaintenanceHandle {
    /// Cancels and awaits every maintenance loop owned by Metadata.
    pub async fn shutdown(mut self) -> Result<(), tokio::task::JoinError> {
        self.shutdown.cancel();
        let mut first_error = None;
        for task in self.tasks.drain(..) {
            if let Err(error) = task.await {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Drains maintenance loops until `deadline`, then aborts and awaits them.
    ///
    /// Returns `true` when forced cancellation was required. A task panic is
    /// still reported after every remaining task has been reclaimed.
    pub async fn shutdown_until(mut self, deadline: Instant) -> Result<bool, tokio::task::JoinError> {
        self.shutdown.cancel();
        let mut forced = false;
        let mut first_error = None;
        for mut task in self.tasks.drain(..) {
            if forced {
                task.abort();
            } else {
                match tokio::time::timeout_at(deadline, &mut task).await {
                    Ok(Ok(())) => continue,
                    Ok(Err(error)) => {
                        first_error.get_or_insert(error);
                        continue;
                    }
                    Err(_) => {
                        forced = true;
                        task.abort();
                    }
                }
            }

            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(forced),
        }
    }
}

impl Drop for MaintenanceHandle {
    fn drop(&mut self) {
        self.shutdown.cancel();
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

/// Dependencies used to start Metadata background cleanup and convergence tasks.
pub struct MaintenanceService {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    cleanup: Arc<BlockCleanupCoordinator>,
    detached_root_reclaimer: Arc<DetachedRootReclaimer>,
    lost_worker_cleanup_interval: Duration,
    session_registry: Arc<SessionRegistry>,
    session_expiry_interval: Duration,
}

impl MaintenanceService {
    /// Constructs maintenance around the report-derived cleanup coordinator.
    ///
    /// The coordinator must be the same instance used by Worker heartbeats so
    /// scan and dispatch share one bounded candidate table.
    pub(crate) fn new(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        cleanup: Arc<BlockCleanupCoordinator>,
        detached_root_reclaimer: Arc<DetachedRootReclaimer>,
        lost_worker_cleanup_interval: Duration,
        session_registry: Arc<SessionRegistry>,
        session_expiry_interval: Duration,
    ) -> Self {
        Self {
            raft_node,
            worker_manager,
            cleanup,
            detached_root_reclaimer,
            lost_worker_cleanup_interval,
            session_registry,
            session_expiry_interval,
        }
    }

    /// Starts bounded write-session, namespace, block, and worker cleanup loops.
    pub(crate) fn start(&self) -> MaintenanceHandle {
        let mut tasks = Vec::with_capacity(4);
        let shutdown = CancellationToken::new();

        let session_registry = Arc::clone(&self.session_registry);
        let scan_interval = self.session_expiry_interval;
        let task_shutdown = shutdown.child_token();
        tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(scan_interval);
            loop {
                tokio::select! {
                    biased;
                    _ = task_shutdown.cancelled() => return,
                    _ = interval.tick() => {}
                }
                let retired = session_registry.retire_expired_batch();
                if retired > 0 {
                    info!(task = "write_session_expiry", retired, "Expired write sessions retired");
                }
            }
        }));

        let detached_root_reclaimer = Arc::clone(&self.detached_root_reclaimer);
        tasks.push(tokio::spawn(detached_root_reclaimer.run(shutdown.child_token())));

        if self.cleanup.enabled() {
            let cleanup = Arc::clone(&self.cleanup);
            let scan_interval = cleanup.scan_interval();
            let task_shutdown = shutdown.child_token();
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(scan_interval);
                loop {
                    tokio::select! {
                        biased;
                        _ = task_shutdown.cancelled() => return,
                        _ = interval.tick() => {}
                    }
                    if let Err(error) = cleanup.scan_once().await {
                        error!(task = "block_cleanup", %error, "Block cleanup scan failed");
                    }
                }
            }));
        }

        let raft_node = Arc::clone(&self.raft_node);
        let worker_manager = Arc::clone(&self.worker_manager);
        let scan_interval = self.lost_worker_cleanup_interval;
        let task_shutdown = shutdown.child_token();
        tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(scan_interval);
            loop {
                tokio::select! {
                    biased;
                    _ = task_shutdown.cancelled() => return,
                    _ = interval.tick() => {}
                }
                remove_expired_workers(&raft_node, &worker_manager);
            }
        }));

        info!(task_count = tasks.len(), "Maintenance service started");
        MaintenanceHandle { shutdown, tasks }
    }
}

fn remove_expired_workers(raft_node: &AppRaftNode, worker_manager: &WorkerManager) {
    if !raft_node.is_leader() {
        return;
    }

    let dead_workers = worker_manager.list_expired_worker_runs();

    for (dead_worker, expected_run_id) in dead_workers {
        if !worker_manager.remove_dead_worker(&dead_worker.group_name, dead_worker.worker_id, expected_run_id) {
            continue;
        }
        info!(
            group_name = %dead_worker.group_name,
            worker_id = dead_worker.worker_id.as_raw(),
            "Removing dead worker"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::remove_expired_workers;
    use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    use crate::worker::{BlockReportBlock, BlockReportBlockState, WorkerDescriptor, WorkerManager};
    use crate::MountTable;
    use beryl_types::ids::{BlockId, BlockIndex, InodeId, WorkerId};
    use beryl_types::{GroupName, Tier, TierFree, WorkerRunId};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::Duration;

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    async fn test_raft(dir: &TempDir, leader: bool) -> Arc<AppRaftNode> {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::default());
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        let raft_node = Arc::new(AppRaftNode::new(1, storage, state_machine, mount_table).await.unwrap());
        if leader {
            raft_node
                .initialize_single_node("127.0.0.1:0".to_string())
                .await
                .unwrap();
            for _ in 0..100 {
                if raft_node.is_leader() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
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
        let group_name = group_name("root");
        let address = format!("127.0.0.1:{}", 9000 + worker_id.as_raw());
        let run_id = worker_run_id(worker_id);
        manager.register_worker_run(&group_name, worker_id, address.clone(), run_id);
        manager
            .record_heartbeat_with_tier_free(
                &group_name,
                worker_id,
                run_id,
                1,
                &address,
                vec![TierFree {
                    tier: Tier::Hdd,
                    free_bytes: 500,
                }],
            )
            .unwrap();
    }

    fn report_block(block_id: BlockId) -> BlockReportBlock {
        BlockReportBlock {
            tier: Some(beryl_types::Tier::Hdd),
            block_id,
            lease_epoch: u64::from(block_id.index.as_raw()) + 1,
            block_state: BlockReportBlockState::Ready,
            effective_len: 64,
        }
    }

    fn persisted_worker(group_name: GroupName, worker_id: WorkerId) -> WorkerDescriptor {
        WorkerDescriptor {
            group_name,
            worker_id,
            address: "127.0.0.1:9090".to_string(),
        }
    }

    fn publish_report(manager: &WorkerManager, worker_id: WorkerId, report_seq: u64, blocks: Vec<BlockId>) {
        let group_name = group_name("root");
        let run_id = manager
            .get_registered_run(&group_name, worker_id)
            .expect("worker registration");
        manager
            .receive_full_block_report(
                &group_name,
                worker_id,
                run_id,
                report_seq,
                0,
                true,
                blocks.into_iter().map(report_block).collect(),
            )
            .unwrap();
    }

    #[tokio::test]
    async fn dead_worker_cleanup_preserves_live_replica() {
        let dir = TempDir::new().unwrap();
        let raft_node = test_raft(&dir, true).await;
        let worker_manager = Arc::new(WorkerManager::new(1_000));
        let source = WorkerId::new(1);
        let dead = WorkerId::new(4);
        let block_id = BlockId::new(InodeId::new(11), BlockIndex::new(0));
        live_worker(&worker_manager, dead);
        publish_report(&worker_manager, dead, 1, vec![block_id]);
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        live_worker(&worker_manager, source);
        publish_report(&worker_manager, source, 1, vec![block_id]);

        remove_expired_workers(&raft_node, &worker_manager);
        assert!(worker_manager.get_registered_run(&group_name("root"), dead).is_none());
        assert!(worker_manager
            .collect_worker_placement_views(&group_name("root"))
            .iter()
            .any(|view| view.worker_id == dead && view.worker_run_id.is_none()));
        assert_eq!(
            worker_manager.get_block_locations(&group_name("root"), block_id),
            vec![source]
        );
        remove_expired_workers(&raft_node, &worker_manager);
        assert_eq!(
            worker_manager.get_block_locations(&group_name("root"), block_id),
            vec![source]
        );
    }

    #[tokio::test]
    async fn persisted_descriptor_without_runtime_is_not_a_dead_worker_after_reload() {
        let dir = TempDir::new().unwrap();
        let raft_node = test_raft(&dir, true).await;
        let worker_manager = Arc::new(WorkerManager::new(1_000));
        let group_name_value = group_name("root");
        let worker_id = WorkerId::new(9);
        worker_manager.load_registered_workers(vec![persisted_worker(group_name_value.clone(), worker_id)]);

        remove_expired_workers(&raft_node, &worker_manager);
        let views = worker_manager.collect_worker_placement_views(&group_name_value);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].worker_id, worker_id);
        assert!(views[0].worker_run_id.is_none());
        assert!(!views[0].lease_valid);
    }
}
