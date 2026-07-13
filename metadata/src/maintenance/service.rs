// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Background worker-state convergence and repair scheduling.

use super::lost_worker::{LostWorkerCleanupDeps, LostWorkerCleanupService};
use super::repair::{RepairPlanner, RepairPolicy, RepairQueue};
use crate::raft::AppRaftNode;
use crate::worker::WorkerManager;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Maintenance background task handles.
pub struct MaintenanceHandle {
    tasks: Vec<JoinHandle<()>>,
}

impl MaintenanceHandle {
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }
}

/// Starts the current worker-state convergence tasks.
pub struct MaintenanceService {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    repair_queue: Arc<RepairQueue>,
    repair_planner: Arc<RepairPlanner>,
    repair_policy: RepairPolicy,
    lost_worker_cleanup_interval_sec: u64,
    rebalance_interval_sec: u64,
    timeout_check_interval_sec: u64,
}

impl MaintenanceService {
    pub(crate) fn new(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        repair_queue: Arc<RepairQueue>,
        repair_planner: Arc<RepairPlanner>,
        repair_policy: RepairPolicy,
    ) -> Self {
        Self {
            raft_node,
            worker_manager,
            repair_queue,
            repair_planner,
            repair_policy,
            lost_worker_cleanup_interval_sec: 30,
            rebalance_interval_sec: 300,
            timeout_check_interval_sec: 10,
        }
    }

    /// Starts lost-worker cleanup, rebalance scanning, and repair timeout requeue.
    pub(crate) fn start(&self) -> MaintenanceHandle {
        let mut tasks = Vec::with_capacity(3);

        let lost_worker = Arc::new(LostWorkerCleanupService::new(LostWorkerCleanupDeps {
            raft_node: Arc::clone(&self.raft_node),
            worker_manager: Arc::clone(&self.worker_manager),
            repair_queue: Arc::clone(&self.repair_queue),
            repair_planner: Arc::clone(&self.repair_planner),
            repair_policy: self.repair_policy,
        }));
        let interval_sec = self.lost_worker_cleanup_interval_sec;
        tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
            loop {
                interval.tick().await;
                if let Err(error) = lost_worker.run_once().await {
                    error!(task = "lost_worker_cleanup", %error, "Lost-worker cleanup task failed");
                }
            }
        }));

        let repair_planner = Arc::clone(&self.repair_planner);
        let repair_queue = Arc::clone(&self.repair_queue);
        let worker_manager = Arc::clone(&self.worker_manager);
        let raft_node = Arc::clone(&self.raft_node);
        let interval_sec = self.rebalance_interval_sec;
        tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
            loop {
                interval.tick().await;
                if raft_node.is_leader() {
                    for action in repair_planner.plan_rebalance(&worker_manager) {
                        let block_id = action.block_id();
                        if let Err(error) = repair_queue.enqueue(action.into_task()) {
                            error!(%block_id, %error, "Failed to enqueue rebalance task");
                        }
                    }
                }
            }
        }));

        let repair_queue = Arc::clone(&self.repair_queue);
        let interval_sec = self.timeout_check_interval_sec;
        tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
            loop {
                interval.tick().await;
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time must not precede UNIX epoch")
                    .as_millis() as u64;
                let timeout_count = repair_queue.requeue_timeouts(now_ms);
                if timeout_count > 0 {
                    info!(timeout_count, "Requeued timed-out repair tasks");
                }
            }
        }));

        info!(task_count = tasks.len(), "Maintenance service started");
        MaintenanceHandle { tasks }
    }
}
