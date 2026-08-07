// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Background namespace, block, and worker-state convergence.

use super::lost_worker::{LostWorkerCleanupDeps, LostWorkerCleanupService};
use super::{BlockCleanupCoordinator, DetachedRootReclaimer};
use crate::raft::AppRaftNode;
use crate::worker::WorkerManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Maintenance background task handles.
pub struct MaintenanceHandle {
    tasks: Vec<JoinHandle<()>>,
}

impl MaintenanceHandle {
    /// Returns the number of background maintenance loops owned by this handle.
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }
}

/// Starts the current worker-state convergence tasks.
pub struct MaintenanceService {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    cleanup: Arc<BlockCleanupCoordinator>,
    detached_root_reclaimer: Arc<DetachedRootReclaimer>,
    lost_worker_cleanup_interval: Duration,
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
    ) -> Self {
        Self {
            raft_node,
            worker_manager,
            cleanup,
            detached_root_reclaimer,
            lost_worker_cleanup_interval,
        }
    }

    /// Starts detached namespace cleanup and the worker convergence loops.
    pub(crate) fn start(&self) -> MaintenanceHandle {
        let mut tasks = Vec::with_capacity(3);

        let detached_root_reclaimer = Arc::clone(&self.detached_root_reclaimer);
        tasks.push(tokio::spawn(detached_root_reclaimer.run()));

        if self.cleanup.enabled() {
            let cleanup = Arc::clone(&self.cleanup);
            let scan_interval = cleanup.scan_interval();
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(scan_interval);
                loop {
                    interval.tick().await;
                    if let Err(error) = cleanup.scan_once().await {
                        error!(task = "block_cleanup", %error, "Block cleanup scan failed");
                    }
                }
            }));
        }

        let lost_worker = Arc::new(LostWorkerCleanupService::new(LostWorkerCleanupDeps {
            raft_node: Arc::clone(&self.raft_node),
            worker_manager: Arc::clone(&self.worker_manager),
        }));
        let scan_interval = self.lost_worker_cleanup_interval;
        tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(scan_interval);
            loop {
                interval.tick().await;
                if let Err(error) = lost_worker.run_once().await {
                    error!(task = "lost_worker_cleanup", %error, "Lost-worker cleanup task failed");
                }
            }
        }));

        info!(task_count = tasks.len(), "Maintenance service started");
        MaintenanceHandle { tasks }
    }
}
