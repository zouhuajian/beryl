// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Background namespace, block, and worker-state convergence.

use super::lost_worker::{LostWorkerCleanupDeps, LostWorkerCleanupService};
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

/// Owns Metadata background cleanup and convergence tasks.
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

        let lost_worker = Arc::new(LostWorkerCleanupService::new(LostWorkerCleanupDeps {
            raft_node: Arc::clone(&self.raft_node),
            worker_manager: Arc::clone(&self.worker_manager),
        }));
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
                if let Err(error) = lost_worker.run_once().await {
                    error!(task = "lost_worker_cleanup", %error, "Lost-worker cleanup task failed");
                }
            }
        }));

        info!(task_count = tasks.len(), "Maintenance service started");
        MaintenanceHandle { shutdown, tasks }
    }
}
