// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Root mount readiness gate and watcher.

use crate::error::{MetadataError, MetadataResult};
use crate::metrics::MetadataMetrics;
use crate::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
use crate::raft::{AppRaftNode, Command};
use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::{info, warn};
use types::ids::ShardGroupId;
use types::CallId;

#[derive(Debug, Clone)]
pub struct RootReadinessConfig {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub warn_after_ms: u64,
}

impl Default for RootReadinessConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 200,
            max_backoff_ms: 5_000,
            warn_after_ms: 60_000,
        }
    }
}

pub struct RootReadinessGate {
    ready: AtomicUsize,
    notify: Notify,
    metrics: Option<Arc<MetadataMetrics>>,
}

impl RootReadinessGate {
    pub fn new(metrics: Option<Arc<MetadataMetrics>>) -> Self {
        if let Some(metrics) = &metrics {
            metrics.root_ready.store(0, Ordering::Relaxed);
        }
        Self {
            ready: AtomicUsize::new(0),
            notify: Notify::new(),
            metrics,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire) == 1
    }

    pub fn set_ready(&self) {
        if self.ready.swap(1, Ordering::Release) == 0 {
            if let Some(metrics) = &self.metrics {
                metrics.root_ready.store(1, Ordering::Relaxed);
            }
            self.notify.notify_waiters();
        }
    }

    pub async fn wait_ready(&self) {
        if self.is_ready() {
            return;
        }
        self.notify.notified().await;
    }
}

pub async fn wait_for_root_ready(
    raft_node: Arc<AppRaftNode>,
    mount_table: Arc<MountTable>,
    namespace_owner_group_id: ShardGroupId,
    readiness_gate: Arc<RootReadinessGate>,
    config: RootReadinessConfig,
) -> MetadataResult<()> {
    wait_for_root_ready_with_metrics(
        raft_node,
        mount_table,
        namespace_owner_group_id,
        readiness_gate,
        config,
        None,
    )
    .await
}

pub async fn wait_for_root_ready_with_metrics(
    raft_node: Arc<AppRaftNode>,
    mount_table: Arc<MountTable>,
    namespace_owner_group_id: ShardGroupId,
    readiness_gate: Arc<RootReadinessGate>,
    config: RootReadinessConfig,
    metrics: Option<Arc<MetadataMetrics>>,
) -> MetadataResult<()> {
    let start = Instant::now();
    let mut backoff_ms = config.initial_backoff_ms.max(1);
    let mut attempts = 0u64;

    loop {
        attempts += 1;

        if let Some(existing) = mount_table
            .list_mounts()
            .into_iter()
            .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
        {
            if existing.root_inode_id != ROOT_INODE_ID {
                return Err(MetadataError::InvalidArgument(format!(
                    "root inode invariant violated: expected inode_id={}, got {}. storage must be migrated or wiped",
                    ROOT_INODE_ID.as_raw(),
                    existing.root_inode_id.as_raw()
                )));
            }
            if existing.mount_kind != MountKind::Internal
                || existing.ufs_uri.is_some()
                || existing.data_io_policy != DataIoPolicy::Forbid
            {
                return Err(MetadataError::InvalidArgument(
                    "root mount exists but violates internal/no-ufs/forbid-data-io invariants".to_string(),
                ));
            }

            if let Some(metrics) = &metrics {
                metrics
                    .root_wait_elapsed_ms
                    .store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
                metrics.root_wait_attempts.store(attempts, Ordering::Relaxed);
            }

            readiness_gate.set_ready();
            info!(
                elapsed_ms = start.elapsed().as_millis(),
                attempts, "Root mount is ready"
            );
            return Ok(());
        }

        if raft_node.is_leader() {
            let mount_id = mount_table.allocate_mount_id();
            let command = Command::CreateMount {
                request_id: CallId::new(),
                mount_id,
                mount_prefix: ROOT_MOUNT_PREFIX.to_string(),
                mount_kind: MountKind::Internal,
                ufs_uri: None,
                data_io_policy: DataIoPolicy::Forbid,
                namespace_owner_group_id,
                root_inode_id: ROOT_INODE_ID,
            };

            if let Err(err) = raft_node.propose(command).await {
                match err {
                    MetadataError::LeaderChanged(msg) => {
                        warn!(error = %msg, "Root mount create deferred to leader");
                    }
                    other => return Err(other),
                }
            }
        }

        if let Some(metrics) = &metrics {
            metrics
                .root_wait_elapsed_ms
                .store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
            metrics.root_wait_attempts.store(attempts, Ordering::Relaxed);
        }

        if start.elapsed() >= Duration::from_millis(config.warn_after_ms) {
            warn!(
                elapsed_ms = start.elapsed().as_millis(),
                attempts, "Root mount still not ready"
            );
        }

        let jitter_upper = backoff_ms / 2;
        let jitter_ms = if jitter_upper > 0 {
            rand::thread_rng().gen_range(0..=jitter_upper)
        } else {
            0
        };
        sleep(Duration::from_millis(backoff_ms + jitter_ms)).await;
        backoff_ms = (backoff_ms * 2).min(config.max_backoff_ms.max(backoff_ms));
    }
}
