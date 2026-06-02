// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Root mount readiness gate and watcher.

use crate::error::{MetadataError, MetadataResult};
use crate::metrics::MetadataMetrics;
use crate::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
use crate::observe;
use crate::raft::{AppRaftNode, RocksDBStorage};
use parking_lot::RwLock;
use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::{info, warn};
use types::GroupName;

#[derive(Debug, Clone)]
pub struct RootReadinessConfig {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub warn_after_ms: u64,
    pub timeout_ms: u64,
    pub fail_fast: bool,
}

impl Default for RootReadinessConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 200,
            max_backoff_ms: 5_000,
            warn_after_ms: 60_000,
            timeout_ms: 120_000,
            fail_fast: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootNotReadyReason {
    Unformatted,
    MarkerMismatch,
    RaftUninitialized,
    NotLeader,
    RootMountMissing,
    RootMountOwnerMismatch,
}

impl RootNotReadyReason {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Unformatted => "Unformatted",
            Self::MarkerMismatch => "MarkerMismatch",
            Self::RaftUninitialized => "RaftUninitialized",
            Self::NotLeader => "NotLeader",
            Self::RootMountMissing => "RootMountMissing",
            Self::RootMountOwnerMismatch => "RootMountOwnerMismatch",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootReadinessState {
    Starting,
    NotReady(RootNotReadyReason),
    Ready,
}

#[derive(Clone, Debug)]
pub struct RootReadinessLogFields {
    pub cluster_id: String,
    pub group_name: String,
    pub node_id: u64,
    pub storage_dir: String,
}

impl RootReadinessLogFields {
    pub fn unknown(namespace_owner_group_name: &GroupName) -> Self {
        Self {
            cluster_id: "unknown".to_string(),
            group_name: namespace_owner_group_name.to_string(),
            node_id: 0,
            storage_dir: "unknown".to_string(),
        }
    }
}

pub struct RootReadinessGate {
    ready: AtomicUsize,
    state: RwLock<RootReadinessState>,
    notify: Notify,
    metrics: Option<Arc<MetadataMetrics>>,
}

impl RootReadinessGate {
    pub fn new(metrics: Option<Arc<MetadataMetrics>>) -> Self {
        if let Some(metrics) = &metrics {
            metrics.root_ready.store(0, Ordering::Relaxed);
            observe::record_root_ready(false);
        }
        Self {
            ready: AtomicUsize::new(0),
            state: RwLock::new(RootReadinessState::Starting),
            notify: Notify::new(),
            metrics,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire) == 1
    }

    pub fn set_ready(&self) {
        *self.state.write() = RootReadinessState::Ready;
        if self.ready.swap(1, Ordering::Release) == 0 {
            if let Some(metrics) = &self.metrics {
                metrics.root_ready.store(1, Ordering::Relaxed);
                observe::record_root_ready(true);
            }
            self.notify.notify_waiters();
        }
    }

    pub fn set_not_ready(&self, reason: RootNotReadyReason) {
        *self.state.write() = RootReadinessState::NotReady(reason);
        self.ready.store(0, Ordering::Release);
        if let Some(metrics) = &self.metrics {
            metrics.root_ready.store(0, Ordering::Relaxed);
            observe::record_root_ready(false);
        }
    }

    pub fn state(&self) -> RootReadinessState {
        self.state.read().clone()
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
    namespace_owner_group_name: GroupName,
    readiness_gate: Arc<RootReadinessGate>,
    config: RootReadinessConfig,
) -> MetadataResult<()> {
    wait_for_root_ready_with_metrics(
        raft_node,
        mount_table,
        namespace_owner_group_name,
        readiness_gate,
        config,
        None,
    )
    .await
}

pub async fn wait_for_root_ready_with_metrics(
    raft_node: Arc<AppRaftNode>,
    mount_table: Arc<MountTable>,
    namespace_owner_group_name: GroupName,
    readiness_gate: Arc<RootReadinessGate>,
    config: RootReadinessConfig,
    metrics: Option<Arc<MetadataMetrics>>,
) -> MetadataResult<()> {
    wait_for_root_ready_inner(RootReadyInputs {
        raft_node,
        mount_table,
        storage: None,
        namespace_owner_group_name: namespace_owner_group_name.clone(),
        readiness_gate,
        config,
        metrics,
        log_fields: RootReadinessLogFields::unknown(&namespace_owner_group_name),
    })
    .await
}

pub(crate) struct RootReadyInputs {
    pub(crate) raft_node: Arc<AppRaftNode>,
    pub(crate) mount_table: Arc<MountTable>,
    pub(crate) storage: Option<Arc<RocksDBStorage>>,
    pub(crate) namespace_owner_group_name: GroupName,
    pub(crate) readiness_gate: Arc<RootReadinessGate>,
    pub(crate) config: RootReadinessConfig,
    pub(crate) metrics: Option<Arc<MetadataMetrics>>,
    pub(crate) log_fields: RootReadinessLogFields,
}

pub(crate) async fn wait_for_root_ready_with_inputs(inputs: RootReadyInputs) -> MetadataResult<()> {
    wait_for_root_ready_inner(inputs).await
}

async fn wait_for_root_ready_inner(inputs: RootReadyInputs) -> MetadataResult<()> {
    let RootReadyInputs {
        raft_node,
        mount_table,
        storage,
        namespace_owner_group_name,
        readiness_gate,
        config,
        metrics,
        log_fields,
    } = inputs;
    let start = Instant::now();
    let mut backoff_ms = config.initial_backoff_ms.max(1);
    let mut attempts = 0u64;
    let mut timeout_logged = false;

    loop {
        attempts += 1;
        let mut reason = if raft_node.is_initialized().await? {
            RootNotReadyReason::RootMountMissing
        } else {
            RootNotReadyReason::RaftUninitialized
        };

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
            if existing.namespace_owner_group_name != namespace_owner_group_name {
                readiness_gate.set_not_ready(RootNotReadyReason::RootMountOwnerMismatch);
                return Err(MetadataError::ServiceUnavailable(format!(
                    "root mount owner group mismatch: expected {}, got {}",
                    namespace_owner_group_name, existing.namespace_owner_group_name
                )));
            }
            if let Some(storage) = &storage {
                if storage.get_inode(ROOT_INODE_ID)?.is_none() {
                    readiness_gate.set_not_ready(RootNotReadyReason::RootMountMissing);
                    return Err(MetadataError::ServiceUnavailable(
                        "root mount exists but root inode is missing".to_string(),
                    ));
                }
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

        if !raft_node.is_leader() && reason != RootNotReadyReason::RaftUninitialized {
            reason = RootNotReadyReason::NotLeader;
        }
        readiness_gate.set_not_ready(reason.clone());

        if let Some(metrics) = &metrics {
            metrics
                .root_wait_elapsed_ms
                .store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
            metrics.root_wait_attempts.store(attempts, Ordering::Relaxed);
        }

        if start.elapsed() >= Duration::from_millis(config.warn_after_ms) {
            warn!(
                reason = reason.as_str(),
                cluster_id = %log_fields.cluster_id,
                group_name = %log_fields.group_name,
                node_id = log_fields.node_id,
                storage_dir = %log_fields.storage_dir,
                elapsed_ms = start.elapsed().as_millis(),
                attempts,
                hint = "run metadata format for unformatted local storage; check raft mode, node id, and root mount state",
                "Root mount still not ready"
            );
        }
        if start.elapsed() >= Duration::from_millis(config.timeout_ms) {
            let message = format!(
                "metadata root readiness timed out: reason={}, cluster_id={}, group_name={}, node_id={}, storage_dir={}, elapsed_ms={}, hint=run metadata format or inspect raft/root initialization",
                reason.as_str(),
                log_fields.cluster_id,
                log_fields.group_name,
                log_fields.node_id,
                log_fields.storage_dir,
                start.elapsed().as_millis()
            );
            if config.fail_fast {
                return Err(MetadataError::ServiceUnavailable(message));
            }
            if !timeout_logged {
                warn!(error = %message, "Root readiness timeout reached; continuing to wait");
                timeout_logged = true;
            }
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
