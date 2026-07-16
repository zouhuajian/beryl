// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Root mount readiness gate and watcher.

use crate::error::{MetadataError, MetadataResult};
use crate::metrics::MetadataMetrics;
use crate::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
use crate::observe;
use crate::raft::{AppRaftNode, RocksDBStorage};
use beryl_types::GroupName;
use parking_lot::RwLock;
use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::{info, warn};

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
                || existing.data_io_policy != DataIoPolicy::Allow
            {
                return Err(MetadataError::InvalidArgument(
                    "root mount exists but violates writable internal root invariants".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RaftConfig, RaftMode};
    use crate::mount::MountEntry;
    use crate::raft::AppRaftStateMachine;
    use beryl_types::ids::MountId;
    use tempfile::TempDir;

    struct ReadinessFixture {
        storage: Arc<RocksDBStorage>,
        mount_table: Arc<MountTable>,
        raft_node: Arc<AppRaftNode>,
        _temp_dir: TempDir,
    }

    impl ReadinessFixture {
        async fn new() -> Self {
            let temp_dir = TempDir::new().unwrap();
            let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).unwrap());
            let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
            let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
            let raft_config = RaftConfig {
                mode: RaftMode::Single,
                ..RaftConfig::default()
            };
            let raft_node = Arc::new(
                AppRaftNode::new(
                    raft_config.node_id,
                    Arc::clone(&storage),
                    state_machine,
                    Arc::clone(&mount_table),
                    &raft_config,
                )
                .await
                .unwrap(),
            );
            Self {
                storage,
                mount_table,
                raft_node,
                _temp_dir: temp_dir,
            }
        }

        async fn initialize_raft(&self) {
            self.raft_node
                .initialize_single_node("127.0.0.1:0".to_string())
                .await
                .unwrap();
        }
    }

    async fn wait_for_root_ready(
        fixture: &ReadinessFixture,
        readiness_gate: Arc<RootReadinessGate>,
    ) -> MetadataResult<()> {
        wait_for_root_ready_with_inputs(RootReadyInputs {
            raft_node: Arc::clone(&fixture.raft_node),
            mount_table: Arc::clone(&fixture.mount_table),
            storage: None,
            namespace_owner_group_name: GroupName::parse("root").unwrap(),
            readiness_gate,
            config: RootReadinessConfig {
                initial_backoff_ms: 1,
                max_backoff_ms: 2,
                warn_after_ms: 1,
                timeout_ms: 10,
                fail_fast: true,
            },
            metrics: None,
            log_fields: RootReadinessLogFields {
                cluster_id: "test-cluster".to_string(),
                group_name: "root".to_string(),
                node_id: 1,
                storage_dir: "test".to_string(),
            },
        })
        .await
    }

    #[tokio::test]
    async fn metadata_start_readiness_does_not_create_missing_root_mount() {
        let fixture = ReadinessFixture::new().await;
        fixture.initialize_raft().await;

        let err = wait_for_root_ready(&fixture, Arc::new(RootReadinessGate::new(None)))
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("RootMountMissing"), "{message}");
        assert!(fixture
            .mount_table
            .list_mounts()
            .into_iter()
            .all(|mount| mount.mount_prefix != ROOT_MOUNT_PREFIX));
        assert!(fixture.storage.get_inode(ROOT_INODE_ID).unwrap().is_none());
    }

    #[tokio::test]
    async fn metadata_readiness_rejects_root_mount_with_wrong_owner_group() {
        let fixture = ReadinessFixture::new().await;
        fixture
            .mount_table
            .upsert(MountEntry {
                mount_id: MountId::new(1),
                mount_prefix: ROOT_MOUNT_PREFIX.to_string(),
                mount_kind: MountKind::Internal,
                ufs_uri: None,
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch: 1,
                namespace_owner_group_name: GroupName::parse("other").unwrap(),
                root_inode_id: ROOT_INODE_ID,
            })
            .unwrap();
        fixture.initialize_raft().await;
        let readiness_gate = Arc::new(RootReadinessGate::new(None));

        let err = wait_for_root_ready(&fixture, Arc::clone(&readiness_gate))
            .await
            .expect_err("wrong root owner group must not become ready");

        let message = err.to_string();
        assert!(
            message.contains("owner group") || message.contains("RootMountOwnerMismatch"),
            "{message}"
        );
        assert!(!readiness_gate.is_ready());
    }

    #[tokio::test]
    async fn metadata_readiness_timeout_reports_raft_reason() {
        let fixture = ReadinessFixture::new().await;

        let err = wait_for_root_ready(&fixture, Arc::new(RootReadinessGate::new(None)))
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("RaftUninitialized"));
        assert!(message.contains("root readiness timed out"));
    }
}
