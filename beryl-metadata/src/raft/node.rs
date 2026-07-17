// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata-owned OpenRaft node lifecycle and request interface.

use crate::config::RaftConfig;
use crate::error::{MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::observe;
use crate::raft::command::Command;
use crate::raft::network::SingleNodeNetworkFactory;
use crate::raft::response::CommandResult;
use crate::raft::state_machine::AppRaftStateMachine as AppStateMachine;
use crate::raft::storage::{AppLogStorage, RocksDBStorage, SnapshotInstallTracker, StateMachineStorage};
use crate::raft::types::{MetadataNode, MetadataRaftTypeConfig};
use crate::raft::MetadataReadView;
use beryl_types::RaftLogId;
use openraft::{Config, Raft, RaftMetrics, RaftTypeConfig, ServerState, SnapshotPolicy};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

/// Raft node ID type.
pub(crate) type NodeId = <MetadataRaftTypeConfig as RaftTypeConfig>::NodeId;

/// Raft node wrapper.
pub(crate) struct AppRaftNode {
    node_id: NodeId,
    raft: Arc<Raft<MetadataRaftTypeConfig>>,
    state_machine: Arc<AppStateMachine>,
    read_view: Arc<MetadataReadView>,
}

impl AppRaftNode {
    /// Create a new Raft node.
    pub async fn new(
        node_id: NodeId,
        storage: Arc<RocksDBStorage>,
        state_machine: Arc<AppStateMachine>,
        mount_table: Arc<MountTable>,
        _raft_config: &RaftConfig,
    ) -> MetadataResult<Self> {
        info!(node_id = node_id, "Initializing Raft node");

        let raft_state = storage.load_raft_state()?;
        let raft_state = Arc::new(RwLock::new(raft_state));
        let read_view = Arc::new(MetadataReadView::new(
            mount_table,
            Arc::clone(&raft_state),
            Arc::clone(&storage),
        )?);

        // Create RaftLogStorage (storage-v2)
        let snapshot_install = Arc::new(SnapshotInstallTracker::default());
        let log_store = AppLogStorage::new(
            Arc::clone(&storage),
            Arc::clone(&raft_state),
            Arc::clone(&snapshot_install),
        );

        // Create RaftStateMachine (storage-v2)
        let sm_store = StateMachineStorage::new_with_tracker(
            Arc::clone(&storage),
            Arc::clone(&state_machine),
            Arc::clone(&raft_state),
            Arc::clone(&read_view),
            snapshot_install,
        )?;

        // Create NetworkFactory
        let network_factory = SingleNodeNetworkFactory::new();

        // TODO: Set Raft timing from metadata configuration.
        // Create Raft Config
        let config = Config {
            heartbeat_interval: 1000,    // 1 second
            election_timeout_min: 5000,  // 5 seconds
            election_timeout_max: 10000, // 10 seconds
            // Explicit snapshot policy: take a snapshot every 1024 logs after the last snapshot.
            snapshot_policy: SnapshotPolicy::LogsSinceLast(1024),
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| MetadataError::Internal(format!("Invalid Raft config: {}", e)))?,
        );

        // Create Raft instance (storage-v2: separate log_store and sm_store)
        let raft = Raft::new(node_id, config, network_factory, log_store, sm_store)
            .await
            .map_err(|e| MetadataError::Internal(format!("Failed to create Raft instance: {}", e)))?;

        let is_initialized = raft
            .is_initialized()
            .await
            .map_err(|e| MetadataError::Internal(format!("Failed to check if initialized: {}", e)))?;
        if is_initialized {
            info!(node_id = node_id, "Raft cluster already initialized");
        }

        let node = Self {
            node_id,
            raft: Arc::new(raft),
            state_machine,
            read_view,
        };
        node.record_current_raft_metrics();
        Ok(node)
    }

    /// Initializes single-node membership for explicit metadata format.
    pub async fn initialize_single_node(&self, address: String) -> MetadataResult<()> {
        let is_initialized = self
            .raft
            .is_initialized()
            .await
            .map_err(|e| MetadataError::Internal(format!("Failed to check if initialized: {}", e)))?;
        if is_initialized {
            return Ok(());
        }

        let mut members = std::collections::BTreeMap::new();
        members.insert(
            self.node_id,
            MetadataNode {
                node_id: self.node_id,
                address,
            },
        );
        self.raft
            .initialize(members)
            .await
            .map_err(|e| MetadataError::Internal(format!("Failed to initialize Raft cluster: {}", e)))?;

        info!(node_id = self.node_id, "Single-node Raft cluster initialized");
        Ok(())
    }

    /// Propose a command to Raft.
    pub async fn propose(&self, command: Command) -> MetadataResult<CommandResult> {
        let started = Instant::now();
        match self.raft.client_write(command).await {
            Ok(result) => {
                self.record_current_raft_metrics();
                match result.data {
                    CommandResult::Rejected(rejection) => {
                        let error = rejection.into_metadata_error();
                        observe::record_raft_proposal(
                            "error",
                            observe::metadata_error_kind(&error),
                            started.elapsed().as_secs_f64(),
                        );
                        Err(error)
                    }
                    response => {
                        observe::record_raft_proposal("ok", "none", started.elapsed().as_secs_f64());
                        Ok(response)
                    }
                }
            }
            Err(e) => {
                let error = match e {
                    openraft::error::RaftError::APIError(api_err) => match api_err {
                        openraft::error::ClientWriteError::ForwardToLeader(forward) => {
                            if let Some(leader_id) = forward.leader_id {
                                MetadataError::LeaderChanged(format!("Leader is node {}", leader_id))
                            } else {
                                MetadataError::LeaderChanged("Leader unknown".to_string())
                            }
                        }
                        openraft::error::ClientWriteError::ChangeMembershipError(change_err) => {
                            MetadataError::Internal(format!("Membership change error: {}", change_err))
                        }
                    },
                    openraft::error::RaftError::Fatal(fatal) => {
                        MetadataError::Internal(format!("Fatal Raft error: {}", fatal))
                    }
                };
                observe::record_raft_proposal(
                    "error",
                    observe::metadata_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                self.record_current_raft_metrics();
                Err(error)
            }
        }
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics_guard = metrics.borrow();
        matches!(metrics_guard.state, ServerState::Leader)
    }

    /// Check whether Raft storage has initialized membership.
    pub async fn is_initialized(&self) -> MetadataResult<bool> {
        self.raft
            .is_initialized()
            .await
            .map_err(|e| MetadataError::Internal(format!("Failed to check if initialized: {}", e)))
    }

    /// Stop the local Raft runtime and wait for background tasks to exit.
    pub async fn shutdown(&self) -> MetadataResult<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| MetadataError::Internal(format!("Failed to shutdown Raft node: {e}")))
    }

    pub(crate) fn route_epoch(&self) -> crate::state::RouteEpoch {
        self.read_view.route_epoch()
    }

    /// Get current leader ID (if known).
    pub fn get_leader_id(&self) -> Option<NodeId> {
        let metrics = self.raft.metrics();
        let metrics_guard = metrics.borrow();
        metrics_guard.current_leader
    }

    /// Read with consistency guarantee.
    ///
    /// - leader_read: Only read from leader (maybe stale)
    /// - linearizable: Read with linearizable guarantee (uses read_index)
    pub async fn read<F, T>(&self, linearizable: bool, f: F) -> MetadataResult<T>
    where
        F: FnOnce(&AppStateMachine) -> MetadataResult<T>,
    {
        if linearizable {
            // Use read_index for linearizable read
            self.raft
                .ensure_linearizable()
                .await
                .map_err(|e| MetadataError::Internal(format!("Failed to ensure linearizability: {}", e)))?;
        } else {
            // Leader read: just check we're leader
            if !self.is_leader() {
                return Err(MetadataError::LeaderChanged(format!(
                    "Node {} is not the leader",
                    self.node_id
                )));
            }
        }

        f(self.state_machine.as_ref())
    }

    /// Get Raft metrics for monitoring.
    pub fn metrics(&self) -> RaftMetrics<u64, MetadataNode> {
        self.raft.metrics().borrow().clone()
    }

    /// Get membership information from the applied in-memory state.
    pub fn get_membership(&self) -> Option<openraft::Membership<u64, MetadataNode>> {
        let membership = self.read_view.raft_state().membership.membership().clone();
        let has_nodes = membership.nodes().next().is_some();
        has_nodes.then_some(membership)
    }

    /// Get the current state machine's last applied log ID (state_id).
    ///
    /// This represents the freshest state that has been applied to the state machine.
    /// Returns None if no log has been applied yet.
    pub fn get_last_applied_state_id(&self) -> Option<RaftLogId> {
        self.read_view.last_applied()
    }

    fn record_current_raft_metrics(&self) {
        let metrics = self.metrics();
        observe::record_raft_role(server_state_label(metrics.state));
        observe::record_raft_term(metrics.current_term);
        observe::record_raft_indexes(metrics.last_applied.map(|log_id| log_id.index), self.committed_index());
    }

    fn committed_index(&self) -> Option<u64> {
        self.read_view.committed_index()
    }
}

fn server_state_label(state: ServerState) -> &'static str {
    match state {
        ServerState::Leader => "leader",
        ServerState::Follower => "follower",
        ServerState::Candidate => "candidate",
        ServerState::Learner => "learner",
        ServerState::Shutdown => "shutdown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::format_metadata_storage;
    use crate::mount::MountTable;
    use crate::raft::storage::RocksDBStorage;
    use crate::state::{RaftStateStore, RouteEpoch, StateStore};
    use crate::MetadataConfig;
    use tempfile::TempDir;

    #[tokio::test]
    async fn applied_and_route_epoch_reads_do_not_hit_rocksdb() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        storage.put_route_epoch(RouteEpoch::new(7)).unwrap();
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppStateMachine::new(Arc::clone(&storage)));
        let node = Arc::new(
            AppRaftNode::new(
                1,
                Arc::clone(&storage),
                state_machine,
                mount_table,
                &RaftConfig::default(),
            )
            .await
            .unwrap(),
        );
        node.initialize_single_node("127.0.0.1:0".to_string()).await.unwrap();
        let route_store = RaftStateStore::new(Arc::clone(&node));

        for _ in 0..100 {
            if node.get_last_applied_state_id().is_some() && node.get_membership().is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let expected_applied = node.get_last_applied_state_id().expect("applied state");
        let expected_membership = node.get_membership().expect("membership");
        let expected_membership_nodes = node.get_membership_nodes();
        let expected_committed = node.committed_index();

        storage
            .with_pinned_db(|db| {
                let cf = db.cf_handle("raft_state").unwrap();
                db.put_cf(cf, b"raft_state", b"invalid raft state")
                    .map_err(|error| crate::MetadataError::Internal(error.to_string()))
            })
            .unwrap();
        storage
            .with_pinned_db(|db| {
                let cf = db.cf_handle("meta").unwrap();
                db.put_cf(cf, b"route_epoch", b"invalid route epoch")
                    .map_err(|error| crate::MetadataError::Internal(error.to_string()))
            })
            .unwrap();

        assert_eq!(node.get_last_applied_state_id(), Some(expected_applied));
        assert_eq!(node.get_membership(), Some(expected_membership));
        assert_eq!(node.get_membership_nodes(), expected_membership_nodes);
        assert_eq!(node.committed_index(), expected_committed);
        assert_eq!(route_store.get_route_epoch().await.unwrap(), RouteEpoch::new(7));
        node.shutdown().await.unwrap();
    }

    impl AppRaftNode {
        /// Get all node IDs from membership (leader and followers).
        pub fn get_membership_nodes(&self) -> (Option<u64>, Vec<u64>) {
            let metrics = self.raft.metrics();
            let metrics_guard = metrics.borrow();
            let leader_id = metrics_guard.current_leader;

            let follower_ids = self
                .read_view
                .raft_state()
                .membership
                .membership()
                .nodes()
                .map(|(node_id, _)| *node_id)
                .filter(|node_id| Some(*node_id) != leader_id)
                .collect();

            (leader_id, follower_ids)
        }
    }

    #[tokio::test]
    async fn metadata_format_initializes_single_node_membership() {
        let dir = TempDir::new().unwrap();
        let config = MetadataConfig {
            storage_dir: dir.path().join("metadata"),
            ..MetadataConfig::default()
        };
        format_metadata_storage(&config).await.unwrap();

        let storage = Arc::new(RocksDBStorage::create_for_format(&config.storage_dir).unwrap());
        let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
        let state_machine = Arc::new(AppStateMachine::new(Arc::clone(&storage)));
        let raft_node = AppRaftNode::new(
            config.raft.node_id,
            Arc::clone(&storage),
            state_machine,
            Arc::clone(&mount_table),
            &config.raft,
        )
        .await
        .unwrap();

        assert!(raft_node.is_initialized().await.unwrap());
    }
}
