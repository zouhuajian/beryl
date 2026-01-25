// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft node wrapper for openraft.
//!
//! This module wraps openraft to provide a simplified interface for the metadata service.

use crate::config::RaftConfig;
use crate::error::{MetadataError, MetadataResult};
use crate::raft::command::Command;
use crate::raft::log_store::AppLogStorage;
use crate::raft::network::NetworkFactory;
use crate::raft::state_machine::AppRaftStateMachine as AppStateMachine;
use crate::raft::state_machine_store::StateMachineStorage;
use crate::raft::storage::RocksDBStorage;
use crate::raft::types::{AppMetadataRaftState, MetadataNode, MetadataRaftTypeConfig};
use crate::raft_conv;
use openraft::{Config, Raft, RaftMetrics, RaftTypeConfig, ServerState, SnapshotPolicy};
use parking_lot::RwLock;
use serde_json;
use std::sync::Arc;
use tracing::info;
use types::RaftLogId;

/// Raft node ID type.
pub type NodeId = <MetadataRaftTypeConfig as RaftTypeConfig>::NodeId;

/// Raft node wrapper.
pub struct AppRaftNode {
    node_id: NodeId,
    raft: Arc<Raft<MetadataRaftTypeConfig>>,
    state_machine: Arc<AppStateMachine>,
    _storage: Arc<RocksDBStorage>,
}

impl AppRaftNode {
    /// Create a new Raft node.
    pub async fn new(
        node_id: NodeId,
        storage: Arc<RocksDBStorage>,
        state_machine: Arc<AppStateMachine>,
        raft_config: &RaftConfig,
    ) -> MetadataResult<Self> {
        info!(node_id = node_id, "Initializing Raft node");

        // Load persisted state from RocksDB
        let raft_state = if let Some(state_data) = storage.get_raft_state()? {
            serde_json::from_slice(&state_data)
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize raft state: {}", e)))?
        } else {
            AppMetadataRaftState::default()
        };
        let raft_state = Arc::new(RwLock::new(raft_state));

        // Create RaftLogStorage (storage-v2)
        let log_store = AppLogStorage::new(Arc::clone(&storage), Arc::clone(&raft_state));

        // Create RaftStateMachine (storage-v2)
        let sm_store = StateMachineStorage::new(
            Arc::clone(&storage),
            Arc::clone(&state_machine),
            Arc::clone(&raft_state),
        )?;

        // Create NetworkFactory
        let network_factory = NetworkFactory::new();

        // TODO: Set config from core-site.yaml
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

        // Bootstrap if this is the first node and configured to bootstrap
        if !raft_config.peers.is_empty() {
            let is_initialized = raft
                .is_initialized()
                .await
                .map_err(|e| MetadataError::Internal(format!("Failed to check if initialized: {}", e)))?;

            if !is_initialized {
                // Bootstrap the cluster
                let mut members = std::collections::BTreeMap::new();
                for (idx, peer) in raft_config.peers.iter().enumerate() {
                    let peer_node_id = (idx + 1) as u64;
                    let node = MetadataNode {
                        node_id: peer_node_id,
                        address: peer.clone(),
                    };
                    members.insert(peer_node_id, node);
                }

                // Initialize with BTreeMap directly (it implements IntoNodes)
                raft.initialize(members)
                    .await
                    .map_err(|e| MetadataError::Internal(format!("Failed to initialize Raft cluster: {}", e)))?;

                info!(node_id = node_id, "Raft cluster initialized");
            } else {
                info!(node_id = node_id, "Raft cluster already initialized");
            }
        }

        Ok(Self {
            node_id,
            raft: Arc::new(raft),
            state_machine,
            _storage: storage,
        })
    }

    /// Propose a command to Raft.
    pub async fn propose(&self, command: Command) -> MetadataResult<Vec<u8>> {
        // Use openraft client_write API
        let result = self.raft.client_write(command).await.map_err(|e| {
            // Map openraft errors to MetadataError
            // e is RaftError<u64, ClientWriteError<u64, MetadataNode>>
            match e {
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
            }
        })?;

        // Extract response from ClientWriteResponse
        // The response is Vec<u8> (MetadataRaftTypeConfig::R)
        Ok(result.data)
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics_guard = metrics.borrow();
        matches!(metrics_guard.state, ServerState::Leader)
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

        f(&self.state_machine)
    }

    /// Get Raft metrics for monitoring.
    pub fn metrics(&self) -> RaftMetrics<u64, MetadataNode> {
        self.raft.metrics().borrow().clone()
    }

    /// Get all node IDs from membership (leader and followers).
    pub fn get_membership_nodes(&self) -> (Option<u64>, Vec<u64>) {
        let metrics = self.raft.metrics();
        let metrics_guard = metrics.borrow();
        let leader_id = metrics_guard.current_leader;

        // Get all node IDs from membership via storage
        let follower_ids: Vec<u64> = {
            // Try to get membership from storage
            if let Ok(Some(state_data)) = self._storage.get_raft_state() {
                if let Ok(raft_state) = serde_json::from_slice::<AppMetadataRaftState>(&state_data) {
                    // Extract all node IDs from membership, exclude leader
                    // openraft::Membership::nodes() returns an iterator over (NodeId, &Node)
                    // We need to collect it into a Vec first, then filter
                    let all_nodes: Vec<u64> = raft_state.membership.nodes().map(|(node_id, _)| *node_id).collect();

                    // Filter out leader
                    all_nodes
                        .into_iter()
                        .filter(|&node_id| Some(node_id) != leader_id)
                        .collect()
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        };

        (leader_id, follower_ids)
    }

    /// Get membership information from storage.
    pub fn get_membership(&self) -> Option<openraft::Membership<u64, MetadataNode>> {
        if let Ok(Some(state_data)) = self._storage.get_raft_state() {
            if let Ok(raft_state) = serde_json::from_slice::<AppMetadataRaftState>(&state_data) {
                return Some(raft_state.membership);
            }
        }
        None
    }

    /// Get the current state machine's last applied log ID (state_id).
    ///
    /// This represents the freshest state that has been applied to the state machine.
    /// Returns None if no log has been applied yet.
    pub fn get_last_applied_state_id(&self) -> Option<RaftLogId> {
        // Get from storage state (last_applied_log_id)
        if let Ok(Some(state_data)) = self._storage.get_raft_state() {
            if let Ok(raft_state) = serde_json::from_slice::<AppMetadataRaftState>(&state_data) {
                return raft_state.last_applied_log_id.map(raft_conv::from_openraft_log_id);
            }
        }
        None
    }
}
