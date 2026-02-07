// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service configuration.
//!
//! Reads configuration from core-site.yaml / client-site.yaml.

use crate::readiness::RootReadinessConfig;
use common::config::CoreConfig;
use common::error::CommonError;
use std::net::SocketAddr;
use std::path::Path;

/// Metadata service configuration.
#[derive(Clone, Debug)]
pub struct MetadataConfig {
    /// RPC server address.
    pub rpc_addr: SocketAddr,
    /// Raft configuration.
    pub raft: RaftConfig,
    /// Shard configuration.
    pub shard: ShardConfig,
    /// Worker/Repair configuration.
    pub worker: WorkerConfig,
    /// Bootstrap/readiness configuration.
    pub bootstrap: BootstrapConfig,
}

/// Bootstrap/readiness configuration.
#[derive(Clone, Debug)]
pub struct BootstrapConfig {
    pub root_readiness: RootReadinessConfig,
}

/// Worker and repair configuration.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// Max commands per heartbeat (default: 8).
    pub max_commands_per_heartbeat: usize,
    /// Repair queue configuration.
    pub repair: RepairConfig,
}

/// Repair queue configuration.
#[derive(Clone, Debug)]
pub struct RepairConfig {
    /// Max queue size (default: 10000).
    pub max_queue_size: usize,
    /// Max attempts per task (default: 3).
    pub max_attempts: u32,
    /// Inflight timeout in milliseconds (default: 300000 = 5 minutes).
    pub inflight_timeout_ms: u64,
    /// Initial backoff in milliseconds (default: 1000 = 1 second).
    pub initial_backoff_ms: u64,
    /// Max backoff in milliseconds (default: 60000 = 1 minute).
    pub max_backoff_ms: u64,
    /// Worker inflight limit (default: 4).
    pub worker_inflight_limit: usize,
}

/// Raft configuration.
/// TODO(config): extend with full Raft parameters once metadata service uses Raft.
#[derive(Clone, Debug)]
pub struct RaftConfig {
    /// Raft cluster ID.
    pub cluster_id: String,
    /// Raft node ID.
    pub node_id: u64,
    /// Raft peers (placeholder).
    pub peers: Vec<String>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            cluster_id: "vecton-metadata".to_string(),
            node_id: 1,
            peers: vec![],
        }
    }
}

/// Shard configuration.
/// TODO(config): expand once sharding is implemented.
#[derive(Clone, Debug)]
pub struct ShardConfig {
    /// Number of shards.
    pub num_shards: u64,
    /// Shard group ID.
    pub shard_group_id: u64,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            num_shards: 1,
            shard_group_id: 0,
        }
    }
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 10000,
            max_attempts: 3,
            inflight_timeout_ms: 300_000, // 5 minutes
            initial_backoff_ms: 1_000,    // 1 second
            max_backoff_ms: 60_000,       // 1 minute
            worker_inflight_limit: 4,
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_commands_per_heartbeat: 8,
            repair: RepairConfig::default(),
        }
    }
}

impl MetadataConfig {
    /// Load configuration from core-site.yaml.
    pub fn load<P: AsRef<Path>>(config_path: P) -> Result<Self, CommonError> {
        let core_config = CoreConfig::load(config_path)?;
        Self::from_core_config(core_config)
    }

    /// Create from CoreConfig.
    pub fn from_core_config(core_config: CoreConfig) -> Result<Self, CommonError> {
        let flat = core_config.as_flat();

        // Read RPC address
        let addr = flat
            .get_str("metadata.rpc.addr")
            .unwrap_or_else(|| "0.0.0.0".to_string());
        let port = flat.get_i64("metadata.rpc.port").unwrap_or(18080) as u16;
        let rpc_addr = format!("{}:{}", addr, port).parse().map_err(|e| {
            CommonError::new(
                common::error::CommonErrorCode::InvalidArgument,
                format!("Invalid metadata.rpc.addr/port: {}", e),
            )
        })?;

        // Read Raft config
        let peers = if let Some(peers_str) = flat.get_str("metadata.raft.peers") {
            // Parse comma-separated list of peers
            peers_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            vec![]
        };

        let raft = RaftConfig {
            cluster_id: flat
                .get_str("metadata.raft.cluster_id")
                .unwrap_or_else(|| "vecton-metadata".to_string()),
            node_id: flat.get_i64("metadata.raft.node_id").unwrap_or(1) as u64,
            peers,
        };

        // Read Shard config
        let shard = ShardConfig {
            num_shards: flat.get_i64("metadata.shard.num_shards").unwrap_or(1) as u64,
            shard_group_id: flat.get_i64("metadata.shard.group_id").unwrap_or(0) as u64,
        };

        // Read Worker/Repair config
        let repair = RepairConfig {
            max_queue_size: flat.get_i64("metadata.repair.max_queue_size").unwrap_or(10000) as usize,
            max_attempts: flat.get_i64("metadata.repair.max_attempts").unwrap_or(3) as u32,
            inflight_timeout_ms: flat.get_i64("metadata.repair.inflight_timeout_ms").unwrap_or(300_000) as u64,
            initial_backoff_ms: flat.get_i64("metadata.repair.initial_backoff_ms").unwrap_or(1_000) as u64,
            max_backoff_ms: flat.get_i64("metadata.repair.max_backoff_ms").unwrap_or(60_000) as u64,
            worker_inflight_limit: flat.get_i64("metadata.repair.worker_inflight_limit").unwrap_or(4) as usize,
        };

        let worker = WorkerConfig {
            max_commands_per_heartbeat: flat.get_i64("metadata.worker.max_commands_per_heartbeat").unwrap_or(8)
                as usize,
            repair,
        };

        let root_readiness = RootReadinessConfig {
            initial_backoff_ms: flat
                .get_i64("metadata.bootstrap.root_ready_initial_backoff_ms")
                .unwrap_or(200) as u64,
            max_backoff_ms: flat
                .get_i64("metadata.bootstrap.root_ready_max_backoff_ms")
                .unwrap_or(5_000) as u64,
            warn_after_ms: flat
                .get_i64("metadata.bootstrap.root_ready_warn_after_ms")
                .unwrap_or(60_000) as u64,
        };

        let bootstrap = BootstrapConfig { root_readiness };

        Ok(Self {
            rpc_addr,
            raft,
            shard,
            worker,
            bootstrap,
        })
    }
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            rpc_addr: "0.0.0.0:18080".parse().unwrap(),
            raft: RaftConfig::default(),
            shard: ShardConfig::default(),
            worker: WorkerConfig::default(),
            bootstrap: BootstrapConfig {
                root_readiness: RootReadinessConfig::default(),
            },
        }
    }
}
