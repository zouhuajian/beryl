// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service configuration.
//!
//! Reads configuration from core-site.yaml / client-site.yaml.

use crate::readiness::RootReadinessConfig;
use common::config::{metadata_authority, metadata_raft, metadata_rpc, metadata_storage, CoreConfig};
use common::error::CommonError;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Metadata service configuration.
#[derive(Clone, Debug)]
pub struct MetadataConfig {
    /// RPC server address.
    pub rpc_addr: SocketAddr,
    /// Local directory for metadata persistent state.
    pub storage_dir: PathBuf,
    /// Authz mode configuration.
    pub authz: MetadataAuthzConfig,
    /// Raft configuration.
    pub raft: RaftConfig,
    /// Metadata authority configuration.
    pub authority: MetadataAuthorityConfig,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FileSystemAuthzMode {
    #[default]
    None,
    Ranger,
    Acl,
}

impl FileSystemAuthzMode {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "NONE" => Some(Self::None),
            "RANGER" => Some(Self::Ranger),
            "ACL" => Some(Self::Acl),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileSystemAuthzConfig {
    pub mode: FileSystemAuthzMode,
}

#[derive(Clone, Debug)]
pub struct MetadataAuthzConfig {
    pub filesystem: FileSystemAuthzConfig,
}

impl Default for MetadataAuthzConfig {
    fn default() -> Self {
        Self {
            filesystem: FileSystemAuthzConfig {
                mode: FileSystemAuthzMode::None,
            },
        }
    }
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
#[derive(Clone, Debug)]
pub struct RaftConfig {
    /// Raft node ID.
    pub node_id: u64,
    /// Raft peers inside the configured authority group.
    pub peers: Vec<String>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            peers: vec![],
        }
    }
}

/// Metadata authority group served by this runtime.
#[derive(Clone, Debug, Default)]
pub struct MetadataAuthorityConfig {
    /// Authority group ID for the root namespace owner served by this metadata runtime.
    pub group_id: u64,
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
            .get_str(metadata_rpc::ADDR)
            .unwrap_or_else(|| "0.0.0.0".to_string());
        let port = flat.get_i64(metadata_rpc::PORT).unwrap_or(18080) as u16;
        let rpc_addr = format!("{}:{}", addr, port).parse().map_err(|e| {
            CommonError::new(
                common::error::CommonErrorCode::InvalidArgument,
                format!("Invalid metadata.rpc.addr/port: {}", e),
            )
        })?;

        let storage_dir = PathBuf::from(
            flat.get_str(metadata_storage::DIR)
                .unwrap_or_else(|| "data/metadata".to_string()),
        );

        let filesystem_mode_raw = flat
            .get_str("metadata.authz.filesystem.mode")
            .unwrap_or_else(|| "NONE".to_string());
        let filesystem_mode = FileSystemAuthzMode::parse(&filesystem_mode_raw).ok_or_else(|| {
            CommonError::new(
                common::error::CommonErrorCode::InvalidArgument,
                format!(
                    "Invalid metadata.authz.filesystem.mode={}, expected one of NONE|RANGER|ACL",
                    filesystem_mode_raw
                ),
            )
        })?;

        let authz = MetadataAuthzConfig {
            filesystem: FileSystemAuthzConfig { mode: filesystem_mode },
        };

        // Read Raft config
        let peers = if let Some(peers_str) = flat.get_str(metadata_raft::PEERS) {
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
            node_id: flat.get_i64(metadata_raft::NODE_ID).unwrap_or(1) as u64,
            peers,
        };

        let authority = MetadataAuthorityConfig {
            group_id: flat.get_i64(metadata_authority::GROUP_ID).unwrap_or(0) as u64,
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
            storage_dir,
            authz,
            raft,
            authority,
            worker,
            bootstrap,
        })
    }
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            rpc_addr: "0.0.0.0:18080".parse().unwrap(),
            storage_dir: PathBuf::from("data/metadata"),
            authz: MetadataAuthzConfig::default(),
            raft: RaftConfig::default(),
            authority: MetadataAuthorityConfig::default(),
            worker: WorkerConfig::default(),
            bootstrap: BootstrapConfig {
                root_readiness: RootReadinessConfig::default(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::CoreConfig;

    #[test]
    fn authz_mode_defaults_to_none() {
        let config = MetadataConfig::default();
        assert_eq!(config.authz.filesystem.mode, FileSystemAuthzMode::None);
        assert_eq!(config.storage_dir, std::path::PathBuf::from("data/metadata"));
    }

    #[test]
    fn authz_mode_parses_valid_values() {
        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set("metadata.authz.filesystem.mode", "acl");

        let config = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap();
        assert_eq!(config.authz.filesystem.mode, FileSystemAuthzMode::Acl);
    }

    #[test]
    fn authority_group_id_parses_from_authority_key() {
        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set("metadata.authority.group_id", 7i64);

        let config = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap();
        assert_eq!(config.authority.group_id, 7);
    }

    #[test]
    fn storage_dir_parses_from_metadata_storage_key() {
        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set("metadata.storage.dir", "/var/lib/vecton/metadata");

        let config = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap();
        assert_eq!(config.storage_dir, std::path::PathBuf::from("/var/lib/vecton/metadata"));
    }

    #[test]
    fn legacy_shard_group_id_key_is_not_a_compatibility_bridge() {
        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set("metadata.shard.group_id", 9i64);

        let config = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap();
        assert_eq!(config.authority.group_id, 0);
    }

    #[test]
    fn authz_filesystem_rejects_unknown_mode() {
        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set("metadata.authz.filesystem.mode", "UNKNOWN");
        let err = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap_err();
        assert!(err.message.contains("metadata.authz.filesystem.mode"));
        assert!(err.message.contains("NONE|RANGER|ACL"));
    }
}
