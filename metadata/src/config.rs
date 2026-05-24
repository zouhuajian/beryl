// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service configuration.
//!
//! Reads configuration from core-site.yaml / client-site.yaml.

use crate::readiness::RootReadinessConfig;
use common::config::CoreConfig;
use common::error::{CommonError, CommonErrorCode};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

const METADATA_RPC_ADDR: &str = "metadata.rpc.addr";
const METADATA_RPC_PORT: &str = "metadata.rpc.port";
const METADATA_STORAGE_DIR: &str = "metadata.storage.dir";
const METADATA_AUTHZ_FILESYSTEM_MODE: &str = "metadata.authz.filesystem.mode";
const METADATA_RAFT_NODE_ID: &str = "metadata.raft.node_id";
const METADATA_RAFT_PEERS: &str = "metadata.raft.peers";
const METADATA_AUTHORITY_GROUP_ID: &str = "metadata.authority.group_id";
const METADATA_REPAIR_MAX_QUEUE_SIZE: &str = "metadata.repair.max_queue_size";
const METADATA_REPAIR_MAX_ATTEMPTS: &str = "metadata.repair.max_attempts";
const METADATA_REPAIR_INFLIGHT_TIMEOUT_MS: &str = "metadata.repair.inflight_timeout_ms";
const METADATA_REPAIR_INITIAL_BACKOFF_MS: &str = "metadata.repair.initial_backoff_ms";
const METADATA_REPAIR_MAX_BACKOFF_MS: &str = "metadata.repair.max_backoff_ms";
const METADATA_REPAIR_WORKER_INFLIGHT_LIMIT: &str = "metadata.repair.worker_inflight_limit";
const METADATA_BOOTSTRAP_ROOT_READY_INITIAL_BACKOFF_MS: &str = "metadata.bootstrap.root_ready_initial_backoff_ms";
const METADATA_BOOTSTRAP_ROOT_READY_MAX_BACKOFF_MS: &str = "metadata.bootstrap.root_ready_max_backoff_ms";
const METADATA_BOOTSTRAP_ROOT_READY_WARN_AFTER_MS: &str = "metadata.bootstrap.root_ready_warn_after_ms";

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
#[derive(Clone, Debug, Default)]
pub struct WorkerConfig {
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
#[derive(Clone, Debug)]
pub struct MetadataAuthorityConfig {
    /// Authority group ID for the root namespace owner served by this metadata runtime.
    pub group_id: u64,
}

impl Default for MetadataAuthorityConfig {
    fn default() -> Self {
        Self { group_id: 1 }
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
        let addr = get_str_or(flat, METADATA_RPC_ADDR, "0.0.0.0")?;
        let port = match get_i64_if_present(flat, METADATA_RPC_PORT)?.unwrap_or(18080) {
            port @ 1..=65535 => port as u16,
            port => {
                return Err(CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    format!("{METADATA_RPC_PORT} must be in range 1-65535, got {port}"),
                ));
            }
        };
        let rpc_addr = format!("{}:{}", addr, port).parse().map_err(|e| {
            CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("Invalid metadata.rpc.addr/port: {}", e),
            )
        })?;

        let storage_dir = PathBuf::from(get_str_or(flat, METADATA_STORAGE_DIR, "data/metadata")?);

        let filesystem_mode_raw = get_str_or(flat, METADATA_AUTHZ_FILESYSTEM_MODE, "NONE")?;
        let filesystem_mode = FileSystemAuthzMode::parse(&filesystem_mode_raw).ok_or_else(|| {
            CommonError::new(
                CommonErrorCode::InvalidArgument,
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
        let peers = get_str_or(flat, METADATA_RAFT_PEERS, "")?
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let raft = RaftConfig {
            node_id: get_positive_u64_or(flat, METADATA_RAFT_NODE_ID, 1)?,
            peers,
        };

        let authority = MetadataAuthorityConfig {
            group_id: get_u64_or(
                flat,
                METADATA_AUTHORITY_GROUP_ID,
                MetadataAuthorityConfig::default().group_id,
            )?,
        };

        // Read Worker/Repair config
        let repair = RepairConfig {
            max_queue_size: get_positive_usize_or(flat, METADATA_REPAIR_MAX_QUEUE_SIZE, 10000)?,
            max_attempts: get_positive_u32_or(flat, METADATA_REPAIR_MAX_ATTEMPTS, 3)?,
            inflight_timeout_ms: get_positive_u64_or(flat, METADATA_REPAIR_INFLIGHT_TIMEOUT_MS, 300_000)?,
            initial_backoff_ms: get_positive_u64_or(flat, METADATA_REPAIR_INITIAL_BACKOFF_MS, 1_000)?,
            max_backoff_ms: get_positive_u64_or(flat, METADATA_REPAIR_MAX_BACKOFF_MS, 60_000)?,
            worker_inflight_limit: get_positive_usize_or(flat, METADATA_REPAIR_WORKER_INFLIGHT_LIMIT, 4)?,
        };

        let worker = WorkerConfig { repair };

        let root_readiness = RootReadinessConfig {
            initial_backoff_ms: get_positive_u64_or(flat, METADATA_BOOTSTRAP_ROOT_READY_INITIAL_BACKOFF_MS, 200)?,
            max_backoff_ms: get_positive_u64_or(flat, METADATA_BOOTSTRAP_ROOT_READY_MAX_BACKOFF_MS, 5_000)?,
            warn_after_ms: get_positive_u64_or(flat, METADATA_BOOTSTRAP_ROOT_READY_WARN_AFTER_MS, 60_000)?,
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

fn get_i64_if_present(flat: &common::config::FlatConfig, key: &'static str) -> Result<Option<i64>, CommonError> {
    if let Some(value) = flat.get_i64(key) {
        return Ok(Some(value));
    }
    if flat.contains_key(key) {
        return Err(invalid_config(key, "must be an integer"));
    }
    Ok(None)
}

fn get_str_or(
    flat: &common::config::FlatConfig,
    key: &'static str,
    default: &'static str,
) -> Result<String, CommonError> {
    if let Some(value) = flat.get_str(key) {
        return Ok(value);
    }
    if flat.contains_key(key) {
        return Err(invalid_config(key, "must be a string"));
    }
    Ok(default.to_string())
}

fn get_u64_or(flat: &common::config::FlatConfig, key: &'static str, default: u64) -> Result<u64, CommonError> {
    let Some(value) = get_i64_if_present(flat, key)? else {
        return Ok(default);
    };
    u64::try_from(value).map_err(|_| invalid_config(key, "must be non-negative"))
}

fn get_positive_u64_or(flat: &common::config::FlatConfig, key: &'static str, default: u64) -> Result<u64, CommonError> {
    let value = get_u64_or(flat, key, default)?;
    if value == 0 {
        return Err(invalid_config(key, "must be greater than zero"));
    }
    Ok(value)
}

fn get_positive_usize_or(
    flat: &common::config::FlatConfig,
    key: &'static str,
    default: usize,
) -> Result<usize, CommonError> {
    let Some(value) = get_i64_if_present(flat, key)? else {
        return Ok(default);
    };
    let value = usize::try_from(value).map_err(|_| invalid_config(key, "must fit usize"))?;
    if value == 0 {
        return Err(invalid_config(key, "must be greater than zero"));
    }
    Ok(value)
}

fn get_positive_u32_or(flat: &common::config::FlatConfig, key: &'static str, default: u32) -> Result<u32, CommonError> {
    let Some(value) = get_i64_if_present(flat, key)? else {
        return Ok(default);
    };
    let value = u32::try_from(value).map_err(|_| invalid_config(key, "must fit u32"))?;
    if value == 0 {
        return Err(invalid_config(key, "must be greater than zero"));
    }
    Ok(value)
}

fn invalid_config(key: &'static str, detail: &'static str) -> CommonError {
    CommonError::new(CommonErrorCode::InvalidArgument, format!("{key} {detail}"))
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
    fn rpc_port_rejects_out_of_range_value() {
        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set("metadata.rpc.port", 70000i64);

        let err = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap_err();
        assert!(err.message.contains("metadata.rpc.port"));
    }

    #[test]
    fn rpc_port_rejects_present_non_integer_value() {
        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set("metadata.rpc.port", true);

        let err = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap_err();
        assert!(err.message.contains("metadata.rpc.port"));
    }

    #[test]
    fn string_keys_reject_present_wrong_type_values() {
        for key in [
            METADATA_RPC_ADDR,
            METADATA_STORAGE_DIR,
            METADATA_AUTHZ_FILESYSTEM_MODE,
            METADATA_RAFT_PEERS,
        ] {
            let mut flat = CoreConfig::default().as_flat().clone();
            flat.set(key, true);

            let err = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap_err();

            assert!(
                err.message.contains(key),
                "error for {key} should mention the offending key: {}",
                err.message
            );
        }
    }

    #[test]
    fn removed_shard_group_id_key_is_ignored() {
        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set("metadata.shard.group_id", 9i64);

        let config = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap();
        assert_eq!(config.authority.group_id, 1);
    }

    #[test]
    fn authz_filesystem_rejects_unknown_mode() {
        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set("metadata.authz.filesystem.mode", "UNKNOWN");
        let err = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap_err();
        assert!(err.message.contains("metadata.authz.filesystem.mode"));
        assert!(err.message.contains("NONE|RANGER|ACL"));
    }

    #[test]
    fn absent_numeric_keys_use_metadata_defaults() {
        let config = MetadataConfig::from_core_config(CoreConfig::default()).unwrap();

        assert_eq!(config.raft.node_id, 1);
        assert_eq!(config.authority.group_id, 1);
        assert_eq!(config.worker.repair.max_queue_size, 10000);
        assert_eq!(config.worker.repair.max_attempts, 3);
        assert_eq!(config.worker.repair.inflight_timeout_ms, 300_000);
        assert_eq!(config.worker.repair.initial_backoff_ms, 1_000);
        assert_eq!(config.worker.repair.max_backoff_ms, 60_000);
        assert_eq!(config.worker.repair.worker_inflight_limit, 4);
        assert_eq!(config.bootstrap.root_readiness.initial_backoff_ms, 200);
        assert_eq!(config.bootstrap.root_readiness.max_backoff_ms, 5_000);
        assert_eq!(config.bootstrap.root_readiness.warn_after_ms, 60_000);
    }

    #[test]
    fn unsigned_numeric_keys_reject_negative_values() {
        for key in [
            METADATA_RAFT_NODE_ID,
            METADATA_AUTHORITY_GROUP_ID,
            METADATA_REPAIR_MAX_QUEUE_SIZE,
            METADATA_REPAIR_MAX_ATTEMPTS,
            METADATA_REPAIR_INFLIGHT_TIMEOUT_MS,
            METADATA_REPAIR_INITIAL_BACKOFF_MS,
            METADATA_REPAIR_MAX_BACKOFF_MS,
            METADATA_REPAIR_WORKER_INFLIGHT_LIMIT,
            METADATA_BOOTSTRAP_ROOT_READY_INITIAL_BACKOFF_MS,
            METADATA_BOOTSTRAP_ROOT_READY_MAX_BACKOFF_MS,
            METADATA_BOOTSTRAP_ROOT_READY_WARN_AFTER_MS,
        ] {
            let mut flat = CoreConfig::default().as_flat().clone();
            flat.set(key, -1i64);

            let err = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap_err();

            assert!(
                err.message.contains(key),
                "error for {key} should mention the offending key: {}",
                err.message
            );
        }
    }

    #[test]
    fn positive_numeric_keys_reject_zero_values() {
        for key in [
            METADATA_RAFT_NODE_ID,
            METADATA_REPAIR_MAX_QUEUE_SIZE,
            METADATA_REPAIR_MAX_ATTEMPTS,
            METADATA_REPAIR_INFLIGHT_TIMEOUT_MS,
            METADATA_REPAIR_INITIAL_BACKOFF_MS,
            METADATA_REPAIR_MAX_BACKOFF_MS,
            METADATA_REPAIR_WORKER_INFLIGHT_LIMIT,
            METADATA_BOOTSTRAP_ROOT_READY_INITIAL_BACKOFF_MS,
            METADATA_BOOTSTRAP_ROOT_READY_MAX_BACKOFF_MS,
            METADATA_BOOTSTRAP_ROOT_READY_WARN_AFTER_MS,
        ] {
            let mut flat = CoreConfig::default().as_flat().clone();
            flat.set(key, 0i64);

            let err = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap_err();

            assert!(
                err.message.contains(key),
                "error for {key} should mention the offending key: {}",
                err.message
            );
        }

        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set(METADATA_AUTHORITY_GROUP_ID, 0i64);
        let config = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap();
        assert_eq!(config.authority.group_id, 0);
    }

    #[test]
    fn metadata_repair_max_attempts_rejects_u32_overflow() {
        let mut flat = CoreConfig::default().as_flat().clone();
        flat.set(METADATA_REPAIR_MAX_ATTEMPTS, i64::from(u32::MAX) + 1);

        let err = MetadataConfig::from_core_config(CoreConfig::from_flat(flat)).unwrap_err();

        assert!(err.message.contains(METADATA_REPAIR_MAX_ATTEMPTS));
    }
}
