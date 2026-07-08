// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service configuration.
//!
//! Reads metadata configuration from server YAML files.

use crate::readiness::RootReadinessConfig;
use common::config::ServerConfig;
use common::error::{CommonError, CommonErrorKind};
use common::observe::ObservabilityConfig;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use types::GroupName;

const VECTON_CLUSTER_ID: &str = "vecton.cluster.id";
const METADATA_RPC_ADDR: &str = "metadata.rpc.addr";
const METADATA_RPC_PORT: &str = "metadata.rpc.port";
const METADATA_GROUP_NAME: &str = "metadata.group.name";
const METADATA_STORAGE_DIR: &str = "metadata.storage.dir";
const METADATA_RAFT_MODE: &str = "metadata.raft.mode";
const METADATA_RAFT_NODE_ID: &str = "metadata.raft.node_id";
const METADATA_REPAIR_MAX_QUEUE_SIZE: &str = "metadata.repair.max_queue_size";
const METADATA_REPAIR_MAX_ATTEMPTS: &str = "metadata.repair.max_attempts";
const METADATA_REPAIR_INFLIGHT_TIMEOUT_MS: &str = "metadata.repair.inflight_timeout_ms";
const METADATA_REPAIR_INITIAL_BACKOFF_MS: &str = "metadata.repair.initial_backoff_ms";
const METADATA_REPAIR_MAX_BACKOFF_MS: &str = "metadata.repair.max_backoff_ms";
const METADATA_REPAIR_WORKER_INFLIGHT_LIMIT: &str = "metadata.repair.worker_inflight_limit";
const METADATA_BOOTSTRAP_ROOT_READY_INITIAL_BACKOFF_MS: &str = "metadata.bootstrap.root_ready_initial_backoff_ms";
const METADATA_BOOTSTRAP_ROOT_READY_MAX_BACKOFF_MS: &str = "metadata.bootstrap.root_ready_max_backoff_ms";
const METADATA_BOOTSTRAP_ROOT_READY_WARN_AFTER_MS: &str = "metadata.bootstrap.root_ready_warn_after_ms";
const METADATA_BOOTSTRAP_READY_TIMEOUT_MS: &str = "metadata.bootstrap.ready.timeout_ms";
const METADATA_BOOTSTRAP_READY_WARN_AFTER_MS: &str = "metadata.bootstrap.ready.warn_after_ms";
const METADATA_BOOTSTRAP_READY_FAIL_FAST: &str = "metadata.bootstrap.ready.fail_fast";

/// Metadata service configuration.
#[derive(Clone, Debug)]
pub struct MetadataConfig {
    /// Cluster identity shared by local metadata and worker storage markers.
    pub cluster_id: String,
    /// RPC server address.
    pub rpc_addr: SocketAddr,
    /// Local directory for metadata persistent state.
    pub storage_dir: PathBuf,
    /// Raft configuration.
    pub raft: RaftConfig,
    /// Metadata authority configuration.
    pub authority: MetadataAuthorityConfig,
    /// Worker/Repair configuration.
    pub worker: WorkerConfig,
    /// Readiness configuration.
    pub bootstrap: BootstrapConfig,
    /// Shared observability configuration.
    pub observability: ObservabilityConfig,
}

/// Bootstrap/readiness configuration.
#[derive(Clone, Debug)]
pub struct BootstrapConfig {
    pub root_readiness: RootReadinessConfig,
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
    /// Raft startup mode for this metadata process.
    pub mode: RaftMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftMode {
    Single,
    Cluster,
}

impl RaftMode {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "single" => Some(Self::Single),
            "cluster" => Some(Self::Cluster),
            _ => None,
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            mode: RaftMode::Single,
        }
    }
}

/// Metadata authority group served by this runtime.
#[derive(Clone, Debug)]
pub struct MetadataAuthorityConfig {
    /// Stable identity for the metadata group served by this runtime.
    pub group_name: GroupName,
}

impl Default for MetadataAuthorityConfig {
    fn default() -> Self {
        Self {
            group_name: GroupName::parse("root").expect("default group name is valid"),
        }
    }
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 10000,
            max_attempts: 3,
            inflight_timeout_ms: 300_000,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 60_000,
            worker_inflight_limit: 4,
        }
    }
}

impl MetadataConfig {
    /// Load metadata configuration from a YAML file.
    pub fn load<P: AsRef<Path>>(config_path: P) -> Result<Self, CommonError> {
        let server_config = ServerConfig::load(config_path)?;
        Self::from_server_config(server_config)
    }

    /// Create from ServerConfig.
    pub fn from_server_config(server_config: ServerConfig) -> Result<Self, CommonError> {
        let flat = server_config.as_flat();

        let cluster_id = get_str_or(flat, VECTON_CLUSTER_ID, "local-vecton")?;
        if cluster_id.trim().is_empty() {
            return Err(invalid_config(VECTON_CLUSTER_ID, "must not be empty"));
        }

        let rpc_addr = rpc_addr_from_config(flat)?;
        let observability = ObservabilityConfig::from_flat(flat)?;
        let storage_dir = PathBuf::from(get_str_or(flat, METADATA_STORAGE_DIR, "data/metadata")?);

        let raft_mode_raw = get_str_or(flat, METADATA_RAFT_MODE, "single")?;
        let raft_mode = RaftMode::parse(&raft_mode_raw)
            .ok_or_else(|| invalid_config(METADATA_RAFT_MODE, "must be single or cluster"))?;
        let raft = RaftConfig {
            node_id: get_positive_u64_or(flat, METADATA_RAFT_NODE_ID, 1)?,
            mode: raft_mode,
        };

        let group_name_raw = get_str_or(flat, METADATA_GROUP_NAME, "root")?;
        let authority = MetadataAuthorityConfig {
            group_name: parse_group_name(METADATA_GROUP_NAME, group_name_raw)?,
        };

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
            warn_after_ms: get_positive_u64_or_any(
                flat,
                &[
                    METADATA_BOOTSTRAP_READY_WARN_AFTER_MS,
                    METADATA_BOOTSTRAP_ROOT_READY_WARN_AFTER_MS,
                ],
                60_000,
            )?,
            timeout_ms: get_positive_u64_or(flat, METADATA_BOOTSTRAP_READY_TIMEOUT_MS, 120_000)?,
            fail_fast: get_bool_or(flat, METADATA_BOOTSTRAP_READY_FAIL_FAST, false)?,
        };
        let bootstrap = BootstrapConfig { root_readiness };

        Ok(Self {
            cluster_id,
            rpc_addr,
            storage_dir,
            raft,
            authority,
            worker,
            bootstrap,
            observability,
        })
    }
}

fn parse_group_name(key: &'static str, raw: String) -> Result<GroupName, CommonError> {
    GroupName::parse(raw).map_err(|err| CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {err}")))
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

fn rpc_addr_from_config(flat: &common::config::FlatConfig) -> Result<SocketAddr, CommonError> {
    let addr = get_str_or(flat, METADATA_RPC_ADDR, "0.0.0.0")?;
    let port = match get_i64_if_present(flat, METADATA_RPC_PORT)?.unwrap_or(18080) {
        port @ 1..=65535 => port as u16,
        port => {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("{METADATA_RPC_PORT} must be in range 1-65535, got {port}"),
            ));
        }
    };
    format!("{}:{}", addr, port).parse().map_err(|e| {
        CommonError::new(
            CommonErrorKind::InvalidArgument,
            format!("Invalid metadata.rpc.addr/port: {}", e),
        )
    })
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

fn get_bool_or(flat: &common::config::FlatConfig, key: &'static str, default: bool) -> Result<bool, CommonError> {
    if let Some(value) = flat.get_bool(key) {
        return Ok(value);
    }
    if flat.contains_key(key) {
        return Err(invalid_config(key, "must be a boolean"));
    }
    Ok(default)
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

fn get_positive_u64_or_any(
    flat: &common::config::FlatConfig,
    keys: &[&'static str],
    default: u64,
) -> Result<u64, CommonError> {
    for key in keys {
        if flat.contains_key(key) {
            let value = get_u64_or(flat, key, default)?;
            if value == 0 {
                return Err(invalid_config(key, "must be greater than zero"));
            }
            return Ok(value);
        }
    }
    Ok(default)
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
    CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::ServerConfig;

    impl Default for MetadataConfig {
        fn default() -> Self {
            Self {
                cluster_id: "local-vecton".to_string(),
                rpc_addr: "0.0.0.0:18080".parse().unwrap(),
                storage_dir: PathBuf::from("data/metadata"),
                raft: RaftConfig::default(),
                authority: MetadataAuthorityConfig::default(),
                worker: WorkerConfig::default(),
                bootstrap: BootstrapConfig {
                    root_readiness: RootReadinessConfig::default(),
                },
                observability: test_observability_config(),
            }
        }
    }

    fn test_observability_config() -> ObservabilityConfig {
        let mut flat = common::config::FlatConfig::new();
        flat.set("observe.log.format", "compact");
        flat.set("observe.log.output", "stderr");
        flat.set(
            "observe.log.level",
            "info,vecton=info,metadata=info,worker=info,common=info,openraft=warn,tonic=warn,tower=warn,h2=warn",
        );
        flat.set("observe.metrics.prometheus.bind", "127.0.0.1:18081");
        flat.set("observe.metrics.prometheus.path", "/metrics");
        ObservabilityConfig::from_flat(&flat).expect("test observe config")
    }

    fn add_observe_config(flat: &mut common::config::FlatConfig) {
        flat.set("observe.log.format", "compact");
        flat.set("observe.log.output", "stderr");
        flat.set(
            "observe.log.level",
            "info,vecton=info,metadata=info,worker=info,common=info,openraft=warn,tonic=warn,tower=warn,h2=warn",
        );
        flat.set("observe.metrics.prometheus.bind", "127.0.0.1:18081");
        flat.set("observe.metrics.prometheus.path", "/metrics");
    }

    fn test_flat() -> common::config::FlatConfig {
        let mut flat = ServerConfig::default().as_flat().clone();
        add_observe_config(&mut flat);
        flat
    }

    #[test]
    fn canonical_group_name_loads_from_metadata_group_name() {
        let mut flat = test_flat();
        flat.set("metadata.group.name", "root-prod");

        let config = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap();
        assert_eq!(config.authority.group_name.as_str(), "root-prod");
    }

    #[test]
    fn observability_loads_from_flat_config_only() {
        let mut flat = test_flat();
        flat.set("observe.log.format", "json");
        flat.set("observe.log.output", "stdout");
        flat.set("observe.metrics.prometheus.bind", "127.0.0.1:19081");

        let config = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap();

        assert_eq!(config.observability.log.format, "json");
        assert_eq!(config.observability.log.output, "stdout");
        assert_eq!(config.observability.metrics.prometheus.bind, "127.0.0.1:19081");
    }

    #[test]
    fn invalid_group_name_is_rejected() {
        for group_name in ["", "Root", "root/prod", "root prod", "-root"] {
            let mut flat = test_flat();
            flat.set("metadata.group.name", group_name);

            let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();
            assert!(err.message.contains("metadata.group.name"));
        }
    }

    #[test]
    fn storage_dir_parses_from_metadata_storage_key() {
        let mut flat = test_flat();
        flat.set("metadata.storage.dir", "/var/lib/vecton/metadata");

        let config = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap();
        assert_eq!(config.storage_dir, std::path::PathBuf::from("/var/lib/vecton/metadata"));
    }

    #[test]
    fn rpc_port_rejects_out_of_range_value() {
        let mut flat = test_flat();
        flat.set("metadata.rpc.port", 70000i64);

        let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();
        assert!(err.message.contains("metadata.rpc.port"));
    }

    #[test]
    fn rpc_port_rejects_present_non_integer_value() {
        let mut flat = test_flat();
        flat.set("metadata.rpc.port", true);

        let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();
        assert!(err.message.contains("metadata.rpc.port"));
    }

    #[test]
    fn string_keys_reject_present_wrong_type_values() {
        for key in [METADATA_RPC_ADDR, METADATA_STORAGE_DIR] {
            let mut flat = test_flat();
            flat.set(key, true);

            let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();

            assert!(
                err.message.contains(key),
                "error for {key} should mention the offending key: {}",
                err.message
            );
        }
    }

    #[test]
    fn raft_mode_parses_single_and_cluster_only() {
        for (raw, expected) in [("single", RaftMode::Single), ("cluster", RaftMode::Cluster)] {
            let mut flat = test_flat();
            flat.set("metadata.raft.mode", raw);

            let config = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap();
            assert_eq!(config.raft.mode, expected);
        }

        let mut flat = test_flat();
        flat.set("metadata.raft.mode", "single_node");
        let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();
        assert!(err.message.contains("metadata.raft.mode"));
    }

    #[test]
    fn absent_numeric_keys_use_metadata_defaults() {
        let config = MetadataConfig::from_server_config(ServerConfig::from_flat(test_flat())).unwrap();

        assert_eq!(config.raft.node_id, 1);
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
            let mut flat = test_flat();
            flat.set(key, -1i64);

            let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();

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
            let mut flat = test_flat();
            flat.set(key, 0i64);

            let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();

            assert!(
                err.message.contains(key),
                "error for {key} should mention the offending key: {}",
                err.message
            );
        }
    }

    #[test]
    fn metadata_repair_max_attempts_rejects_u32_overflow() {
        let mut flat = test_flat();
        flat.set(METADATA_REPAIR_MAX_ATTEMPTS, i64::from(u32::MAX) + 1);

        let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();

        assert!(err.message.contains(METADATA_REPAIR_MAX_ATTEMPTS));
    }
}
