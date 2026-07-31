// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata service configuration.
//!
//! Reads metadata configuration from server YAML files.

use crate::raft::{
    MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES, MAX_RECLAIM_DETACHED_ROOT_CANDIDATES, MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
    MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
};
use crate::readiness::RootReadinessConfig;
use beryl_common::config::ServerConfig;
use beryl_common::error::{CommonError, CommonErrorKind};
use beryl_common::observe::ObservabilityConfig;
use beryl_types::GroupName;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

const CLUSTER_ID: &str = "cluster.id";
const METADATA_RPC_ADDR: &str = "metadata.rpc.addr";
const METADATA_RPC_PORT: &str = "metadata.rpc.port";
const METADATA_GROUP_NAME: &str = "metadata.group.name";
const METADATA_STORAGE_DIR: &str = "metadata.storage.dir";
const METADATA_RAFT_MODE: &str = "metadata.raft.mode";
const METADATA_RAFT_NODE_ID: &str = "metadata.raft.node_id";
const METADATA_CLEANUP_SCAN_INTERVAL_MS: &str = "metadata.cleanup.scan_interval_ms";
const METADATA_CLEANUP_RECLAIM_GRACE_MS: &str = "metadata.cleanup.reclaim_grace_ms";
const METADATA_CLEANUP_MAX_REPLICAS_PER_SCAN: &str = "metadata.cleanup.max_replicas_per_scan";
const METADATA_CLEANUP_MAX_CANDIDATES: &str = "metadata.cleanup.max_candidates";
const METADATA_CLEANUP_DISPATCH_ENABLED: &str = "metadata.cleanup.dispatch_enabled";
const METADATA_CLEANUP_MAX_COMMANDS_PER_HEARTBEAT: &str = "metadata.cleanup.max_commands_per_heartbeat";
const METADATA_CLEANUP_RETRY_INITIAL_BACKOFF_MS: &str = "metadata.cleanup.retry_initial_backoff_ms";
const METADATA_CLEANUP_RETRY_MAX_BACKOFF_MS: &str = "metadata.cleanup.retry_max_backoff_ms";
const METADATA_DETACHED_ROOT_RECLAIM_SCAN_INTERVAL_MS: &str = "metadata.detached_root_reclamation.scan_interval_ms";
const METADATA_DETACHED_ROOT_RECLAIM_MAX_CANDIDATES: &str = "metadata.detached_root_reclamation.max_candidates";
const METADATA_DETACHED_ROOT_RECLAIM_MAX_ENTRIES: &str = "metadata.detached_root_reclamation.max_entries";
const METADATA_DETACHED_ROOT_RECLAIM_MAX_BATCH_BYTES: &str = "metadata.detached_root_reclamation.max_batch_bytes";
const METADATA_DETACHED_ROOT_RECLAIM_RETRY_INITIAL_BACKOFF_MS: &str =
    "metadata.detached_root_reclamation.retry_initial_backoff_ms";
const METADATA_DETACHED_ROOT_RECLAIM_RETRY_MAX_BACKOFF_MS: &str =
    "metadata.detached_root_reclamation.retry_max_backoff_ms";
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
    /// Block cleanup detection and dispatch configuration.
    pub cleanup: CleanupConfig,
    /// Bounded detached namespace reclamation configuration.
    pub detached_root_reclamation: DetachedRootReclamationConfig,
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

/// Block cleanup detection and dispatch configuration.
#[derive(Clone, Debug)]
pub struct CleanupConfig {
    /// Interval between scans.
    pub scan_interval_ms: u64,
    /// Time a replica must remain reclaimable before it is reported as ready.
    pub reclaim_grace_ms: u64,
    /// Maximum number of ready replicas copied into one complete scan.
    pub max_replicas_per_scan: usize,
    /// Maximum number of in-memory cleanup candidates.
    pub max_candidates: usize,
    /// Whether heartbeats may return cleanup commands.
    pub dispatch_enabled: bool,
    /// Maximum cleanup commands returned by one heartbeat.
    pub max_commands_per_heartbeat: usize,
    /// Initial retry delay after returning a cleanup command.
    pub retry_initial_backoff_ms: u64,
    /// Maximum retry delay after repeated cleanup commands.
    pub retry_max_backoff_ms: u64,
}

/// Leader-only detached-root proposal and retry bounds.
#[derive(Clone, Debug)]
pub struct DetachedRootReclamationConfig {
    /// Delay between successful or idle maintenance passes.
    pub scan_interval_ms: u64,
    /// Maximum marker candidates carried by one Raft command.
    pub max_candidates: u32,
    /// Maximum namespace children removed by one Raft apply.
    pub max_entries: u32,
    /// Maximum deterministic key/value bytes in one authority batch.
    pub max_batch_bytes: u32,
    /// Initial delay after a failed proposal or authority read.
    pub retry_initial_backoff_ms: u64,
    /// Maximum delay after repeated failures.
    pub retry_max_backoff_ms: u64,
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

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            scan_interval_ms: 30_000,
            reclaim_grace_ms: 300_000,
            max_replicas_per_scan: 10_000,
            max_candidates: 10_000,
            dispatch_enabled: true,
            max_commands_per_heartbeat: 32,
            retry_initial_backoff_ms: 1_000,
            retry_max_backoff_ms: 60_000,
        }
    }
}

impl Default for DetachedRootReclamationConfig {
    fn default() -> Self {
        Self {
            scan_interval_ms: 1_000,
            max_candidates: MAX_RECLAIM_DETACHED_ROOT_CANDIDATES,
            max_entries: MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
            max_batch_bytes: MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
            retry_initial_backoff_ms: 1_000,
            retry_max_backoff_ms: 60_000,
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

        let cluster_id = get_str_or(flat, CLUSTER_ID, "local-beryl")?;
        if cluster_id.trim().is_empty() {
            return Err(invalid_config(CLUSTER_ID, "must not be empty"));
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

        let cleanup = CleanupConfig {
            scan_interval_ms: get_positive_u64_or(flat, METADATA_CLEANUP_SCAN_INTERVAL_MS, 30_000)?,
            reclaim_grace_ms: get_positive_u64_or(flat, METADATA_CLEANUP_RECLAIM_GRACE_MS, 300_000)?,
            max_replicas_per_scan: get_positive_usize_or(flat, METADATA_CLEANUP_MAX_REPLICAS_PER_SCAN, 10_000)?,
            max_candidates: get_positive_usize_or(flat, METADATA_CLEANUP_MAX_CANDIDATES, 10_000)?,
            dispatch_enabled: get_bool_or(flat, METADATA_CLEANUP_DISPATCH_ENABLED, true)?,
            max_commands_per_heartbeat: get_positive_usize_or(flat, METADATA_CLEANUP_MAX_COMMANDS_PER_HEARTBEAT, 32)?,
            retry_initial_backoff_ms: get_positive_u64_or(flat, METADATA_CLEANUP_RETRY_INITIAL_BACKOFF_MS, 1_000)?,
            retry_max_backoff_ms: get_positive_u64_or(flat, METADATA_CLEANUP_RETRY_MAX_BACKOFF_MS, 60_000)?,
        };
        if cleanup.retry_max_backoff_ms < cleanup.retry_initial_backoff_ms {
            return Err(invalid_config(
                METADATA_CLEANUP_RETRY_MAX_BACKOFF_MS,
                "must be greater than or equal to metadata.cleanup.retry_initial_backoff_ms",
            ));
        }

        let detached_root_reclamation = DetachedRootReclamationConfig {
            scan_interval_ms: get_positive_u64_or(flat, METADATA_DETACHED_ROOT_RECLAIM_SCAN_INTERVAL_MS, 1_000)?,
            max_candidates: get_positive_u32_or(
                flat,
                METADATA_DETACHED_ROOT_RECLAIM_MAX_CANDIDATES,
                MAX_RECLAIM_DETACHED_ROOT_CANDIDATES,
            )?,
            max_entries: get_positive_u32_or(
                flat,
                METADATA_DETACHED_ROOT_RECLAIM_MAX_ENTRIES,
                MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
            )?,
            max_batch_bytes: get_positive_u32_or(
                flat,
                METADATA_DETACHED_ROOT_RECLAIM_MAX_BATCH_BYTES,
                MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
            )?,
            retry_initial_backoff_ms: get_positive_u64_or(
                flat,
                METADATA_DETACHED_ROOT_RECLAIM_RETRY_INITIAL_BACKOFF_MS,
                1_000,
            )?,
            retry_max_backoff_ms: get_positive_u64_or(
                flat,
                METADATA_DETACHED_ROOT_RECLAIM_RETRY_MAX_BACKOFF_MS,
                60_000,
            )?,
        };
        if detached_root_reclamation.max_candidates > MAX_RECLAIM_DETACHED_ROOT_CANDIDATES {
            return Err(invalid_config(
                METADATA_DETACHED_ROOT_RECLAIM_MAX_CANDIDATES,
                "exceeds the replicated protocol maximum",
            ));
        }
        if detached_root_reclamation.max_entries > MAX_RECLAIM_DETACHED_ROOT_ENTRIES {
            return Err(invalid_config(
                METADATA_DETACHED_ROOT_RECLAIM_MAX_ENTRIES,
                "exceeds the replicated protocol maximum",
            ));
        }
        if !(MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES..=MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES)
            .contains(&detached_root_reclamation.max_batch_bytes)
        {
            return Err(invalid_config(
                METADATA_DETACHED_ROOT_RECLAIM_MAX_BATCH_BYTES,
                "is outside the replicated protocol byte range",
            ));
        }
        if detached_root_reclamation.retry_max_backoff_ms < detached_root_reclamation.retry_initial_backoff_ms {
            return Err(invalid_config(
                METADATA_DETACHED_ROOT_RECLAIM_RETRY_MAX_BACKOFF_MS,
                "must be greater than or equal to metadata.detached_root_reclamation.retry_initial_backoff_ms",
            ));
        }

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
            cleanup,
            detached_root_reclamation,
            worker,
            bootstrap,
            observability,
        })
    }
}

fn parse_group_name(key: &'static str, raw: String) -> Result<GroupName, CommonError> {
    GroupName::parse(raw).map_err(|err| CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {err}")))
}

fn get_i64_if_present(flat: &beryl_common::config::FlatConfig, key: &'static str) -> Result<Option<i64>, CommonError> {
    if let Some(value) = flat.get_i64(key) {
        return Ok(Some(value));
    }
    if flat.contains_key(key) {
        return Err(invalid_config(key, "must be an integer"));
    }
    Ok(None)
}

fn rpc_addr_from_config(flat: &beryl_common::config::FlatConfig) -> Result<SocketAddr, CommonError> {
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
    flat: &beryl_common::config::FlatConfig,
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

fn get_bool_or(flat: &beryl_common::config::FlatConfig, key: &'static str, default: bool) -> Result<bool, CommonError> {
    if let Some(value) = flat.get_bool(key) {
        return Ok(value);
    }
    if flat.contains_key(key) {
        return Err(invalid_config(key, "must be a boolean"));
    }
    Ok(default)
}

fn get_u64_or(flat: &beryl_common::config::FlatConfig, key: &'static str, default: u64) -> Result<u64, CommonError> {
    let Some(value) = get_i64_if_present(flat, key)? else {
        return Ok(default);
    };
    u64::try_from(value).map_err(|_| invalid_config(key, "must be non-negative"))
}

fn get_positive_u64_or(
    flat: &beryl_common::config::FlatConfig,
    key: &'static str,
    default: u64,
) -> Result<u64, CommonError> {
    let value = get_u64_or(flat, key, default)?;
    if value == 0 {
        return Err(invalid_config(key, "must be greater than zero"));
    }
    Ok(value)
}

fn get_positive_u64_or_any(
    flat: &beryl_common::config::FlatConfig,
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
    flat: &beryl_common::config::FlatConfig,
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

fn get_positive_u32_or(
    flat: &beryl_common::config::FlatConfig,
    key: &'static str,
    default: u32,
) -> Result<u32, CommonError> {
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
    use beryl_common::config::ServerConfig;

    impl Default for MetadataConfig {
        fn default() -> Self {
            Self {
                cluster_id: "local-beryl".to_string(),
                rpc_addr: "0.0.0.0:18080".parse().unwrap(),
                storage_dir: PathBuf::from("data/metadata"),
                raft: RaftConfig::default(),
                authority: MetadataAuthorityConfig::default(),
                cleanup: CleanupConfig::default(),
                detached_root_reclamation: DetachedRootReclamationConfig::default(),
                worker: WorkerConfig::default(),
                bootstrap: BootstrapConfig {
                    root_readiness: RootReadinessConfig::default(),
                },
                observability: test_observability_config(),
            }
        }
    }

    fn test_observability_config() -> ObservabilityConfig {
        let mut flat = beryl_common::config::FlatConfig::new();
        flat.set("observe.log.format", "compact");
        flat.set("observe.log.output", "stderr");
        flat.set(
            "observe.log.level",
            "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn",
        );
        flat.set("observe.metrics.prometheus.bind", "127.0.0.1:18081");
        flat.set("observe.metrics.prometheus.path", "/metrics");
        ObservabilityConfig::from_flat(&flat).expect("test observe config")
    }

    fn add_observe_config(flat: &mut beryl_common::config::FlatConfig) {
        flat.set("observe.log.format", "compact");
        flat.set("observe.log.output", "stderr");
        flat.set(
            "observe.log.level",
            "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn",
        );
        flat.set("observe.metrics.prometheus.bind", "127.0.0.1:18081");
        flat.set("observe.metrics.prometheus.path", "/metrics");
    }

    fn test_flat() -> beryl_common::config::FlatConfig {
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
        assert_eq!(config.cleanup.scan_interval_ms, 30_000);
        assert_eq!(config.cleanup.reclaim_grace_ms, 300_000);
        assert_eq!(config.cleanup.max_replicas_per_scan, 10_000);
        assert_eq!(config.cleanup.max_candidates, 10_000);
        assert!(config.cleanup.dispatch_enabled);
        assert_eq!(config.cleanup.max_commands_per_heartbeat, 32);
        assert_eq!(config.cleanup.retry_initial_backoff_ms, 1_000);
        assert_eq!(config.cleanup.retry_max_backoff_ms, 60_000);
        assert_eq!(config.detached_root_reclamation.scan_interval_ms, 1_000);
        assert_eq!(
            config.detached_root_reclamation.max_candidates,
            MAX_RECLAIM_DETACHED_ROOT_CANDIDATES
        );
        assert_eq!(
            config.detached_root_reclamation.max_entries,
            MAX_RECLAIM_DETACHED_ROOT_ENTRIES
        );
        assert_eq!(
            config.detached_root_reclamation.max_batch_bytes,
            MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES
        );
        assert_eq!(config.detached_root_reclamation.retry_initial_backoff_ms, 1_000);
        assert_eq!(config.detached_root_reclamation.retry_max_backoff_ms, 60_000);
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
    fn cleanup_dispatch_can_be_explicitly_disabled() {
        let mut flat = test_flat();
        flat.set(METADATA_CLEANUP_DISPATCH_ENABLED, false);

        let config = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap();

        assert!(!config.cleanup.dispatch_enabled);
    }

    #[test]
    fn invalid_numeric_values_are_rejected() {
        for port in [0i64, 70_000] {
            let mut flat = test_flat();
            flat.set(METADATA_RPC_PORT, port);
            let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();
            assert!(err.message.contains(METADATA_RPC_PORT));
        }

        let mut non_integer_port = test_flat();
        non_integer_port.set(METADATA_RPC_PORT, true);
        let err = MetadataConfig::from_server_config(ServerConfig::from_flat(non_integer_port)).unwrap_err();
        assert!(err.message.contains(METADATA_RPC_PORT));

        let positive_keys = [
            METADATA_RAFT_NODE_ID,
            METADATA_CLEANUP_SCAN_INTERVAL_MS,
            METADATA_CLEANUP_RECLAIM_GRACE_MS,
            METADATA_CLEANUP_MAX_REPLICAS_PER_SCAN,
            METADATA_CLEANUP_MAX_CANDIDATES,
            METADATA_CLEANUP_MAX_COMMANDS_PER_HEARTBEAT,
            METADATA_CLEANUP_RETRY_INITIAL_BACKOFF_MS,
            METADATA_CLEANUP_RETRY_MAX_BACKOFF_MS,
            METADATA_DETACHED_ROOT_RECLAIM_SCAN_INTERVAL_MS,
            METADATA_DETACHED_ROOT_RECLAIM_MAX_CANDIDATES,
            METADATA_DETACHED_ROOT_RECLAIM_MAX_ENTRIES,
            METADATA_DETACHED_ROOT_RECLAIM_MAX_BATCH_BYTES,
            METADATA_DETACHED_ROOT_RECLAIM_RETRY_INITIAL_BACKOFF_MS,
            METADATA_DETACHED_ROOT_RECLAIM_RETRY_MAX_BACKOFF_MS,
            METADATA_REPAIR_MAX_QUEUE_SIZE,
            METADATA_REPAIR_MAX_ATTEMPTS,
            METADATA_REPAIR_INFLIGHT_TIMEOUT_MS,
            METADATA_REPAIR_INITIAL_BACKOFF_MS,
            METADATA_REPAIR_MAX_BACKOFF_MS,
            METADATA_REPAIR_WORKER_INFLIGHT_LIMIT,
            METADATA_BOOTSTRAP_ROOT_READY_INITIAL_BACKOFF_MS,
            METADATA_BOOTSTRAP_ROOT_READY_MAX_BACKOFF_MS,
            METADATA_BOOTSTRAP_ROOT_READY_WARN_AFTER_MS,
        ];
        for value in [-1i64, 0] {
            for key in positive_keys {
                let mut flat = test_flat();
                flat.set(key, value);
                let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();
                assert!(
                    err.message.contains(key),
                    "error for {key}={value} should mention the offending key: {}",
                    err.message
                );
            }
        }

        let mut flat = test_flat();
        flat.set(METADATA_REPAIR_MAX_ATTEMPTS, i64::from(u32::MAX) + 1);
        let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();
        assert!(err.message.contains(METADATA_REPAIR_MAX_ATTEMPTS));

        let mut flat = test_flat();
        flat.set(METADATA_CLEANUP_RETRY_INITIAL_BACKOFF_MS, 100_i64);
        flat.set(METADATA_CLEANUP_RETRY_MAX_BACKOFF_MS, 99_i64);
        let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();
        assert!(err.message.contains(METADATA_CLEANUP_RETRY_MAX_BACKOFF_MS));

        for (key, value) in [
            (
                METADATA_DETACHED_ROOT_RECLAIM_MAX_CANDIDATES,
                i64::from(MAX_RECLAIM_DETACHED_ROOT_CANDIDATES) + 1,
            ),
            (
                METADATA_DETACHED_ROOT_RECLAIM_MAX_ENTRIES,
                i64::from(MAX_RECLAIM_DETACHED_ROOT_ENTRIES) + 1,
            ),
            (
                METADATA_DETACHED_ROOT_RECLAIM_MAX_BATCH_BYTES,
                i64::from(MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES) + 1,
            ),
            (
                METADATA_DETACHED_ROOT_RECLAIM_MAX_BATCH_BYTES,
                i64::from(MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES) - 1,
            ),
        ] {
            let mut flat = test_flat();
            flat.set(key, value);
            let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();
            assert!(err.message.contains(key));
        }

        let mut flat = test_flat();
        flat.set(METADATA_DETACHED_ROOT_RECLAIM_RETRY_INITIAL_BACKOFF_MS, 100_i64);
        flat.set(METADATA_DETACHED_ROOT_RECLAIM_RETRY_MAX_BACKOFF_MS, 99_i64);
        let err = MetadataConfig::from_server_config(ServerConfig::from_flat(flat)).unwrap_err();
        assert!(err
            .message
            .contains(METADATA_DETACHED_ROOT_RECLAIM_RETRY_MAX_BACKOFF_MS));
    }
}
