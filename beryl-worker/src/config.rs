// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker process configuration.

use std::collections::{BTreeMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use beryl_common::config::{format_host_port, load_from_yaml_file, validate_public_host, FlatConfig};
use beryl_common::error::{CommonError, CommonErrorKind};
use beryl_common::observe::ObservabilityConfig;
use beryl_types::{GroupName, Tier};
use serde_yaml::{Mapping, Value};
use tokio::sync::Semaphore;
use tonic::transport::Endpoint;
use tracing::info;

const DEFAULT_GRPC_MAX_CONCURRENT_READS: usize = 64;
const DEFAULT_GRPC_MAX_CONCURRENT_WRITES: usize = 32;

const CLUSTER_ID: &str = "beryl.cluster.id";
const HOST: &str = "beryl.worker.host";
const BIND_HOST: &str = "beryl.worker.bind-host";
const RPC_PORT: &str = "beryl.worker.rpc.port";
const HTTP_PORT: &str = "beryl.worker.http.port";
const IDENTITY_FILE: &str = "beryl.worker.identity-file";
const RPC_MAX_CONCURRENT_READ_REQUESTS: &str = "beryl.worker.rpc.max-concurrent-read-requests";
const RPC_MAX_CONCURRENT_WRITE_REQUESTS: &str = "beryl.worker.rpc.max-concurrent-write-requests";
const STREAM_FRAME_SIZE: &str = "beryl.worker.stream.frame-size";
const STREAM_MAX_FRAME_SIZE: &str = "beryl.worker.stream.max-frame-size";
const STORAGE_DIRS: &str = "beryl.worker.storage.dirs";
const STORAGE_RESERVED_SPACE: &str = "beryl.worker.storage.reserved-space";
const STORAGE_CHECK_INTERVAL: &str = "beryl.worker.storage.check-interval";
const METADATA_ADDRESSES: &str = "beryl.worker.metadata.addresses";
const METADATA_REQUEST_TIMEOUT: &str = "beryl.worker.metadata.request-timeout";
const METADATA_RETRY_INITIAL_BACKOFF: &str = "beryl.worker.metadata.retry.initial-backoff";
const METADATA_RETRY_MAX_BACKOFF: &str = "beryl.worker.metadata.retry.max-backoff";
const HEARTBEAT_INTERVAL: &str = "beryl.worker.heartbeat.interval";
const BLOCK_REPORT_DELTA_FLUSH_INTERVAL: &str = "beryl.worker.block.report.delta-flush-interval";
const BLOCK_REPORT_BATCH_SIZE: &str = "beryl.worker.block.report.batch-size";
const BLOCK_CLEANUP_QUEUE_CAPACITY: &str = "beryl.worker.block.cleanup.queue-capacity";
const BLOCK_CLEANUP_CONCURRENCY: &str = "beryl.worker.block.cleanup.concurrency";
const BLOCK_CLEANUP_RETRY_INITIAL_BACKOFF: &str = "beryl.worker.block.cleanup.retry.initial-backoff";
const BLOCK_CLEANUP_RETRY_MAX_BACKOFF: &str = "beryl.worker.block.cleanup.retry.max-backoff";
const SHUTDOWN_TIMEOUT: &str = "beryl.worker.shutdown.timeout";

/// Process-wide admission limits for Worker data RPCs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerNetConfig {
    /// Maximum admitted read RPC lifecycles shared by all connections.
    pub max_concurrent_reads: usize,
    /// Maximum admitted write RPC lifecycles shared by all connections.
    pub max_concurrent_writes: usize,
}

/// Worker-to-Metadata request and retry configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRegistrationConfig {
    /// Internal identity for the one supported metadata group.
    pub group_name: GroupName,
    /// The single Metadata leader endpoint supported by the current runtime.
    pub endpoint: String,
    /// Timeout shared by registration, heartbeat, and block report RPCs.
    pub request_timeout_ms: u64,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
}

impl Default for WorkerRegistrationConfig {
    fn default() -> Self {
        Self {
            group_name: GroupName::parse("root").expect("the supported metadata group is valid"),
            endpoint: "http://127.0.0.1:18080".to_string(),
            request_timeout_ms: 5_000,
            retry_initial_backoff_ms: 200,
            retry_max_backoff_ms: 5_000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreDirConfig {
    pub path: PathBuf,
    pub tier: Tier,
    pub capacity_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerStoreConfig {
    pub dirs: BTreeMap<String, StoreDirConfig>,
    pub reserve_space_bytes: u64,
    pub check_interval_ms: u64,
}

impl Default for WorkerStoreConfig {
    fn default() -> Self {
        let mut dirs = BTreeMap::new();
        dirs.insert(
            "hdd0".to_string(),
            StoreDirConfig {
                path: PathBuf::from("data/worker/hdd0"),
                tier: Tier::Hdd,
                capacity_bytes: 10 * 1024 * 1024 * 1024,
            },
        );
        Self {
            dirs,
            reserve_space_bytes: 1024 * 1024 * 1024,
            check_interval_ms: 30_000,
        }
    }
}

/// Bounds local execution of Metadata-authorized block cleanup commands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockCleanupOptions {
    /// Maximum number of distinct queued and active cleanup commands.
    pub max_pending: usize,
    /// Maximum number of concurrent local reclamation attempts.
    pub max_concurrent: usize,
    pub retry_initial_backoff: Duration,
    pub retry_max_backoff: Duration,
}

impl Default for BlockCleanupOptions {
    fn default() -> Self {
        Self {
            max_pending: 1_024,
            max_concurrent: 4,
            retry_initial_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_secs(30),
        }
    }
}

impl BlockCleanupOptions {
    pub(crate) fn validate(&self) -> Result<(), CommonError> {
        if self.max_pending == 0 {
            return Err(invalid_config(BLOCK_CLEANUP_QUEUE_CAPACITY, "must be positive"));
        }
        if self.max_concurrent == 0 || self.max_concurrent > self.max_pending {
            return Err(invalid_config(
                BLOCK_CLEANUP_CONCURRENCY,
                "must be positive and not exceed the cleanup queue capacity",
            ));
        }
        if self.retry_initial_backoff.is_zero() {
            return Err(invalid_config(BLOCK_CLEANUP_RETRY_INITIAL_BACKOFF, "must be positive"));
        }
        if self.retry_max_backoff < self.retry_initial_backoff {
            return Err(invalid_config(
                BLOCK_CLEANUP_RETRY_MAX_BACKOFF,
                "must not be smaller than the initial backoff",
            ));
        }
        Ok(())
    }
}

/// Configuration consumed by one Worker process.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub cluster_id: String,
    /// Host published to Metadata and clients.
    pub host: String,
    /// Local interface shared by Worker listeners.
    pub bind_host: IpAddr,
    pub rpc_port: u16,
    pub http_port: u16,
    pub identity_path: PathBuf,
    /// Default payload size for streamed read responses.
    pub default_frame_size: u32,
    /// Maximum payload size selected for streamed read responses.
    pub max_frame_size: u32,
    pub store: WorkerStoreConfig,
    pub net: WorkerNetConfig,
    pub metadata: WorkerRegistrationConfig,
    pub heartbeat_interval_ms: u64,
    /// Maximum delay before retrying or flushing retained Delta changes.
    pub block_report_delta_flush_interval_ms: u64,
    pub block_report_batch_size: usize,
    pub block_cleanup: BlockCleanupOptions,
    /// Graceful RPC/background drain interval before remaining work is cancelled.
    pub shutdown_timeout_ms: u64,
    pub observability: ObservabilityConfig,
}

impl WorkerConfig {
    pub fn rpc_address(&self) -> String {
        format_host_port(&self.host, self.rpc_port)
    }

    pub fn rpc_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.rpc_port)
    }

    pub fn http_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.http_port)
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let bind_host = "0.0.0.0".parse().expect("default bind host is valid");
        Self {
            cluster_id: "local-beryl".to_string(),
            host: "127.0.0.1".to_string(),
            bind_host,
            rpc_port: 19090,
            http_port: 19091,
            identity_path: PathBuf::from("data/worker/worker.identity"),
            default_frame_size: 1024 * 1024,
            max_frame_size: 4 * 1024 * 1024,
            store: WorkerStoreConfig::default(),
            net: WorkerNetConfig {
                max_concurrent_reads: DEFAULT_GRPC_MAX_CONCURRENT_READS,
                max_concurrent_writes: DEFAULT_GRPC_MAX_CONCURRENT_WRITES,
            },
            metadata: WorkerRegistrationConfig::default(),
            heartbeat_interval_ms: 1_000,
            block_report_delta_flush_interval_ms: 1_000,
            block_report_batch_size: 1_000,
            block_cleanup: BlockCleanupOptions::default(),
            shutdown_timeout_ms: 30_000,
            observability: ObservabilityConfig {
                format: "compact".to_string(),
                output: "stderr".to_string(),
                level: "info".to_string(),
            },
        }
    }
}

impl WorkerConfig {
    /// Load Worker configuration from one YAML file.
    pub fn load<P: AsRef<Path>>(config_path: P) -> Result<Self, CommonError> {
        Self::from_flat(load_from_yaml_file(config_path)?)
    }

    /// Build typed Worker configuration from shared YAML mechanics.
    pub fn from_flat(flat: FlatConfig) -> Result<Self, CommonError> {
        let flat = &flat;
        let defaults = Self::default();

        let cluster_id = flat.string_or(CLUSTER_ID, &defaults.cluster_id)?;
        let host = flat.string_or(HOST, &defaults.host)?;
        let bind_host = flat
            .string_or(BIND_HOST, &defaults.bind_host.to_string())?
            .parse::<IpAddr>()
            .map_err(|_| invalid_config(BIND_HOST, "must be an IP address"))?;
        let rpc_port = flat.port_or(RPC_PORT, defaults.rpc_port)?;
        let http_port = flat.port_or(HTTP_PORT, defaults.http_port)?;
        let identity_path = PathBuf::from(flat.string_or(IDENTITY_FILE, defaults.identity_path.to_str().unwrap())?);
        let rpc_max_concurrent_reads =
            flat.positive_usize_or(RPC_MAX_CONCURRENT_READ_REQUESTS, DEFAULT_GRPC_MAX_CONCURRENT_READS)?;
        let rpc_max_concurrent_writes =
            flat.positive_usize_or(RPC_MAX_CONCURRENT_WRITE_REQUESTS, DEFAULT_GRPC_MAX_CONCURRENT_WRITES)?;
        let default_frame_size = flat.bytes_u32_or(STREAM_FRAME_SIZE, defaults.default_frame_size)?;
        let max_frame_size = flat.bytes_u32_or(STREAM_MAX_FRAME_SIZE, defaults.max_frame_size)?;
        let store = parse_store_config(flat, &defaults.store)?;
        let metadata = parse_metadata_config(flat, &defaults.metadata)?;
        let heartbeat_interval_ms = flat.duration_ms_or(HEARTBEAT_INTERVAL, defaults.heartbeat_interval_ms)?;
        let block_report_delta_flush_interval_ms = flat.duration_ms_or(
            BLOCK_REPORT_DELTA_FLUSH_INTERVAL,
            defaults.block_report_delta_flush_interval_ms,
        )?;
        let block_report_batch_size =
            flat.positive_usize_or(BLOCK_REPORT_BATCH_SIZE, defaults.block_report_batch_size)?;
        let shutdown_timeout_ms = flat.duration_ms_or(SHUTDOWN_TIMEOUT, defaults.shutdown_timeout_ms)?;
        let cleanup_defaults = &defaults.block_cleanup;
        let block_cleanup = BlockCleanupOptions {
            max_pending: flat.positive_usize_or(BLOCK_CLEANUP_QUEUE_CAPACITY, cleanup_defaults.max_pending)?,
            max_concurrent: flat.positive_usize_or(BLOCK_CLEANUP_CONCURRENCY, cleanup_defaults.max_concurrent)?,
            retry_initial_backoff: Duration::from_millis(flat.duration_ms_or(
                BLOCK_CLEANUP_RETRY_INITIAL_BACKOFF,
                cleanup_defaults.retry_initial_backoff.as_millis() as u64,
            )?),
            retry_max_backoff: Duration::from_millis(flat.duration_ms_or(
                BLOCK_CLEANUP_RETRY_MAX_BACKOFF,
                cleanup_defaults.retry_max_backoff.as_millis() as u64,
            )?),
        };
        if rpc_port == http_port {
            return Err(invalid_config(HTTP_PORT, "must differ from the RPC port"));
        }
        let observability = ObservabilityConfig::from_flat(flat)?;
        let config = Self {
            cluster_id,
            host,
            bind_host,
            rpc_port,
            http_port,
            identity_path,
            default_frame_size,
            max_frame_size,
            store,
            net: WorkerNetConfig {
                max_concurrent_reads: rpc_max_concurrent_reads,
                max_concurrent_writes: rpc_max_concurrent_writes,
            },
            metadata,
            heartbeat_interval_ms,
            block_report_delta_flush_interval_ms,
            block_report_batch_size,
            block_cleanup,
            shutdown_timeout_ms,
            observability,
        };
        config.validate()?;

        info!(
            host = %config.host,
            rpc_bind = %config.rpc_bind_addr(),
            http_bind = %config.http_addr(),
            store_dirs = config.store.dirs.len(),
            metadata_address = %config.metadata.endpoint,
            "Worker configuration loaded"
        );
        Ok(config)
    }

    /// Validate direct safety constraints without touching local storage.
    pub fn validate(&self) -> Result<(), CommonError> {
        if self.cluster_id.trim().is_empty() {
            return Err(invalid_config(CLUSTER_ID, "must not be empty"));
        }
        validate_public_host(HOST, &self.host)?;
        if self.identity_path.as_os_str().is_empty() {
            return Err(invalid_config(IDENTITY_FILE, "must not be empty"));
        }
        if self.default_frame_size == 0 || self.default_frame_size > self.max_frame_size {
            return Err(invalid_config(
                STREAM_FRAME_SIZE,
                "must be positive and not exceed the maximum frame size",
            ));
        }
        if self.max_frame_size > beryl_proto::MAX_WORKER_DATA_FRAME_SIZE {
            return Err(invalid_config(
                STREAM_MAX_FRAME_SIZE,
                "exceeds the Worker data protocol maximum",
            ));
        }
        if self.block_report_batch_size > beryl_types::MAX_REPORT_ENTRIES {
            return Err(invalid_config(
                BLOCK_REPORT_BATCH_SIZE,
                "exceeds the shared block-report protocol maximum",
            ));
        }
        self.block_cleanup.validate()?;
        validate_store_config(self)?;
        self.metadata.validate()?;
        if self.net.max_concurrent_reads == 0 || self.net.max_concurrent_reads > Semaphore::MAX_PERMITS {
            return Err(invalid_config(
                RPC_MAX_CONCURRENT_READ_REQUESTS,
                "must fit the process semaphore capacity",
            ));
        }
        if self.net.max_concurrent_writes == 0 || self.net.max_concurrent_writes > Semaphore::MAX_PERMITS {
            return Err(invalid_config(
                RPC_MAX_CONCURRENT_WRITE_REQUESTS,
                "must fit the process semaphore capacity",
            ));
        }
        Ok(())
    }
}

fn parse_metadata_config(
    flat: &FlatConfig,
    defaults: &WorkerRegistrationConfig,
) -> Result<WorkerRegistrationConfig, CommonError> {
    let endpoint = if flat.contains_key(METADATA_ADDRESSES) {
        let addresses = flat
            .get_string_list(METADATA_ADDRESSES)
            .ok_or_else(|| invalid_config(METADATA_ADDRESSES, "must be a list of host:port addresses"))?;
        if addresses.len() != 1 || addresses[0].trim().is_empty() {
            return Err(invalid_config(
                METADATA_ADDRESSES,
                "must contain exactly one non-empty Metadata leader address",
            ));
        }
        normalize_endpoint(&addresses[0])
    } else {
        defaults.endpoint.clone()
    };
    Ok(WorkerRegistrationConfig {
        group_name: defaults.group_name.clone(),
        endpoint,
        request_timeout_ms: flat.duration_ms_or(METADATA_REQUEST_TIMEOUT, defaults.request_timeout_ms)?,
        retry_initial_backoff_ms: flat
            .duration_ms_or(METADATA_RETRY_INITIAL_BACKOFF, defaults.retry_initial_backoff_ms)?,
        retry_max_backoff_ms: flat.duration_ms_or(METADATA_RETRY_MAX_BACKOFF, defaults.retry_max_backoff_ms)?,
    })
}

fn normalize_endpoint(address: &str) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

fn parse_store_config(flat: &FlatConfig, defaults: &WorkerStoreConfig) -> Result<WorkerStoreConfig, CommonError> {
    let dirs = match flat.get_mapping(STORAGE_DIRS) {
        Some(dirs) => parse_store_dirs(dirs)?,
        None if flat.contains_key(STORAGE_DIRS) => {
            return Err(invalid_config(STORAGE_DIRS, "must be a mapping keyed by directory id"));
        }
        None => defaults.dirs.clone(),
    };
    Ok(WorkerStoreConfig {
        dirs,
        reserve_space_bytes: flat.bytes_u64_or(STORAGE_RESERVED_SPACE, defaults.reserve_space_bytes)?,
        check_interval_ms: flat.duration_ms_or(STORAGE_CHECK_INTERVAL, defaults.check_interval_ms)?,
    })
}

/// Parses each configured storage directory while ignoring unrecognized string fields.
fn parse_store_dirs(mapping: &Mapping) -> Result<BTreeMap<String, StoreDirConfig>, CommonError> {
    if mapping.is_empty() {
        return Err(invalid_config(STORAGE_DIRS, "must not be empty"));
    }
    let mut dirs = BTreeMap::new();
    for (id, value) in mapping {
        let id = id
            .as_str()
            .filter(|id| !id.trim().is_empty() && *id == id.trim())
            .ok_or_else(|| invalid_config(STORAGE_DIRS, "contains an invalid directory id"))?;
        let fields = value
            .as_mapping()
            .ok_or_else(|| invalid_config(STORAGE_DIRS, "entries must be mappings"))?;
        for field in fields.keys() {
            if field.as_str().is_none() {
                return Err(CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    format!("config key under {STORAGE_DIRS}.{id} must be a string: {field:?}"),
                ));
            }
        }
        let path = mapping_string(fields, "path")?;
        let tier = Tier::parse(mapping_string(fields, "tier")?.to_ascii_uppercase()).map_err(|error| {
            CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("{STORAGE_DIRS}.{id}.tier {error}"),
            )
        })?;
        let capacity_bytes = mapping_bytes(fields, "capacity")?;
        dirs.insert(
            id.to_string(),
            StoreDirConfig {
                path: PathBuf::from(path),
                tier,
                capacity_bytes,
            },
        );
    }
    Ok(dirs)
}

fn mapping_string(mapping: &Mapping, field: &'static str) -> Result<String, CommonError> {
    mapping
        .get(Value::String(field.to_string()))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid_config(STORAGE_DIRS, "entries require non-empty path, tier, and capacity"))
}

fn mapping_bytes(mapping: &Mapping, field: &'static str) -> Result<u64, CommonError> {
    let value = mapping
        .get(Value::String(field.to_string()))
        .ok_or_else(|| invalid_config(STORAGE_DIRS, "entries require non-empty path, tier, and capacity"))?;
    let mut flat = FlatConfig::new();
    flat.insert(field.to_string(), value.clone());
    let bytes = flat
        .get_bytes(field)
        .filter(|bytes| *bytes != 0)
        .ok_or_else(|| invalid_config(STORAGE_DIRS, "capacity must be a positive size"))?;
    u64::try_from(bytes).map_err(|_| invalid_config(STORAGE_DIRS, "capacity is too large"))
}

fn validate_store_config(config: &WorkerConfig) -> Result<(), CommonError> {
    if config.store.dirs.is_empty() {
        return Err(invalid_config(STORAGE_DIRS, "must not be empty"));
    }
    let mut paths = HashSet::new();
    for dir in config.store.dirs.values() {
        if dir.path.as_os_str().is_empty() || dir.capacity_bytes == 0 {
            return Err(invalid_config(STORAGE_DIRS, "contains an empty path or capacity"));
        }
        if !paths.insert(dir.path.clone()) {
            return Err(invalid_config(STORAGE_DIRS, "contains duplicate paths"));
        }
    }
    Ok(())
}

impl WorkerRegistrationConfig {
    /// Validates registration settings and returns their parsed Metadata endpoint.
    pub fn validate(&self) -> Result<Endpoint, CommonError> {
        let endpoint = Endpoint::from_shared(self.endpoint.clone())
            .map_err(|_| invalid_config(METADATA_ADDRESSES, "contains an invalid address"))?;
        if self.request_timeout_ms == 0
            || self.retry_initial_backoff_ms == 0
            || self.retry_max_backoff_ms < self.retry_initial_backoff_ms
        {
            return Err(invalid_config(
                METADATA_REQUEST_TIMEOUT,
                "requires positive timeout and ordered retry backoff",
            ));
        }
        Ok(endpoint)
    }
}

fn invalid_config(key: &'static str, detail: &'static str) -> CommonError {
    CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_addresses_require_one_leader() {
        let defaults = WorkerRegistrationConfig::default();
        for addresses in [vec![], vec![""], vec!["127.0.0.1:18080", "127.0.0.1:18081"]] {
            let mut flat = FlatConfig::new();
            flat.insert(METADATA_ADDRESSES.to_string(), serde_yaml::to_value(addresses).unwrap());
            assert!(parse_metadata_config(&flat, &defaults).is_err());
        }
        let mut flat = FlatConfig::new();
        flat.insert(
            METADATA_ADDRESSES.to_string(),
            serde_yaml::to_value(["127.0.0.1:18080"]).unwrap(),
        );
        assert_eq!(
            parse_metadata_config(&flat, &defaults).unwrap().endpoint,
            "http://127.0.0.1:18080"
        );
    }
}
