// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker process configuration.

use std::collections::{BTreeMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use beryl_common::config::{keys::logging, FlatConfig, ServerConfig};
use beryl_common::error::{CommonError, CommonErrorKind};
use beryl_common::observe::config::{LogConfig, ResourceConfig};
use beryl_common::observe::ObservabilityConfig;
use beryl_types::{GroupName, Tier};
use serde_yaml::{Mapping, Value};
use tonic::transport::Endpoint;
use tracing::info;

use crate::net::config::WorkerNetConfig;
use crate::net::protocol::WorkerNetProtocol;

const CLUSTER_ID: &str = "beryl.cluster.id";
const HOST: &str = "beryl.worker.host";
const BIND_HOST: &str = "beryl.worker.bind-host";
const RPC_PORT: &str = "beryl.worker.rpc.port";
const HTTP_PORT: &str = "beryl.worker.http.port";
const IDENTITY_FILE: &str = "beryl.worker.identity-file";
const RPC_MAX_CONCURRENT_REQUESTS: &str = "beryl.worker.rpc.max-concurrent-requests";
const STREAM_FRAME_SIZE: &str = "beryl.worker.stream.frame-size";
const STREAM_MAX_FRAME_SIZE: &str = "beryl.worker.stream.max-frame-size";
const STREAM_IDLE_TIMEOUT: &str = "beryl.worker.stream.idle-timeout";
const STORAGE_DIRS: &str = "beryl.worker.storage.dirs";
const STORAGE_RESERVED_SPACE: &str = "beryl.worker.storage.reserved-space";
const STORAGE_CHECK_INTERVAL: &str = "beryl.worker.storage.check-interval";
const METADATA_ADDRESSES: &str = "beryl.worker.metadata.addresses";
const METADATA_REQUEST_TIMEOUT: &str = "beryl.worker.metadata.request-timeout";
const METADATA_RETRY_INITIAL_BACKOFF: &str = "beryl.worker.metadata.retry.initial-backoff";
const METADATA_RETRY_MAX_BACKOFF: &str = "beryl.worker.metadata.retry.max-backoff";
const HEARTBEAT_INTERVAL: &str = "beryl.worker.heartbeat.interval";
const BLOCK_REPORT_INTERVAL: &str = "beryl.worker.block.report.interval";
const BLOCK_REPORT_BATCH_SIZE: &str = "beryl.worker.block.report.batch-size";
const BLOCK_CLEANUP_QUEUE_CAPACITY: &str = "beryl.worker.block.cleanup.queue-capacity";
const BLOCK_CLEANUP_CONCURRENCY: &str = "beryl.worker.block.cleanup.concurrency";
const BLOCK_CLEANUP_RETRY_INITIAL_BACKOFF: &str = "beryl.worker.block.cleanup.retry.initial-backoff";
const BLOCK_CLEANUP_RETRY_MAX_BACKOFF: &str = "beryl.worker.block.cleanup.retry.max-backoff";
const SHUTDOWN_TIMEOUT: &str = "beryl.worker.shutdown.timeout";

/// Complete public key set consumed by one Worker configuration file.
const KNOWN_KEYS: &[&str] = &[
    CLUSTER_ID,
    HOST,
    BIND_HOST,
    RPC_PORT,
    HTTP_PORT,
    IDENTITY_FILE,
    RPC_MAX_CONCURRENT_REQUESTS,
    STREAM_FRAME_SIZE,
    STREAM_MAX_FRAME_SIZE,
    STREAM_IDLE_TIMEOUT,
    STORAGE_DIRS,
    STORAGE_RESERVED_SPACE,
    STORAGE_CHECK_INTERVAL,
    METADATA_ADDRESSES,
    METADATA_REQUEST_TIMEOUT,
    METADATA_RETRY_INITIAL_BACKOFF,
    METADATA_RETRY_MAX_BACKOFF,
    HEARTBEAT_INTERVAL,
    BLOCK_REPORT_INTERVAL,
    BLOCK_REPORT_BATCH_SIZE,
    BLOCK_CLEANUP_QUEUE_CAPACITY,
    BLOCK_CLEANUP_CONCURRENCY,
    BLOCK_CLEANUP_RETRY_INITIAL_BACKOFF,
    BLOCK_CLEANUP_RETRY_MAX_BACKOFF,
    SHUTDOWN_TIMEOUT,
    logging::FORMAT,
    logging::OUTPUT,
    logging::LEVEL,
];

/// Worker-to-Metadata request and retry configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRegistrationConfig {
    /// Internal identity for the one supported metadata group.
    pub group_name: GroupName,
    /// Tonic endpoint URIs derived from configured `host:port` addresses.
    pub endpoints: Vec<String>,
    /// Timeout shared by registration, heartbeat, and block report RPCs.
    pub request_timeout_ms: u64,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
}

impl Default for WorkerRegistrationConfig {
    fn default() -> Self {
        Self {
            group_name: GroupName::parse("root").expect("the supported metadata group is valid"),
            endpoints: vec!["http://127.0.0.1:18080".to_string()],
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

/// Bounded local execution of Metadata-authorized block cleanup commands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerBlockCleanupConfig {
    pub queue_capacity: usize,
    pub concurrency: usize,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
}

impl Default for WorkerBlockCleanupConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1_024,
            concurrency: 4,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 30_000,
        }
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
    /// Derived RPC bind address retained by the network runtime.
    pub rpc_bind: String,
    pub rpc_max_inflight: usize,
    pub default_frame_size: u32,
    pub max_frame_size: u32,
    pub stream_idle_timeout_ms: u64,
    pub store: WorkerStoreConfig,
    pub net: WorkerNetConfig,
    pub metadata: WorkerRegistrationConfig,
    pub heartbeat_interval_ms: u64,
    pub block_report_interval_ms: u64,
    pub block_report_batch_size: usize,
    pub block_cleanup: WorkerBlockCleanupConfig,
    /// Graceful RPC/background drain interval before remaining work is cancelled.
    pub shutdown_timeout_ms: u64,
    pub observability: ObservabilityConfig,
}

impl WorkerConfig {
    pub fn rpc_address(&self) -> String {
        format_host_port(&self.host, self.rpc_port)
    }

    pub fn rpc_address_parts(&self) -> (String, u32) {
        (self.host.clone(), u32::from(self.rpc_port))
    }

    pub fn http_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.http_port)
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let bind_host = "0.0.0.0".parse().expect("default bind host is valid");
        let rpc_bind = SocketAddr::new(bind_host, 19090).to_string();
        Self {
            cluster_id: "local-beryl".to_string(),
            host: "127.0.0.1".to_string(),
            bind_host,
            rpc_port: 19090,
            http_port: 19091,
            identity_path: PathBuf::from("data/worker/worker.identity"),
            rpc_bind: rpc_bind.clone(),
            rpc_max_inflight: 100,
            default_frame_size: 1024 * 1024,
            max_frame_size: 4 * 1024 * 1024,
            stream_idle_timeout_ms: 60_000,
            store: WorkerStoreConfig::default(),
            net: WorkerNetConfig::grpc_from_rpc(rpc_bind, 100, 4 * 1024 * 1024),
            metadata: WorkerRegistrationConfig::default(),
            heartbeat_interval_ms: 1_000,
            block_report_interval_ms: 1_000,
            block_report_batch_size: 1_000,
            block_cleanup: WorkerBlockCleanupConfig::default(),
            shutdown_timeout_ms: 30_000,
            observability: ObservabilityConfig {
                log: LogConfig {
                    format: "compact".to_string(),
                    output: "stderr".to_string(),
                    level: "info".to_string(),
                },
                resource: ResourceConfig::default(),
            },
        }
    }
}

impl WorkerConfig {
    /// Load Worker configuration from one YAML file.
    pub fn load<P: AsRef<Path>>(config_path: P) -> Result<Self, CommonError> {
        Self::from_server_config(&ServerConfig::load(config_path)?)
    }

    /// Build typed Worker configuration from shared YAML mechanics.
    pub fn from_server_config(server_config: &ServerConfig) -> Result<Self, CommonError> {
        let flat = server_config.as_flat();
        flat.ensure_only_known_keys(KNOWN_KEYS)?;
        let defaults = Self::default();

        let cluster_id = string_or(flat, CLUSTER_ID, &defaults.cluster_id)?;
        let host = string_or(flat, HOST, &defaults.host)?;
        let bind_host = string_or(flat, BIND_HOST, &defaults.bind_host.to_string())?
            .parse::<IpAddr>()
            .map_err(|_| invalid_config(BIND_HOST, "must be an IP address"))?;
        let rpc_port = port_or(flat, RPC_PORT, defaults.rpc_port)?;
        let http_port = port_or(flat, HTTP_PORT, defaults.http_port)?;
        let identity_path = PathBuf::from(string_or(
            flat,
            IDENTITY_FILE,
            defaults.identity_path.to_str().unwrap(),
        )?);
        let rpc_max_inflight = positive_usize_or(flat, RPC_MAX_CONCURRENT_REQUESTS, defaults.rpc_max_inflight)?;
        let default_frame_size = bytes_u32_or(flat, STREAM_FRAME_SIZE, defaults.default_frame_size)?;
        let max_frame_size = bytes_u32_or(flat, STREAM_MAX_FRAME_SIZE, defaults.max_frame_size)?;
        let stream_idle_timeout_ms = duration_ms_or(flat, STREAM_IDLE_TIMEOUT, defaults.stream_idle_timeout_ms)?;
        let store = parse_store_config(flat, &defaults.store)?;
        let metadata = parse_metadata_config(flat, &defaults.metadata)?;
        let heartbeat_interval_ms = duration_ms_or(flat, HEARTBEAT_INTERVAL, defaults.heartbeat_interval_ms)?;
        let block_report_interval_ms = duration_ms_or(flat, BLOCK_REPORT_INTERVAL, defaults.block_report_interval_ms)?;
        let block_report_batch_size =
            positive_usize_or(flat, BLOCK_REPORT_BATCH_SIZE, defaults.block_report_batch_size)?;
        let shutdown_timeout_ms = duration_ms_or(flat, SHUTDOWN_TIMEOUT, defaults.shutdown_timeout_ms)?;
        let cleanup_defaults = WorkerBlockCleanupConfig::default();
        let block_cleanup = WorkerBlockCleanupConfig {
            queue_capacity: positive_usize_or(flat, BLOCK_CLEANUP_QUEUE_CAPACITY, cleanup_defaults.queue_capacity)?,
            concurrency: positive_usize_or(flat, BLOCK_CLEANUP_CONCURRENCY, cleanup_defaults.concurrency)?,
            retry_initial_backoff_ms: duration_ms_or(
                flat,
                BLOCK_CLEANUP_RETRY_INITIAL_BACKOFF,
                cleanup_defaults.retry_initial_backoff_ms,
            )?,
            retry_max_backoff_ms: duration_ms_or(
                flat,
                BLOCK_CLEANUP_RETRY_MAX_BACKOFF,
                cleanup_defaults.retry_max_backoff_ms,
            )?,
        };
        if block_cleanup.retry_max_backoff_ms < block_cleanup.retry_initial_backoff_ms {
            return Err(invalid_config(
                BLOCK_CLEANUP_RETRY_MAX_BACKOFF,
                "must not be smaller than the initial backoff",
            ));
        }
        let rpc_bind = SocketAddr::new(bind_host, rpc_port).to_string();
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
            rpc_bind: rpc_bind.clone(),
            rpc_max_inflight,
            default_frame_size,
            max_frame_size,
            stream_idle_timeout_ms,
            store,
            net: WorkerNetConfig::grpc_from_rpc(rpc_bind, rpc_max_inflight, max_frame_size),
            metadata,
            heartbeat_interval_ms,
            block_report_interval_ms,
            block_report_batch_size,
            block_cleanup,
            shutdown_timeout_ms,
            observability,
        };
        config.validate()?;

        info!(
            host = %config.host,
            rpc_bind = %config.rpc_bind,
            http_bind = %config.http_addr(),
            store_dirs = config.store.dirs.len(),
            metadata_addresses = ?config.metadata.endpoints,
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
        if self.block_cleanup.concurrency > self.block_cleanup.queue_capacity {
            return Err(invalid_config(
                BLOCK_CLEANUP_CONCURRENCY,
                "must not exceed the cleanup queue capacity",
            ));
        }
        validate_store_config(self)?;
        self.metadata.validate()?;
        if self.net.listeners.is_empty() {
            return Err(invalid_config(RPC_PORT, "must create a Worker RPC listener"));
        }
        for listener in &self.net.listeners {
            if listener.protocol == WorkerNetProtocol::Grpc && listener.bind.parse::<SocketAddr>().is_err() {
                return Err(invalid_config(BIND_HOST, "does not form a valid RPC bind address"));
            }
        }
        Ok(())
    }
}

fn parse_metadata_config(
    flat: &FlatConfig,
    defaults: &WorkerRegistrationConfig,
) -> Result<WorkerRegistrationConfig, CommonError> {
    let addresses = if flat.contains_key(METADATA_ADDRESSES) {
        flat.get_string_list(METADATA_ADDRESSES)
            .ok_or_else(|| invalid_config(METADATA_ADDRESSES, "must be a list of host:port addresses"))?
    } else {
        vec!["127.0.0.1:18080".to_string()]
    };
    if addresses.is_empty() || addresses.iter().any(|address| address.trim().is_empty()) {
        return Err(invalid_config(
            METADATA_ADDRESSES,
            "must contain at least one non-empty address",
        ));
    }
    let endpoints = addresses
        .into_iter()
        .map(|address| normalize_endpoint(&address))
        .collect();
    let config = WorkerRegistrationConfig {
        group_name: GroupName::parse("root").expect("the supported metadata group is valid"),
        endpoints,
        request_timeout_ms: duration_ms_or(flat, METADATA_REQUEST_TIMEOUT, defaults.request_timeout_ms)?,
        retry_initial_backoff_ms: duration_ms_or(
            flat,
            METADATA_RETRY_INITIAL_BACKOFF,
            defaults.retry_initial_backoff_ms,
        )?,
        retry_max_backoff_ms: duration_ms_or(flat, METADATA_RETRY_MAX_BACKOFF, defaults.retry_max_backoff_ms)?,
    };
    config.validate()?;
    Ok(config)
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
        reserve_space_bytes: bytes_u64_or(flat, STORAGE_RESERVED_SPACE, defaults.reserve_space_bytes)?,
        check_interval_ms: duration_ms_or(flat, STORAGE_CHECK_INTERVAL, defaults.check_interval_ms)?,
    })
}

/// Parses each configured storage directory and rejects fields the Worker does not consume.
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
            let Some(field_name) = field.as_str() else {
                return Err(CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    format!("unknown config key under {STORAGE_DIRS}.{id}: {field:?}; field names must be strings"),
                ));
            };
            if !matches!(field_name, "path" | "tier" | "capacity") {
                return Err(CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    format!("unknown config key: {STORAGE_DIRS}.{id}.{field_name}"),
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
    pub fn validate(&self) -> Result<(), CommonError> {
        if self.endpoints.is_empty() {
            return Err(invalid_config(METADATA_ADDRESSES, "must not be empty"));
        }
        for endpoint in &self.endpoints {
            Endpoint::from_shared(endpoint.clone())
                .map_err(|_| invalid_config(METADATA_ADDRESSES, "contains an invalid address"))?;
        }
        if self.request_timeout_ms == 0
            || self.retry_initial_backoff_ms == 0
            || self.retry_max_backoff_ms < self.retry_initial_backoff_ms
        {
            return Err(invalid_config(
                METADATA_REQUEST_TIMEOUT,
                "requires positive timeout and ordered retry backoff",
            ));
        }
        Ok(())
    }
}

fn string_or(flat: &FlatConfig, key: &'static str, default: &str) -> Result<String, CommonError> {
    if !flat.contains_key(key) {
        return Ok(default.to_string());
    }
    flat.get_str(key).ok_or_else(|| invalid_config(key, "must be a string"))
}

fn port_or(flat: &FlatConfig, key: &'static str, default: u16) -> Result<u16, CommonError> {
    if !flat.contains_key(key) {
        return Ok(default);
    }
    let value = flat
        .get_i64(key)
        .ok_or_else(|| invalid_config(key, "must be an integer"))?;
    u16::try_from(value)
        .ok()
        .filter(|port| *port != 0)
        .ok_or_else(|| invalid_config(key, "must be in range 1-65535"))
}

fn positive_usize_or(flat: &FlatConfig, key: &'static str, default: usize) -> Result<usize, CommonError> {
    if !flat.contains_key(key) {
        return Ok(default);
    }
    flat.get_usize(key)
        .filter(|value| *value != 0)
        .ok_or_else(|| invalid_config(key, "must be greater than zero"))
}

fn duration_ms_or(flat: &FlatConfig, key: &'static str, default: u64) -> Result<u64, CommonError> {
    if !flat.contains_key(key) {
        return Ok(default);
    }
    let duration = flat
        .get_duration(key)
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| invalid_config(key, "must be a positive duration"))?;
    u64::try_from(duration.as_millis()).map_err(|_| invalid_config(key, "is too large"))
}

fn bytes_u32_or(flat: &FlatConfig, key: &'static str, default: u32) -> Result<u32, CommonError> {
    if !flat.contains_key(key) {
        return Ok(default);
    }
    let bytes = flat
        .get_bytes(key)
        .ok_or_else(|| invalid_config(key, "must be a size such as 1MiB"))?;
    u32::try_from(bytes).map_err(|_| invalid_config(key, "is too large"))
}

fn bytes_u64_or(flat: &FlatConfig, key: &'static str, default: u64) -> Result<u64, CommonError> {
    if !flat.contains_key(key) {
        return Ok(default);
    }
    flat.get_bytes(key)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| invalid_config(key, "must be a size such as 1GiB"))
}

fn invalid_config(key: &'static str, detail: &'static str) -> CommonError {
    CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {detail}"))
}

fn validate_public_host(key: &'static str, host: &str) -> Result<(), CommonError> {
    if host.is_empty() || host != host.trim() || host.chars().any(char::is_whitespace) {
        return Err(invalid_config(key, "must be a host or IP without whitespace"));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return if ip.is_unspecified() {
            Err(invalid_config(key, "must be a routable host or IP"))
        } else {
            Ok(())
        };
    }
    if host.contains("://") || host.contains([':', '/', '\\']) {
        return Err(invalid_config(key, "must not include a scheme, port, or path"));
    }
    Ok(())
}

fn format_host_port(host: &str, port: u16) -> String {
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{host}]:{port}"),
        _ => format!("{host}:{port}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_flat() -> FlatConfig {
        let mut flat = FlatConfig::new();
        flat.set("beryl.logging.format", "compact");
        flat.set("beryl.logging.output", "stderr");
        flat.set("beryl.logging.level", "info");
        flat
    }

    #[test]
    fn target_config_loads_network_storage_and_background_limits() {
        let mut flat = base_flat();
        flat.set(HOST, "worker-01");
        flat.set(BIND_HOST, "127.0.0.1");
        flat.set(RPC_PORT, 29090i64);
        flat.set(HTTP_PORT, 29091i64);
        flat.set(HEARTBEAT_INTERVAL, "2s");
        flat.set(BLOCK_REPORT_BATCH_SIZE, 500i64);
        flat.set(SHUTDOWN_TIMEOUT, "20s");
        flat.insert(
            STORAGE_DIRS.to_string(),
            serde_yaml::from_str::<Value>("hdd0:\n  path: /tmp/hdd0\n  tier: hdd\n  capacity: 2GiB\n").unwrap(),
        );
        flat.insert(
            METADATA_ADDRESSES.to_string(),
            Value::Sequence(vec![Value::String("metadata:18080".to_string())]),
        );

        let config = WorkerConfig::from_server_config(&ServerConfig::from_flat(flat)).unwrap();

        assert_eq!(config.rpc_address(), "worker-01:29090");
        assert_eq!(config.rpc_bind, "127.0.0.1:29090");
        assert_eq!(config.http_addr(), "127.0.0.1:29091".parse().unwrap());
        assert_eq!(config.metadata.group_name.as_str(), "root");
        assert_eq!(config.metadata.endpoints, vec!["http://metadata:18080"]);
        assert_eq!(config.heartbeat_interval_ms, 2_000);
        assert_eq!(config.block_report_batch_size, 500);
        assert_eq!(config.shutdown_timeout_ms, 20_000);
        assert_eq!(config.store.dirs["hdd0"].tier, Tier::Hdd);
    }

    #[test]
    fn invalid_active_frame_and_address_values_are_rejected() {
        let mut flat = base_flat();
        flat.set(STREAM_FRAME_SIZE, "8MiB");
        flat.set(STREAM_MAX_FRAME_SIZE, "4MiB");
        assert!(WorkerConfig::from_server_config(&ServerConfig::from_flat(flat)).is_err());

        let mut flat = base_flat();
        flat.set(STREAM_FRAME_SIZE, "4MiB");
        flat.set(STREAM_MAX_FRAME_SIZE, "8MiB");
        assert!(WorkerConfig::from_server_config(&ServerConfig::from_flat(flat)).is_err());

        for host in ["0.0.0.0", " worker-01", "http://worker-01", "worker-01:19090"] {
            let mut flat = base_flat();
            flat.set(HOST, host);
            assert!(WorkerConfig::from_server_config(&ServerConfig::from_flat(flat)).is_err());
        }
    }

    #[test]
    fn unknown_worker_key_is_rejected() {
        let mut flat = base_flat();
        flat.set("beryl.worker.rpc.prt", 19090i64);

        let error =
            WorkerConfig::from_server_config(&ServerConfig::from_flat(flat)).expect_err("unknown Worker key must fail");

        assert!(error.message.contains("beryl.worker.rpc.prt"));
    }

    #[test]
    fn unknown_store_directory_fields_are_rejected() {
        for (entry, unknown_field) in [
            (
                "hdd0:\n  path: /tmp/hdd0\n  tier: hdd\n  capacity: 2GiB\n  capcity: 3GiB\n",
                "capcity",
            ),
            (
                "hdd0:\n  path: /tmp/hdd0\n  tier: hdd\n  capacity: 2GiB\n  1: extra\n",
                "1",
            ),
        ] {
            let mut flat = base_flat();
            flat.insert(STORAGE_DIRS.to_string(), serde_yaml::from_str::<Value>(entry).unwrap());

            let error = WorkerConfig::from_server_config(&ServerConfig::from_flat(flat))
                .expect_err("unknown storage directory field must fail");

            assert!(error.message.contains("beryl.worker.storage.dirs.hdd0"));
            assert!(error.message.contains(unknown_field));
        }
    }
}
