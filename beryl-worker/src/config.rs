// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker configuration for the current data service.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use beryl_common::config::ServerConfig;
use beryl_common::error::{CommonError, CommonErrorKind};
use beryl_common::observe::ObservabilityConfig;
use beryl_types::{GroupName, Tier};
use tonic::transport::Endpoint;
use tracing::info;

use crate::net::config::WorkerNetConfig;
use crate::net::protocol::WorkerNetProtocol;

/// Worker metadata registration configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRegistrationConfig {
    /// Stable metadata group name registered by this worker process.
    pub group_name: GroupName,
    /// Metadata service endpoints used by worker registration, heartbeat, and block report.
    /// The first endpoint may be used as the initial registration target.
    pub endpoints: Vec<String>,
    /// Per-attempt registration timeout shared by startup registration and heartbeat RPCs.
    pub register_timeout_ms: u64,
    /// Initial retry backoff after retryable registration failures.
    pub register_retry_initial_backoff_ms: u64,
    /// Maximum retry backoff after retryable registration failures.
    pub register_retry_max_backoff_ms: u64,
}

impl Default for WorkerRegistrationConfig {
    fn default() -> Self {
        Self {
            group_name: GroupName::parse("root").expect("default group name is valid"),
            endpoints: vec!["http://127.0.0.1:18080".to_string()],
            register_timeout_ms: 5_000,
            register_retry_initial_backoff_ms: 200,
            register_retry_max_backoff_ms: 5_000,
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
    pub selection_policy: String,
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
            selection_policy: "round_robin".to_string(),
            check_interval_ms: 30_000,
        }
    }
}

/// Worker configuration.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// Cluster identity validated against local worker storage marker.
    pub cluster_id: String,
    /// Local persisted worker identity file.
    pub identity_path: PathBuf,
    /// RPC server bind address.
    pub rpc_bind: String,
    /// Routable gRPC data endpoint registered with metadata.
    pub rpc_advertised_endpoint: String,
    /// Maximum concurrent RPC requests per gRPC connection.
    pub rpc_max_inflight: usize,
    /// Transport frame payload size negotiated at stream open.
    /// This controls network batching and does not define StorageChunk size.
    pub default_frame_size: u32,
    /// Upper bound for negotiated transport frame payload size.
    pub max_frame_size: u32,
    /// Idle timeout for runtime stream state.
    pub stream_idle_timeout_ms: u64,
    /// Worker-local block store configuration.
    pub store: WorkerStoreConfig,
    /// Worker-owned service-specific network configuration.
    pub net: WorkerNetConfig,
    /// Worker metadata registration configuration.
    pub metadata: WorkerRegistrationConfig,
    /// Shared observability configuration.
    pub observability: ObservabilityConfig,
}

impl WorkerConfig {
    /// Load worker configuration from a YAML file.
    pub fn load<P: AsRef<Path>>(config_path: P) -> Result<Self, CommonError> {
        let server_config = ServerConfig::load(config_path)?;
        Self::from_server_config(&server_config)
    }

    /// Create worker configuration from the repository-wide config shape.
    pub fn from_server_config(server_config: &ServerConfig) -> Result<Self, CommonError> {
        let worker_sub = server_config.as_flat().sub("worker");
        let flat = server_config.as_flat();
        let default_cluster_id = "local-beryl".to_string();
        let default_identity_path = PathBuf::from("data/worker/worker.identity");
        let default_rpc_bind = "0.0.0.0:9090".to_string();
        let default_rpc_max_inflight = 100usize;
        let default_frame_size = 1024 * 1024;
        let default_max_frame_size = 4 * 1024 * 1024;
        let default_stream_idle_timeout_ms = 60_000u64;
        let default_store = WorkerStoreConfig::default();
        let metadata_defaults = WorkerRegistrationConfig::default();

        let cluster_id = Self::root_str_or(flat, "cluster.id", &default_cluster_id)?;
        let identity_path = Self::path_or(&worker_sub, "identity.path", default_identity_path)?;
        let rpc_bind = Self::str_or(&worker_sub, "rpc.bind", &default_rpc_bind, "worker.rpc.bind")?;
        let observability = ObservabilityConfig::from_flat(flat)?;
        let rpc_advertised_endpoint = worker_sub
            .get_str("rpc.advertised_endpoint")
            .ok_or_else(|| invalid_config("worker.rpc.advertised_endpoint", "must be present and be a string"))?;
        if worker_sub.contains_key("rpc.advertised_endpoint") && rpc_advertised_endpoint.trim().is_empty() {
            return Err(invalid_config("worker.rpc.advertised_endpoint", "must not be empty"));
        }
        let rpc_max_inflight = Self::usize_or(
            &worker_sub,
            "rpc.max_inflight",
            default_rpc_max_inflight,
            "worker.rpc.max_inflight",
        )?;
        let default_frame_size = Self::bytes_u32(
            &worker_sub,
            "default_frame_size",
            default_frame_size,
            "worker.default_frame_size",
        )?;
        let max_frame_size = Self::bytes_u32(
            &worker_sub,
            "max_frame_size",
            default_max_frame_size,
            "worker.max_frame_size",
        )?;
        if worker_sub.contains_key("window_bytes") {
            return Err(invalid_config("worker.window_bytes", "is no longer supported"));
        }
        let stream_idle_timeout_ms = Self::usize_or(
            &worker_sub,
            "stream.idle_timeout_ms",
            default_stream_idle_timeout_ms as usize,
            "worker.stream.idle_timeout_ms",
        )? as u64;
        let store = parse_store_config(&worker_sub, &default_store)?;
        let endpoints = metadata_endpoints(&worker_sub, &metadata_defaults)?;
        let group_name = Self::str_or(
            &worker_sub,
            "metadata.group.name",
            metadata_defaults.group_name.as_str(),
            "worker.metadata.group.name",
        )?;
        let metadata = WorkerRegistrationConfig {
            group_name: parse_group_name("worker.metadata.group.name", group_name)?,
            endpoints,
            register_timeout_ms: Self::usize_or(
                &worker_sub,
                "metadata.register_timeout_ms",
                metadata_defaults.register_timeout_ms as usize,
                "worker.metadata.register_timeout_ms",
            )? as u64,
            register_retry_initial_backoff_ms: Self::usize_or(
                &worker_sub,
                "metadata.register_retry_initial_backoff_ms",
                metadata_defaults.register_retry_initial_backoff_ms as usize,
                "worker.metadata.register_retry_initial_backoff_ms",
            )? as u64,
            register_retry_max_backoff_ms: Self::usize_or(
                &worker_sub,
                "metadata.register_retry_max_backoff_ms",
                metadata_defaults.register_retry_max_backoff_ms as usize,
                "worker.metadata.register_retry_max_backoff_ms",
            )? as u64,
        };

        let config = Self {
            cluster_id,
            identity_path,
            rpc_bind: rpc_bind.clone(),
            rpc_advertised_endpoint,
            rpc_max_inflight,
            default_frame_size,
            max_frame_size,
            stream_idle_timeout_ms,
            store,
            net: WorkerNetConfig::grpc_from_rpc(rpc_bind, rpc_max_inflight, max_frame_size),
            metadata,
            observability,
        };

        config.validate()?;

        info!(
            identity_path = ?config.identity_path,
            rpc_bind = %config.rpc_bind,
            metrics_bind = %config.observability.metrics.prometheus.bind,
            rpc_advertised_endpoint = %config.rpc_advertised_endpoint,
            rpc_max_inflight = config.rpc_max_inflight,
            default_frame_size = config.default_frame_size,
            max_frame_size = config.max_frame_size,
            store_dirs = config.store.dirs.len(),
            store_reserve_space_bytes = config.store.reserve_space_bytes,
            store_selection_policy = %config.store.selection_policy,
            store_check_interval_ms = config.store.check_interval_ms,
            net_listeners = config.net.listeners.len(),
            metadata_endpoints = ?config.metadata.endpoints,
            metadata_group_name = %config.metadata.group_name,
            register_timeout_ms = config.metadata.register_timeout_ms,
            register_retry_initial_backoff_ms = config.metadata.register_retry_initial_backoff_ms,
            register_retry_max_backoff_ms = config.metadata.register_retry_max_backoff_ms,
            "Worker configuration loaded"
        );

        Ok(config)
    }

    /// Validate shape-only constraints without touching local storage.
    pub fn validate(&self) -> Result<(), CommonError> {
        if self.cluster_id.trim().is_empty() {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "cluster.id must not be empty",
            ));
        }

        if self.identity_path.as_os_str().is_empty() {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "worker.identity.path must not be empty",
            ));
        }

        if self.rpc_bind.parse::<std::net::SocketAddr>().is_err() {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("invalid worker.rpc.bind address: {}", self.rpc_bind),
            ));
        }

        self.rpc_advertised_endpoint_parts()?;

        if self.rpc_max_inflight == 0 {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "worker.rpc.max_inflight must be greater than zero",
            ));
        }

        if self.default_frame_size == 0 {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "worker.default_frame_size must be greater than zero",
            ));
        }

        if self.max_frame_size == 0 {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "worker.max_frame_size must be greater than zero",
            ));
        }

        if self.default_frame_size > self.max_frame_size {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!(
                    "worker.default_frame_size ({}) must be <= worker.max_frame_size ({})",
                    self.default_frame_size, self.max_frame_size
                ),
            ));
        }

        if self.stream_idle_timeout_ms == 0 {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "worker.stream.idle_timeout_ms must be greater than zero",
            ));
        }

        validate_store_config(self)?;

        self.metadata.validate()?;

        if self.net.listeners.is_empty() {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "worker.net.listeners must not be empty",
            ));
        }

        for listener in &self.net.listeners {
            if listener.protocol == WorkerNetProtocol::Grpc && listener.bind.parse::<std::net::SocketAddr>().is_err() {
                return Err(CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    format!("invalid worker gRPC listener bind address: {}", listener.bind),
                ));
            }
            if listener.max_inflight == 0 {
                return Err(CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    "worker.net.listeners.max_inflight must be greater than zero",
                ));
            }
            if listener.max_frame_size == 0 {
                return Err(CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    "worker.net.listeners.max_frame_size must be greater than zero",
                ));
            }
        }

        Ok(())
    }

    /// Return the host and port that registration advertises to metadata.
    pub fn rpc_advertised_endpoint_parts(&self) -> Result<(String, u32), CommonError> {
        parse_advertised_endpoint(&self.rpc_advertised_endpoint)
    }

    fn str_or(
        flat: &beryl_common::config::FlatConfig,
        key: &str,
        fallback: &str,
        field_name: &'static str,
    ) -> Result<String, CommonError> {
        if let Some(value) = flat.get_str(key) {
            return Ok(value);
        }
        if flat.contains_key(key) {
            return Err(invalid_config(field_name, "must be a string"));
        }
        Ok(fallback.to_string())
    }

    fn root_str_or(
        flat: &beryl_common::config::FlatConfig,
        key: &'static str,
        fallback: &str,
    ) -> Result<String, CommonError> {
        if let Some(value) = flat.get_str(key) {
            return Ok(value);
        }
        if flat.contains_key(key) {
            return Err(invalid_config(key, "must be a string"));
        }
        Ok(fallback.to_string())
    }

    fn path_or(flat: &beryl_common::config::FlatConfig, key: &str, fallback: PathBuf) -> Result<PathBuf, CommonError> {
        if let Some(value) = flat.get_str(key) {
            return Ok(PathBuf::from(value));
        }
        if flat.contains_key(key) {
            return Err(invalid_config("worker.identity.path", "must be a string"));
        }
        Ok(fallback)
    }

    fn usize_or(
        flat: &beryl_common::config::FlatConfig,
        key: &str,
        fallback: usize,
        field_name: &'static str,
    ) -> Result<usize, CommonError> {
        if let Some(value) = flat.get_usize(key) {
            return Ok(value);
        }
        if flat.contains_key(key) {
            return Err(invalid_config(field_name, "must be a non-negative integer"));
        }
        Ok(fallback)
    }

    fn bytes_u32(
        flat: &beryl_common::config::FlatConfig,
        key: &str,
        fallback: u32,
        field_name: &'static str,
    ) -> Result<u32, CommonError> {
        match flat.get_bytes(key) {
            Some(value) => u32::try_from(value).map_err(|_| {
                CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    format!("{field_name} exceeds u32 byte size"),
                )
            }),
            None if flat.contains_key(key) => Err(invalid_config(field_name, "must be a byte size")),
            None => Ok(fallback),
        }
    }

    fn bytes_u64(
        flat: &beryl_common::config::FlatConfig,
        key: &str,
        fallback: u64,
        field_name: &'static str,
    ) -> Result<u64, CommonError> {
        match flat.get_bytes(key) {
            Some(value) => u64::try_from(value).map_err(|_| invalid_config(field_name, "exceeds u64 byte size")),
            None if flat.contains_key(key) => Err(invalid_config(field_name, "must be a byte size")),
            None => Ok(fallback),
        }
    }
}

fn parse_store_config(
    flat: &beryl_common::config::FlatConfig,
    defaults: &WorkerStoreConfig,
) -> Result<WorkerStoreConfig, CommonError> {
    Ok(WorkerStoreConfig {
        dirs: parse_store_dirs(flat)?,
        reserve_space_bytes: WorkerConfig::bytes_u64(
            flat,
            "store.reserve_space",
            defaults.reserve_space_bytes,
            "worker.store.reserve_space",
        )?,
        selection_policy: WorkerConfig::str_or(
            flat,
            "store.selection_policy",
            &defaults.selection_policy,
            "worker.store.selection_policy",
        )?,
        check_interval_ms: WorkerConfig::usize_or(
            flat,
            "store.check_interval_ms",
            defaults.check_interval_ms as usize,
            "worker.store.check_interval_ms",
        )? as u64,
    })
}

fn parse_store_dirs(flat: &beryl_common::config::FlatConfig) -> Result<BTreeMap<String, StoreDirConfig>, CommonError> {
    if flat.contains_key("store.dirs") {
        return Err(invalid_config(
            "worker.store.dirs",
            "must use worker.store.dirs.<dir_id>.path/tier/capacity",
        ));
    }

    let keys = flat.keys_with_prefix("store.dirs");
    if keys.is_empty() {
        return Err(invalid_config("worker.store.dirs", "must be present and non-empty"));
    }

    let mut ids = BTreeSet::new();
    for key in keys {
        let rest = key.strip_prefix("store.dirs.").ok_or_else(|| {
            CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("worker.{key} must use worker.store.dirs.<dir_id>.<field>"),
            )
        })?;
        let (id, field) = rest.split_once('.').ok_or_else(|| {
            CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("worker.{key} must use worker.store.dirs.<dir_id>.<field>"),
            )
        })?;
        if id.is_empty() || id.trim() != id {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("worker.{key} has an invalid store dir id"),
            ));
        }
        if field.contains('.') {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("worker.{key} has an unsupported nested store dir field"),
            ));
        }
        match field {
            "path" | "tier" | "capacity" => {}
            "id" => {
                return Err(CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    format!("worker.{key} is unsupported; dir id must come from the key segment"),
                ));
            }
            _ => {
                return Err(CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    format!("worker.{key} is unsupported"),
                ));
            }
        }
        ids.insert(id.to_string());
    }

    if ids.is_empty() {
        return Err(invalid_config("worker.store.dirs", "must be non-empty"));
    }

    let mut out = BTreeMap::new();
    for id in ids {
        let path_key = format!("store.dirs.{id}.path");
        let tier_key = format!("store.dirs.{id}.tier");
        let capacity_key = format!("store.dirs.{id}.capacity");
        let path = required_store_str(flat, &path_key, format!("worker.{path_key}"))?;
        let tier_raw = required_store_str(flat, &tier_key, format!("worker.{tier_key}"))?;
        let tier = Tier::parse(&tier_raw).map_err(|err| {
            CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("worker.store.dirs.{id}.tier {err}"),
            )
        })?;
        let capacity_bytes = required_store_bytes(flat, &capacity_key, format!("worker.{capacity_key}"))?;
        if capacity_bytes == 0 {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("worker.store.dirs.{id}.capacity must be greater than zero"),
            ));
        }
        out.insert(
            id,
            StoreDirConfig {
                path: PathBuf::from(path),
                tier,
                capacity_bytes,
            },
        );
    }
    Ok(out)
}

fn required_store_str(
    flat: &beryl_common::config::FlatConfig,
    field: &str,
    display_key: String,
) -> Result<String, CommonError> {
    let value = flat.get_str(field).ok_or_else(|| {
        CommonError::new(
            CommonErrorKind::InvalidArgument,
            format!("{display_key} must be present"),
        )
    })?;
    if value.trim().is_empty() {
        return Err(CommonError::new(
            CommonErrorKind::InvalidArgument,
            format!("{display_key} must not be empty"),
        ));
    }
    Ok(value)
}

fn required_store_bytes(
    flat: &beryl_common::config::FlatConfig,
    field: &str,
    display_key: String,
) -> Result<u64, CommonError> {
    let value = flat.get_bytes(field).ok_or_else(|| {
        CommonError::new(
            CommonErrorKind::InvalidArgument,
            format!("{display_key} must be a byte size"),
        )
    })?;
    u64::try_from(value).map_err(|_| {
        CommonError::new(
            CommonErrorKind::InvalidArgument,
            format!("{display_key} exceeds u64 byte size"),
        )
    })
}

fn validate_store_config(config: &WorkerConfig) -> Result<(), CommonError> {
    if config.store.dirs.is_empty() {
        return Err(invalid_config("worker.store.dirs", "must be non-empty"));
    }
    let mut paths = HashSet::new();
    for (id, dir) in &config.store.dirs {
        if id.trim().is_empty() || id.trim() != id {
            return Err(invalid_config(
                "worker.store.dirs.<dir_id>",
                "must be a non-empty segment",
            ));
        }
        if dir.path.as_os_str().is_empty() {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("worker.store.dirs.{id}.path must not be empty"),
            ));
        }
        if !paths.insert(dir.path.clone()) {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("worker.store.dirs duplicate path: {}", dir.path.display()),
            ));
        }
        if dir.capacity_bytes == 0 {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("worker.store.dirs.{id}.capacity must be greater than zero"),
            ));
        }
    }
    if config.store.selection_policy != "round_robin" {
        return Err(CommonError::new(
            CommonErrorKind::InvalidArgument,
            "worker.store.selection_policy must be round_robin; balanced is TODO(store)".to_string(),
        ));
    }
    if config.store.check_interval_ms == 0 {
        return Err(invalid_config(
            "worker.store.check_interval_ms",
            "must be greater than zero",
        ));
    }
    Ok(())
}

impl WorkerRegistrationConfig {
    /// Validate worker metadata registration config without opening a connection.
    pub fn validate(&self) -> Result<(), CommonError> {
        if self.endpoints.is_empty() {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "worker.metadata.endpoints must not be empty",
            ));
        }

        for endpoint in &self.endpoints {
            if endpoint.is_empty() {
                return Err(CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    "worker.metadata.endpoints entries must not be empty",
                ));
            }
            if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
                return Err(CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    "worker.metadata.endpoints entries must include http:// or https:// scheme",
                ));
            }
            Endpoint::from_shared(endpoint.clone()).map_err(|err| {
                CommonError::new(
                    CommonErrorKind::InvalidArgument,
                    format!("worker.metadata.endpoints entry must be a valid tonic endpoint URI: {err}"),
                )
            })?;
        }

        if self.register_timeout_ms == 0 {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "worker.metadata.register_timeout_ms must be greater than zero",
            ));
        }

        if self.register_retry_initial_backoff_ms == 0 {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "worker.metadata.register_retry_initial_backoff_ms must be greater than zero",
            ));
        }

        if self.register_retry_max_backoff_ms == 0 {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "worker.metadata.register_retry_max_backoff_ms must be greater than zero",
            ));
        }

        if self.register_retry_max_backoff_ms < self.register_retry_initial_backoff_ms {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!(
                    "worker.metadata.register_retry_max_backoff_ms ({}) must be >= worker.metadata.register_retry_initial_backoff_ms ({})",
                    self.register_retry_max_backoff_ms, self.register_retry_initial_backoff_ms
                ),
            ));
        }

        Ok(())
    }
}

fn invalid_config(key: &'static str, detail: &'static str) -> CommonError {
    CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {detail}"))
}

fn parse_group_name(key: &'static str, raw: String) -> Result<GroupName, CommonError> {
    GroupName::parse(raw).map_err(|err| CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {err}")))
}

fn metadata_endpoints(
    worker_sub: &beryl_common::config::FlatConfig,
    defaults: &WorkerRegistrationConfig,
) -> Result<Vec<String>, CommonError> {
    if let Some(endpoints) = worker_sub.get_str("metadata.endpoints") {
        return parse_csv_endpoints(endpoints);
    }
    if worker_sub.contains_key("metadata.endpoints") {
        return Err(invalid_config("worker.metadata.endpoints", "must be a string"));
    }
    Ok(defaults.endpoints.clone())
}

fn parse_csv_endpoints(value: String) -> Result<Vec<String>, CommonError> {
    let endpoints: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    if endpoints.is_empty() {
        return Err(invalid_config(
            "worker.metadata.endpoints",
            "must contain at least one endpoint",
        ));
    }
    Ok(endpoints)
}

fn parse_advertised_endpoint(value: &str) -> Result<(String, u32), CommonError> {
    if value.is_empty() {
        return Err(CommonError::new(
            CommonErrorKind::InvalidArgument,
            "worker.rpc.advertised_endpoint must not be empty",
        ));
    }

    if !(value.starts_with("http://") || value.starts_with("https://")) {
        return Err(CommonError::new(
            CommonErrorKind::InvalidArgument,
            "worker.rpc.advertised_endpoint must include http:// or https:// scheme",
        ));
    }

    let endpoint = Endpoint::from_shared(value.to_string()).map_err(|err| {
        CommonError::new(
            CommonErrorKind::InvalidArgument,
            format!("worker.rpc.advertised_endpoint must be a valid tonic endpoint URI: {err}"),
        )
    })?;
    let uri = endpoint.uri();
    let raw_host = uri.host().ok_or_else(|| {
        CommonError::new(
            CommonErrorKind::InvalidArgument,
            "worker.rpc.advertised_endpoint must include a host",
        )
    })?;
    let host = raw_host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(raw_host);
    if host.is_empty() {
        return Err(CommonError::new(
            CommonErrorKind::InvalidArgument,
            "worker.rpc.advertised_endpoint host must not be empty",
        ));
    }
    if host.parse::<IpAddr>().is_ok_and(|ip| ip.is_unspecified()) {
        return Err(CommonError::new(
            CommonErrorKind::InvalidArgument,
            "worker.rpc.advertised_endpoint must not use a wildcard host",
        ));
    }
    let port = uri.port_u16().ok_or_else(|| {
        CommonError::new(
            CommonErrorKind::InvalidArgument,
            "worker.rpc.advertised_endpoint must include a port",
        )
    })?;

    Ok((host.to_string(), u32::from(port)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::protocol::WorkerNetProtocol;
    use std::fs;
    use tempfile::TempDir;

    fn test_worker_config() -> WorkerConfig {
        WorkerConfig {
            cluster_id: "local-beryl".to_string(),
            identity_path: PathBuf::from("data/worker/worker.identity"),
            rpc_bind: "0.0.0.0:9090".to_string(),
            rpc_advertised_endpoint: "http://127.0.0.1:9090".to_string(),
            rpc_max_inflight: 100,
            default_frame_size: 1024 * 1024,
            max_frame_size: 4 * 1024 * 1024,
            stream_idle_timeout_ms: 60_000,
            store: WorkerStoreConfig::default(),
            net: WorkerNetConfig::grpc_from_rpc("0.0.0.0:9090".to_string(), 100, 4 * 1024 * 1024),
            metadata: WorkerRegistrationConfig::default(),
            observability: test_observability_config(),
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
        flat.set("observe.metrics.prometheus.bind", "127.0.0.1:19091");
        flat.set("observe.metrics.prometheus.path", "/metrics");
        ObservabilityConfig::from_flat(&flat).expect("test observe config")
    }

    fn with_test_observe_yaml(config: impl AsRef<str>) -> String {
        format!(
            "{}\n{}",
            config.as_ref().trim_end(),
            r#"
observe.log.format: compact
observe.log.output: stderr
observe.log.level: "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn"
observe.metrics.prometheus.bind: "127.0.0.1:19091"
observe.metrics.prometheus.path: "/metrics"
"#
            .trim_start()
        )
    }

    fn load_test_config(config: impl AsRef<str>) -> Result<WorkerConfig, CommonError> {
        let temp_dir = TempDir::new().expect("temp config dir");
        let config_path = temp_dir.path().join("worker.yaml");
        fs::write(&config_path, with_test_observe_yaml(config)).expect("write worker config");
        WorkerConfig::load(&config_path)
    }

    #[test]
    fn test_load_real_worker_config() {
        let config_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("worker lives under workspace root")
            .join("conf/worker.yaml");

        let config = WorkerConfig::load(&config_path)
            .unwrap_or_else(|err| panic!("Failed to load {}: {err:?}", config_path.display()));

        let hdd0 = config.store.dirs.get("hdd0").expect("hdd0 store dir");
        assert_eq!(hdd0.path, PathBuf::from("data/worker/hdd0"));
        assert_eq!(hdd0.tier, beryl_types::Tier::Hdd);
        assert_eq!(config.identity_path, PathBuf::from("data/worker/worker.identity"));
        assert_eq!(config.rpc_bind, "0.0.0.0:9090");
        assert_eq!(config.rpc_advertised_endpoint, "http://127.0.0.1:9090");
        assert_eq!(config.observability.metrics.prometheus.bind, "127.0.0.1:19091");
    }

    #[test]
    fn loads_default_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("worker.yaml");
        fs::write(
            &config_path,
            with_test_observe_yaml(
                r#"
worker.rpc.bind: "127.0.0.1:9090"
worker.rpc.advertised_endpoint: "http://127.0.0.1:9090"
worker.store.dirs.hdd0.path: "/tmp/beryl-worker/hdd0"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
worker.store.reserve_space: "1GB"
worker.store.selection_policy: "round_robin"
worker.store.check_interval_ms: 30000
worker.metadata.endpoints: "http://127.0.0.1:18080"
"#,
            ),
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.rpc_bind, "127.0.0.1:9090");
        assert_eq!(config.identity_path, PathBuf::from("data/worker/worker.identity"));
        assert_eq!(config.rpc_max_inflight, 100);
        assert_eq!(config.default_frame_size, 1024 * 1024);
        assert_eq!(config.max_frame_size, 4 * 1024 * 1024);
        assert_eq!(config.stream_idle_timeout_ms, 60_000);
        let hdd0 = config.store.dirs.get("hdd0").unwrap();
        assert_eq!(config.store.dirs.len(), 1);
        assert_eq!(hdd0.path, PathBuf::from("/tmp/beryl-worker/hdd0"));
        assert_eq!(hdd0.tier, beryl_types::Tier::Hdd);
        assert_eq!(hdd0.capacity_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(config.store.reserve_space_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.store.selection_policy, "round_robin");
        assert_eq!(config.store.check_interval_ms, 30_000);
        assert_eq!(config.rpc_advertised_endpoint, "http://127.0.0.1:9090");
        assert_eq!(config.metadata.group_name.as_str(), "root");
        assert_eq!(config.metadata.endpoints, vec!["http://127.0.0.1:18080"]);
        assert_eq!(config.metadata.register_timeout_ms, 5_000);
        assert_eq!(config.metadata.register_retry_initial_backoff_ms, 200);
        assert_eq!(config.metadata.register_retry_max_backoff_ms, 5_000);
        assert_eq!(config.net.listeners.len(), 1);
        assert_eq!(config.net.listeners[0].protocol, WorkerNetProtocol::Grpc);
        assert_eq!(config.net.listeners[0].bind, "127.0.0.1:9090");
        assert_eq!(config.net.listeners[0].max_inflight, 100);
    }

    #[test]
    fn observability_loads_from_flat_config_only() {
        let mut flat = ServerConfig::default().as_flat().clone();
        flat.set("worker.rpc.advertised_endpoint", "http://127.0.0.1:9090");
        flat.set("worker.store.dirs.hdd0.path", "/tmp/beryl-worker/hdd0");
        flat.set("worker.store.dirs.hdd0.tier", "HDD");
        flat.set("worker.store.dirs.hdd0.capacity", "10GB");
        flat.set("observe.log.format", "json");
        flat.set("observe.log.output", "stdout");
        flat.set("observe.log.level", "warn");
        flat.set("observe.metrics.prometheus.bind", "127.0.0.1:19091");
        flat.set("observe.metrics.prometheus.path", "/metrics");

        let config = WorkerConfig::from_server_config(&ServerConfig::from_flat(flat)).unwrap();

        assert_eq!(config.observability.log.format, "json");
        assert_eq!(config.observability.log.output, "stdout");
        assert_eq!(config.observability.metrics.prometheus.bind, "127.0.0.1:19091");
    }

    #[test]
    fn loads_current_worker_knobs() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("worker.yaml");
        fs::write(
            &config_path,
            with_test_observe_yaml(
                r#"
worker.identity.path: "/tmp/beryl-worker.identity"
worker.rpc.bind: "127.0.0.1:9091"
worker.rpc.advertised_endpoint: "http://127.0.0.1:19091"
worker.rpc.max_inflight: 8
worker.default_frame_size: 4096
worker.max_frame_size: 8192
worker.stream.idle_timeout_ms: 500
worker.store.dirs.ssd0.path: "/tmp/beryl-worker/ssd0"
worker.store.dirs.ssd0.tier: "SSD"
worker.store.dirs.ssd0.capacity: "12MB"
worker.store.dirs.hdd0.path: "/tmp/beryl-worker/hdd0"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "34MB"
worker.store.reserve_space: "2MB"
worker.store.selection_policy: "round_robin"
worker.store.check_interval_ms: 2500
worker.metadata.group.name: "analytics"
worker.metadata.endpoints: "http://127.0.0.1:18080,http://127.0.0.1:18081"
worker.metadata.register_timeout_ms: 2500
worker.metadata.register_retry_initial_backoff_ms: 25
worker.metadata.register_retry_max_backoff_ms: 250
"#,
            ),
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.identity_path, PathBuf::from("/tmp/beryl-worker.identity"));
        assert_eq!(config.rpc_bind, "127.0.0.1:9091");
        assert_eq!(config.rpc_max_inflight, 8);
        assert_eq!(config.default_frame_size, 4096);
        assert_eq!(config.max_frame_size, 8192);
        assert_eq!(config.stream_idle_timeout_ms, 500);
        let ssd0 = config.store.dirs.get("ssd0").unwrap();
        let hdd0 = config.store.dirs.get("hdd0").unwrap();
        assert_eq!(config.store.dirs.len(), 2);
        assert_eq!(ssd0.path, PathBuf::from("/tmp/beryl-worker/ssd0"));
        assert_eq!(ssd0.tier, beryl_types::Tier::Ssd);
        assert_eq!(ssd0.capacity_bytes, 12 * 1024 * 1024);
        assert_eq!(hdd0.path, PathBuf::from("/tmp/beryl-worker/hdd0"));
        assert_eq!(hdd0.tier, beryl_types::Tier::Hdd);
        assert_eq!(hdd0.capacity_bytes, 34 * 1024 * 1024);
        assert_eq!(config.store.reserve_space_bytes, 2 * 1024 * 1024);
        assert_eq!(config.store.selection_policy, "round_robin");
        assert_eq!(config.store.check_interval_ms, 2_500);
        assert_eq!(config.rpc_advertised_endpoint, "http://127.0.0.1:19091");
        assert_eq!(config.metadata.group_name.as_str(), "analytics");
        assert_eq!(
            config.metadata.endpoints,
            vec!["http://127.0.0.1:18080", "http://127.0.0.1:18081"]
        );
        assert_eq!(config.metadata.register_timeout_ms, 2_500);
        assert_eq!(config.metadata.register_retry_initial_backoff_ms, 25);
        assert_eq!(config.metadata.register_retry_max_backoff_ms, 250);
        assert_eq!(config.net.listeners[0].bind, "127.0.0.1:9091");
        assert_eq!(config.net.listeners[0].max_inflight, 8);
    }

    #[test]
    fn rejects_empty_worker_net_listeners() {
        let mut config = test_worker_config();
        config.net.listeners.clear();

        let error = config.validate().unwrap_err();

        assert!(error.message.contains("net.listeners"));
    }

    #[test]
    fn rejects_invalid_frame_size_order() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("worker.yaml");
        fs::write(
            &config_path,
            with_test_observe_yaml(
                r#"
worker.rpc.advertised_endpoint: "http://127.0.0.1:9090"
worker.store.dirs.hdd0.path: "/tmp/beryl-worker/hdd0"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
worker.default_frame_size: 8192
worker.max_frame_size: 4096
worker.metadata.endpoints: "http://127.0.0.1:18080"
"#,
            ),
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("must be <="));
    }

    #[test]
    fn rejects_removed_window_bytes_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("worker.yaml");
        fs::write(
            &config_path,
            with_test_observe_yaml(
                r#"
worker.rpc.advertised_endpoint: "http://127.0.0.1:9090"
worker.window_bytes: 8192
worker.metadata.endpoints: "http://127.0.0.1:18080"
"#,
            ),
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.window_bytes"));
        assert!(error.message.contains("no longer supported"));
    }

    #[test]
    fn rejects_wrong_type_current_worker_knobs() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("worker.yaml");
        fs::write(
            &config_path,
            with_test_observe_yaml(
                r#"
worker.rpc.advertised_endpoint: "http://127.0.0.1:9090"
worker.rpc.max_inflight: false
worker.metadata.endpoints: "http://127.0.0.1:18080"
"#,
            ),
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.rpc.max_inflight"));
    }

    #[test]
    fn rejects_invalid_store_configurations() {
        let cases = [
            ("missing dirs", "", "worker.store.dirs"),
            ("empty dirs", "worker.store.dirs: []", "worker.store.dirs"),
            (
                "missing path",
                r#"worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB""#,
                "path",
            ),
            (
                "missing tier",
                r#"worker.store.dirs.hdd0.path: "/tmp/a"
worker.store.dirs.hdd0.capacity: "10GB""#,
                "tier",
            ),
            (
                "missing capacity",
                r#"worker.store.dirs.hdd0.path: "/tmp/a"
worker.store.dirs.hdd0.tier: "HDD""#,
                "capacity",
            ),
            (
                "old id field",
                r#"worker.store.dirs.hdd0.id: "old"
worker.store.dirs.hdd0.path: "/tmp/a"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB""#,
                "id",
            ),
            (
                "zero capacity",
                r#"worker.store.dirs.hdd0.path: "/tmp/a"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "0""#,
                "capacity",
            ),
            (
                "bad tier",
                r#"worker.store.dirs.hdd0.path: "/tmp/a"
worker.store.dirs.hdd0.tier: "TAPE"
worker.store.dirs.hdd0.capacity: "10GB""#,
                "tier",
            ),
            (
                "duplicate path",
                r#"worker.store.dirs.hdd0.path: "/tmp/a"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
worker.store.dirs.hdd1.path: "/tmp/a"
worker.store.dirs.hdd1.tier: "HDD"
worker.store.dirs.hdd1.capacity: "10GB""#,
                "duplicate path",
            ),
            (
                "empty id",
                r#"worker.store.dirs..path: "/tmp/a"
worker.store.dirs..tier: "HDD"
worker.store.dirs..capacity: "10GB""#,
                "invalid store dir id",
            ),
            (
                "unsupported selection policy",
                r#"worker.store.dirs.hdd0.path: "/tmp/a"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
worker.store.selection_policy: "balanced"
worker.store.check_interval_ms: 30000"#,
                "worker.store.selection_policy",
            ),
            (
                "zero check interval",
                r#"worker.store.dirs.hdd0.path: "/tmp/a"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
worker.store.selection_policy: "round_robin"
worker.store.check_interval_ms: 0"#,
                "worker.store.check_interval_ms",
            ),
        ];

        for (case, store_config, expected) in cases {
            let error = load_test_config(format!(
                r#"
worker.rpc.advertised_endpoint: "http://127.0.0.1:9090"
{store_config}
worker.metadata.endpoints: "http://127.0.0.1:18080"
"#
            ))
            .expect_err("invalid store config must fail");

            assert!(
                error.message.contains(expected),
                "{case} expected {expected:?}, got {}",
                error.message
            );
        }
    }

    #[test]
    fn uses_default_worker_metadata_endpoints_when_absent() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("worker.yaml");
        fs::write(
            &config_path,
            with_test_observe_yaml(
                r#"
worker.rpc.bind: "127.0.0.1:9090"
worker.rpc.advertised_endpoint: "http://127.0.0.1:9090"
worker.store.dirs.hdd0.path: "/tmp/beryl-worker/hdd0"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
"#,
            ),
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.metadata.endpoints, WorkerRegistrationConfig::default().endpoints);
    }

    #[test]
    fn rejects_invalid_worker_metadata_endpoints() {
        for endpoints in [" , ", "127.0.0.1:18080"] {
            let error = load_test_config(format!(
                r#"
worker.rpc.advertised_endpoint: "http://127.0.0.1:9090"
worker.store.dirs.hdd0.path: "/tmp/beryl-worker/hdd0"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
worker.metadata.endpoints: "{endpoints}"
"#
            ))
            .expect_err("empty or non-URL metadata endpoint must fail");

            assert!(error.message.contains("worker.metadata.endpoints"));
        }
    }

    #[test]
    fn rejects_missing_or_wildcard_worker_rpc_advertised_endpoint() {
        for (case, advertised_config, expected) in [
            ("missing", "", "worker.rpc.advertised_endpoint"),
            (
                "IPv4 wildcard",
                r#"worker.rpc.advertised_endpoint: "http://0.0.0.0:9090""#,
                "wildcard",
            ),
            (
                "IPv6 wildcard",
                r#"worker.rpc.advertised_endpoint: "http://[::]:9090""#,
                "wildcard",
            ),
        ] {
            let error = load_test_config(format!(
                r#"
worker.rpc.bind: "0.0.0.0:9090"
{advertised_config}
worker.store.dirs.hdd0.path: "/tmp/beryl-worker/hdd0"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
worker.metadata.endpoints: "http://127.0.0.1:18080"
"#
            ))
            .expect_err("missing or wildcard advertised endpoint must fail");

            assert!(
                error.message.contains("worker.rpc.advertised_endpoint"),
                "case {case}: {}",
                error.message
            );
            assert!(error.message.contains(expected), "case {case}: {}", error.message);
        }
    }

    #[test]
    fn rejects_invalid_worker_metadata_register_timing() {
        for (timing_config, expected) in [
            (
                "worker.metadata.register_timeout_ms: 0",
                "worker.metadata.register_timeout_ms",
            ),
            (
                "worker.metadata.register_retry_initial_backoff_ms: 0",
                "worker.metadata.register_retry_initial_backoff_ms",
            ),
            (
                "worker.metadata.register_retry_initial_backoff_ms: 500\n\
                 worker.metadata.register_retry_max_backoff_ms: 100",
                "worker.metadata.register_retry_max_backoff_ms",
            ),
        ] {
            let error = load_test_config(format!(
                r#"
worker.rpc.advertised_endpoint: "http://127.0.0.1:9090"
worker.store.dirs.hdd0.path: "/tmp/beryl-worker/hdd0"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
worker.metadata.endpoints: "http://127.0.0.1:18080"
{timing_config}
"#
            ))
            .expect_err("invalid registration timing must fail");

            assert!(error.message.contains(expected), "{}", error.message);
        }
    }
}
