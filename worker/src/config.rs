// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker configuration for the current data service skeleton.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use common::config::CoreConfig;
use common::error::{CommonError, CommonErrorCode};
use tonic::transport::Endpoint;
use tracing::info;
use types::{GroupName, Tier};

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
    /// HTTP/admin/metrics bind address.
    pub http_bind: String,
    /// Routable gRPC data endpoint registered with metadata.
    pub rpc_advertised_endpoint: String,
    /// Maximum concurrent RPC requests per gRPC connection.
    pub rpc_max_inflight: usize,
    /// Transport frame payload size negotiated at stream open.
    /// This controls network batching and does not define StorageChunk size.
    pub default_frame_size: u32,
    /// Upper bound for negotiated transport frame payload size.
    pub max_frame_size: u32,
    /// Per-stream application-level in-flight byte window.
    /// This is independent from protocol-native flow control.
    pub window_bytes: u32,
    /// Idle timeout for runtime stream state.
    pub stream_idle_timeout_ms: u64,
    /// Worker-local block store configuration.
    pub store: WorkerStoreConfig,
    /// Worker-owned service-specific network configuration.
    pub net: WorkerNetConfig,
    /// Worker metadata registration configuration.
    pub metadata: WorkerRegistrationConfig,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            cluster_id: "local-vecton".to_string(),
            identity_path: PathBuf::from("data/worker/worker.identity"),
            rpc_bind: "0.0.0.0:9090".to_string(),
            http_bind: "0.0.0.0:19091".to_string(),
            rpc_advertised_endpoint: "http://127.0.0.1:9090".to_string(),
            rpc_max_inflight: 100,
            default_frame_size: 1024 * 1024,
            max_frame_size: 4 * 1024 * 1024,
            window_bytes: 8 * 1024 * 1024,
            stream_idle_timeout_ms: 60_000,
            store: WorkerStoreConfig::default(),
            net: WorkerNetConfig::grpc_from_rpc("0.0.0.0:9090".to_string(), 100, 4 * 1024 * 1024),
            metadata: WorkerRegistrationConfig::default(),
        }
    }
}

impl WorkerConfig {
    /// Load worker configuration from a core-site YAML file.
    pub fn load<P: AsRef<Path>>(config_path: P) -> Result<Self, CommonError> {
        let core_config = CoreConfig::load(config_path)?;
        Self::from_core_config(&core_config)
    }

    /// Create worker configuration from the repository-wide config shape.
    pub fn from_core_config(core_config: &CoreConfig) -> Result<Self, CommonError> {
        let worker_sub = core_config.as_flat().sub("worker");
        let flat = core_config.as_flat();
        let defaults = Self::default();
        let metadata_defaults = WorkerRegistrationConfig::default();

        let cluster_id = Self::root_str_or(flat, "vecton.cluster.id", &defaults.cluster_id)?;
        reject_removed_keys(&worker_sub)?;
        let identity_path = Self::path_or(&worker_sub, "identity.path", defaults.identity_path.clone())?;
        let rpc_bind = Self::str_or(&worker_sub, "rpc.bind", &defaults.rpc_bind, "worker.rpc.bind")?;
        let http_bind = Self::str_or(&worker_sub, "http.bind", &defaults.http_bind, "worker.http.bind")?;
        let rpc_advertised_endpoint = worker_sub
            .get_str("rpc.advertised_endpoint")
            .ok_or_else(|| invalid_config("worker.rpc.advertised_endpoint", "must be present and be a string"))?;
        if worker_sub.contains_key("rpc.advertised_endpoint") && rpc_advertised_endpoint.trim().is_empty() {
            return Err(invalid_config("worker.rpc.advertised_endpoint", "must not be empty"));
        }
        let rpc_max_inflight = Self::usize_or(
            &worker_sub,
            "rpc.max_inflight",
            defaults.rpc_max_inflight,
            "worker.rpc.max_inflight",
        )?;
        let default_frame_size = Self::bytes_u32(
            &worker_sub,
            "default_frame_size",
            defaults.default_frame_size,
            "worker.default_frame_size",
        )?;
        let max_frame_size = Self::bytes_u32(
            &worker_sub,
            "max_frame_size",
            defaults.max_frame_size,
            "worker.max_frame_size",
        )?;
        let window_bytes = Self::bytes_u32(
            &worker_sub,
            "window_bytes",
            defaults.window_bytes,
            "worker.window_bytes",
        )?;
        let stream_idle_timeout_ms = Self::usize_or(
            &worker_sub,
            "stream.idle_timeout_ms",
            defaults.stream_idle_timeout_ms as usize,
            "worker.stream.idle_timeout_ms",
        )? as u64;
        let store = parse_store_config(&worker_sub, &defaults.store)?;
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
            http_bind,
            rpc_advertised_endpoint,
            rpc_max_inflight,
            default_frame_size,
            max_frame_size,
            window_bytes,
            stream_idle_timeout_ms,
            store,
            net: WorkerNetConfig::grpc_from_rpc(rpc_bind, rpc_max_inflight, max_frame_size),
            metadata,
        };

        config.validate()?;

        info!(
            identity_path = ?config.identity_path,
            rpc_bind = %config.rpc_bind,
            http_bind = %config.http_bind,
            rpc_advertised_endpoint = %config.rpc_advertised_endpoint,
            rpc_max_inflight = config.rpc_max_inflight,
            default_frame_size = config.default_frame_size,
            max_frame_size = config.max_frame_size,
            window_bytes = config.window_bytes,
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
                CommonErrorCode::InvalidArgument,
                "vecton.cluster.id must not be empty",
            ));
        }

        if self.identity_path.as_os_str().is_empty() {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.identity.path must not be empty",
            ));
        }

        if self.rpc_bind.parse::<std::net::SocketAddr>().is_err() {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("invalid worker.rpc.bind address: {}", self.rpc_bind),
            ));
        }

        if self.http_bind.parse::<std::net::SocketAddr>().is_err() {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("invalid worker.http.bind address: {}", self.http_bind),
            ));
        }

        self.rpc_advertised_endpoint_parts()?;

        if self.rpc_max_inflight == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.rpc.max_inflight must be greater than zero",
            ));
        }

        if self.default_frame_size == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.default_frame_size must be greater than zero",
            ));
        }

        if self.max_frame_size == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.max_frame_size must be greater than zero",
            ));
        }

        if self.default_frame_size > self.max_frame_size {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!(
                    "worker.default_frame_size ({}) must be <= worker.max_frame_size ({})",
                    self.default_frame_size, self.max_frame_size
                ),
            ));
        }

        if self.window_bytes == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.window_bytes must be greater than zero",
            ));
        }

        if self.stream_idle_timeout_ms == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.stream.idle_timeout_ms must be greater than zero",
            ));
        }

        validate_store_config(self)?;

        self.metadata.validate()?;

        if self.net.listeners.is_empty() {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.net.listeners must not be empty",
            ));
        }

        for listener in &self.net.listeners {
            if listener.protocol == WorkerNetProtocol::Grpc && listener.bind.parse::<std::net::SocketAddr>().is_err() {
                return Err(CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    format!("invalid worker gRPC listener bind address: {}", listener.bind),
                ));
            }
            if listener.max_inflight == 0 {
                return Err(CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    "worker.net.listeners.max_inflight must be greater than zero",
                ));
            }
            if listener.max_frame_size == 0 {
                return Err(CommonError::new(
                    CommonErrorCode::InvalidArgument,
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
        flat: &common::config::FlatConfig,
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
        flat: &common::config::FlatConfig,
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

    fn path_or(flat: &common::config::FlatConfig, key: &str, fallback: PathBuf) -> Result<PathBuf, CommonError> {
        if let Some(value) = flat.get_str(key) {
            return Ok(PathBuf::from(value));
        }
        if flat.contains_key(key) {
            return Err(invalid_config("worker.identity.path", "must be a string"));
        }
        Ok(fallback)
    }

    fn usize_or(
        flat: &common::config::FlatConfig,
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
        flat: &common::config::FlatConfig,
        key: &str,
        fallback: u32,
        field_name: &'static str,
    ) -> Result<u32, CommonError> {
        match flat.get_bytes(key) {
            Some(value) => u32::try_from(value).map_err(|_| {
                CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    format!("{field_name} exceeds u32 byte size"),
                )
            }),
            None if flat.contains_key(key) => Err(invalid_config(field_name, "must be a byte size")),
            None => Ok(fallback),
        }
    }

    fn bytes_u64(
        flat: &common::config::FlatConfig,
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

fn reject_removed_keys(flat: &common::config::FlatConfig) -> Result<(), CommonError> {
    if let Some(key) = flat.keys_with_prefix("storage").into_iter().next() {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("worker.{key} is unsupported: use worker.store.*"),
        ));
    }
    for (key, full_key, detail) in [
        (
            "id",
            "worker.id",
            "worker.id is unsupported; worker identity must come from worker.identity.path",
        ),
        (
            "metadata.group_id",
            "worker.metadata.group_id",
            "worker.metadata.group_id is unsupported; use worker.metadata.group.name",
        ),
        (
            "metadata.group.id",
            "worker.metadata.group.id",
            "worker.metadata.group.id is unsupported; use worker.metadata.group.name",
        ),
        (
            "metadata.endpoint",
            "worker.metadata.endpoint",
            "worker.metadata.endpoint is unsupported; use worker.metadata.endpoints",
        ),
        (
            "store.reserve",
            "worker.store.reserve",
            "worker.store.reserve is unsupported; use worker.store.reserve_space",
        ),
        (
            "store.pick",
            "worker.store.pick",
            "worker.store.pick is unsupported; use worker.store.selection_policy",
        ),
        (
            "store.check_ms",
            "worker.store.check_ms",
            "worker.store.check_ms is unsupported; use worker.store.check_interval_ms",
        ),
        (
            "bootstrap.auto_format",
            "worker.bootstrap.auto_format",
            "worker.bootstrap.auto_format is unsupported",
        ),
        ("auto_format", "worker.auto_format", "worker.auto_format is unsupported"),
    ] {
        if flat.contains_key(key) {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("{full_key} is unsupported: {detail}"),
            ));
        }
    }
    Ok(())
}

fn parse_store_config(
    flat: &common::config::FlatConfig,
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

fn parse_store_dirs(flat: &common::config::FlatConfig) -> Result<BTreeMap<String, StoreDirConfig>, CommonError> {
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
                CommonErrorCode::InvalidArgument,
                format!("worker.{key} must use worker.store.dirs.<dir_id>.<field>"),
            )
        })?;
        let (id, field) = rest.split_once('.').ok_or_else(|| {
            CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("worker.{key} must use worker.store.dirs.<dir_id>.<field>"),
            )
        })?;
        if id.is_empty() || id.trim() != id {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("worker.{key} has an invalid store dir id"),
            ));
        }
        if field.contains('.') {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("worker.{key} has an unsupported nested store dir field"),
            ));
        }
        match field {
            "path" | "tier" | "capacity" => {}
            "id" => {
                return Err(CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    format!("worker.{key} is unsupported; dir id must come from the key segment"),
                ));
            }
            "cap" => {
                return Err(CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    format!("worker.{key} is unsupported; use worker.store.dirs.{id}.capacity"),
                ));
            }
            _ => {
                return Err(CommonError::new(
                    CommonErrorCode::InvalidArgument,
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
                CommonErrorCode::InvalidArgument,
                format!("worker.store.dirs.{id}.tier {err}"),
            )
        })?;
        let capacity_bytes = required_store_bytes(flat, &capacity_key, format!("worker.{capacity_key}"))?;
        if capacity_bytes == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
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
    flat: &common::config::FlatConfig,
    field: &str,
    display_key: String,
) -> Result<String, CommonError> {
    let value = flat.get_str(field).ok_or_else(|| {
        CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("{display_key} must be present"),
        )
    })?;
    if value.trim().is_empty() {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("{display_key} must not be empty"),
        ));
    }
    Ok(value)
}

fn required_store_bytes(
    flat: &common::config::FlatConfig,
    field: &str,
    display_key: String,
) -> Result<u64, CommonError> {
    let value = flat.get_bytes(field).ok_or_else(|| {
        CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("{display_key} must be a byte size"),
        )
    })?;
    u64::try_from(value).map_err(|_| {
        CommonError::new(
            CommonErrorCode::InvalidArgument,
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
                CommonErrorCode::InvalidArgument,
                format!("worker.store.dirs.{id}.path must not be empty"),
            ));
        }
        if !paths.insert(dir.path.clone()) {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("worker.store.dirs duplicate path: {}", dir.path.display()),
            ));
        }
        if dir.capacity_bytes == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("worker.store.dirs.{id}.capacity must be greater than zero"),
            ));
        }
    }
    if config.store.selection_policy != "round_robin" {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
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
                CommonErrorCode::InvalidArgument,
                "worker.metadata.endpoints must not be empty",
            ));
        }

        for endpoint in &self.endpoints {
            if endpoint.is_empty() {
                return Err(CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    "worker.metadata.endpoints entries must not be empty",
                ));
            }
            if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
                return Err(CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    "worker.metadata.endpoints entries must include http:// or https:// scheme",
                ));
            }
            Endpoint::from_shared(endpoint.clone()).map_err(|err| {
                CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    format!("worker.metadata.endpoints entry must be a valid tonic endpoint URI: {err}"),
                )
            })?;
        }

        if self.register_timeout_ms == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.metadata.register_timeout_ms must be greater than zero",
            ));
        }

        if self.register_retry_initial_backoff_ms == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.metadata.register_retry_initial_backoff_ms must be greater than zero",
            ));
        }

        if self.register_retry_max_backoff_ms == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.metadata.register_retry_max_backoff_ms must be greater than zero",
            ));
        }

        if self.register_retry_max_backoff_ms < self.register_retry_initial_backoff_ms {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
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
    CommonError::new(CommonErrorCode::InvalidArgument, format!("{key} {detail}"))
}

fn parse_group_name(key: &'static str, raw: String) -> Result<GroupName, CommonError> {
    GroupName::parse(raw).map_err(|err| CommonError::new(CommonErrorCode::InvalidArgument, format!("{key} {err}")))
}

fn metadata_endpoints(
    worker_sub: &common::config::FlatConfig,
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
            CommonErrorCode::InvalidArgument,
            "worker.rpc.advertised_endpoint must not be empty",
        ));
    }

    if !(value.starts_with("http://") || value.starts_with("https://")) {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            "worker.rpc.advertised_endpoint must include http:// or https:// scheme",
        ));
    }

    let endpoint = Endpoint::from_shared(value.to_string()).map_err(|err| {
        CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("worker.rpc.advertised_endpoint must be a valid tonic endpoint URI: {err}"),
        )
    })?;
    let uri = endpoint.uri();
    let raw_host = uri.host().ok_or_else(|| {
        CommonError::new(
            CommonErrorCode::InvalidArgument,
            "worker.rpc.advertised_endpoint must include a host",
        )
    })?;
    let host = raw_host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(raw_host);
    if host.is_empty() {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            "worker.rpc.advertised_endpoint host must not be empty",
        ));
    }
    if host.parse::<IpAddr>().is_ok_and(|ip| ip.is_unspecified()) {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            "worker.rpc.advertised_endpoint must not use a wildcard host",
        ));
    }
    let port = uri.port_u16().ok_or_else(|| {
        CommonError::new(
            CommonErrorCode::InvalidArgument,
            "worker.rpc.advertised_endpoint must include a port",
        )
    })?;

    Ok((host.to_string(), u32::from(port)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::config::PeerProtocolSelectionPolicy;
    use crate::net::endpoint::WorkerEndpointRole;
    use crate::net::protocol::WorkerNetProtocol;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn loads_default_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    bind: "127.0.0.1:9090"
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
    reserve_space: "1GB"
    selection_policy: "round_robin"
    check_interval_ms: 30000
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.rpc_bind, "127.0.0.1:9090");
        assert_eq!(config.identity_path, PathBuf::from("data/worker/worker.identity"));
        assert_eq!(config.rpc_max_inflight, 100);
        assert_eq!(config.default_frame_size, 1024 * 1024);
        assert_eq!(config.max_frame_size, 4 * 1024 * 1024);
        assert_eq!(config.window_bytes, 8 * 1024 * 1024);
        assert_eq!(config.stream_idle_timeout_ms, 60_000);
        let hdd0 = config.store.dirs.get("hdd0").unwrap();
        assert_eq!(config.store.dirs.len(), 1);
        assert_eq!(hdd0.path, PathBuf::from("/tmp/vecton-worker/hdd0"));
        assert_eq!(hdd0.tier, types::Tier::Hdd);
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
        assert_eq!(config.net.listeners[0].role, vec![WorkerEndpointRole::ClientData]);
        assert_eq!(config.net.listeners[0].max_inflight, 100);
        assert_eq!(config.net.peer.enabled_protocols, vec![WorkerNetProtocol::Grpc]);
        assert_eq!(
            config.net.peer.selection_policy,
            PeerProtocolSelectionPolicy::PreferGrpc
        );
    }

    #[test]
    fn loads_current_worker_knobs() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  identity:
    path: "/tmp/vecton-worker.identity"
  rpc:
    bind: "127.0.0.1:9091"
    advertised_endpoint: "http://127.0.0.1:19091"
    max_inflight: 8
  default_frame_size: 4096
  max_frame_size: 8192
  window_bytes: 16384
  stream:
    idle_timeout_ms: 500
  store:
    dirs:
      ssd0:
        path: "/tmp/vecton-worker/ssd0"
        tier: "SSD"
        capacity: "12MB"
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "34MB"
    reserve_space: "2MB"
    selection_policy: "round_robin"
    check_interval_ms: 2500
  metadata:
    group:
      name: "analytics"
    endpoints: "http://127.0.0.1:18080,http://127.0.0.1:18081"
    register_timeout_ms: 2500
    register_retry_initial_backoff_ms: 25
    register_retry_max_backoff_ms: 250
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.identity_path, PathBuf::from("/tmp/vecton-worker.identity"));
        assert_eq!(config.rpc_bind, "127.0.0.1:9091");
        assert_eq!(config.rpc_max_inflight, 8);
        assert_eq!(config.default_frame_size, 4096);
        assert_eq!(config.max_frame_size, 8192);
        assert_eq!(config.window_bytes, 16_384);
        assert_eq!(config.stream_idle_timeout_ms, 500);
        let ssd0 = config.store.dirs.get("ssd0").unwrap();
        let hdd0 = config.store.dirs.get("hdd0").unwrap();
        assert_eq!(config.store.dirs.len(), 2);
        assert_eq!(ssd0.path, PathBuf::from("/tmp/vecton-worker/ssd0"));
        assert_eq!(ssd0.tier, types::Tier::Ssd);
        assert_eq!(ssd0.capacity_bytes, 12 * 1024 * 1024);
        assert_eq!(hdd0.path, PathBuf::from("/tmp/vecton-worker/hdd0"));
        assert_eq!(hdd0.tier, types::Tier::Hdd);
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
    fn loads_id_keyed_store_dirs_from_dotted_keys() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker.rpc.advertised_endpoint: "http://127.0.0.1:9090"
worker.store.dirs.hdd0.path: "/tmp/vecton-worker/hdd0"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
worker.store.reserve_space: "1GB"
worker.store.selection_policy: "round_robin"
worker.store.check_interval_ms: 30000
worker.metadata.endpoints: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        let hdd0 = config.store.dirs.get("hdd0").unwrap();
        assert_eq!(config.store.dirs.len(), 1);
        assert_eq!(hdd0.path, PathBuf::from("/tmp/vecton-worker/hdd0"));
        assert_eq!(hdd0.tier, types::Tier::Hdd);
        assert_eq!(hdd0.capacity_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(config.store.reserve_space_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.store.selection_policy, "round_robin");
        assert_eq!(config.store.check_interval_ms, 30_000);
    }

    #[test]
    fn removed_worker_identity_and_group_keys_are_rejected() {
        for removed_key in [
            "id: 91",
            "metadata:\n    group_id: 7\n    endpoints: \"http://127.0.0.1:18080\"",
            "metadata:\n    group:\n      id: 7\n    endpoints: \"http://127.0.0.1:18080\"",
            "bootstrap:\n    auto_format: true\n  metadata:\n    endpoints: \"http://127.0.0.1:18080\"",
            "auto_format: true\n  metadata:\n    endpoints: \"http://127.0.0.1:18080\"",
        ] {
            let temp_dir = TempDir::new().unwrap();
            let config_path = temp_dir.path().join("core-site.yaml");
            fs::write(
                &config_path,
                format!(
                    r#"
worker:
  {removed_key}
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
"#
                ),
            )
            .unwrap();

            let err = WorkerConfig::load(&config_path).expect_err("removed worker key must fail");

            assert!(err.message.contains("unsupported"));
        }
    }

    #[test]
    fn ignores_removed_worker_transport_frame_size_keys() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
  transport:
    default_frame_size: 8388608
    max_frame_size: 16777216
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();
        let defaults = WorkerConfig::default();

        assert_eq!(config.default_frame_size, defaults.default_frame_size);
        assert_eq!(config.max_frame_size, defaults.max_frame_size);
    }

    #[test]
    fn rejects_empty_worker_net_listeners() {
        let mut config = WorkerConfig::default();
        config.net.listeners.clear();

        let error = config.validate().unwrap_err();

        assert!(error.message.contains("net.listeners"));
    }

    #[test]
    fn rejects_invalid_frame_size_order() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
  default_frame_size: 8192
  max_frame_size: 4096
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("must be <="));
    }

    #[test]
    fn rejects_wrong_type_current_worker_knobs() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
    max_inflight: false
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.rpc.max_inflight"));
    }

    #[test]
    fn worker_storage_root_is_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  storage:
    root: "/data/old"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let err = WorkerConfig::load(&config_path).expect_err("old storage root must fail");

        assert!(err.message.contains("worker.storage.root"));
        assert!(err.message.contains("unsupported"));
    }

    #[test]
    fn rejects_missing_or_empty_store_dirs() {
        for store_config in ["", "store:\n    dirs: []\n"] {
            let temp_dir = TempDir::new().unwrap();
            let config_path = temp_dir.path().join("core-site.yaml");
            fs::write(
                &config_path,
                format!(
                    r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  {store_config}
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#
                ),
            )
            .unwrap();

            let err = WorkerConfig::load(&config_path).expect_err("missing or empty store dirs must fail");

            assert!(err.message.contains("worker.store.dirs"), "{}", err.message);
        }
    }

    #[test]
    fn rejects_old_list_based_store_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      - id: "hdd0"
        path: "/tmp/a"
        tier: "HDD"
        cap: "10GB"
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let err = WorkerConfig::load(&config_path).expect_err("old list-based store dirs must fail");

        assert!(err.message.contains("worker.store.dirs"), "{}", err.message);
    }

    #[test]
    fn rejects_invalid_store_dir_entries() {
        for (name, dirs_config, expected) in [
            (
                "missing path",
                r#"hdd0:
        tier: "HDD"
        capacity: "10GB""#,
                "path",
            ),
            (
                "missing tier",
                r#"hdd0:
        path: "/tmp/a"
        capacity: "10GB""#,
                "tier",
            ),
            (
                "missing capacity",
                r#"hdd0:
        path: "/tmp/a"
        tier: "HDD""#,
                "capacity",
            ),
            (
                "old id field",
                r#"hdd0:
        id: "old"
        path: "/tmp/a"
        tier: "HDD"
        capacity: "10GB""#,
                "id",
            ),
            (
                "old cap field",
                r#"hdd0:
        path: "/tmp/a"
        tier: "HDD"
        cap: "10GB""#,
                "cap",
            ),
            (
                "zero capacity",
                r#"hdd0:
        path: "/tmp/a"
        tier: "HDD"
        capacity: "0""#,
                "capacity",
            ),
            (
                "bad tier",
                r#"hdd0:
        path: "/tmp/a"
        tier: "TAPE"
        capacity: "10GB""#,
                "tier",
            ),
        ] {
            let temp_dir = TempDir::new().unwrap();
            let config_path = temp_dir.path().join("core-site.yaml");
            fs::write(
                &config_path,
                format!(
                    r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      {dirs_config}
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#
                ),
            )
            .unwrap();

            let err = WorkerConfig::load(&config_path).unwrap_err();

            assert!(
                err.message.contains(expected),
                "{name} expected {expected:?}, got {}",
                err.message
            );
        }
    }

    #[test]
    fn rejects_duplicate_store_dir_path() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/a"
        tier: "HDD"
        capacity: "10GB"
      hdd1:
        path: "/tmp/a"
        tier: "HDD"
        capacity: "10GB"
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let err = WorkerConfig::load(&config_path).unwrap_err();

        assert!(err.message.contains("duplicate path"), "{}", err.message);
    }

    #[test]
    fn rejects_empty_store_dir_id_segment() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker.rpc.advertised_endpoint: "http://127.0.0.1:9090"
worker.store.dirs..path: "/tmp/a"
worker.store.dirs..tier: "HDD"
worker.store.dirs..capacity: "10GB"
worker.metadata.endpoints: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let err = WorkerConfig::load(&config_path).unwrap_err();

        assert!(err.message.contains("invalid store dir id"), "{}", err.message);
    }

    #[test]
    fn rejects_unsupported_store_selection_policy_or_zero_check_interval() {
        for (store_tail, expected) in [
            (
                "selection_policy: \"balanced\"\n    check_interval_ms: 30000",
                "worker.store.selection_policy",
            ),
            (
                "selection_policy: \"round_robin\"\n    check_interval_ms: 0",
                "worker.store.check_interval_ms",
            ),
        ] {
            let temp_dir = TempDir::new().unwrap();
            let config_path = temp_dir.path().join("core-site.yaml");
            fs::write(
                &config_path,
                format!(
                    r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/a"
        tier: "HDD"
        capacity: "10GB"
    {store_tail}
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#
                ),
            )
            .unwrap();

            let err = WorkerConfig::load(&config_path).unwrap_err();

            assert!(err.message.contains(expected), "{}", err.message);
        }
    }

    #[test]
    fn rejects_removed_store_keys() {
        for (store_tail, expected) in [
            ("reserve: \"1GB\"", "worker.store.reserve"),
            ("pick: \"round_robin\"", "worker.store.pick"),
            ("check_ms: 30000", "worker.store.check_ms"),
        ] {
            let temp_dir = TempDir::new().unwrap();
            let config_path = temp_dir.path().join("core-site.yaml");
            fs::write(
                &config_path,
                format!(
                    r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/a"
        tier: "HDD"
        capacity: "10GB"
    {store_tail}
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#
                ),
            )
            .unwrap();

            let err = WorkerConfig::load(&config_path).unwrap_err();

            assert!(err.message.contains(expected), "{}", err.message);
            assert!(err.message.contains("unsupported"), "{}", err.message);
        }
    }

    #[test]
    fn uses_default_worker_metadata_endpoints_when_absent() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    bind: "127.0.0.1:9090"
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.metadata.endpoints, WorkerRegistrationConfig::default().endpoints);
    }

    #[test]
    fn worker_metadata_endpoint_key_is_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  metadata:
    endpoint: "http://127.0.0.1:19080"
"#,
        )
        .unwrap();

        let err = WorkerConfig::load(&config_path).unwrap_err();

        assert!(err.message.contains("worker.metadata.endpoint"));
        assert!(err.message.contains("unsupported"));
    }

    #[test]
    fn rejects_empty_worker_metadata_endpoints() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
  metadata:
    endpoints: " , "
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.metadata.endpoints"));
    }

    #[test]
    fn rejects_invalid_explicit_worker_id() {
        for worker_id in ["0", "not-a-number", ""] {
            let temp_dir = TempDir::new().unwrap();
            let config_path = temp_dir.path().join("core-site.yaml");
            fs::write(
                &config_path,
                format!(
                    r#"
worker:
  id: "{worker_id}"
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#
                ),
            )
            .unwrap();

            let error = WorkerConfig::load(&config_path).unwrap_err();

            assert!(error.message.contains("worker.id"));
        }
    }

    #[test]
    fn rejects_invalid_worker_metadata_endpoints() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
  metadata:
    endpoints: "127.0.0.1:18080"
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.metadata.endpoints"));
    }

    #[test]
    fn rejects_missing_worker_rpc_advertised_endpoint() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    bind: "0.0.0.0:9090"
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.rpc.advertised_endpoint"));
    }

    #[test]
    fn rejects_wildcard_worker_rpc_advertised_endpoint() {
        for advertised_endpoint in ["http://0.0.0.0:9090", "http://[::]:9090"] {
            let temp_dir = TempDir::new().unwrap();
            let config_path = temp_dir.path().join("core-site.yaml");
            fs::write(
                &config_path,
                format!(
                    r#"
worker:
  rpc:
    bind: "0.0.0.0:9090"
    advertised_endpoint: "{advertised_endpoint}"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
  metadata:
    endpoints: "http://127.0.0.1:18080"
"#
                ),
            )
            .unwrap();

            let error = WorkerConfig::load(&config_path).unwrap_err();

            assert!(error.message.contains("worker.rpc.advertised_endpoint"));
            assert!(error.message.contains("wildcard"));
        }
    }

    #[test]
    fn rejects_invalid_worker_metadata_register_timing() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
  metadata:
    endpoints: "http://127.0.0.1:18080"
    register_timeout_ms: 0
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.metadata.register_timeout_ms"));

        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
  metadata:
    endpoints: "http://127.0.0.1:18080"
    register_retry_initial_backoff_ms: 0
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error
            .message
            .contains("worker.metadata.register_retry_initial_backoff_ms"));

        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  store:
    dirs:
      hdd0:
        path: "/tmp/vecton-worker/hdd0"
        tier: "HDD"
        capacity: "10GB"
  metadata:
    endpoints: "http://127.0.0.1:18080"
    register_retry_initial_backoff_ms: 500
    register_retry_max_backoff_ms: 100
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.metadata.register_retry_max_backoff_ms"));
    }
}
