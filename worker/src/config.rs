// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker configuration for the current data service skeleton.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use common::config::CoreConfig;
use common::error::{CommonError, CommonErrorCode};
use tonic::transport::Endpoint;
use tracing::info;
use types::ids::{ShardGroupId, WorkerId};

use crate::net::config::WorkerNetConfig;
use crate::net::protocol::WorkerNetProtocol;

/// Worker metadata registration configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRegistrationConfig {
    /// Metadata group registered by this worker process.
    pub group_id: ShardGroupId,
    /// MetadataWorkerService endpoint URI used during worker startup registration.
    pub endpoint: String,
    /// Per-attempt registration timeout.
    pub register_timeout_ms: u64,
    /// Initial retry backoff after retryable registration failures.
    pub register_retry_initial_backoff_ms: u64,
    /// Maximum retry backoff after retryable registration failures.
    pub register_retry_max_backoff_ms: u64,
}

impl Default for WorkerRegistrationConfig {
    fn default() -> Self {
        Self {
            group_id: ShardGroupId::new(1),
            endpoint: "http://127.0.0.1:18080".to_string(),
            register_timeout_ms: 5_000,
            register_retry_initial_backoff_ms: 200,
            register_retry_max_backoff_ms: 5_000,
        }
    }
}

/// Worker configuration.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// Optional explicit stable worker identity.
    pub worker_id: Option<WorkerId>,
    /// Local persisted identity file used when worker_id is not explicit.
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
    /// Per-stream application-level in-flight byte window.
    /// This is independent from protocol-native flow control.
    pub window_bytes: u32,
    /// Worker-local StorageChunk size.
    /// This is the IO/checksum/valid-bitmap granularity, not a transport frame size.
    pub chunk_size: u32,
    /// Idle timeout for runtime stream state.
    pub stream_idle_timeout_ms: u64,
    /// Root for worker-local block storage.
    pub storage_root: PathBuf,
    /// Worker-owned service-specific network configuration.
    pub net: WorkerNetConfig,
    /// Worker metadata registration configuration.
    pub metadata: WorkerRegistrationConfig,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: None,
            identity_path: PathBuf::from("./data/worker.identity"),
            rpc_bind: "0.0.0.0:9090".to_string(),
            rpc_advertised_endpoint: "http://127.0.0.1:9090".to_string(),
            rpc_max_inflight: 100,
            default_frame_size: 1024 * 1024,
            max_frame_size: 4 * 1024 * 1024,
            window_bytes: 8 * 1024 * 1024,
            chunk_size: 1024 * 1024,
            stream_idle_timeout_ms: 60_000,
            storage_root: PathBuf::from("./data"),
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
        let defaults = Self::default();
        let metadata_defaults = WorkerRegistrationConfig::default();

        let worker_id = Self::optional_worker_id(&worker_sub)?;
        let identity_path = worker_sub
            .get_str("identity.path")
            .map(PathBuf::from)
            .or_else(|| (!worker_sub.contains_key("identity.path")).then(|| defaults.identity_path.clone()))
            .ok_or_else(|| invalid_config("worker.identity.path", "must be a string"))?;
        let rpc_bind = Self::str_or(&worker_sub, "rpc.bind", &defaults.rpc_bind, "worker.rpc.bind")?;
        let rpc_advertised_endpoint = worker_sub
            .get_str("rpc.advertised_endpoint")
            .ok_or_else(|| invalid_config("worker.rpc.advertised_endpoint", "must be present and be a string"))?;
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
        let chunk_size = Self::bytes_u32(&worker_sub, "chunk_size", defaults.chunk_size, "worker.chunk_size")?;
        let stream_idle_timeout_ms = Self::usize_or(
            &worker_sub,
            "stream.idle_timeout_ms",
            defaults.stream_idle_timeout_ms as usize,
            "worker.stream.idle_timeout_ms",
        )? as u64;
        let storage_root = worker_sub
            .get_str("storage.root")
            .map(PathBuf::from)
            .or_else(|| (!worker_sub.contains_key("storage.root")).then(|| defaults.storage_root.clone()))
            .ok_or_else(|| invalid_config("worker.storage.root", "must be a string"))?;
        let endpoint = worker_sub
            .get_str("metadata.endpoint")
            .ok_or_else(|| invalid_config("worker.metadata.endpoint", "must be present and be a string"))?;
        let group_id = ShardGroupId::new(Self::usize_or(
            &worker_sub,
            "metadata.group_id",
            metadata_defaults.group_id.as_raw() as usize,
            "worker.metadata.group_id",
        )? as u64);
        let metadata = WorkerRegistrationConfig {
            group_id,
            endpoint,
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
            worker_id,
            identity_path,
            rpc_bind: rpc_bind.clone(),
            rpc_advertised_endpoint,
            rpc_max_inflight,
            default_frame_size,
            max_frame_size,
            window_bytes,
            chunk_size,
            stream_idle_timeout_ms,
            storage_root,
            net: WorkerNetConfig::grpc_from_rpc(rpc_bind, rpc_max_inflight, max_frame_size),
            metadata,
        };

        config.validate()?;

        info!(
            worker_id = config.worker_id.map(|id| id.as_raw()),
            identity_path = ?config.identity_path,
            rpc_bind = %config.rpc_bind,
            rpc_advertised_endpoint = %config.rpc_advertised_endpoint,
            rpc_max_inflight = config.rpc_max_inflight,
            default_frame_size = config.default_frame_size,
            max_frame_size = config.max_frame_size,
            window_bytes = config.window_bytes,
            chunk_size = config.chunk_size,
            storage_root = ?config.storage_root,
            net_listeners = config.net.listeners.len(),
            metadata_endpoint = %config.metadata.endpoint,
            metadata_group_id = config.metadata.group_id.as_raw(),
            register_timeout_ms = config.metadata.register_timeout_ms,
            register_retry_initial_backoff_ms = config.metadata.register_retry_initial_backoff_ms,
            register_retry_max_backoff_ms = config.metadata.register_retry_max_backoff_ms,
            "Worker configuration loaded"
        );

        Ok(config)
    }

    /// Validate shape-only constraints without touching local storage.
    pub fn validate(&self) -> Result<(), CommonError> {
        if self.worker_id == Some(WorkerId::new(0)) {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.id must be a non-zero integer",
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

        if self.chunk_size == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.chunk_size must be greater than zero",
            ));
        }

        if self.stream_idle_timeout_ms == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.stream.idle_timeout_ms must be greater than zero",
            ));
        }

        if self.storage_root.as_os_str().is_empty() {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.storage.root must not be empty",
            ));
        }

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

    fn optional_worker_id(flat: &common::config::FlatConfig) -> Result<Option<WorkerId>, CommonError> {
        let Some(value) = flat.get_str("id") else {
            if flat.contains_key("id") {
                return Err(invalid_config("worker.id", "must be a non-zero integer"));
            }
            return Ok(None);
        };
        if value.trim().is_empty() {
            return Err(invalid_config("worker.id", "must not be empty"));
        }
        let raw = value
            .parse::<u64>()
            .map_err(|_| invalid_config("worker.id", "must be a non-zero integer"))?;
        if raw == 0 {
            return Err(invalid_config("worker.id", "must be a non-zero integer"));
        }
        Ok(Some(WorkerId::new(raw)))
    }
}

impl WorkerRegistrationConfig {
    /// Validate worker metadata registration config without opening a connection.
    pub fn validate(&self) -> Result<(), CommonError> {
        if self.group_id.as_raw() == 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.metadata.group_id must be greater than zero",
            ));
        }

        if self.endpoint.is_empty() {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.metadata.endpoint must not be empty",
            ));
        }

        if !(self.endpoint.starts_with("http://") || self.endpoint.starts_with("https://")) {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                "worker.metadata.endpoint must include http:// or https:// scheme",
            ));
        }

        Endpoint::from_shared(self.endpoint.clone()).map_err(|err| {
            CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("worker.metadata.endpoint must be a valid tonic endpoint URI: {err}"),
            )
        })?;

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
  metadata:
    endpoint: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.rpc_bind, "127.0.0.1:9090");
        assert_eq!(config.worker_id, None);
        assert_eq!(config.identity_path, PathBuf::from("./data/worker.identity"));
        assert_eq!(config.rpc_max_inflight, 100);
        assert_eq!(config.default_frame_size, 1024 * 1024);
        assert_eq!(config.max_frame_size, 4 * 1024 * 1024);
        assert_eq!(config.window_bytes, 8 * 1024 * 1024);
        assert_eq!(config.chunk_size, 1024 * 1024);
        assert_eq!(config.stream_idle_timeout_ms, 60_000);
        assert_eq!(config.storage_root, PathBuf::from("./data"));
        assert_eq!(config.rpc_advertised_endpoint, "http://127.0.0.1:9090");
        assert_eq!(config.metadata.group_id, ShardGroupId::new(1));
        assert_eq!(config.metadata.endpoint, "http://127.0.0.1:18080");
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
  id: 77
  identity:
    path: "/tmp/vecton-worker.identity"
  rpc:
    bind: "127.0.0.1:9091"
    advertised_endpoint: "http://127.0.0.1:19091"
    max_inflight: 8
  default_frame_size: 4096
  max_frame_size: 8192
  window_bytes: 16384
  chunk_size: 32768
  stream:
    idle_timeout_ms: 500
  storage:
    root: "/tmp/vecton-worker"
  metadata:
    group_id: 12
    endpoint: "http://127.0.0.1:18080"
    register_timeout_ms: 2500
    register_retry_initial_backoff_ms: 25
    register_retry_max_backoff_ms: 250
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.worker_id, Some(WorkerId::new(77)));
        assert_eq!(config.identity_path, PathBuf::from("/tmp/vecton-worker.identity"));
        assert_eq!(config.rpc_bind, "127.0.0.1:9091");
        assert_eq!(config.rpc_max_inflight, 8);
        assert_eq!(config.default_frame_size, 4096);
        assert_eq!(config.max_frame_size, 8192);
        assert_eq!(config.window_bytes, 16_384);
        assert_eq!(config.chunk_size, 32_768);
        assert_eq!(config.stream_idle_timeout_ms, 500);
        assert_eq!(config.storage_root, PathBuf::from("/tmp/vecton-worker"));
        assert_eq!(config.rpc_advertised_endpoint, "http://127.0.0.1:19091");
        assert_eq!(config.metadata.group_id, ShardGroupId::new(12));
        assert_eq!(config.metadata.endpoint, "http://127.0.0.1:18080");
        assert_eq!(config.metadata.register_timeout_ms, 2_500);
        assert_eq!(config.metadata.register_retry_initial_backoff_ms, 25);
        assert_eq!(config.metadata.register_retry_max_backoff_ms, 250);
        assert_eq!(config.net.listeners[0].bind, "127.0.0.1:9091");
        assert_eq!(config.net.listeners[0].max_inflight, 8);
    }

    #[test]
    fn worker_id_config_loads_explicit_worker_id() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  id: 91
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  metadata:
    endpoint: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.worker_id, Some(WorkerId::new(91)));
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
  transport:
    default_frame_size: 8388608
    max_frame_size: 16777216
  metadata:
    endpoint: "http://127.0.0.1:18080"
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
  default_frame_size: 8192
  max_frame_size: 4096
  metadata:
    endpoint: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("must be <="));
    }

    #[test]
    fn rejects_zero_chunk_size() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  chunk_size: 0
  metadata:
    endpoint: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("chunk_size"));
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
    endpoint: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.rpc.max_inflight"));
    }

    #[test]
    fn removed_storage_aliases_do_not_change_storage_root() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  storage:
    dir: "/data/a"
    dirs: "/data/b,/data/c"
  metadata:
    endpoint: "http://127.0.0.1:18080"
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.storage_root, WorkerConfig::default().storage_root);
    }

    #[test]
    fn rejects_missing_worker_metadata_endpoint() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            "worker:\n  rpc:\n    bind: \"127.0.0.1:9090\"\n    advertised_endpoint: \"http://127.0.0.1:9090\"\n",
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.metadata.endpoint"));
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
    endpoint: "http://127.0.0.1:18080"
"#
                ),
            )
            .unwrap();

            let error = WorkerConfig::load(&config_path).unwrap_err();

            assert!(error.message.contains("worker.id"));
        }
    }

    #[test]
    fn rejects_invalid_worker_metadata_endpoint() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  rpc:
    advertised_endpoint: "http://127.0.0.1:9090"
  metadata:
    endpoint: "127.0.0.1:18080"
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.metadata.endpoint"));
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
    endpoint: "http://127.0.0.1:18080"
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
  metadata:
    endpoint: "http://127.0.0.1:18080"
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
  metadata:
    endpoint: "http://127.0.0.1:18080"
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
  metadata:
    endpoint: "http://127.0.0.1:18080"
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
  metadata:
    endpoint: "http://127.0.0.1:18080"
    register_retry_initial_backoff_ms: 500
    register_retry_max_backoff_ms: 100
"#,
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("worker.metadata.register_retry_max_backoff_ms"));
    }
}
