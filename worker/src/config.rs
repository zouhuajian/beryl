// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker configuration for the current data service skeleton.

use std::path::{Path, PathBuf};

use common::config::CoreConfig;
use common::error::{CommonError, CommonErrorCode};
use tracing::info;

use crate::net::config::WorkerNetConfig;
use crate::net::protocol::WorkerNetProtocol;

/// Worker configuration.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// RPC server bind address.
    pub rpc_bind: String,
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
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            rpc_bind: "0.0.0.0:9090".to_string(),
            rpc_max_inflight: 100,
            default_frame_size: 1024 * 1024,
            max_frame_size: 4 * 1024 * 1024,
            window_bytes: 8 * 1024 * 1024,
            chunk_size: 1024 * 1024,
            stream_idle_timeout_ms: 60_000,
            storage_root: PathBuf::from("./data"),
            net: WorkerNetConfig::grpc_from_rpc("0.0.0.0:9090".to_string(), 100, 4 * 1024 * 1024),
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

        let rpc_bind = Self::str_or(&worker_sub, "rpc.bind", &defaults.rpc_bind, "worker.rpc.bind")?;
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

        let config = Self {
            rpc_bind: rpc_bind.clone(),
            rpc_max_inflight,
            default_frame_size,
            max_frame_size,
            window_bytes,
            chunk_size,
            stream_idle_timeout_ms,
            storage_root,
            net: WorkerNetConfig::grpc_from_rpc(rpc_bind, rpc_max_inflight, max_frame_size),
        };

        config.validate()?;

        info!(
            rpc_bind = %config.rpc_bind,
            rpc_max_inflight = config.rpc_max_inflight,
            default_frame_size = config.default_frame_size,
            max_frame_size = config.max_frame_size,
            window_bytes = config.window_bytes,
            chunk_size = config.chunk_size,
            storage_root = ?config.storage_root,
            net_listeners = config.net.listeners.len(),
            "Worker configuration loaded"
        );

        Ok(config)
    }

    /// Validate shape-only constraints without touching local storage.
    pub fn validate(&self) -> Result<(), CommonError> {
        if self.rpc_bind.parse::<std::net::SocketAddr>().is_err() {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("invalid worker.rpc.bind address: {}", self.rpc_bind),
            ));
        }

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
}

fn invalid_config(key: &'static str, detail: &'static str) -> CommonError {
    CommonError::new(CommonErrorCode::InvalidArgument, format!("{key} {detail}"))
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
        fs::write(&config_path, "worker:\n  rpc:\n    bind: \"127.0.0.1:9090\"\n").unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.rpc_bind, "127.0.0.1:9090");
        assert_eq!(config.rpc_max_inflight, 100);
        assert_eq!(config.default_frame_size, 1024 * 1024);
        assert_eq!(config.max_frame_size, 4 * 1024 * 1024);
        assert_eq!(config.window_bytes, 8 * 1024 * 1024);
        assert_eq!(config.chunk_size, 1024 * 1024);
        assert_eq!(config.stream_idle_timeout_ms, 60_000);
        assert_eq!(config.storage_root, PathBuf::from("./data"));
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
  rpc:
    bind: "127.0.0.1:9091"
    max_inflight: 8
  default_frame_size: 4096
  max_frame_size: 8192
  window_bytes: 16384
  chunk_size: 32768
  stream:
    idle_timeout_ms: 500
  storage:
    root: "/tmp/vecton-worker"
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.rpc_bind, "127.0.0.1:9091");
        assert_eq!(config.rpc_max_inflight, 8);
        assert_eq!(config.default_frame_size, 4096);
        assert_eq!(config.max_frame_size, 8192);
        assert_eq!(config.window_bytes, 16_384);
        assert_eq!(config.chunk_size, 32_768);
        assert_eq!(config.stream_idle_timeout_ms, 500);
        assert_eq!(config.storage_root, PathBuf::from("/tmp/vecton-worker"));
        assert_eq!(config.net.listeners[0].bind, "127.0.0.1:9091");
        assert_eq!(config.net.listeners[0].max_inflight, 8);
    }

    #[test]
    fn ignores_removed_worker_transport_frame_size_keys() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            r#"
worker:
  transport:
    default_frame_size: 8388608
    max_frame_size: 16777216
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
            "worker:\n  default_frame_size: 8192\n  max_frame_size: 4096\n",
        )
        .unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("must be <="));
    }

    #[test]
    fn rejects_zero_chunk_size() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(&config_path, "worker:\n  chunk_size: 0\n").unwrap();

        let error = WorkerConfig::load(&config_path).unwrap_err();

        assert!(error.message.contains("chunk_size"));
    }

    #[test]
    fn rejects_wrong_type_current_worker_knobs() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(&config_path, "worker:\n  rpc:\n    max_inflight: false\n").unwrap();

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
  storage:
    dir: "/data/a"
    dirs: "/data/b,/data/c"
"#,
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.storage_root, WorkerConfig::default().storage_root);
    }
}
