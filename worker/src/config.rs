// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker configuration for the current data service skeleton.

use std::path::{Path, PathBuf};

use common::config::CoreConfig;
use common::error::{CommonError, CommonErrorCode};
use tracing::info;

/// Worker configuration.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// RPC server bind address.
    pub rpc_bind: String,
    /// Maximum concurrent RPC requests.
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
    /// Placeholder root for worker-local block storage.
    pub storage_root: PathBuf,
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

        let config = Self {
            rpc_bind: worker_sub
                .get_str("rpc.bind")
                .unwrap_or_else(|| defaults.rpc_bind.clone()),
            rpc_max_inflight: worker_sub
                .get_usize("rpc.max_inflight")
                .unwrap_or(defaults.rpc_max_inflight),
            default_frame_size: Self::bytes_u32(
                worker_sub
                    .get_bytes("default_frame_size")
                    .or_else(|| worker_sub.get_bytes("transport.default_frame_size")),
                defaults.default_frame_size,
                "worker.default_frame_size",
            )?,
            max_frame_size: Self::bytes_u32(
                worker_sub
                    .get_bytes("max_frame_size")
                    .or_else(|| worker_sub.get_bytes("transport.max_frame_size")),
                defaults.max_frame_size,
                "worker.max_frame_size",
            )?,
            window_bytes: Self::bytes_u32(
                worker_sub
                    .get_bytes("window_bytes")
                    .or_else(|| worker_sub.get_bytes("stream.window_bytes")),
                defaults.window_bytes,
                "worker.window_bytes",
            )?,
            chunk_size: Self::bytes_u32(
                worker_sub
                    .get_bytes("chunk_size")
                    .or_else(|| worker_sub.get_bytes("storage.chunk_size")),
                defaults.chunk_size,
                "worker.chunk_size",
            )?,
            stream_idle_timeout_ms: worker_sub
                .get_usize("stream.idle_timeout_ms")
                .map(|value| value as u64)
                .unwrap_or(defaults.stream_idle_timeout_ms),
            storage_root: worker_sub
                .get_str("storage.root")
                .or_else(|| worker_sub.get_str("storage.dir"))
                .or_else(|| Self::first_storage_dir(worker_sub.get_str("storage.dirs")))
                .map(PathBuf::from)
                .unwrap_or_else(|| defaults.storage_root.clone()),
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

        Ok(())
    }

    fn bytes_u32(value: Option<usize>, fallback: u32, field_name: &str) -> Result<u32, CommonError> {
        match value {
            Some(value) => u32::try_from(value).map_err(|_| {
                CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    format!("{field_name} exceeds u32 byte size"),
                )
            }),
            None => Ok(fallback),
        }
    }

    fn first_storage_dir(value: Option<String>) -> Option<String> {
        value.and_then(|dirs| {
            dirs.split(',')
                .map(str::trim)
                .find(|entry| !entry.is_empty())
                .map(str::to_string)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn storage_dirs_fallback_uses_first_entry_as_root_placeholder() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(&config_path, "worker:\n  storage:\n    dirs: \"/data/a,/data/b\"\n").unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();

        assert_eq!(config.storage_root, PathBuf::from("/data/a"));
    }
}
