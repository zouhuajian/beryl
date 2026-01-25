// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker configuration loading and validation.

use common::config::CoreConfig;
use common::error::{CommonError, ErrorCode};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::info;

/// Worker configuration.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// RPC server bind address.
    pub rpc_bind: String,
    /// Maximum concurrent RPC requests.
    pub rpc_max_inflight: usize,
    /// Storage directories.
    pub storage_dirs: Vec<PathBuf>,
    /// Default block size in bytes.
    pub block_size: u32,
    /// Default chunk size in bytes.
    pub chunk_size: u32,
    /// Maximum concurrent read operations.
    pub max_read_ops: usize,
    /// Maximum concurrent write operations.
    pub max_write_ops: usize,
    /// Request queue size.
    pub queue_size: usize,
    /// Eviction configuration.
    pub eviction: EvictionConfig,
    /// Orphan detection configuration.
    pub orphan: OrphanConfig,
    /// Volume health configuration.
    pub volume_health: VolumeHealthConfig,
    /// UFS configuration.
    pub ufs: UfsConfig,
    /// Metadata configuration.
    pub metadata: MetadataConfig,
    /// Replication configuration.
    pub replication: ReplicationConfig,
    /// Transport configuration.
    pub transport: TransportConfig,
    /// Storage configuration.
    pub storage: StorageConfig,
}

/// Eviction configuration.
#[derive(Clone, Debug)]
pub struct EvictionConfig {
    /// High watermark (0.0-1.0).
    pub high_watermark: f64,
    /// Low watermark (0.0-1.0).
    pub low_watermark: f64,
    /// Eviction rate (bytes per second).
    pub eviction_rate_bytes_per_sec: u64,
    /// Eviction rate (IOPS).
    pub eviction_rate_iops: u64,
}

/// Orphan detection configuration.
#[derive(Clone, Debug)]
pub struct OrphanConfig {
    /// Grace period before deleting orphans (seconds).
    pub grace_period_secs: u64,
    /// Scan interval (seconds).
    pub scan_interval_secs: u64,
}

/// UFS configuration.
#[derive(Clone, Debug)]
pub struct UfsConfig {
    /// Default UFS instance ID.
    pub default_ufs_id: Option<String>,
    /// Maximum concurrent reads per UFS instance.
    pub max_concurrent_per_ufs: usize,
    /// UFS read timeout in milliseconds.
    pub timeout_ms: u64,
    /// Whether to use async fill-back.
    pub async_fill: bool,
}

/// Metadata configuration.
#[derive(Clone, Debug)]
pub struct MetadataConfig {
    /// Metadata group endpoints: group_id -> endpoint
    pub groups: Vec<MetadataGroupConfig>,
    /// Heartbeat interval in seconds.
    pub heartbeat_interval_sec: u64,
    /// Block report interval in seconds.
    pub block_report_interval_sec: u64,
    /// Backoff duration in seconds on failure.
    pub backoff_duration_sec: u64,
}

/// Metadata group configuration.
#[derive(Clone, Debug)]
pub struct MetadataGroupConfig {
    /// Group ID.
    pub group_id: u64,
    /// Metadata endpoint (e.g., "http://localhost:8080").
    pub endpoint: String,
}

/// Replication configuration.
#[derive(Clone, Debug)]
pub struct ReplicationConfig {
    /// Peer worker endpoints: worker_id -> endpoint (e.g., "http://127.0.0.1:50051")
    pub peer_endpoints: HashMap<u64, String>,
    /// Connection pool size per peer.
    pub peer_connection_pool_size: usize,
    /// Maximum concurrent blocks being replicated.
    pub max_concurrent_blocks: usize,
    /// Maximum concurrent chunks per block.
    pub max_concurrent_chunks_per_block: usize,
    /// Chunk replication timeout in milliseconds.
    pub chunk_timeout_ms: u64,
    /// Fencing token mode: strict, special, skip.
    pub fencing_mode: String,
    /// Special token value for replication (when mode=special).
    pub special_token: Option<String>,
}

/// Volume health configuration.
#[derive(Clone, Debug)]
pub struct VolumeHealthConfig {
    /// Error rate threshold (errors per second).
    pub error_rate_threshold: f64,
    /// Consecutive failures threshold.
    pub consecutive_failures_threshold: u32,
    /// Recovery probe interval (seconds).
    pub recovery_probe_interval_secs: u64,
    /// Recovery probe timeout (seconds).
    pub recovery_probe_timeout_secs: u64,
}

/// Transport configuration.
#[derive(Clone, Debug)]
pub struct TransportConfig {
    /// Transport kind (grpc, quic, rdma, io_uring, local).
    pub kind: String,
    /// Connection timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Request timeout in milliseconds.
    pub request_timeout_ms: u64,
    /// Maximum concurrent inflight requests.
    pub max_inflight_requests: usize,
    /// Maximum concurrent inflight streams.
    pub max_inflight_streams: usize,
    /// Server-side max inflight.
    pub server_max_inflight: usize,
    /// Keep-alive interval in milliseconds.
    pub keepalive_interval_ms: u64,
    /// Keep-alive timeout in milliseconds.
    pub keepalive_timeout_ms: u64,
    /// Zero-copy required flag.
    pub zero_copy_required: bool,
    /// Allow fallback to another transport if combo is invalid.
    pub combo_allow_fallback: bool,
    /// Fallback transport kind (if allow_fallback is true).
    pub fallback_transport: Option<String>,
}

/// Storage configuration.
#[derive(Clone, Debug)]
pub struct StorageConfig {
    /// Storage kind (fs, io_uring, spdk).
    pub kind: String,
}

impl WorkerConfig {
    /// Load worker configuration from core-site.yaml.
    pub fn load<P: AsRef<Path>>(config_path: P) -> Result<Self, CommonError> {
        let core_config = CoreConfig::load(config_path)?;
        Self::from_core_config(&core_config)
    }

    /// Create from CoreConfig.
    pub fn from_core_config(core_config: &CoreConfig) -> Result<Self, CommonError> {
        let flat = core_config.as_flat();
        let worker_sub = flat.sub("worker");

        // RPC configuration
        let rpc_bind = worker_sub
            .get_str("rpc.bind")
            .unwrap_or_else(|| "0.0.0.0:9090".to_string());
        let rpc_max_inflight = worker_sub.get_usize("rpc.max_inflight").unwrap_or(100);

        // Storage configuration
        let block_size = worker_sub
            .get_bytes("storage.block_size")
            .map(|v| v as u32)
            .unwrap_or(33_554_432); // 32MB
        let chunk_size = worker_sub
            .get_bytes("storage.chunk_size")
            .map(|v| v as u32)
            .unwrap_or(1_048_576); // 1MB

        // Parse storage.dirs (can be string or array)
        let storage_dirs = if let Some(dirs_str) = worker_sub.get_str("storage.dirs") {
            // Single directory or comma-separated list
            dirs_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(PathBuf::from)
                .collect()
        } else {
            // Default to ./data
            vec![PathBuf::from("./data")]
        };

        // Concurrency configuration
        let max_read_ops = worker_sub.get_usize("concurrency.max_read_ops").unwrap_or(100);
        let max_write_ops = worker_sub.get_usize("concurrency.max_write_ops").unwrap_or(50);
        let queue_size = worker_sub.get_usize("concurrency.queue_size").unwrap_or(1000);

        // Eviction configuration
        let eviction = EvictionConfig {
            high_watermark: worker_sub
                .get_i64("eviction.high_watermark")
                .map(|v| v as f64 / 100.0)
                .or_else(|| {
                    worker_sub
                        .get_str("eviction.high_watermark")
                        .and_then(|s| s.parse::<f64>().ok())
                })
                .unwrap_or(0.90),
            low_watermark: worker_sub
                .get_i64("eviction.low_watermark")
                .map(|v| v as f64 / 100.0)
                .or_else(|| {
                    worker_sub
                        .get_str("eviction.low_watermark")
                        .and_then(|s| s.parse::<f64>().ok())
                })
                .unwrap_or(0.80),
            eviction_rate_bytes_per_sec: worker_sub
                .get_bytes("eviction.rate_bytes_per_sec")
                .unwrap_or(100 * 1024 * 1024) as u64, // 100MB/s
            eviction_rate_iops: worker_sub
                .get_usize("eviction.rate_iops")
                .map(|v| v as u64)
                .unwrap_or(100),
        };

        // Orphan configuration
        let orphan = OrphanConfig {
            grace_period_secs: worker_sub
                .get_usize("orphan.grace_period_secs")
                .map(|v| v as u64)
                .unwrap_or(3600), // 1 hour
            scan_interval_secs: worker_sub
                .get_usize("orphan.scan_interval_secs")
                .map(|v| v as u64)
                .unwrap_or(300), // 5 minutes
        };

        // Volume health configuration
        let volume_health = VolumeHealthConfig {
            error_rate_threshold: worker_sub
                .get_i64("volume_health.error_rate_threshold")
                .map(|v| v as f64)
                .or_else(|| {
                    worker_sub
                        .get_str("volume_health.error_rate_threshold")
                        .and_then(|s| s.parse::<f64>().ok())
                })
                .unwrap_or(10.0),
            consecutive_failures_threshold: worker_sub
                .get_usize("volume_health.consecutive_failures_threshold")
                .map(|v| v as u32)
                .unwrap_or(5),
            recovery_probe_interval_secs: worker_sub
                .get_usize("volume_health.recovery_probe_interval_secs")
                .map(|v| v as u64)
                .unwrap_or(60), // 1 minute
            recovery_probe_timeout_secs: worker_sub
                .get_usize("volume_health.recovery_probe_timeout_secs")
                .map(|v| v as u64)
                .unwrap_or(5),
        };

        // UFS configuration
        let ufs = UfsConfig {
            default_ufs_id: worker_sub.get_str("ufs.default_id").map(|s| s.to_string()),
            max_concurrent_per_ufs: worker_sub.get_usize("ufs.max_concurrent_per_instance").unwrap_or(10),
            timeout_ms: worker_sub
                .get_usize("ufs.timeout_ms")
                .map(|v| v as u64)
                .unwrap_or(30000),
            async_fill: worker_sub
                .get_str("ufs.async_fill")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(false),
        };

        // Metadata configuration
        let mut groups = Vec::new();
        // Parse metadata.groups as array or single group
        if let Some(groups_str) = worker_sub.get_str("metadata.groups") {
            // Format: "group_id1:endpoint1,group_id2:endpoint2" or JSON array
            for group_str in groups_str.split(',') {
                let parts: Vec<&str> = group_str.split(':').collect();
                if parts.len() == 2 {
                    if let Ok(group_id) = parts[0].trim().parse::<u64>() {
                        groups.push(MetadataGroupConfig {
                            group_id,
                            endpoint: parts[1].trim().to_string(),
                        });
                    }
                }
            }
        }
        // Also check for individual group configs: metadata.group.0.endpoint, metadata.group.1.endpoint, etc.
        for key in flat.keys() {
            if key.starts_with("metadata.group.") && key.ends_with(".endpoint") {
                if let Some(group_id_str) = key
                    .strip_prefix("metadata.group.")
                    .and_then(|s| s.strip_suffix(".endpoint"))
                {
                    if let Ok(group_id) = group_id_str.parse::<u64>() {
                        if let Some(endpoint) = worker_sub.get_str(key) {
                            groups.push(MetadataGroupConfig {
                                group_id,
                                endpoint: endpoint.to_string(),
                            });
                        }
                    }
                }
            }
        }

        let metadata = MetadataConfig {
            groups,
            heartbeat_interval_sec: worker_sub
                .get_usize("metadata.heartbeat_interval_sec")
                .map(|v| v as u64)
                .unwrap_or(5),
            block_report_interval_sec: worker_sub
                .get_usize("metadata.block_report_interval_sec")
                .map(|v| v as u64)
                .unwrap_or(60),
            backoff_duration_sec: worker_sub
                .get_usize("metadata.backoff_duration_sec")
                .map(|v| v as u64)
                .unwrap_or(10),
        };

        // Replication configuration
        let mut peer_endpoints = HashMap::new();
        // Parse worker.replication.peer_endpoints
        // Format: "worker_id1:endpoint1,worker_id2:endpoint2" or individual keys
        if let Some(peers_str) = worker_sub.get_str("replication.peer_endpoints") {
            for peer_str in peers_str.split(',') {
                let parts: Vec<&str> = peer_str.split(':').collect();
                if parts.len() >= 2 {
                    if let Ok(worker_id) = parts[0].trim().parse::<u64>() {
                        let endpoint = parts[1..].join(":"); // Handle endpoints with colons (e.g., http://...)
                        peer_endpoints.insert(worker_id, endpoint);
                    }
                }
            }
        }
        // Also check for individual peer configs: replication.peer.1.endpoint, etc.
        for key in flat.keys() {
            if key.starts_with("replication.peer.") && key.ends_with(".endpoint") {
                if let Some(worker_id_str) = key
                    .strip_prefix("replication.peer.")
                    .and_then(|s| s.strip_suffix(".endpoint"))
                {
                    if let Ok(worker_id) = worker_id_str.parse::<u64>() {
                        if let Some(endpoint) = worker_sub.get_str(key) {
                            peer_endpoints.insert(worker_id, endpoint.to_string());
                        }
                    }
                }
            }
        }

        let replication = ReplicationConfig {
            peer_endpoints,
            peer_connection_pool_size: worker_sub
                .get_usize("replication.peer_connection_pool_size")
                .unwrap_or(4),
            max_concurrent_blocks: worker_sub.get_usize("replication.max_concurrent_blocks").unwrap_or(10),
            max_concurrent_chunks_per_block: worker_sub
                .get_usize("replication.max_concurrent_chunks_per_block")
                .unwrap_or(4),
            chunk_timeout_ms: worker_sub
                .get_usize("replication.chunk_timeout_ms")
                .map(|v| v as u64)
                .unwrap_or(30000), // 30 seconds
            fencing_mode: worker_sub
                .get_str("replication.fencing.mode")
                .unwrap_or_else(|| "special".to_string()),
            special_token: worker_sub
                .get_str("replication.fencing.special_token")
                .map(|s| s.to_string()),
        };

        // Transport configuration
        // Read from worker.transport.xxx only
        let transport_sub = worker_sub.sub("transport");

        let transport = TransportConfig {
            kind: transport_sub.get_str("kind").unwrap_or_else(|| "grpc".to_string()),
            connect_timeout_ms: transport_sub
                .get_usize("connect_timeout_ms")
                .map(|v| v as u64)
                .unwrap_or(5000),
            request_timeout_ms: transport_sub
                .get_usize("request_timeout_ms")
                .map(|v| v as u64)
                .unwrap_or(30000),
            max_inflight_requests: transport_sub.get_usize("max_inflight_requests").unwrap_or(100),
            max_inflight_streams: transport_sub.get_usize("max_inflight_streams").unwrap_or(10),
            server_max_inflight: transport_sub.get_usize("server.max_inflight").unwrap_or(100),
            keepalive_interval_ms: transport_sub
                .get_usize("keepalive_interval_ms")
                .map(|v| v as u64)
                .unwrap_or(30000),
            keepalive_timeout_ms: transport_sub
                .get_usize("keepalive_timeout_ms")
                .map(|v| v as u64)
                .unwrap_or(5000),
            zero_copy_required: transport_sub
                .get_str("zero_copy.required")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
            combo_allow_fallback: transport_sub
                .get_str("combo.allow_fallback")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(false),
            fallback_transport: transport_sub.get_str("combo.fallback_transport").map(|s| s.to_string()),
        };

        // Storage configuration
        let storage = StorageConfig {
            kind: worker_sub.get_str("storage.kind").unwrap_or_else(|| "fs".to_string()),
        };

        let config = Self {
            rpc_bind,
            rpc_max_inflight,
            storage_dirs,
            block_size,
            chunk_size,
            max_read_ops,
            max_write_ops,
            queue_size,
            eviction,
            orphan,
            volume_health,
            ufs,
            metadata,
            replication,
            transport,
            storage,
        };

        // Validate configuration
        config.validate()?;

        info!(
            rpc_bind = %config.rpc_bind,
            rpc_max_inflight = config.rpc_max_inflight,
            storage_dirs = ?config.storage_dirs,
            block_size = config.block_size,
            chunk_size = config.chunk_size,
            "Worker configuration loaded"
        );

        Ok(config)
    }

    /// Validate configuration.
    pub fn validate(&self) -> Result<(), CommonError> {
        // Validate block_size % chunk_size == 0
        if self.block_size % self.chunk_size != 0 {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "block_size ({}) must be divisible by chunk_size ({})",
                    self.block_size, self.chunk_size
                ),
            ));
        }

        // Validate rpc.bind is a valid socket address
        if self.rpc_bind.parse::<std::net::SocketAddr>().is_err() {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                format!("invalid rpc.bind address: {}", self.rpc_bind),
            ));
        }

        // Validate rpc_max_inflight > 0
        if self.rpc_max_inflight == 0 {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                "rpc.max_inflight must be > 0",
            ));
        }

        // Validate storage_dirs
        if self.storage_dirs.is_empty() {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                "at least one storage directory is required",
            ));
        }

        // Check each directory exists and is writable
        for dir in &self.storage_dirs {
            if !dir.exists() {
                // Try to create it
                if let Err(e) = std::fs::create_dir_all(dir) {
                    return Err(CommonError::new(
                        ErrorCode::Io,
                        format!("failed to create storage directory {}: {}", dir.display(), e),
                    ));
                }
            }

            // Check if writable
            let metadata = std::fs::metadata(dir).map_err(|e| {
                CommonError::new(
                    ErrorCode::Io,
                    format!("failed to access storage directory {}: {}", dir.display(), e),
                )
            })?;

            if !metadata.is_dir() {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
                    format!("storage path is not a directory: {}", dir.display()),
                ));
            }

            // Try to write a test file
            let test_file = dir.join(".worker_test_write");
            if let Err(e) = std::fs::write(&test_file, b"test") {
                return Err(CommonError::new(
                    ErrorCode::Io,
                    format!("storage directory is not writable: {}: {}", dir.display(), e),
                ));
            }
            // Clean up test file
            let _ = std::fs::remove_file(&test_file);
        }

        // Validate concurrency settings
        if self.max_read_ops == 0 {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                "concurrency.max_read_ops must be > 0",
            ));
        }
        if self.max_write_ops == 0 {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                "concurrency.max_write_ops must be > 0",
            ));
        }
        if self.queue_size == 0 {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                "concurrency.queue_size must be > 0",
            ));
        }

        // Validate metadata group configurations
        let mut group_ids = std::collections::HashSet::new();
        for group in &self.metadata.groups {
            // Check group_id uniqueness
            if !group_ids.insert(group.group_id) {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
                    format!("duplicate metadata group_id: {}", group.group_id),
                ));
            }

            // Validate endpoint format (basic check: should be a valid URL or socket address)
            if group.endpoint.is_empty() {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
                    format!("empty endpoint for metadata group {}", group.group_id),
                ));
            }

            // Try to parse as socket address (e.g., "127.0.0.1:8080")
            if group.endpoint.parse::<std::net::SocketAddr>().is_ok() {
                // Valid socket address
                continue;
            }

            // Check if it looks like a URL (starts with http:// or https://)
            if group.endpoint.starts_with("http://") || group.endpoint.starts_with("https://") {
                // Basic URL format check: should contain :// and at least one character after
                if group.endpoint.len() > 7 && group.endpoint.contains("://") {
                    // Looks like a valid URL format
                    continue;
                }
            }

            // If neither socket address nor URL-like, warn but don't fail (might be a hostname)
            tracing::warn!(
                group_id = group.group_id,
                endpoint = %group.endpoint,
                "metadata endpoint format may be invalid (expected socket address or http/https URL)"
            );
        }

        // Validate UFS configuration (if default_ufs_id is set, it should be a valid identifier)
        if let Some(ref ufs_id) = self.ufs.default_ufs_id {
            if ufs_id.is_empty() {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
                    "ufs.default_id cannot be empty if specified",
                ));
            }
        }

        // Validate UFS timeout
        if self.ufs.timeout_ms == 0 {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                "ufs.timeout_ms must be > 0",
            ));
        }

        // Validate UFS concurrency
        if self.ufs.max_concurrent_per_ufs == 0 {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                "ufs.max_concurrent_per_instance must be > 0",
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(&config_path, "worker:\n  rpc:\n    bind: \"127.0.0.1:9090\"\n").unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();
        assert_eq!(config.rpc_bind, "127.0.0.1:9090");
        assert_eq!(config.rpc_max_inflight, 100);
        assert_eq!(config.block_size, 33_554_432);
        assert_eq!(config.chunk_size, 1_048_576);
    }

    #[test]
    fn test_validate_block_size_divisible() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            "worker:\n  storage:\n    block_size: 100\n    chunk_size: 33\n",
        )
        .unwrap();

        let result = WorkerConfig::load(&config_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("must be divisible"));
    }

    #[test]
    fn test_storage_dirs_validation() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path().join("storage");
        fs::create_dir_all(&storage_dir).unwrap();

        let config_path = temp_dir.path().join("core-site.yaml");
        fs::write(
            &config_path,
            format!("worker:\n  storage:\n    dirs: [\"{}\"]\n", storage_dir.display()),
        )
        .unwrap();

        let config = WorkerConfig::load(&config_path).unwrap();
        assert_eq!(config.storage_dirs.len(), 1);
    }
}
