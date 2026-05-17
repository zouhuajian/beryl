// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Configuration system with flat dotted-key support.

mod env;
mod files;
mod flat;
pub mod keys;
mod validate;

pub use env::{dotted_to_env_key, load_from_env};
pub use files::{load_from_yaml_file, load_merged};
pub use flat::FlatConfig;
pub use validate::{validate_client, validate_core};

pub use keys::{
    client, client_cache, client_consistency, client_read_mode, client_retry, client_worker_direct_read,
    client_write_mode, metadata_authority, metadata_raft, metadata_rpc, metadata_storage, observe_logging,
    observe_metrics, observe_tracing, worker_concurrency, worker_eviction, worker_metadata, worker_orphan,
    worker_replication, worker_service_rpc, worker_storage, worker_ufs, worker_volume_health,
};

use crate::error::CommonError;
use std::path::Path;

/// Core-site configuration (server-side).
#[derive(Clone, Debug)]
pub struct CoreConfig {
    /// Underlying flat configuration.
    pub inner: FlatConfig,
}

impl CoreConfig {
    /// Load core-site configuration from a file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, CommonError> {
        let default = Self::default();
        let config = load_merged(default.inner, Some(path.as_ref()), true)?;
        validate_core(&config)?;
        Ok(Self { inner: config })
    }

    /// Create from a FlatConfig.
    pub fn from_flat(inner: FlatConfig) -> Self {
        Self { inner }
    }

    /// Get the underlying FlatConfig.
    pub fn as_flat(&self) -> &FlatConfig {
        &self.inner
    }
}

impl Default for CoreConfig {
    fn default() -> Self {
        let mut config = FlatConfig::new();

        // ============================================================================
        // Metadata RPC Configuration
        // ============================================================================
        config.set(metadata_rpc::ADDR, "0.0.0.0");
        config.set(metadata_rpc::PORT, 18080i64);

        // ============================================================================
        // Metadata Storage Configuration
        // ============================================================================
        config.set(metadata_storage::DIR, "data/metadata");

        // ============================================================================
        // Metadata Raft Configuration
        // ============================================================================
        config.set(metadata_raft::NODE_ID, 1i64);

        // ============================================================================
        // Metadata Authority Configuration
        // ============================================================================
        config.set(metadata_authority::GROUP_ID, 0i64);

        // ============================================================================
        // Worker RPC Configuration
        // ============================================================================
        config.set(worker_service_rpc::BIND, "0.0.0.0:9090");
        config.set(worker_service_rpc::MAX_INFLIGHT, 100i64);

        // ============================================================================
        // Worker Storage Configuration
        // ============================================================================
        config.set(worker_storage::DIRS, "./data");
        config.set(worker_storage::BLOCK_SIZE, "32MB");
        config.set(worker_storage::CHUNK_SIZE, "1MB");
        config.set(worker_storage::KIND, "fs");

        // ============================================================================
        // Worker Concurrency Configuration
        // ============================================================================
        config.set(worker_concurrency::MAX_READ_OPS, 100i64);
        config.set(worker_concurrency::MAX_WRITE_OPS, 50i64);
        config.set(worker_concurrency::QUEUE_SIZE, 1000i64);

        // ============================================================================
        // Worker Eviction Configuration
        // ============================================================================
        config.set(worker_eviction::HIGH_WATERMARK, "0.90");
        config.set(worker_eviction::LOW_WATERMARK, "0.80");
        config.set(worker_eviction::RATE_BYTES_PER_SEC, "100MB");
        config.set(worker_eviction::RATE_IOPS, 100i64);

        // ============================================================================
        // Worker Orphan Configuration
        // ============================================================================
        config.set(worker_orphan::GRACE_PERIOD_SECS, 3600i64);
        config.set(worker_orphan::SCAN_INTERVAL_SECS, 300i64);

        // ============================================================================
        // Worker Volume Health Configuration
        // ============================================================================
        config.set(worker_volume_health::ERROR_RATE_THRESHOLD, 10i64);
        config.set(worker_volume_health::CONSECUTIVE_FAILURES_THRESHOLD, 5i64);
        config.set(worker_volume_health::RECOVERY_PROBE_INTERVAL_SECS, 60i64);
        config.set(worker_volume_health::RECOVERY_PROBE_TIMEOUT_SECS, 5i64);

        // ============================================================================
        // Worker UFS Configuration
        // ============================================================================
        config.set(worker_ufs::MAX_CONCURRENT_PER_INSTANCE, 10i64);
        config.set(worker_ufs::TIMEOUT_MS, 30000i64);
        config.set(worker_ufs::ASYNC_FILL, false);

        // ============================================================================
        // Worker Metadata Configuration
        // ============================================================================
        config.set(worker_metadata::HEARTBEAT_INTERVAL_SEC, 5i64);
        config.set(worker_metadata::BLOCK_REPORT_INTERVAL_SEC, 60i64);
        config.set(worker_metadata::BACKOFF_DURATION_SEC, 10i64);

        // ============================================================================
        // Worker Replication Configuration
        // ============================================================================
        config.set(worker_replication::PEER_CONNECTION_POOL_SIZE, 4i64);
        config.set(worker_replication::MAX_CONCURRENT_BLOCKS, 10i64);
        config.set(worker_replication::MAX_CONCURRENT_CHUNKS_PER_BLOCK, 4i64);
        config.set(worker_replication::CHUNK_TIMEOUT_MS, 30000i64);
        config.set(worker_replication::FENCING_MODE, "special");

        // ============================================================================
        // Observability Logging Configuration
        // ============================================================================
        config.set(observe_logging::LEVEL, "info");
        config.set(observe_logging::FORMAT, "json");
        config.set(observe_logging::STDOUT, true);

        // ============================================================================
        // Observability Tracing Configuration
        // ============================================================================
        config.set(observe_tracing::ENABLED, true);
        config.set(observe_tracing::SAMPLING_RATIO, "1.0");
        config.set(observe_tracing::SAMPLING_PARENT_BASED, true);
        config.set(observe_tracing::OTLP_ENABLED, false);
        config.set(observe_tracing::OTLP_ENDPOINT, "http://localhost:4317");
        config.set(observe_tracing::OTLP_PROTOCOL, "grpc");
        config.set(observe_tracing::OTLP_TIMEOUT_MS, 10000i64);

        // ============================================================================
        // Observability Metrics Configuration
        // ============================================================================
        config.set(observe_metrics::ENABLED, true);
        config.set(observe_metrics::PROMETHEUS_ENABLED, true);
        config.set(observe_metrics::PROMETHEUS_BIND, "0.0.0.0:9090");
        config.set(observe_metrics::PROMETHEUS_PATH, "/metrics");
        config.set(observe_metrics::OTLP_ENABLED, false);
        config.set(observe_metrics::OTLP_ENDPOINT, "http://localhost:4317");
        config.set(observe_metrics::OTLP_PROTOCOL, "grpc");
        config.set(observe_metrics::OTLP_INTERVAL_MS, 60000i64);

        Self { inner: config }
    }
}

/// Client-site configuration (client-side).
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Underlying flat configuration.
    pub inner: FlatConfig,
}

impl ClientConfig {
    /// Load client-site configuration from a file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, CommonError> {
        let default = Self::default();
        let config = load_merged(default.inner, Some(path.as_ref()), true)?;
        validate_client(&config)?;
        Ok(Self { inner: config })
    }

    /// Create from a FlatConfig.
    pub fn from_flat(inner: FlatConfig) -> Self {
        Self { inner }
    }

    /// Get the underlying FlatConfig.
    pub fn as_flat(&self) -> &FlatConfig {
        &self.inner
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        let mut config = FlatConfig::new();

        // ============================================================================
        // Client Configuration
        // ============================================================================
        config.set(client::ID, 0i64);
        config.set(client::DEFAULT_TIMEOUT_MS, 30000i64);
        config.set(client::METADATA_ENDPOINTS, "127.0.0.1:18080");

        // ============================================================================
        // Client Consistency Configuration
        // ============================================================================
        config.set(client_consistency::DEFAULT, "normal");

        // ============================================================================
        // Client Read Mode Configuration
        // ============================================================================
        config.set(client_read_mode::DEFAULT, "cached");
        config.set(client_read_mode::FALLBACK, "direct");

        // ============================================================================
        // Client Write Mode Configuration
        // ============================================================================
        config.set(client_write_mode::DEFAULT, "back");
        config.set(client_write_mode::FALLBACK, "through");

        // ============================================================================
        // Client Cache Configuration
        // ============================================================================
        config.set(client_cache::FILE_META_MAX_ENTRIES, 10000i64);
        config.set(client_cache::FILE_META_TTL_SECS, 300i64);
        config.set(client_cache::ROUTE_MAX_ENTRIES, 1000i64);
        config.set(client_cache::ROUTE_TTL_SECS, 60i64);

        // ============================================================================
        // Client Retry Configuration
        // ============================================================================
        config.set(client_retry::MAX_RETRIES, 3i64);
        config.set(client_retry::INITIAL_BACKOFF_MS, 100i64);
        config.set(client_retry::MAX_BACKOFF_MS, 5000i64);
        config.set(client_retry::BACKOFF_MULTIPLIER, "2.0");

        // ============================================================================
        // Client Worker Direct Read Configuration
        // ============================================================================
        config.set(client_worker_direct_read::ENABLED, true);
        config.set(client_worker_direct_read::CACHE_MAX_ENTRIES, 1000i64);
        config.set(client_worker_direct_read::CACHE_TTL_SECS, 60i64);
        config.set(client_worker_direct_read::VERSION_CHECK, true);

        Self { inner: config }
    }
}

/// Convenience functions for loading configs.
pub fn load_core_site<P: AsRef<Path>>(path: P) -> Result<CoreConfig, CommonError> {
    CoreConfig::load(path)
}

pub fn load_client_site<P: AsRef<Path>>(path: P) -> Result<ClientConfig, CommonError> {
    ClientConfig::load(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_core_config_default() {
        use crate::config::keys::{metadata_rpc, worker_service_rpc};
        let config = CoreConfig::default();
        assert_eq!(config.inner.get_i64(metadata_rpc::PORT), Some(18080));
        assert_eq!(config.inner.get_str(metadata_rpc::ADDR), Some("0.0.0.0".to_string()));
        assert_eq!(
            config.inner.get_str("metadata.storage.dir"),
            Some("data/metadata".to_string())
        );
        assert_eq!(
            config.inner.get_str(worker_service_rpc::BIND),
            Some("0.0.0.0:9090".to_string())
        );
        assert_eq!(config.inner.get_i64(worker_service_rpc::MAX_INFLIGHT), Some(100));
    }

    #[test]
    fn test_client_config_default() {
        use crate::config::keys::client;
        let config = ClientConfig::default();
        assert_eq!(config.inner.get_i64(client::DEFAULT_TIMEOUT_MS), Some(30000));
        assert_eq!(
            config.inner.get_str(client::METADATA_ENDPOINTS),
            Some("127.0.0.1:18080".to_string())
        );
    }

    #[test]
    fn test_load_core_site_yaml() {
        use crate::config::keys::{metadata_rpc, worker_service_rpc};
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");

        let yaml_content = r#"
                metadata.rpc.port: 18081
                worker.rpc.bind: "127.0.0.1:9091"
                "#;
        fs::write(&config_path, yaml_content).unwrap();

        let config = CoreConfig::load(&config_path).unwrap();
        assert_eq!(config.inner.get_i64(metadata_rpc::PORT), Some(18081));
        assert_eq!(
            config.inner.get_str(worker_service_rpc::BIND),
            Some("127.0.0.1:9091".to_string())
        );
        // Default value should still be present
        assert_eq!(config.inner.get_i64(worker_service_rpc::MAX_INFLIGHT), Some(100));
    }

    #[test]
    fn test_load_client_site_yaml() {
        use crate::config::keys::client;
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("client-site.yaml");

        let yaml_content = r#"
client.metadata.endpoints: "127.0.0.1:18081,127.0.0.1:18082"
client.default_timeout_ms: 60000
"#;
        fs::write(&config_path, yaml_content).unwrap();

        let config = ClientConfig::load(&config_path).unwrap();
        assert_eq!(
            config.inner.get_str(client::METADATA_ENDPOINTS),
            Some("127.0.0.1:18081,127.0.0.1:18082".to_string())
        );
        assert_eq!(config.inner.get_i64(client::DEFAULT_TIMEOUT_MS), Some(60000));
    }

    #[test]
    fn test_validate_core_invalid_port() {
        use crate::config::keys::metadata_rpc;
        let mut config = CoreConfig::default();
        config.inner.set(metadata_rpc::PORT, 70000i64);

        let result = validate_core(&config.inner);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains(metadata_rpc::PORT));
    }

    #[test]
    fn test_validate_core_valid_worker_service_bind() {
        use crate::config::keys::worker_service_rpc;

        for bind in ["127.0.0.1:9000", "0.0.0.0:9000"] {
            let mut config = CoreConfig::default();
            config.inner.set(worker_service_rpc::BIND, bind);

            validate_core(&config.inner).unwrap_or_else(|err| panic!("{bind} should be valid: {err:?}"));
        }
    }

    #[test]
    fn test_validate_core_invalid_worker_service_bind() {
        use crate::config::keys::worker_service_rpc;

        for bind in ["abc", "abc:xyz", "127.0.0.1:notaport"] {
            let mut config = CoreConfig::default();
            config.inner.set(worker_service_rpc::BIND, bind);

            let result = validate_core(&config.inner);

            assert!(result.is_err(), "{bind} should be invalid");
            assert!(result.unwrap_err().message.contains(worker_service_rpc::BIND));
        }
    }

    #[test]
    fn test_validate_core_invalid_block_chunk_size() {
        use crate::config::keys::worker_storage;
        let mut config = CoreConfig::default();
        // Set block_size to 10MB (10 * 1024 * 1024 = 10485760 bytes)
        // Set chunk_size to 3MB (3 * 1024 * 1024 = 3145728 bytes)
        // 10485760 % 3145728 = 1048576, which is not 0, so should fail
        config.inner.set(worker_storage::BLOCK_SIZE, "10MB");
        config.inner.set(worker_storage::CHUNK_SIZE, "3MB");

        let result = validate_core(&config.inner);
        assert!(
            result.is_err(),
            "Expected validation to fail for non-divisible block_size/chunk_size"
        );
        let err_msg = result.unwrap_err().message;
        assert!(
            err_msg.contains("must be divisible") || err_msg.contains("chunk_size"),
            "Error message should mention divisibility, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_validate_core_invalid_worker_service_rpc_max_inflight() {
        use crate::config::keys::worker_service_rpc;
        let mut config = CoreConfig::default();
        config.inner.set(worker_service_rpc::MAX_INFLIGHT, 0i64);

        let result = validate_core(&config.inner);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains(worker_service_rpc::MAX_INFLIGHT));
    }

    #[test]
    fn test_validate_client_invalid_endpoints() {
        use crate::config::keys::client;
        let mut config = ClientConfig::default();
        config.inner.set(client::METADATA_ENDPOINTS, "");

        let result = validate_client(&config.inner);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains(client::METADATA_ENDPOINTS));
    }

    #[test]
    fn test_validate_client_invalid_consistency() {
        use crate::config::keys::client_consistency;
        let mut config = ClientConfig::default();
        config.inner.set(client_consistency::DEFAULT, "invalid");

        let result = validate_client(&config.inner);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains(client_consistency::DEFAULT));
    }

    #[test]
    fn test_validate_core_eviction_watermarks() {
        use crate::config::keys::worker_eviction;
        let mut config = CoreConfig::default();
        config.inner.set(worker_eviction::HIGH_WATERMARK, "0.80");
        config.inner.set(worker_eviction::LOW_WATERMARK, "0.90");

        let result = validate_core(&config.inner);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("low_watermark"));
    }

    #[test]
    fn test_load_real_core_site() {
        // Test loading the actual conf/core-site.yaml file
        let config_path = "conf/core-site.yaml";
        if std::path::Path::new(config_path).exists() {
            let result = CoreConfig::load(config_path);
            let config = result.unwrap_or_else(|err| panic!("Failed to load conf/core-site.yaml: {err:?}"));
            assert_eq!(
                config.inner.get_str("metadata.storage.dir"),
                Some("data/metadata".to_string())
            );

            let file_config = load_from_yaml_file(config_path).unwrap();
            assert_eq!(
                file_config.get_str("metadata.storage.dir"),
                Some("data/metadata".to_string())
            );
        }
    }

    #[test]
    fn test_load_real_client_site() {
        // Test loading the actual conf/client-site.yaml file
        let config_path = "conf/client-site.yaml";
        if std::path::Path::new(config_path).exists() {
            let result = ClientConfig::load(config_path);
            assert!(
                result.is_ok(),
                "Failed to load conf/client-site.yaml: {:?}",
                result.err()
            );
        }
    }
}
