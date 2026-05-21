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

pub use keys::{observe_logging, observe_metrics, observe_tracing};

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

        config.set(observe_logging::LEVEL, "info");
        config.set(observe_logging::FORMAT, "json");
        config.set(observe_logging::STDOUT, true);

        config.set(observe_tracing::ENABLED, true);
        config.set(observe_tracing::SAMPLING_RATIO, "1.0");
        config.set(observe_tracing::SAMPLING_PARENT_BASED, true);
        config.set(observe_tracing::OTLP_ENABLED, false);
        config.set(observe_tracing::OTLP_ENDPOINT, "http://localhost:4317");
        config.set(observe_tracing::OTLP_PROTOCOL, "grpc");
        config.set(observe_tracing::OTLP_TIMEOUT_MS, 10000i64);

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
#[derive(Clone, Debug, Default)]
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
    fn core_config_default_owns_only_shared_observability_defaults() {
        let config = CoreConfig::default();

        assert_eq!(config.inner.get_str(observe_logging::LEVEL), Some("info".to_string()));
        assert_eq!(
            config.inner.get_str(observe_metrics::PROMETHEUS_BIND),
            Some("0.0.0.0:9090".to_string())
        );
        assert!(
            config.inner.get_str("metadata.storage.dir").is_none(),
            "metadata defaults belong to metadata::config"
        );
        assert!(
            config.inner.get_str("worker.storage.dirs").is_none(),
            "worker defaults belong to worker::config"
        );
    }

    #[test]
    fn client_config_default_does_not_own_client_module_defaults() {
        let config = ClientConfig::default();

        assert!(config.inner.get_str("client.metadata.endpoints").is_none());
        assert!(config.inner.get_i64("client.retry.max_retry_attempts").is_none());
    }

    #[test]
    fn load_core_site_yaml_merges_file_values_with_shared_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("core-site.yaml");

        let yaml_content = r#"
metadata.rpc.port: 18081
worker.rpc.bind: "127.0.0.1:9091"
"#;
        fs::write(&config_path, yaml_content).unwrap();

        let config = CoreConfig::load(&config_path).unwrap();
        assert_eq!(config.inner.get_i64("metadata.rpc.port"), Some(18081));
        assert_eq!(
            config.inner.get_str("worker.rpc.bind"),
            Some("127.0.0.1:9091".to_string())
        );
        assert_eq!(config.inner.get_str(observe_logging::LEVEL), Some("info".to_string()));
    }

    #[test]
    fn load_client_site_yaml_preserves_client_values_without_common_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("client-site.yaml");

        let yaml_content = r#"
client.metadata.endpoints: "127.0.0.1:18081,127.0.0.1:18082"
client.default_timeout_ms: 60000
"#;
        fs::write(&config_path, yaml_content).unwrap();

        let config = ClientConfig::load(&config_path).unwrap();
        assert_eq!(
            config.inner.get_str("client.metadata.endpoints"),
            Some("127.0.0.1:18081,127.0.0.1:18082".to_string())
        );
        assert_eq!(config.inner.get_i64("client.default_timeout_ms"), Some(60000));
        assert!(config.inner.get_i64("client.retry.max_retry_attempts").is_none());
    }

    #[test]
    fn validate_core_checks_shared_observability_bind() {
        let mut config = CoreConfig::default();
        config.inner.set(observe_metrics::PROMETHEUS_BIND, "127.0.0.1:70000");

        let result = validate_core(&config.inner);

        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains(observe_metrics::PROMETHEUS_BIND));
    }

    #[test]
    fn validate_core_does_not_claim_module_specific_keys() {
        let mut config = CoreConfig::default();
        config.inner.set("worker.rpc.bind", "not-a-socket");
        config.inner.set("metadata.rpc.port", 70000i64);

        validate_core(&config.inner).expect("module validation belongs to owning crates");
    }

    #[test]
    fn test_load_real_core_site() {
        let config_path = "conf/core-site.yaml";
        if std::path::Path::new(config_path).exists() {
            let config = CoreConfig::load(config_path)
                .unwrap_or_else(|err| panic!("Failed to load conf/core-site.yaml: {err:?}"));
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
