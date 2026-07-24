// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Configuration system with flat dotted-key support.

mod files;
mod flat;
pub mod keys;
mod validate;

pub use files::{load_from_yaml_file, load_merged};
pub use flat::FlatConfig;
pub use validate::{validate_client, validate_core};

pub use keys::{observe_log, observe_metrics};

use crate::error::CommonError;
use std::path::Path;

/// Server-side flat configuration.
#[derive(Clone, Debug, Default)]
pub struct ServerConfig {
    /// Underlying flat configuration.
    pub inner: FlatConfig,
}

impl ServerConfig {
    /// Load server-side configuration from a file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, CommonError> {
        let default = Self::default();
        let config = load_merged(default.inner, Some(path.as_ref()))?;
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
        let config = load_merged(default.inner, Some(path.as_ref()))?;
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
pub fn load_server_config<P: AsRef<Path>>(path: P) -> Result<ServerConfig, CommonError> {
    ServerConfig::load(path)
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
    fn server_config_default_does_not_own_observability_policy_defaults() {
        let config = ServerConfig::default();

        assert!(config.inner.get_str(observe_log::LEVEL).is_none());
        assert!(config.inner.get_str(observe_log::FORMAT).is_none());
        assert!(config.inner.get_str(observe_metrics::PROMETHEUS_BIND).is_none());
        assert!(
            config.inner.get_str("metadata.storage.dir").is_none(),
            "metadata defaults belong to beryl_metadata::config"
        );
        assert!(
            config.inner.keys_with_prefix("worker.store.dirs").is_empty(),
            "worker defaults belong to beryl_worker::config"
        );
    }

    #[test]
    fn client_config_default_does_not_own_client_module_defaults() {
        let config = ClientConfig::default();

        assert!(config.inner.get_str("client.metadata.endpoints").is_none());
        assert!(config.inner.get_i64("client.retry.max_retry_attempts").is_none());
    }

    #[test]
    fn load_server_yaml_merges_file_values_with_shared_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("server.yaml");

        let yaml_content = r#"
metadata.rpc.port: 18081
worker.rpc.bind: "127.0.0.1:9091"
observe.log.format: compact
observe.log.output: stderr
observe.log.level: "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn"
observe.metrics.prometheus.bind: "127.0.0.1:18081"
observe.metrics.prometheus.path: "/metrics"
"#;
        fs::write(&config_path, yaml_content).unwrap();

        let config = ServerConfig::load(&config_path).unwrap();
        assert_eq!(config.inner.get_i64("metadata.rpc.port"), Some(18081));
        assert_eq!(
            config.inner.get_str("worker.rpc.bind"),
            Some("127.0.0.1:9091".to_string())
        );
        assert_eq!(config.inner.get_str(observe_log::FORMAT), Some("compact".to_string()));
    }

    #[test]
    fn load_server_yaml_uses_file_observe_values() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("server.yaml");
        fs::write(
            &config_path,
            r#"
observe.log.format: json
observe.log.output: stdout
observe.log.level: "warn"
observe.metrics.prometheus.bind: "127.0.0.1:18081"
observe.metrics.prometheus.path: "/metrics"
"#,
        )
        .unwrap();

        let loaded = ServerConfig::load(&config_path).unwrap();
        assert_eq!(loaded.inner.get_str(observe_log::FORMAT), Some("json".to_string()));
        assert_eq!(
            loaded.inner.get_str(observe_metrics::PROMETHEUS_BIND),
            Some("127.0.0.1:18081".to_string())
        );
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
        let mut config = ServerConfig::default();
        config.inner.set(observe_metrics::PROMETHEUS_BIND, "127.0.0.1:70000");

        let result = validate_core(&config.inner);

        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains(observe_metrics::PROMETHEUS_BIND));
    }

    #[test]
    fn validate_core_does_not_claim_module_specific_keys() {
        let mut config = ServerConfig::default();
        config.inner.set("worker.rpc.bind", "not-a-socket");
        config.inner.set("metadata.rpc.port", 70000i64);

        validate_core(&config.inner).expect("module validation belongs to owning crates");
    }
}
