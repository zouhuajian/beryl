// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Configuration system with flat dotted-key support.

mod files;
mod flat;
pub mod keys;

pub use files::{load_from_yaml_file, load_merged};
pub use flat::FlatConfig;

pub use keys::logging;

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

/// Client configuration.
#[derive(Clone, Debug, Default)]
pub struct ClientConfig {
    /// Underlying flat configuration.
    pub inner: FlatConfig,
}

impl ClientConfig {
    /// Load client configuration from a file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, CommonError> {
        let default = Self::default();
        let config = load_merged(default.inner, Some(path.as_ref()))?;
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

pub fn load_client_config<P: AsRef<Path>>(path: P) -> Result<ClientConfig, CommonError> {
    ClientConfig::load(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn server_config_preserves_flat_and_structured_values() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("server.yaml");
        fs::write(
            &config_path,
            r#"
beryl.metadata.rpc.port: 18081
beryl.worker.storage.dirs:
  data:
    path: /var/lib/beryl
    tier: hdd
    capacity: 1GiB
"#,
        )
        .unwrap();

        let config = ServerConfig::load(&config_path).unwrap();
        assert_eq!(config.inner.get_i64("beryl.metadata.rpc.port"), Some(18081));
        assert!(config.inner.get_mapping("beryl.worker.storage.dirs").is_some());
    }

    #[test]
    fn client_config_preserves_client_values() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("client.yaml");
        fs::write(
            &config_path,
            r#"
beryl.client.metadata.addresses:
  - 127.0.0.1:18080
beryl.client.request.timeout: 30s
"#,
        )
        .unwrap();

        let config = ClientConfig::load(&config_path).unwrap();
        assert_eq!(
            config.inner.get_string_list("beryl.client.metadata.addresses"),
            Some(vec!["127.0.0.1:18080".to_string()])
        );
        assert_eq!(
            config.inner.get_duration("beryl.client.request.timeout"),
            Some(std::time::Duration::from_secs(30))
        );
    }
}
