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
