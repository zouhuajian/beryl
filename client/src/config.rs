// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client configuration loading and validation.

use common::{ClientConfig as CommonClientConfig, CommonError, FlatConfig};
use std::path::Path;

// Timeout/backpressure settings can be configured via client.rpc.* if needed (TODO).

/// Client-specific configuration.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Underlying common client config.
    pub inner: CommonClientConfig,
    /// Default consistency level.
    pub default_consistency: ConsistencyLevel,
    /// Default read mode.
    pub default_read_mode: ReadMode,
    /// Default write mode.
    pub default_write_mode: WriteMode,
    /// Read mode fallback strategy.
    pub read_mode_fallback: ReadModeFallback,
    /// Write mode fallback strategy.
    pub write_mode_fallback: WriteModeFallback,
    /// Cache configuration.
    pub cache: CacheConfig,
    /// Retry configuration.
    pub retry: RetryConfig,
    /// Metadata endpoints.
    pub metadata_endpoints: Vec<String>,
}

/// Consistency level (re-exported from consistency module).
pub use crate::consistency::ConsistencyLevel;

/// Read mode (re-exported from modes module).
pub use crate::modes::ReadMode;

/// Write mode (re-exported from modes module).
pub use crate::modes::WriteMode;

/// Read mode fallback strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadModeFallback {
    /// Fallback to direct read (skip cache).
    Direct,
    /// Disable read mode (use default path).
    Disable,
}

/// Write mode fallback strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteModeFallback {
    /// Fallback to write-through.
    Through,
    /// Fallback to direct-to-UFS.
    Direct,
    /// Disable write mode (use default path).
    Disable,
}

/// Cache configuration.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Maximum number of file metadata entries.
    pub max_file_meta_entries: usize,
    /// Maximum memory for file metadata cache (bytes).
    pub max_file_meta_bytes: Option<usize>,
    /// TTL for file metadata cache (seconds).
    pub file_meta_ttl_secs: u64,
    /// Maximum number of route table entries.
    pub max_route_entries: usize,
    /// TTL for route table cache (seconds).
    pub route_ttl_secs: u64,
}

/// Retry configuration.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retries.
    pub max_retries: usize,
    /// Initial backoff delay (milliseconds).
    pub initial_backoff_ms: u64,
    /// Maximum backoff delay (milliseconds).
    pub max_backoff_ms: u64,
    /// Backoff multiplier.
    pub backoff_multiplier: f64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        let flat = FlatConfig::new();
        Self::from_flat(flat).unwrap()
    }
}

impl ClientConfig {
    /// Load client configuration from a file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, CommonError> {
        let common_config = CommonClientConfig::load(path)?;
        Self::from_common_config(common_config)
    }

    /// Create from CommonClientConfig.
    pub fn from_common_config(common_config: CommonClientConfig) -> Result<Self, CommonError> {
        Self::from_flat(common_config.inner.clone())
    }

    /// Create from FlatConfig.
    pub fn from_flat(flat: FlatConfig) -> Result<Self, CommonError> {
        // Default consistency level
        let default_consistency = flat
            .get_str("client.consistency.default")
            .and_then(|s| s.parse().ok())
            .unwrap_or(ConsistencyLevel::Normal);

        // Default read mode
        let default_read_mode = flat
            .get_str("client.read_mode.default")
            .and_then(|s| s.parse().ok())
            .unwrap_or(ReadMode::Cached);

        // Default write mode
        let default_write_mode = flat
            .get_str("client.write_mode.default")
            .and_then(|s| s.parse().ok())
            .unwrap_or(WriteMode::Back);

        // Read mode fallback
        let read_mode_fallback = flat
            .get_str("client.read_mode.fallback")
            .and_then(|s| match s.as_str() {
                "direct" => Some(ReadModeFallback::Direct),
                "disable" => Some(ReadModeFallback::Disable),
                _ => None,
            })
            .unwrap_or(ReadModeFallback::Direct);

        // Write mode fallback
        let write_mode_fallback = flat
            .get_str("client.write_mode.fallback")
            .and_then(|s| match s.as_str() {
                "through" => Some(WriteModeFallback::Through),
                "direct" => Some(WriteModeFallback::Direct),
                "disable" => Some(WriteModeFallback::Disable),
                _ => None,
            })
            .unwrap_or(WriteModeFallback::Through);

        // Cache config
        let cache = CacheConfig {
            max_file_meta_entries: flat.get_i64("client.cache.file_meta.max_entries").unwrap_or(10000) as usize,
            max_file_meta_bytes: flat.get_i64("client.cache.file_meta.max_bytes").map(|v| v as usize),
            file_meta_ttl_secs: flat.get_i64("client.cache.file_meta.ttl_secs").unwrap_or(300) as u64,
            max_route_entries: flat.get_i64("client.cache.route.max_entries").unwrap_or(1000) as usize,
            route_ttl_secs: flat.get_i64("client.cache.route.ttl_secs").unwrap_or(60) as u64,
        };

        // Retry config
        let retry = RetryConfig {
            max_retries: flat.get_i64("client.retry.max_retries").unwrap_or(3) as usize,
            initial_backoff_ms: flat.get_i64("client.retry.initial_backoff_ms").unwrap_or(100) as u64,
            max_backoff_ms: flat.get_i64("client.retry.max_backoff_ms").unwrap_or(5000) as u64,
            backoff_multiplier: flat.get_f64("client.retry.backoff_multiplier").unwrap_or(2.0),
        };

        // Metadata endpoints
        let metadata_endpoints = if let Some(endpoints_str) = flat.get_str("client.metadata.endpoints") {
            endpoints_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            vec!["127.0.0.1:18080".to_string()]
        };

        // Worker net protocol is determined from metadata worker information.
        // If client needs timeout/backpressure settings, they can be configured
        // via client.rpc.* (TODO: implement if needed).

        Ok(Self {
            inner: CommonClientConfig::from_flat(flat),
            default_consistency,
            default_read_mode,
            default_write_mode,
            read_mode_fallback,
            write_mode_fallback,
            cache,
            retry,
            metadata_endpoints,
        })
    }

    /// Get the underlying CommonClientConfig.
    pub fn as_common(&self) -> &CommonClientConfig {
        &self.inner
    }
}

// Helper trait for parsing config values
trait FlatConfigExt {
    fn get_f64(&self, key: &str) -> Option<f64>;
}

impl FlatConfigExt for FlatConfig {
    fn get_f64(&self, key: &str) -> Option<f64> {
        // Use get_i64 and convert to f64, or add a helper method
        self.get_i64(key).map(|v| v as f64)
    }
}
