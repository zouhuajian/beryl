// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client configuration loading and validation.

use common::{ClientConfig as CommonClientConfig, CommonError, FlatConfig};
use std::path::Path;
use std::time::Duration;

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
    /// Refresh configuration.
    pub refresh: RefreshConfig,
    /// Backoff configuration.
    pub backoff: BackoffConfig,
    /// Channel/client pool configuration.
    pub channel_pool: ChannelPoolConfig,
    /// Metadata endpoints.
    pub metadata_endpoints: Vec<String>,
    /// Configured metadata owner groups used as non-zero bootstrap targets.
    pub metadata_group_ids: Vec<u64>,
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
    /// Enable validated read-layout cache reuse.
    pub layout_cache_enabled: bool,
    /// TTL for read-layout cache entries.
    pub layout_cache_ttl: Duration,
    /// Maximum number of read-layout cache entries.
    pub layout_cache_max_entries: usize,
    /// Enable metadata-authoritative worker endpoint cache reuse.
    pub worker_endpoint_cache_enabled: bool,
    /// TTL for worker endpoint cache entries.
    pub worker_endpoint_cache_ttl: Duration,
    /// Maximum number of worker endpoint cache entries.
    pub worker_endpoint_cache_max_entries: usize,
}

/// Channel/client pool configuration.
#[derive(Clone, Debug)]
pub struct ChannelPoolConfig {
    /// Enable metadata channel/client reuse.
    pub metadata_channel_pool_enabled: bool,
    /// Maximum cached metadata channels per group.
    pub metadata_channel_pool_max_per_group: usize,
    /// Enable worker channel/client reuse.
    pub worker_channel_pool_enabled: bool,
    /// Maximum cached worker channels per worker identity.
    pub worker_channel_pool_max_per_worker: usize,
}

/// Retry configuration.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Compatibility cap for maximum retries.
    pub max_retries: usize,
    /// Maximum retry attempts per logical operation.
    pub max_retry_attempts: usize,
    /// Metadata retry budget per logical operation.
    pub metadata_retry_budget: usize,
    /// Worker retry budget per logical operation.
    pub worker_retry_budget: usize,
    /// Session barrier retry budget per logical operation.
    pub session_barrier_retry_budget: usize,
    /// Optional per-operation timeout in milliseconds.
    pub operation_timeout_ms: Option<u64>,
}

/// Refresh configuration.
#[derive(Clone, Debug)]
pub struct RefreshConfig {
    /// Maximum refresh attempts per logical operation.
    pub max_refresh_attempts: usize,
}

/// Backoff configuration.
#[derive(Clone, Debug)]
pub struct BackoffConfig {
    /// Initial backoff delay in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum backoff delay in milliseconds.
    pub max_backoff_ms: u64,
    /// Multiplicative backoff factor.
    pub backoff_multiplier: f64,
}

impl RetryConfig {
    /// Return the effective maximum retry attempt cap.
    pub fn max_retry_attempts(&self) -> usize {
        self.max_retry_attempts.min(self.max_retries)
    }

    /// Return the effective metadata retry budget.
    pub fn metadata_retry_budget(&self) -> usize {
        self.metadata_retry_budget.min(self.max_retry_attempts())
    }

    /// Return the effective worker retry budget.
    pub fn worker_retry_budget(&self) -> usize {
        self.worker_retry_budget.min(self.max_retry_attempts())
    }

    /// Return the effective session barrier retry budget.
    pub fn session_barrier_retry_budget(&self) -> usize {
        self.session_barrier_retry_budget.min(self.max_retry_attempts())
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            max_retry_attempts: 3,
            metadata_retry_budget: 3,
            worker_retry_budget: 3,
            session_barrier_retry_budget: 0,
            operation_timeout_ms: None,
        }
    }
}

impl Default for RefreshConfig {
    fn default() -> Self {
        Self {
            max_refresh_attempts: 3,
        }
    }
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 100,
            max_backoff_ms: 5000,
            backoff_multiplier: 2.0,
        }
    }
}

impl Default for ChannelPoolConfig {
    fn default() -> Self {
        Self {
            metadata_channel_pool_enabled: true,
            metadata_channel_pool_max_per_group: 1,
            worker_channel_pool_enabled: true,
            worker_channel_pool_max_per_worker: 1,
        }
    }
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

        let cache = cache_config_from_flat(&flat)?;

        let retry = retry_config_from_flat(&flat)?;
        let refresh = refresh_config_from_flat(&flat)?;
        let backoff = backoff_config_from_flat(&flat)?;
        let channel_pool = channel_pool_config_from_flat(&flat)?;

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

        let metadata_group_ids = parse_metadata_group_ids(&flat)?;

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
            refresh,
            backoff,
            channel_pool,
            metadata_endpoints,
            metadata_group_ids,
        })
    }

    /// Get the underlying CommonClientConfig.
    pub fn as_common(&self) -> &CommonClientConfig {
        &self.inner
    }

    /// Return the configured non-zero client id for request headers.
    pub fn client_id(&self) -> crate::error::ClientResult<types::ClientId> {
        let id = self.inner.as_flat().get_i64("client.id").unwrap_or(0) as u64;
        if id == 0 {
            Err(crate::error::ClientError::InvalidArgument(
                "client metadata operations require non-zero client.id".to_string(),
            ))
        } else {
            Ok(types::ClientId::new(id))
        }
    }
}

fn cache_config_from_flat(flat: &FlatConfig) -> Result<CacheConfig, CommonError> {
    let layout_cache_enabled = get_bool_or(flat, "client.cache.layout.enabled", false)?;
    let layout_cache_ttl = Duration::from_secs(get_u64_or_strict(flat, "client.cache.layout.ttl_secs", 0)?);
    let layout_cache_max_entries = get_usize_or_strict(flat, "client.cache.layout.max_entries", 1024)?;
    let worker_endpoint_cache_enabled = get_bool_or(flat, "client.cache.worker_endpoint.enabled", false)?;
    let worker_endpoint_cache_ttl =
        Duration::from_secs(get_u64_or_strict(flat, "client.cache.worker_endpoint.ttl_secs", 0)?);
    let worker_endpoint_cache_max_entries =
        get_usize_or_strict(flat, "client.cache.worker_endpoint.max_entries", 1024)?;

    if layout_cache_enabled && layout_cache_max_entries == 0 {
        return Err(invalid_config(
            "client.cache.layout.max_entries",
            "must be greater than zero when layout cache is enabled",
        ));
    }
    if worker_endpoint_cache_enabled && worker_endpoint_cache_max_entries == 0 {
        return Err(invalid_config(
            "client.cache.worker_endpoint.max_entries",
            "must be greater than zero when worker endpoint cache is enabled",
        ));
    }

    Ok(CacheConfig {
        max_file_meta_entries: get_usize_or_strict(flat, "client.cache.file_meta.max_entries", 10000)?,
        max_file_meta_bytes: get_optional_usize(flat, "client.cache.file_meta.max_bytes")?,
        file_meta_ttl_secs: get_u64_or_strict(flat, "client.cache.file_meta.ttl_secs", 300)?,
        max_route_entries: get_usize_or_strict(flat, "client.cache.route.max_entries", 1000)?,
        route_ttl_secs: get_u64_or_strict(flat, "client.cache.route.ttl_secs", 60)?,
        layout_cache_enabled,
        layout_cache_ttl,
        layout_cache_max_entries,
        worker_endpoint_cache_enabled,
        worker_endpoint_cache_ttl,
        worker_endpoint_cache_max_entries,
    })
}

fn channel_pool_config_from_flat(flat: &FlatConfig) -> Result<ChannelPoolConfig, CommonError> {
    let config = ChannelPoolConfig {
        metadata_channel_pool_enabled: get_bool_or(
            flat,
            "client.channel_pool.metadata.enabled",
            ChannelPoolConfig::default().metadata_channel_pool_enabled,
        )?,
        metadata_channel_pool_max_per_group: get_usize_or_strict(
            flat,
            "client.channel_pool.metadata.max_per_group",
            ChannelPoolConfig::default().metadata_channel_pool_max_per_group,
        )?,
        worker_channel_pool_enabled: get_bool_or(
            flat,
            "client.channel_pool.worker.enabled",
            ChannelPoolConfig::default().worker_channel_pool_enabled,
        )?,
        worker_channel_pool_max_per_worker: get_usize_or_strict(
            flat,
            "client.channel_pool.worker.max_per_worker",
            ChannelPoolConfig::default().worker_channel_pool_max_per_worker,
        )?,
    };
    if config.metadata_channel_pool_max_per_group == 0 {
        return Err(invalid_config(
            "client.channel_pool.metadata.max_per_group",
            "must be greater than zero",
        ));
    }
    if config.worker_channel_pool_max_per_worker == 0 {
        return Err(invalid_config(
            "client.channel_pool.worker.max_per_worker",
            "must be greater than zero",
        ));
    }
    Ok(config)
}

fn parse_metadata_group_ids(flat: &FlatConfig) -> Result<Vec<u64>, CommonError> {
    let groups = if let Some(groups_str) = flat.get_str("client.metadata.group_ids") {
        let parsed = groups_str
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.parse::<u64>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                CommonError::new(
                    common::CommonErrorCode::InvalidArgument,
                    format!("invalid client.metadata.group_ids: {err}"),
                )
            })?;
        parsed
    } else if let Some(group_id) = flat.get_i64("client.metadata.group_id") {
        vec![group_id as u64]
    } else {
        vec![1]
    };

    if groups.is_empty() || groups.contains(&0) {
        return Err(CommonError::new(
            common::CommonErrorCode::InvalidArgument,
            "client.metadata.group_ids must contain non-zero group ids",
        ));
    }
    Ok(groups)
}

fn retry_config_from_flat(flat: &FlatConfig) -> Result<RetryConfig, CommonError> {
    let defaults = RetryConfig::default();
    let max_retry_attempts = get_usize_with_legacy(
        flat,
        "client.retry.max_retry_attempts",
        "client.retry.max_retries",
        defaults.max_retry_attempts,
    )?;
    let metadata_retry_budget = get_usize_or(flat, "client.retry.metadata_budget", max_retry_attempts)?;
    let worker_retry_budget = get_usize_or(flat, "client.retry.worker_budget", max_retry_attempts)?;
    let session_barrier_retry_budget = get_usize_or(
        flat,
        "client.retry.session_barrier_budget",
        defaults.session_barrier_retry_budget.min(max_retry_attempts),
    )?;
    let operation_timeout_ms = get_optional_u64(flat, "client.operation.timeout_ms")?;
    Ok(RetryConfig {
        max_retries: max_retry_attempts,
        max_retry_attempts,
        metadata_retry_budget,
        worker_retry_budget,
        session_barrier_retry_budget,
        operation_timeout_ms,
    })
}

fn refresh_config_from_flat(flat: &FlatConfig) -> Result<RefreshConfig, CommonError> {
    Ok(RefreshConfig {
        max_refresh_attempts: get_usize_or(
            flat,
            "client.refresh.max_attempts",
            RefreshConfig::default().max_refresh_attempts,
        )?,
    })
}

fn backoff_config_from_flat(flat: &FlatConfig) -> Result<BackoffConfig, CommonError> {
    let defaults = BackoffConfig::default();
    let backoff = BackoffConfig {
        initial_backoff_ms: get_u64_with_legacy(
            flat,
            "client.backoff.initial_ms",
            "client.retry.initial_backoff_ms",
            defaults.initial_backoff_ms,
        )?,
        max_backoff_ms: get_u64_with_legacy(
            flat,
            "client.backoff.max_ms",
            "client.retry.max_backoff_ms",
            defaults.max_backoff_ms,
        )?,
        backoff_multiplier: get_f64_with_legacy(
            flat,
            "client.backoff.multiplier",
            "client.retry.backoff_multiplier",
            defaults.backoff_multiplier,
        )?,
    };
    if backoff.max_backoff_ms < backoff.initial_backoff_ms {
        return Err(invalid_config(
            "client.backoff.max_ms",
            "must be greater than or equal to client.backoff.initial_ms",
        ));
    }
    if !backoff.backoff_multiplier.is_finite() || backoff.backoff_multiplier < 1.0 {
        return Err(invalid_config(
            "client.backoff.multiplier",
            "must be finite and greater than or equal to 1.0",
        ));
    }
    Ok(backoff)
}

fn get_usize_with_legacy(
    flat: &FlatConfig,
    key: &'static str,
    legacy_key: &'static str,
    default: usize,
) -> Result<usize, CommonError> {
    if flat.get_str(key).is_some() {
        return get_usize_or(flat, key, default);
    }
    get_usize_or(flat, legacy_key, default)
}

fn get_u64_with_legacy(
    flat: &FlatConfig,
    key: &'static str,
    legacy_key: &'static str,
    default: u64,
) -> Result<u64, CommonError> {
    if flat.get_str(key).is_some() {
        return get_u64_or(flat, key, default);
    }
    get_u64_or(flat, legacy_key, default)
}

fn get_f64_with_legacy(
    flat: &FlatConfig,
    key: &'static str,
    legacy_key: &'static str,
    default: f64,
) -> Result<f64, CommonError> {
    if flat.get_str(key).is_some() {
        return get_f64_or(flat, key, default);
    }
    get_f64_or(flat, legacy_key, default)
}

fn get_usize_or(flat: &FlatConfig, key: &'static str, default: usize) -> Result<usize, CommonError> {
    match flat.get_i64(key) {
        Some(value) if value >= 0 => Ok(value as usize),
        Some(_) => Err(invalid_config(key, "must be non-negative")),
        None => Ok(default),
    }
}

fn get_usize_or_strict(flat: &FlatConfig, key: &'static str, default: usize) -> Result<usize, CommonError> {
    match get_i64_or_strict(flat, key)? {
        Some(value) if value >= 0 => Ok(value as usize),
        Some(_) => Err(invalid_config(key, "must be non-negative")),
        None => Ok(default),
    }
}

fn get_u64_or(flat: &FlatConfig, key: &'static str, default: u64) -> Result<u64, CommonError> {
    match flat.get_i64(key) {
        Some(value) if value >= 0 => Ok(value as u64),
        Some(_) => Err(invalid_config(key, "must be non-negative")),
        None => Ok(default),
    }
}

fn get_u64_or_strict(flat: &FlatConfig, key: &'static str, default: u64) -> Result<u64, CommonError> {
    match get_i64_or_strict(flat, key)? {
        Some(value) if value >= 0 => Ok(value as u64),
        Some(_) => Err(invalid_config(key, "must be non-negative")),
        None => Ok(default),
    }
}

fn get_optional_usize(flat: &FlatConfig, key: &'static str) -> Result<Option<usize>, CommonError> {
    match get_i64_or_strict(flat, key)? {
        Some(value) if value >= 0 => Ok(Some(value as usize)),
        Some(_) => Err(invalid_config(key, "must be non-negative")),
        None => Ok(None),
    }
}

fn get_i64_or_strict(flat: &FlatConfig, key: &'static str) -> Result<Option<i64>, CommonError> {
    if let Some(value) = flat.get_i64(key) {
        return Ok(Some(value));
    }
    if flat.get_str(key).is_some() {
        return Err(invalid_config(key, "must be an integer"));
    }
    Ok(None)
}

fn get_optional_u64(flat: &FlatConfig, key: &'static str) -> Result<Option<u64>, CommonError> {
    match flat.get_i64(key) {
        Some(value) if value >= 0 => Ok(Some(value as u64)),
        Some(_) => Err(invalid_config(key, "must be non-negative")),
        None => Ok(None),
    }
}

fn get_bool_or(flat: &FlatConfig, key: &'static str, default: bool) -> Result<bool, CommonError> {
    if let Some(value) = flat.get_bool(key) {
        return Ok(value);
    }
    if flat.get_str(key).is_some() {
        return Err(invalid_config(key, "must be a boolean"));
    }
    Ok(default)
}

fn get_f64_or(flat: &FlatConfig, key: &'static str, default: f64) -> Result<f64, CommonError> {
    match flat.get_str(key) {
        Some(value) => value
            .parse::<f64>()
            .map_err(|_| invalid_config(key, "must be a number")),
        None => Ok(default),
    }
}

fn invalid_config(key: &'static str, detail: &'static str) -> CommonError {
    CommonError::new(common::CommonErrorCode::InvalidArgument, format!("{key} {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_conservative_bounded_retry_refresh_and_backoff() {
        let config = ClientConfig::default();

        assert_eq!(config.retry.max_retry_attempts(), 3);
        assert_eq!(config.retry.metadata_retry_budget(), 3);
        assert_eq!(config.retry.worker_retry_budget(), 3);
        assert_eq!(config.retry.session_barrier_retry_budget(), 0);
        assert_eq!(config.refresh.max_refresh_attempts, 3);
        assert_eq!(config.backoff.initial_backoff_ms, 100);
        assert_eq!(config.backoff.max_backoff_ms, 5000);
        assert!(config.backoff.backoff_multiplier >= 1.0);
    }

    #[test]
    fn default_config_has_bounded_cache_and_pool_settings() {
        let config = ClientConfig::default();

        assert!(!config.cache.layout_cache_enabled);
        assert_eq!(config.cache.layout_cache_max_entries, 1024);
        assert!(config.cache.layout_cache_ttl.is_zero());
        assert!(!config.cache.worker_endpoint_cache_enabled);
        assert_eq!(config.cache.worker_endpoint_cache_max_entries, 1024);
        assert!(config.cache.worker_endpoint_cache_ttl.is_zero());
        assert!(config.channel_pool.metadata_channel_pool_enabled);
        assert_eq!(config.channel_pool.metadata_channel_pool_max_per_group, 1);
        assert!(config.channel_pool.worker_channel_pool_enabled);
        assert_eq!(config.channel_pool.worker_channel_pool_max_per_worker, 1);
    }

    #[test]
    fn cache_and_pool_config_is_loaded_from_flat_config() {
        let mut flat = FlatConfig::new();
        flat.set("client.cache.layout.enabled", true);
        flat.set("client.cache.layout.ttl_secs", 30i64);
        flat.set("client.cache.layout.max_entries", 7i64);
        flat.set("client.cache.worker_endpoint.enabled", true);
        flat.set("client.cache.worker_endpoint.ttl_secs", 45i64);
        flat.set("client.cache.worker_endpoint.max_entries", 9i64);
        flat.set("client.channel_pool.metadata.enabled", false);
        flat.set("client.channel_pool.metadata.max_per_group", 2i64);
        flat.set("client.channel_pool.worker.enabled", false);
        flat.set("client.channel_pool.worker.max_per_worker", 3i64);

        let config = ClientConfig::from_flat(flat).expect("cache and pool config");

        assert!(config.cache.layout_cache_enabled);
        assert_eq!(config.cache.layout_cache_ttl, std::time::Duration::from_secs(30));
        assert_eq!(config.cache.layout_cache_max_entries, 7);
        assert!(config.cache.worker_endpoint_cache_enabled);
        assert_eq!(
            config.cache.worker_endpoint_cache_ttl,
            std::time::Duration::from_secs(45)
        );
        assert_eq!(config.cache.worker_endpoint_cache_max_entries, 9);
        assert!(!config.channel_pool.metadata_channel_pool_enabled);
        assert_eq!(config.channel_pool.metadata_channel_pool_max_per_group, 2);
        assert!(!config.channel_pool.worker_channel_pool_enabled);
        assert_eq!(config.channel_pool.worker_channel_pool_max_per_worker, 3);
    }

    #[test]
    fn zero_cache_ttl_is_explicit_and_forces_miss_behavior() {
        let mut flat = FlatConfig::new();
        flat.set("client.cache.layout.enabled", true);
        flat.set("client.cache.layout.ttl_secs", 0i64);
        flat.set("client.cache.layout.max_entries", 4i64);

        let config = ClientConfig::from_flat(flat).expect("zero ttl is valid");

        assert!(config.cache.layout_cache_enabled);
        assert!(config.cache.layout_cache_ttl.is_zero());
        assert_eq!(config.cache.layout_cache_max_entries, 4);
    }

    #[test]
    fn invalid_cache_and_pool_config_is_rejected() {
        for (key, value) in [
            ("client.cache.layout.ttl_secs", -1i64),
            ("client.cache.worker_endpoint.ttl_secs", -1i64),
            ("client.channel_pool.metadata.max_per_group", 0i64),
            ("client.channel_pool.worker.max_per_worker", 0i64),
        ] {
            let mut flat = FlatConfig::new();
            flat.set(key, value);

            let err = ClientConfig::from_flat(flat).expect_err("invalid cache or pool config must fail");

            assert!(format!("{err}").contains(key));
        }

        for (enabled_key, max_key) in [
            ("client.cache.layout.enabled", "client.cache.layout.max_entries"),
            (
                "client.cache.worker_endpoint.enabled",
                "client.cache.worker_endpoint.max_entries",
            ),
        ] {
            let mut flat = FlatConfig::new();
            flat.set(enabled_key, true);
            flat.set(max_key, 0i64);

            let err = ClientConfig::from_flat(flat).expect_err("enabled cache must be bounded");

            assert!(format!("{err}").contains(max_key));
        }

        let mut flat = FlatConfig::new();
        flat.set("client.cache.layout.enabled", "sometimes");
        let err = ClientConfig::from_flat(flat).expect_err("invalid bool must fail");
        assert!(format!("{err}").contains("client.cache.layout.enabled"));
    }

    #[test]
    fn zero_retry_is_valid_and_disables_effective_retry_budgets() {
        let mut flat = FlatConfig::new();
        flat.set("client.retry.max_retry_attempts", 0i64);
        flat.set("client.retry.metadata_budget", 0i64);
        flat.set("client.retry.worker_budget", 0i64);
        flat.set("client.retry.session_barrier_budget", 0i64);

        let config = ClientConfig::from_flat(flat).expect("zero retry config is valid");

        assert_eq!(config.retry.max_retry_attempts(), 0);
        assert_eq!(config.retry.metadata_retry_budget(), 0);
        assert_eq!(config.retry.worker_retry_budget(), 0);
        assert_eq!(config.retry.session_barrier_retry_budget(), 0);
    }

    #[test]
    fn invalid_retry_and_backoff_config_is_rejected() {
        for (key, value) in [
            ("client.retry.max_retry_attempts", -1i64),
            ("client.retry.metadata_budget", -1i64),
            ("client.retry.worker_budget", -1i64),
            ("client.retry.session_barrier_budget", -1i64),
            ("client.refresh.max_attempts", -1i64),
            ("client.backoff.initial_ms", -1i64),
            ("client.backoff.max_ms", -1i64),
        ] {
            let mut flat = FlatConfig::new();
            flat.set(key, value);

            let err = ClientConfig::from_flat(flat).expect_err("negative client budget must be rejected");

            assert!(
                err.to_string().contains(key),
                "error for {key} should mention the offending key: {err}"
            );
        }

        let mut flat = FlatConfig::new();
        flat.set("client.backoff.initial_ms", 1000i64);
        flat.set("client.backoff.max_ms", 100i64);
        let err = ClientConfig::from_flat(flat).expect_err("max backoff below initial must be rejected");
        assert!(err.to_string().contains("client.backoff.max_ms"));

        let mut flat = FlatConfig::new();
        flat.set("client.backoff.multiplier", 0i64);
        let err = ClientConfig::from_flat(flat).expect_err("backoff multiplier below one must be rejected");
        assert!(err.to_string().contains("client.backoff.multiplier"));

        let mut flat = FlatConfig::new();
        flat.set("client.backoff.multiplier", "not-a-number");
        let err = ClientConfig::from_flat(flat).expect_err("non-numeric backoff multiplier must be rejected");
        assert!(err.to_string().contains("client.backoff.multiplier"));
    }

    #[test]
    fn legacy_max_retries_key_still_populates_explicit_retry_budget() {
        let mut flat = FlatConfig::new();
        flat.set("client.retry.max_retries", 2i64);

        let config = ClientConfig::from_flat(flat).expect("legacy retry config");

        assert_eq!(config.retry.max_retries, 2);
        assert_eq!(config.retry.max_retry_attempts(), 2);
        assert_eq!(config.retry.metadata_retry_budget(), 2);
        assert_eq!(config.retry.worker_retry_budget(), 2);
    }
}
