// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client configuration loading and validation.

use common::{ClientConfig as CommonClientConfig, CommonError, FlatConfig};
use std::path::Path;
use std::time::Duration;
use types::GroupName;

pub const DEFAULT_CLIENT_NAME: &str = "default_client";

/// Client-specific configuration.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Underlying common client config.
    pub inner: CommonClientConfig,
    /// Low-cardinality display identity carried in request headers.
    pub client_name: String,
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
    /// Configured metadata owner groups used as bootstrap targets.
    pub metadata_group_names: Vec<GroupName>,
}

/// Cache configuration.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Enable metadata-authoritative worker endpoint cache reuse.
    pub worker_endpoint_cache_enabled: bool,
    /// TTL for worker endpoint cache entries.
    pub worker_endpoint_cache_ttl: Duration,
    /// Maximum number of worker endpoint cache entries.
    pub worker_endpoint_cache_max_entries: usize,
    /// Enable temporary endpoint health penalties after repeated failures.
    pub endpoint_health_enabled: bool,
    /// Consecutive failure threshold before an endpoint is temporarily unhealthy.
    pub endpoint_health_failure_threshold: usize,
    /// Temporary endpoint health penalty TTL.
    pub endpoint_health_ttl: Duration,
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
    /// Maximum retry attempts per logical operation.
    pub max_retry_attempts: usize,
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
        self.max_retry_attempts
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retry_attempts: 3,
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
        let client_name = client_name_from_flat(&flat)?;
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

        let metadata_group_names = parse_metadata_group_names(&flat)?;

        Ok(Self {
            inner: CommonClientConfig::from_flat(flat),
            client_name,
            cache,
            retry,
            refresh,
            backoff,
            channel_pool,
            metadata_endpoints,
            metadata_group_names,
        })
    }

    /// Get the underlying CommonClientConfig.
    pub fn as_common(&self) -> &CommonClientConfig {
        &self.inner
    }

    /// Return the low-cardinality display identity used in request headers.
    pub fn client_name(&self) -> &str {
        &self.client_name
    }
}

fn client_name_from_flat(flat: &FlatConfig) -> Result<String, CommonError> {
    if !flat.contains_key("client.name") {
        return Ok(DEFAULT_CLIENT_NAME.to_string());
    }
    let name = flat
        .get_str("client.name")
        .ok_or_else(|| invalid_config("client.name", "must be a string"))?;
    if name.trim().is_empty() {
        return Err(invalid_config("client.name", "must not be blank"));
    }
    Ok(name)
}

fn cache_config_from_flat(flat: &FlatConfig) -> Result<CacheConfig, CommonError> {
    let worker_endpoint_cache_enabled = get_bool_or(flat, "client.cache.worker_endpoint.enabled", false)?;
    let worker_endpoint_cache_ttl =
        Duration::from_secs(get_u64_or_strict(flat, "client.cache.worker_endpoint.ttl_secs", 0)?);
    let worker_endpoint_cache_max_entries =
        get_usize_or_strict(flat, "client.cache.worker_endpoint.max_entries", 1024)?;
    let endpoint_health_enabled = get_bool_or(flat, "client.cache.worker_endpoint.health.enabled", true)?;
    let endpoint_health_failure_threshold =
        get_usize_or_strict(flat, "client.cache.worker_endpoint.health.failure_threshold", 2)?;
    let endpoint_health_ttl = Duration::from_secs(get_u64_or_strict(
        flat,
        "client.cache.worker_endpoint.health.ttl_secs",
        5,
    )?);

    if worker_endpoint_cache_enabled && worker_endpoint_cache_max_entries == 0 {
        return Err(invalid_config(
            "client.cache.worker_endpoint.max_entries",
            "must be greater than zero when worker endpoint cache is enabled",
        ));
    }
    if endpoint_health_enabled && endpoint_health_failure_threshold == 0 {
        return Err(invalid_config(
            "client.cache.worker_endpoint.health.failure_threshold",
            "must be greater than zero when endpoint health is enabled",
        ));
    }

    Ok(CacheConfig {
        worker_endpoint_cache_enabled,
        worker_endpoint_cache_ttl,
        worker_endpoint_cache_max_entries,
        endpoint_health_enabled,
        endpoint_health_failure_threshold,
        endpoint_health_ttl,
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

fn parse_metadata_group_names(flat: &FlatConfig) -> Result<Vec<GroupName>, CommonError> {
    if flat.contains_key("client.metadata.group_ids") {
        return Err(CommonError::new(
            common::CommonErrorCode::InvalidArgument,
            "client.metadata.group_ids is unsupported; use client.metadata.group.names",
        ));
    }
    let groups = if let Some(groups_str) = flat.get_str("client.metadata.group.names") {
        groups_str
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(GroupName::parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                CommonError::new(
                    common::CommonErrorCode::InvalidArgument,
                    format!("invalid client.metadata.group.names: {err}"),
                )
            })?
    } else {
        vec![GroupName::parse("root").expect("default group name is valid")]
    };

    if groups.is_empty() {
        return Err(CommonError::new(
            common::CommonErrorCode::InvalidArgument,
            "client.metadata.group.names must contain at least one group name",
        ));
    }
    Ok(groups)
}

fn retry_config_from_flat(flat: &FlatConfig) -> Result<RetryConfig, CommonError> {
    let defaults = RetryConfig::default();
    let max_retry_attempts = get_usize_or_strict(flat, "client.retry.max_retry_attempts", defaults.max_retry_attempts)?;
    let operation_timeout_ms = get_optional_u64(flat, "client.operation.timeout_ms")?;
    Ok(RetryConfig {
        max_retry_attempts,
        operation_timeout_ms,
    })
}

fn refresh_config_from_flat(flat: &FlatConfig) -> Result<RefreshConfig, CommonError> {
    Ok(RefreshConfig {
        max_refresh_attempts: get_usize_or_strict(
            flat,
            "client.refresh.max_attempts",
            RefreshConfig::default().max_refresh_attempts,
        )?,
    })
}

fn backoff_config_from_flat(flat: &FlatConfig) -> Result<BackoffConfig, CommonError> {
    let defaults = BackoffConfig::default();
    let backoff = BackoffConfig {
        initial_backoff_ms: get_u64_or_strict(flat, "client.backoff.initial_ms", defaults.initial_backoff_ms)?,
        max_backoff_ms: get_u64_or_strict(flat, "client.backoff.max_ms", defaults.max_backoff_ms)?,
        backoff_multiplier: get_f64_or(flat, "client.backoff.multiplier", defaults.backoff_multiplier)?,
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

fn get_usize_or_strict(flat: &FlatConfig, key: &'static str, default: usize) -> Result<usize, CommonError> {
    match get_i64_or_strict(flat, key)? {
        Some(value) if value >= 0 => Ok(value as usize),
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
    match get_i64_or_strict(flat, key)? {
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

fn invalid_config(key: &'static str, detail: impl Into<String>) -> CommonError {
    CommonError::new(
        common::CommonErrorCode::InvalidArgument,
        format!("{key} {}", detail.into()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_conservative_bounded_retry_refresh_and_backoff() {
        let config = ClientConfig::default();

        assert_eq!(config.retry.max_retry_attempts(), 3);
        assert_eq!(config.refresh.max_refresh_attempts, 3);
        assert_eq!(config.backoff.initial_backoff_ms, 100);
        assert_eq!(config.backoff.max_backoff_ms, 5000);
        assert!(config.backoff.backoff_multiplier >= 1.0);
    }

    #[test]
    fn default_config_has_bounded_cache_and_pool_settings() {
        let config = ClientConfig::default();

        assert!(!config.cache.worker_endpoint_cache_enabled);
        assert_eq!(config.cache.worker_endpoint_cache_max_entries, 1024);
        assert!(config.cache.worker_endpoint_cache_ttl.is_zero());
        assert!(config.cache.endpoint_health_enabled);
        assert_eq!(config.cache.endpoint_health_failure_threshold, 2);
        assert_eq!(config.cache.endpoint_health_ttl, std::time::Duration::from_secs(5));
        assert!(config.channel_pool.metadata_channel_pool_enabled);
        assert_eq!(config.channel_pool.metadata_channel_pool_max_per_group, 1);
        assert!(config.channel_pool.worker_channel_pool_enabled);
        assert_eq!(config.channel_pool.worker_channel_pool_max_per_worker, 1);
    }

    #[test]
    fn removed_numeric_metadata_group_config_is_rejected() {
        let mut flat = FlatConfig::new();
        flat.set("client.metadata.group_ids", "-1");
        let err = ClientConfig::from_flat(flat).expect_err("numeric metadata group config must fail");
        assert!(err.to_string().contains("client.metadata.group_ids"));
    }

    #[test]
    fn client_name_defaults_preserves_nonblank_and_rejects_blank() {
        assert_eq!(ClientConfig::default().client_name(), DEFAULT_CLIENT_NAME);

        let mut named = FlatConfig::new();
        named.set("client.name", " prod_ns01 ");
        let config = ClientConfig::from_flat(named).expect("client name config");

        assert_eq!(config.client_name(), " prod_ns01 ");

        let mut blank = FlatConfig::new();
        blank.set("client.name", "   ");
        let err = ClientConfig::from_flat(blank).expect_err("blank client name must fail");

        assert!(err.to_string().contains("client.name"));
    }

    #[test]
    fn cache_and_pool_config_is_loaded_from_flat_config() {
        let mut flat = FlatConfig::new();
        flat.set("client.cache.worker_endpoint.enabled", true);
        flat.set("client.cache.worker_endpoint.ttl_secs", 45i64);
        flat.set("client.cache.worker_endpoint.max_entries", 9i64);
        flat.set("client.cache.worker_endpoint.health.enabled", false);
        flat.set("client.cache.worker_endpoint.health.failure_threshold", 4i64);
        flat.set("client.cache.worker_endpoint.health.ttl_secs", 12i64);
        flat.set("client.channel_pool.metadata.enabled", false);
        flat.set("client.channel_pool.metadata.max_per_group", 2i64);
        flat.set("client.channel_pool.worker.enabled", false);
        flat.set("client.channel_pool.worker.max_per_worker", 3i64);

        let config = ClientConfig::from_flat(flat).expect("cache and pool config");

        assert!(config.cache.worker_endpoint_cache_enabled);
        assert_eq!(
            config.cache.worker_endpoint_cache_ttl,
            std::time::Duration::from_secs(45)
        );
        assert_eq!(config.cache.worker_endpoint_cache_max_entries, 9);
        assert!(!config.cache.endpoint_health_enabled);
        assert_eq!(config.cache.endpoint_health_failure_threshold, 4);
        assert_eq!(config.cache.endpoint_health_ttl, std::time::Duration::from_secs(12));
        assert!(!config.channel_pool.metadata_channel_pool_enabled);
        assert_eq!(config.channel_pool.metadata_channel_pool_max_per_group, 2);
        assert!(!config.channel_pool.worker_channel_pool_enabled);
        assert_eq!(config.channel_pool.worker_channel_pool_max_per_worker, 3);
    }

    #[test]
    fn zero_worker_endpoint_cache_ttl_is_explicit_and_forces_miss_behavior() {
        let mut flat = FlatConfig::new();
        flat.set("client.cache.worker_endpoint.enabled", true);
        flat.set("client.cache.worker_endpoint.ttl_secs", 0i64);
        flat.set("client.cache.worker_endpoint.max_entries", 4i64);

        let config = ClientConfig::from_flat(flat).expect("zero ttl is valid");

        assert!(config.cache.worker_endpoint_cache_enabled);
        assert!(config.cache.worker_endpoint_cache_ttl.is_zero());
        assert_eq!(config.cache.worker_endpoint_cache_max_entries, 4);
    }

    #[test]
    fn invalid_cache_and_pool_config_is_rejected() {
        for (key, value) in [
            ("client.cache.worker_endpoint.ttl_secs", -1i64),
            ("client.channel_pool.metadata.max_per_group", 0i64),
            ("client.channel_pool.worker.max_per_worker", 0i64),
        ] {
            let mut flat = FlatConfig::new();
            flat.set(key, value);

            let err = ClientConfig::from_flat(flat).expect_err("invalid cache or pool config must fail");

            assert!(format!("{err}").contains(key));
        }

        let mut flat = FlatConfig::new();
        flat.set("client.cache.worker_endpoint.enabled", true);
        flat.set("client.cache.worker_endpoint.max_entries", 0i64);

        let err = ClientConfig::from_flat(flat).expect_err("enabled cache must be bounded");

        assert!(format!("{err}").contains("client.cache.worker_endpoint.max_entries"));

        let mut flat = FlatConfig::new();
        flat.set("client.cache.worker_endpoint.enabled", "sometimes");
        let err = ClientConfig::from_flat(flat).expect_err("invalid bool must fail");
        assert!(format!("{err}").contains("client.cache.worker_endpoint.enabled"));
    }

    #[test]
    fn zero_retry_is_valid_and_disables_retry_attempts() {
        let mut flat = FlatConfig::new();
        flat.set("client.retry.max_retry_attempts", 0i64);

        let config = ClientConfig::from_flat(flat).expect("zero retry config is valid");

        assert_eq!(config.retry.max_retry_attempts(), 0);
    }

    #[test]
    fn current_retry_refresh_and_backoff_keys_are_loaded_from_flat_config() {
        let mut flat = FlatConfig::new();
        flat.set("client.retry.max_retry_attempts", 5i64);
        flat.set("client.refresh.max_attempts", 6i64);
        flat.set("client.operation.timeout_ms", 7000i64);
        flat.set("client.backoff.initial_ms", 25i64);
        flat.set("client.backoff.max_ms", 400i64);
        flat.set("client.backoff.multiplier", "1.5");

        let config = ClientConfig::from_flat(flat).expect("current retry and backoff config");

        assert_eq!(config.retry.max_retry_attempts(), 5);
        assert_eq!(config.retry.operation_timeout_ms, Some(7000));
        assert_eq!(config.refresh.max_refresh_attempts, 6);
        assert_eq!(config.backoff.initial_backoff_ms, 25);
        assert_eq!(config.backoff.max_backoff_ms, 400);
        assert_eq!(config.backoff.backoff_multiplier, 1.5);
    }

    #[test]
    fn metadata_group_names_parse_comma_separated_values() {
        let mut flat = FlatConfig::new();
        flat.set("client.metadata.group.names", "root,analytics,tenant-a");

        let config = ClientConfig::from_flat(flat).expect("metadata group names config");

        assert_eq!(
            config.metadata_group_names,
            vec![
                GroupName::parse("root").unwrap(),
                GroupName::parse("analytics").unwrap(),
                GroupName::parse("tenant-a").unwrap()
            ]
        );
    }

    #[test]
    fn invalid_retry_and_backoff_config_is_rejected() {
        for (key, value) in [
            ("client.retry.max_retry_attempts", -1i64),
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
}
