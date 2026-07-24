// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client configuration loading and validation.

use beryl_common::{ClientConfig as CommonClientConfig, CommonError, FlatConfig};
use beryl_types::GroupName;
use std::path::Path;

pub const DEFAULT_CLIENT_NAME: &str = "default_client";
pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_WRITE_LEASE_RENEW_BEFORE_EXPIRY_MS: u64 = 30_000;
pub const DEFAULT_WORKER_ENDPOINT_COOLDOWN_MS: u64 = 1_000;

/// Client-specific configuration.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Underlying common client config.
    pub inner: CommonClientConfig,
    /// Low-cardinality display identity carried in request headers.
    pub client_name: String,
    /// Retry configuration.
    pub retry: RetryConfig,
    /// Client-side write lease renewal policy.
    pub write_lease: WriteLeaseConfig,
    /// Channel/client pool configuration.
    pub channel_pool: ChannelPoolConfig,
    /// Configured metadata owner groups and their bootstrap endpoints.
    pub metadata_groups: Vec<MetadataGroupConfig>,
}

/// Metadata group bootstrap endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataGroupConfig {
    /// Stable metadata group name.
    pub group_name: GroupName,
    /// Metadata endpoints configured for the group.
    pub endpoints: Vec<String>,
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
    /// Cooldown duration after transient worker endpoint failures.
    pub worker_endpoint_cooldown_ms: u64,
}

/// Retry configuration.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum attempts for one primary RPC, including the first.
    pub max_attempts: usize,
    /// Total public-operation timeout in milliseconds.
    pub operation_timeout_ms: u64,
}

/// Write lease renewal configuration.
#[derive(Clone, Debug)]
pub struct WriteLeaseConfig {
    /// Renew write leases automatically before side-effecting writer operations.
    pub auto_renew: bool,
    /// Renew when the current metadata lease expires within this many milliseconds.
    pub renew_before_expiry_ms: u64,
}

impl RetryConfig {
    /// Return the total primary RPC attempt cap, including the first attempt.
    pub fn max_attempts(&self) -> usize {
        self.max_attempts
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            operation_timeout_ms: DEFAULT_OPERATION_TIMEOUT_MS,
        }
    }
}

impl Default for WriteLeaseConfig {
    fn default() -> Self {
        Self {
            auto_renew: true,
            renew_before_expiry_ms: DEFAULT_WRITE_LEASE_RENEW_BEFORE_EXPIRY_MS,
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
            worker_endpoint_cooldown_ms: DEFAULT_WORKER_ENDPOINT_COOLDOWN_MS,
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

        let retry = retry_config_from_flat(&flat)?;
        let write_lease = write_lease_config_from_flat(&flat)?;
        let channel_pool = channel_pool_config_from_flat(&flat)?;

        let metadata_groups = parse_metadata_groups(&flat)?;

        Ok(Self {
            inner: CommonClientConfig::from_flat(flat),
            client_name,
            retry,
            write_lease,
            channel_pool,
            metadata_groups,
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
        worker_endpoint_cooldown_ms: get_u64_or_strict(
            flat,
            "client.channel_pool.worker.endpoint_cooldown_ms",
            ChannelPoolConfig::default().worker_endpoint_cooldown_ms,
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

fn parse_metadata_groups(flat: &FlatConfig) -> Result<Vec<MetadataGroupConfig>, CommonError> {
    let group_names = if let Some(groups_str) = flat.get_str("client.metadata.group.names") {
        let groups = groups_str
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(GroupName::parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                CommonError::new(
                    beryl_common::CommonErrorKind::InvalidArgument,
                    format!("invalid client.metadata.group.names: {err}"),
                )
            })?;
        if groups.is_empty() {
            return Err(CommonError::new(
                beryl_common::CommonErrorKind::InvalidArgument,
                "client.metadata.group.names must contain at least one group name",
            ));
        }
        groups
    } else {
        vec![GroupName::parse("root").expect("default group name is valid")]
    };

    let explicit_group_names = flat.get_str("client.metadata.group.names").is_some();
    group_names
        .into_iter()
        .map(|group_name| {
            let key = format!("client.metadata.group.{}.endpoints", group_name.as_str());
            let endpoints = match flat.get_str(&key) {
                Some(raw) => {
                    let endpoints = raw
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>();
                    if endpoints.is_empty() {
                        return Err(CommonError::new(
                            beryl_common::CommonErrorKind::InvalidArgument,
                            format!("{key} must be configured and non-empty"),
                        ));
                    }
                    endpoints
                }
                None if !explicit_group_names && group_name.as_str() == "root" => {
                    vec!["127.0.0.1:18080".to_string()]
                }
                None => {
                    return Err(CommonError::new(
                        beryl_common::CommonErrorKind::InvalidArgument,
                        format!("{key} must be configured and non-empty"),
                    ));
                }
            };
            Ok(MetadataGroupConfig { group_name, endpoints })
        })
        .collect()
}

fn retry_config_from_flat(flat: &FlatConfig) -> Result<RetryConfig, CommonError> {
    reject_removed_retry_keys(flat)?;
    let defaults = RetryConfig::default();
    let max_attempts = get_usize_or_strict(flat, "client.retry.max_attempts", defaults.max_attempts)?;
    if max_attempts == 0 {
        return Err(invalid_config("client.retry.max_attempts", "must be greater than zero"));
    }
    let operation_timeout_ms = get_u64_or_strict(flat, "client.operation.timeout_ms", defaults.operation_timeout_ms)?;
    if operation_timeout_ms == 0 {
        return Err(invalid_config(
            "client.operation.timeout_ms",
            "must be greater than zero",
        ));
    }
    Ok(RetryConfig {
        max_attempts,
        operation_timeout_ms,
    })
}

fn reject_removed_retry_keys(flat: &FlatConfig) -> Result<(), CommonError> {
    for key in [
        "client.retry.max_retry_attempts",
        "client.refresh.max_attempts",
        "client.backoff.initial_ms",
        "client.backoff.max_ms",
        "client.backoff.multiplier",
    ] {
        if flat.contains_key(key) {
            return Err(invalid_config(key, "is no longer supported"));
        }
    }
    Ok(())
}

fn write_lease_config_from_flat(flat: &FlatConfig) -> Result<WriteLeaseConfig, CommonError> {
    let defaults = WriteLeaseConfig::default();
    let config = WriteLeaseConfig {
        auto_renew: get_bool_or(flat, "client.write_lease.auto_renew", defaults.auto_renew)?,
        renew_before_expiry_ms: get_u64_or_strict(
            flat,
            "client.write_lease.renew_before_expiry_ms",
            defaults.renew_before_expiry_ms,
        )?,
    };
    if config.renew_before_expiry_ms == 0 {
        return Err(invalid_config(
            "client.write_lease.renew_before_expiry_ms",
            "must be greater than zero",
        ));
    }
    Ok(config)
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

fn get_bool_or(flat: &FlatConfig, key: &'static str, default: bool) -> Result<bool, CommonError> {
    if let Some(value) = flat.get_bool(key) {
        return Ok(value);
    }
    if flat.get_str(key).is_some() {
        return Err(invalid_config(key, "must be a boolean"));
    }
    Ok(default)
}

fn invalid_config(key: &'static str, detail: impl Into<String>) -> CommonError {
    CommonError::new(
        beryl_common::CommonErrorKind::InvalidArgument,
        format!("{key} {}", detail.into()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_bounded_retry_and_deadline() {
        let config = ClientConfig::default();
        assert_eq!(config.retry.max_attempts(), 3);
        assert_eq!(config.retry.operation_timeout_ms, DEFAULT_OPERATION_TIMEOUT_MS);
        assert!(config.write_lease.auto_renew);
        assert_eq!(
            config.write_lease.renew_before_expiry_ms,
            DEFAULT_WRITE_LEASE_RENEW_BEFORE_EXPIRY_MS
        );
        assert!(config.channel_pool.metadata_channel_pool_enabled);
        assert_eq!(config.channel_pool.metadata_channel_pool_max_per_group, 1);
        assert!(config.channel_pool.worker_channel_pool_enabled);
        assert_eq!(config.channel_pool.worker_channel_pool_max_per_worker, 1);
        assert_eq!(
            config.channel_pool.worker_endpoint_cooldown_ms,
            DEFAULT_WORKER_ENDPOINT_COOLDOWN_MS
        );
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
    fn current_retry_and_deadline_keys_are_loaded() {
        let mut flat = FlatConfig::new();
        flat.set("client.retry.max_attempts", 5i64);
        flat.set("client.operation.timeout_ms", 7_000i64);
        let config = ClientConfig::from_flat(flat).expect("retry config");
        assert_eq!(config.retry.max_attempts(), 5);
        assert_eq!(config.retry.operation_timeout_ms, 7_000);
    }

    #[test]
    fn channel_pool_config_is_loaded_from_flat_config() {
        let mut flat = FlatConfig::new();
        flat.set("client.channel_pool.metadata.enabled", false);
        flat.set("client.channel_pool.metadata.max_per_group", 2i64);
        flat.set("client.channel_pool.worker.enabled", false);
        flat.set("client.channel_pool.worker.max_per_worker", 3i64);
        flat.set("client.channel_pool.worker.endpoint_cooldown_ms", 4_000i64);

        let config = ClientConfig::from_flat(flat).expect("channel pool config");

        assert!(!config.channel_pool.metadata_channel_pool_enabled);
        assert_eq!(config.channel_pool.metadata_channel_pool_max_per_group, 2);
        assert!(!config.channel_pool.worker_channel_pool_enabled);
        assert_eq!(config.channel_pool.worker_channel_pool_max_per_worker, 3);
        assert_eq!(config.channel_pool.worker_endpoint_cooldown_ms, 4_000);
    }

    #[test]
    fn invalid_client_config_values_are_rejected() {
        for key in [
            "client.channel_pool.metadata.max_per_group",
            "client.channel_pool.worker.max_per_worker",
        ] {
            let mut flat = FlatConfig::new();
            flat.set(key, 0i64);
            let err = ClientConfig::from_flat(flat).expect_err("invalid pool config must fail");
            assert!(err.to_string().contains(key));
        }

        let mut flat = FlatConfig::new();
        flat.set("client.metadata.group.names", "root,analytics");
        flat.set("client.metadata.group.root.endpoints", "a,b");
        let err = ClientConfig::from_flat(flat).expect_err("missing group endpoints must fail");
        assert!(err.to_string().contains("client.metadata.group.analytics.endpoints"));

        for key in ["client.retry.max_attempts", "client.operation.timeout_ms"] {
            let mut flat = FlatConfig::new();
            flat.set(key, 0i64);
            let err = ClientConfig::from_flat(flat).expect_err("zero value must fail");
            assert!(err.to_string().contains(key));
        }

        for key in [
            "client.retry.max_retry_attempts",
            "client.refresh.max_attempts",
            "client.backoff.initial_ms",
            "client.backoff.max_ms",
            "client.backoff.multiplier",
        ] {
            let mut flat = FlatConfig::new();
            flat.set(key, 1i64);
            let err = ClientConfig::from_flat(flat).expect_err("removed key must fail");
            assert!(err.to_string().contains(key));
        }

        for key in [
            "client.write_lease.renew_before_expiry_ms",
            "client.channel_pool.worker.endpoint_cooldown_ms",
        ] {
            let mut flat = FlatConfig::new();
            flat.set(key, -1i64);
            let err = ClientConfig::from_flat(flat).expect_err("negative value must fail");
            assert!(err.to_string().contains(key));
        }

        let mut flat = FlatConfig::new();
        flat.set("client.write_lease.renew_before_expiry_ms", 0i64);
        let err = ClientConfig::from_flat(flat).expect_err("zero lease renewal threshold must fail");
        assert!(err.to_string().contains("client.write_lease.renew_before_expiry_ms"));
    }

    #[test]
    fn metadata_groups_parse_group_scoped_endpoints() {
        let mut flat = FlatConfig::new();
        flat.set("client.metadata.group.names", "root,analytics");
        flat.set("client.metadata.group.root.endpoints", "a,b");
        flat.set("client.metadata.group.analytics.endpoints", "c,d");

        let config = ClientConfig::from_flat(flat).expect("metadata group endpoint config");

        assert_eq!(config.metadata_groups.len(), 2);
        assert_eq!(config.metadata_groups[0].group_name, GroupName::parse("root").unwrap());
        assert_eq!(config.metadata_groups[0].endpoints, vec!["a", "b"]);
        assert_eq!(
            config.metadata_groups[1].group_name,
            GroupName::parse("analytics").unwrap()
        );
        assert_eq!(config.metadata_groups[1].endpoints, vec!["c", "d"]);
    }
}
