// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client configuration loading and validation.

use beryl_common::config::load_from_yaml_file;
use beryl_common::{CommonError, FlatConfig};
use beryl_types::GroupName;
use std::path::Path;

pub const DEFAULT_CLIENT_NAME: &str = "default-client";
pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 30_000;
/// Default maximum result size for one positioned owned-buffer read.
pub const DEFAULT_READ_MAX_REQUEST_BYTES: u32 = 8 * 1024 * 1024;
/// Default maximum file size accepted by the whole-file convenience read.
pub const DEFAULT_READ_MAX_BUFFERED_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_WRITE_LEASE_RENEW_BEFORE_EXPIRY_MS: u64 = 30_000;
pub const DEFAULT_WORKER_ENDPOINT_COOLDOWN_MS: u64 = 1_000;

const READ_MAX_REQUEST_BYTES_KEY: &str = "beryl.client.read.max-request-bytes";
const READ_MAX_BUFFERED_BYTES_KEY: &str = "beryl.client.read.max-buffered-bytes";

/// Client-specific configuration.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Underlying flat configuration.
    pub inner: FlatConfig,
    /// Low-cardinality display identity carried in request headers.
    pub client_name: String,
    /// Retry configuration.
    pub retry: RetryConfig,
    /// Bounds for public APIs that return owned read buffers.
    pub read: ReadConfig,
    /// Client-side write lease renewal policy.
    pub write_lease: WriteLeaseConfig,
    /// Metadata and Worker connection reuse configuration.
    pub connections: ConnectionConfig,
    /// Bootstrap endpoints for the single supported metadata group.
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

/// Metadata and Worker connection reuse configuration.
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Enable Metadata connection reuse.
    pub metadata_enabled: bool,
    /// Maximum cached Metadata connections per group.
    pub metadata_max_per_group: usize,
    /// Enable Worker connection reuse.
    pub worker_enabled: bool,
    /// Maximum cached worker channels per worker identity.
    pub worker_max_per_worker: usize,
    /// Cooldown duration after transient worker endpoint failures.
    pub worker_failure_cooldown_ms: u64,
}

/// Retry configuration.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum attempts for one primary RPC, including the first.
    pub max_attempts: usize,
    /// Total public-operation timeout in milliseconds.
    pub operation_timeout_ms: u64,
}

/// Memory bounds for the current owned-buffer read APIs.
#[derive(Clone, Debug)]
pub struct ReadConfig {
    /// Maximum byte count accepted by one `read_at` or `read_exact_at` call.
    pub max_request_bytes: u32,
    /// Maximum file size accepted by `read_all`.
    pub max_buffered_bytes: u64,
}

impl ReadConfig {
    /// Ensures both owned-buffer limits admit nonempty reads.
    pub(crate) fn validate(&self) -> Result<(), CommonError> {
        if self.max_request_bytes == 0 {
            return Err(invalid_config(READ_MAX_REQUEST_BYTES_KEY, "must be greater than zero"));
        }
        if self.max_buffered_bytes == 0 {
            return Err(invalid_config(READ_MAX_BUFFERED_BYTES_KEY, "must be greater than zero"));
        }
        Ok(())
    }
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

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            max_request_bytes: DEFAULT_READ_MAX_REQUEST_BYTES,
            max_buffered_bytes: DEFAULT_READ_MAX_BUFFERED_BYTES,
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

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            metadata_enabled: true,
            metadata_max_per_group: 1,
            worker_enabled: true,
            worker_max_per_worker: 1,
            worker_failure_cooldown_ms: DEFAULT_WORKER_ENDPOINT_COOLDOWN_MS,
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
        Self::from_flat(load_from_yaml_file(path)?)
    }

    /// Create from FlatConfig.
    pub fn from_flat(flat: FlatConfig) -> Result<Self, CommonError> {
        let client_name = client_name_from_flat(&flat)?;

        let retry = retry_config_from_flat(&flat)?;
        let read = read_config_from_flat(&flat)?;
        let write_lease = write_lease_config_from_flat(&flat)?;
        let connections = connection_config_from_flat(&flat)?;
        let metadata_groups = parse_metadata_endpoints(&flat)?;

        Ok(Self {
            inner: flat,
            client_name,
            retry,
            read,
            write_lease,
            connections,
            metadata_groups,
        })
    }

    /// Get the underlying flat configuration.
    pub fn as_flat(&self) -> &FlatConfig {
        &self.inner
    }

    /// Return the low-cardinality display identity used in request headers.
    pub fn client_name(&self) -> &str {
        &self.client_name
    }
}

fn client_name_from_flat(flat: &FlatConfig) -> Result<String, CommonError> {
    const KEY: &str = "beryl.client.name";
    let name = flat.string_or(KEY, DEFAULT_CLIENT_NAME)?;
    if name.trim().is_empty() {
        return Err(invalid_config(KEY, "must not be blank"));
    }
    Ok(name)
}

fn connection_config_from_flat(flat: &FlatConfig) -> Result<ConnectionConfig, CommonError> {
    let defaults = ConnectionConfig::default();
    let config = ConnectionConfig {
        metadata_enabled: flat.bool_or("beryl.client.metadata.connections.enabled", defaults.metadata_enabled)?,
        metadata_max_per_group: flat
            .positive_usize_or("beryl.client.metadata.connections.max", defaults.metadata_max_per_group)?,
        worker_enabled: flat.bool_or("beryl.client.worker.connections.enabled", defaults.worker_enabled)?,
        worker_max_per_worker: flat.positive_usize_or(
            "beryl.client.worker.connections.max-per-worker",
            defaults.worker_max_per_worker,
        )?,
        worker_failure_cooldown_ms: flat.duration_ms_or(
            "beryl.client.worker.connections.failure-cooldown",
            defaults.worker_failure_cooldown_ms,
        )?,
    };
    Ok(config)
}

fn parse_metadata_endpoints(flat: &FlatConfig) -> Result<Vec<MetadataGroupConfig>, CommonError> {
    const KEY: &str = "beryl.client.metadata.addresses";
    let endpoints = if flat.contains_key(KEY) {
        flat.get_string_list(KEY)
            .ok_or_else(|| invalid_config(KEY, "must be a list of addresses"))?
    } else {
        vec!["127.0.0.1:18080".to_string()]
    };
    if endpoints.is_empty() || endpoints.iter().any(|endpoint| endpoint.trim().is_empty()) {
        return Err(invalid_config(KEY, "must contain at least one non-empty address"));
    }
    Ok(vec![MetadataGroupConfig {
        group_name: GroupName::parse("root").expect("the supported metadata group is valid"),
        endpoints,
    }])
}

fn retry_config_from_flat(flat: &FlatConfig) -> Result<RetryConfig, CommonError> {
    let defaults = RetryConfig::default();
    let max_attempts = flat.positive_usize_or("beryl.client.request.max-attempts", defaults.max_attempts)?;
    let operation_timeout_ms = flat.duration_ms_or("beryl.client.request.timeout", defaults.operation_timeout_ms)?;
    Ok(RetryConfig {
        max_attempts,
        operation_timeout_ms,
    })
}

fn read_config_from_flat(flat: &FlatConfig) -> Result<ReadConfig, CommonError> {
    let defaults = ReadConfig::default();
    let config = ReadConfig {
        max_request_bytes: flat.bytes_u32_or(READ_MAX_REQUEST_BYTES_KEY, defaults.max_request_bytes)?,
        max_buffered_bytes: flat.bytes_u64_or(READ_MAX_BUFFERED_BYTES_KEY, defaults.max_buffered_bytes)?,
    };
    config.validate()?;
    Ok(config)
}

fn write_lease_config_from_flat(flat: &FlatConfig) -> Result<WriteLeaseConfig, CommonError> {
    let defaults = WriteLeaseConfig::default();
    let config = WriteLeaseConfig {
        auto_renew: flat.bool_or("beryl.client.write-lease.auto-renew", defaults.auto_renew)?,
        renew_before_expiry_ms: flat.duration_ms_or(
            "beryl.client.write-lease.renew-before-expiry",
            defaults.renew_before_expiry_ms,
        )?,
    };
    Ok(config)
}

fn invalid_config(key: &'static str, detail: impl Into<String>) -> CommonError {
    CommonError::new(
        beryl_common::CommonErrorKind::InvalidArgument,
        format!("{key} {}", detail.into()),
    )
}
