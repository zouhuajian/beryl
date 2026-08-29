// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Immutable client configuration, loading, and validation.

use std::path::Path;
use std::time::Duration;

use beryl_common::config::load_from_yaml_file;
use beryl_common::{CommonError, CommonErrorKind, FlatConfig};

use crate::error::{ClientError, ClientResult};

const DEFAULT_CLIENT_NAME: &str = "default-client";
const DEFAULT_METADATA_ENDPOINT: &str = "127.0.0.1:18080";
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_ATTEMPTS: usize = 3;
const DEFAULT_MAX_READ_STEP_BYTES: u32 = 8 * 1024 * 1024;
const DEFAULT_READ_TO_END_LIMIT: u64 = 64 * 1024 * 1024;
const DEFAULT_LEASE_RENEWAL_THRESHOLD: Duration = Duration::from_secs(30);
const DEFAULT_METADATA_CONNECTION_LIMIT: usize = 1;
const DEFAULT_WORKER_CONNECTION_LIMIT: usize = 1;
pub(crate) const DEFAULT_WORKER_ENDPOINT_COOLDOWN: Duration = Duration::from_secs(1);

const CLIENT_NAME_KEY: &str = "beryl.client.name";
const METADATA_ADDRESSES_KEY: &str = "beryl.client.metadata.addresses";
const OPERATION_TIMEOUT_KEY: &str = "beryl.client.request.timeout";
const MAX_ATTEMPTS_KEY: &str = "beryl.client.request.max-attempts";
const MAX_READ_STEP_BYTES_KEY: &str = "beryl.client.read.max-request-bytes";
const READ_TO_END_LIMIT_KEY: &str = "beryl.client.read.max-buffered-bytes";
const AUTOMATIC_LEASE_RENEWAL_KEY: &str = "beryl.client.write-lease.auto-renew";
const LEASE_RENEWAL_THRESHOLD_KEY: &str = "beryl.client.write-lease.renew-before-expiry";
const METADATA_CONNECTION_REUSE_KEY: &str = "beryl.client.metadata.connections.enabled";
const METADATA_CONNECTION_LIMIT_KEY: &str = "beryl.client.metadata.connections.max";
const WORKER_CONNECTION_REUSE_KEY: &str = "beryl.client.worker.connections.enabled";
const WORKER_CONNECTION_LIMIT_KEY: &str = "beryl.client.worker.connections.max-per-worker";
const WORKER_ENDPOINT_COOLDOWN_KEY: &str = "beryl.client.worker.connections.failure-cooldown";

/// Immutable configuration for one native Beryl filesystem client.
///
/// YAML loading and programmatic construction both produce this same typed
/// value. Fields remain private so callers cannot bypass validation after
/// construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    client_name: String,
    metadata_endpoints: Vec<String>,
    operation_timeout: Duration,
    max_attempts: usize,
    max_read_step_bytes: u32,
    read_to_end_limit: u64,
    automatic_lease_renewal: bool,
    lease_renewal_threshold: Duration,
    metadata_connection_reuse: bool,
    metadata_connection_limit: usize,
    worker_connection_reuse: bool,
    worker_connection_limit: usize,
    worker_endpoint_cooldown: Duration,
}

impl ClientConfig {
    /// Starts a programmatic configuration using the documented client defaults.
    pub fn builder() -> ClientConfigBuilder {
        ClientConfigBuilder {
            config: Self::defaults(),
        }
    }

    /// Loads dotted-key YAML and converts it into validated typed configuration.
    ///
    /// Unknown top-level keys are ignored. Known keys use the same type, range,
    /// endpoint, and duration validation as [`ClientConfigBuilder::build`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientErrorKind::Io`](crate::ClientErrorKind::Io) when the file
    /// cannot be read, or [`ClientErrorKind::InvalidConfiguration`](crate::ClientErrorKind::InvalidConfiguration)
    /// when YAML or a known configuration value is invalid.
    pub fn load<P: AsRef<Path>>(path: P) -> ClientResult<Self> {
        let flat = load_from_yaml_file(path).map_err(config_load_error)?;
        Self::from_flat(&flat)
    }

    /// Returns the low-cardinality display identity carried in request headers.
    pub fn client_name(&self) -> &str {
        &self.client_name
    }

    /// Returns bootstrap endpoints for the single supported root Metadata route.
    pub fn metadata_endpoints(&self) -> &[String] {
        &self.metadata_endpoints
    }

    /// Returns the absolute timeout budget for one public client operation.
    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// Returns the maximum number of primary RPC attempts, including the first.
    pub fn max_attempts(&self) -> usize {
        self.max_attempts
    }

    /// Returns the maximum byte count for one bounded Worker read step.
    pub fn max_read_step_bytes(&self) -> u32 {
        self.max_read_step_bytes
    }

    /// Returns the maximum owned allocation accepted by `FileReader::read_to_end`.
    pub fn read_to_end_limit(&self) -> u64 {
        self.read_to_end_limit
    }

    /// Returns whether writer operations automatically renew a near-expiry lease.
    pub fn automatic_lease_renewal(&self) -> bool {
        self.automatic_lease_renewal
    }

    /// Returns how close to lease expiry automatic renewal begins.
    pub fn lease_renewal_threshold(&self) -> Duration {
        self.lease_renewal_threshold
    }

    pub(crate) fn metadata_connection_reuse(&self) -> bool {
        self.metadata_connection_reuse
    }

    pub(crate) fn metadata_connection_limit(&self) -> usize {
        self.metadata_connection_limit
    }

    pub(crate) fn worker_connection_reuse(&self) -> bool {
        self.worker_connection_reuse
    }

    pub(crate) fn worker_connection_limit(&self) -> usize {
        self.worker_connection_limit
    }

    pub(crate) fn worker_endpoint_cooldown(&self) -> Duration {
        self.worker_endpoint_cooldown
    }

    pub(crate) fn operation_timeout_ms(&self) -> u64 {
        duration_millis(self.operation_timeout)
    }

    pub(crate) fn lease_renewal_threshold_ms(&self) -> u64 {
        duration_millis(self.lease_renewal_threshold)
    }

    /// Revalidates every correctness and resource bound at runtime construction.
    pub(crate) fn validate(&self) -> ClientResult<()> {
        if self.client_name.trim().is_empty() {
            return Err(invalid_config(CLIENT_NAME_KEY, "must not be blank"));
        }
        if self.metadata_endpoints.is_empty() {
            return Err(invalid_config(
                METADATA_ADDRESSES_KEY,
                "must contain at least one endpoint for the root route",
            ));
        }
        for endpoint in &self.metadata_endpoints {
            validate_metadata_endpoint(endpoint)?;
        }
        validate_millisecond_duration(OPERATION_TIMEOUT_KEY, self.operation_timeout)?;
        if tokio::time::Instant::now()
            .checked_add(self.operation_timeout)
            .is_none()
        {
            return Err(invalid_config(
                OPERATION_TIMEOUT_KEY,
                "is too large for the monotonic clock",
            ));
        }
        validate_positive(MAX_ATTEMPTS_KEY, self.max_attempts)?;
        validate_positive(MAX_READ_STEP_BYTES_KEY, self.max_read_step_bytes)?;
        validate_positive(READ_TO_END_LIMIT_KEY, self.read_to_end_limit)?;
        validate_millisecond_duration(LEASE_RENEWAL_THRESHOLD_KEY, self.lease_renewal_threshold)?;
        validate_positive(METADATA_CONNECTION_LIMIT_KEY, self.metadata_connection_limit)?;
        validate_positive(WORKER_CONNECTION_LIMIT_KEY, self.worker_connection_limit)?;
        validate_nonzero_duration(WORKER_ENDPOINT_COOLDOWN_KEY, self.worker_endpoint_cooldown)?;
        if std::time::Instant::now()
            .checked_add(self.worker_endpoint_cooldown)
            .is_none()
        {
            return Err(invalid_config(
                WORKER_ENDPOINT_COOLDOWN_KEY,
                "is too large for the monotonic clock",
            ));
        }
        Ok(())
    }

    /// Converts transient flat configuration into the same sealed value used by
    /// the public builder. Unknown keys deliberately remain ignored.
    fn from_flat(flat: &FlatConfig) -> ClientResult<Self> {
        let defaults = Self::defaults();
        let metadata_endpoints = if flat.contains_key(METADATA_ADDRESSES_KEY) {
            flat.get_string_list(METADATA_ADDRESSES_KEY)
                .ok_or_else(|| invalid_config(METADATA_ADDRESSES_KEY, "must be a list of endpoints"))?
        } else {
            defaults.metadata_endpoints
        };
        let config = Self {
            client_name: flat
                .string_or(CLIENT_NAME_KEY, &defaults.client_name)
                .map_err(config_value_error)?,
            metadata_endpoints,
            operation_timeout: Duration::from_millis(
                flat.duration_ms_or(OPERATION_TIMEOUT_KEY, duration_millis(defaults.operation_timeout))
                    .map_err(config_value_error)?,
            ),
            max_attempts: flat
                .positive_usize_or(MAX_ATTEMPTS_KEY, defaults.max_attempts)
                .map_err(config_value_error)?,
            max_read_step_bytes: flat
                .bytes_u32_or(MAX_READ_STEP_BYTES_KEY, defaults.max_read_step_bytes)
                .map_err(config_value_error)?,
            read_to_end_limit: flat
                .bytes_u64_or(READ_TO_END_LIMIT_KEY, defaults.read_to_end_limit)
                .map_err(config_value_error)?,
            automatic_lease_renewal: flat
                .bool_or(AUTOMATIC_LEASE_RENEWAL_KEY, defaults.automatic_lease_renewal)
                .map_err(config_value_error)?,
            lease_renewal_threshold: Duration::from_millis(
                flat.duration_ms_or(
                    LEASE_RENEWAL_THRESHOLD_KEY,
                    duration_millis(defaults.lease_renewal_threshold),
                )
                .map_err(config_value_error)?,
            ),
            metadata_connection_reuse: flat
                .bool_or(METADATA_CONNECTION_REUSE_KEY, defaults.metadata_connection_reuse)
                .map_err(config_value_error)?,
            metadata_connection_limit: flat
                .positive_usize_or(METADATA_CONNECTION_LIMIT_KEY, defaults.metadata_connection_limit)
                .map_err(config_value_error)?,
            worker_connection_reuse: flat
                .bool_or(WORKER_CONNECTION_REUSE_KEY, defaults.worker_connection_reuse)
                .map_err(config_value_error)?,
            worker_connection_limit: flat
                .positive_usize_or(WORKER_CONNECTION_LIMIT_KEY, defaults.worker_connection_limit)
                .map_err(config_value_error)?,
            worker_endpoint_cooldown: Duration::from_millis(
                flat.duration_ms_or(
                    WORKER_ENDPOINT_COOLDOWN_KEY,
                    duration_millis(defaults.worker_endpoint_cooldown),
                )
                .map_err(config_value_error)?,
            ),
        };
        config.validate()?;
        Ok(config)
    }

    fn defaults() -> Self {
        Self {
            client_name: DEFAULT_CLIENT_NAME.to_string(),
            metadata_endpoints: vec![DEFAULT_METADATA_ENDPOINT.to_string()],
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            max_read_step_bytes: DEFAULT_MAX_READ_STEP_BYTES,
            read_to_end_limit: DEFAULT_READ_TO_END_LIMIT,
            automatic_lease_renewal: true,
            lease_renewal_threshold: DEFAULT_LEASE_RENEWAL_THRESHOLD,
            metadata_connection_reuse: true,
            metadata_connection_limit: DEFAULT_METADATA_CONNECTION_LIMIT,
            worker_connection_reuse: true,
            worker_connection_limit: DEFAULT_WORKER_CONNECTION_LIMIT,
            worker_endpoint_cooldown: DEFAULT_WORKER_ENDPOINT_COOLDOWN,
        }
    }
}

/// Programmatic constructor for a validated [`ClientConfig`].
///
/// The builder owns the only mutable configuration state. `build` validates
/// the complete value and returns an immutable configuration.
#[derive(Clone, Debug)]
pub struct ClientConfigBuilder {
    config: ClientConfig,
}

impl ClientConfigBuilder {
    /// Sets the low-cardinality display identity carried in request headers.
    pub fn client_name(mut self, client_name: impl Into<String>) -> Self {
        self.config.client_name = client_name.into();
        self
    }

    /// Replaces bootstrap endpoints for the single supported root Metadata route.
    pub fn metadata_endpoints<I, S>(mut self, endpoints: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.metadata_endpoints = endpoints.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the absolute timeout budget for one public client operation.
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.config.operation_timeout = timeout;
        self
    }

    /// Sets the primary RPC attempt cap, including the first attempt.
    pub fn max_attempts(mut self, max_attempts: usize) -> Self {
        self.config.max_attempts = max_attempts;
        self
    }

    /// Sets the maximum byte count for one bounded Worker read step.
    pub fn max_read_step_bytes(mut self, max_bytes: u32) -> Self {
        self.config.max_read_step_bytes = max_bytes;
        self
    }

    /// Sets the maximum owned allocation accepted by `FileReader::read_to_end`.
    pub fn read_to_end_limit(mut self, max_bytes: u64) -> Self {
        self.config.read_to_end_limit = max_bytes;
        self
    }

    /// Enables or disables automatic renewal before writer side effects.
    pub fn automatic_lease_renewal(mut self, enabled: bool) -> Self {
        self.config.automatic_lease_renewal = enabled;
        self
    }

    /// Sets how close to lease expiry automatic renewal begins.
    pub fn lease_renewal_threshold(mut self, threshold: Duration) -> Self {
        self.config.lease_renewal_threshold = threshold;
        self
    }

    /// Enables or disables Metadata channel reuse.
    pub fn metadata_connection_reuse(mut self, enabled: bool) -> Self {
        self.config.metadata_connection_reuse = enabled;
        self
    }

    /// Sets the maximum cached Metadata channels for the root route.
    pub fn metadata_connection_limit(mut self, limit: usize) -> Self {
        self.config.metadata_connection_limit = limit;
        self
    }

    /// Enables or disables Worker channel reuse.
    pub fn worker_connection_reuse(mut self, enabled: bool) -> Self {
        self.config.worker_connection_reuse = enabled;
        self
    }

    /// Sets the maximum cached channel identities per Worker.
    pub fn worker_connection_limit(mut self, limit: usize) -> Self {
        self.config.worker_connection_limit = limit;
        self
    }

    /// Sets the cooldown after a transient Worker endpoint failure.
    pub fn worker_endpoint_cooldown(mut self, cooldown: Duration) -> Self {
        self.config.worker_endpoint_cooldown = cooldown;
        self
    }

    /// Validates and seals the complete configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ClientErrorKind::InvalidConfiguration`](crate::ClientErrorKind::InvalidConfiguration)
    /// for a blank client name, missing or invalid Metadata endpoints, zero
    /// resource bounds, non-millisecond operation or lease durations, or a
    /// duration that the runtime clock cannot represent safely.
    pub fn build(self) -> ClientResult<ClientConfig> {
        self.config.validate()?;
        Ok(self.config)
    }
}

/// Normalizes and validates one Metadata endpoint without opening a connection.
pub(crate) fn normalize_metadata_endpoint(endpoint: &str) -> ClientResult<String> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Err(invalid_config(
            METADATA_ADDRESSES_KEY,
            "must not contain blank endpoints",
        ));
    }
    if endpoint != trimmed {
        return Err(invalid_config(
            METADATA_ADDRESSES_KEY,
            "must not contain surrounding whitespace",
        ));
    }
    let endpoint = trimmed;
    let normalized = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    tonic::transport::Endpoint::from_shared(normalized.clone()).map_err(|error| {
        invalid_config(
            METADATA_ADDRESSES_KEY,
            format!("contains invalid endpoint {endpoint}: {error}"),
        )
    })?;
    Ok(normalized)
}

fn validate_metadata_endpoint(endpoint: &str) -> ClientResult<()> {
    normalize_metadata_endpoint(endpoint).map(|_| ())
}

fn validate_positive(key: &'static str, value: impl PartialEq + Default) -> ClientResult<()> {
    if value == Default::default() {
        return Err(invalid_config(key, "must be greater than zero"));
    }
    Ok(())
}

fn validate_nonzero_duration(key: &'static str, value: Duration) -> ClientResult<()> {
    if value < Duration::from_millis(1) {
        return Err(invalid_config(key, "must be at least 1ms"));
    }
    Ok(())
}

fn validate_millisecond_duration(key: &'static str, value: Duration) -> ClientResult<()> {
    validate_nonzero_duration(key, value)?;
    let millis = u64::try_from(value.as_millis()).map_err(|_| invalid_config(key, "is too large"))?;
    if Duration::from_millis(millis) != value {
        return Err(invalid_config(key, "must use whole milliseconds"));
    }
    Ok(())
}

fn duration_millis(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).expect("validated client duration fits u64 milliseconds")
}

fn config_load_error(error: CommonError) -> ClientError {
    if error.kind == CommonErrorKind::Io {
        error.into()
    } else {
        config_value_error(error)
    }
}

fn config_value_error(error: CommonError) -> ClientError {
    ClientError::invalid_configuration(error.to_string())
}

fn invalid_config(key: &'static str, detail: impl Into<String>) -> ClientError {
    ClientError::invalid_configuration(format!("{key} {}", detail.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_inner::ClientInner;

    #[test]
    fn builder_and_flat_loading_share_values_and_ignore_unknown_keys() {
        let mut flat = FlatConfig::new();
        flat.set(CLIENT_NAME_KEY, "configured-client");
        flat.set(METADATA_ADDRESSES_KEY, vec!["metadata.internal:18080".to_string()]);
        flat.set(OPERATION_TIMEOUT_KEY, "12s");
        flat.set(MAX_ATTEMPTS_KEY, 5i64);
        flat.set(MAX_READ_STEP_BYTES_KEY, "4MiB");
        flat.set(READ_TO_END_LIMIT_KEY, "32MiB");
        flat.set(AUTOMATIC_LEASE_RENEWAL_KEY, false);
        flat.set(LEASE_RENEWAL_THRESHOLD_KEY, "8s");
        flat.set(METADATA_CONNECTION_REUSE_KEY, false);
        flat.set(METADATA_CONNECTION_LIMIT_KEY, 2i64);
        flat.set(WORKER_CONNECTION_REUSE_KEY, false);
        flat.set(WORKER_CONNECTION_LIMIT_KEY, 3i64);
        flat.set(WORKER_ENDPOINT_COOLDOWN_KEY, "4s");
        flat.set("beryl.future.option", "ignored");

        let from_flat = ClientConfig::from_flat(&flat).expect("flat config");
        let from_builder = ClientConfig::builder()
            .client_name("configured-client")
            .metadata_endpoints(["metadata.internal:18080"])
            .operation_timeout(Duration::from_secs(12))
            .max_attempts(5)
            .max_read_step_bytes(4 * 1024 * 1024)
            .read_to_end_limit(32 * 1024 * 1024)
            .automatic_lease_renewal(false)
            .lease_renewal_threshold(Duration::from_secs(8))
            .metadata_connection_reuse(false)
            .metadata_connection_limit(2)
            .worker_connection_reuse(false)
            .worker_connection_limit(3)
            .worker_endpoint_cooldown(Duration::from_secs(4))
            .build()
            .expect("builder config");

        assert_eq!(from_flat, from_builder);
    }

    #[test]
    fn builder_rejects_invalid_identity_endpoints_and_resource_bounds() {
        let invalid = [
            ClientConfig::builder().client_name(" ").build(),
            ClientConfig::builder().metadata_endpoints(Vec::<String>::new()).build(),
            ClientConfig::builder()
                .metadata_endpoints([" metadata.internal:18080"])
                .build(),
            ClientConfig::builder().metadata_endpoints(["http://["]).build(),
            ClientConfig::builder().operation_timeout(Duration::ZERO).build(),
            ClientConfig::builder().max_attempts(0).build(),
            ClientConfig::builder().max_read_step_bytes(0).build(),
            ClientConfig::builder().read_to_end_limit(0).build(),
            ClientConfig::builder().lease_renewal_threshold(Duration::ZERO).build(),
            ClientConfig::builder().metadata_connection_limit(0).build(),
            ClientConfig::builder().worker_connection_limit(0).build(),
            ClientConfig::builder().worker_endpoint_cooldown(Duration::ZERO).build(),
        ];

        assert!(invalid
            .into_iter()
            .all(|result| { result.is_err_and(|error| error.kind() == crate::ClientErrorKind::InvalidConfiguration) }));
    }

    #[test]
    fn runtime_construction_revalidates_typed_configuration() {
        let mut config = ClientConfig::builder().build().expect("config");
        config.max_read_step_bytes = 0;

        let error = match ClientInner::from_config(config) {
            Ok(_) => panic!("runtime construction must reject invalid configuration"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), crate::ClientErrorKind::InvalidConfiguration);
    }
}
