// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Configuration validation.

use crate::config::flat::FlatConfig;
use crate::config::keys::{
    client, client_backoff, client_cache, client_consistency, client_read_mode, client_retry,
    client_worker_direct_read, client_write_mode, metadata_raft, metadata_rpc, observe_metrics, worker_eviction,
    worker_replication, worker_service_rpc, worker_storage, worker_ufs,
};
use crate::error::{CommonError, CommonErrorCode};

/// Validate core-site configuration.
pub fn validate_core(config: &FlatConfig) -> Result<(), CommonError> {
    // Validate metadata.rpc.port range
    if let Some(port) = config.get_i64(metadata_rpc::PORT)
        && !(1..=65535).contains(&port)
    {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("{} must be in range 1-65535, got {}", metadata_rpc::PORT, port),
        ));
    }

    // Validate metadata.raft.node_id
    if let Some(node_id) = config.get_i64(metadata_raft::NODE_ID)
        && node_id < 1
    {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("{} must be >= 1, got {}", metadata_raft::NODE_ID, node_id),
        ));
    }

    // Validate worker.rpc.bind as a concrete socket address.
    if let Some(bind) = config.get_str(worker_service_rpc::BIND)
        && bind.parse::<std::net::SocketAddr>().is_err()
    {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!(
                "{} must be a valid socket address, got {}",
                worker_service_rpc::BIND,
                bind
            ),
        ));
    }
    if let Some(max_inflight) = config.get_usize(worker_service_rpc::MAX_INFLIGHT)
        && max_inflight == 0
    {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("{} must be > 0", worker_service_rpc::MAX_INFLIGHT),
        ));
    }

    // Validate worker.storage.block_size and chunk_size
    if let Some(block_size) = config.get_bytes(worker_storage::BLOCK_SIZE)
        && let Some(chunk_size) = config.get_bytes(worker_storage::CHUNK_SIZE)
    {
        if chunk_size > block_size {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!(
                    "{} ({}) must be <= {} ({})",
                    worker_storage::CHUNK_SIZE,
                    chunk_size,
                    worker_storage::BLOCK_SIZE,
                    block_size
                ),
            ));
        }
        if block_size % chunk_size != 0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!(
                    "{} ({}) must be divisible by {} ({})",
                    worker_storage::BLOCK_SIZE,
                    block_size,
                    worker_storage::CHUNK_SIZE,
                    chunk_size
                ),
            ));
        }
    }

    // Validate other timeouts
    for key in &[worker_ufs::TIMEOUT_MS, worker_replication::CHUNK_TIMEOUT_MS] {
        if let Some(ms) = config.get_i64(key)
            && ms <= 0
        {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("{} must be > 0", key),
            ));
        }
    }

    // Validate worker.storage.kind enum
    if let Some(kind) = config.get_str(worker_storage::KIND) {
        let valid_kinds = ["fs", "io_uring", "spdk"];
        if !valid_kinds.contains(&kind.as_str()) {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!(
                    "{} must be one of {:?}, got {}",
                    worker_storage::KIND,
                    valid_kinds,
                    kind
                ),
            ));
        }
    }

    // Validate UFS instance max_inflight
    for key in config.keys() {
        if key.starts_with("ufs.")
            && key.ends_with(".max_inflight")
            && let Some(max) = config.get_usize(key)
            && max == 0
        {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("{} must be > 0", key),
            ));
        }
    }

    // Validate observe ports
    if let Some(bind) = config.get_str(observe_metrics::PROMETHEUS_BIND)
        && let Some(port_str) = bind.rsplit(':').next()
        && let Ok(port) = port_str.parse::<i64>()
        && !(1..=65535).contains(&port)
    {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!(
                "{} port must be in range 1-65535, got {}",
                observe_metrics::PROMETHEUS_BIND,
                port
            ),
        ));
    }

    // Validate eviction watermarks
    if let Some(high_str) = config.get_str(worker_eviction::HIGH_WATERMARK)
        && let Ok(high) = high_str.parse::<f64>()
    {
        if !(0.0..=1.0).contains(&high) {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!(
                    "{} must be in range 0.0-1.0, got {}",
                    worker_eviction::HIGH_WATERMARK,
                    high
                ),
            ));
        }
        if let Some(low_str) = config.get_str(worker_eviction::LOW_WATERMARK)
            && let Ok(low) = low_str.parse::<f64>()
            && low >= high
        {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!(
                    "{} ({}) must be < {} ({})",
                    worker_eviction::LOW_WATERMARK,
                    low,
                    worker_eviction::HIGH_WATERMARK,
                    high
                ),
            ));
        }
    }

    Ok(())
}

/// Validate client-site configuration.
pub fn validate_client(config: &FlatConfig) -> Result<(), CommonError> {
    // Validate client.default_timeout_ms
    if let Some(ms) = config.get_i64(client::DEFAULT_TIMEOUT_MS)
        && ms <= 0
    {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("{} must be > 0", client::DEFAULT_TIMEOUT_MS),
        ));
    }

    // Validate client.metadata.endpoints (at least one endpoint)
    if let Some(endpoints_str) = config.get_str(client::METADATA_ENDPOINTS) {
        let endpoints: Vec<&str> = endpoints_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        if endpoints.is_empty() {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("{} must contain at least one endpoint", client::METADATA_ENDPOINTS),
            ));
        }
        // Basic validation: each endpoint should be a valid socket address or URL
        for endpoint in endpoints {
            if endpoint.parse::<std::net::SocketAddr>().is_err()
                && !endpoint.starts_with("http://")
                && !endpoint.starts_with("https://")
            {
                // Warn but don't fail (might be a hostname)
                tracing::warn!(endpoint = %endpoint, "client.metadata.endpoints entry may be invalid");
            }
        }
    }

    // Validate client.consistency.default enum
    if let Some(consistency) = config.get_str(client_consistency::DEFAULT) {
        let valid_values = ["normal", "strong", "weak"];
        if !valid_values.contains(&consistency.as_str()) {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!(
                    "{} must be one of {:?}, got {}",
                    client_consistency::DEFAULT,
                    valid_values,
                    consistency
                ),
            ));
        }
    }

    // Validate client.read_mode.default enum
    if let Some(mode) = config.get_str(client_read_mode::DEFAULT) {
        let valid_values = ["cached", "direct"];
        if !valid_values.contains(&mode.as_str()) {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!(
                    "{} must be one of {:?}, got {}",
                    client_read_mode::DEFAULT,
                    valid_values,
                    mode
                ),
            ));
        }
    }

    // Validate client.write_mode.default enum
    if let Some(mode) = config.get_str(client_write_mode::DEFAULT) {
        let valid_values = ["back", "through", "direct"];
        if !valid_values.contains(&mode.as_str()) {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!(
                    "{} must be one of {:?}, got {}",
                    client_write_mode::DEFAULT,
                    valid_values,
                    mode
                ),
            ));
        }
    }

    // Validate cache TTLs
    for key in &[
        client_cache::FILE_META_TTL_SECS,
        client_cache::ROUTE_TTL_SECS,
        client_worker_direct_read::CACHE_TTL_SECS,
    ] {
        if let Some(ttl) = config.get_i64(key)
            && ttl <= 0
        {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("{} must be > 0", key),
            ));
        }
    }

    // Validate retry configuration
    for key in &[
        client_retry::MAX_RETRY_ATTEMPTS,
        client_retry::METADATA_BUDGET,
        client_retry::WORKER_BUDGET,
        client_retry::SESSION_BARRIER_BUDGET,
        client_backoff::INITIAL_MS,
        client_backoff::MAX_MS,
    ] {
        if let Some(value) = config.get_i64(key)
            && value < 0
        {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("{key} must be >= 0, got {value}"),
            ));
        }
    }

    if let Some(initial) = config.get_i64(client_backoff::INITIAL_MS)
        && let Some(max) = config.get_i64(client_backoff::MAX_MS)
        && max < initial
    {
        return Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("{} must be >= {}", client_backoff::MAX_MS, client_backoff::INITIAL_MS),
        ));
    }

    if let Some(multiplier_str) = config.get_str(client_backoff::MULTIPLIER) {
        let multiplier = multiplier_str.parse::<f64>().map_err(|_| {
            CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("{} must be a number", client_backoff::MULTIPLIER),
            )
        })?;
        if !multiplier.is_finite() || multiplier < 1.0 {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!(
                    "{} must be finite and >= 1.0, got {}",
                    client_backoff::MULTIPLIER,
                    multiplier
                ),
            ));
        }
    }

    Ok(())
}
