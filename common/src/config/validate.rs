// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Configuration validation.

use crate::config::flat::FlatConfig;
use crate::config::keys::{
    client, client_cache, client_consistency, client_read_mode, client_retry, client_worker_direct_read,
    client_write_mode, metadata_raft, metadata_rpc, observe_metrics, worker_eviction, worker_replication, worker_rpc,
    worker_storage, worker_transport, worker_ufs,
};
use crate::error::{CommonError, ErrorCode};

/// Validate core-site configuration.
pub fn validate_core(config: &FlatConfig) -> Result<(), CommonError> {
    // Validate metadata.rpc.port range
    if let Some(port) = config.get_i64(metadata_rpc::PORT) {
        if !(1..=65535).contains(&port) {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                format!("{} must be in range 1-65535, got {}", metadata_rpc::PORT, port),
            ));
        }
    }

    // Validate metadata.raft.node_id
    if let Some(node_id) = config.get_i64(metadata_raft::NODE_ID) {
        if node_id < 1 {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                format!("{} must be >= 1, got {}", metadata_raft::NODE_ID, node_id),
            ));
        }
    }

    // Validate worker.rpc.bind format (basic check)
    if let Some(bind) = config.get_str(worker_rpc::BIND) {
        if bind.parse::<std::net::SocketAddr>().is_err() && !bind.contains(':') {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                format!("{} must be a valid socket address, got {}", worker_rpc::BIND, bind),
            ));
        }
    }

    // Validate worker.storage.block_size and chunk_size
    if let Some(block_size) = config.get_bytes(worker_storage::BLOCK_SIZE) {
        if let Some(chunk_size) = config.get_bytes(worker_storage::CHUNK_SIZE) {
            if chunk_size > block_size {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
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
                    ErrorCode::InvalidArgument,
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
    }

    // Validate worker.transport.kind and worker.storage.kind combo
    let transport_kind = config
        .get_str(worker_transport::KIND)
        .unwrap_or_else(|| "grpc".to_string());
    let storage_kind = config.get_str(worker_storage::KIND).unwrap_or_else(|| "fs".to_string());

    // io_uring transport requires io_uring storage (or fs with io_uring support)
    if transport_kind == "io_uring" && storage_kind != "io_uring" && storage_kind != "fs" {
        return Err(CommonError::new(
            ErrorCode::InvalidArgument,
            format!(
                "{}=io_uring requires {}=io_uring or fs, got {}",
                worker_transport::KIND,
                worker_storage::KIND,
                storage_kind
            ),
        ));
    }

    // Validate worker.transport parameters
    for key in &[
        worker_transport::MAX_INFLIGHT_REQUESTS,
        worker_transport::MAX_INFLIGHT_STREAMS,
        worker_transport::SERVER_MAX_INFLIGHT,
        worker_transport::CONNECT_TIMEOUT_MS,
        worker_transport::REQUEST_TIMEOUT_MS,
        worker_transport::KEEPALIVE_INTERVAL_MS,
        worker_transport::KEEPALIVE_TIMEOUT_MS,
    ] {
        if let Some(val) = config.get_usize(key) {
            if val == 0 {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
                    format!("{} must be > 0", key),
                ));
            }
        } else if let Some(ms) = config.get_i64(key) {
            if ms <= 0 {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
                    format!("{} must be > 0", key),
                ));
            }
        }
    }

    // Validate other timeouts
    for key in &[worker_ufs::TIMEOUT_MS, worker_replication::CHUNK_TIMEOUT_MS] {
        if let Some(ms) = config.get_i64(key) {
            if ms <= 0 {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
                    format!("{} must be > 0", key),
                ));
            }
        }
    }

    // Validate worker.transport.kind enum
    if let Some(kind) = config.get_str(worker_transport::KIND) {
        let valid_kinds = ["grpc", "quic", "rdma", "io_uring", "local"];
        if !valid_kinds.contains(&kind.as_str()) {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "{} must be one of {:?}, got {}",
                    worker_transport::KIND,
                    valid_kinds,
                    kind
                ),
            ));
        }
    }

    // Validate worker.storage.kind enum
    if let Some(kind) = config.get_str(worker_storage::KIND) {
        let valid_kinds = ["fs", "io_uring", "spdk"];
        if !valid_kinds.contains(&kind.as_str()) {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
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
        if key.starts_with("ufs.") && key.ends_with(".max_inflight") {
            if let Some(max) = config.get_usize(key) {
                if max == 0 {
                    return Err(CommonError::new(
                        ErrorCode::InvalidArgument,
                        format!("{} must be > 0", key),
                    ));
                }
            }
        }
    }

    // Validate observe ports
    if let Some(bind) = config.get_str(observe_metrics::PROMETHEUS_BIND) {
        if let Some(port_str) = bind.split(':').last() {
            if let Ok(port) = port_str.parse::<i64>() {
                if !(1..=65535).contains(&port) {
                    return Err(CommonError::new(
                        ErrorCode::InvalidArgument,
                        format!(
                            "{} port must be in range 1-65535, got {}",
                            observe_metrics::PROMETHEUS_BIND,
                            port
                        ),
                    ));
                }
            }
        }
    }

    // Validate eviction watermarks
    if let Some(high_str) = config.get_str(worker_eviction::HIGH_WATERMARK) {
        if let Ok(high) = high_str.parse::<f64>() {
            if !(0.0..=1.0).contains(&high) {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
                    format!(
                        "{} must be in range 0.0-1.0, got {}",
                        worker_eviction::HIGH_WATERMARK,
                        high
                    ),
                ));
            }
            if let Some(low_str) = config.get_str(worker_eviction::LOW_WATERMARK) {
                if let Ok(low) = low_str.parse::<f64>() {
                    if low >= high {
                        return Err(CommonError::new(
                            ErrorCode::InvalidArgument,
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
            }
        }
    }

    Ok(())
}

/// Validate client-site configuration.
pub fn validate_client(config: &FlatConfig) -> Result<(), CommonError> {
    // Validate client.default_timeout_ms
    if let Some(ms) = config.get_i64(client::DEFAULT_TIMEOUT_MS) {
        if ms <= 0 {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                format!("{} must be > 0", client::DEFAULT_TIMEOUT_MS),
            ));
        }
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
                ErrorCode::InvalidArgument,
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
                ErrorCode::InvalidArgument,
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
                ErrorCode::InvalidArgument,
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
                ErrorCode::InvalidArgument,
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
        if let Some(ttl) = config.get_i64(key) {
            if ttl <= 0 {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
                    format!("{} must be > 0", key),
                ));
            }
        }
    }

    // Validate retry configuration
    if let Some(max_retries) = config.get_i64(client_retry::MAX_RETRIES) {
        if max_retries < 0 {
            return Err(CommonError::new(
                ErrorCode::InvalidArgument,
                format!("{} must be >= 0, got {}", client_retry::MAX_RETRIES, max_retries),
            ));
        }
    }
    if let Some(multiplier_str) = config.get_str(client_retry::BACKOFF_MULTIPLIER) {
        if let Ok(multiplier) = multiplier_str.parse::<f64>() {
            if multiplier <= 0.0 {
                return Err(CommonError::new(
                    ErrorCode::InvalidArgument,
                    format!("{} must be > 0, got {}", client_retry::BACKOFF_MULTIPLIER, multiplier),
                ));
            }
        }
    }

    Ok(())
}
