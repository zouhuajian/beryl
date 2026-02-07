// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Transport × Storage combination validator.
//!
//! This module validates that the selected NetTransport and LocalIoStorage
//! combination is compatible, especially for zero-copy operations.

use common::error::{CommonError, CommonErrorCode};
use std::sync::Arc;
use tracing::{error, info, warn};
use transport::local_io::{build_local_io, LocalIoConfig, LocalIoKind};
use transport::net::{build_net_transport, NetTransportBox, NetTransportConfig, NetTransportKind};
use transport::{LocalIoEngine, NetTransportCapability};

/// Transport capabilities for validation.
#[derive(Clone, Debug)]
pub struct TransportCapabilities {
    /// Whether transport supports zero-copy payload operations.
    pub zero_copy_payload: bool,
    /// Whether transport supports file_region send (e.g., sendfile).
    pub supports_file_region: bool,
    /// Transport kind.
    pub kind: String,
}

/// Storage capabilities for validation.
#[derive(Clone, Debug)]
pub struct StorageCapabilities {
    /// Whether storage supports zero-copy read (e.g., mmap).
    pub zero_copy_read: bool,
    /// Whether storage supports file_region (e.g., sendfile).
    pub supports_file_region: bool,
    /// Storage kind.
    pub kind: String,
}

/// Combination validation result.
#[derive(Clone, Debug)]
pub enum ComboValidationResult {
    /// Combination is valid.
    Valid,
    /// Combination is invalid, but fallback is allowed.
    InvalidWithFallback { reason: String, fallback_transport: String },
    /// Combination is invalid and must fail.
    Invalid { reason: String },
}

/// Get transport capabilities from a transport instance.
pub fn get_transport_capabilities(transport: &NetTransportBox) -> TransportCapabilities {
    match transport {
        NetTransportBox::Grpc(grpc) => {
            TransportCapabilities {
                zero_copy_payload: grpc.zero_copy_payload(),
                supports_file_region: false, // gRPC doesn't support sendfile
                kind: "grpc".to_string(),
            }
        }
    }
}

/// Get storage capabilities from a storage instance.
pub fn get_storage_capabilities(_storage: &Arc<dyn LocalIoEngine>) -> StorageCapabilities {
    // For now, we check based on the type
    // In the future, we could add a capability trait to LocalIoEngine
    StorageCapabilities {
        zero_copy_read: false,       // Default: no mmap support
        supports_file_region: false, // Default: no sendfile support
        kind: "unknown".to_string(),
    }
}

/// Get storage capabilities from kind string.
pub fn get_storage_capabilities_from_kind(kind: &str) -> StorageCapabilities {
    match kind {
        "fs" => StorageCapabilities {
            zero_copy_read: false,
            supports_file_region: true, // File system can use sendfile
            kind: "fs".to_string(),
        },
        "io_uring" => StorageCapabilities {
            zero_copy_read: true, // io_uring can use registered buffers
            supports_file_region: true,
            kind: "io_uring".to_string(),
        },
        "spdk" => StorageCapabilities {
            zero_copy_read: true, // SPDK uses DMA
            supports_file_region: true,
            kind: "spdk".to_string(),
        },
        _ => StorageCapabilities {
            zero_copy_read: false,
            supports_file_region: false,
            kind: kind.to_string(),
        },
    }
}

/// Validate transport × storage combination.
pub fn validate_combo(
    transport_caps: &TransportCapabilities,
    storage_caps: &StorageCapabilities,
    zero_copy_required: bool,
    allow_fallback: bool,
    fallback_transport: Option<&str>,
) -> ComboValidationResult {
    // If zero-copy is not required, any combination is valid
    if !zero_copy_required {
        return ComboValidationResult::Valid;
    }

    // Check if combination supports zero-copy
    let supports_zero_copy = (transport_caps.zero_copy_payload && storage_caps.zero_copy_read)
        || (transport_caps.supports_file_region && storage_caps.supports_file_region);

    if supports_zero_copy {
        return ComboValidationResult::Valid;
    }

    // Combination doesn't support zero-copy
    let reason = format!(
        "transport={} (zero_copy={}, file_region={}) × storage={} (zero_copy={}, file_region={}) does not support zero-copy",
        transport_caps.kind,
        transport_caps.zero_copy_payload,
        transport_caps.supports_file_region,
        storage_caps.kind,
        storage_caps.zero_copy_read,
        storage_caps.supports_file_region
    );

    if allow_fallback {
        let fallback = fallback_transport.unwrap_or("grpc");
        warn!(
            reason = %reason,
            fallback = fallback,
            "Invalid transport×storage combo, falling back to {}",
            fallback
        );

        // Record metrics (TODO: implement metrics when metrics system is ready)
        // metrics::counter!("worker_combo_fallback_total", ...).increment(1);

        return ComboValidationResult::InvalidWithFallback {
            reason,
            fallback_transport: fallback.to_string(),
        };
    }

    error!(
        reason = %reason,
        "Invalid transport×storage combo, zero-copy required but not supported"
    );

    // Record metrics (TODO: implement metrics when metrics system is ready)
    // metrics::counter!("worker_combo_invalid_total", ...).increment(1);

    ComboValidationResult::Invalid { reason }
}

/// Build and validate transport × storage combination.
pub fn build_and_validate_combo(
    transport_kind: &str,
    storage_kind: &str,
    zero_copy_required: bool,
    allow_fallback: bool,
    fallback_transport: Option<&str>,
) -> Result<(NetTransportBox, Arc<dyn LocalIoEngine>, String), CommonError> {
    // Build transport
    let net_kind = match transport_kind {
        "grpc" => NetTransportKind::Grpc,
        "quic" => NetTransportKind::Quic,
        "rdma" => NetTransportKind::Rdma,
        _ => {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("Unknown transport kind: {}", transport_kind),
            ));
        }
    };

    let transport_config = NetTransportConfig::new(net_kind);
    let transport = build_net_transport(&transport_config).map_err(|e| {
        CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("Failed to build transport: {}", e),
        )
    })?;

    // Build storage
    let storage_kind_enum = match storage_kind {
        "fs" => LocalIoKind::Fs,
        "io_uring" => LocalIoKind::IoUring,
        "spdk" => LocalIoKind::Spdk,
        _ => {
            return Err(CommonError::new(
                CommonErrorCode::InvalidArgument,
                format!("Unknown storage kind: {}", storage_kind),
            ));
        }
    };

    let storage_config = LocalIoConfig::new(storage_kind_enum);
    let storage = build_local_io(&storage_config).map_err(|e| {
        CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("Failed to build storage: {}", e),
        )
    })?;

    // Get capabilities
    let transport_caps = get_transport_capabilities(&transport);
    let storage_caps = get_storage_capabilities_from_kind(storage_kind);

    // Validate combination
    let validation_result = validate_combo(
        &transport_caps,
        &storage_caps,
        zero_copy_required,
        allow_fallback,
        fallback_transport,
    );

    match validation_result {
        ComboValidationResult::Valid => {
            info!(
                transport = %transport_caps.kind,
                storage = %storage_caps.kind,
                "Transport×storage combo validated successfully"
            );

            // Record effective combo (TODO: implement metrics when metrics system is ready)
            // metrics::gauge!("worker_effective_combo_transport", ...).set(1.0);
            // metrics::gauge!("worker_effective_combo_storage", ...).set(1.0);

            Ok((transport, storage, transport_kind.to_string()))
        }
        ComboValidationResult::InvalidWithFallback {
            fallback_transport: fallback,
            ..
        } => {
            // Try fallback transport
            let fallback_kind = match fallback.as_str() {
                "grpc" => NetTransportKind::Grpc,
                "quic" => NetTransportKind::Quic,
                "rdma" => NetTransportKind::Rdma,
                _ => {
                    return Err(CommonError::new(
                        CommonErrorCode::InvalidArgument,
                        format!("Unknown fallback transport kind: {}", fallback),
                    ));
                }
            };

            let fallback_config = NetTransportConfig::new(fallback_kind);
            let fallback_transport_box = build_net_transport(&fallback_config).map_err(|e| {
                CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    format!("Failed to build fallback transport: {}", e),
                )
            })?;

            let fallback_caps = get_transport_capabilities(&fallback_transport_box);
            let fallback_validation = validate_combo(
                &fallback_caps,
                &storage_caps,
                zero_copy_required,
                false, // Don't allow another fallback
                None,
            );

            match fallback_validation {
                ComboValidationResult::Valid => {
                    info!(
                        original_transport = %transport_caps.kind,
                        fallback_transport = %fallback,
                        storage = %storage_caps.kind,
                        "Using fallback transport"
                    );
                    Ok((fallback_transport_box, storage, fallback))
                }
                _ => Err(CommonError::new(
                    CommonErrorCode::InvalidArgument,
                    format!(
                        "Fallback transport {} also invalid with storage {}",
                        fallback, storage_caps.kind
                    ),
                )),
            }
        }
        ComboValidationResult::Invalid { reason } => Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("Invalid transport×storage combo: {}", reason),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_combo_zero_copy_not_required() {
        let transport_caps = TransportCapabilities {
            zero_copy_payload: false,
            supports_file_region: false,
            kind: "grpc".to_string(),
        };
        let storage_caps = StorageCapabilities {
            zero_copy_read: false,
            supports_file_region: false,
            kind: "fs".to_string(),
        };

        let result = validate_combo(&transport_caps, &storage_caps, false, false, None);
        assert!(matches!(result, ComboValidationResult::Valid));
    }

    #[test]
    fn test_validate_combo_zero_copy_required_valid() {
        let transport_caps = TransportCapabilities {
            zero_copy_payload: true,
            supports_file_region: true,
            kind: "rdma".to_string(),
        };
        let storage_caps = StorageCapabilities {
            zero_copy_read: true,
            supports_file_region: true,
            kind: "io_uring".to_string(),
        };

        let result = validate_combo(&transport_caps, &storage_caps, true, false, None);
        assert!(matches!(result, ComboValidationResult::Valid));
    }

    #[test]
    fn test_validate_combo_zero_copy_required_invalid_no_fallback() {
        let transport_caps = TransportCapabilities {
            zero_copy_payload: false,
            supports_file_region: false,
            kind: "grpc".to_string(),
        };
        let storage_caps = StorageCapabilities {
            zero_copy_read: false,
            supports_file_region: false,
            kind: "fs".to_string(),
        };

        let result = validate_combo(&transport_caps, &storage_caps, true, false, None);
        assert!(matches!(result, ComboValidationResult::Invalid { .. }));
    }

    #[test]
    fn test_validate_combo_zero_copy_required_invalid_with_fallback() {
        let transport_caps = TransportCapabilities {
            zero_copy_payload: false,
            supports_file_region: false,
            kind: "grpc".to_string(),
        };
        let storage_caps = StorageCapabilities {
            zero_copy_read: false,
            supports_file_region: false,
            kind: "fs".to_string(),
        };

        let result = validate_combo(&transport_caps, &storage_caps, true, true, Some("rdma"));
        assert!(matches!(result, ComboValidationResult::InvalidWithFallback { .. }));
    }
}
