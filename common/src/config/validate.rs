// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Generic shared configuration validation.
//!
//! Module-specific typed config owns module key validation.

use crate::config::flat::FlatConfig;
use crate::config::keys::observe_metrics;
use crate::error::{CommonError, CommonErrorCode};

/// Validate shared core-site configuration primitives.
pub fn validate_core(config: &FlatConfig) -> Result<(), CommonError> {
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

    Ok(())
}

/// Validate shared client-site configuration primitives.
pub fn validate_client(_config: &FlatConfig) -> Result<(), CommonError> {
    Ok(())
}
