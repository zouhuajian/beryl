// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Observability configuration structures.

use crate::config::{FlatConfig, keys::logging};
use crate::error::{CommonError, CommonErrorKind};
use serde::{Deserialize, Serialize};

/// Process observability configuration loaded from shared logging keys.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Logging configuration.
    pub log: LogConfig,
}

impl ObservabilityConfig {
    /// Parse the shared logging configuration model.
    pub fn from_flat(flat: &FlatConfig) -> Result<Self, CommonError> {
        let config = Self {
            log: LogConfig {
                format: required_str(flat, logging::FORMAT)?,
                output: required_str(flat, logging::OUTPUT)?,
                level: required_str(flat, logging::LEVEL)?,
            },
        };

        validate_log_config(&config.log)?;
        Ok(config)
    }
}

/// Logging configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogConfig {
    /// Format: "compact" or "json".
    pub format: String,
    /// Output stream: "stderr" or "stdout".
    pub output: String,
    /// EnvFilter directive string.
    pub level: String,
}

/// Service information for observability initialization.
#[derive(Clone, Debug)]
pub struct ServiceInfo {
    /// Service name.
    pub name: String,
    /// Service version.
    pub version: String,
    /// Environment.
    pub environment: String,
}

fn required_str(flat: &FlatConfig, key: &'static str) -> Result<String, CommonError> {
    let value = flat
        .get_str(key)
        .ok_or_else(|| invalid_config(key, "must be present and be a string"))?;
    if value.trim().is_empty() {
        return Err(invalid_config(key, "must not be empty"));
    }
    Ok(value)
}

fn validate_log_config(config: &LogConfig) -> Result<(), CommonError> {
    match config.format.as_str() {
        "compact" | "json" => {}
        _ => return Err(invalid_config(logging::FORMAT, "must be compact or json")),
    }
    match config.output.as_str() {
        "stderr" | "stdout" => {}
        _ => return Err(invalid_config(logging::OUTPUT, "must be stderr or stdout")),
    }
    Ok(())
}

fn invalid_config(key: &'static str, detail: impl Into<String>) -> CommonError {
    CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {}", detail.into()))
}
