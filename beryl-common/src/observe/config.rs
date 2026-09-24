// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Observability configuration structures.

use crate::config::FlatConfig;
use crate::error::{CommonError, CommonErrorKind};
use serde::{Deserialize, Serialize};

const LOG_LEVEL: &str = "beryl.logging.level";
const LOG_FORMAT: &str = "beryl.logging.format";
const LOG_OUTPUT: &str = "beryl.logging.output";

/// Process observability configuration loaded from shared logging keys.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Format: "compact" or "json".
    pub format: String,
    /// Output stream: "stderr" or "stdout".
    pub output: String,
    /// EnvFilter directive string.
    pub level: String,
}

impl ObservabilityConfig {
    /// Parse the shared logging configuration model.
    pub fn from_flat(flat: &FlatConfig) -> Result<Self, CommonError> {
        let config = Self {
            format: required_str(flat, LOG_FORMAT)?,
            output: required_str(flat, LOG_OUTPUT)?,
            level: required_str(flat, LOG_LEVEL)?,
        };

        validate_log_config(&config)?;
        Ok(config)
    }
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

fn validate_log_config(config: &ObservabilityConfig) -> Result<(), CommonError> {
    match config.format.as_str() {
        "compact" | "json" => {}
        _ => return Err(invalid_config(LOG_FORMAT, "must be compact or json")),
    }
    match config.output.as_str() {
        "stderr" | "stdout" => {}
        _ => return Err(invalid_config(LOG_OUTPUT, "must be stderr or stdout")),
    }
    Ok(())
}

fn invalid_config(key: &'static str, detail: impl Into<String>) -> CommonError {
    CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {}", detail.into()))
}
