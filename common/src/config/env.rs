// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Environment variable override support.

use crate::config::flat::FlatConfig;
use serde_yaml::Value;
use std::env;

/// Load configuration from environment variables.
///
/// Converts environment variable names to dotted keys:
/// - `METADATA_RPC_PORT` -> `metadata.rpc.port`
/// - `TRANSPORT_MAX_INFLIGHT` -> `transport.max_inflight`
pub fn load_from_env() -> FlatConfig {
    let mut config = FlatConfig::new();

    for (key, value) in env::vars() {
        // Only process uppercase keys (convention for env vars)
        if key.chars().all(|c| c.is_uppercase() || c == '_' || c.is_ascii_digit()) {
            let dotted_key = env_key_to_dotted(&key);
            // Try to parse as number or bool, otherwise treat as string
            let yaml_value = if let Ok(num) = value.parse::<i64>() {
                Value::Number(serde_yaml::Number::from(num))
            } else if let Ok(num) = value.parse::<f64>() {
                // Try to convert to i64 if it's a whole number
                if num.fract() == 0.0 {
                    Value::Number(serde_yaml::Number::from(num as i64))
                } else {
                    // For non-integer floats, use string representation
                    Value::String(value)
                }
            } else if let Ok(b) = value.parse::<bool>() {
                Value::Bool(b)
            } else {
                Value::String(value)
            };
            config.insert(dotted_key, yaml_value);
        }
    }

    config
}

/// Convert environment variable name to dotted key.
///
/// Examples:
/// - `METADATA_RPC_PORT` -> `metadata.rpc.port`
/// - `TRANSPORT_MAX_INFLIGHT_REQUESTS` -> `transport.max_inflight.requests`
fn env_key_to_dotted(env_key: &str) -> String {
    env_key.to_lowercase().split('_').collect::<Vec<_>>().join(".")
}

/// Convert dotted key to environment variable name.
///
/// Examples:
/// - `metadata.rpc.port` -> `METADATA_RPC_PORT`
/// - `transport.max_inflight.requests` -> `TRANSPORT_MAX_INFLIGHT_REQUESTS`
pub fn dotted_to_env_key(dotted: &str) -> String {
    dotted
        .split('.')
        .map(|s| s.to_uppercase())
        .collect::<Vec<_>>()
        .join("_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_key_to_dotted() {
        assert_eq!(env_key_to_dotted("METADATA_RPC_PORT"), "metadata.rpc.port");
        assert_eq!(env_key_to_dotted("TRANSPORT_MAX_INFLIGHT"), "transport.max.inflight");
    }

    #[test]
    fn test_dotted_to_env_key() {
        assert_eq!(dotted_to_env_key("metadata.rpc.port"), "METADATA_RPC_PORT");
        assert_eq!(dotted_to_env_key("transport.max.inflight"), "TRANSPORT_MAX_INFLIGHT");
    }
}
