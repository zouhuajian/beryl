// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Configuration file loading.

use crate::config::flat::FlatConfig;
use crate::error::{CommonError, CommonErrorCode};
use serde_yaml::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use tracing::{info, warn};

/// Load configuration from a YAML file with flat dotted keys.
///
/// The YAML file can contain either:
/// 1. Flat keys with dots: `metadata.rpc.port: 8080`
/// 2. Nested structure: `metadata: { rpc: { port: 8080 } }`
///
/// Both will be flattened to dotted keys.
pub fn load_from_yaml_file<P: AsRef<Path>>(path: P) -> Result<FlatConfig, CommonError> {
    let path = path.as_ref();
    info!(path = %path.display(), "loading config from YAML file");

    let content = fs::read_to_string(path).map_err(|e| {
        CommonError::new(
            CommonErrorCode::Io,
            format!("failed to read config file {}: {}", path.display(), e),
        )
    })?;

    let value: Value = serde_yaml::from_str(&content).map_err(|e| {
        CommonError::new(
            CommonErrorCode::InvalidArgument,
            format!("failed to parse YAML file {}: {}", path.display(), e),
        )
    })?;

    let flat = flatten_value(value, String::new());
    info!(keys = flat.keys().count(), "loaded config from YAML file");
    Ok(FlatConfig::from_map(flat))
}

/// Flatten a YAML Value into dotted keys.
fn flatten_value(value: Value, prefix: String) -> BTreeMap<String, Value> {
    let mut result = BTreeMap::new();

    match value {
        Value::Mapping(map) => {
            for (key, val) in map {
                let key_str = match key {
                    Value::String(s) => s,
                    Value::Number(n) => n.to_string(),
                    _ => continue,
                };

                let new_prefix = if prefix.is_empty() {
                    key_str
                } else {
                    format!("{}.{}", prefix, key_str)
                };

                match val {
                    Value::Mapping(_) | Value::Sequence(_) => {
                        // Recursively flatten nested structures
                        let nested = flatten_value(val, new_prefix.clone());
                        result.extend(nested);
                    }
                    _ => {
                        // Leaf value
                        result.insert(new_prefix, val);
                    }
                }
            }
        }
        Value::Sequence(_) => {
            // Sequences are kept as-is (could be enhanced to support indexed access)
            if !prefix.is_empty() {
                result.insert(prefix, value);
            }
        }
        _ => {
            // Scalar value
            if !prefix.is_empty() {
                result.insert(prefix, value);
            }
        }
    }

    result
}

/// Load configuration from multiple sources and merge them.
///
/// Sources are merged in order (later sources override earlier ones):
/// 1. Default values
/// 2. YAML file (if provided)
/// 3. Environment variables
pub fn load_merged(default: FlatConfig, yaml_path: Option<&Path>, use_env: bool) -> Result<FlatConfig, CommonError> {
    let mut config = default;

    // Load from YAML file
    if let Some(path) = yaml_path {
        if path.exists() {
            let file_config = load_from_yaml_file(path)?;
            config.merge(file_config);
        } else {
            warn!(path = %path.display(), "config file does not exist, skipping");
        }
    }

    // Load from environment variables (highest priority)
    if use_env {
        let env_config = crate::config::env::load_from_env();
        config.merge(env_config);
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_load_flat_yaml() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_config_flat.yaml");
        let mut file = File::create(&path).unwrap();
        writeln!(file, "metadata.rpc.port: 8080").unwrap();
        writeln!(file, "worker.transport.max_inflight_requests: 100").unwrap();
        drop(file);

        let config = load_from_yaml_file(&path).unwrap();
        assert_eq!(config.get_i64("metadata.rpc.port"), Some(8080));
        assert_eq!(config.get_i64("worker.transport.max_inflight_requests"), Some(100));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_load_nested_yaml() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_config_nested.yaml");
        let mut file = File::create(&path).unwrap();
        writeln!(file, "metadata:").unwrap();
        writeln!(file, "  rpc:").unwrap();
        writeln!(file, "    port: 8080").unwrap();
        drop(file);

        let config = load_from_yaml_file(&path).unwrap();
        assert_eq!(config.get_i64("metadata.rpc.port"), Some(8080));
        let _ = std::fs::remove_file(&path);
    }
}
