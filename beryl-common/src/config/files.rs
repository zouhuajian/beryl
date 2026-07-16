// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Configuration file loading.

use crate::config::flat::FlatConfig;
use crate::error::{CommonError, CommonErrorKind};
use serde_yaml::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use tracing::{info, warn};

/// Load configuration from a YAML file with flat dotted keys.
pub fn load_from_yaml_file<P: AsRef<Path>>(path: P) -> Result<FlatConfig, CommonError> {
    let path = path.as_ref();
    info!(path = %path.display(), "loading config from YAML file");

    let content = fs::read_to_string(path).map_err(|e| {
        CommonError::new(
            CommonErrorKind::Io,
            format!("failed to read config file {}: {}", path.display(), e),
        )
    })?;

    let value: Value = serde_yaml::from_str(&content).map_err(|e| {
        CommonError::new(
            CommonErrorKind::InvalidArgument,
            format!("failed to parse YAML file {}: {}", path.display(), e),
        )
    })?;

    let flat = flat_mapping(value)?;
    info!(keys = flat.keys().count(), "loaded config from YAML file");
    Ok(FlatConfig::from_map(flat))
}

fn flat_mapping(value: Value) -> Result<BTreeMap<String, Value>, CommonError> {
    let mut result = BTreeMap::new();

    let Value::Mapping(map) = value else {
        return Err(CommonError::new(
            CommonErrorKind::InvalidArgument,
            "config file must be a YAML mapping with flat keys",
        ));
    };

    for (key, val) in map {
        let key_str = match key {
            Value::String(s) => s,
            Value::Number(n) => n.to_string(),
            _ => continue,
        };

        if matches!(val, Value::Mapping(_)) {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!(
                    "nested YAML config is not supported; use flat keys such as observe.log.format instead of {key_str}"
                ),
            ));
        }

        if let Value::Sequence(entries) = &val
            && entries.iter().any(|entry| matches!(entry, Value::Mapping(_)))
        {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!(
                    "nested YAML config is not supported; use flat keys such as observe.log.format instead of {key_str}"
                ),
            ));
        }

        result.insert(key_str, val);
    }

    Ok(result)
}

/// Load configuration from defaults and an optional YAML file.
///
/// Sources are merged in order (later sources override earlier ones):
/// 1. Default values
/// 2. YAML file (if provided)
pub fn load_merged(default: FlatConfig, yaml_path: Option<&Path>) -> Result<FlatConfig, CommonError> {
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
        writeln!(file, "worker.rpc.max_inflight: 100").unwrap();
        writeln!(file, "observe.log.format: compact").unwrap();
        writeln!(file, "observe.metrics.prometheus.path: /metrics").unwrap();
        drop(file);

        let config = load_from_yaml_file(&path).unwrap();
        assert_eq!(config.get_i64("metadata.rpc.port"), Some(8080));
        assert_eq!(config.get_i64("worker.rpc.max_inflight"), Some(100));
        assert_eq!(config.get_str("observe.log.format"), Some("compact".to_string()));
        assert_eq!(
            config.get_str("observe.metrics.prometheus.path"),
            Some("/metrics".to_string())
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_reject_nested_observe_yaml() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_config_nested_observe.yaml");
        let mut file = File::create(&path).unwrap();
        writeln!(file, "observe:").unwrap();
        writeln!(file, "  log:").unwrap();
        writeln!(file, "    format: compact").unwrap();
        drop(file);

        let err = load_from_yaml_file(&path).unwrap_err();
        assert!(err.message.contains("flat keys"), "{err:?}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_reject_nested_non_observe_yaml() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_config_nested_metadata.yaml");
        let mut file = File::create(&path).unwrap();
        writeln!(file, "metadata:").unwrap();
        writeln!(file, "  rpc:").unwrap();
        writeln!(file, "    port: 8080").unwrap();
        drop(file);

        let err = load_from_yaml_file(&path).unwrap_err();
        assert!(err.message.contains("flat keys"), "{err:?}");
        let _ = std::fs::remove_file(&path);
    }
}
