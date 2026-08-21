// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Configuration file loading for dotted keys with scalar or structured values.

use crate::config::flat::FlatConfig;
use crate::error::{CommonError, CommonErrorKind};
use serde_yaml::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use tracing::info;

/// Load configuration from a YAML file whose top-level keys use dotted names.
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
        let Value::String(key_str) = key else {
            return Err(CommonError::new(
                CommonErrorKind::InvalidArgument,
                "config keys must be strings",
            ));
        };

        result.insert(key_str, val);
    }

    Ok(result)
}
