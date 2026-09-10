// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Flat configuration with dotted-key support.

use crate::error::{CommonError, CommonErrorKind};
use std::collections::BTreeMap;
use std::time::Duration;

use serde_yaml::Value;

/// Flat configuration storage using dotted keys.
#[derive(Clone, Debug)]
pub struct FlatConfig {
    /// Internal storage: key -> value
    data: BTreeMap<String, Value>,
}

impl FlatConfig {
    /// Create an empty FlatConfig.
    pub fn new() -> Self {
        Self { data: BTreeMap::new() }
    }

    /// Create from a BTreeMap.
    pub fn from_map(data: BTreeMap<String, Value>) -> Self {
        Self { data }
    }

    /// Insert a key-value pair.
    pub fn insert(&mut self, key: String, value: Value) {
        self.data.insert(key, value);
    }

    #[inline]
    pub fn set<V: Into<Value>>(&mut self, key: &str, value: V) {
        self.insert(key.to_string(), value.into());
    }

    /// Get a string value.
    pub fn get_str(&self, key: &str) -> Option<String> {
        self.data.get(key).and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
    }

    /// Get an i64 value.
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.data.get(key).and_then(|v| match v {
            Value::Number(n) => n.as_i64(),
            Value::String(s) => s.parse().ok(),
            _ => None,
        })
    }

    /// Get a bool value.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.data.get(key).and_then(|v| match v {
            Value::Bool(b) => Some(*b),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Some(true),
                "false" | "0" | "no" | "off" => Some(false),
                _ => None,
            },
            _ => None,
        })
    }

    /// Get a duration from an integer millisecond value or a value with a unit.
    ///
    /// Supported units are `ms`, `s`, `min`, `h`, and `d`.
    pub fn get_duration(&self, key: &str) -> Option<Duration> {
        self.data.get(key).and_then(parse_duration)
    }

    /// Get bytes from an integer or a human-readable binary size.
    pub fn get_bytes(&self, key: &str) -> Option<usize> {
        self.data.get(key).and_then(parse_bytes)
    }

    /// Get a YAML sequence containing only scalar strings.
    pub fn get_string_list(&self, key: &str) -> Option<Vec<String>> {
        self.data.get(key).and_then(|value| match value {
            Value::Sequence(values) => values
                .iter()
                .map(|value| match value {
                    Value::String(value) => Some(value.clone()),
                    Value::Number(value) => Some(value.to_string()),
                    _ => None,
                })
                .collect(),
            _ => None,
        })
    }

    /// Get a structured YAML mapping value.
    pub fn get_mapping(&self, key: &str) -> Option<&serde_yaml::Mapping> {
        self.data.get(key).and_then(Value::as_mapping)
    }

    /// Get a string value or return the provided default when the key is absent.
    pub fn string_or(&self, key: &str, default: &str) -> Result<String, CommonError> {
        if !self.contains_key(key) {
            return Ok(default.to_string());
        }
        self.get_str(key).ok_or_else(|| invalid_config(key, "must be a string"))
    }

    /// Get a boolean value or return the provided default when the key is absent.
    pub fn bool_or(&self, key: &str, default: bool) -> Result<bool, CommonError> {
        if !self.contains_key(key) {
            return Ok(default);
        }
        self.get_bool(key)
            .ok_or_else(|| invalid_config(key, "must be a boolean"))
    }

    /// Get a non-zero TCP/UDP port or return the provided default when the key is absent.
    pub fn port_or(&self, key: &str, default: u16) -> Result<u16, CommonError> {
        if !self.contains_key(key) {
            return Ok(default);
        }
        let value = self
            .get_i64(key)
            .ok_or_else(|| invalid_config(key, "must be an integer"))?;
        u16::try_from(value)
            .ok()
            .filter(|port| *port != 0)
            .ok_or_else(|| invalid_config(key, "must be in range 1-65535"))
    }

    /// Get a positive u32 value or return the provided default when the key is absent.
    pub fn positive_u32_or(&self, key: &str, default: u32) -> Result<u32, CommonError> {
        if !self.contains_key(key) {
            return Ok(default);
        }
        let value = self
            .get_i64(key)
            .ok_or_else(|| invalid_config(key, "must be an integer"))?;
        u32::try_from(value)
            .ok()
            .filter(|value| *value != 0)
            .ok_or_else(|| invalid_config(key, "must be greater than zero and fit u32"))
    }

    /// Get a positive usize value or return the provided default when the key is absent.
    pub fn positive_usize_or(&self, key: &str, default: usize) -> Result<usize, CommonError> {
        if !self.contains_key(key) {
            return Ok(default);
        }
        let value = self
            .get_i64(key)
            .ok_or_else(|| invalid_config(key, "must be an integer"))?;
        usize::try_from(value)
            .ok()
            .filter(|value| *value != 0)
            .ok_or_else(|| invalid_config(key, "must be greater than zero"))
    }

    /// Get a positive duration in milliseconds or return the provided default when the key is absent.
    pub fn duration_ms_or(&self, key: &str, default: u64) -> Result<u64, CommonError> {
        if !self.contains_key(key) {
            return Ok(default);
        }
        let duration = self
            .get_duration(key)
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| invalid_config(key, "must be a positive duration"))?;
        u64::try_from(duration.as_millis()).map_err(|_| invalid_config(key, "is too large"))
    }

    /// Get a byte size that fits u32 or return the provided default when the key is absent.
    pub fn bytes_u32_or(&self, key: &str, default: u32) -> Result<u32, CommonError> {
        if !self.contains_key(key) {
            return Ok(default);
        }
        let bytes = self
            .get_bytes(key)
            .ok_or_else(|| invalid_config(key, "must be a size such as 1MiB"))?;
        u32::try_from(bytes).map_err(|_| invalid_config(key, "is too large"))
    }

    /// Get a byte size that fits u64 or return the provided default when the key is absent.
    pub fn bytes_u64_or(&self, key: &str, default: u64) -> Result<u64, CommonError> {
        if !self.contains_key(key) {
            return Ok(default);
        }
        self.get_bytes(key)
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| invalid_config(key, "must be a size such as 1GiB"))
    }

    /// Get all keys.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.data.keys()
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }
}

fn invalid_config(key: &str, detail: &str) -> CommonError {
    CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {detail}"))
}

fn parse_duration(value: &Value) -> Option<Duration> {
    match value {
        Value::Number(number) => number.as_u64().map(Duration::from_millis),
        Value::String(raw) => {
            let raw = raw.trim().to_ascii_lowercase();
            let (number, multiplier) = if let Some(number) = raw.strip_suffix("ms") {
                (number, 1)
            } else if let Some(number) = raw.strip_suffix("min") {
                (number, 60_000)
            } else if let Some(number) = raw.strip_suffix('s') {
                (number, 1_000)
            } else if let Some(number) = raw.strip_suffix('h') {
                (number, 60 * 60_000)
            } else if let Some(number) = raw.strip_suffix('d') {
                (number, 24 * 60 * 60_000)
            } else {
                return raw.parse::<u64>().ok().map(Duration::from_millis);
            };
            number
                .trim()
                .parse::<u64>()
                .ok()
                .and_then(|number| number.checked_mul(multiplier))
                .map(Duration::from_millis)
        }
        _ => None,
    }
}

fn parse_bytes(value: &Value) -> Option<usize> {
    match value {
        Value::Number(number) => number.as_u64().and_then(|value| usize::try_from(value).ok()),
        Value::String(raw) => {
            let normalized = raw.trim().to_ascii_uppercase();
            let units = [
                ("GIB", 1024usize.pow(3)),
                ("MIB", 1024usize.pow(2)),
                ("KIB", 1024usize),
                ("GB", 1024usize.pow(3)),
                ("MB", 1024usize.pow(2)),
                ("KB", 1024usize),
                ("B", 1usize),
            ];
            for (suffix, multiplier) in units {
                if let Some(number) = normalized.strip_suffix(suffix) {
                    return number
                        .trim()
                        .parse::<usize>()
                        .ok()
                        .and_then(|number| number.checked_mul(multiplier));
                }
            }
            normalized.parse().ok()
        }
        _ => None,
    }
}

impl Default for FlatConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_values_use_defaults_and_parse_supported_units() {
        let mut config = FlatConfig::new();
        config.set("string", "value");
        config.set("bool", "yes");
        config.set("port", 19090i64);
        config.set("u32", 10i64);
        config.set("usize", 20i64);
        config.set("duration", "2s");
        config.set("bytes-u32", "1MiB");
        config.set("bytes-u64", "1GiB");

        assert_eq!(config.string_or("string", "default").unwrap(), "value");
        assert!(config.bool_or("bool", false).unwrap());
        assert_eq!(config.port_or("port", 1).unwrap(), 19090);
        assert_eq!(config.positive_u32_or("u32", 1).unwrap(), 10);
        assert_eq!(config.positive_usize_or("usize", 1).unwrap(), 20);
        assert_eq!(config.duration_ms_or("duration", 1).unwrap(), 2_000);
        assert_eq!(config.bytes_u32_or("bytes-u32", 1).unwrap(), 1024 * 1024);
        assert_eq!(config.bytes_u64_or("bytes-u64", 1).unwrap(), 1024 * 1024 * 1024);
        assert_eq!(config.string_or("missing", "default").unwrap(), "default");
    }

    #[test]
    fn typed_values_reject_invalid_and_non_positive_input() {
        let mut config = FlatConfig::new();
        config.set("bool", "sometimes");
        config.set("port", 0i64);
        config.set("u32", -1i64);
        config.set("usize", 0i64);
        config.set("duration", "0s");
        config.set("bytes", "large");

        for result in [
            config.bool_or("bool", true).map(|_| ()),
            config.port_or("port", 1).map(|_| ()),
            config.positive_u32_or("u32", 1).map(|_| ()),
            config.positive_usize_or("usize", 1).map(|_| ()),
            config.duration_ms_or("duration", 1).map(|_| ()),
            config.bytes_u32_or("bytes", 1).map(|_| ()),
            config.bytes_u64_or("bytes", 1).map(|_| ()),
        ] {
            assert_eq!(result.unwrap_err().kind, CommonErrorKind::InvalidArgument);
        }
    }
}
