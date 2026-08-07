// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Flat configuration with dotted-key support.

use crate::error::{CommonError, CommonErrorKind};
use std::collections::BTreeMap;
use std::time::Duration;

use serde_yaml::{Number, Value};

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
    pub fn set<V: IntoYamlValue>(&mut self, key: &str, value: V) {
        self.insert(key.to_string(), value.into_yaml_value());
    }

    pub fn insert_str(&mut self, key: String, value: Value) {
        self.data.insert(key, value);
    }

    /// Get a string value.
    pub fn get_str(&self, key: &str) -> Option<String> {
        self.data.get(key).and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
    }

    /// Get a required string value.
    pub fn get_required_str(&self, key: &str) -> Result<String, CommonError> {
        self.get_str(key).ok_or_else(|| {
            CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("missing required config key: {}", key),
            )
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

    /// Get a required i64 value.
    pub fn get_required_i64(&self, key: &str) -> Result<i64, CommonError> {
        self.get_i64(key).ok_or_else(|| {
            CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("missing or invalid config key: {} (expected i64)", key),
            )
        })
    }

    /// Get a usize value.
    pub fn get_usize(&self, key: &str) -> Option<usize> {
        self.get_i64(key)
            .and_then(|v| if v >= 0 { Some(v as usize) } else { None })
    }

    /// Get a required usize value.
    pub fn get_required_usize(&self, key: &str) -> Result<usize, CommonError> {
        self.get_usize(key).ok_or_else(|| {
            CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("missing or invalid config key: {} (expected usize)", key),
            )
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

    /// Get a duration in milliseconds.
    pub fn get_duration_ms(&self, key: &str) -> Option<Duration> {
        self.get_i64(key).map(|ms| Duration::from_millis(ms.max(0) as u64))
    }

    /// Get a required duration in milliseconds.
    pub fn get_required_duration_ms(&self, key: &str) -> Result<Duration, CommonError> {
        self.get_duration_ms(key).ok_or_else(|| {
            CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("missing or invalid config key: {} (expected duration_ms)", key),
            )
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

    /// Get a sub-configuration with the given prefix.
    ///
    /// Returns a new FlatConfig containing only keys that start with `prefix.`.
    pub fn sub(&self, prefix: &str) -> FlatConfig {
        let prefix_with_dot = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}.", prefix)
        };

        let mut sub_data = BTreeMap::new();
        for (key, value) in &self.data {
            if key.starts_with(&prefix_with_dot) {
                let sub_key = key[prefix_with_dot.len()..].to_string();
                sub_data.insert(sub_key, value.clone());
            }
        }

        FlatConfig::from_map(sub_data)
    }

    /// Get all keys with the given prefix.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        let prefix_with_dot = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}.", prefix)
        };

        self.data
            .keys()
            .filter(|k| k.starts_with(&prefix_with_dot))
            .cloned()
            .collect()
    }

    /// Merge another FlatConfig into this one (other takes precedence).
    pub fn merge(&mut self, other: FlatConfig) {
        for (key, value) in other.data {
            self.data.insert(key, value);
        }
    }

    /// Redact sensitive keys for logging.
    ///
    /// Returns a new FlatConfig with sensitive values replaced with "***".
    pub fn redact_for_log(&self) -> FlatConfig {
        let sensitive_patterns = &["secret", "token", "password", "key", "credential"];
        let mut redacted = BTreeMap::new();

        for (key, value) in &self.data {
            let key_lower = key.to_lowercase();
            let is_sensitive = sensitive_patterns.iter().any(|pattern| key_lower.contains(pattern));

            if is_sensitive {
                redacted.insert(key.clone(), Value::String("***".to_string()));
            } else {
                redacted.insert(key.clone(), value.clone());
            }
        }

        FlatConfig::from_map(redacted)
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

pub trait IntoYamlValue {
    fn into_yaml_value(self) -> Value;
}

impl IntoYamlValue for &str {
    fn into_yaml_value(self) -> Value {
        Value::String(self.to_string())
    }
}
impl IntoYamlValue for String {
    fn into_yaml_value(self) -> Value {
        Value::String(self)
    }
}
impl IntoYamlValue for bool {
    fn into_yaml_value(self) -> Value {
        Value::Bool(self)
    }
}
impl IntoYamlValue for i64 {
    fn into_yaml_value(self) -> Value {
        Value::Number(Number::from(self))
    }
}
impl IntoYamlValue for u64 {
    fn into_yaml_value(self) -> Value {
        Value::Number(Number::from(self))
    }
}
impl IntoYamlValue for Vec<String> {
    fn into_yaml_value(self) -> Value {
        Value::Sequence(self.into_iter().map(Value::String).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    #[test]
    fn typed_getters_parse_supported_values() {
        let mut config = FlatConfig::new();
        config.insert("key1".to_string(), Value::String("value1".to_string()));
        config.insert("key2".to_string(), Value::Number(serde_yaml::Number::from(42)));
        assert_eq!(config.get_str("key1"), Some("value1".to_string()));
        assert_eq!(config.get_str("key2"), Some("42".to_string()));
        assert_eq!(config.get_str("key3"), None);

        config.insert("num1".to_string(), Value::Number(serde_yaml::Number::from(42)));
        config.insert("num2".to_string(), Value::String("100".to_string()));
        assert_eq!(config.get_i64("num1"), Some(42));
        assert_eq!(config.get_i64("num2"), Some(100));
        assert_eq!(config.get_i64("num3"), None);

        config.insert("bool1".to_string(), Value::Bool(true));
        config.insert("bool2".to_string(), Value::String("false".to_string()));
        config.insert("bool3".to_string(), Value::String("yes".to_string()));
        assert_eq!(config.get_bool("bool1"), Some(true));
        assert_eq!(config.get_bool("bool2"), Some(false));
        assert_eq!(config.get_bool("bool3"), Some(true));

        config.insert("size1".to_string(), Value::String("1KB".to_string()));
        config.insert("size2".to_string(), Value::String("2MB".to_string()));
        config.insert("size3".to_string(), Value::Number(serde_yaml::Number::from(1024)));
        assert_eq!(config.get_bytes("size1"), Some(1024));
        assert_eq!(config.get_bytes("size2"), Some(2 * 1024 * 1024));
        assert_eq!(config.get_bytes("size3"), Some(1024));
    }

    #[test]
    fn test_sub() {
        let mut config = FlatConfig::new();
        config.insert(
            "beryl.metadata.rpc.port".to_string(),
            Value::Number(serde_yaml::Number::from(8080)),
        );
        config.insert(
            "beryl.metadata.rpc.host".to_string(),
            Value::String("localhost".to_string()),
        );
        config.insert(
            "beryl.worker.rpc.bind".to_string(),
            Value::String("127.0.0.1:9090".to_string()),
        );

        let sub = config.sub("beryl.metadata.rpc");
        assert_eq!(sub.get_i64("port"), Some(8080));
        assert_eq!(sub.get_str("host"), Some("localhost".to_string()));
        assert_eq!(sub.get_str("kind"), None); // Not in sub
    }

    #[test]
    fn test_redact_for_log() {
        let mut config = FlatConfig::new();
        config.insert("password".to_string(), Value::String("secret123".to_string()));
        config.insert("api_key".to_string(), Value::String("key123".to_string()));
        config.insert("normal_name".to_string(), Value::String("value".to_string()));

        let redacted = config.redact_for_log();
        assert_eq!(redacted.get_str("password"), Some("***".to_string()));
        assert_eq!(redacted.get_str("api_key"), Some("***".to_string()));
        assert_eq!(redacted.get_str("normal_name"), Some("value".to_string()));
    }
}
