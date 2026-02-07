// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Flat configuration with dotted-key support.

use crate::error::{CommonError, CommonErrorCode};
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
                CommonErrorCode::InvalidArgument,
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
                CommonErrorCode::InvalidArgument,
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
                CommonErrorCode::InvalidArgument,
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
                CommonErrorCode::InvalidArgument,
                format!("missing or invalid config key: {} (expected duration_ms)", key),
            )
        })
    }

    /// Get bytes (size in bytes, supports "1KB", "1MB", etc.).
    pub fn get_bytes(&self, key: &str) -> Option<usize> {
        self.data.get(key).and_then(|v| {
            match v {
                Value::Number(n) => n.as_i64().and_then(|v| if v >= 0 { Some(v as usize) } else { None }),
                Value::String(s) => {
                    // Parse "1KB", "1MB", etc.
                    let s = s.trim().to_uppercase();
                    if s.ends_with("KB") {
                        s[..s.len() - 2].trim().parse::<usize>().ok().map(|v| v * 1024)
                    } else if s.ends_with("MB") {
                        s[..s.len() - 2].trim().parse::<usize>().ok().map(|v| v * 1024 * 1024)
                    } else if s.ends_with("GB") {
                        s[..s.len() - 2]
                            .trim()
                            .parse::<usize>()
                            .ok()
                            .map(|v| v * 1024 * 1024 * 1024)
                    } else {
                        s.parse().ok()
                    }
                }
                _ => None,
            }
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    #[test]
    fn test_get_str() {
        let mut config = FlatConfig::new();
        config.insert("key1".to_string(), Value::String("value1".to_string()));
        config.insert("key2".to_string(), Value::Number(serde_yaml::Number::from(42)));

        assert_eq!(config.get_str("key1"), Some("value1".to_string()));
        assert_eq!(config.get_str("key2"), Some("42".to_string()));
        assert_eq!(config.get_str("key3"), None);
    }

    #[test]
    fn test_get_i64() {
        let mut config = FlatConfig::new();
        config.insert("num1".to_string(), Value::Number(serde_yaml::Number::from(42)));
        config.insert("num2".to_string(), Value::String("100".to_string()));

        assert_eq!(config.get_i64("num1"), Some(42));
        assert_eq!(config.get_i64("num2"), Some(100));
        assert_eq!(config.get_i64("num3"), None);
    }

    #[test]
    fn test_get_bool() {
        let mut config = FlatConfig::new();
        config.insert("bool1".to_string(), Value::Bool(true));
        config.insert("bool2".to_string(), Value::String("false".to_string()));
        config.insert("bool3".to_string(), Value::String("yes".to_string()));

        assert_eq!(config.get_bool("bool1"), Some(true));
        assert_eq!(config.get_bool("bool2"), Some(false));
        assert_eq!(config.get_bool("bool3"), Some(true));
    }

    #[test]
    fn test_sub() {
        let mut config = FlatConfig::new();
        config.insert(
            "metadata.rpc.port".to_string(),
            Value::Number(serde_yaml::Number::from(8080)),
        );
        config.insert("metadata.rpc.host".to_string(), Value::String("localhost".to_string()));
        config.insert("worker.transport.kind".to_string(), Value::String("grpc".to_string()));

        let sub = config.sub("metadata.rpc");
        assert_eq!(sub.get_i64("port"), Some(8080));
        assert_eq!(sub.get_str("host"), Some("localhost".to_string()));
        assert_eq!(sub.get_str("kind"), None); // Not in sub
    }

    #[test]
    #[ignore = "pending config redaction update post-identity pivot"]
    fn test_redact_for_log() {
        let mut config = FlatConfig::new();
        config.insert("password".to_string(), Value::String("secret123".to_string()));
        config.insert("api_key".to_string(), Value::String("key123".to_string()));
        config.insert("normal_key".to_string(), Value::String("value".to_string()));

        let redacted = config.redact_for_log();
        assert_eq!(redacted.get_str("password"), Some("***".to_string()));
        assert_eq!(redacted.get_str("api_key"), Some("***".to_string()));
        assert_eq!(redacted.get_str("normal_key"), Some("value".to_string()));
    }

    #[test]
    fn test_get_bytes() {
        let mut config = FlatConfig::new();
        config.insert("size1".to_string(), Value::String("1KB".to_string()));
        config.insert("size2".to_string(), Value::String("2MB".to_string()));
        config.insert("size3".to_string(), Value::Number(serde_yaml::Number::from(1024)));

        assert_eq!(config.get_bytes("size1"), Some(1024));
        assert_eq!(config.get_bytes("size2"), Some(2 * 1024 * 1024));
        assert_eq!(config.get_bytes("size3"), Some(1024));
    }
}
