// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! UFS specification and configuration types.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A unique identifier for a UFS instance.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct UfsId(pub String);

impl UfsId {
    /// Creates a new UfsId from a string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UfsId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for UfsId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for UfsId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Backend kind for UFS.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    /// AWS S3 or S3-compatible storage.
    S3,
    /// Alibaba Cloud OSS.
    Oss,
    /// HDFS (Hadoop Distributed File System).
    Hdfs,
    /// Local filesystem.
    Fs,
}

impl fmt::Display for BackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackendKind::S3 => write!(f, "s3"),
            BackendKind::Oss => write!(f, "oss"),
            BackendKind::Hdfs => write!(f, "hdfs"),
            BackendKind::Fs => write!(f, "fs"),
        }
    }
}

/// S3 backend configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 endpoint URL.
    pub endpoint: String,
    /// S3 bucket name.
    pub bucket: String,
    /// Root path prefix within the bucket.
    #[serde(default)]
    pub root: Option<String>,
    /// AWS access key ID.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// AWS secret access key.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// AWS region.
    #[serde(default)]
    pub region: Option<String>,
}

/// OSS backend configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OssConfig {
    /// OSS endpoint URL.
    pub endpoint: String,
    /// OSS bucket name.
    pub bucket: String,
    /// Root path prefix within the bucket.
    #[serde(default)]
    pub root: Option<String>,
    /// OSS access key ID.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// OSS access key secret.
    #[serde(default)]
    pub access_key_secret: Option<String>,
}

/// HDFS backend configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HdfsConfig {
    /// HDFS namenode address (e.g., "hdfs://namenode:9000").
    pub namenode: String,
    /// Root path in HDFS.
    #[serde(default)]
    pub root: Option<String>,
    /// Optional WebHDFS URL (alternative to namenode).
    #[serde(default)]
    pub webhdfs_url: Option<String>,
}

/// Local filesystem backend configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FsConfig {
    /// Root directory path.
    pub root: String,
}

/// Backend-specific configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackendConfig {
    S3(S3Config),
    Oss(OssConfig),
    Hdfs(HdfsConfig),
    Fs(FsConfig),
}

/// Capability overrides for a UFS instance.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CapabilityOverrides {
    /// Enable rename fallback (copy + delete) when backend doesn't support native rename.
    #[serde(default)]
    pub rename_fallback_enabled: bool,
}

/// Complete UFS specification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UfsSpec {
    /// Unique identifier for this UFS instance.
    pub id: UfsId,
    /// Backend kind.
    pub kind: BackendKind,
    /// Optional logical mount name.
    #[serde(default)]
    pub mount: Option<String>,
    /// Backend-specific configuration.
    pub config: BackendConfig,
    /// Optional capability overrides.
    #[serde(default)]
    pub capability_overrides: Option<CapabilityOverrides>,
}

impl UfsSpec {
    /// Creates a new UFS specification.
    pub fn new(id: impl Into<UfsId>, kind: BackendKind, config: BackendConfig) -> Self {
        Self {
            id: id.into(),
            kind,
            mount: None,
            config,
            capability_overrides: None,
        }
    }

    /// Sets the mount name.
    pub fn with_mount(mut self, mount: impl Into<String>) -> Self {
        self.mount = Some(mount.into());
        self
    }

    /// Sets capability overrides.
    pub fn with_capability_overrides(mut self, overrides: CapabilityOverrides) -> Self {
        self.capability_overrides = Some(overrides);
        self
    }
}
