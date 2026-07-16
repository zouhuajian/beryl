// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! UFS (Underlying File System) adapter boundary for Beryl.
//!
//! This crate isolates external backend configuration, capability description, and
//! path-based adapter construction. Current Beryl file reads and writes do not use UFS;
//! they go through metadata-authorized worker block storage.
//!
//! # Architecture
//!
//! - **Specification**: `UfsSpec` defines a UFS instance (backend kind, configuration, capabilities)
//! - **Registry**: `UfsRegistry` manages multiple UFS instances dynamically
//! - **Traits**: `UfsMeta` and `UfsData` provide unified operations across backends
//! - **Implementation**: `OpendalUfs` implements the traits using OpenDAL
//!
//! # Adapter usage
//!
//! ```ignore
//! use beryl_ufs::{UfsRegistry, UfsSpec, BackendKind, BackendConfig, FsConfig};
//!
//! // Create a registry
//! let registry = UfsRegistry::new();
//!
//! // Add a filesystem backend
//! let spec = UfsSpec::new(
//!     "local-fs",
//!     BackendKind::Fs,
//!     BackendConfig::Fs(FsConfig {
//!         root: "/data".to_string(),
//!     }),
//! );
//! registry.upsert(spec)?;
//!
//! // Use the UFS instance as an external adapter boundary.
//! if let Some(ufs) = registry.get(&beryl_ufs::UfsId::new("local-fs")) {
//!     let data = ufs.read_all("path/to/file").await?;
//!     let status = ufs.stat("path/to/file").await?;
//! }
//! ```
//!
//! # Feature flags
//!
//! - `ufs-jvm`: enables HDFS support (OpenDAL `services-hdfs`) and tests that require
//!   linking against `libjvm`. Disabled by default so `cargo test` works on hosts
//!   without a JVM. Enable explicitly via `cargo test -p beryl-ufs --features ufs-jvm`.

#![forbid(unsafe_code)]

pub mod capability;
pub mod error;
pub mod opendal_impl;
pub mod registry;
pub mod spec;
pub mod traits;

// Re-export commonly used types
pub use capability::Capability;
pub use error::UfsError;
pub use opendal_impl::OpendalUfs;
pub use registry::UfsRegistry;
pub use spec::{
    BackendConfig, BackendKind, CapabilityOverrides, FsConfig, HdfsConfig, OssConfig, S3Config, UfsId, UfsSpec,
};
pub use traits::{UfsAccess, UfsData, UfsDirEntry, UfsFileStatus, UfsMeta};

use std::fs;
use std::path::Path;
use tracing::info;

/// Loads UFS specifications from a JSON file.
///
/// The file should contain a JSON array of `UfsSpec` objects.
///
/// # Example JSON format
///
/// ```json
/// [
///   {
///     "id": "s3-backend",
///     "kind": "s3",
///     "config": {
///       "type": "s3",
///       "endpoint": "https://s3.amazonaws.com",
///       "bucket": "my-bucket",
///       "root": "prefix/",
///       "access_key_id": "AKIA...",
///       "secret_access_key": "...",
///       "region": "us-east-1"
///     }
///   }
/// ]
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The file cannot be read
/// - The JSON is invalid
/// - The JSON structure doesn't match `UfsSpec`
pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<UfsSpec>, UfsError> {
    let path = path.as_ref();
    info!(path = %path.display(), "loading UFS specs from file");

    let content = fs::read_to_string(path)
        .map_err(|e| UfsError::InvalidSpec(format!("failed to read file {}: {}", path.display(), e)))?;

    let specs: Vec<UfsSpec> =
        serde_json::from_str(&content).map_err(|e| UfsError::InvalidSpec(format!("failed to parse JSON: {}", e)))?;

    info!(count = specs.len(), "loaded UFS specs from file");
    Ok(specs)
}

#[cfg(test)]
mod tests;
