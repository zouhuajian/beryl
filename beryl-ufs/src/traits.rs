// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Traits for UFS operations.

use async_trait::async_trait;
use beryl_common::header::RequestHeader;
use bytes::Bytes;

use crate::error::UfsError;

/// File status information from UFS.
#[derive(Clone, Debug)]
pub struct UfsFileStatus {
    /// Whether the path is a directory.
    pub is_dir: bool,
    /// File size in bytes (None for directories or if unavailable).
    pub size: Option<u64>,
    /// Last modification time (Unix timestamp, None if unavailable).
    pub modified: Option<i64>,
    /// ETag or version identifier (None if unavailable).
    pub etag: Option<String>,
}

/// Directory entry from listing operations.
#[derive(Clone, Debug)]
pub struct UfsDirEntry {
    /// Path of the entry (relative to the listing prefix or absolute).
    pub path: String,
    /// Whether this entry is a directory.
    pub is_dir: bool,
    /// File size (None for directories or if unavailable).
    pub size: Option<u64>,
}

/// Metadata operations for UFS.
#[async_trait]
pub trait UfsMeta: Send + Sync {
    /// Get file/directory status.
    async fn stat(&self, path: &str, ctx: &RequestHeader) -> Result<UfsFileStatus, UfsError>;

    /// List entries under a prefix (directory listing).
    async fn list(&self, prefix: &str, ctx: &RequestHeader) -> Result<Vec<UfsDirEntry>, UfsError>;

    /// Rename or move a file/directory.
    ///
    /// Note: For backends that don't support native rename, this may use
    /// copy + delete fallback if enabled in capabilities.
    async fn rename(&self, from: &str, to: &str, ctx: &RequestHeader) -> Result<(), UfsError>;

    /// Delete a file or directory.
    ///
    /// If `recursive` is true and the path is a directory, delete all contents.
    /// If the backend doesn't support recursive delete, this may fail or
    /// require manual traversal.
    async fn delete(&self, path: &str, recursive: bool, ctx: &RequestHeader) -> Result<(), UfsError>;

    /// Create a directory (and parent directories if needed).
    ///
    /// For object storage backends, this may be a no-op.
    async fn mkdirs(&self, path: &str, ctx: &RequestHeader) -> Result<(), UfsError>;

    /// Check if a path exists.
    async fn exists(&self, path: &str, ctx: &RequestHeader) -> Result<bool, UfsError>;
}

/// Data operations for UFS.
#[async_trait]
pub trait UfsData: Send + Sync {
    /// Read a byte range from a file.
    ///
    /// Returns the actual bytes read (may be less than `len` if EOF is reached).
    /// For strict mode, use `read_range_strict` or check the returned length.
    async fn read_range(&self, path: &str, offset: u64, len: usize, ctx: &RequestHeader) -> Result<Bytes, UfsError>;

    /// Read the entire file.
    async fn read_all(&self, path: &str, ctx: &RequestHeader) -> Result<Bytes, UfsError>;

    /// Write data to a file (overwrites if exists).
    async fn write_all(&self, path: &str, data: Bytes, ctx: &RequestHeader) -> Result<(), UfsError>;
}

/// Combined trait for both metadata and data operations.
#[async_trait]
pub trait UfsAccess: UfsMeta + UfsData {}

// Blanket implementation: any type implementing both traits also implements UfsAccess.
impl<T: UfsMeta + UfsData> UfsAccess for T {}
