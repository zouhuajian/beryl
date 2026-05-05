// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! UFS metadata proxy for unified namespace support.
//!
//! This module provides metadata operations (list/stat/rename/delete) that
//! proxy requests to the underlying UFS based on mount table resolution.
//! The runtime constructs this proxy today, but `MetadataFileSystemServiceImpl`
//! does not call it on the namespace read/write path.

use crate::error::{MetadataError, MetadataResult};
use crate::mount::MountTable;
use common::header::RequestHeader;
use std::sync::Arc;
use tracing::{instrument, warn};
use ufs::{UfsAccess, UfsDirEntry, UfsFileStatus, UfsMeta, UfsRegistry};

/// UFS metadata proxy.
pub struct UfsMetadataProxy {
    mount_table: Arc<MountTable>,
    ufs_registry: Arc<UfsRegistry>,
}

impl UfsMetadataProxy {
    /// Create a new UFS metadata proxy.
    pub fn new(mount_table: Arc<MountTable>, ufs_registry: Arc<UfsRegistry>) -> Self {
        Self {
            mount_table,
            ufs_registry,
        }
    }

    /// Resolve vecton path to UFS instance and relative path.
    fn resolve_ufs(&self, vecton_path: &str) -> MetadataResult<(Arc<dyn UfsMeta>, String)> {
        let (ufs_uri, relative_path) = self
            .mount_table
            .resolve_path(vecton_path)?
            .ok_or_else(|| MetadataError::NotFound(format!("No mount found for path: {}", vecton_path)))?;

        // Parse UFS URI to extract UFS ID
        // Format: "s3://bucket/path" or "hdfs://namenode/path" or "fs:///local/path"
        // For now, we use a simple heuristic: extract the scheme and use it as UFS ID
        // In production, this should be more sophisticated
        let ufs_id = if let Some(stripped) = ufs_uri.strip_prefix("s3://") {
            // Extract bucket name as UFS ID (simplified)
            let parts: Vec<&str> = stripped.splitn(2, '/').collect();
            ufs::UfsId::new(parts[0])
        } else if let Some(stripped) = ufs_uri.strip_prefix("hdfs://") {
            // Extract namenode as UFS ID (simplified)
            let parts: Vec<&str> = stripped.splitn(2, '/').collect();
            ufs::UfsId::new(parts[0])
        } else if ufs_uri.starts_with("fs://") {
            ufs::UfsId::new("local-fs")
        } else {
            return Err(MetadataError::InvalidArgument(format!(
                "Unsupported UFS URI format: {}",
                ufs_uri
            )));
        };

        let ufs = self
            .ufs_registry
            .get(&ufs_id)
            .ok_or_else(|| MetadataError::NotFound(format!("UFS instance not found: {:?}", ufs_id)))?;

        // Get UfsMeta trait object
        let ufs_meta: Arc<dyn UfsMeta> = ufs.clone();

        Ok((ufs_meta, relative_path))
    }

    /// Resolve vecton path to UFS access (for data operations).
    fn resolve_ufs_access(&self, vecton_path: &str) -> MetadataResult<(Arc<dyn UfsAccess>, String)> {
        let (ufs_uri, relative_path) = self
            .mount_table
            .resolve_path(vecton_path)?
            .ok_or_else(|| MetadataError::NotFound(format!("No mount found for path: {}", vecton_path)))?;

        // Parse UFS URI to extract UFS ID (same logic as resolve_ufs)
        let ufs_id = if let Some(stripped) = ufs_uri.strip_prefix("s3://") {
            let parts: Vec<&str> = stripped.splitn(2, '/').collect();
            ufs::UfsId::new(parts[0])
        } else if let Some(stripped) = ufs_uri.strip_prefix("hdfs://") {
            let parts: Vec<&str> = stripped.splitn(2, '/').collect();
            ufs::UfsId::new(parts[0])
        } else if ufs_uri.starts_with("fs://") {
            ufs::UfsId::new("local-fs")
        } else {
            return Err(MetadataError::InvalidArgument(format!(
                "Unsupported UFS URI format: {}",
                ufs_uri
            )));
        };

        let ufs = self
            .ufs_registry
            .get(&ufs_id)
            .ok_or_else(|| MetadataError::NotFound(format!("UFS instance not found: {:?}", ufs_id)))?;

        Ok((ufs, relative_path))
    }

    /// Get file/directory status from UFS.
    #[instrument(skip(self), fields(path = %vecton_path))]
    pub async fn stat(&self, vecton_path: &str, ctx: &RequestHeader) -> MetadataResult<UfsFileStatus> {
        let (ufs, relative_path) = self.resolve_ufs(vecton_path)?;

        ufs.stat(&relative_path, ctx)
            .await
            .map_err(|e| MetadataError::Internal(format!("UFS stat failed: {}", e)))
    }

    /// List entries under a prefix (directory listing).
    #[instrument(skip(self), fields(prefix = %vecton_path))]
    pub async fn list(&self, vecton_path: &str, ctx: &RequestHeader) -> MetadataResult<Vec<UfsDirEntry>> {
        let (ufs, relative_path) = self.resolve_ufs(vecton_path)?;

        ufs.list(&relative_path, ctx)
            .await
            .map_err(|e| MetadataError::Internal(format!("UFS list failed: {}", e)))
    }

    /// Rename or move a file/directory in UFS.
    /// Supports cross-mount rename using copy+delete fallback.
    #[instrument(skip(self), fields(from = %from_path, to = %to_path))]
    pub async fn rename(&self, from_path: &str, to_path: &str, ctx: &RequestHeader) -> MetadataResult<()> {
        let mount_from = self.mount_table.resolve_path(from_path)?;
        let mount_to = self.mount_table.resolve_path(to_path)?;

        if mount_from.is_none() || mount_to.is_none() {
            return Err(MetadataError::InvalidArgument("Both paths must be mounted".to_string()));
        }

        // Check if both paths are in the same mount
        if mount_from == mount_to {
            // Same mount: use native rename
            let (ufs_from, relative_from) = self.resolve_ufs(from_path)?;
            let (_, relative_to) = self.resolve_ufs(to_path)?;

            ufs_from
                .rename(&relative_from, &relative_to, ctx)
                .await
                .map_err(|e| MetadataError::Internal(format!("UFS rename failed: {}", e)))
        } else {
            // Cross-mount rename: use copy + delete
            warn!(
                from_path = %from_path,
                to_path = %to_path,
                "Cross-mount rename: using copy+delete fallback (non-atomic)"
            );

            self.rename_cross_mount(from_path, to_path, ctx).await
        }
    }

    /// Cross-mount rename using copy + delete.
    async fn rename_cross_mount(&self, from_path: &str, to_path: &str, ctx: &RequestHeader) -> MetadataResult<()> {
        // Get UFS access for both paths (need UfsAccess for read/write)
        let (ufs_from, relative_from) = self.resolve_ufs_access(from_path)?;
        let (ufs_to, relative_to) = self.resolve_ufs_access(to_path)?;

        // Read all data from source
        let data = ufs_from
            .read_all(&relative_from, ctx)
            .await
            .map_err(|e| MetadataError::Internal(format!("Failed to read from source UFS: {}", e)))?;

        // Write data to destination
        ufs_to.write_all(&relative_to, data, ctx).await.map_err(|e| {
            // If write fails, source file is still intact
            MetadataError::Internal(format!("Failed to write to destination UFS: {}", e))
        })?;

        // Delete source file
        // Note: If delete fails, we have a duplicate file, but operation is "successful"
        // In production, we might want to track this and clean up later
        if let Err(e) = ufs_from.delete(&relative_from, false, ctx).await {
            warn!(
                from_path = %from_path,
                error = %e,
                "Cross-mount rename: failed to delete source file after copy"
            );
            // Continue anyway - the rename is "complete" from user's perspective
        }

        Ok(())
    }

    /// Delete a file or directory from UFS.
    #[instrument(skip(self), fields(path = %vecton_path, recursive = %recursive))]
    pub async fn delete(&self, vecton_path: &str, recursive: bool, ctx: &RequestHeader) -> MetadataResult<()> {
        let (ufs, relative_path) = self.resolve_ufs(vecton_path)?;

        ufs.delete(&relative_path, recursive, ctx)
            .await
            .map_err(|e| MetadataError::Internal(format!("UFS delete failed: {}", e)))
    }

    /// Check if a path exists in UFS.
    #[instrument(skip(self), fields(path = %vecton_path))]
    pub async fn exists(&self, vecton_path: &str, ctx: &RequestHeader) -> MetadataResult<bool> {
        let (ufs, relative_path) = self.resolve_ufs(vecton_path)?;

        ufs.exists(&relative_path, ctx)
            .await
            .map_err(|e| MetadataError::Internal(format!("UFS exists check failed: {}", e)))
    }
}
