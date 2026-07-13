// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Path resolver: converts paths to inode IDs via mount resolution and dentry walking.
//!
//! This module provides the core path resolution logic for the PathService adapter layer.
//! It does NOT write any path indices to storage - it only reads from dentry/inode CFs.

use crate::error::{MetadataError, MetadataResult};
use crate::mount::{mount_prefix_matches_path, MountEntry, MountTable};
use crate::raft::RocksDBStorage;
use std::sync::Arc;
use types::fs::{Inode, InodeId};
use types::ids::MountId;
use types::GroupName;

/// Mount context: information about the mount point for a resolved path.
#[derive(Clone, Debug)]
pub struct MountContext {
    pub mount_id: MountId,
    pub mount_epoch: u64,
    pub owner_group_name: GroupName,
    pub root_inode_id: InodeId,
}

/// Provider-neutral facts produced by path resolution.
///
/// Existing-target flows require `inode_id`; parent/create flows require
/// `parent_inode_id` and `name`. Mount-root resolution has no parent/name.
#[derive(Clone, Debug)]
pub struct ResolvedPath {
    pub mount_ctx: MountContext,
    pub parent_inode_id: Option<InodeId>,
    pub name: Option<String>,
    pub inode_id: Option<InodeId>,
}

impl ResolvedPath {
    pub fn expect_inode(&self) -> MetadataResult<InodeId> {
        self.inode_id
            .ok_or_else(|| MetadataError::NotFound("resolved path has no target inode".to_string()))
    }

    pub fn expect_parent(&self) -> MetadataResult<InodeId> {
        self.parent_inode_id
            .ok_or_else(|| MetadataError::InvalidArgument("resolved path has no parent inode".to_string()))
    }

    pub fn expect_name(&self) -> MetadataResult<&str> {
        self.name
            .as_deref()
            .ok_or_else(|| MetadataError::InvalidArgument("resolved path has no terminal name".to_string()))
    }
}

/// Path resolver: converts paths to inode IDs.
pub struct PathResolver {
    mount_table: Arc<MountTable>,
    storage: Arc<RocksDBStorage>,
}

impl PathResolver {
    pub(crate) fn new(mount_table: Arc<MountTable>, storage: Arc<RocksDBStorage>) -> Self {
        Self { mount_table, storage }
    }

    /// Normalize a path:
    /// - Remove empty path (return error)
    /// - Remove duplicate '/' (collapse to single '/')
    /// - Remove trailing '/' (except for root '/')
    /// - Reject paths containing '\0'
    pub fn normalize(path: &str) -> MetadataResult<String> {
        if path.is_empty() {
            return Err(MetadataError::InvalidArgument("Path cannot be empty".to_string()));
        }

        if path.contains('\0') {
            return Err(MetadataError::InvalidArgument(
                "Path cannot contain null byte".to_string(),
            ));
        }

        // Split by '/' and filter out empty components
        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        if components.is_empty() {
            // Path is "/" or all slashes
            return Ok("/".to_string());
        }

        // Rejoin with single '/'
        let normalized = format!("/{}", components.join("/"));

        Ok(normalized)
    }

    /// Resolve mount: find the longest matching mount prefix.
    /// Returns (mount_entry, relative_components).
    fn resolve_mount(&self, path: &str) -> MetadataResult<(MountEntry, Vec<String>)> {
        let normalized = Self::normalize(path)?;

        // Find longest matching mount prefix
        let mounts = self.mount_table.list_mounts();
        let mut best_match: Option<(MountEntry, Vec<String>)> = None;
        let mut best_prefix_len = 0;

        for mount in mounts {
            let prefix = &mount.mount_prefix;
            if mount_prefix_matches_path(prefix, &normalized) {
                let prefix_len = prefix.len();
                if prefix_len > best_prefix_len {
                    // Extract relative path components
                    let relative = if prefix_len == normalized.len() {
                        vec![]
                    } else if normalized.as_bytes()[prefix_len] == b'/' {
                        // Skip the '/' after prefix
                        normalized[prefix_len + 1..]
                            .split('/')
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .collect()
                    } else {
                        // No '/' after prefix (shouldn't happen with normalized paths)
                        normalized[prefix_len..]
                            .split('/')
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .collect()
                    };
                    best_match = Some((mount.clone(), relative));
                    best_prefix_len = prefix_len;
                }
            }
        }

        best_match.ok_or_else(|| MetadataError::NotFound(format!("No mount found for path: {}", normalized)))
    }

    /// Resolve path to its owning mount and mount-relative components without
    /// requiring the namespace entries to exist.
    pub(crate) fn resolve_mount_components(&self, path: &str) -> MetadataResult<(MountContext, Vec<String>)> {
        let (mount_entry, components) = self.resolve_mount(path)?;
        Ok((
            MountContext {
                mount_id: mount_entry.mount_id,
                mount_epoch: mount_entry.mount_epoch,
                owner_group_name: mount_entry.namespace_owner_group_name,
                root_inode_id: mount_entry.root_inode_id,
            },
            components,
        ))
    }

    /// Walk dentry tree and return the final inode id after following all components.
    fn walk_dentry(&self, root_inode_id: InodeId, components: &[String]) -> MetadataResult<InodeId> {
        let mut current_inode_id = root_inode_id;

        for component in components {
            // Get dentry
            let child_inode_id = self.storage.get_dentry(current_inode_id, component)?.ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "Entry not found: {} (parent inode: {})",
                    component, current_inode_id
                ))
            })?;

            current_inode_id = child_inode_id;
        }

        Ok(current_inode_id)
    }

    /// Resolve path to ResolvedPath (for create/unlink/rename operations).
    /// Returns parent_inode_id and name for the target entry.
    pub fn resolve_path(&self, path: &str) -> MetadataResult<ResolvedPath> {
        let (mount_entry, components) = self.resolve_mount(path)?;

        if components.is_empty() {
            // Path is mount root
            return Err(MetadataError::InvalidArgument(
                "Cannot operate on mount root".to_string(),
            ));
        }

        // Split into parent components and name
        let (parent_components, name) = components.split_at(components.len() - 1);
        let name = name[0].clone();

        // Walk to parent directory.
        let parent_inode_id = if parent_components.is_empty() {
            mount_entry.root_inode_id
        } else {
            self.walk_dentry(mount_entry.root_inode_id, parent_components)?
        };

        // Verify parent is a directory
        let parent_inode = self
            .storage
            .get_inode(parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;

        if !parent_inode.kind.is_dir() {
            return Err(MetadataError::NotDir(format!(
                "Parent is not a directory: {}",
                parent_inode_id
            )));
        }

        // Optionally check if entry already exists (for lookup operations)
        let inode_id = self.storage.get_dentry(parent_inode_id, &name)?;

        Ok(ResolvedPath {
            mount_ctx: MountContext {
                mount_id: mount_entry.mount_id,
                mount_epoch: mount_entry.mount_epoch,
                owner_group_name: mount_entry.namespace_owner_group_name,
                root_inode_id: mount_entry.root_inode_id,
            },
            parent_inode_id: Some(parent_inode_id),
            name: Some(name),
            inode_id,
        })
    }

    /// Resolve path to a unified ResolvedPath with `inode_id` populated.
    /// Returns the inode_id for the target path.
    pub fn resolve_inode(&self, path: &str) -> MetadataResult<ResolvedPath> {
        let (mount_entry, components) = self.resolve_mount(path)?;

        let (inode_id, parent_inode_id, name) = if components.is_empty() {
            (mount_entry.root_inode_id, None, None)
        } else {
            let (parent_components, name) = components.split_at(components.len() - 1);
            let parent_inode_id = if parent_components.is_empty() {
                mount_entry.root_inode_id
            } else {
                self.walk_dentry(mount_entry.root_inode_id, parent_components)?
            };
            let name = name[0].clone();
            let inode_id = self.storage.get_dentry(parent_inode_id, &name)?.ok_or_else(|| {
                MetadataError::NotFound(format!("Entry not found: {} (parent inode: {})", name, parent_inode_id))
            })?;
            (inode_id, Some(parent_inode_id), Some(name))
        };

        Ok(ResolvedPath {
            mount_ctx: MountContext {
                mount_id: mount_entry.mount_id,
                mount_epoch: mount_entry.mount_epoch,
                owner_group_name: mount_entry.namespace_owner_group_name,
                root_inode_id: mount_entry.root_inode_id,
            },
            parent_inode_id,
            name,
            inode_id: Some(inode_id),
        })
    }

    /// Resolve two paths for rename operation.
    /// Returns (src_resolved, dst_resolved).
    /// If paths are in different mounts, returns error (caller should convert to EXDEV).
    pub fn resolve_rename(&self, src_path: &str, dst_path: &str) -> MetadataResult<(ResolvedPath, ResolvedPath)> {
        let src_resolved = self.resolve_path(src_path)?;
        let dst_resolved = self.resolve_path(dst_path)?;

        // Check if same mount
        if src_resolved.mount_ctx.mount_id != dst_resolved.mount_ctx.mount_id {
            return Err(MetadataError::CrossMountRename(format!(
                "Cross-mount rename not allowed: src_mount={:?}, dst_mount={:?}",
                src_resolved.mount_ctx.mount_id, dst_resolved.mount_ctx.mount_id
            )));
        }

        Ok((src_resolved, dst_resolved))
    }

    pub(crate) fn get_inode(&self, inode_id: InodeId) -> MetadataResult<Option<Inode>> {
        self.storage.get_inode(inode_id)
    }

    pub(crate) fn get_dentry(&self, parent_inode_id: InodeId, name: &str) -> MetadataResult<Option<InodeId>> {
        self.storage.get_dentry(parent_inode_id, name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mount::{DataIoPolicy, MountKind, MountTable};
    use crate::raft::RocksDBStorage;
    use tempfile::TempDir;
    use types::fs::{FileAttrs, Inode, InodeId};
    use types::ids::DataHandleId;
    use types::GroupName;

    fn test_resolver(mount_table: Arc<MountTable>, storage: Arc<RocksDBStorage>) -> PathResolver {
        PathResolver::new(mount_table, storage)
    }

    #[test]
    fn test_normalize() {
        assert_eq!(PathResolver::normalize("/").unwrap(), "/");
        assert_eq!(PathResolver::normalize("/a/b").unwrap(), "/a/b");
        assert_eq!(PathResolver::normalize("//a//b//").unwrap(), "/a/b");
        assert_eq!(PathResolver::normalize("/a/b/").unwrap(), "/a/b");
        assert!(PathResolver::normalize("").is_err());
        assert!(PathResolver::normalize("/a\0b").is_err());
    }

    #[test]
    fn test_resolve_mount() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());

        // Create test mount
        let root_inode_id = InodeId::new(1);
        mount_table
            .create_mount(
                "/mnt/s3".to_string(),
                crate::mount::MountKind::External,
                Some("s3://bucket/path".to_string()),
                crate::mount::DataIoPolicy::Allow,
                GroupName::parse("g1").unwrap(),
                root_inode_id,
            )
            .unwrap();

        let resolver = test_resolver(mount_table.clone(), storage);

        // Test mount resolution
        let (mount, components) = resolver.resolve_mount("/mnt/s3/file.txt").unwrap();
        assert_eq!(mount.mount_prefix, "/mnt/s3");
        assert_eq!(components, vec!["file.txt"]);

        let (_mount, components) = resolver.resolve_mount("/mnt/s3/dir/file.txt").unwrap();
        assert_eq!(components, vec!["dir", "file.txt"]);

        // Test longest prefix match
        mount_table
            .create_mount(
                "/mnt".to_string(),
                crate::mount::MountKind::External,
                Some("s3://bucket2".to_string()),
                crate::mount::DataIoPolicy::Allow,
                GroupName::parse("g2").unwrap(),
                InodeId::new(2),
            )
            .unwrap();

        let (mount, _) = resolver.resolve_mount("/mnt/s3/file.txt").unwrap();
        assert_eq!(mount.mount_prefix, "/mnt/s3"); // Should match longer prefix

        mount_table
            .create_mount(
                "/".to_string(),
                crate::mount::MountKind::Internal,
                None,
                crate::mount::DataIoPolicy::Allow,
                GroupName::parse("g3").unwrap(),
                InodeId::new(3),
            )
            .unwrap();

        let (mount, components) = resolver.resolve_mount("/mnt2/file.txt").unwrap();
        assert_eq!(mount.mount_prefix, "/");
        assert_eq!(components, vec!["mnt2", "file.txt"]);

        let (mount, components) = resolver.resolve_mount("/mnt/s3x/file.txt").unwrap();
        assert_eq!(mount.mount_prefix, "/mnt");
        assert_eq!(components, vec!["s3x", "file.txt"]);

        let (mount, components) = resolver.resolve_mount("/mnt/s3/").unwrap();
        assert_eq!(mount.mount_prefix, "/mnt/s3");
        assert!(components.is_empty());
    }

    #[test]
    fn resolve_inode_returns_parent_and_terminal_name_for_nested_path() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());

        let root_inode_id = InodeId::new(100);
        let mount = mount_table
            .create_mount(
                "/mnt/test".to_string(),
                MountKind::External,
                Some("file:///tmp/test".to_string()),
                DataIoPolicy::Allow,
                GroupName::parse("g1").unwrap(),
                root_inode_id,
            )
            .unwrap();

        let mut root_attrs = FileAttrs::new();
        root_attrs.mode = 0o755;
        storage
            .put_inode(&Inode::new_dir(root_inode_id, root_attrs, mount.mount_id))
            .unwrap();

        let dir_a = InodeId::new(101);
        let mut dir_a_attrs = FileAttrs::new();
        dir_a_attrs.mode = 0o755;
        storage
            .put_inode(&Inode::new_dir(dir_a, dir_a_attrs, mount.mount_id))
            .unwrap();
        storage.put_dentry(root_inode_id, "a", dir_a).unwrap();

        let dir_b = InodeId::new(102);
        let mut dir_b_attrs = FileAttrs::new();
        dir_b_attrs.mode = 0o755;
        storage
            .put_inode(&Inode::new_dir(dir_b, dir_b_attrs, mount.mount_id))
            .unwrap();
        storage.put_dentry(dir_a, "b", dir_b).unwrap();

        let file_c = InodeId::new(103);
        let mut file_attrs = FileAttrs::new();
        file_attrs.mode = 0o644;
        storage
            .put_inode(&Inode::new_file(
                file_c,
                file_attrs,
                mount.mount_id,
                DataHandleId::new(1),
            ))
            .unwrap();
        storage.put_dentry(dir_b, "c", file_c).unwrap();

        let resolver = test_resolver(mount_table, storage);
        let resolved = resolver.resolve_inode("/mnt/test/a/b/c").unwrap();
        assert_eq!(resolved.expect_inode().unwrap(), file_c);
        assert_eq!(resolved.expect_parent().unwrap(), dir_b);
        assert_eq!(resolved.expect_name().unwrap(), "c");
    }

    #[test]
    fn resolve_path_returns_parent_and_terminal_name() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());

        let root_inode_id = InodeId::new(200);
        let mount = mount_table
            .create_mount(
                "/mnt/test2".to_string(),
                MountKind::External,
                Some("file:///tmp/test2".to_string()),
                DataIoPolicy::Allow,
                GroupName::parse("g2").unwrap(),
                root_inode_id,
            )
            .unwrap();

        let mut root_attrs = FileAttrs::new();
        root_attrs.mode = 0o755;
        storage
            .put_inode(&Inode::new_dir(root_inode_id, root_attrs, mount.mount_id))
            .unwrap();

        let dir_a = InodeId::new(201);
        let mut dir_a_attrs = FileAttrs::new();
        dir_a_attrs.mode = 0o755;
        storage
            .put_inode(&Inode::new_dir(dir_a, dir_a_attrs, mount.mount_id))
            .unwrap();
        storage.put_dentry(root_inode_id, "a", dir_a).unwrap();

        let dir_b = InodeId::new(202);
        let mut dir_b_attrs = FileAttrs::new();
        dir_b_attrs.mode = 0o755;
        storage
            .put_inode(&Inode::new_dir(dir_b, dir_b_attrs, mount.mount_id))
            .unwrap();
        storage.put_dentry(dir_a, "b", dir_b).unwrap();

        let resolver = test_resolver(mount_table, storage);
        let resolved = resolver.resolve_path("/mnt/test2/a/b/new-file").unwrap();
        assert_eq!(resolved.expect_parent().unwrap(), dir_b);
        assert_eq!(resolved.expect_name().unwrap(), "new-file");
        assert!(resolved.inode_id.is_none());
    }
}
