// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Path resolver: converts paths to inode IDs via mount resolution and dentry walking.
//!
//! This module provides the core path resolution logic for metadata filesystem operations.
//! It does NOT write any path indices to storage - it only reads from dentry/inode CFs.

use crate::error::{MetadataError, MetadataResult};
use crate::mount::{mount_prefix_matches_path, MountEntry, MountTable};
use crate::raft::RocksDBStorage;
use beryl_types::fs::InodeId;
use beryl_types::ids::MountId;
use beryl_types::GroupName;
use std::sync::Arc;

/// Maximum accepted UTF-8 path length, measured in bytes before and after normalization.
pub(crate) const MAX_PATH_BYTES: usize = 4096;
/// Maximum accepted UTF-8 path-component length, measured in bytes.
pub(crate) const MAX_PATH_COMPONENT_BYTES: usize = 255;
/// Maximum number of non-empty components in one normalized path.
pub(crate) const MAX_PATH_COMPONENTS: usize = 256;

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
    /// Mount root through the resolved target, or through its parent when the
    /// final entry does not exist.
    pub ancestor_inode_ids: Vec<InodeId>,
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
    /// - Enforce fixed byte, component-length, and component-count limits
    pub fn normalize(path: &str) -> MetadataResult<String> {
        if path.is_empty() {
            return Err(MetadataError::InvalidArgument("Path cannot be empty".to_string()));
        }

        if path.len() > MAX_PATH_BYTES {
            return Err(MetadataError::InvalidArgument(format!(
                "Path exceeds {MAX_PATH_BYTES} bytes"
            )));
        }

        if path.contains('\0') {
            return Err(MetadataError::InvalidArgument(
                "Path cannot contain null byte".to_string(),
            ));
        }

        // Split by '/' and filter out empty components
        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if components.len() > MAX_PATH_COMPONENTS {
            return Err(MetadataError::InvalidArgument(format!(
                "Path exceeds {MAX_PATH_COMPONENTS} components"
            )));
        }
        if let Some(component) = components
            .iter()
            .find(|component| component.len() > MAX_PATH_COMPONENT_BYTES)
        {
            return Err(MetadataError::InvalidArgument(format!(
                "Path component exceeds {MAX_PATH_COMPONENT_BYTES} bytes: {component}"
            )));
        }

        if components.is_empty() {
            // Path is "/" or all slashes
            return Ok("/".to_string());
        }

        // Rejoin with single '/'
        let normalized = format!("/{}", components.join("/"));
        if normalized.len() > MAX_PATH_BYTES {
            return Err(MetadataError::InvalidArgument(format!(
                "Normalized path exceeds {MAX_PATH_BYTES} bytes"
            )));
        }

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

    /// Walk the dentry tree and append every visited inode to the bounded ancestor chain.
    fn walk_dentry(
        &self,
        root_inode_id: InodeId,
        components: &[String],
        ancestor_inode_ids: &mut Vec<InodeId>,
    ) -> MetadataResult<InodeId> {
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
            ancestor_inode_ids.push(child_inode_id);
        }

        Ok(current_inode_id)
    }

    /// Resolve a path into its mount, parent entry, and optional target inode.
    ///
    /// The mount root resolves directly to its root inode without a parent or
    /// terminal name. For other paths, the parent and terminal name are always
    /// populated while the target inode remains optional so create operations
    /// can resolve a path whose final entry does not exist yet.
    pub fn resolve_path(&self, path: &str) -> MetadataResult<ResolvedPath> {
        let (mount_entry, components) = self.resolve_mount(path)?;

        if components.is_empty() {
            return Ok(ResolvedPath {
                mount_ctx: MountContext {
                    mount_id: mount_entry.mount_id,
                    mount_epoch: mount_entry.mount_epoch,
                    owner_group_name: mount_entry.namespace_owner_group_name,
                    root_inode_id: mount_entry.root_inode_id,
                },
                parent_inode_id: None,
                name: None,
                inode_id: Some(mount_entry.root_inode_id),
                ancestor_inode_ids: vec![mount_entry.root_inode_id],
            });
        }

        // Split into parent components and name
        let (parent_components, name) = components.split_at(components.len() - 1);
        let name = name[0].clone();
        let mut ancestor_inode_ids = vec![mount_entry.root_inode_id];

        // Walk to parent directory.
        let parent_inode_id = if parent_components.is_empty() {
            mount_entry.root_inode_id
        } else {
            self.walk_dentry(mount_entry.root_inode_id, parent_components, &mut ancestor_inode_ids)?
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

        // The final entry is optional because create and rename destinations
        // are valid resolution targets before their dentry exists.
        let inode_id = self.storage.get_dentry(parent_inode_id, &name)?;
        if let Some(inode_id) = inode_id {
            ancestor_inode_ids.push(inode_id);
        }

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
            ancestor_inode_ids,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mount::{DataIoPolicy, MountEntry, MountKind, MountTable};
    use crate::raft::RocksDBStorage;
    use beryl_types::fs::{FileAttrs, Inode, InodeId};
    use beryl_types::ids::{DataHandleId, MountId};
    use beryl_types::GroupName;
    use tempfile::TempDir;

    fn test_resolver(mount_table: Arc<MountTable>, storage: Arc<RocksDBStorage>) -> PathResolver {
        PathResolver::new(mount_table, storage)
    }

    fn publish_mount(
        table: &MountTable,
        mount_id: u64,
        mount_prefix: &str,
        mount_kind: MountKind,
        ufs_uri: Option<&str>,
        group_name: &str,
        root_inode_id: InodeId,
    ) -> MountEntry {
        let entry = MountEntry {
            mount_id: MountId::new(mount_id),
            mount_prefix: mount_prefix.to_string(),
            mount_kind,
            ufs_uri: ufs_uri.map(str::to_string),
            data_io_policy: DataIoPolicy::Allow,
            mount_epoch: mount_id,
            namespace_owner_group_name: GroupName::parse(group_name).unwrap(),
            root_inode_id,
        };
        table.upsert(entry.clone()).unwrap();
        entry
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
    fn normalize_enforces_path_component_and_depth_limits() {
        let longest_component = "a".repeat(MAX_PATH_COMPONENT_BYTES);
        assert!(PathResolver::normalize(&format!("/{longest_component}")).is_ok());
        assert!(PathResolver::normalize(&format!("/{longest_component}a")).is_err());

        let deepest_path = format!("/{}", vec!["a"; MAX_PATH_COMPONENTS].join("/"));
        assert!(PathResolver::normalize(&deepest_path).is_ok());
        let too_deep_path = format!("/{}", vec!["a"; MAX_PATH_COMPONENTS + 1].join("/"));
        assert!(PathResolver::normalize(&too_deep_path).is_err());

        let component = "a".repeat(MAX_PATH_COMPONENT_BYTES);
        let longest_path = format!("/{}", vec![component.as_str(); 16].join("/"));
        assert_eq!(longest_path.len(), MAX_PATH_BYTES);
        assert!(PathResolver::normalize(&longest_path).is_ok());
        assert!(PathResolver::normalize(&format!("{longest_path}/a")).is_err());
    }

    #[test]
    fn test_resolve_mount() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());

        let root_inode_id = InodeId::new(1);
        publish_mount(
            &mount_table,
            1,
            "/mnt/s3",
            MountKind::External,
            Some("s3://bucket/path"),
            "g1",
            root_inode_id,
        );

        let resolver = test_resolver(mount_table.clone(), storage);

        // Test mount resolution
        let (mount, components) = resolver.resolve_mount("/mnt/s3/file.txt").unwrap();
        assert_eq!(mount.mount_prefix, "/mnt/s3");
        assert_eq!(components, vec!["file.txt"]);

        let (_mount, components) = resolver.resolve_mount("/mnt/s3/dir/file.txt").unwrap();
        assert_eq!(components, vec!["dir", "file.txt"]);

        // Test longest prefix match
        publish_mount(
            &mount_table,
            2,
            "/mnt",
            MountKind::External,
            Some("s3://bucket2"),
            "g2",
            InodeId::new(2),
        );

        let (mount, _) = resolver.resolve_mount("/mnt/s3/file.txt").unwrap();
        assert_eq!(mount.mount_prefix, "/mnt/s3"); // Should match longer prefix

        publish_mount(&mount_table, 3, "/", MountKind::Internal, None, "g3", InodeId::new(3));

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
    fn resolve_path_returns_existing_target_parent_and_terminal_name() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());

        let root_inode_id = InodeId::new(100);
        let mount = publish_mount(
            &mount_table,
            1,
            "/mnt/test",
            MountKind::External,
            Some("file:///tmp/test"),
            "g1",
            root_inode_id,
        );

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
        let resolved = resolver.resolve_path("/mnt/test/a/b/c").unwrap();
        assert_eq!(resolved.inode_id, Some(file_c));
        assert_eq!(resolved.parent_inode_id, Some(dir_b));
        assert_eq!(resolved.name.as_deref(), Some("c"));
        assert_eq!(resolved.ancestor_inode_ids, vec![root_inode_id, dir_a, dir_b, file_c]);

        let root = resolver.resolve_path("/mnt/test").unwrap();
        assert_eq!(root.inode_id, Some(root_inode_id));
        assert!(root.parent_inode_id.is_none());
        assert!(root.name.is_none());
        assert_eq!(root.ancestor_inode_ids, vec![root_inode_id]);
    }

    #[test]
    fn resolve_path_returns_parent_and_terminal_name() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());

        let root_inode_id = InodeId::new(200);
        let mount = publish_mount(
            &mount_table,
            1,
            "/mnt/test2",
            MountKind::External,
            Some("file:///tmp/test2"),
            "g2",
            root_inode_id,
        );

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
        assert_eq!(resolved.parent_inode_id, Some(dir_b));
        assert_eq!(resolved.name.as_deref(), Some("new-file"));
        assert!(resolved.inode_id.is_none());
        assert_eq!(resolved.ancestor_inode_ids, vec![root_inode_id, dir_a, dir_b]);
    }
}
