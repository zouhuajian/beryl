// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Mount table management for UFS path mapping.
//!
//! Implements RBF (one-to-one) mount model: path -> ufs path.

use crate::error::{MetadataError, MetadataResult};
use crate::raft::RocksDBStorage;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use types::fs::InodeId;
use types::ids::{MountId, ShardGroupId};

pub const ROOT_MOUNT_PREFIX: &str = "/";
pub const ROOT_INODE_ID: InodeId = InodeId::new(1);

pub(crate) fn mount_prefix_matches_path(prefix: &str, path: &str) -> bool {
    if prefix == ROOT_MOUNT_PREFIX {
        return path.starts_with('/');
    }
    path == prefix || path.strip_prefix(prefix).is_some_and(|suffix| suffix.starts_with('/'))
}

/// Mount kind: internal (no UFS) or UFS-backed.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MountKind {
    Internal,
    External,
}

/// Data IO policy for a mount.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum DataIoPolicy {
    #[default]
    Allow,
    Forbid,
}

/// Mount entry: maps vecton path prefix to UFS URI.
///
/// Extended with namespace_owner_group_id and root_inode_id for FS operations.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountEntry {
    pub mount_id: MountId,
    pub mount_prefix: String, // e.g., "/mnt/s3"
    pub mount_kind: MountKind,
    pub ufs_uri: Option<String>, // e.g., "s3://bucket/path"
    pub data_io_policy: DataIoPolicy,
    pub mount_version: u64,
    /// Namespace owner group ID: all FS write operations within this mount
    /// must route to this single Raft group for atomic rename within mount.
    pub namespace_owner_group_id: ShardGroupId,
    /// Root inode ID: the root directory inode for this mount.
    /// Must be a directory inode and must exist when mount is created/loaded.
    pub root_inode_id: InodeId,
}

/// Mount table: manages all mount entries.
pub struct MountTable {
    entries: Arc<RwLock<HashMap<MountId, MountEntry>>>,
    prefix_index: Arc<RwLock<HashMap<String, MountId>>>, // mount_prefix -> mount_id
    next_mount_id: Arc<RwLock<u64>>,
    version: Arc<RwLock<u64>>,
}

impl MountTable {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            prefix_index: Arc::new(RwLock::new(HashMap::new())),
            next_mount_id: Arc::new(RwLock::new(1)),
            version: Arc::new(RwLock::new(1)),
        }
    }

    /// Load mount entries from RocksDB storage.
    ///
    /// This should be called at service startup to restore mounts from persistent storage.
    /// Returns an empty table if no mounts exist (first startup), but returns error
    /// if RocksDB read fails or data is corrupted.
    pub fn load_from_storage(storage: &RocksDBStorage) -> MetadataResult<Self> {
        let mounts = storage.list_mounts()?;

        let mut entries = HashMap::new();
        let mut prefix_index = HashMap::new();
        let mut max_mount_id = 0u64;
        let mut max_version = 0u64;

        // Build entries and prefix_index from loaded mounts
        for entry in mounts {
            let mount_id_raw = entry.mount_id.as_raw();
            max_mount_id = max_mount_id.max(mount_id_raw);
            max_version = max_version.max(entry.mount_version);

            // Check for duplicate prefix (data corruption)
            if prefix_index.contains_key(&entry.mount_prefix) {
                let existing_id = prefix_index[&entry.mount_prefix];
                return Err(MetadataError::Internal(format!(
                    "Duplicate mount prefix in storage: {} (mount_id: {:?} and {:?})",
                    entry.mount_prefix, existing_id, entry.mount_id
                )));
            }

            entries.insert(entry.mount_id, entry.clone());
            prefix_index.insert(entry.mount_prefix, entry.mount_id);
        }

        Ok(Self {
            entries: Arc::new(RwLock::new(entries)),
            prefix_index: Arc::new(RwLock::new(prefix_index)),
            next_mount_id: Arc::new(RwLock::new(max_mount_id + 1)),
            version: Arc::new(RwLock::new(max_version + 1)),
        })
    }

    /// Create a new mount entry.
    ///
    /// The root_inode_id must already exist as a directory inode.
    pub fn create_mount(
        &self,
        mount_prefix: String,
        mount_kind: MountKind,
        ufs_uri: Option<String>,
        data_io_policy: DataIoPolicy,
        namespace_owner_group_id: ShardGroupId,
        root_inode_id: InodeId,
    ) -> MetadataResult<MountEntry> {
        let mut entries = self.entries.write();
        let mut prefix_index = self.prefix_index.write();
        let mut version = self.version.write();

        // Check if prefix already exists
        if prefix_index.contains_key(&mount_prefix) {
            return Err(MetadataError::AlreadyExists(format!(
                "Mount prefix already exists: {}",
                mount_prefix
            )));
        }

        let mut next_id = self.next_mount_id.write();
        let mount_id = MountId::new(*next_id);
        *next_id += 1;

        let entry = MountEntry {
            mount_id,
            mount_prefix: mount_prefix.clone(),
            mount_kind,
            ufs_uri,
            data_io_policy,
            mount_version: *version,
            namespace_owner_group_id,
            root_inode_id,
        };

        entries.insert(mount_id, entry.clone());
        prefix_index.insert(mount_prefix, mount_id);
        *version += 1;

        Ok(entry)
    }

    /// Delete a mount entry.
    pub fn delete_mount(&self, mount_id: MountId) -> MetadataResult<()> {
        let mut entries = self.entries.write();
        let mut prefix_index = self.prefix_index.write();
        let mut version = self.version.write();

        if let Some(entry) = entries.remove(&mount_id) {
            prefix_index.remove(&entry.mount_prefix);
            *version += 1;
            Ok(())
        } else {
            Err(MetadataError::NotFound(format!("Mount not found: {:?}", mount_id)))
        }
    }

    /// Upsert (insert or update) a mount entry.
    ///
    /// Used by Raft state machine to synchronize MountTable after RocksDB write.
    /// This ensures in-memory MountTable stays consistent with RocksDB.
    pub fn upsert(&self, entry: MountEntry) -> MetadataResult<()> {
        let mut entries = self.entries.write();
        let mut prefix_index = self.prefix_index.write();
        let mut version = self.version.write();

        // Remove old prefix mapping if updating existing mount
        if let Some(old_entry) = entries.get(&entry.mount_id) {
            if old_entry.mount_prefix != entry.mount_prefix {
                prefix_index.remove(&old_entry.mount_prefix);
            }
        }

        // Check for prefix conflict (different mount_id with same prefix)
        if let Some(&existing_id) = prefix_index.get(&entry.mount_prefix) {
            if existing_id != entry.mount_id {
                return Err(MetadataError::AlreadyExists(format!(
                    "Mount prefix already exists: {} (mount_id: {:?})",
                    entry.mount_prefix, existing_id
                )));
            }
        }

        // Update entries and prefix_index
        entries.insert(entry.mount_id, entry.clone());
        prefix_index.insert(entry.mount_prefix, entry.mount_id);

        // Update version to match entry's mount_version (from RocksDB)
        *version = entry.mount_version.max(*version);

        Ok(())
    }

    /// Remove a mount entry by ID.
    ///
    /// Used by Raft state machine to synchronize MountTable after RocksDB delete.
    pub fn remove(&self, mount_id: MountId) -> MetadataResult<()> {
        self.delete_mount(mount_id)
    }

    /// List all mount entries.
    pub fn list_mounts(&self) -> Vec<MountEntry> {
        let entries = self.entries.read();
        entries.values().cloned().collect()
    }

    /// Get mount entry by ID.
    pub fn get_mount(&self, mount_id: MountId) -> MetadataResult<Option<MountEntry>> {
        let entries = self.entries.read();
        Ok(entries.get(&mount_id).cloned())
    }

    /// Resolve vecton path to UFS path.
    ///
    /// Returns (ufs_uri, relative_path) if a mount matches the prefix.
    pub fn resolve_path(&self, unified_path: &str) -> MetadataResult<Option<(String, String)>> {
        let prefix_index = self.prefix_index.read();
        let entries = self.entries.read();

        // Find the longest matching prefix
        let mut best_match: Option<(String, String)> = None;
        let mut best_prefix_len = 0;

        for (prefix, &mount_id) in prefix_index.iter() {
            if mount_prefix_matches_path(prefix, unified_path) {
                let prefix_len = prefix.len();
                if prefix_len > best_prefix_len {
                    if let Some(entry) = entries.get(&mount_id) {
                        let ufs_uri = match entry.ufs_uri.as_ref() {
                            Some(uri) => uri,
                            None => continue,
                        };
                        let relative_path = if prefix_len == unified_path.len() {
                            "".to_string()
                        } else if unified_path.as_bytes()[prefix_len] == b'/' {
                            unified_path[prefix_len + 1..].to_string()
                        } else {
                            unified_path[prefix_len..].to_string()
                        };
                        best_match = Some((ufs_uri.to_string(), relative_path));
                        best_prefix_len = prefix_len;
                    }
                }
            }
        }

        Ok(best_match)
    }

    /// Get current version.
    pub fn version(&self) -> u64 {
        *self.version.read()
    }

    /// Allocate a new mount ID (in-memory only).
    pub fn allocate_mount_id(&self) -> MountId {
        let mut next_id = self.next_mount_id.write();
        let mount_id = MountId::new(*next_id);
        *next_id += 1;
        mount_id
    }
}

impl Default for MountTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RocksDBStorage;
    use std::sync::Arc;
    use tempfile::TempDir;
    use types::ids::MountId;

    #[test]
    fn test_mount_create_and_resolve() {
        let table = MountTable::new();

        // Create mount (test-only: use placeholder values)
        use types::fs::InodeId;
        use types::ids::ShardGroupId;
        let root_inode_id = InodeId::new(1);
        let _entry = table
            .create_mount(
                "/mnt/s3".to_string(),
                MountKind::External,
                Some("s3://bucket/path".to_string()),
                DataIoPolicy::Allow,
                ShardGroupId::new(1),
                root_inode_id,
            )
            .unwrap();

        // Resolve path
        let result = table.resolve_path("/mnt/s3/file.txt").unwrap();
        assert!(result.is_some());
        let (ufs_uri, relative_path) = result.unwrap();
        assert_eq!(ufs_uri, "s3://bucket/path");
        assert_eq!(relative_path, "file.txt");
    }

    #[test]
    fn test_mount_longest_prefix() {
        let table = MountTable::new();

        use types::fs::InodeId;
        use types::ids::ShardGroupId;
        let root1 = InodeId::new(1);
        let root2 = InodeId::new(2);
        table
            .create_mount(
                "/mnt".to_string(),
                MountKind::External,
                Some("s3://bucket1".to_string()),
                DataIoPolicy::Allow,
                ShardGroupId::new(1),
                root1,
            )
            .unwrap();
        table
            .create_mount(
                "/mnt/s3".to_string(),
                MountKind::External,
                Some("s3://bucket2".to_string()),
                DataIoPolicy::Allow,
                ShardGroupId::new(2),
                root2,
            )
            .unwrap();

        // Should match longest prefix
        let result = table.resolve_path("/mnt/s3/file.txt").unwrap();
        assert!(result.is_some());
        let (ufs_uri, _) = result.unwrap();
        assert_eq!(ufs_uri, "s3://bucket2");
    }

    #[test]
    fn test_mount_prefix_component_boundaries() {
        let table = MountTable::new();

        use types::fs::InodeId;
        use types::ids::ShardGroupId;
        table
            .create_mount(
                "/".to_string(),
                MountKind::External,
                Some("ufs://root".to_string()),
                DataIoPolicy::Allow,
                ShardGroupId::new(1),
                InodeId::new(1),
            )
            .unwrap();
        table
            .create_mount(
                "/mnt".to_string(),
                MountKind::External,
                Some("ufs://mnt".to_string()),
                DataIoPolicy::Allow,
                ShardGroupId::new(2),
                InodeId::new(2),
            )
            .unwrap();
        table
            .create_mount(
                "/mnt/s3".to_string(),
                MountKind::External,
                Some("ufs://s3".to_string()),
                DataIoPolicy::Allow,
                ShardGroupId::new(3),
                InodeId::new(3),
            )
            .unwrap();

        let (uri, relative) = table.resolve_path("/other/file").unwrap().unwrap();
        assert_eq!(uri, "ufs://root");
        assert_eq!(relative, "other/file");

        let (uri, relative) = table.resolve_path("/mnt/s3").unwrap().unwrap();
        assert_eq!(uri, "ufs://s3");
        assert_eq!(relative, "");

        let (uri, relative) = table.resolve_path("/mnt/s3/file").unwrap().unwrap();
        assert_eq!(uri, "ufs://s3");
        assert_eq!(relative, "file");

        let (uri, relative) = table.resolve_path("/mnt/s3/").unwrap().unwrap();
        assert_eq!(uri, "ufs://s3");
        assert_eq!(relative, "");

        let (uri, relative) = table.resolve_path("/mnt/s3x").unwrap().unwrap();
        assert_eq!(uri, "ufs://mnt");
        assert_eq!(relative, "s3x");

        let (uri, relative) = table.resolve_path("/mnt2").unwrap().unwrap();
        assert_eq!(uri, "ufs://root");
        assert_eq!(relative, "mnt2");

        let (uri, relative) = table.resolve_path("/mnt/s30/a").unwrap().unwrap();
        assert_eq!(uri, "ufs://mnt");
        assert_eq!(relative, "s30/a");
    }

    /// Test mount persistence and consistency: write -> load -> resolve -> delete -> verify
    #[test]
    fn test_mount_persistence_and_consistency() {
        // Create temporary RocksDB
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(temp_dir.path()).unwrap());

        // Create initial mount entries via storage (simulating Raft apply)
        use types::fs::InodeId;
        use types::ids::ShardGroupId;
        let entry1 = MountEntry {
            mount_id: MountId::new(1),
            mount_prefix: "/mnt/s3".to_string(),
            mount_kind: MountKind::External,
            ufs_uri: Some("s3://bucket1/path".to_string()),
            data_io_policy: DataIoPolicy::Allow,
            mount_version: 1,
            namespace_owner_group_id: ShardGroupId::new(1),
            root_inode_id: InodeId::new(1),
        };
        let entry2 = MountEntry {
            mount_id: MountId::new(2),
            mount_prefix: "/mnt/oss".to_string(),
            mount_kind: MountKind::External,
            ufs_uri: Some("oss://bucket2/path".to_string()),
            data_io_policy: DataIoPolicy::Allow,
            mount_version: 2,
            namespace_owner_group_id: ShardGroupId::new(2),
            root_inode_id: InodeId::new(2),
        };

        storage.put_mount(&entry1).unwrap();
        storage.put_mount(&entry2).unwrap();
        storage.put_mount_version(2).unwrap();

        // Load mount table from storage (simulating service restart)
        let mount_table = MountTable::load_from_storage(&storage).unwrap();

        // Verify mounts are loaded and resolve works
        let result1 = mount_table.resolve_path("/mnt/s3/file.txt").unwrap();
        assert!(result1.is_some());
        let (ufs_uri1, rel_path1) = result1.unwrap();
        assert_eq!(ufs_uri1, "s3://bucket1/path");
        assert_eq!(rel_path1, "file.txt");

        let result2 = mount_table.resolve_path("/mnt/oss/dir/file.txt").unwrap();
        assert!(result2.is_some());
        let (ufs_uri2, rel_path2) = result2.unwrap();
        assert_eq!(ufs_uri2, "oss://bucket2/path");
        assert_eq!(rel_path2, "dir/file.txt");

        // Verify list_mounts returns both entries
        let mounts = mount_table.list_mounts();
        assert_eq!(mounts.len(), 2);

        // Delete one mount via storage (simulating Raft apply_delete_mount)
        storage.delete_mount(MountId::new(1)).unwrap();
        storage.put_mount_version(3).unwrap();

        // Synchronize MountTable (simulating apply_delete_mount calling remove)
        mount_table.remove(MountId::new(1)).unwrap();

        // Verify deleted mount no longer resolves
        let result_deleted = mount_table.resolve_path("/mnt/s3/file.txt").unwrap();
        assert!(result_deleted.is_none());

        // Verify remaining mount still works
        let result_remaining = mount_table.resolve_path("/mnt/oss/file.txt").unwrap();
        assert!(result_remaining.is_some());

        // Verify storage and MountTable are consistent
        let storage_mounts = storage.list_mounts().unwrap();
        assert_eq!(storage_mounts.len(), 1);
        assert_eq!(storage_mounts[0].mount_id, MountId::new(2));
        assert_eq!(mount_table.list_mounts().len(), 1);
    }

    /// Test load_from_storage with empty storage (first startup)
    #[test]
    fn test_load_from_storage_empty() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(temp_dir.path()).unwrap());

        // Should return empty table, not error
        let mount_table = MountTable::load_from_storage(&storage).unwrap();
        assert_eq!(mount_table.list_mounts().len(), 0);

        // Resolve should return None
        let result = mount_table.resolve_path("/any/path").unwrap();
        assert!(result.is_none());
    }
}
