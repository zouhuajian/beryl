// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Mount table management for UFS path mapping.
//!
//! Implements RBF (one-to-one) mount model: path -> ufs path.

use crate::error::{MetadataError, MetadataResult};
use crate::raft::RocksDBStorage;
use beryl_types::fs::InodeId;
use beryl_types::ids::MountId;
use beryl_types::GroupName;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

/// Mount entry: maps beryl path prefix to UFS URI.
///
/// Extended with namespace_owner_group_name and root_inode_id for FS operations.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountEntry {
    pub mount_id: MountId,
    pub mount_prefix: String, // e.g., "/mnt/s3"
    pub mount_kind: MountKind,
    pub ufs_uri: Option<String>, // e.g., "s3://bucket/path"
    pub data_io_policy: DataIoPolicy,
    pub mount_epoch: u64,
    /// Namespace owner group name: all FS write operations within this mount
    /// must route to this single Raft group for atomic rename within mount.
    pub namespace_owner_group_name: GroupName,
    /// Root inode ID: the root directory inode for this mount.
    /// Must be a directory inode and must exist when mount is created/loaded.
    pub root_inode_id: InodeId,
}

/// Mount table: manages all mount entries.
pub struct MountTable {
    state: RwLock<MountTableState>,
}

#[derive(Debug)]
pub(crate) struct MountTableState {
    entries: HashMap<MountId, MountEntry>,
    prefix_index: HashMap<String, MountId>,
    epoch: u64,
}

impl MountTable {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(MountTableState {
                entries: HashMap::new(),
                prefix_index: HashMap::new(),
                epoch: 1,
            }),
        }
    }

    pub(crate) fn build_replacement(mounts: Vec<MountEntry>) -> MetadataResult<MountTableState> {
        let mut entries = HashMap::with_capacity(mounts.len());
        let mut prefix_index = HashMap::with_capacity(mounts.len());
        let mut max_epoch = 0u64;

        for entry in mounts {
            max_epoch = max_epoch.max(entry.mount_epoch);

            if entries.contains_key(&entry.mount_id) {
                return Err(MetadataError::Internal(format!(
                    "Duplicate mount ID in storage: {:?}",
                    entry.mount_id
                )));
            }
            if let Some(existing_id) = prefix_index.get(&entry.mount_prefix) {
                return Err(MetadataError::Internal(format!(
                    "Duplicate mount prefix in storage: {} (mount_id: {:?} and {:?})",
                    entry.mount_prefix, existing_id, entry.mount_id
                )));
            }

            prefix_index.insert(entry.mount_prefix.clone(), entry.mount_id);
            entries.insert(entry.mount_id, entry);
        }

        let epoch = max_epoch
            .checked_add(1)
            .ok_or_else(|| MetadataError::Internal("Mount epoch exhausted while loading storage".to_string()))?;

        Ok(MountTableState {
            entries,
            prefix_index,
            epoch,
        })
    }

    pub(crate) fn replace(&self, replacement: MountTableState) {
        *self.state.write() = replacement;
    }

    /// Load mount entries from RocksDB storage.
    ///
    /// This should be called at service startup to restore mounts from persistent storage.
    /// Returns an empty table if no mounts exist (first startup), but returns error
    /// if RocksDB read fails or data is corrupted.
    pub(crate) fn load_from_storage(storage: &RocksDBStorage) -> MetadataResult<Self> {
        let mounts = storage.list_mounts()?;
        let state = Self::build_replacement(mounts)?;
        Ok(Self {
            state: RwLock::new(state),
        })
    }

    /// Upsert (insert or update) a mount entry.
    ///
    /// Used by Raft read-view publication after the authoritative RocksDB commit.
    pub(crate) fn upsert(&self, entry: MountEntry) -> MetadataResult<()> {
        let mut state = self.state.write();

        // Remove old prefix mapping if updating existing mount
        if let Some(old_entry) = state.entries.get(&entry.mount_id) {
            if old_entry.mount_prefix != entry.mount_prefix {
                let old_prefix = old_entry.mount_prefix.clone();
                state.prefix_index.remove(&old_prefix);
            }
        }

        // Check for prefix conflict (different mount_id with same prefix)
        if let Some(&existing_id) = state.prefix_index.get(&entry.mount_prefix) {
            if existing_id != entry.mount_id {
                return Err(MetadataError::AlreadyExists(format!(
                    "Mount prefix already exists: {} (mount_id: {:?})",
                    entry.mount_prefix, existing_id
                )));
            }
        }

        // Update entries and prefix_index
        state.entries.insert(entry.mount_id, entry.clone());
        state.prefix_index.insert(entry.mount_prefix, entry.mount_id);

        // Update epoch to match entry's mount_epoch from RocksDB.
        state.epoch = entry.mount_epoch.max(state.epoch);

        Ok(())
    }

    /// List all mount entries.
    pub fn list_mounts(&self) -> Vec<MountEntry> {
        self.state.read().entries.values().cloned().collect()
    }

    /// Get mount entry by ID.
    pub fn get_mount(&self, mount_id: MountId) -> MetadataResult<Option<MountEntry>> {
        Ok(self.state.read().entries.get(&mount_id).cloned())
    }

    /// Resolve beryl path to UFS path.
    ///
    /// Returns (ufs_uri, relative_path) if a mount matches the prefix.
    pub fn resolve_path(&self, unified_path: &str) -> MetadataResult<Option<(String, String)>> {
        let state = self.state.read();

        // Find the longest matching prefix
        let mut best_match: Option<(String, String)> = None;
        let mut best_prefix_len = 0;

        for (prefix, &mount_id) in &state.prefix_index {
            if mount_prefix_matches_path(prefix, unified_path) {
                let prefix_len = prefix.len();
                if prefix_len > best_prefix_len {
                    if let Some(entry) = state.entries.get(&mount_id) {
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

    /// Get current mount epoch.
    pub fn epoch(&self) -> u64 {
        self.state.read().epoch
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
    use beryl_types::ids::MountId;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn publish_external_mount(
        table: &MountTable,
        mount_id: u64,
        mount_prefix: &str,
        ufs_uri: &str,
        group_name: &str,
        root_inode_id: InodeId,
    ) {
        table
            .upsert(MountEntry {
                mount_id: MountId::new(mount_id),
                mount_prefix: mount_prefix.to_string(),
                mount_kind: MountKind::External,
                ufs_uri: Some(ufs_uri.to_string()),
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch: mount_id,
                namespace_owner_group_name: GroupName::parse(group_name).unwrap(),
                root_inode_id,
            })
            .unwrap();
    }

    #[test]
    fn published_mount_resolves_external_path() {
        let table = MountTable::new();

        publish_external_mount(&table, 1, "/mnt/s3", "s3://bucket/path", "g1", InodeId::new(1));

        let result = table.resolve_path("/mnt/s3/file.txt").unwrap();
        assert!(result.is_some());
        let (ufs_uri, relative_path) = result.unwrap();
        assert_eq!(ufs_uri, "s3://bucket/path");
        assert_eq!(relative_path, "file.txt");
    }

    #[test]
    fn test_mount_longest_prefix() {
        let table = MountTable::new();

        publish_external_mount(&table, 1, "/mnt", "s3://bucket1", "g1", InodeId::new(1));
        publish_external_mount(&table, 2, "/mnt/s3", "s3://bucket2", "g2", InodeId::new(2));

        let result = table.resolve_path("/mnt/s3/file.txt").unwrap();
        assert!(result.is_some());
        let (ufs_uri, _) = result.unwrap();
        assert_eq!(ufs_uri, "s3://bucket2");
    }

    #[test]
    fn test_mount_prefix_component_boundaries() {
        let table = MountTable::new();

        publish_external_mount(&table, 1, "/", "ufs://root", "g1", InodeId::new(1));
        publish_external_mount(&table, 2, "/mnt", "ufs://mnt", "g2", InodeId::new(2));
        publish_external_mount(&table, 3, "/mnt/s3", "ufs://s3", "g3", InodeId::new(3));

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
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).unwrap());

        // Create initial mount entries via storage (simulating Raft apply)
        use beryl_types::fs::InodeId;
        use beryl_types::GroupName;
        let entry1 = MountEntry {
            mount_id: MountId::new(1),
            mount_prefix: "/mnt/s3".to_string(),
            mount_kind: MountKind::External,
            ufs_uri: Some("s3://bucket1/path".to_string()),
            data_io_policy: DataIoPolicy::Allow,
            mount_epoch: 1,
            namespace_owner_group_name: GroupName::parse("g1").unwrap(),
            root_inode_id: InodeId::new(1),
        };
        let entry2 = MountEntry {
            mount_id: MountId::new(2),
            mount_prefix: "/mnt/oss".to_string(),
            mount_kind: MountKind::External,
            ufs_uri: Some("oss://bucket2/path".to_string()),
            data_io_policy: DataIoPolicy::Allow,
            mount_epoch: 2,
            namespace_owner_group_name: GroupName::parse("g2").unwrap(),
            root_inode_id: InodeId::new(2),
        };

        storage.put_mount(&entry1).unwrap();
        storage.put_mount(&entry2).unwrap();
        storage.put_mount_epoch(2).unwrap();

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
        storage.put_mount_epoch(3).unwrap();

        // Reload the derived table from authoritative storage.
        let mount_table = MountTable::load_from_storage(&storage).unwrap();

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
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path()).unwrap());

        // Should return empty table, not error
        let mount_table = MountTable::load_from_storage(&storage).unwrap();
        assert_eq!(mount_table.list_mounts().len(), 0);

        // Resolve should return None
        let result = mount_table.resolve_path("/any/path").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn duplicate_prefix_replacement_does_not_change_current_table() {
        let table = MountTable::new();
        publish_external_mount(&table, 1, "/current", "ufs://current", "root", InodeId::new(1));
        let original_epoch = table.epoch();

        let duplicate_prefix = vec![
            MountEntry {
                mount_id: MountId::new(10),
                mount_prefix: "/replacement".to_string(),
                mount_kind: MountKind::External,
                ufs_uri: Some("ufs://first".to_string()),
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch: 10,
                namespace_owner_group_name: GroupName::parse("root").unwrap(),
                root_inode_id: InodeId::new(10),
            },
            MountEntry {
                mount_id: MountId::new(11),
                mount_prefix: "/replacement".to_string(),
                mount_kind: MountKind::External,
                ufs_uri: Some("ufs://second".to_string()),
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch: 11,
                namespace_owner_group_name: GroupName::parse("root").unwrap(),
                root_inode_id: InodeId::new(11),
            },
        ];

        let error = MountTable::build_replacement(duplicate_prefix).unwrap_err();
        assert!(error.to_string().contains("Duplicate mount prefix"));
        assert_eq!(table.epoch(), original_epoch);
        assert_eq!(table.list_mounts().len(), 1);
        assert_eq!(
            table.resolve_path("/current/file").unwrap(),
            Some(("ufs://current".to_string(), "file".to_string()))
        );
        assert!(table.resolve_path("/replacement/file").unwrap().is_none());
    }

    #[test]
    fn replacement_updates_entries_prefix_and_epoch_together() {
        let table = MountTable::new();
        publish_external_mount(&table, 1, "/old", "ufs://old", "root", InodeId::new(1));

        let replacement = MountTable::build_replacement(vec![MountEntry {
            mount_id: MountId::new(9),
            mount_prefix: "/new".to_string(),
            mount_kind: MountKind::External,
            ufs_uri: Some("ufs://new".to_string()),
            data_io_policy: DataIoPolicy::Allow,
            mount_epoch: 41,
            namespace_owner_group_name: GroupName::parse("root").unwrap(),
            root_inode_id: InodeId::new(9),
        }])
        .unwrap();
        table.replace(replacement);

        {
            let state = table.state.read();
            assert_eq!(state.entries.len(), 1);
            assert_eq!(state.entries[&MountId::new(9)].mount_prefix, "/new");
            assert_eq!(state.prefix_index.get("/new"), Some(&MountId::new(9)));
            assert!(!state.prefix_index.contains_key("/old"));
            assert_eq!(state.epoch, 42);
        }
    }
}
