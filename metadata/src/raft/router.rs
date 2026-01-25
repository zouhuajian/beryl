// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Router for mapping inode_ids to shard groups.
//!
//! Implements fixed sharding strategy (split/migrate is currently not supported).

use crate::error::MetadataResult;
use crate::raft::storage::RocksDBStorage;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use types::fs::InodeId;
use types::ids::{ShardGroupId, ShardId};

/// Router that maps inode_ids to shard groups.
pub trait Router: Send + Sync {
    /// Route an inode_id to a shard group.
    fn route_inode_id(&self, inode_id: InodeId) -> MetadataResult<ShardGroupId>;

    /// Get shard ID for an inode_id (within a group).
    fn route_to_shard(&self, inode_id: InodeId) -> MetadataResult<ShardId>;
}

/// Fixed shard router implementation.
///
/// Uses hash-based routing: data_handle_id % num_shards -> shard_id
/// shard_id -> shard_group_id (via shard map)
pub struct ShardRouter {
    /// Number of shards per group (fixed).
    num_shards: u64,
    /// Shard to group mapping.
    shard_to_group: Arc<RwLock<HashMap<ShardId, ShardGroupId>>>,
    /// Default group ID (for inodes not yet assigned).
    default_group_id: ShardGroupId,
    /// Storage for path->inode_id lookup (inode table).
    storage: Option<Arc<RocksDBStorage>>,
}

impl ShardRouter {
    pub fn new(num_shards: u64, default_group_id: ShardGroupId) -> Self {
        Self {
            num_shards,
            shard_to_group: Arc::new(RwLock::new(HashMap::new())),
            default_group_id,
            storage: None,
        }
    }

    /// Set storage for inode table lookup.
    pub fn with_storage(mut self, storage: Arc<RocksDBStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Load shard to group mappings from storage.
    ///
    /// This should be called after setting storage to restore mappings from RocksDB.
    pub fn load_from_storage(&self) -> MetadataResult<()> {
        if let Some(ref storage) = self.storage {
            let mappings = storage.load_all_shard_routings()?;
            let mut map = self.shard_to_group.write();
            map.clear();
            map.extend(mappings);
        }
        Ok(())
    }

    /// Add shard to group mapping and persist to storage.
    pub fn add_shard_mapping(&self, shard_id: ShardId, group_id: ShardGroupId) -> MetadataResult<()> {
        // Update in-memory map
        let mut map = self.shard_to_group.write();
        map.insert(shard_id, group_id);

        // Persist to storage if available
        if let Some(ref storage) = self.storage {
            storage.put_shard_routing(shard_id, group_id)?;
        }

        Ok(())
    }

    /// Remove shard mapping and persist to storage.
    pub fn remove_shard_mapping(&self, shard_id: ShardId) -> MetadataResult<()> {
        // Update in-memory map
        let mut map = self.shard_to_group.write();
        map.remove(&shard_id);

        // Persist to storage if available
        if let Some(ref storage) = self.storage {
            storage.delete_shard_routing(shard_id)?;
        }

        Ok(())
    }

    /// Calculate shard ID from inode_id.
    fn calculate_shard_id(&self, inode_id: InodeId) -> ShardId {
        // Simple hash-based routing: inode_id % num_shards
        let shard_idx = inode_id.as_raw() % self.num_shards;
        ShardId::new(shard_idx)
    }
}

impl Router for ShardRouter {
    fn route_inode_id(&self, inode_id: InodeId) -> MetadataResult<ShardGroupId> {
        let shard_id = self.calculate_shard_id(inode_id);
        let map = self.shard_to_group.read();

        // Look up shard in map, fallback to default group
        Ok(map.get(&shard_id).copied().unwrap_or(self.default_group_id))
    }

    fn route_to_shard(&self, inode_id: InodeId) -> MetadataResult<ShardId> {
        Ok(self.calculate_shard_id(inode_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_shard_calculation() {
        let router = ShardRouter::new(4, ShardGroupId::new(0));
        let inode_id1 = InodeId::new(10);
        let inode_id2 = InodeId::new(11);

        let shard1 = router.route_to_shard(inode_id1).unwrap();
        let shard2 = router.route_to_shard(inode_id2).unwrap();

        // 10 % 4 = 2, 11 % 4 = 3
        assert_eq!(shard1.as_raw(), 2);
        assert_eq!(shard2.as_raw(), 3);
    }

    #[test]
    fn test_router_group_routing() {
        let router = ShardRouter::new(4, ShardGroupId::new(0));
        let shard_id = ShardId::new(2);
        let group_id = ShardGroupId::new(1);

        router.add_shard_mapping(shard_id, group_id).unwrap();

        let inode_id = InodeId::new(10); // routes to shard 2
        let routed_group = router.route_inode_id(inode_id).unwrap();
        assert_eq!(routed_group, group_id);

        // Inode not in mapped shard should use default group
        let inode_id2 = InodeId::new(11); // routes to shard 3
        let routed_group2 = router.route_inode_id(inode_id2).unwrap();
        assert_eq!(routed_group2, ShardGroupId::new(0));
    }
}
