// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Route table for path/inode_id -> group_id mapping.

use crate::cache::RouteCache;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use types::fs::InodeId;
use types::ids::ShardGroupId;

/// Route table cache.
pub struct RouteTable {
    /// Route cache.
    cache: RouteCache,
    /// Current route epoch.
    route_epoch: Arc<RwLock<u64>>,
    /// Shard to group mapping.
    shard_to_group: Arc<RwLock<DashMap<u64, ShardGroupId>>>,
}

impl RouteTable {
    /// Create a new route table.
    pub fn new(cache: RouteCache) -> Self {
        Self {
            cache,
            route_epoch: Arc::new(RwLock::new(0)),
            shard_to_group: Arc::new(RwLock::new(DashMap::new())),
        }
    }

    /// Get route for an inode_id (namespace identity).
    pub fn route_inode_id(&self, inode_id: InodeId) -> Option<(ShardGroupId, u64)> {
        // Try cache first
        if let Some((group_id, epoch)) = self.cache.get(&inode_id) {
            let current_epoch = *self.route_epoch.read();
            if epoch == current_epoch {
                return Some((group_id, epoch));
            }
        }
        None
    }

    /// Update route for an inode_id.
    pub fn update_route(&self, inode_id: InodeId, group_id: ShardGroupId, route_epoch: u64) {
        let current_epoch = *self.route_epoch.read();
        if route_epoch >= current_epoch {
            *self.route_epoch.write() = route_epoch;
            self.cache.put(inode_id, group_id, route_epoch);
        }
    }

    /// Update route table from metadata response.
    pub fn update_from_route_table(&self, route_epoch: u64, shard_to_group: std::collections::HashMap<u64, u64>) {
        let current_epoch = *self.route_epoch.read();
        if route_epoch > current_epoch {
            *self.route_epoch.write() = route_epoch;
            let map = self.shard_to_group.write();
            map.clear();
            for (shard_id, group_id) in shard_to_group {
                map.insert(shard_id, ShardGroupId::new(group_id));
            }
            // Invalidate cache entries with old epoch
            self.cache.invalidate_epoch(route_epoch);
        }
    }

    /// Get current route epoch.
    pub fn route_epoch(&self) -> u64 {
        *self.route_epoch.read()
    }

    /// Invalidate route for an inode_id.
    pub fn invalidate(&self, inode_id: &InodeId) {
        self.cache.invalidate(inode_id);
    }
}
