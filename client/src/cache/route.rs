// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Route table cache.

use lru::LruCache;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use types::fs::InodeId;
use types::ids::ShardGroupId;

/// Cached route entry.
#[derive(Clone, Debug)]
pub struct CachedRoute {
    /// Shard group ID.
    pub group_id: ShardGroupId,
    /// Route epoch.
    pub route_epoch: u64,
    /// Cached timestamp.
    pub cached_at: Instant,
}

impl CachedRoute {
    /// Check if this entry is expired.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// Route cache for inode_id -> group_id mapping.
pub struct RouteCache {
    /// LRU cache: inode_id -> CachedRoute.
    cache: Arc<RwLock<LruCache<InodeId, CachedRoute>>>,
    /// TTL for cache entries.
    ttl: Duration,
    /// Maximum number of entries.
    max_entries: usize,
}

impl RouteCache {
    /// Create a new route cache.
    pub fn new(max_entries: usize, ttl_secs: u64) -> Self {
        use std::num::NonZeroUsize;
        let capacity = NonZeroUsize::new(max_entries.max(1)).unwrap();
        Self {
            cache: Arc::new(RwLock::new(LruCache::new(capacity))),
            ttl: Duration::from_secs(ttl_secs),
            max_entries,
        }
    }

    /// Get cached route.
    pub fn get(&self, inode_id: &InodeId) -> Option<(ShardGroupId, u64)> {
        let mut cache = self.cache.write();
        if let Some(cached) = cache.get(inode_id) {
            if !cached.is_expired(self.ttl) {
                return Some((cached.group_id, cached.route_epoch));
            } else {
                // Expired, remove it
                cache.pop(inode_id);
            }
        }
        None
    }

    /// Put route into cache.
    pub fn put(&self, inode_id: InodeId, group_id: ShardGroupId, route_epoch: u64) {
        let mut cache = self.cache.write();
        let cached = CachedRoute {
            group_id,
            route_epoch,
            cached_at: Instant::now(),
        };
        cache.put(inode_id, cached);
    }

    /// Invalidate a specific route entry.
    pub fn invalidate(&self, inode_id: &InodeId) {
        let mut cache = self.cache.write();
        cache.pop(inode_id);
    }

    /// Invalidate all entries with route epoch less than the given epoch.
    pub fn invalidate_epoch(&self, min_epoch: u64) {
        let mut cache = self.cache.write();
        let keys_to_remove: Vec<_> = cache
            .iter()
            .filter(|(_, v)| v.route_epoch < min_epoch)
            .map(|(k, _)| *k)
            .collect();
        for key in keys_to_remove {
            cache.pop(&key);
        }
    }

    /// Clear all cache entries.
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
    }
}

impl Clone for RouteCache {
    fn clone(&self) -> Self {
        Self {
            cache: Arc::clone(&self.cache),
            ttl: self.ttl,
            max_entries: self.max_entries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_cache_put_get() {
        let cache = RouteCache::new(10, 300);
        let inode_id = InodeId::new(1);
        let group_id = ShardGroupId::new(100);
        let route_epoch = 1;

        cache.put(inode_id, group_id, route_epoch);
        let retrieved = cache.get(&inode_id);
        assert!(retrieved.is_some());
        let (retrieved_group_id, retrieved_epoch) = retrieved.unwrap();
        assert_eq!(retrieved_group_id, group_id);
        assert_eq!(retrieved_epoch, route_epoch);
    }

    #[test]
    fn test_route_cache_ttl() {
        let cache = RouteCache::new(10, 1); // 1 second TTL
        let inode_id = InodeId::new(1);
        let group_id = ShardGroupId::new(100);

        cache.put(inode_id, group_id, 1);
        assert!(cache.get(&inode_id).is_some());

        // Wait for expiration
        std::thread::sleep(Duration::from_secs(2));
        assert!(cache.get(&inode_id).is_none());
    }

    #[test]
    fn test_route_cache_invalidate_epoch() {
        let cache = RouteCache::new(10, 300);
        let inode_id1 = InodeId::new(1);
        let inode_id2 = InodeId::new(2);
        let inode_id3 = InodeId::new(3);

        cache.put(inode_id1, ShardGroupId::new(100), 1);
        cache.put(inode_id2, ShardGroupId::new(100), 2);
        cache.put(inode_id3, ShardGroupId::new(100), 3);

        // Invalidate entries with epoch < 3
        cache.invalidate_epoch(3);
        assert!(cache.get(&inode_id1).is_none()); // epoch 1 < 3
        assert!(cache.get(&inode_id2).is_none()); // epoch 2 < 3
        assert!(cache.get(&inode_id3).is_some()); // epoch 3 >= 3
    }
}
