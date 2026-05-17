// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker endpoint cache.

use crate::worker::client::WorkerEndpointInfo;
use lru::LruCache;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use types::ids::WorkerId;

/// Cached worker endpoint entry.
#[derive(Clone, Debug)]
pub struct CachedWorkerEndpoint {
    /// Worker endpoint information.
    pub endpoint_info: WorkerEndpointInfo,
    /// Timestamp when this entry was cached.
    pub cached_at: Instant,
}

impl CachedWorkerEndpoint {
    /// Check if this entry is expired.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// Worker endpoint cache with LRU eviction and TTL.
pub struct WorkerEndpointCache {
    /// LRU cache: worker_id -> CachedWorkerEndpoint.
    cache: Arc<RwLock<LruCache<WorkerId, CachedWorkerEndpoint>>>,
    /// TTL for cache entries.
    ttl: Duration,
    /// Maximum number of entries.
    max_entries: usize,
}

impl WorkerEndpointCache {
    /// Create a new worker endpoint cache.
    pub fn new(max_entries: usize, ttl_secs: u64) -> Self {
        use std::num::NonZeroUsize;
        let capacity = NonZeroUsize::new(max_entries.max(1)).unwrap();
        Self {
            cache: Arc::new(RwLock::new(LruCache::new(capacity))),
            ttl: Duration::from_secs(ttl_secs),
            max_entries,
        }
    }

    /// Get cached worker endpoint information.
    pub fn get(&self, worker_id: &WorkerId) -> Option<WorkerEndpointInfo> {
        let mut cache = self.cache.write();
        if let Some(cached) = cache.get(worker_id) {
            if !cached.is_expired(self.ttl) {
                return Some(cached.endpoint_info.clone());
            } else {
                // Expired, remove it
                cache.pop(worker_id);
            }
        }
        None
    }

    /// Put worker endpoint information into cache.
    pub fn put(&self, endpoint_info: WorkerEndpointInfo) {
        let mut cache = self.cache.write();
        let cached = CachedWorkerEndpoint {
            endpoint_info: endpoint_info.clone(),
            cached_at: Instant::now(),
        };
        cache.put(endpoint_info.worker_id, cached);
    }

    /// Invalidate a specific worker entry.
    pub fn invalidate(&self, worker_id: &WorkerId) {
        let mut cache = self.cache.write();
        cache.pop(worker_id);
    }

    /// Clear all cache entries.
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.read();
        CacheStats {
            entries: cache.len(),
            max_entries: self.max_entries,
        }
    }
}

/// Cache statistics.
#[derive(Clone, Debug)]
pub struct CacheStats {
    /// Current number of entries.
    pub entries: usize,
    /// Maximum number of entries.
    pub max_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_endpoint_info(worker_id: u64, kind: i32, epoch: u64) -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(worker_id),
            endpoint: format!("127.0.0.1:{}", 9000 + worker_id),
            worker_net_protocol: kind,
            worker_epoch: epoch,
        }
    }

    #[test]
    fn test_cache_put_get() {
        let cache = WorkerEndpointCache::new(10, 300);
        let endpoint_info = create_test_endpoint_info(1, 1, 100);

        cache.put(endpoint_info.clone());
        let retrieved = cache.get(&endpoint_info.worker_id);
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.worker_id, endpoint_info.worker_id);
        assert_eq!(retrieved.worker_net_protocol, endpoint_info.worker_net_protocol);
        assert_eq!(retrieved.worker_epoch, endpoint_info.worker_epoch);
    }

    #[test]
    fn test_cache_ttl() {
        let cache = WorkerEndpointCache::new(10, 1); // 1 second TTL
        let endpoint_info = create_test_endpoint_info(1, 1, 100);

        cache.put(endpoint_info.clone());
        assert!(cache.get(&endpoint_info.worker_id).is_some());

        // Wait for expiration
        std::thread::sleep(Duration::from_secs(2));
        assert!(cache.get(&endpoint_info.worker_id).is_none());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let cache = WorkerEndpointCache::new(2, 300); // Max 2 entries
        let endpoint_info1 = create_test_endpoint_info(1, 1, 100);
        let endpoint_info2 = create_test_endpoint_info(2, 1, 200);
        let endpoint_info3 = create_test_endpoint_info(3, 1, 300);

        cache.put(endpoint_info1.clone());
        cache.put(endpoint_info2.clone());
        assert!(cache.get(&endpoint_info1.worker_id).is_some());
        assert!(cache.get(&endpoint_info2.worker_id).is_some());

        // Add third entry, should evict first
        cache.put(endpoint_info3.clone());
        assert!(cache.get(&endpoint_info1.worker_id).is_none()); // Evicted
        assert!(cache.get(&endpoint_info2.worker_id).is_some());
        assert!(cache.get(&endpoint_info3.worker_id).is_some());
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = WorkerEndpointCache::new(10, 300);
        let endpoint_info = create_test_endpoint_info(1, 1, 100);

        cache.put(endpoint_info.clone());
        assert!(cache.get(&endpoint_info.worker_id).is_some());

        cache.invalidate(&endpoint_info.worker_id);
        assert!(cache.get(&endpoint_info.worker_id).is_none());
    }

    #[test]
    fn test_cache_stats() {
        let cache = WorkerEndpointCache::new(10, 300);
        let stats = cache.stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.max_entries, 10);

        cache.put(create_test_endpoint_info(1, 1, 100));
        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
    }
}
