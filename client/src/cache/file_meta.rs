// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! File metadata cache.

use lru::LruCache;
use parking_lot::RwLock;
use proto::common::FileMetaProto;
use std::sync::Arc;
use std::time::{Duration, Instant};
use types::ids::DataHandleId;

/// Cached file metadata entry.
#[derive(Clone, Debug)]
pub struct CachedFileMeta {
    /// File metadata.
    pub meta: FileMetaProto,
    /// Timestamp when this entry was cached.
    pub cached_at: Instant,
}

impl CachedFileMeta {
    /// Check if this entry is expired.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// File metadata cache with LRU eviction and TTL.
pub struct FileMetaCache {
    /// LRU cache: data_handle_id -> CachedFileMeta.
    cache: Arc<RwLock<LruCache<DataHandleId, CachedFileMeta>>>,
    /// TTL for cache entries.
    ttl: Duration,
    /// Maximum number of entries.
    max_entries: usize,
}

impl FileMetaCache {
    /// Create a new file metadata cache.
    pub fn new(max_entries: usize, ttl_secs: u64) -> Self {
        use std::num::NonZeroUsize;
        let capacity = NonZeroUsize::new(max_entries.max(1)).unwrap();
        Self {
            cache: Arc::new(RwLock::new(LruCache::new(capacity))),
            ttl: Duration::from_secs(ttl_secs),
            max_entries,
        }
    }

    /// Get cached file metadata.
    pub fn get(&self, data_handle_id: &DataHandleId) -> Option<FileMetaProto> {
        let mut cache = self.cache.write();
        if let Some(cached) = cache.get(data_handle_id) {
            if !cached.is_expired(self.ttl) {
                return Some(cached.meta.clone());
            } else {
                // Expired, remove it
                cache.pop(data_handle_id);
            }
        }
        None
    }

    /// Put file metadata into cache.
    pub fn put(&self, data_handle_id: DataHandleId, meta: FileMetaProto) {
        let mut cache = self.cache.write();
        let cached = CachedFileMeta {
            meta,
            cached_at: Instant::now(),
        };
        cache.put(data_handle_id, cached);
    }

    /// Invalidate a specific file entry.
    pub fn invalidate(&self, data_handle_id: &DataHandleId) {
        let mut cache = self.cache.write();
        cache.pop(data_handle_id);
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
    use proto::common::{FileLayoutProto, FileMetaProto};

    fn create_test_file_meta(data_handle_id: u64, version: u64) -> FileMetaProto {
        FileMetaProto {
            inode_id: data_handle_id,
            data_handle_id,
            file_version: version,
            blocks: vec![],
            route_epoch: 0,
            consistency_token: 0,
            layout: Some(FileLayoutProto {
                block_size: 64 * 1024 * 1024,
                chunk_size: 64 * 1024,
                replication: 3,
            }),
            committed_length: 0,
        }
    }

    #[test]
    fn test_cache_put_get() {
        let cache = FileMetaCache::new(10, 300);
        let data_handle_id = DataHandleId::new(1);
        let meta = create_test_file_meta(1, 1);

        cache.put(data_handle_id, meta.clone());
        let retrieved = cache.get(&data_handle_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data_handle_id, meta.data_handle_id);
    }

    #[test]
    fn test_cache_ttl() {
        let cache = FileMetaCache::new(10, 1); // 1 second TTL
        let data_handle_id = DataHandleId::new(1);
        let meta = create_test_file_meta(1, 1);

        cache.put(data_handle_id, meta);
        assert!(cache.get(&data_handle_id).is_some());

        // Wait for expiration
        std::thread::sleep(Duration::from_secs(2));
        assert!(cache.get(&data_handle_id).is_none());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let cache = FileMetaCache::new(2, 300); // Max 2 entries
        let data_handle_id1 = DataHandleId::new(1);
        let data_handle_id2 = DataHandleId::new(2);
        let data_handle_id3 = DataHandleId::new(3);

        cache.put(data_handle_id1, create_test_file_meta(1, 1));
        cache.put(data_handle_id2, create_test_file_meta(2, 1));
        assert!(cache.get(&data_handle_id1).is_some());
        assert!(cache.get(&data_handle_id2).is_some());

        // Add third entry, should evict first
        cache.put(data_handle_id3, create_test_file_meta(3, 1));
        assert!(cache.get(&data_handle_id1).is_none()); // Evicted
        assert!(cache.get(&data_handle_id2).is_some());
        assert!(cache.get(&data_handle_id3).is_some());
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = FileMetaCache::new(10, 300);
        let data_handle_id = DataHandleId::new(1);
        let meta = create_test_file_meta(1, 1);

        cache.put(data_handle_id, meta);
        assert!(cache.get(&data_handle_id).is_some());

        cache.invalidate(&data_handle_id);
        assert!(cache.get(&data_handle_id).is_none());
    }

    #[test]
    fn test_cache_stats() {
        let cache = FileMetaCache::new(10, 300);
        let stats = cache.stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.max_entries, 10);

        cache.put(DataHandleId::new(1), create_test_file_meta(1, 1));
        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
    }
}
