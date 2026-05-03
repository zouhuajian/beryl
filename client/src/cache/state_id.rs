// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! State ID cache for consistency checking.
//!
//! This cache stores the last seen GroupStateWatermark per group_id for consistency checking.
//! Watermarks must be compared only within the same group_id.

use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use types::ids::ShardGroupId;
use types::GroupStateWatermark;

/// Cached watermark entry.
#[derive(Clone, Debug)]
pub struct CachedWatermark {
    /// The group watermark.
    pub watermark: GroupStateWatermark,
    /// When this entry was cached.
    pub cached_at: Instant,
}

impl CachedWatermark {
    /// Create a new cached watermark entry.
    pub fn new(watermark: GroupStateWatermark) -> Self {
        Self {
            watermark,
            cached_at: Instant::now(),
        }
    }

    /// Check if this entry is still fresh (within TTL).
    pub fn is_fresh(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() < ttl
    }
}

/// State ID cache per group.
pub struct StateIdCache {
    /// Watermark map: group_id -> CachedWatermark.
    cache: Arc<RwLock<DashMap<ShardGroupId, CachedWatermark>>>,
    /// TTL for cache entries.
    ttl: Duration,
}

impl StateIdCache {
    /// Create a new state ID cache.
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            cache: Arc::new(RwLock::new(DashMap::new())),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    /// Get the cached watermark for a group.
    /// Returns None if not cached or expired.
    pub fn get(&self, group_id: &ShardGroupId) -> Option<GroupStateWatermark> {
        let cache = self.cache.read();
        cache.get(group_id).and_then(|entry| {
            if entry.is_fresh(self.ttl) {
                Some(entry.watermark)
            } else {
                None
            }
        })
    }

    /// Update the cached watermark for a group.
    pub fn put(&self, watermark: GroupStateWatermark) {
        let cache = self.cache.write();
        cache.insert(watermark.group_id, CachedWatermark::new(watermark));
    }

    /// Update the cached watermark for a group if the new watermark is ahead.
    /// This ensures we only advance the watermark, never go backwards.
    pub fn update_if_ahead(&self, new_watermark: GroupStateWatermark) {
        let cache = self.cache.write();
        let should_update = cache
            .get(&new_watermark.group_id)
            .and_then(|entry| {
                new_watermark
                    .cmp_same_group(&entry.watermark)
                    .map(|ord| ord == std::cmp::Ordering::Greater)
            })
            .unwrap_or(true);

        if should_update {
            cache.insert(new_watermark.group_id, CachedWatermark::new(new_watermark));
        }
    }

    /// Merge a response state vector without rolling back any group.
    pub fn merge_if_ahead<I>(&self, watermarks: I)
    where
        I: IntoIterator<Item = GroupStateWatermark>,
    {
        for watermark in watermarks {
            self.update_if_ahead(watermark);
        }
    }

    /// Compare a watermark with the cached one for the same group.
    /// Returns:
    /// - Some(true) if cached watermark >= provided watermark (safe to read)
    /// - Some(false) if cached watermark < provided watermark (stale, need sync)
    /// - None if no cached watermark for this group or different groups
    pub fn compare(&self, watermark: &GroupStateWatermark) -> Option<bool> {
        self.get(&watermark.group_id).and_then(|cached| {
            cached
                .cmp_same_group(watermark)
                .map(|ord| ord != std::cmp::Ordering::Less)
        })
    }

    /// Invalidate the cached watermark for a group.
    pub fn invalidate(&self, group_id: &ShardGroupId) {
        let cache = self.cache.write();
        cache.remove(group_id);
    }

    /// Clear all cached watermarks.
    pub fn clear(&self) {
        let cache = self.cache.write();
        cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::{GroupStateWatermark, RaftLogId, ShardGroupId};

    #[test]
    fn merge_if_ahead_updates_each_group_without_rollback() {
        let cache = StateIdCache::new(60);
        let group_a = ShardGroupId::new(10);
        let group_b = ShardGroupId::new(20);

        cache.merge_if_ahead(vec![
            GroupStateWatermark::new(group_a, RaftLogId::new(1, 1, 10)),
            GroupStateWatermark::new(group_b, RaftLogId::new(1, 1, 5)),
        ]);
        cache.merge_if_ahead(vec![GroupStateWatermark::new(group_a, RaftLogId::new(1, 1, 8))]);
        cache.merge_if_ahead(vec![GroupStateWatermark::new(group_b, RaftLogId::new(1, 1, 6))]);

        assert_eq!(cache.get(&group_a).unwrap().state_id, RaftLogId::new(1, 1, 10));
        assert_eq!(cache.get(&group_b).unwrap().state_id, RaftLogId::new(1, 1, 6));
    }
}
