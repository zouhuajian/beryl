// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! State ID cache for consistency checking.
//!
//! This cache stores the last seen GroupStateWatermark per group name for consistency checking.
//! Watermarks must be compared only within the same group name.

use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use types::{GroupName, GroupStateWatermark};

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
#[derive(Clone, Debug)]
pub struct StateIdCache {
    /// Watermark map: group name -> CachedWatermark.
    cache: Arc<RwLock<DashMap<GroupName, CachedWatermark>>>,
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
    pub fn get(&self, group_name: &GroupName) -> Option<GroupStateWatermark> {
        let cache = self.cache.read();
        cache.get(group_name).and_then(|entry| {
            if entry.is_fresh(self.ttl) {
                Some(entry.watermark.clone())
            } else {
                None
            }
        })
    }

    /// Update the cached watermark for a group if the new watermark is ahead.
    /// This ensures we only advance the watermark, never go backwards.
    pub fn update_if_ahead(&self, new_watermark: GroupStateWatermark) {
        let cache = self.cache.write();
        let should_update = cache
            .get(&new_watermark.group_name)
            .and_then(|entry| {
                new_watermark
                    .cmp_same_group(&entry.watermark)
                    .map(|ord| ord == std::cmp::Ordering::Greater)
            })
            .unwrap_or(true);

        if should_update {
            cache.insert(new_watermark.group_name.clone(), CachedWatermark::new(new_watermark));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::{GroupName, GroupStateWatermark, RaftLogId};

    #[test]
    fn merge_if_ahead_updates_each_group_without_rollback() {
        let cache = StateIdCache::new(60);
        let group_a = GroupName::parse("group-a").unwrap();
        let group_b = GroupName::parse("group-b").unwrap();

        cache.update_if_ahead(GroupStateWatermark::new(group_a.clone(), RaftLogId::new(1, 1, 10)));
        cache.update_if_ahead(GroupStateWatermark::new(group_b.clone(), RaftLogId::new(1, 1, 5)));
        cache.update_if_ahead(GroupStateWatermark::new(group_a.clone(), RaftLogId::new(1, 1, 8)));
        cache.update_if_ahead(GroupStateWatermark::new(group_b.clone(), RaftLogId::new(1, 1, 6)));

        assert_eq!(cache.get(&group_a).unwrap().state_id, RaftLogId::new(1, 1, 10));
        assert_eq!(cache.get(&group_b).unwrap().state_id, RaftLogId::new(1, 1, 6));
    }
}
