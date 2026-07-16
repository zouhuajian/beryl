// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! State ID cache for consistency checking.
//!
//! This cache stores the last seen GroupStateWatermark per group name for consistency checking.
//! Watermarks must be compared only within the same group name.

use beryl_types::{GroupName, GroupStateWatermark};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Cached watermark entry.
#[derive(Clone, Debug)]
pub(crate) struct CachedWatermark {
    /// The group watermark.
    watermark: GroupStateWatermark,
    /// When this entry was cached.
    cached_at: Instant,
}

impl CachedWatermark {
    /// Create a new cached watermark entry.
    fn new(watermark: GroupStateWatermark) -> Self {
        Self {
            watermark,
            cached_at: Instant::now(),
        }
    }

    /// Check if this entry is still fresh (within TTL).
    fn is_fresh(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() < ttl
    }
}

/// State ID cache per group.
#[derive(Clone, Debug)]
pub(crate) struct StateIdCache {
    /// Watermark map: group name -> CachedWatermark.
    cache: Arc<DashMap<GroupName, CachedWatermark>>,
    /// TTL for cache entries.
    ttl: Duration,
}

impl StateIdCache {
    /// Create a new state ID cache.
    pub(crate) fn new(ttl_secs: u64) -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    /// Get the cached watermark for a group.
    /// Returns None if not cached or expired.
    pub(crate) fn get(&self, group_name: &GroupName) -> Option<GroupStateWatermark> {
        self.cache.get(group_name).and_then(|entry| {
            if entry.is_fresh(self.ttl) {
                Some(entry.watermark.clone())
            } else {
                None
            }
        })
    }

    /// Update the cached watermark for a group if the new watermark is ahead.
    /// This ensures we only advance the watermark, never go backwards.
    pub(crate) fn update_if_ahead(&self, new_watermark: GroupStateWatermark) {
        use dashmap::mapref::entry::Entry;

        match self.cache.entry(new_watermark.group_name.clone()) {
            Entry::Occupied(mut entry) => {
                let should_update = new_watermark
                    .cmp_same_group(&entry.get().watermark)
                    .map(|ord| ord == std::cmp::Ordering::Greater)
                    .unwrap_or(false);
                if should_update {
                    entry.insert(CachedWatermark::new(new_watermark));
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(CachedWatermark::new(new_watermark));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::{GroupName, GroupStateWatermark, RaftLogId};

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
