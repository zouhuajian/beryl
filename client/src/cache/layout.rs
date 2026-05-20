// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Validated read-layout cache.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::RwLock;
use types::{DataHandleId, InodeId};

use crate::cache::{cache_labels, CacheInvalidationReason};
use crate::config::CacheConfig;
use crate::error::{ClientError, ClientResult};
use crate::metadata::LayoutSnapshot;
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetrics};
use crate::planner::read_planner::{PlannedReadRange, ReadPlanner};

const CACHE_NAME: &str = "layout";
const PLANE: &str = "metadata";
const OPERATION: &str = "read";

/// Monotonic clock used by client caches.
pub(crate) trait CacheClock: Send + Sync + std::fmt::Debug {
    /// Return the current monotonic instant.
    fn now(&self) -> Instant;
}

/// System monotonic cache clock.
#[derive(Debug, Default)]
pub(crate) struct SystemCacheClock;

impl CacheClock for SystemCacheClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Safe key for a validated read-layout cache entry.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct LayoutCacheKey {
    inode_id: u64,
    data_handle_id: u64,
    file_version: u64,
    file_offset: u64,
    len: u32,
}

impl LayoutCacheKey {
    /// Build a cache key from stable read-handle identity and requested span.
    pub(crate) fn new(
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        file_version: u64,
        span: PlannedReadRange,
    ) -> Self {
        Self {
            inode_id: inode_id.0,
            data_handle_id: data_handle_id.as_raw(),
            file_version,
            file_offset: span.file_offset,
            len: span.len,
        }
    }

    fn span(self) -> PlannedReadRange {
        PlannedReadRange {
            file_offset: self.file_offset,
            len: self.len,
        }
    }

    fn inode_id(self) -> InodeId {
        InodeId::new(self.inode_id)
    }

    fn data_handle_id(self) -> DataHandleId {
        DataHandleId::new(self.data_handle_id)
    }
}

#[derive(Clone, Debug)]
struct CachedLayout {
    response: LayoutSnapshot,
    inserted_at: Instant,
}

impl CachedLayout {
    fn is_expired(&self, now: Instant, ttl: Duration) -> bool {
        now.duration_since(self.inserted_at) >= ttl
    }
}

/// Thread-safe validated read-layout cache.
#[derive(Clone)]
pub(crate) struct LayoutCache {
    enabled: bool,
    ttl: Duration,
    cache: Arc<RwLock<LruCache<LayoutCacheKey, CachedLayout>>>,
    clock: Arc<dyn CacheClock>,
    metrics: Arc<dyn ClientMetrics>,
}

impl LayoutCache {
    /// Create a layout cache from client config.
    pub(crate) fn from_config(config: &CacheConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::new(
            config.layout_cache_enabled,
            config.layout_cache_ttl,
            config.layout_cache_max_entries,
            metrics,
        )
    }

    /// Create a layout cache with the system clock.
    pub(crate) fn new(enabled: bool, ttl: Duration, max_entries: usize, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::with_clock(enabled, ttl, max_entries, metrics, Arc::new(SystemCacheClock))
    }

    /// Create a layout cache with an injected clock.
    pub(crate) fn with_clock(
        enabled: bool,
        ttl: Duration,
        max_entries: usize,
        metrics: Arc<dyn ClientMetrics>,
        clock: Arc<dyn CacheClock>,
    ) -> Self {
        let capacity = NonZeroUsize::new(max_entries.max(1)).expect("capacity is non-zero");
        Self {
            enabled,
            ttl,
            cache: Arc::new(RwLock::new(LruCache::new(capacity))),
            clock,
            metrics,
        }
    }

    /// Return a validated cached layout, or None on miss/expiry/disabled state.
    pub(crate) fn get(&self, key: &LayoutCacheKey) -> Option<LayoutSnapshot> {
        self.record(ClientMetric::LayoutCacheLookup, "lookup", None);
        if !self.reuse_enabled() {
            self.record(ClientMetric::LayoutCacheMiss, "miss", None);
            return None;
        }

        let now = self.clock.now();
        let mut cache = self.cache.write();
        let Some(entry) = cache.get(key) else {
            drop(cache);
            self.record(ClientMetric::LayoutCacheMiss, "miss", None);
            return None;
        };
        if entry.is_expired(now, self.ttl) {
            cache.pop(key);
            drop(cache);
            self.record(
                ClientMetric::LayoutCacheExpired,
                "expired",
                Some(CacheInvalidationReason::Ttl),
            );
            return None;
        }
        let response = entry.response.clone();
        drop(cache);
        self.record(ClientMetric::LayoutCacheHit, "hit", None);
        Some(response)
    }

    /// Insert a layout after validating identity, coverage, and block stamps.
    pub(crate) fn insert_validated(&self, key: LayoutCacheKey, response: LayoutSnapshot) -> ClientResult<()> {
        if let Err(err) = validate_layout_for_key(key, &response) {
            match &err {
                ClientError::StaleHandle { .. } => self.invalidate_all(CacheInvalidationReason::DataHandle),
                ClientError::VersionMismatch { .. } => self.invalidate_all(CacheInvalidationReason::FileVersion),
                _ => {}
            }
            return Err(err);
        }
        if !self.reuse_enabled() {
            return Ok(());
        }
        let inserted = CachedLayout {
            response,
            inserted_at: self.clock.now(),
        };
        let evicted = self.cache.write().push(key, inserted).is_some();
        self.record(ClientMetric::LayoutCacheInsert, "insert", None);
        if evicted {
            self.record(ClientMetric::LayoutCacheEvict, "evicted", None);
        }
        Ok(())
    }

    /// Invalidate all cached layouts for a correctness reason.
    pub(crate) fn invalidate_all(&self, reason: CacheInvalidationReason) {
        if !self.enabled {
            return;
        }
        let removed = {
            let mut cache = self.cache.write();
            let removed = cache.len();
            cache.clear();
            removed
        };
        if removed > 0 {
            self.record(ClientMetric::LayoutCacheInvalidate, "invalidated", Some(reason));
        }
    }

    /// Return current entry count.
    pub(crate) fn len(&self) -> usize {
        self.cache.read().len()
    }

    fn reuse_enabled(&self) -> bool {
        self.enabled && !self.ttl.is_zero()
    }

    fn record(&self, metric: ClientMetric, outcome: &'static str, reason: Option<CacheInvalidationReason>) {
        let mut labels = cache_labels(CACHE_NAME, PLANE, OPERATION, outcome);
        if let Some(reason) = reason {
            labels = labels.with_reason(reason.label());
        }
        self.metrics.record(ClientMetricEvent::new(metric, labels));
    }
}

impl std::fmt::Debug for LayoutCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayoutCache")
            .field("enabled", &self.enabled)
            .field("ttl", &self.ttl)
            .field("entries", &self.len())
            .finish_non_exhaustive()
    }
}

fn validate_layout_for_key(key: LayoutCacheKey, response: &LayoutSnapshot) -> ClientResult<()> {
    ReadPlanner::resolve_response(
        key.inode_id(),
        key.data_handle_id(),
        Some(key.file_version),
        key.span(),
        response,
    )
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::metrics::NoopClientMetrics;
    use types::{BlockId, BlockIndex, FileBlockLocation, WorkerEndpointInfo, WorkerId, WorkerNetProtocol};

    #[derive(Debug)]
    struct ManualClock {
        now: Mutex<Instant>,
    }

    impl ManualClock {
        fn new(now: Instant) -> Self {
            Self { now: Mutex::new(now) }
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.now.lock().expect("clock");
            *now += duration;
        }
    }

    impl CacheClock for ManualClock {
        fn now(&self) -> Instant {
            *self.now.lock().expect("clock")
        }
    }

    #[derive(Debug, Default)]
    struct RecordingMetrics {
        events: Mutex<Vec<ClientMetricEvent>>,
    }

    impl ClientMetrics for RecordingMetrics {
        fn record(&self, event: ClientMetricEvent) {
            self.events.lock().expect("events").push(event);
        }
    }

    impl RecordingMetrics {
        fn events(&self) -> Vec<ClientMetricEvent> {
            self.events.lock().expect("events").clone()
        }
    }

    #[test]
    fn disabled_cache_misses_without_inserted_reuse() {
        let metrics = Arc::new(RecordingMetrics::default());
        let cache = LayoutCache::new(false, Duration::from_secs(60), 8, metrics.clone());
        let key = key(0, 4);

        cache
            .insert_validated(key, layout(vec![location(0, 4, 77)]))
            .expect("valid layout");

        assert!(cache.get(&key).is_none());
        assert_metric(&metrics.events(), ClientMetric::LayoutCacheMiss);
    }

    #[test]
    fn ttl_expiry_is_a_miss_without_sleeping() {
        let metrics = Arc::new(RecordingMetrics::default());
        let clock = Arc::new(ManualClock::new(Instant::now()));
        let cache = LayoutCache::with_clock(true, Duration::from_secs(5), 8, metrics.clone(), clock.clone());
        let key = key(0, 4);

        cache
            .insert_validated(key, layout(vec![location(0, 4, 77)]))
            .expect("valid layout");
        clock.advance(Duration::from_secs(5));

        assert!(cache.get(&key).is_none());
        assert_metric(&metrics.events(), ClientMetric::LayoutCacheExpired);
    }

    #[test]
    fn max_entries_eviction_is_bounded_and_deterministic() {
        let metrics = Arc::new(RecordingMetrics::default());
        let cache = LayoutCache::new(true, Duration::from_secs(60), 1, metrics.clone());
        let first = key(0, 4);
        let second = key(4, 4);

        cache
            .insert_validated(first, layout(vec![location(0, 4, 77)]))
            .expect("first layout");
        cache
            .insert_validated(second, layout(vec![location(4, 4, 88)]))
            .expect("second layout");

        assert!(cache.get(&first).is_none());
        assert!(cache.get(&second).is_some());
        assert_eq!(cache.len(), 1);
        assert_metric(&metrics.events(), ClientMetric::LayoutCacheEvict);
    }

    #[test]
    fn zero_block_stamp_is_rejected_before_insert() {
        let cache = LayoutCache::new(true, Duration::from_secs(60), 8, Arc::new(NoopClientMetrics));
        let key = key(0, 4);

        let zero = cache
            .insert_validated(key, layout(vec![location(0, 4, 0)]))
            .expect_err("zero stamp rejected");

        assert!(matches!(zero, ClientError::InvalidLayout(msg) if msg.contains("block_stamp")));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn unordered_layout_is_cached_with_stamps_preserved() {
        let cache = LayoutCache::new(true, Duration::from_secs(60), 8, Arc::new(NoopClientMetrics));
        let key = key(0, 8);
        let response = layout(vec![location(4, 4, 88), location(0, 4, 77)]);

        cache.insert_validated(key, response).expect("unordered valid layout");
        let cached = cache.get(&key).expect("cached layout");
        let (_, segments) =
            ReadPlanner::resolve_response(InodeId::new(101), DataHandleId::new(202), Some(3), key.span(), &cached)
                .expect("cached response resolves");

        assert_eq!(
            segments.iter().map(|segment| segment.block_stamp).collect::<Vec<_>>(),
            vec![77, 88]
        );
    }

    fn assert_metric(events: &[ClientMetricEvent], metric: ClientMetric) {
        assert!(
            events.iter().any(|event| event.metric == metric),
            "missing metric {metric:?}: {events:?}"
        );
    }

    fn key(file_offset: u64, len: u32) -> LayoutCacheKey {
        LayoutCacheKey::new(
            InodeId::new(101),
            DataHandleId::new(202),
            3,
            PlannedReadRange { file_offset, len },
        )
    }

    fn layout(locations: Vec<FileBlockLocation>) -> LayoutSnapshot {
        LayoutSnapshot {
            group_id: 9,
            inode_id: InodeId::new(101),
            data_handle_id: DataHandleId::new(202),
            file_size: 16,
            file_version: Some(3),
            locations,
        }
    }

    fn location(file_offset: u64, len: u64, block_stamp: u64) -> FileBlockLocation {
        FileBlockLocation {
            block_id: BlockId::new(DataHandleId::new(202), BlockIndex::new((file_offset / 4) as u32)),
            file_offset,
            len,
            workers: vec![WorkerEndpointInfo {
                worker_id: WorkerId::new(1),
                endpoint: "127.0.0.1:19101".to_string(),
                worker_net_protocol: WorkerNetProtocol::Grpc,
                worker_epoch: 7,
            }],
            worker_epoch: Some(7),
            block_stamp,
        }
    }
}
