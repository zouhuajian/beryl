// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata-authoritative worker endpoint cache.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::RwLock;
use proto::common::{WorkerEndpointInfoProto, WorkerNetProtocolProto};

use crate::cache::layout::{CacheClock, SystemCacheClock};
use crate::cache::{cache_labels, CacheInvalidationReason};
use crate::config::CacheConfig;
use crate::error::{ClientError, ClientResult};
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetrics, NoopClientMetrics};

const CACHE_NAME: &str = "worker_endpoint";
const PLANE: &str = "worker";
const OPERATION: &str = "read";

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct WorkerEndpointCacheKey {
    worker_id: u64,
    protocol: i32,
    endpoint: String,
    worker_epoch: u64,
}

impl WorkerEndpointCacheKey {
    fn from_candidate(candidate: &WorkerEndpointInfoProto) -> ClientResult<Self> {
        validate_worker_endpoint(candidate)?;
        Ok(Self {
            worker_id: candidate.worker_id,
            protocol: candidate.worker_net_protocol,
            endpoint: candidate.endpoint.clone(),
            worker_epoch: candidate.worker_epoch,
        })
    }
}

#[derive(Clone, Debug)]
struct CachedWorkerEndpoint {
    endpoint: WorkerEndpointInfoProto,
    inserted_at: Instant,
}

impl CachedWorkerEndpoint {
    fn is_expired(&self, now: Instant, ttl: Duration) -> bool {
        now.duration_since(self.inserted_at) >= ttl
    }
}

/// Thread-safe worker endpoint cache.
#[derive(Clone)]
pub(crate) struct WorkerEndpointCache {
    enabled: bool,
    ttl: Duration,
    cache: Arc<RwLock<LruCache<WorkerEndpointCacheKey, CachedWorkerEndpoint>>>,
    clock: Arc<dyn CacheClock>,
    metrics: Arc<dyn ClientMetrics>,
}

impl WorkerEndpointCache {
    /// Create a worker endpoint cache from client config.
    pub(crate) fn from_config(config: &CacheConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::new(
            config.worker_endpoint_cache_enabled,
            config.worker_endpoint_cache_ttl,
            config.worker_endpoint_cache_max_entries,
            metrics,
        )
    }

    /// Create a worker endpoint cache with the system clock.
    pub(crate) fn new(enabled: bool, ttl: Duration, max_entries: usize, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::with_clock(enabled, ttl, max_entries, metrics, Arc::new(SystemCacheClock))
    }

    /// Create a worker endpoint cache with an injected clock.
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

    /// Create a disabled worker endpoint cache.
    pub(crate) fn disabled() -> Self {
        Self::new(false, Duration::ZERO, 1, Arc::new(NoopClientMetrics))
    }

    /// Resolve and cache a metadata-authoritative endpoint candidate.
    pub(crate) fn get_or_insert_authoritative(
        &self,
        candidate: &WorkerEndpointInfoProto,
    ) -> ClientResult<WorkerEndpointInfoProto> {
        let key = match WorkerEndpointCacheKey::from_candidate(candidate) {
            Ok(key) => key,
            Err(err) => {
                self.invalidate_all(CacheInvalidationReason::Protocol);
                return Err(err);
            }
        };
        self.record(ClientMetric::WorkerEndpointCacheLookup, "lookup", None);
        if !self.reuse_enabled() {
            self.record(ClientMetric::WorkerEndpointCacheMiss, "miss", None);
            return Ok(candidate.clone());
        }

        let now = self.clock.now();
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get(&key) {
            if entry.is_expired(now, self.ttl) {
                cache.pop(&key);
                drop(cache);
                self.record(
                    ClientMetric::WorkerEndpointCacheExpired,
                    "expired",
                    Some(CacheInvalidationReason::Ttl),
                );
            } else {
                let endpoint = entry.endpoint.clone();
                drop(cache);
                self.record(ClientMetric::WorkerEndpointCacheHit, "hit", None);
                return Ok(endpoint);
            }
        } else {
            drop(cache);
            self.record(ClientMetric::WorkerEndpointCacheMiss, "miss", None);
        }

        let evicted = self.cache.write().push(
            key,
            CachedWorkerEndpoint {
                endpoint: candidate.clone(),
                inserted_at: now,
            },
        );
        if evicted.is_some() {
            self.record(ClientMetric::WorkerEndpointCacheEvict, "evicted", None);
        }
        Ok(candidate.clone())
    }

    /// Invalidate one candidate endpoint if its key is valid.
    pub(crate) fn invalidate_candidate(&self, candidate: &WorkerEndpointInfoProto, reason: CacheInvalidationReason) {
        let Ok(key) = WorkerEndpointCacheKey::from_candidate(candidate) else {
            self.invalidate_all(reason);
            return;
        };
        let removed = self.cache.write().pop(&key).is_some();
        if removed {
            self.record(ClientMetric::WorkerEndpointCacheInvalidate, "invalidated", Some(reason));
        }
    }

    /// Invalidate all cached endpoints for a correctness reason.
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
            self.record(ClientMetric::WorkerEndpointCacheInvalidate, "invalidated", Some(reason));
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

impl std::fmt::Debug for WorkerEndpointCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerEndpointCache")
            .field("enabled", &self.enabled)
            .field("ttl", &self.ttl)
            .field("entries", &self.len())
            .finish_non_exhaustive()
    }
}

fn validate_worker_endpoint(candidate: &WorkerEndpointInfoProto) -> ClientResult<()> {
    if candidate.worker_id == 0 {
        return Err(ClientError::InvalidLayout(
            "worker endpoint candidate worker_id must be non-zero".to_string(),
        ));
    }
    if candidate.endpoint.is_empty() {
        return Err(ClientError::InvalidArgument(
            "worker endpoint must not be empty".to_string(),
        ));
    }
    if candidate.worker_epoch == 0 {
        return Err(ClientError::InvalidLayout(
            "worker endpoint candidate worker_epoch must be non-zero".to_string(),
        ));
    }
    match candidate.worker_net_protocol {
        value if value == WorkerNetProtocolProto::WorkerNetProtocolUnspecified as i32 => Err(
            ClientError::InvalidArgument("unspecified worker_net_protocol must not default to gRPC".to_string()),
        ),
        value if value == WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32 => Ok(()),
        value
            if value == WorkerNetProtocolProto::WorkerNetProtocolQuic as i32
                || value == WorkerNetProtocolProto::WorkerNetProtocolRdma as i32 =>
        {
            Err(ClientError::Unsupported("unsupported worker net protocol".to_string()))
        }
        other => Err(ClientError::InvalidArgument(format!(
            "unknown worker_net_protocol value {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

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
    fn endpoint_cache_hits_for_same_worker_identity_and_epoch() {
        let metrics = Arc::new(RecordingMetrics::default());
        let cache = WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone());
        let candidate = endpoint(1, 7);

        let first = cache
            .get_or_insert_authoritative(&candidate)
            .expect("first authoritative endpoint");
        let second = cache
            .get_or_insert_authoritative(&candidate)
            .expect("cached authoritative endpoint");

        assert_eq!(first, second);
        assert_eq!(cache.len(), 1);
        assert_metric(&metrics.events(), ClientMetric::WorkerEndpointCacheHit);
    }

    #[test]
    fn worker_epoch_mismatch_uses_distinct_cache_key() {
        let cache = WorkerEndpointCache::new(true, Duration::from_secs(60), 8, Arc::new(NoopClientMetrics));

        cache.get_or_insert_authoritative(&endpoint(1, 7)).expect("first epoch");
        cache
            .get_or_insert_authoritative(&endpoint(1, 8))
            .expect("second epoch");

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn invalid_or_unspecified_protocol_is_rejected_without_insert() {
        let cache = WorkerEndpointCache::new(true, Duration::from_secs(60), 8, Arc::new(NoopClientMetrics));
        let mut unspecified = endpoint(1, 7);
        unspecified.worker_net_protocol = WorkerNetProtocolProto::WorkerNetProtocolUnspecified as i32;
        let mut unknown = endpoint(1, 7);
        unknown.worker_net_protocol = 99;

        let unspecified_err = cache
            .get_or_insert_authoritative(&unspecified)
            .expect_err("unspecified protocol rejected");
        let unknown_err = cache
            .get_or_insert_authoritative(&unknown)
            .expect_err("unknown protocol rejected");

        assert!(matches!(unspecified_err, ClientError::InvalidArgument(msg) if msg.contains("unspecified")));
        assert!(matches!(unknown_err, ClientError::InvalidArgument(msg) if msg.contains("unknown")));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn ttl_expiry_is_miss_without_sleeping() {
        let metrics = Arc::new(RecordingMetrics::default());
        let clock = Arc::new(ManualClock::new(Instant::now()));
        let cache = WorkerEndpointCache::with_clock(true, Duration::from_secs(5), 8, metrics.clone(), clock.clone());
        let candidate = endpoint(1, 7);

        cache.get_or_insert_authoritative(&candidate).expect("insert");
        clock.advance(Duration::from_secs(5));
        cache
            .get_or_insert_authoritative(&candidate)
            .expect("refresh after expiry");

        assert_metric(&metrics.events(), ClientMetric::WorkerEndpointCacheExpired);
    }

    #[test]
    fn unavailable_invalidation_evicts_candidate() {
        let metrics = Arc::new(RecordingMetrics::default());
        let cache = WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone());
        let candidate = endpoint(1, 7);

        cache.get_or_insert_authoritative(&candidate).expect("insert");
        cache.invalidate_candidate(&candidate, CacheInvalidationReason::Unavailable);

        assert_eq!(cache.len(), 0);
        assert_metric(&metrics.events(), ClientMetric::WorkerEndpointCacheInvalidate);
    }

    fn assert_metric(events: &[ClientMetricEvent], metric: ClientMetric) {
        assert!(
            events.iter().any(|event| event.metric == metric),
            "missing metric {metric:?}: {events:?}"
        );
    }

    fn endpoint(worker_id: u64, worker_epoch: u64) -> WorkerEndpointInfoProto {
        WorkerEndpointInfoProto {
            worker_id,
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
            worker_epoch,
        }
    }
}
