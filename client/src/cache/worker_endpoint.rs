// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata-authoritative worker endpoint cache.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::RwLock;
use types::{WorkerEndpointInfo, WorkerNetProtocol};

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
    protocol: WorkerNetProtocol,
    endpoint: String,
    worker_epoch: u64,
}

impl WorkerEndpointCacheKey {
    fn from_candidate(candidate: &WorkerEndpointInfo) -> ClientResult<Self> {
        validate_worker_endpoint(candidate)?;
        Ok(Self {
            worker_id: candidate.worker_id.as_raw(),
            protocol: candidate.worker_net_protocol,
            endpoint: candidate.endpoint.clone(),
            worker_epoch: candidate.worker_epoch,
        })
    }
}

#[derive(Clone, Debug)]
struct CachedWorkerEndpoint {
    endpoint: WorkerEndpointInfo,
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
    health_enabled: bool,
    health_failure_threshold: usize,
    health_ttl: Duration,
    cache: Arc<RwLock<LruCache<WorkerEndpointCacheKey, CachedWorkerEndpoint>>>,
    health: Arc<RwLock<std::collections::HashMap<WorkerEndpointCacheKey, EndpointHealth>>>,
    metrics: Arc<dyn ClientMetrics>,
}

#[derive(Clone, Debug, Default)]
struct EndpointHealth {
    consecutive_failures: usize,
    unhealthy_until: Option<Instant>,
}

impl WorkerEndpointCache {
    /// Create a worker endpoint cache from client config.
    pub(crate) fn from_config(config: &CacheConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::with_policy(config, metrics)
    }

    /// Create a worker endpoint cache with the system clock.
    pub(crate) fn new(enabled: bool, ttl: Duration, max_entries: usize, metrics: Arc<dyn ClientMetrics>) -> Self {
        let capacity = NonZeroUsize::new(max_entries.max(1)).expect("capacity is non-zero");
        Self {
            enabled,
            ttl,
            health_enabled: true,
            health_failure_threshold: 2,
            health_ttl: Duration::from_secs(5),
            cache: Arc::new(RwLock::new(LruCache::new(capacity))),
            health: Arc::new(RwLock::new(std::collections::HashMap::new())),
            metrics,
        }
    }

    /// Create a worker endpoint cache from all cache policy options.
    pub(crate) fn with_policy(config: &CacheConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        let capacity =
            NonZeroUsize::new(config.worker_endpoint_cache_max_entries.max(1)).expect("capacity is non-zero");
        Self {
            enabled: config.worker_endpoint_cache_enabled,
            ttl: config.worker_endpoint_cache_ttl,
            health_enabled: config.endpoint_health_enabled,
            health_failure_threshold: config.endpoint_health_failure_threshold.max(1),
            health_ttl: config.endpoint_health_ttl,
            cache: Arc::new(RwLock::new(LruCache::new(capacity))),
            health: Arc::new(RwLock::new(std::collections::HashMap::new())),
            metrics,
        }
    }

    /// Create a disabled worker endpoint cache.
    pub(crate) fn disabled() -> Self {
        Self::new(false, Duration::ZERO, 1, Arc::new(NoopClientMetrics))
    }

    /// Resolve and cache a metadata-authoritative endpoint candidate.
    pub(crate) async fn get_or_resolve_authoritative(
        &self,
        candidate: &WorkerEndpointInfo,
    ) -> ClientResult<WorkerEndpointInfo> {
        self.get_or_resolve_authoritative_with(candidate, |candidate| async move {
            tokio::task::yield_now().await;
            Ok(candidate)
        })
        .await
    }

    /// Resolve and cache a candidate through an injected resolver.
    pub(crate) async fn get_or_resolve_authoritative_with<F, Fut>(
        &self,
        candidate: &WorkerEndpointInfo,
        resolver: F,
    ) -> ClientResult<WorkerEndpointInfo>
    where
        F: FnOnce(WorkerEndpointInfo) -> Fut + Send + 'static,
        Fut: Future<Output = ClientResult<WorkerEndpointInfo>> + Send + 'static,
    {
        let key = match WorkerEndpointCacheKey::from_candidate(candidate) {
            Ok(key) => key,
            Err(err) => {
                self.invalidate_all(CacheInvalidationReason::Protocol);
                return Err(err);
            }
        };
        self.record(ClientMetric::WorkerEndpointCacheLookup, "lookup", None);
        if !self.is_key_healthy(&key) {
            self.record(
                ClientMetric::WorkerEndpointHealthFailure,
                "unhealthy",
                Some(CacheInvalidationReason::Unavailable),
            );
            return Err(ClientError::Worker(
                "worker endpoint is temporarily unavailable".to_string(),
            ));
        }
        if let Some(endpoint) = self.get_cached_after_lookup(&key) {
            return Ok(endpoint);
        }

        let resolved = resolver(candidate.clone()).await?;
        self.insert_resolved(key, resolved.clone());
        Ok(resolved)
    }

    /// Resolve and cache a metadata-authoritative endpoint candidate.
    #[cfg(test)]
    pub(crate) fn get_or_insert_authoritative(
        &self,
        candidate: &WorkerEndpointInfo,
    ) -> ClientResult<WorkerEndpointInfo> {
        let key = match WorkerEndpointCacheKey::from_candidate(candidate) {
            Ok(key) => key,
            Err(err) => {
                self.invalidate_all(CacheInvalidationReason::Protocol);
                return Err(err);
            }
        };
        self.record(ClientMetric::WorkerEndpointCacheLookup, "lookup", None);
        if let Some(endpoint) = self.get_cached_after_lookup(&key) {
            return Ok(endpoint);
        }
        self.insert_resolved(key, candidate.clone());
        Ok(candidate.clone())
    }

    /// Invalidate one candidate endpoint if its key is valid.
    pub(crate) fn invalidate_candidate(&self, candidate: &WorkerEndpointInfo, reason: CacheInvalidationReason) {
        let Ok(key) = WorkerEndpointCacheKey::from_candidate(candidate) else {
            self.invalidate_all(reason);
            return;
        };
        let removed = self.cache.write().pop(&key).is_some();
        if removed {
            self.record(ClientMetric::WorkerEndpointCacheInvalidate, "invalidated", Some(reason));
            self.metrics.record(ClientMetricEvent::new(
                ClientMetric::CachePreciseInvalidation,
                cache_labels(CACHE_NAME, PLANE, OPERATION, "precise").with_reason(reason.label()),
            ));
        }
    }

    /// Record a retryable failure for one endpoint candidate.
    pub(crate) fn record_candidate_failure(&self, candidate: &WorkerEndpointInfo, reason: CacheInvalidationReason) {
        let Ok(key) = WorkerEndpointCacheKey::from_candidate(candidate) else {
            self.invalidate_all(reason);
            return;
        };
        if !self.health_enabled {
            self.invalidate_candidate(candidate, reason);
            return;
        }
        let now = Instant::now();
        let mut health = self.health.write();
        let entry = health.entry(key).or_default();
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        if entry.consecutive_failures >= self.health_failure_threshold {
            entry.unhealthy_until = Some(now + self.health_ttl);
            drop(health);
            self.record(ClientMetric::WorkerEndpointHealthFailure, "failure", Some(reason));
            self.invalidate_candidate(candidate, reason);
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
            self.metrics.record(ClientMetricEvent::new(
                ClientMetric::CacheBroadInvalidationFallback,
                cache_labels(CACHE_NAME, PLANE, OPERATION, "broad").with_reason(reason.label()),
            ));
        }
    }

    /// Return current entry count.
    pub(crate) fn len(&self) -> usize {
        self.cache.read().len()
    }

    fn reuse_enabled(&self) -> bool {
        self.enabled && !self.ttl.is_zero()
    }

    fn get_cached_after_lookup(&self, key: &WorkerEndpointCacheKey) -> Option<WorkerEndpointInfo> {
        if !self.reuse_enabled() {
            self.record(ClientMetric::WorkerEndpointCacheMiss, "miss", None);
            return None;
        }

        let now = Instant::now();
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get(key) {
            if entry.is_expired(now, self.ttl) {
                cache.pop(key);
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
                return Some(endpoint);
            }
        } else {
            drop(cache);
            self.record(ClientMetric::WorkerEndpointCacheMiss, "miss", None);
        }
        None
    }

    fn insert_resolved(&self, key: WorkerEndpointCacheKey, endpoint: WorkerEndpointInfo) {
        if !self.reuse_enabled() {
            return;
        }
        let evicted = {
            let mut cache = self.cache.write();
            if cache.contains(&key) {
                return;
            }
            cache.push(
                key.clone(),
                CachedWorkerEndpoint {
                    endpoint,
                    inserted_at: Instant::now(),
                },
            )
        };
        if evicted.is_some() {
            self.record(ClientMetric::WorkerEndpointCacheEvict, "evicted", None);
        }
        self.health.write().remove(&key);
    }

    fn is_key_healthy(&self, key: &WorkerEndpointCacheKey) -> bool {
        if !self.health_enabled {
            return true;
        }
        let now = Instant::now();
        let mut health = self.health.write();
        let Some(entry) = health.get(key) else {
            return true;
        };
        if let Some(unhealthy_until) = entry.unhealthy_until {
            if now < unhealthy_until {
                return false;
            }
            health.remove(key);
            drop(health);
            self.record(
                ClientMetric::WorkerEndpointHealthRecovery,
                "recovery",
                Some(CacheInvalidationReason::Ttl),
            );
            return true;
        }
        true
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

fn validate_worker_endpoint(candidate: &WorkerEndpointInfo) -> ClientResult<()> {
    if candidate.worker_id.as_raw() == 0 {
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
        WorkerNetProtocol::Grpc => Ok(()),
        WorkerNetProtocol::Quic | WorkerNetProtocol::Rdma => {
            Err(ClientError::Unsupported("unsupported worker net protocol".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::sync::Notify;

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
    fn unsupported_protocol_is_rejected_without_insert() {
        let cache = WorkerEndpointCache::new(true, Duration::from_secs(60), 8, Arc::new(NoopClientMetrics));
        let mut unsupported = endpoint(1, 7);
        unsupported.worker_net_protocol = WorkerNetProtocol::Quic;

        let err = cache
            .get_or_insert_authoritative(&unsupported)
            .expect_err("unsupported protocol rejected");

        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("unsupported")));
        assert_eq!(cache.len(), 0);
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

    #[tokio::test]
    async fn concurrent_same_endpoint_misses_resolve_directly_and_keep_one_cached_entry() {
        let metrics = Arc::new(RecordingMetrics::default());
        let cache = WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone());
        let attempts = Arc::new(AtomicUsize::new(0));
        let all_started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let candidate = endpoint(1, 7);
        const TASKS: usize = 4;

        let mut tasks = Vec::with_capacity(TASKS);
        for _ in 0..TASKS {
            let cache = cache.clone();
            let attempts = Arc::clone(&attempts);
            let all_started = Arc::clone(&all_started);
            let release = Arc::clone(&release);
            let candidate = candidate.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_resolve_authoritative_with(&candidate, move |candidate| async move {
                        let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                        if current == TASKS {
                            all_started.notify_one();
                        }
                        release.notified().await;
                        Ok(candidate)
                    })
                    .await
            }));
        }
        all_started.notified().await;
        release.notify_waiters();

        for task in tasks {
            assert_eq!(task.await.expect("task").expect("endpoint"), candidate);
        }
        assert_eq!(attempts.load(Ordering::SeqCst), TASKS);
        assert_eq!(cache.len(), 1);
        assert_metric(&metrics.events(), ClientMetric::WorkerEndpointCacheMiss);
    }

    #[tokio::test]
    async fn worker_endpoint_different_epoch_does_not_share_resolution() {
        let cache = WorkerEndpointCache::new(true, Duration::from_secs(60), 8, Arc::new(NoopClientMetrics));
        let attempts = Arc::new(AtomicUsize::new(0));

        let first = {
            let cache = cache.clone();
            let attempts = Arc::clone(&attempts);
            tokio::spawn(async move {
                cache
                    .get_or_resolve_authoritative_with(&endpoint(1, 7), move |candidate| async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        Ok(candidate)
                    })
                    .await
            })
        };
        let second = {
            let cache = cache.clone();
            let attempts = Arc::clone(&attempts);
            tokio::spawn(async move {
                cache
                    .get_or_resolve_authoritative_with(&endpoint(1, 8), move |candidate| async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        Ok(candidate)
                    })
                    .await
            })
        };

        first.await.expect("first").expect("first endpoint");
        second.await.expect("second").expect("second endpoint");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn endpoint_resolution_failure_does_not_insert_or_poison_cache() {
        let metrics = Arc::new(RecordingMetrics::default());
        let cache = WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone());
        let attempts = Arc::new(AtomicUsize::new(0));
        let all_started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let candidate = endpoint(1, 7);
        const TASKS: usize = 4;

        let mut tasks = Vec::with_capacity(TASKS);
        for _ in 0..TASKS {
            let cache = cache.clone();
            let attempts = Arc::clone(&attempts);
            let all_started = Arc::clone(&all_started);
            let release = Arc::clone(&release);
            let candidate = candidate.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_resolve_authoritative_with(&candidate, move |_candidate| async move {
                        let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                        if current == TASKS {
                            all_started.notify_one();
                        }
                        release.notified().await;
                        Err(ClientError::Worker("injected endpoint resolution failure".to_string()))
                    })
                    .await
            }));
        }
        all_started.notified().await;
        release.notify_waiters();

        for task in tasks {
            let err = task.await.expect("task").expect_err("resolution failure");
            assert!(matches!(err, ClientError::Worker(msg) if msg.contains("injected endpoint resolution failure")));
        }
        assert_eq!(attempts.load(Ordering::SeqCst), TASKS);
        assert_eq!(cache.len(), 0);

        cache
            .get_or_resolve_authoritative_with(&candidate, |candidate| async move { Ok(candidate) })
            .await
            .expect("future resolution is not poisoned");
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn endpoint_health_penalty_is_epoch_scoped() {
        let metrics = Arc::new(RecordingMetrics::default());
        let cache = WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone());
        let stale_epoch = endpoint(1, 7);
        let fresh_epoch = endpoint(1, 8);

        cache.record_candidate_failure(&stale_epoch, CacheInvalidationReason::Unavailable);
        cache.record_candidate_failure(&stale_epoch, CacheInvalidationReason::Unavailable);

        let err = cache
            .get_or_resolve_authoritative_with(&stale_epoch, |candidate| async move { Ok(candidate) })
            .await
            .expect_err("penalized endpoint is rejected");
        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("temporarily unavailable")));

        cache
            .get_or_resolve_authoritative_with(&fresh_epoch, |candidate| async move { Ok(candidate) })
            .await
            .expect("fresh epoch remains usable");
        assert_metric(&metrics.events(), ClientMetric::WorkerEndpointHealthFailure);
    }

    fn assert_metric(events: &[ClientMetricEvent], metric: ClientMetric) {
        assert!(
            events.iter().any(|event| event.metric == metric),
            "missing metric {metric:?}: {events:?}"
        );
    }

    fn endpoint(worker_id: u64, worker_epoch: u64) -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: types::WorkerId::new(worker_id),
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: WorkerNetProtocol::Grpc,
            worker_epoch,
        }
    }
}
