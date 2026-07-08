// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! gRPC worker channel cache for the client data plane.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use proto::worker::worker_data_service_client::WorkerDataServiceClient;
use tonic::transport as tonic_net;
use types::{WorkerEndpointInfo, WorkerNetProtocol};

use crate::cache::CacheInvalidationReason;
use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult};
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics};
use crate::runtime::{ErrorClass, ErrorClassifier, MetadataRefreshCause};

const WORKER_ENDPOINT_COOLDOWN_CACHE_LIMIT: usize = 1_024;

#[derive(Debug)]
pub(super) struct GrpcWorkerChannelPool {
    channels: RwLock<HashMap<WorkerChannelKey, tonic_net::Channel>>,
    cooldowns: RwLock<HashMap<WorkerChannelKey, Instant>>,
    enabled: bool,
    max_cached_keys_per_worker: usize,
    endpoint_cooldown: Duration,
    metrics: Arc<dyn ClientMetrics>,
}

impl GrpcWorkerChannelPool {
    pub(super) fn new(enabled: bool, max_cached_keys_per_worker: usize, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::new_with_cooldown_ms(
            enabled,
            max_cached_keys_per_worker,
            crate::config::DEFAULT_WORKER_ENDPOINT_COOLDOWN_MS,
            metrics,
        )
    }

    pub(super) fn new_with_cooldown_ms(
        enabled: bool,
        max_cached_keys_per_worker: usize,
        endpoint_cooldown_ms: u64,
        metrics: Arc<dyn ClientMetrics>,
    ) -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            cooldowns: RwLock::new(HashMap::new()),
            enabled,
            max_cached_keys_per_worker: max_cached_keys_per_worker.max(1),
            endpoint_cooldown: Duration::from_millis(endpoint_cooldown_ms),
            metrics,
        }
    }

    pub(super) fn from_config(config: &ClientConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::new_with_cooldown_ms(
            config.channel_pool.worker_channel_pool_enabled,
            config.channel_pool.worker_channel_pool_max_per_worker,
            config.channel_pool.worker_endpoint_cooldown_ms,
            metrics,
        )
    }

    pub(super) fn is_worker_cooling_down(&self, worker: &WorkerEndpointInfo) -> bool {
        let Ok(key) = Self::channel_key(worker) else {
            return false;
        };
        self.is_key_cooling_down(&key)
    }

    pub(super) fn mark_worker_unavailable(&self, worker: &WorkerEndpointInfo, reason: CacheInvalidationReason) {
        let Ok(key) = Self::channel_key(worker) else {
            return;
        };
        self.invalidate_key(&key, reason);
        if !self.endpoint_cooldown.is_zero() {
            let now = Instant::now();
            let mut cooldowns = self.cooldowns.write();
            prune_expired_cooldowns(&mut cooldowns, now);
            evict_worker_cooldown_if_needed(&mut cooldowns, &key);
            cooldowns.insert(key, now + self.endpoint_cooldown);
        }
    }

    pub(super) fn clear_worker_cooldown(&self, worker: &WorkerEndpointInfo) {
        if let Ok(key) = Self::channel_key(worker) {
            self.cooldowns.write().remove(&key);
        }
    }

    fn is_key_cooling_down(&self, key: &WorkerChannelKey) -> bool {
        if self.endpoint_cooldown.is_zero() {
            return false;
        }
        let now = Instant::now();
        let mut cooldowns = self.cooldowns.write();
        prune_expired_cooldowns(&mut cooldowns, now);
        cooldowns.get(key).is_some_and(|until| *until > now)
    }

    pub(super) fn worker_data_service_client(
        &self,
        worker: &WorkerEndpointInfo,
        operation: &'static str,
    ) -> ClientResult<WorkerDataServiceClient<tonic_net::Channel>> {
        let key = Self::channel_key(worker)?;
        if self.is_key_cooling_down(&key) {
            return Err(ClientError::Worker("worker endpoint is cooling down".to_string()));
        }
        if !self.enabled {
            self.record_pool_metric(ClientMetric::WorkerChannelPoolMiss, operation, "miss");
            return build_lazy_worker_channel(&key.endpoint)
                .map(WorkerDataServiceClient::new)
                .inspect_err(|_err| {
                    self.record_pool_metric(ClientMetric::ChannelBuildError, operation, "error");
                });
        }
        let channel = self.channel_for_key(key, operation)?;
        Ok(WorkerDataServiceClient::new(channel))
    }

    pub(super) fn invalidate_worker_channel(&self, worker: &WorkerEndpointInfo, reason: CacheInvalidationReason) {
        if let Ok(key) = Self::channel_key(worker) {
            self.invalidate_key(&key, reason);
        }
    }

    fn invalidate_key(&self, key: &WorkerChannelKey, reason: CacheInvalidationReason) {
        if self.channels.write().remove(key).is_some() {
            self.record_pool_metric(
                ClientMetric::CachePreciseInvalidation,
                "channel_invalidate",
                reason.label(),
            );
        }
    }

    pub(super) fn invalidate_on_worker_run_mismatch(&self, worker: &WorkerEndpointInfo, error: &ClientError) {
        let Some(reason) = worker_run_mismatch_invalidation_reason(error) else {
            return;
        };
        self.invalidate_worker_channel(worker, reason);
    }

    fn channel_key(worker: &WorkerEndpointInfo) -> ClientResult<WorkerChannelKey> {
        Ok(WorkerChannelKey {
            worker_id: worker.worker_id.as_raw(),
            endpoint: normalize_endpoint(&worker.endpoint)?,
            protocol: worker.worker_net_protocol,
            worker_run_id: worker.worker_run_id,
        })
    }

    fn channel_for_key(&self, key: WorkerChannelKey, operation: &'static str) -> ClientResult<tonic_net::Channel> {
        if let Some(channel) = self.get_cached_channel(&key) {
            self.record_pool_metric(ClientMetric::WorkerChannelPoolHit, operation, "hit");
            return Ok(channel);
        }
        self.record_pool_metric(ClientMetric::WorkerChannelPoolMiss, operation, "miss");

        let channel = build_lazy_worker_channel(&key.endpoint).inspect_err(|_err| {
            self.record_pool_metric(ClientMetric::ChannelBuildError, operation, "error");
        })?;
        Ok(self.insert_or_get_existing(key, channel))
    }

    fn get_cached_channel(&self, key: &WorkerChannelKey) -> Option<tonic_net::Channel> {
        self.channels.read().get(key).cloned()
    }

    fn insert_or_get_existing(&self, key: WorkerChannelKey, channel: tonic_net::Channel) -> tonic_net::Channel {
        let mut channels = self.channels.write();
        if let Some(existing) = channels.get(&key).cloned() {
            return existing;
        }
        evict_worker_channel_if_needed(&mut channels, &key, self.max_cached_keys_per_worker);
        channels.insert(key, channel.clone());
        channel
    }

    fn record_pool_metric(&self, metric: ClientMetric, operation: &'static str, outcome: &'static str) {
        self.metrics.record(ClientMetricEvent::new(
            metric,
            ClientMetricLabels::default()
                .with_cache("channel_pool")
                .with_target_plane("worker")
                .with_operation_name(operation)
                .with_outcome(outcome),
        ));
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct WorkerChannelKey {
    worker_id: u64,
    endpoint: String,
    protocol: WorkerNetProtocol,
    worker_run_id: types::WorkerRunId,
}

fn normalize_endpoint(endpoint: &str) -> ClientResult<String> {
    if endpoint.is_empty() {
        return Err(ClientError::InvalidArgument(
            "worker endpoint must not be empty".to_string(),
        ));
    }
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        Ok(endpoint.to_string())
    } else {
        Ok(format!("http://{endpoint}"))
    }
}

fn build_lazy_worker_channel(endpoint: &str) -> ClientResult<tonic_net::Channel> {
    tonic_net::Endpoint::from_shared(endpoint.to_string())
        .map_err(|err| ClientError::Worker(format!("invalid worker endpoint {endpoint}: {err}")))
        .map(|endpoint| endpoint.connect_lazy())
}

fn evict_worker_channel_if_needed(
    channels: &mut HashMap<WorkerChannelKey, tonic_net::Channel>,
    key: &WorkerChannelKey,
    max_cached_keys_per_worker: usize,
) {
    if channels.contains_key(key) {
        return;
    }
    let count = channels
        .keys()
        .filter(|existing| existing.worker_id == key.worker_id)
        .count();
    if count < max_cached_keys_per_worker {
        return;
    }
    if let Some(evicted) = channels
        .keys()
        .find(|existing| existing.worker_id == key.worker_id)
        .cloned()
    {
        channels.remove(&evicted);
    }
}

fn prune_expired_cooldowns(cooldowns: &mut HashMap<WorkerChannelKey, Instant>, now: Instant) {
    cooldowns.retain(|_, until| *until > now);
}

fn evict_worker_cooldown_if_needed(cooldowns: &mut HashMap<WorkerChannelKey, Instant>, key: &WorkerChannelKey) {
    if cooldowns.contains_key(key) || cooldowns.len() < WORKER_ENDPOINT_COOLDOWN_CACHE_LIMIT {
        return;
    }
    if let Some(evicted) = cooldowns.keys().next().cloned() {
        cooldowns.remove(&evicted);
    }
}

fn worker_run_mismatch_invalidation_reason(err: &ClientError) -> Option<CacheInvalidationReason> {
    match ErrorClassifier.classify_error(err) {
        ErrorClass::RefreshMetadata(MetadataRefreshCause::WorkerRunMismatch) => {
            Some(CacheInvalidationReason::WorkerRun)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use common::error::rpc::{ErrorKind, RefreshHint as RpcRefreshHint, RpcErrorDetail, WorkerErrorKind};
    use proto::convert::rpc_error_to_proto;
    use std::sync::Mutex;
    use types::{ClientId, WorkerEndpointInfo, WorkerId};

    use crate::data::protocol::parse_worker_control_header;
    use crate::metrics::NoopClientMetrics;
    use crate::runtime::{AttemptContext, OperationContext, OperationIdentity, OperationKind};

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

    // connect_lazy touches Hyper's Tokio executor even though acquisition is synchronous.
    #[tokio::test]
    async fn worker_channel_pool_reuses_channel_for_same_worker_endpoint() {
        let metrics = Arc::new(RecordingMetrics::default());
        let pool = GrpcWorkerChannelPool::new(true, 1, metrics.clone());
        let worker = worker_endpoint();

        let _first = pool.worker_data_service_client(&worker, "read").expect("first client");
        let _second = pool.worker_data_service_client(&worker, "read").expect("second client");

        let events = metrics.events();
        assert_metric(&events, ClientMetric::WorkerChannelPoolMiss);
        assert_metric(&events, ClientMetric::WorkerChannelPoolHit);
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
    }

    #[tokio::test]
    async fn concurrent_worker_channel_requests_same_key_reuse_inserted_channel() {
        let task_count = 8;
        let metrics = Arc::new(RecordingMetrics::default());
        let pool = Arc::new(GrpcWorkerChannelPool::new(true, 8, metrics.clone()));
        let worker = worker_endpoint();

        let mut tasks = Vec::with_capacity(task_count);
        for _ in 0..task_count {
            let pool = Arc::clone(&pool);
            let worker = worker.clone();
            tasks.push(tokio::spawn(
                async move { pool.worker_data_service_client(&worker, "read") },
            ));
        }

        for task in tasks {
            let _client = task.await.expect("task").expect("worker client");
        }
        assert_eq!(pool.channels.read().len(), 1);
        let events = metrics.events();
        let miss_count = count_metric(&events, ClientMetric::WorkerChannelPoolMiss);
        assert!(
            (1..=task_count).contains(&miss_count),
            "miss count {miss_count} outside expected race-visible bounds: {events:?}"
        );
        assert_eq!(count_metric(&events, ClientMetric::ChannelBuildError), 0);
        assert_safe_metric_labels(&events);
    }

    // connect_lazy touches Hyper's Tokio executor even though acquisition is synchronous.
    #[tokio::test]
    async fn worker_channel_different_run_does_not_share_channel() {
        let metrics = Arc::new(NoopClientMetrics);
        let pool = GrpcWorkerChannelPool::new(true, 8, metrics);
        let mut first = worker_endpoint();
        first.worker_run_id = "550e8400-e29b-41d4-a716-446655440007"
            .parse()
            .expect("valid first WorkerRunId");
        let mut second = worker_endpoint();
        second.worker_run_id = "550e8400-e29b-41d4-a716-446655440008"
            .parse()
            .expect("valid second WorkerRunId");

        pool.worker_data_service_client(&first, "read").expect("first client");
        pool.worker_data_service_client(&second, "read").expect("second client");
        assert_eq!(pool.channels.read().len(), 2);
    }

    // connect_lazy touches Hyper's Tokio executor even though acquisition is synchronous.
    #[tokio::test]
    async fn worker_run_mismatch_invalidates_target_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let pool = GrpcWorkerChannelPool::new(true, 1, metrics.clone());
        let worker = worker_endpoint();
        let attempt = data_attempt_context();

        let _worker_client = pool.worker_data_service_client(&worker, "read").expect("worker client");
        assert_eq!(pool.channels.read().len(), 1);

        let err = parse_worker_control_header(
            &attempt,
            Some(&data_header_with_error(
                &attempt,
                RpcErrorDetail::refresh_metadata(
                    ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                    RpcRefreshHint::default(),
                    "worker run mismatch",
                ),
            )),
        )
        .expect_err("worker run mismatch must fail");

        pool.invalidate_on_worker_run_mismatch(&worker, &err);

        assert_eq!(pool.channels.read().len(), 0);
        assert_metric(&metrics.events(), ClientMetric::CachePreciseInvalidation);
    }

    #[test]
    fn failed_worker_channel_creation_does_not_insert() {
        let metrics = Arc::new(RecordingMetrics::default());
        let pool = Arc::new(GrpcWorkerChannelPool::new(true, 8, metrics.clone()));
        let mut worker = worker_endpoint();
        worker.endpoint = "http://[invalid".to_string();

        let mut tasks = Vec::with_capacity(4);
        for _ in 0..4 {
            let pool = Arc::clone(&pool);
            let worker = worker.clone();
            tasks.push(std::thread::spawn(move || {
                pool.worker_data_service_client(&worker, "read")
            }));
        }

        for task in tasks {
            let err = task.join().expect("task").expect_err("invalid endpoint");
            assert!(matches!(err, ClientError::Worker(msg) if msg.contains("invalid worker endpoint")));
        }
        assert!(pool.channels.read().is_empty());
        let events = metrics.events();
        assert_metric_with_target_plane(&events, ClientMetric::ChannelBuildError, "worker");
        assert_metric_labels_do_not_contain(&events, "http://[invalid");
    }

    // connect_lazy touches Hyper's Tokio executor even though acquisition is synchronous.
    #[tokio::test]
    async fn disabled_worker_channel_pool_does_not_reuse_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let pool = GrpcWorkerChannelPool::new(false, 1, metrics.clone());
        let worker = worker_endpoint();

        let _first = pool.worker_data_service_client(&worker, "read").expect("first client");
        let _second = pool.worker_data_service_client(&worker, "read").expect("second client");

        let events = metrics.events();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.metric == ClientMetric::WorkerChannelPoolMiss)
                .count(),
            2
        );
        assert!(events
            .iter()
            .all(|event| event.metric != ClientMetric::WorkerChannelPoolHit));
    }

    #[test]
    fn worker_channel_build_error_is_reported_with_safe_labels() {
        let metrics = Arc::new(RecordingMetrics::default());
        let pool = GrpcWorkerChannelPool::new(true, 1, metrics.clone());
        let mut worker = worker_endpoint();
        worker.endpoint = "http://[invalid".to_string();

        let err = pool
            .worker_data_service_client(&worker, "read")
            .expect_err("invalid endpoint fails");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("invalid worker endpoint")));
        let events = metrics.events();
        assert_metric_with_target_plane(&events, ClientMetric::ChannelBuildError, "worker");
        assert_metric_labels_do_not_contain(&events, "http://[invalid");
    }

    #[test]
    fn worker_endpoint_cooldowns_are_bounded() {
        let metrics = Arc::new(NoopClientMetrics);
        let pool = GrpcWorkerChannelPool::new_with_cooldown_ms(true, 1, 60_000, metrics);

        for index in 0..1_100 {
            let mut worker = worker_endpoint();
            worker.worker_id = WorkerId::new(index + 1);
            worker.endpoint = format!("127.0.0.1:{}", 20_000 + index);

            pool.mark_worker_unavailable(&worker, CacheInvalidationReason::Unavailable);
        }

        assert!(
            pool.cooldowns.read().len() <= WORKER_ENDPOINT_COOLDOWN_CACHE_LIMIT,
            "cooldown map must stay bounded, got {} entries",
            pool.cooldowns.read().len()
        );
    }

    fn worker_endpoint() -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(1),
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: WorkerNetProtocol::Grpc,
            worker_run_id: "550e8400-e29b-41d4-a716-446655440000"
                .parse()
                .expect("valid test WorkerRunId"),
        }
    }

    fn data_attempt_context() -> AttemptContext {
        let operation = OperationContext::new(
            ClientId::new(7),
            OperationKind::WorkerReadData,
            "OpenReadStream",
            OperationIdentity::path("/alpha"),
        )
        .expect("operation context");
        AttemptContext::for_data(&operation, 0)
    }

    fn data_header_with_error(
        attempt: &AttemptContext,
        rpc_error: RpcErrorDetail,
    ) -> proto::worker::DataResponseHeaderProto {
        proto::worker::DataResponseHeaderProto {
            client: Some(attempt.client_info()),
            error: Some(rpc_error_to_proto(&rpc_error)),
        }
    }

    fn assert_metric(events: &[ClientMetricEvent], metric: ClientMetric) {
        assert!(
            events.iter().any(|event| event.metric == metric),
            "missing metric {metric:?}: {events:?}"
        );
    }

    fn assert_metric_with_target_plane(events: &[ClientMetricEvent], metric: ClientMetric, target_plane: &'static str) {
        assert!(
            events
                .iter()
                .any(|event| event.metric == metric && event.labels.target_plane == Some(target_plane)),
            "missing metric {metric:?} with target_plane={target_plane}: {events:?}"
        );
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
        let stale_metric = ["ChannelPool", "ConnectError"].concat();
        assert!(events
            .iter()
            .all(|event| !format!("{:?}", event.metric).contains(&stale_metric)));
    }

    fn assert_safe_metric_labels(events: &[ClientMetricEvent]) {
        assert!(
            events.iter().all(|event| event.labels.has_only_safe_values()),
            "unsafe metric labels: {events:?}"
        );
    }

    fn assert_metric_labels_do_not_contain(events: &[ClientMetricEvent], value: &str) {
        assert!(
            events
                .iter()
                .all(|event| !metric_label_values(&event.labels).any(|label| label.contains(value))),
            "metric labels unexpectedly contain {value:?}: {events:?}"
        );
    }

    fn metric_label_values(labels: &ClientMetricLabels) -> impl Iterator<Item = &str> {
        [
            labels.operation_kind,
            labels.operation_name.as_deref(),
            labels.error_class,
            labels.metadata_refresh_cause,
            labels.target_plane,
            labels.cache,
            labels.reason,
            labels.outcome,
        ]
        .into_iter()
        .flatten()
    }

    fn count_metric(events: &[ClientMetricEvent], metric: ClientMetric) -> usize {
        events.iter().filter(|event| event.metric == metric).count()
    }
}
