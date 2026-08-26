// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! gRPC channel ownership and endpoint failure tracking for Worker transport.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beryl_proto::worker::worker_data_service_client::WorkerDataServiceClient;
use beryl_types::{WorkerEndpointInfo, WorkerNetProtocol};
use parking_lot::RwLock;
use tonic::transport as tonic_net;

use crate::cache::CacheInvalidationReason;
use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult};
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics};
use beryl_common::error::rpc::{ErrorKind, RecoveryAction, WorkerErrorKind};

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
            config.connections.worker_enabled,
            config.connections.worker_max_per_worker,
            config.connections.worker_failure_cooldown_ms,
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
            return Err(ClientError::worker("worker endpoint is cooling down".to_string()));
        }
        if !self.enabled {
            self.record_pool_metric(ClientMetric::WorkerChannelPoolMiss, operation, "miss");
            return build_lazy_worker_channel(&key.endpoint)
                .map(configure_worker_data_client)
                .inspect_err(|_err| {
                    self.record_pool_metric(ClientMetric::ChannelBuildError, operation, "error");
                });
        }
        let channel = self.channel_for_key(key, operation)?;
        Ok(configure_worker_data_client(channel))
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

/// Applies the shared Worker data-message bound to every pooled or lazy client.
fn configure_worker_data_client(channel: tonic_net::Channel) -> WorkerDataServiceClient<tonic_net::Channel> {
    WorkerDataServiceClient::new(channel)
        .max_decoding_message_size(beryl_proto::MAX_WORKER_DATA_MESSAGE_SIZE)
        .max_encoding_message_size(beryl_proto::MAX_WORKER_DATA_MESSAGE_SIZE)
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct WorkerChannelKey {
    worker_id: u64,
    endpoint: String,
    protocol: WorkerNetProtocol,
    worker_run_id: beryl_types::WorkerRunId,
}

fn normalize_endpoint(endpoint: &str) -> ClientResult<String> {
    if endpoint.is_empty() {
        return Err(ClientError::invalid_argument(
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
        .map_err(|err| ClientError::worker(format!("invalid worker endpoint {endpoint}: {err}")))
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
    if err.remote_error().is_some_and(|error| {
        matches!(error.recovery, RecoveryAction::RefreshMetadata { .. })
            && error.kind == ErrorKind::Worker(WorkerErrorKind::RunMismatch)
    }) {
        Some(CacheInvalidationReason::WorkerRun)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use beryl_common::error::rpc::{ErrorKind, RefreshHint as RpcRefreshHint, RpcErrorDetail, WorkerErrorKind};
    use beryl_proto::convert::rpc_error_to_proto;
    use beryl_types::{ClientId, WorkerEndpointInfo, WorkerId};
    use std::sync::Mutex;

    use crate::runtime::{AttemptContext, Operation, OperationContext, OperationDeadline};
    use crate::worker::protocol::parse_worker_control_header;

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

    fn test_pool(
        enabled: bool,
        max_cached_keys_per_worker: usize,
        metrics: Arc<dyn ClientMetrics>,
    ) -> GrpcWorkerChannelPool {
        GrpcWorkerChannelPool::new_with_cooldown_ms(
            enabled,
            max_cached_keys_per_worker,
            crate::config::DEFAULT_WORKER_ENDPOINT_COOLDOWN_MS,
            metrics,
        )
    }

    #[tokio::test]
    async fn concurrent_worker_channel_requests_same_key_reuse_inserted_channel() {
        let task_count = 8;
        let metrics = Arc::new(RecordingMetrics::default());
        let pool = Arc::new(test_pool(true, 8, metrics.clone()));
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
    async fn worker_run_mismatch_invalidates_target_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let pool = test_pool(true, 1, metrics.clone());
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
        let operation = OperationContext::new_named(
            ClientId::new(7),
            "test-client",
            Operation::Read,
            Some("/alpha".to_string()),
            OperationDeadline::new(1_000),
        )
        .expect("operation context");
        AttemptContext::for_data(&operation, 0)
    }

    fn data_header_with_error(
        attempt: &AttemptContext,
        rpc_error: RpcErrorDetail,
    ) -> beryl_proto::worker::DataResponseHeaderProto {
        beryl_proto::worker::DataResponseHeaderProto {
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

    fn assert_safe_metric_labels(events: &[ClientMetricEvent]) {
        assert!(
            events.iter().all(|event| event.labels.has_only_safe_values()),
            "unsafe metric labels: {events:?}"
        );
    }

    fn count_metric(events: &[ClientMetricEvent], metric: ClientMetric) -> usize {
        events.iter().filter(|event| event.metric == metric).count()
    }
}
