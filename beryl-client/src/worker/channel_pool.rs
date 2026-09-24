// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! gRPC channel ownership and endpoint failure tracking for Worker transport.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use beryl_proto::worker::worker_data_service_client::WorkerDataServiceClient;
use beryl_types::WorkerEndpointInfo;
use parking_lot::RwLock;
use tonic::transport as tonic_net;

use crate::config::{normalize_endpoint, ClientConfig};
use crate::error::{ClientError, ClientResult};
use crate::metrics::{self, ClientMetric, ClientMetricLabels};
use beryl_common::error::rpc::{ErrorKind, RecoveryAction, WorkerErrorKind};

/// Low-cardinality cache invalidation reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CacheInvalidationReason {
    /// Worker process-run mismatch.
    WorkerRun,
    /// Worker endpoint unavailable.
    Unavailable,
}

impl CacheInvalidationReason {
    /// Low-cardinality metric label.
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::WorkerRun => "worker_run",
            Self::Unavailable => "unavailable",
        }
    }
}

const WORKER_ENDPOINT_COOLDOWN_CACHE_LIMIT: usize = 1_024;

/// Owns bounded Worker channels and transient endpoint cooldown state.
#[derive(Debug)]
pub(super) struct GrpcWorkerChannelPool {
    channels: RwLock<HashMap<WorkerChannelKey, tonic_net::Channel>>,
    cooldowns: RwLock<HashMap<WorkerChannelKey, Instant>>,
    enabled: bool,
    max_cached_keys_per_worker: usize,
    endpoint_cooldown: Duration,
}

impl GrpcWorkerChannelPool {
    /// Creates the bounded Worker pool from sealed client configuration.
    pub(super) fn from_config(config: &ClientConfig) -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            cooldowns: RwLock::new(HashMap::new()),
            enabled: config.worker_connection_reuse(),
            max_cached_keys_per_worker: config.worker_connection_limit(),
            endpoint_cooldown: config.worker_endpoint_cooldown(),
        }
    }

    /// Returns whether the exact Metadata-authorized Worker identity is cooling down.
    pub(super) fn is_worker_cooling_down(&self, worker: &WorkerEndpointInfo) -> bool {
        let key = Self::channel_key(worker);
        self.is_key_cooling_down(&key)
    }

    /// Invalidates one exact channel and starts its bounded failure cooldown.
    pub(super) fn mark_worker_unavailable(&self, worker: &WorkerEndpointInfo, reason: CacheInvalidationReason) {
        let key = Self::channel_key(worker);
        self.invalidate_key(&key, reason);
        let now = Instant::now();
        let Some(cooldown_until) = now.checked_add(self.endpoint_cooldown) else {
            return;
        };
        let mut cooldowns = self.cooldowns.write();
        prune_expired_cooldowns(&mut cooldowns, now);
        evict_worker_cooldown_if_needed(&mut cooldowns, &key);
        cooldowns.insert(key, cooldown_until);
    }

    /// Clears cooldown state after a validated success from the same Worker identity.
    pub(super) fn clear_worker_cooldown(&self, worker: &WorkerEndpointInfo) {
        self.cooldowns.write().remove(&Self::channel_key(worker));
    }

    fn is_key_cooling_down(&self, key: &WorkerChannelKey) -> bool {
        let now = Instant::now();
        let mut cooldowns = self.cooldowns.write();
        prune_expired_cooldowns(&mut cooldowns, now);
        cooldowns.contains_key(key)
    }

    /// Returns a bounded lazy Worker client unless the exact endpoint is cooling down.
    pub(super) fn worker_data_service_client(
        &self,
        worker: &WorkerEndpointInfo,
        operation: &'static str,
    ) -> ClientResult<WorkerDataServiceClient<tonic_net::Channel>> {
        let key = Self::channel_key(worker);
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

    /// Invalidates the cached channel for one exact Worker identity.
    pub(super) fn invalidate_worker_channel(&self, worker: &WorkerEndpointInfo, reason: CacheInvalidationReason) {
        self.invalidate_key(&Self::channel_key(worker), reason);
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

    fn channel_key(worker: &WorkerEndpointInfo) -> WorkerChannelKey {
        WorkerChannelKey {
            worker_id: worker.worker_id.as_raw(),
            endpoint: normalize_endpoint(&worker.endpoint),
            worker_run_id: worker.worker_run_id,
        }
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
        metrics::record(
            metric,
            ClientMetricLabels::default()
                .with_cache("channel_pool")
                .with_operation(operation, "worker")
                .with_outcome(outcome),
        );
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
    worker_run_id: beryl_types::WorkerRunId,
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
    let count = channels
        .keys()
        .filter(|existing| existing.worker_id == key.worker_id)
        .count();
    if count < max_cached_keys_per_worker {
        return;
    }
    let evicted = channels
        .keys()
        .find(|existing| existing.worker_id == key.worker_id)
        .expect("worker channel count reached its positive limit")
        .clone();
    channels.remove(&evicted);
}

fn prune_expired_cooldowns(cooldowns: &mut HashMap<WorkerChannelKey, Instant>, now: Instant) {
    cooldowns.retain(|_, until| *until > now);
}

fn evict_worker_cooldown_if_needed(cooldowns: &mut HashMap<WorkerChannelKey, Instant>, key: &WorkerChannelKey) {
    if cooldowns.contains_key(key) || cooldowns.len() < WORKER_ENDPOINT_COOLDOWN_CACHE_LIMIT {
        return;
    }
    let evicted = cooldowns.keys().next().expect("full cooldown cache").clone();
    cooldowns.remove(&evicted);
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

    use crate::runtime::{Operation, OperationContext, OperationDeadline};
    use crate::worker::protocol::parse_worker_control_header;

    fn test_pool(enabled: bool, max_cached_keys_per_worker: usize) -> GrpcWorkerChannelPool {
        GrpcWorkerChannelPool::from_config(
            &ClientConfig::builder()
                .worker_connection_reuse(enabled)
                .worker_connection_limit(max_cached_keys_per_worker)
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn worker_channel_requests_cache_one_key() {
        let pool = test_pool(true, 8);
        let worker = worker_endpoint();
        for _ in 0..2 {
            pool.worker_data_service_client(&worker, "read").expect("worker client");
        }
        assert_eq!(pool.channels.read().len(), 1);
    }

    // connect_lazy touches Hyper's Tokio executor even though acquisition is synchronous.
    #[tokio::test]
    async fn worker_run_mismatch_invalidates_target_channel() {
        let pool = test_pool(true, 2);
        let worker = worker_endpoint();
        let mut new_run = worker.clone();
        new_run.worker_run_id = "550e8400-e29b-41d4-a716-446655440001".parse().unwrap();
        let attempt = data_attempt_context();

        let _worker_client = pool.worker_data_service_client(&worker, "read").expect("worker client");
        let _new_client = pool
            .worker_data_service_client(&new_run, "read")
            .expect("new worker client");
        assert_eq!(pool.channels.read().len(), 2);

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

        assert_eq!(pool.channels.read().len(), 1);
        assert!(pool
            .channels
            .read()
            .contains_key(&GrpcWorkerChannelPool::channel_key(&new_run)));

        pool.mark_worker_unavailable(&worker, CacheInvalidationReason::WorkerRun);
        assert!(pool.is_worker_cooling_down(&worker));
        assert!(!pool.is_worker_cooling_down(&new_run));
    }

    fn worker_endpoint() -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(1),
            endpoint: "127.0.0.1:19101".to_string(),
            worker_run_id: "550e8400-e29b-41d4-a716-446655440000"
                .parse()
                .expect("valid test WorkerRunId"),
        }
    }

    fn data_attempt_context() -> OperationContext {
        OperationContext::new_named(
            ClientId::new(7),
            "test-client",
            Operation::Read,
            OperationDeadline::new(1_000),
        )
    }

    fn data_header_with_error(
        attempt: &OperationContext,
        rpc_error: RpcErrorDetail,
    ) -> beryl_proto::worker::DataResponseHeaderProto {
        beryl_proto::worker::DataResponseHeaderProto {
            client: Some(attempt.client_info()),
            error: Some(rpc_error_to_proto(&rpc_error)),
        }
    }
}
