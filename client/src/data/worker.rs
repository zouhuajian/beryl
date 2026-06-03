// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker data-plane boundary owned by the client crate.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream;
use parking_lot::RwLock;
use proto::worker::worker_data_service_client::WorkerDataServiceClient;
use tonic::transport as tonic_net;
use types::chunk::ByteRange;
use types::{GroupName, WorkerEndpointInfo, WorkerNetProtocol, WriteTarget};

use super::{WorkerBlockSyncResult, WorkerCommitResult, WorkerDataClient, WorkerWriteBlock, WorkerWriteTarget};
use crate::cache::{CacheInvalidationReason, WorkerEndpointCache};
use crate::canonical::{invalid_header_action, validate_data_header_or_action};
use crate::config::ClientConfig;
use crate::error::{side_effect_response_body_mismatch, ClientError, ClientResult};
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics, NoopClientMetrics};
use crate::planner::read_planner::PlannedReadSegment;
use crate::runtime::{AttemptContext, ErrorClass, ErrorClassifier, RefreshReason};

#[derive(Debug)]
struct TonicWorkerDataClient {
    channels: RwLock<std::collections::HashMap<WorkerChannelKey, tonic_net::Channel>>,
    endpoint_cache: WorkerEndpointCache,
    channel_pool_enabled: bool,
    max_channels_per_worker: usize,
    metrics: Arc<dyn ClientMetrics>,
}

impl TonicWorkerDataClient {
    fn new() -> Self {
        Self::with_parts(WorkerEndpointCache::disabled(), true, 1, Arc::new(NoopClientMetrics))
    }

    fn from_config(config: &ClientConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::with_parts(
            WorkerEndpointCache::from_config(&config.cache, Arc::clone(&metrics)),
            config.channel_pool.worker_channel_pool_enabled,
            config.channel_pool.worker_channel_pool_max_per_worker,
            metrics,
        )
    }

    fn with_parts(
        endpoint_cache: WorkerEndpointCache,
        channel_pool_enabled: bool,
        max_channels_per_worker: usize,
        metrics: Arc<dyn ClientMetrics>,
    ) -> Self {
        Self {
            channels: RwLock::new(std::collections::HashMap::new()),
            endpoint_cache,
            channel_pool_enabled,
            max_channels_per_worker: max_channels_per_worker.max(1),
            metrics,
        }
    }

    fn endpoint_cache(&self) -> WorkerEndpointCache {
        self.endpoint_cache.clone()
    }

    async fn client(
        &self,
        candidate: &WorkerEndpointInfo,
        operation: &'static str,
    ) -> ClientResult<WorkerDataServiceClient<tonic_net::Channel>> {
        let candidate = self.endpoint_cache.get_or_resolve_authoritative(candidate).await?;
        let endpoint = normalize_endpoint(&candidate.endpoint)?;
        let key = WorkerChannelKey {
            worker_id: candidate.worker_id.as_raw(),
            endpoint,
            protocol: candidate.worker_net_protocol,
            worker_run_id: candidate.worker_run_id,
        };
        if !self.channel_pool_enabled {
            self.record_pool_metric(ClientMetric::WorkerChannelPoolMiss, operation, "miss");
            return lazy_channel(&key.endpoint)
                .map(WorkerDataServiceClient::new)
                .inspect_err(|_err| {
                    self.record_pool_metric(ClientMetric::ChannelPoolConnectError, operation, "error");
                });
        }
        let channel = {
            let channels = self.channels.read();
            channels.get(&key).cloned()
        };
        let channel = match channel {
            Some(channel) => {
                self.record_pool_metric(ClientMetric::WorkerChannelPoolHit, operation, "hit");
                channel
            }
            None => {
                self.record_pool_metric(ClientMetric::WorkerChannelPoolMiss, operation, "miss");
                self.create_worker_channel(key, operation).await?
            }
        };
        Ok(WorkerDataServiceClient::new(channel))
    }

    async fn create_worker_channel(
        &self,
        key: WorkerChannelKey,
        operation: &'static str,
    ) -> ClientResult<tonic_net::Channel> {
        if let Some(channel) = self.channels.read().get(&key).cloned() {
            self.record_pool_metric(ClientMetric::WorkerChannelPoolHit, operation, "hit");
            return Ok(channel);
        }
        let channel = lazy_channel(&key.endpoint).inspect_err(|_err| {
            self.record_pool_metric(ClientMetric::ChannelPoolConnectError, operation, "error");
        })?;
        Ok(self.insert_worker_channel(key, channel))
    }

    fn insert_worker_channel(&self, key: WorkerChannelKey, channel: tonic_net::Channel) -> tonic_net::Channel {
        let mut channels = self.channels.write();
        if let Some(existing) = channels.get(&key).cloned() {
            return existing;
        }
        evict_worker_channel_if_needed(&mut channels, &key, self.max_channels_per_worker);
        channels.insert(key, channel.clone());
        channel
    }

    fn invalidate_endpoint(&self, candidate: &WorkerEndpointInfo, reason: CacheInvalidationReason) {
        self.endpoint_cache.invalidate_candidate(candidate, reason);
    }

    fn record_endpoint_failure(&self, candidate: &WorkerEndpointInfo, reason: CacheInvalidationReason) {
        self.endpoint_cache.record_candidate_failure(candidate, reason);
    }

    fn invalidate_channel(&self, candidate: &WorkerEndpointInfo, reason: CacheInvalidationReason) {
        if let Ok(endpoint) = normalize_endpoint(&candidate.endpoint) {
            let key = WorkerChannelKey {
                worker_id: candidate.worker_id.as_raw(),
                endpoint,
                protocol: candidate.worker_net_protocol,
                worker_run_id: candidate.worker_run_id,
            };
            if self.channels.write().remove(&key).is_some() {
                self.record_pool_metric(
                    ClientMetric::CachePreciseInvalidation,
                    "channel_invalidate",
                    reason.label(),
                );
            }
        }
    }

    fn invalidate_worker_identity_mismatch(&self, candidate: &WorkerEndpointInfo, err: &ClientError) {
        let Some(reason) = worker_identity_mismatch_invalidation_reason(err) else {
            return;
        };
        self.invalidate_endpoint(candidate, reason);
        self.invalidate_channel(candidate, reason);
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

fn evict_worker_channel_if_needed(
    channels: &mut std::collections::HashMap<WorkerChannelKey, tonic_net::Channel>,
    key: &WorkerChannelKey,
    max_per_worker: usize,
) {
    if channels.contains_key(key) {
        return;
    }
    let count = channels
        .keys()
        .filter(|existing| existing.worker_id == key.worker_id)
        .count();
    if count < max_per_worker {
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

#[async_trait]
impl WorkerDataClient for TonicWorkerDataClient {
    async fn read_segment(
        &self,
        ctx: AttemptContext,
        group_name: GroupName,
        segment: &PlannedReadSegment,
    ) -> ClientResult<Bytes> {
        let mut last_transport_error = None;
        for candidate in &segment.workers {
            ensure_supported_worker_protocol(candidate.worker_net_protocol)?;
            let mut client = match self.client(candidate, "read").await {
                Ok(client) => client,
                Err(err) if is_endpoint_health_error(&err) => {
                    last_transport_error = Some(err);
                    continue;
                }
                Err(err) => return Err(err),
            };
            let request = build_open_read_stream_request(&ctx, &group_name, segment, candidate)?;
            let open_response = match client.open_read_stream(tonic_request(&ctx, request)).await {
                Ok(response) => response.into_inner(),
                Err(status) if is_retryable_worker_status(&status) => {
                    self.record_endpoint_failure(candidate, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(ClientError::from(status));
                    continue;
                }
                Err(status) => return Err(ClientError::from(status)),
            };
            if let Err(err) = parse_worker_control_header(&ctx, open_response.header.as_ref()) {
                self.invalidate_worker_identity_mismatch(candidate, &err);
                return Err(err);
            }
            let stream_id = open_response
                .stream_id
                .ok_or_else(|| invalid_worker_header("worker OK response missing stream_id"))?;
            if stream_id.high == 0 && stream_id.low == 0 {
                return Err(invalid_worker_header(
                    "worker OK response invalid stream_id: zero value",
                ));
            }
            let max_bytes = open_response.frame_size.max(1);
            let stream_request = proto::worker::ReadStreamRequestProto {
                stream_id: Some(stream_id),
                max_bytes,
            };
            let mut stream = client
                .read_stream(tonic::Request::new(stream_request))
                .await
                .map_err(ClientError::from)?
                .into_inner();
            return read_stream_to_bytes(&mut stream, segment).await;
        }
        Err(last_transport_error
            .unwrap_or_else(|| ClientError::Worker("worker read has no reachable worker candidates".to_string())))
    }

    async fn open_write(&self, ctx: AttemptContext, target: WorkerWriteTarget) -> ClientResult<WorkerWriteBlock> {
        validate_worker_write_target(&target)?;
        let mut last_transport_error = None;
        for candidate in &target.target.worker_endpoints {
            ensure_supported_worker_protocol(candidate.worker_net_protocol)?;
            let mut client = match self.client(candidate, "write").await {
                Ok(client) => client,
                Err(err) if is_endpoint_health_error(&err) => {
                    last_transport_error = Some(err);
                    continue;
                }
                Err(err) => return Err(err),
            };
            let request = build_open_write_stream_request(&ctx, &target, candidate)?;
            let response = match client.open_write_stream(tonic_request(&ctx, request)).await {
                Ok(response) => response.into_inner(),
                Err(status) if is_retryable_worker_status(&status) => {
                    self.record_endpoint_failure(candidate, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(ClientError::UnknownOutcome(format!(
                        "worker OpenWriteStream outcome is unknown after transport status {}: {}",
                        status.code(),
                        status.message()
                    )));
                    continue;
                }
                Err(status) => return Err(ClientError::from(status)),
            };
            return worker_write_block_from_open_response(&ctx, &target, candidate, response)
                .inspect_err(|err| self.invalidate_worker_identity_mismatch(candidate, err));
        }
        Err(last_transport_error
            .unwrap_or_else(|| ClientError::Worker("worker write has no reachable worker candidates".to_string())))
    }

    async fn write_stream(
        &self,
        block: &WorkerWriteBlock,
        data: Bytes,
    ) -> ClientResult<proto::worker::WriteStreamResponseProto> {
        if data.is_empty() {
            return Ok(proto::worker::WriteStreamResponseProto {
                accepted: true,
                last_acked_seq: block.next_seq.saturating_sub(1),
                written_through: 0,
            });
        }
        let endpoint = worker_endpoint_from_block(block);
        let mut client = self.client(&endpoint, "write").await?;
        let expected_written_through = data.len() as u64;
        let frames = build_write_stream_frames(block, data)?;
        let expected_last_seq = frames
            .last()
            .map(|frame| frame.seq)
            .unwrap_or_else(|| block.next_seq.saturating_sub(1));
        let response = client
            .write_stream(tonic::Request::new(stream::iter(frames)))
            .await
            .map_err(|status| {
                ClientError::UnknownOutcome(format!(
                    "worker WriteStream outcome is unknown after transport status {}: {}",
                    status.code(),
                    status.message()
                ))
            })?
            .into_inner();
        if !response.accepted {
            return Err(ClientError::UnknownOutcome(
                "worker WriteStream did not accept the submitted frames".to_string(),
            ));
        }
        validate_write_stream_response(response, expected_last_seq, expected_written_through)
    }

    async fn commit_write(
        &self,
        ctx: AttemptContext,
        block: &WorkerWriteBlock,
        effective_len: u64,
        commit_seq: u64,
        require_sync: bool,
    ) -> ClientResult<WorkerCommitResult> {
        let endpoint = worker_endpoint_from_block(block);
        let mut client = self.client(&endpoint, "write").await?;
        let request = build_commit_write_request(&ctx, block, effective_len, commit_seq, require_sync)?;
        let response = client
            .commit_write(tonic_request(&ctx, request))
            .await
            .map_err(|status| {
                ClientError::UnknownOutcome(format!(
                    "worker CommitWrite outcome is unknown after transport status {}: {}",
                    status.code(),
                    status.message()
                ))
            })?
            .into_inner();
        worker_commit_result_from_response(&ctx, block, effective_len, response)
            .inspect_err(|err| self.invalidate_worker_identity_mismatch(&endpoint, err))
    }

    async fn sync_committed_block(
        &self,
        ctx: AttemptContext,
        block: &WorkerWriteBlock,
        expected_len: u64,
    ) -> ClientResult<WorkerBlockSyncResult> {
        let endpoint = worker_endpoint_from_block(block);
        let mut client = self.client(&endpoint, "write").await?;
        let request = build_sync_committed_block_request(&ctx, block, expected_len)?;
        let response = client
            .sync_committed_block(tonic_request(&ctx, request))
            .await
            .map_err(|status| {
                ClientError::UnknownOutcome(format!(
                    "worker SyncCommittedBlock outcome is unknown after transport status {}: {}",
                    status.code(),
                    status.message()
                ))
            })?
            .into_inner();
        worker_block_sync_result_from_response(&ctx, block, expected_len, response)
            .inspect_err(|err| self.invalidate_worker_identity_mismatch(&endpoint, err))
    }

    async fn abort_write(&self, ctx: AttemptContext, block: &WorkerWriteBlock) -> ClientResult<()> {
        let endpoint = worker_endpoint_from_block(block);
        let mut client = self.client(&endpoint, "write").await?;
        let request = build_abort_write_request(&ctx, block)?;
        let response = client
            .abort_write(tonic_request(&ctx, request))
            .await
            .map_err(|status| {
                ClientError::UnknownOutcome(format!(
                    "worker AbortWrite outcome is unknown after transport status {}: {}",
                    status.code(),
                    status.message()
                ))
            })?
            .into_inner();
        validate_abort_write_response(&ctx, response)
    }
}

/// Internal data-plane boundary holder used by the public facade.
#[derive(Clone)]
pub(crate) struct DataPlaneBoundary {
    client: Arc<dyn WorkerDataClient>,
    worker_endpoint_cache: Option<WorkerEndpointCache>,
    grpc_protocol: WorkerNetProtocol,
}

impl DataPlaneBoundary {
    /// Create a data-plane boundary.
    pub(crate) fn new() -> Self {
        Self::with_tonic_client(Arc::new(TonicWorkerDataClient::new()))
    }

    /// Create a data-plane boundary from client config.
    pub(crate) fn from_config(config: &ClientConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::with_tonic_client(Arc::new(TonicWorkerDataClient::from_config(config, metrics)))
    }

    fn with_tonic_client(client: Arc<TonicWorkerDataClient>) -> Self {
        let worker_endpoint_cache = client.endpoint_cache();
        let client: Arc<dyn WorkerDataClient> = client;
        let mut boundary = Self::with_client(client);
        boundary.worker_endpoint_cache = Some(worker_endpoint_cache);
        boundary
    }

    /// Create a data-plane boundary around an already selected worker client implementation.
    pub(crate) fn with_client(client: Arc<dyn WorkerDataClient>) -> Self {
        let grpc_protocol = WorkerNetProtocol::Grpc;
        ensure_supported_worker_protocol(grpc_protocol).expect("gRPC must be supported");
        Self {
            client,
            worker_endpoint_cache: None,
            grpc_protocol,
        }
    }

    /// Return the worker endpoint cache when the tonic client owns one.
    pub(crate) fn worker_endpoint_cache(&self) -> Option<WorkerEndpointCache> {
        self.worker_endpoint_cache.clone()
    }

    pub(crate) async fn read_all(
        &self,
        ctx: AttemptContext,
        group_name: GroupName,
        segments: &[PlannedReadSegment],
    ) -> ClientResult<Bytes> {
        let total_len = segments.iter().map(|segment| segment.len as usize).sum();
        let mut output = BytesMut::with_capacity(total_len);
        for segment in segments {
            if segment.block_stamp == 0 {
                return Err(ClientError::InvalidLayout(
                    "planned read segment has zero block_stamp".to_string(),
                ));
            }
            let expected_end = segment
                .file_offset
                .checked_add(segment.len as u64)
                .ok_or_else(|| ClientError::InvalidLayout("planned read segment end overflow".to_string()))?;
            if expected_end != segment.end_file_offset {
                return Err(ClientError::InvalidLayout(
                    "planned read segment coverage is inconsistent".to_string(),
                ));
            }
            let bytes = self
                .client
                .read_segment(ctx.clone(), group_name.clone(), segment)
                .await?;
            if bytes.len() != segment.len as usize {
                return Err(ClientError::Worker(format!(
                    "worker read returned {} bytes for {} byte segment",
                    bytes.len(),
                    segment.len
                )));
            }
            output.extend_from_slice(&bytes);
        }
        Ok(output.freeze())
    }

    pub(crate) async fn open_write(
        &self,
        ctx: AttemptContext,
        group_name: GroupName,
        target: WriteTarget,
    ) -> ClientResult<WorkerWriteBlock> {
        let worker_target = WorkerWriteTarget { group_name, target };
        self.client.open_write(ctx, worker_target).await
    }

    pub(crate) async fn write_all(
        &self,
        block: &WorkerWriteBlock,
        data: Bytes,
    ) -> ClientResult<proto::worker::WriteStreamResponseProto> {
        self.client.write_stream(block, data).await
    }

    pub(crate) async fn commit_write(
        &self,
        ctx: AttemptContext,
        block: &WorkerWriteBlock,
        effective_len: u64,
        commit_seq: u64,
        require_sync: bool,
    ) -> ClientResult<WorkerCommitResult> {
        self.client
            .commit_write(ctx, block, effective_len, commit_seq, require_sync)
            .await
    }

    pub(crate) async fn sync_committed_block(
        &self,
        ctx: AttemptContext,
        block: &WorkerWriteBlock,
        expected_len: u64,
    ) -> ClientResult<WorkerBlockSyncResult> {
        self.client.sync_committed_block(ctx, block, expected_len).await
    }

    pub(crate) async fn abort_write(&self, ctx: AttemptContext, block: &WorkerWriteBlock) -> ClientResult<()> {
        self.client.abort_write(ctx, block).await
    }
}

impl fmt::Debug for DataPlaneBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPlaneBoundary")
            .field("grpc_protocol", &self.grpc_protocol)
            .finish_non_exhaustive()
    }
}

impl Default for DataPlaneBoundary {
    fn default() -> Self {
        Self::new()
    }
}

fn ensure_supported_worker_protocol(protocol: WorkerNetProtocol) -> ClientResult<()> {
    match protocol {
        WorkerNetProtocol::Grpc => Ok(()),
        WorkerNetProtocol::Quic | WorkerNetProtocol::Rdma => Err(ClientError::Unsupported(format!(
            "unsupported worker net protocol {protocol:?}"
        ))),
    }
}

fn build_open_read_stream_request(
    ctx: &AttemptContext,
    group_name: &GroupName,
    segment: &PlannedReadSegment,
    candidate: &WorkerEndpointInfo,
) -> ClientResult<proto::worker::OpenReadStreamRequestProto> {
    if segment.block_stamp == 0 {
        return Err(ClientError::InvalidLayout(
            "planned read segment has zero block_stamp".to_string(),
        ));
    }
    if segment.block_size == 0
        || segment.chunk_size == 0
        || segment.effective_block_len == 0
        || segment.effective_block_len > segment.block_size
    {
        return Err(ClientError::InvalidLayout(
            "planned read segment has invalid expected block shape".to_string(),
        ));
    }
    Ok(proto::worker::OpenReadStreamRequestProto {
        header: Some(ctx.data_header()),
        group_name: group_name.to_string(),
        block_id: Some(segment.block_id.into()),
        byte_range: Some(
            ByteRange {
                offset: segment.block_offset,
                len: segment.len,
            }
            .into(),
        ),
        block_stamp: segment.block_stamp,
        frame_size: default_frame_size(segment.len),
        worker_run_id: candidate.worker_run_id.to_string(),
        block_format_id: segment.block_format_id.as_raw(),
        block_size: segment.block_size,
        chunk_size: segment.chunk_size,
        effective_block_len: segment.effective_block_len,
    })
}

fn validate_worker_write_target(target: &WorkerWriteTarget) -> ClientResult<()> {
    let block = target.target.block_id;
    if block.data_handle_id.as_raw() == 0 {
        return Err(ClientError::InvalidLayout(
            "write target block_id data_handle_id must be non-zero".to_string(),
        ));
    }
    if target.target.block_size == 0 {
        return Err(ClientError::InvalidLayout(
            "write target block_size must be non-zero".to_string(),
        ));
    }
    if target.target.effective_block_len == 0 {
        return Err(ClientError::InvalidLayout(
            "write target effective_block_len must be non-zero".to_string(),
        ));
    }
    if target.target.effective_block_len > target.target.block_size {
        return Err(ClientError::InvalidLayout(
            "write target effective_block_len must not exceed block_size".to_string(),
        ));
    }
    if target.target.worker_endpoints.is_empty() {
        return Err(ClientError::InvalidLayout(
            "write target has no worker endpoints".to_string(),
        ));
    }
    if target.target.block_stamp == 0 {
        return Err(ClientError::InvalidLayout(
            "write target block_stamp must be non-zero".to_string(),
        ));
    }
    if target.target.chunk_size == 0 {
        return Err(ClientError::InvalidLayout(
            "write target chunk_size must be non-zero".to_string(),
        ));
    }
    if u64::from(target.target.chunk_size) > target.target.block_size {
        return Err(ClientError::InvalidLayout(
            "write target chunk_size must not exceed block_size".to_string(),
        ));
    }
    if !target
        .target
        .block_size
        .is_multiple_of(u64::from(target.target.chunk_size))
    {
        return Err(ClientError::InvalidLayout(
            "write target block_size must be a multiple of chunk_size".to_string(),
        ));
    }
    validate_fencing_token(&target.target)?;
    Ok(())
}

fn build_open_write_stream_request(
    ctx: &AttemptContext,
    target: &WorkerWriteTarget,
    candidate: &WorkerEndpointInfo,
) -> ClientResult<proto::worker::OpenWriteStreamRequestProto> {
    validate_worker_write_target(target)?;
    Ok(proto::worker::OpenWriteStreamRequestProto {
        header: Some(ctx.data_header()),
        group_name: target.group_name.to_string(),
        block_id: Some(target.target.block_id.into()),
        block_size: target.target.block_size,
        block_stamp: target.target.block_stamp,
        chunk_size: target.target.chunk_size,
        checksum_kind: proto::worker::ChecksumKindProto::ChecksumKindNone as i32,
        token: Some(target.target.fencing_token.into()),
        frame_size: default_frame_size(target.target.effective_block_len.min(u64::from(u32::MAX)) as u32),
        block_format_id: target.target.block_format_id.as_raw(),
        worker_run_id: candidate.worker_run_id.to_string(),
        effective_block_len: target.target.effective_block_len,
    })
}

fn build_write_stream_frames(
    block: &WorkerWriteBlock,
    data: Bytes,
) -> ClientResult<Vec<proto::worker::WriteStreamRequestProto>> {
    let frame_size = block.frame_size.max(1) as usize;
    let frame_count = data.len().div_ceil(frame_size);
    let mut frames = Vec::with_capacity(frame_count);
    let mut offset = 0usize;
    while offset < data.len() {
        let end = (offset + frame_size).min(data.len());
        let seq = block
            .next_seq
            .checked_add(frames.len() as u64)
            .ok_or_else(|| ClientError::Worker("worker write frame sequence overflow".to_string()))?;
        frames.push(proto::worker::WriteStreamRequestProto {
            stream_id: Some(block.stream_id),
            seq,
            offset_in_block: offset as u64,
            data: data.slice(offset..end),
            checksum32: 0,
        });
        offset = end;
    }
    Ok(frames)
}

fn build_commit_write_request(
    ctx: &AttemptContext,
    block: &WorkerWriteBlock,
    effective_len: u64,
    commit_seq: u64,
    require_sync: bool,
) -> ClientResult<proto::worker::CommitWriteRequestProto> {
    validate_block_for_worker_control(block)?;
    Ok(proto::worker::CommitWriteRequestProto {
        header: Some(ctx.data_header()),
        group_name: block.group_name.to_string(),
        block_id: Some(block.target.block_id.into()),
        stream_id: Some(block.stream_id),
        effective_block_len: effective_len,
        block_stamp: block.target.block_stamp,
        token: Some(block.target.fencing_token.into()),
        commit_seq,
        require_sync,
        worker_run_id: block.worker.worker_run_id.to_string(),
        block_format_id: block.target.block_format_id.as_raw(),
        block_size: block.target.block_size,
        chunk_size: block.target.chunk_size,
    })
}

fn build_sync_committed_block_request(
    ctx: &AttemptContext,
    block: &WorkerWriteBlock,
    expected_len: u64,
) -> ClientResult<proto::worker::SyncCommittedBlockRequestProto> {
    validate_block_for_worker_sync(block)?;
    Ok(proto::worker::SyncCommittedBlockRequestProto {
        header: Some(ctx.data_header()),
        group_name: block.group_name.to_string(),
        block_id: Some(block.target.block_id.into()),
        block_stamp: block.target.block_stamp,
        expected_block_len: expected_len,
        worker_run_id: block.worker.worker_run_id.to_string(),
        block_format_id: block.target.block_format_id.as_raw(),
        block_size: block.target.block_size,
        chunk_size: block.target.chunk_size,
    })
}

fn build_abort_write_request(
    ctx: &AttemptContext,
    block: &WorkerWriteBlock,
) -> ClientResult<proto::worker::AbortWriteRequestProto> {
    validate_block_for_worker_control(block)?;
    Ok(proto::worker::AbortWriteRequestProto {
        header: Some(ctx.data_header()),
        group_name: block.group_name.to_string(),
        block_id: Some(block.target.block_id.into()),
        stream_id: Some(block.stream_id),
        token: Some(block.target.fencing_token.into()),
    })
}

fn worker_write_block_from_open_response(
    ctx: &AttemptContext,
    target: &WorkerWriteTarget,
    candidate: &WorkerEndpointInfo,
    response: proto::worker::OpenWriteStreamResponseProto,
) -> ClientResult<WorkerWriteBlock> {
    parse_worker_control_header(ctx, response.header.as_ref())?;
    let stream_id = response
        .stream_id
        .ok_or_else(|| side_effect_response_body_mismatch("OpenWriteStream", "missing stream_id"))?;
    if stream_id.high == 0 && stream_id.low == 0 {
        return Err(side_effect_response_body_mismatch(
            "OpenWriteStream",
            "stream_id is zero",
        ));
    }
    if response.block_stamp != target.target.block_stamp {
        return Err(side_effect_response_body_mismatch(
            "OpenWriteStream",
            format!(
                "block_stamp expected {}, got {}",
                target.target.block_stamp, response.block_stamp
            ),
        ));
    }
    if response.committed_length != 0 {
        return Err(side_effect_response_body_mismatch(
            "OpenWriteStream",
            format!("committed_length expected 0, got {}", response.committed_length),
        ));
    }
    Ok(WorkerWriteBlock {
        group_name: target.group_name.clone(),
        worker: candidate.clone(),
        target: target.target.clone(),
        stream_id,
        frame_size: response.frame_size.max(1),
        next_seq: 1,
    })
}

fn worker_endpoint_from_block(block: &WorkerWriteBlock) -> WorkerEndpointInfo {
    block.worker.clone()
}

fn validate_write_stream_response(
    response: proto::worker::WriteStreamResponseProto,
    expected_last_seq: u64,
    expected_written_through: u64,
) -> ClientResult<proto::worker::WriteStreamResponseProto> {
    if response.last_acked_seq != expected_last_seq {
        return Err(ClientError::UnknownOutcome(format!(
            "worker WriteStream ack mismatch: expected {}, got {}",
            expected_last_seq, response.last_acked_seq
        )));
    }
    if response.written_through != expected_written_through {
        return Err(ClientError::UnknownOutcome(format!(
            "worker WriteStream written_through mismatch: expected {}, got {}",
            expected_written_through, response.written_through
        )));
    }
    Ok(response)
}

fn worker_commit_result_from_response(
    ctx: &AttemptContext,
    block: &WorkerWriteBlock,
    effective_len: u64,
    response: proto::worker::CommitWriteResponseProto,
) -> ClientResult<WorkerCommitResult> {
    parse_worker_control_header(ctx, response.header.as_ref())?;
    if response.effective_block_len != effective_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!(
                "effective_block_len expected {}, got {}",
                effective_len, response.effective_block_len
            ),
        ));
    }
    if response.block_stamp != block.target.block_stamp {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!(
                "block_stamp expected {}, got {}",
                block.target.block_stamp, response.block_stamp
            ),
        ));
    }
    if response.written_through != effective_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!(
                "written_through expected {}, got {}",
                effective_len, response.written_through
            ),
        ));
    }
    Ok(WorkerCommitResult {
        effective_block_len: response.effective_block_len,
        block_stamp: response.block_stamp,
        written_through: response.written_through,
    })
}

fn worker_block_sync_result_from_response(
    ctx: &AttemptContext,
    block: &WorkerWriteBlock,
    expected_len: u64,
    response: proto::worker::SyncCommittedBlockResponseProto,
) -> ClientResult<WorkerBlockSyncResult> {
    parse_worker_control_header(ctx, response.header.as_ref())?;
    if response.effective_block_len != expected_len {
        return Err(side_effect_response_body_mismatch(
            "SyncCommittedBlock",
            format!(
                "effective_block_len expected {}, got {}",
                expected_len, response.effective_block_len
            ),
        ));
    }
    if response.block_stamp != block.target.block_stamp {
        return Err(side_effect_response_body_mismatch(
            "SyncCommittedBlock",
            format!(
                "block_stamp expected {}, got {}",
                block.target.block_stamp, response.block_stamp
            ),
        ));
    }
    Ok(WorkerBlockSyncResult {
        effective_block_len: response.effective_block_len,
        block_stamp: response.block_stamp,
    })
}

fn validate_abort_write_response(
    ctx: &AttemptContext,
    response: proto::worker::AbortWriteResponseProto,
) -> ClientResult<()> {
    parse_worker_control_header(ctx, response.header.as_ref())?;
    if !response.aborted {
        return Err(ClientError::UnknownOutcome(
            "worker AbortWrite response did not confirm abort".to_string(),
        ));
    }
    Ok(())
}

fn validate_block_for_worker_control(block: &WorkerWriteBlock) -> ClientResult<()> {
    if block.stream_id.high == 0 && block.stream_id.low == 0 {
        return Err(ClientError::InvalidArgument(
            "worker write control requires non-zero stream_id".to_string(),
        ));
    }
    validate_fencing_token(&block.target)
}

fn validate_fencing_token(target: &WriteTarget) -> ClientResult<()> {
    let block = target.block_id;
    let token = target.fencing_token;
    if token.owner.is_zero() || token.epoch == 0 {
        return Err(ClientError::InvalidLayout(
            "write target fencing_token owner and epoch must be non-zero".to_string(),
        ));
    }
    if token.block_id != block {
        return Err(ClientError::InvalidLayout(
            "write target fencing_token block_id must match target block_id".to_string(),
        ));
    }
    Ok(())
}

fn validate_block_for_worker_sync(block: &WorkerWriteBlock) -> ClientResult<()> {
    if block.target.block_stamp == 0 {
        return Err(ClientError::InvalidArgument(
            "worker block sync requires non-zero block_stamp".to_string(),
        ));
    }
    Ok(())
}

async fn read_stream_to_bytes(
    stream: &mut tonic::codec::Streaming<proto::worker::ReadStreamResponseProto>,
    segment: &PlannedReadSegment,
) -> ClientResult<Bytes> {
    let mut output = BytesMut::with_capacity(segment.len as usize);
    let mut expected_offset = segment.block_offset;
    while let Some(frame) = stream.message().await.map_err(ClientError::from)? {
        if append_read_stream_frame(&mut output, &mut expected_offset, segment, frame)? {
            break;
        }
    }
    finish_read_stream_output(output, segment)
}

fn finish_read_stream_output(output: BytesMut, segment: &PlannedReadSegment) -> ClientResult<Bytes> {
    if output.len() != segment.len as usize {
        return Err(ClientError::Worker(format!(
            "worker read ended after {} bytes, expected {}",
            output.len(),
            segment.len
        )));
    }
    Ok(output.freeze())
}

fn append_read_stream_frame(
    output: &mut BytesMut,
    expected_offset: &mut u64,
    segment: &PlannedReadSegment,
    frame: proto::worker::ReadStreamResponseProto,
) -> ClientResult<bool> {
    if frame.offset_in_block != *expected_offset {
        return Err(ClientError::Worker(format!(
            "worker read frame offset mismatch: expected {}, got {}",
            *expected_offset, frame.offset_in_block
        )));
    }
    if frame.data.is_empty() && !frame.eos {
        return Err(ClientError::Worker(
            "worker read returned zero-length non-final frame".to_string(),
        ));
    }
    let remaining = segment.len as usize - output.len();
    if frame.data.len() > remaining {
        return Err(ClientError::Worker(format!(
            "worker read frame exceeded requested segment: remaining {}, got {}",
            remaining,
            frame.data.len()
        )));
    }
    *expected_offset = expected_offset
        .checked_add(frame.data.len() as u64)
        .ok_or_else(|| ClientError::Worker("worker read frame offset overflow".to_string()))?;
    output.extend_from_slice(&frame.data);
    Ok(frame.eos)
}

fn parse_worker_control_header(
    ctx: &AttemptContext,
    header: Option<&proto::worker::DataResponseHeaderProto>,
) -> ClientResult<()> {
    let Some(header) = header else {
        return Err(invalid_worker_header("worker OK response missing DataResponseHeader"));
    };
    let client = header.client.as_ref().ok_or_else(|| {
        invalid_worker_header("worker OK response invalid DataResponseHeader: missing client identity")
    })?;
    let client_id = proto::convert::required_client_id(client.client_id, "client_id")
        .map_err(|err| invalid_worker_header(format!("worker OK response invalid DataResponseHeader: {err}")))?;
    if client_id != ctx.client_id() {
        return Err(invalid_worker_header(
            "worker OK response invalid DataResponseHeader: client_id mismatch",
        ));
    }
    if client.call_id.is_empty() {
        return Err(invalid_worker_header(
            "worker OK response invalid DataResponseHeader: call_id must not be empty",
        ));
    }
    if client.call_id != ctx.call_id() {
        return Err(invalid_worker_header(
            "worker OK response invalid DataResponseHeader: call_id mismatch",
        ));
    }
    validate_data_header_or_action(Some(header)).map_err(ClientError::from)
}

fn invalid_worker_header(message: impl Into<String>) -> ClientError {
    ClientError::from(invalid_header_action(message))
}

fn is_retryable_worker_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
    )
}

fn worker_identity_mismatch_invalidation_reason(err: &ClientError) -> Option<CacheInvalidationReason> {
    match ErrorClassifier.classify_error(err) {
        ErrorClass::NeedRefresh(RefreshReason::WorkerRunMismatch) => Some(CacheInvalidationReason::WorkerRun),
        _ => None,
    }
}

fn is_endpoint_health_error(err: &ClientError) -> bool {
    matches!(err, ClientError::Worker(message) if message.contains("temporarily unavailable"))
}

fn default_frame_size(len: u32) -> u32 {
    len.clamp(1, 1024 * 1024)
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

fn lazy_channel(endpoint: &str) -> ClientResult<tonic_net::Channel> {
    tonic_net::Endpoint::from_shared(endpoint.to_string())
        .map_err(|err| ClientError::Worker(format!("invalid worker endpoint {endpoint}: {err}")))
        .map(|endpoint| endpoint.connect_lazy())
}

fn tonic_request<T>(ctx: &AttemptContext, message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(timeout) = ctx.timeout_remaining() {
        request.set_timeout(timeout.max(Duration::from_millis(1)));
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use common::error::canonical::{
        CanonicalError, ErrorClass as CanonicalErrorClass, ErrorCode as CanonicalErrorCode,
        RefreshHint as CanonicalRefreshHint, RefreshReason,
    };
    use common::header::RpcErrorCode;
    use proto::convert::canonical_to_error_detail;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use types::lease::FencingToken;
    use types::{
        BlockId, BlockIndex, ClientId, DataHandleId, WorkerEndpointInfo, WorkerId, WorkerNetProtocol, WriteTarget,
    };

    use crate::canonical::ClientAction;
    use crate::metrics::NoopClientMetrics;
    use crate::planner::read_planner::PlannedReadSegment;
    use crate::runtime::{ErrorClass, ErrorClassifier, OperationContext, OperationIdentity, OperationKind};

    fn test_worker_run_id() -> types::WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("valid test WorkerRunId")
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
    fn worker_data_boundary_accepts_grpc_protocol() {
        assert!(ensure_supported_worker_protocol(WorkerNetProtocol::Grpc).is_ok());
    }

    #[tokio::test]
    async fn worker_channel_pool_reuses_channel_for_same_worker_endpoint() {
        let metrics = Arc::new(RecordingMetrics::default());
        let client = TonicWorkerDataClient::with_parts(
            WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone()),
            true,
            1,
            metrics.clone(),
        );
        let candidate = worker_endpoint();

        let _first = client.client(&candidate, "read").await.expect("first client");
        let _second = client.client(&candidate, "read").await.expect("second client");

        let events = metrics.events();
        assert_metric(&events, ClientMetric::WorkerChannelPoolMiss);
        assert_metric(&events, ClientMetric::WorkerChannelPoolHit);
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
    }

    #[tokio::test]
    async fn concurrent_worker_channel_requests_same_key_reuse_inserted_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let client = Arc::new(TonicWorkerDataClient::with_parts(
            WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone()),
            true,
            8,
            metrics.clone(),
        ));
        let candidate = worker_endpoint();

        let mut tasks = Vec::with_capacity(8);
        for _ in 0..8 {
            let client = Arc::clone(&client);
            let candidate = candidate.clone();
            tasks.push(tokio::spawn(async move { client.client(&candidate, "read").await }));
        }

        for task in tasks {
            let _client = task.await.expect("task").expect("worker client");
        }
        assert_eq!(client.channels.read().len(), 1);
        let events = metrics.events();
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
    }

    #[tokio::test]
    async fn worker_channel_different_run_does_not_share_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let client = Arc::new(TonicWorkerDataClient::with_parts(
            WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone()),
            true,
            8,
            metrics,
        ));
        let mut first = worker_endpoint();
        first.worker_run_id = "550e8400-e29b-41d4-a716-446655440007"
            .parse()
            .expect("valid first WorkerRunId");
        let mut second = worker_endpoint();
        second.worker_run_id = "550e8400-e29b-41d4-a716-446655440008"
            .parse()
            .expect("valid second WorkerRunId");

        let first_task = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.client(&first, "read").await })
        };
        let second_task = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.client(&second, "read").await })
        };

        first_task.await.expect("first").expect("first client");
        second_task.await.expect("second").expect("second client");
        assert_eq!(client.channels.read().len(), 2);
    }

    #[tokio::test]
    async fn worker_identity_mismatch_invalidates_target_endpoint_and_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let client = TonicWorkerDataClient::with_parts(
            WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone()),
            true,
            1,
            metrics.clone(),
        );
        let candidate = worker_endpoint();
        let ctx = data_attempt_context();

        for (code, reason, message) in [(
            RpcErrorCode::WorkerRunMismatch,
            RefreshReason::WorkerRunMismatch,
            "worker run mismatch",
        )] {
            let _worker_client = client.client(&candidate, "read").await.expect("worker client");
            assert_eq!(client.endpoint_cache().len(), 1);
            assert_eq!(client.channels.read().len(), 1);

            let err = parse_worker_control_header(
                &ctx,
                Some(&data_header_with_error(
                    &ctx,
                    CanonicalError::need_refresh(code, reason, message),
                )),
            )
            .expect_err("worker identity mismatch must fail");

            client.invalidate_worker_identity_mismatch(&candidate, &err);

            assert_eq!(client.endpoint_cache().len(), 0);
            assert_eq!(client.channels.read().len(), 0);
        }
        assert_metric(&metrics.events(), ClientMetric::CachePreciseInvalidation);
    }

    #[tokio::test]
    async fn failed_worker_channel_creation_does_not_insert() {
        let metrics = Arc::new(RecordingMetrics::default());
        let client = Arc::new(TonicWorkerDataClient::with_parts(
            WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone()),
            true,
            8,
            metrics.clone(),
        ));
        let mut candidate = worker_endpoint();
        candidate.endpoint = "http://[invalid".to_string();

        let mut tasks = Vec::with_capacity(4);
        for _ in 0..4 {
            let client = Arc::clone(&client);
            let candidate = candidate.clone();
            tasks.push(tokio::spawn(async move { client.client(&candidate, "read").await }));
        }

        for task in tasks {
            let err = task.await.expect("task").expect_err("invalid endpoint");
            assert!(matches!(err, ClientError::Worker(msg) if msg.contains("invalid worker endpoint")));
        }
        assert!(client.channels.read().is_empty());
        assert_metric(&metrics.events(), ClientMetric::ChannelPoolConnectError);
    }

    #[tokio::test]
    async fn disabled_worker_channel_pool_does_not_reuse_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let client = TonicWorkerDataClient::with_parts(
            WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone()),
            false,
            1,
            metrics.clone(),
        );
        let candidate = worker_endpoint();

        let _first = client.client(&candidate, "read").await.expect("first client");
        let _second = client.client(&candidate, "read").await.expect("second client");

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

    #[tokio::test]
    async fn unsupported_worker_protocol_does_not_create_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let client = TonicWorkerDataClient::with_parts(
            WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics),
            true,
            1,
            Arc::new(NoopClientMetrics),
        );
        let mut candidate = worker_endpoint();
        candidate.worker_net_protocol = WorkerNetProtocol::Quic;

        let err = client
            .client(&candidate, "read")
            .await
            .expect_err("unsupported protocol rejected");

        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("unsupported worker net protocol")));
        assert!(client.channels.read().is_empty());
    }

    #[tokio::test]
    async fn worker_channel_pool_connection_error_is_reported() {
        let metrics = Arc::new(RecordingMetrics::default());
        let client = TonicWorkerDataClient::with_parts(
            WorkerEndpointCache::new(true, Duration::from_secs(60), 8, metrics.clone()),
            true,
            1,
            metrics.clone(),
        );
        let mut candidate = worker_endpoint();
        candidate.endpoint = "http://[invalid".to_string();

        let err = client
            .client(&candidate, "read")
            .await
            .expect_err("invalid endpoint fails");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("invalid worker endpoint")));
        assert_metric(&metrics.events(), ClientMetric::ChannelPoolConnectError);
    }

    #[test]
    fn worker_data_boundary_returns_unsupported_for_quic_and_rdma() {
        for protocol in [WorkerNetProtocol::Quic, WorkerNetProtocol::Rdma] {
            let err = ensure_supported_worker_protocol(protocol).expect_err("known non-GRPC protocol is unsupported");

            assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("unsupported worker net protocol")));
        }
    }

    #[test]
    fn missing_worker_control_header_is_invalid_header_action() {
        let ctx = data_attempt_context();
        let err = parse_worker_control_header(&ctx, None).expect_err("missing data header must fail");

        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        match action(&err) {
            ClientAction::Fail { canonical } => {
                assert!(matches!(
                    canonical.code,
                    Some(common::error::canonical::ErrorCode::RpcCode(
                        RpcErrorCode::InvalidHeader
                    ))
                ));
                assert!(canonical.message.contains("missing DataResponseHeader"));
            }
            other => panic!("expected invalid header failure, got {other:?}"),
        }
    }

    #[test]
    fn malformed_worker_control_header_is_invalid_header_not_transport_retry() {
        let ctx = data_attempt_context();
        let malformed = proto::worker::DataResponseHeaderProto::default();

        let err = parse_worker_control_header(&ctx, Some(&malformed)).expect_err("malformed data header must fail");

        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        match action(&err) {
            ClientAction::Fail { canonical } => {
                assert!(matches!(
                    canonical.code,
                    Some(common::error::canonical::ErrorCode::RpcCode(
                        RpcErrorCode::InvalidHeader
                    ))
                ));
                assert!(canonical.message.contains("invalid DataResponseHeader"));
            }
            other => panic!("expected invalid header failure, got {other:?}"),
        }
    }

    #[test]
    fn worker_control_header_preserves_refresh_reason() {
        let ctx = data_attempt_context();
        let canonical = CanonicalError::need_refresh_with_hint(
            RpcErrorCode::BlockStampMismatch,
            RefreshReason::BlockStampMismatch,
            CanonicalRefreshHint {
                worker_resolve_required: true,
                ..CanonicalRefreshHint::default()
            },
            "worker requires refreshed location",
        );
        let header = proto::worker::DataResponseHeaderProto {
            client: Some(ctx.client_info()),
            error: Some(canonical_to_error_detail(&canonical)),
        };

        let err = parse_worker_control_header(&ctx, Some(&header)).expect_err("refresh error must surface");

        match action(&err) {
            ClientAction::Refresh { reason, hint, .. } => {
                assert_eq!(*reason, RefreshReason::BlockStampMismatch);
                assert!(hint.worker_resolve_required);
            }
            other => panic!("expected refresh action, got {other:?}"),
        }
    }

    #[test]
    fn open_read_stream_request_uses_metadata_block_stamp() {
        let ctx = data_attempt_context();
        let segment = planned_segment(77);
        let candidate = worker_endpoint();

        let group_name = test_group_name();
        let request = build_open_read_stream_request(&ctx, &group_name, &segment, &candidate).expect("request");

        assert_eq!(request.block_stamp, 77);
        assert_eq!(request.worker_run_id, test_worker_run_id().to_string());
        assert_eq!(
            request.block_format_id,
            types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw()
        );
        assert_eq!(request.block_size, 4096);
        assert_eq!(request.chunk_size, 4096);
        assert_eq!(request.effective_block_len, 5);
    }

    #[test]
    fn open_read_stream_request_rejects_zero_block_stamp() {
        let ctx = data_attempt_context();
        let segment = planned_segment(0);
        let candidate = worker_endpoint();

        let group_name = test_group_name();
        let err =
            build_open_read_stream_request(&ctx, &group_name, &segment, &candidate).expect_err("zero stamp must fail");

        assert!(matches!(err, ClientError::InvalidLayout(msg) if msg.contains("block_stamp")));
    }

    #[test]
    fn open_read_stream_request_rejects_zero_expected_fields() {
        let ctx = data_attempt_context();
        let mut segment = planned_segment(77);
        segment.block_size = 0;
        let candidate = worker_endpoint();

        let group_name = test_group_name();
        let err = build_open_read_stream_request(&ctx, &group_name, &segment, &candidate)
            .expect_err("zero block_size must not be defaulted");

        assert!(matches!(err, ClientError::InvalidLayout(msg) if msg.contains("expected block shape")));
    }

    #[test]
    fn open_write_stream_request_uses_metadata_target_fields() {
        let ctx = write_attempt_context();
        let target = worker_write_target();
        let candidate = target.target.worker_endpoints[0].clone();

        let request = build_open_write_stream_request(&ctx, &target, &candidate).expect("open write request");

        assert_eq!(request.group_name, "root");
        assert_eq!(request.block_id.as_ref().map(|block| block.data_handle_id), Some(202));
        assert_eq!(request.block_size, 4096);
        assert_eq!(request.block_stamp, 77);
        assert_eq!(request.chunk_size, 4096);
        assert_eq!(
            request.block_format_id,
            types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw()
        );
        assert_eq!(request.worker_run_id, test_worker_run_id().to_string());
        assert_eq!(request.effective_block_len, 5);
        assert_eq!(
            request.token.as_ref().and_then(|token| token.owner),
            Some(ClientId::new(7).into())
        );
    }

    #[test]
    fn open_write_stream_request_rejects_zero_metadata_target_shape() {
        let ctx = write_attempt_context();
        let mut target = worker_write_target();
        target.target.chunk_size = 0;
        let candidate = target.target.worker_endpoints[0].clone();

        let err = build_open_write_stream_request(&ctx, &target, &candidate)
            .expect_err("zero chunk_size must not be defaulted");

        assert!(matches!(err, ClientError::InvalidLayout(msg) if msg.contains("chunk_size")));
    }

    #[test]
    fn write_stream_frames_are_monotonic() {
        let block = worker_write_block(4);

        let frames = build_write_stream_frames(&block, Bytes::from_static(b"abcdef")).expect("frames");

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].seq, 1);
        assert_eq!(frames[0].offset_in_block, 0);
        assert_eq!(frames[0].data, Bytes::from_static(b"abcd"));
        assert_eq!(frames[1].seq, 2);
        assert_eq!(frames[1].offset_in_block, 4);
        assert_eq!(frames[1].data, Bytes::from_static(b"ef"));
    }

    #[test]
    fn commit_write_request_uses_length_and_fencing_token() {
        let ctx = write_attempt_context();
        let block = worker_write_block(1024);

        let request = build_commit_write_request(&ctx, &block, 5, 1, false).expect("commit write request");

        assert_eq!(request.effective_block_len, 5);
        assert_eq!(request.block_stamp, 77);
        assert_eq!(request.worker_run_id, test_worker_run_id().to_string());
        assert_eq!(
            request.block_format_id,
            types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw()
        );
        assert_eq!(request.block_size, 4096);
        assert_eq!(request.chunk_size, 4096);
        assert_eq!(request.commit_seq, 1);
        assert!(!request.require_sync);
        assert_eq!(
            request.token.as_ref().and_then(|token| token.owner),
            Some(ClientId::new(7).into())
        );
    }

    #[test]
    fn commit_write_request_can_require_sync() {
        let ctx = write_attempt_context();
        let block = worker_write_block(1024);

        let request = build_commit_write_request(&ctx, &block, 5, 1, true).expect("commit write request");

        assert!(request.require_sync);
    }

    #[test]
    fn sync_committed_block_request_uses_metadata_target_shape() {
        let ctx = write_attempt_context();
        let block = worker_write_block(1024);

        let request = build_sync_committed_block_request(&ctx, &block, 5).expect("sync request");

        assert_eq!(request.block_stamp, 77);
        assert_eq!(request.expected_block_len, 5);
        assert_eq!(request.worker_run_id, test_worker_run_id().to_string());
        assert_eq!(
            request.block_format_id,
            types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw()
        );
        assert_eq!(request.block_size, 4096);
        assert_eq!(request.chunk_size, 4096);
    }

    #[test]
    fn open_write_stream_missing_header_is_invalid_header() {
        let ctx = write_attempt_context();
        let target = worker_write_target();
        let candidate = target.target.worker_endpoints[0].clone();
        let response = proto::worker::OpenWriteStreamResponseProto {
            stream_id: Some(proto::common::StreamIdProto { high: 1, low: 1 }),
            frame_size: 1024,
            block_stamp: target.target.block_stamp,
            ..proto::worker::OpenWriteStreamResponseProto::default()
        };

        let err = worker_write_block_from_open_response(&ctx, &target, &candidate, response)
            .expect_err("missing OpenWriteStream header must fail");

        assert_invalid_worker_header(&err);
    }

    #[test]
    fn commit_write_missing_header_is_invalid_header() {
        let ctx = write_attempt_context();
        let block = worker_write_block(1024);
        let response = proto::worker::CommitWriteResponseProto {
            effective_block_len: 5,
            block_stamp: block.target.block_stamp,
            written_through: 5,
            ..proto::worker::CommitWriteResponseProto::default()
        };

        let err = worker_commit_result_from_response(&ctx, &block, 5, response)
            .expect_err("missing CommitWrite header must fail");

        assert_invalid_worker_header(&err);
    }

    #[test]
    fn abort_write_missing_header_is_invalid_header() {
        let ctx = write_attempt_context();
        let response = proto::worker::AbortWriteResponseProto {
            aborted: true,
            ..proto::worker::AbortWriteResponseProto::default()
        };

        let err = validate_abort_write_response(&ctx, response).expect_err("missing AbortWrite header must fail");

        assert_invalid_worker_header(&err);
    }

    #[test]
    fn worker_control_header_with_wrong_client_id_is_invalid_header() {
        let ctx = write_attempt_context();
        let mut header = ok_data_header(&ctx);
        header.client.as_mut().expect("client").client_id = Some(ClientId::new(ctx.client_id().as_raw() + 1).into());

        let err = parse_worker_control_header(&ctx, Some(&header)).expect_err("wrong client_id must fail");

        assert_invalid_worker_header(&err);
        match action(&err) {
            ClientAction::Fail { canonical } => assert!(canonical.message.contains("client_id")),
            other => panic!("expected invalid header failure, got {other:?}"),
        }
    }

    #[test]
    fn worker_control_header_with_wrong_call_id_is_invalid_header() {
        let ctx = write_attempt_context();
        let mut header = ok_data_header(&ctx);
        header.client.as_mut().expect("client").call_id = types::CallId::new().to_string();

        let err = parse_worker_control_header(&ctx, Some(&header)).expect_err("wrong call_id must fail");

        assert_invalid_worker_header(&err);
        match action(&err) {
            ClientAction::Fail { canonical } => assert!(canonical.message.contains("call_id")),
            other => panic!("expected invalid header failure, got {other:?}"),
        }
    }

    #[test]
    fn open_write_stream_malformed_header_is_invalid_header() {
        let ctx = write_attempt_context();
        let target = worker_write_target();
        let candidate = target.target.worker_endpoints[0].clone();
        let response = proto::worker::OpenWriteStreamResponseProto {
            header: Some(proto::worker::DataResponseHeaderProto::default()),
            stream_id: Some(proto::common::StreamIdProto { high: 1, low: 1 }),
            frame_size: 1024,
            block_stamp: target.target.block_stamp,
            ..proto::worker::OpenWriteStreamResponseProto::default()
        };

        let err = worker_write_block_from_open_response(&ctx, &target, &candidate, response)
            .expect_err("malformed OpenWriteStream header must fail");

        assert_invalid_worker_header(&err);
    }

    #[test]
    fn open_write_stream_missing_stream_id_is_unknown_outcome() {
        let ctx = write_attempt_context();
        let target = worker_write_target();
        let candidate = target.target.worker_endpoints[0].clone();
        let response = proto::worker::OpenWriteStreamResponseProto {
            header: Some(ok_data_header(&ctx)),
            stream_id: None,
            frame_size: 1024,
            block_stamp: target.target.block_stamp,
            ..proto::worker::OpenWriteStreamResponseProto::default()
        };

        let err = worker_write_block_from_open_response(&ctx, &target, &candidate, response)
            .expect_err("missing OpenWriteStream stream_id must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("OpenWriteStream")));
    }

    #[test]
    fn open_write_stream_zero_stream_id_is_unknown_outcome() {
        let ctx = write_attempt_context();
        let target = worker_write_target();
        let candidate = target.target.worker_endpoints[0].clone();
        let response = proto::worker::OpenWriteStreamResponseProto {
            header: Some(ok_data_header(&ctx)),
            stream_id: Some(proto::common::StreamIdProto { high: 0, low: 0 }),
            frame_size: 1024,
            block_stamp: target.target.block_stamp,
            ..proto::worker::OpenWriteStreamResponseProto::default()
        };

        let err = worker_write_block_from_open_response(&ctx, &target, &candidate, response)
            .expect_err("zero OpenWriteStream stream_id must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("OpenWriteStream")));
    }

    #[test]
    fn open_write_stream_body_mismatch_is_unknown_outcome() {
        let ctx = write_attempt_context();
        let target = worker_write_target();
        let candidate = target.target.worker_endpoints[0].clone();
        let response = proto::worker::OpenWriteStreamResponseProto {
            header: Some(ok_data_header(&ctx)),
            stream_id: Some(proto::common::StreamIdProto { high: 1, low: 1 }),
            frame_size: 1024,
            block_stamp: target.target.block_stamp + 1,
            ..proto::worker::OpenWriteStreamResponseProto::default()
        };

        let err = worker_write_block_from_open_response(&ctx, &target, &candidate, response)
            .expect_err("OpenWriteStream block_stamp mismatch must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("OpenWriteStream")));
    }

    #[test]
    fn open_write_stream_committed_length_body_mismatch_is_unknown_outcome() {
        let ctx = write_attempt_context();
        let target = worker_write_target();
        let candidate = target.target.worker_endpoints[0].clone();
        let response = proto::worker::OpenWriteStreamResponseProto {
            header: Some(ok_data_header(&ctx)),
            stream_id: Some(proto::common::StreamIdProto { high: 1, low: 1 }),
            frame_size: 1024,
            block_stamp: target.target.block_stamp,
            committed_length: 1,
            ..proto::worker::OpenWriteStreamResponseProto::default()
        };

        let err = worker_write_block_from_open_response(&ctx, &target, &candidate, response)
            .expect_err("OpenWriteStream committed_length mismatch must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("OpenWriteStream")));
    }

    #[test]
    fn commit_write_malformed_header_is_invalid_header() {
        let ctx = write_attempt_context();
        let block = worker_write_block(1024);
        let response = proto::worker::CommitWriteResponseProto {
            header: Some(proto::worker::DataResponseHeaderProto::default()),
            effective_block_len: 5,
            block_stamp: block.target.block_stamp,
            written_through: 5,
        };

        let err = worker_commit_result_from_response(&ctx, &block, 5, response)
            .expect_err("malformed CommitWrite header must fail");

        assert_invalid_worker_header(&err);
    }

    #[test]
    fn commit_write_length_body_mismatch_is_unknown_outcome() {
        let ctx = write_attempt_context();
        let block = worker_write_block(1024);
        let response = proto::worker::CommitWriteResponseProto {
            header: Some(ok_data_header(&ctx)),
            effective_block_len: 4,
            block_stamp: block.target.block_stamp,
            written_through: 5,
        };

        let err = worker_commit_result_from_response(&ctx, &block, 5, response)
            .expect_err("CommitWrite length mismatch must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitWrite")));
    }

    #[test]
    fn commit_write_written_through_body_mismatch_is_unknown_outcome() {
        let ctx = write_attempt_context();
        let block = worker_write_block(1024);
        let response = proto::worker::CommitWriteResponseProto {
            header: Some(ok_data_header(&ctx)),
            effective_block_len: 5,
            block_stamp: block.target.block_stamp,
            written_through: 4,
        };

        let err = worker_commit_result_from_response(&ctx, &block, 5, response)
            .expect_err("CommitWrite written_through mismatch must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitWrite")));
    }

    #[test]
    fn commit_write_block_stamp_body_mismatch_is_unknown_outcome() {
        let ctx = write_attempt_context();
        let block = worker_write_block(1024);
        let response = proto::worker::CommitWriteResponseProto {
            header: Some(ok_data_header(&ctx)),
            effective_block_len: 5,
            block_stamp: 0,
            written_through: 5,
        };

        let err = worker_commit_result_from_response(&ctx, &block, 5, response)
            .expect_err("CommitWrite block_stamp mismatch must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitWrite")));
    }

    #[test]
    fn abort_write_malformed_header_is_invalid_header() {
        let ctx = write_attempt_context();
        let response = proto::worker::AbortWriteResponseProto {
            header: Some(proto::worker::DataResponseHeaderProto::default()),
            aborted: true,
        };

        let err = validate_abort_write_response(&ctx, response).expect_err("malformed AbortWrite header must fail");

        assert_invalid_worker_header(&err);
    }

    #[test]
    fn worker_fencing_mismatch_is_typed_error() {
        let ctx = write_attempt_context();
        let err = parse_worker_control_header(
            &ctx,
            Some(&data_header_with_error(
                &ctx,
                CanonicalError::need_refresh(RpcErrorCode::Fencing, RefreshReason::Fencing, "fencing mismatch"),
            )),
        )
        .expect_err("fencing mismatch must fail");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::Fencing);
    }

    #[test]
    fn worker_fatal_fencing_mismatch_is_typed_error() {
        let ctx = write_attempt_context();
        let err = parse_worker_control_header(
            &ctx,
            Some(&data_header_with_error(
                &ctx,
                CanonicalError {
                    class: CanonicalErrorClass::Fatal,
                    code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing)),
                    reason: None,
                    retry_after_ms: None,
                    message: "fencing mismatch".to_string(),
                    refresh_hint: None,
                },
            )),
        )
        .expect_err("fatal fencing mismatch must fail");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::Fencing);
        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
    }

    #[test]
    fn worker_run_mismatch_is_typed_refresh_error() {
        let ctx = write_attempt_context();
        let err = parse_worker_control_header(
            &ctx,
            Some(&data_header_with_error(
                &ctx,
                CanonicalError::need_refresh(
                    RpcErrorCode::WorkerRunMismatch,
                    RefreshReason::WorkerRunMismatch,
                    "worker run mismatch",
                ),
            )),
        )
        .expect_err("worker run mismatch must fail");

        assert_eq!(
            ErrorClassifier.classify_error(&err),
            ErrorClass::NeedRefresh(crate::runtime::RefreshReason::WorkerRunMismatch)
        );
    }

    #[test]
    fn worker_unsupported_error_is_not_retryable_transport() {
        let ctx = write_attempt_context();
        let err = parse_worker_control_header(
            &ctx,
            Some(&data_header_with_error(
                &ctx,
                CanonicalError::fatal_fs(types::fs::FsErrorCode::ENotsup, "unsupported worker operation"),
            )),
        )
        .expect_err("unsupported worker operation must fail");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::Unsupported);
    }

    #[test]
    fn worker_permission_denied_error_is_not_retryable_transport() {
        let ctx = write_attempt_context();
        let err = parse_worker_control_header(
            &ctx,
            Some(&data_header_with_error(
                &ctx,
                CanonicalError {
                    class: CanonicalErrorClass::Fatal,
                    code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::PermissionDenied)),
                    reason: None,
                    retry_after_ms: None,
                    message: "permission denied".to_string(),
                    refresh_hint: None,
                },
            )),
        )
        .expect_err("permission denied must fail");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::PermissionDenied);
    }

    #[test]
    fn write_stream_partial_ack_is_unknown_outcome() {
        let response = proto::worker::WriteStreamResponseProto {
            accepted: true,
            last_acked_seq: 1,
            written_through: 2,
        };

        let err = validate_write_stream_response(response, 2, 4).expect_err("partial WriteStream ack must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("WriteStream")));
    }

    #[test]
    fn write_stream_over_ack_is_unknown_outcome() {
        let response = proto::worker::WriteStreamResponseProto {
            accepted: true,
            last_acked_seq: 3,
            written_through: 4,
        };

        let err = validate_write_stream_response(response, 2, 4).expect_err("over-ack must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("WriteStream")));
    }

    #[tokio::test]
    async fn data_boundary_rejects_zero_block_stamp_before_worker_io() {
        let worker = Arc::new(CountingWorkerDataClient::default());
        let boundary = DataPlaneBoundary::with_client(worker.clone());
        let segment = planned_segment(0);

        let err = boundary
            .read_all(data_attempt_context(), test_group_name(), &[segment])
            .await
            .expect_err("zero stamp must fail before worker IO");

        assert!(matches!(err, ClientError::InvalidLayout(msg) if msg.contains("block_stamp")));
        assert_eq!(worker.calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn read_stream_frame_validation_rejects_offset_mismatch() {
        let segment = planned_segment(77);
        let mut output = BytesMut::new();
        let mut expected_offset = 0;

        let err = append_read_stream_frame(
            &mut output,
            &mut expected_offset,
            &segment,
            read_frame(1, b"abcd", true),
        )
        .expect_err("offset mismatch must fail");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("offset mismatch")));
        assert!(output.is_empty());
    }

    #[test]
    fn read_stream_frame_validation_rejects_oversized_frame() {
        let segment = planned_segment(77);
        let mut output = BytesMut::new();
        let mut expected_offset = 0;

        let err = append_read_stream_frame(
            &mut output,
            &mut expected_offset,
            &segment,
            read_frame(0, b"abcde", true),
        )
        .expect_err("oversized frame must fail");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("exceeded requested segment")));
        assert!(output.is_empty());
    }

    #[test]
    fn read_stream_frame_validation_rejects_zero_length_non_final_frame() {
        let segment = planned_segment(77);
        let mut output = BytesMut::new();
        let mut expected_offset = 0;

        let err = append_read_stream_frame(&mut output, &mut expected_offset, &segment, read_frame(0, b"", false))
            .expect_err("zero-length non-final frame must fail");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("zero-length non-final")));
        assert!(output.is_empty());
    }

    #[test]
    fn read_stream_frame_validation_rejects_early_eof() {
        let segment = planned_segment(77);
        let mut output = BytesMut::new();
        output.extend_from_slice(b"ab");

        let err = finish_read_stream_output(output, &segment).expect_err("short stream must fail");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("ended after 2 bytes")));
    }

    #[test]
    fn read_stream_frame_validation_accepts_exact_final_frame() {
        let segment = planned_segment(77);
        let mut output = BytesMut::new();
        let mut expected_offset = 0;

        let eos = append_read_stream_frame(
            &mut output,
            &mut expected_offset,
            &segment,
            read_frame(0, b"abcd", true),
        )
        .expect("exact final frame");

        assert!(eos);
        assert_eq!(output.freeze(), Bytes::from_static(b"abcd"));
        assert_eq!(expected_offset, 4);
    }

    #[derive(Default)]
    struct CountingWorkerDataClient {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl WorkerDataClient for CountingWorkerDataClient {
        async fn read_segment(
            &self,
            _ctx: AttemptContext,
            _group_name: GroupName,
            segment: &PlannedReadSegment,
        ) -> ClientResult<Bytes> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(Bytes::from(vec![0; segment.len as usize]))
        }

        async fn open_write(&self, _ctx: AttemptContext, target: WorkerWriteTarget) -> ClientResult<WorkerWriteBlock> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(WorkerWriteBlock {
                group_name: target.group_name,
                worker: worker_endpoint(),
                target: target.target,
                stream_id: proto::common::StreamIdProto { high: 1, low: 1 },
                frame_size: 1024,
                next_seq: 1,
            })
        }

        async fn write_stream(
            &self,
            _block: &WorkerWriteBlock,
            data: Bytes,
        ) -> ClientResult<proto::worker::WriteStreamResponseProto> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(proto::worker::WriteStreamResponseProto {
                accepted: true,
                last_acked_seq: 1,
                written_through: data.len() as u64,
            })
        }

        async fn commit_write(
            &self,
            _ctx: AttemptContext,
            block: &WorkerWriteBlock,
            effective_len: u64,
            _commit_seq: u64,
            _require_sync: bool,
        ) -> ClientResult<WorkerCommitResult> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(WorkerCommitResult {
                effective_block_len: effective_len,
                block_stamp: block.target.block_stamp,
                written_through: effective_len,
            })
        }

        async fn sync_committed_block(
            &self,
            _ctx: AttemptContext,
            block: &WorkerWriteBlock,
            expected_len: u64,
        ) -> ClientResult<WorkerBlockSyncResult> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(WorkerBlockSyncResult {
                effective_block_len: expected_len,
                block_stamp: block.target.block_stamp,
            })
        }

        async fn abort_write(&self, _ctx: AttemptContext, _block: &WorkerWriteBlock) -> ClientResult<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
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

    fn assert_metric(events: &[ClientMetricEvent], metric: ClientMetric) {
        assert!(
            events.iter().any(|event| event.metric == metric),
            "missing metric {metric:?}: {events:?}"
        );
    }

    fn write_attempt_context() -> AttemptContext {
        let operation = OperationContext::new(
            ClientId::new(7),
            OperationKind::WorkerWriteData,
            "OpenWriteStream",
            OperationIdentity::session("/alpha", "handle=1"),
        )
        .expect("operation context");
        AttemptContext::for_data(&operation, 0)
    }

    fn worker_write_target() -> WorkerWriteTarget {
        WorkerWriteTarget {
            group_name: test_group_name(),
            target: WriteTarget {
                block_id: BlockId::new(DataHandleId::new(202), BlockIndex::new(0)),
                file_offset: 0,
                block_size: 4096,
                effective_block_len: 5,
                worker_endpoints: vec![worker_endpoint()],
                fencing_token: FencingToken {
                    block_id: BlockId::new(DataHandleId::new(202), BlockIndex::new(0)),
                    owner: ClientId::new(7),
                    epoch: 1,
                },
                block_stamp: 77,
                chunk_size: 4096,
                block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            },
        }
    }

    fn worker_endpoint() -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(1),
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: WorkerNetProtocol::Grpc,
            worker_run_id: test_worker_run_id(),
        }
    }

    fn worker_write_block(frame_size: u32) -> WorkerWriteBlock {
        WorkerWriteBlock {
            group_name: test_group_name(),
            worker: worker_endpoint(),
            target: worker_write_target().target,
            stream_id: proto::common::StreamIdProto { high: 1, low: 1 },
            frame_size,
            next_seq: 1,
        }
    }

    fn test_group_name() -> GroupName {
        GroupName::parse("root").unwrap()
    }

    fn planned_segment(block_stamp: u64) -> PlannedReadSegment {
        PlannedReadSegment {
            file_offset: 0,
            len: 4,
            end_file_offset: 4,
            block_id: BlockId::new(DataHandleId::new(202), BlockIndex::new(0)),
            block_offset: 0,
            workers: vec![worker_endpoint()],
            block_stamp,
            block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: 4096,
            chunk_size: 4096,
            effective_block_len: 5,
        }
    }

    fn read_frame(offset_in_block: u64, data: &'static [u8], eos: bool) -> proto::worker::ReadStreamResponseProto {
        proto::worker::ReadStreamResponseProto {
            offset_in_block,
            data: Bytes::from_static(data),
            checksum32: 0,
            eos,
        }
    }

    fn data_header_with_error(
        ctx: &AttemptContext,
        canonical: CanonicalError,
    ) -> proto::worker::DataResponseHeaderProto {
        proto::worker::DataResponseHeaderProto {
            client: Some(ctx.client_info()),
            error: Some(canonical_to_error_detail(&canonical)),
        }
    }

    fn ok_data_header(ctx: &AttemptContext) -> proto::worker::DataResponseHeaderProto {
        proto::worker::DataResponseHeaderProto {
            client: Some(ctx.client_info()),
            error: None,
        }
    }

    fn assert_invalid_worker_header(err: &ClientError) {
        assert_ne!(ErrorClassifier.classify_error(err), ErrorClass::RetryableTransport);
        match action(err) {
            ClientAction::Fail { canonical } => {
                assert!(matches!(
                    canonical.code,
                    Some(common::error::canonical::ErrorCode::RpcCode(
                        RpcErrorCode::InvalidHeader
                    ))
                ));
            }
            other => panic!("expected invalid header failure, got {other:?}"),
        }
    }

    fn action(err: &ClientError) -> &ClientAction {
        match err {
            ClientError::Action(action) => action.as_ref(),
            other => panic!("expected action error, got {other:?}"),
        }
    }
}
