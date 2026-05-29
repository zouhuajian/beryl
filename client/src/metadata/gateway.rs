// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataGateway trait and tonic implementation.

use async_trait::async_trait;
use common::header::ResponseHeader;
use proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport as tonic_net;

use crate::canonical::{invalid_header_action, validate_header_or_action};
use crate::config::ClientConfig;
use crate::error::{side_effect_response_body_mismatch, ClientError, ClientResult};
use crate::metadata::header::ensure_metadata_header;
use crate::metadata::ops::{
    AbortFileWriteOp, AddBlockOp, AppendFileOp, CommitFileOp, CreateFileOp, DeleteOp, GetBlockLocationsOp, GetStatusOp,
    ListStatusOp, MsyncOp, OpenFileOp, RenameOp, RenewLeaseOp, SyncWriteOp,
};
use crate::metadata::snapshot::{
    AbortFileWriteResult, AddBlockResult, CommitFileResult, DeleteResult, FileSnapshot, LayoutSnapshot, ListSnapshot,
    RenameResult, RenewLeaseResult, StateWatermark, StatusSnapshot, SyncWriteResult, WriteSessionSeed,
};
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics};
use crate::runtime::AttemptContext;

/// Client-owned metadata control-plane adapter.
#[async_trait]
pub(crate) trait MetadataGateway: Send + Sync {
    /// Get file or directory status.
    async fn get_status(&self, ctx: AttemptContext, req: GetStatusOp) -> ClientResult<StatusSnapshot>;

    /// List directory status.
    async fn list_status(&self, ctx: AttemptContext, req: ListStatusOp) -> ClientResult<ListSnapshot>;

    /// Delete a namespace entry.
    async fn delete(&self, ctx: AttemptContext, req: DeleteOp) -> ClientResult<DeleteResult>;

    /// Rename a namespace entry.
    async fn rename(&self, ctx: AttemptContext, req: RenameOp) -> ClientResult<RenameResult>;

    /// Open a file for read planning.
    async fn open_file(&self, ctx: AttemptContext, req: OpenFileOp) -> ClientResult<FileSnapshot>;

    /// Get the file data layout for a public read.
    async fn read_layout(&self, ctx: AttemptContext, req: GetBlockLocationsOp) -> ClientResult<LayoutSnapshot>;

    /// Create a file and seed a write session.
    async fn create_file(&self, ctx: AttemptContext, req: CreateFileOp) -> ClientResult<WriteSessionSeed>;

    /// Append to an existing file and seed a write session.
    async fn append_file(&self, ctx: AttemptContext, req: AppendFileOp) -> ClientResult<WriteSessionSeed>;

    /// Allocate a worker write target for a write session.
    async fn add_block(&self, ctx: AttemptContext, req: AddBlockOp) -> ClientResult<AddBlockResult>;

    /// Commit a write session after worker data commit succeeds.
    async fn commit_file(&self, ctx: AttemptContext, req: CommitFileOp) -> ClientResult<CommitFileResult>;

    /// Abort a write session best effort.
    async fn abort_file_write(&self, ctx: AttemptContext, req: AbortFileWriteOp) -> ClientResult<AbortFileWriteResult>;

    /// Renew an active write session lease.
    async fn renew_lease(&self, ctx: AttemptContext, req: RenewLeaseOp) -> ClientResult<RenewLeaseResult>;

    /// Apply a write-session visibility or durability barrier.
    async fn sync_write(&self, ctx: AttemptContext, req: SyncWriteOp) -> ClientResult<SyncWriteResult>;

    /// Synchronize metadata state freshness.
    async fn msync(&self, ctx: AttemptContext, req: MsyncOp) -> ClientResult<StateWatermark>;
}

/// Tonic-backed metadata gateway.
#[derive(Clone, Debug)]
pub(crate) struct TonicMetadataGateway {
    default_endpoint: String,
    channels: Arc<parking_lot::RwLock<HashMap<MetadataChannelKey, tonic_net::Channel>>>,
    channel_pool_enabled: bool,
    max_channels_per_group: usize,
    metrics: Arc<dyn ClientMetrics>,
}

impl TonicMetadataGateway {
    /// Create a lazily connecting metadata gateway from client config.
    pub(crate) fn new_lazy_with_config(
        endpoint: impl Into<String>,
        config: &ClientConfig,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        Self::new_lazy_with_pool_options(
            endpoint,
            config.channel_pool.metadata_channel_pool_enabled,
            config.channel_pool.metadata_channel_pool_max_per_group,
            metrics,
        )
    }

    #[cfg(test)]
    fn new_lazy_with_pool(
        endpoint: impl Into<String>,
        channel_pool_enabled: bool,
        max_channels_per_group: usize,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        Self::new_lazy_with_pool_options(endpoint, channel_pool_enabled, max_channels_per_group, metrics)
    }

    fn new_lazy_with_pool_options(
        endpoint: impl Into<String>,
        channel_pool_enabled: bool,
        max_channels_per_group: usize,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        let endpoint = normalize_endpoint(&endpoint.into());
        let channel = lazy_channel(&endpoint)?;
        let mut channels = HashMap::new();
        channels.insert(
            MetadataChannelKey {
                group_id: 0,
                endpoint: endpoint.clone(),
            },
            channel,
        );
        Ok(Self {
            default_endpoint: endpoint,
            channels: Arc::new(parking_lot::RwLock::new(channels)),
            channel_pool_enabled,
            max_channels_per_group: max_channels_per_group.max(1),
            metrics,
        })
    }

    async fn client(
        &self,
        ctx: &AttemptContext,
        operation: &'static str,
    ) -> ClientResult<FileSystemServiceProtoClient<tonic_net::Channel>> {
        let endpoint = ctx
            .metadata_endpoint()
            .map(normalize_endpoint)
            .unwrap_or_else(|| self.default_endpoint.clone());
        let group_id = ctx.metadata_header()?.group_id;
        let key = MetadataChannelKey { group_id, endpoint };
        if !self.channel_pool_enabled {
            self.record_pool_metric(ClientMetric::MetadataChannelPoolMiss, operation, "miss");
            return lazy_channel(&key.endpoint)
                .map(FileSystemServiceProtoClient::new)
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
                self.record_pool_metric(ClientMetric::MetadataChannelPoolHit, operation, "hit");
                channel
            }
            None => {
                self.record_pool_metric(ClientMetric::MetadataChannelPoolMiss, operation, "miss");
                self.create_metadata_channel(key, operation).await?
            }
        };
        Ok(FileSystemServiceProtoClient::new(channel))
    }

    async fn create_metadata_channel(
        &self,
        key: MetadataChannelKey,
        operation: &'static str,
    ) -> ClientResult<tonic_net::Channel> {
        if let Some(channel) = self.channels.read().get(&key).cloned() {
            self.record_pool_metric(ClientMetric::MetadataChannelPoolHit, operation, "hit");
            return Ok(channel);
        }
        let channel = lazy_channel(&key.endpoint).inspect_err(|_err| {
            self.record_pool_metric(ClientMetric::ChannelPoolConnectError, operation, "error");
        })?;
        Ok(self.insert_metadata_channel(key, channel))
    }

    fn insert_metadata_channel(&self, key: MetadataChannelKey, channel: tonic_net::Channel) -> tonic_net::Channel {
        let mut channels = self.channels.write();
        if let Some(existing) = channels.get(&key).cloned() {
            return existing;
        }
        evict_metadata_channel_if_needed(&mut channels, &key, self.max_channels_per_group);
        channels.insert(key, channel.clone());
        channel
    }

    fn record_pool_metric(&self, metric: ClientMetric, operation: &'static str, outcome: &'static str) {
        self.metrics.record(ClientMetricEvent::new(
            metric,
            ClientMetricLabels::default()
                .with_cache("channel_pool")
                .with_target_plane("metadata")
                .with_operation_name(operation)
                .with_outcome(outcome),
        ));
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct MetadataChannelKey {
    group_id: u64,
    endpoint: String,
}

fn evict_metadata_channel_if_needed(
    channels: &mut HashMap<MetadataChannelKey, tonic_net::Channel>,
    key: &MetadataChannelKey,
    max_per_group: usize,
) {
    if channels.contains_key(key) {
        return;
    }
    let count = channels
        .keys()
        .filter(|existing| existing.group_id == key.group_id)
        .count();
    if count < max_per_group {
        return;
    }
    if let Some(evicted) = channels
        .keys()
        .filter(|existing| existing.group_id == key.group_id)
        .min()
        .cloned()
    {
        channels.remove(&evicted);
    }
}

#[async_trait]
impl MetadataGateway for TonicMetadataGateway {
    async fn get_status(&self, ctx: AttemptContext, mut req: GetStatusOp) -> ClientResult<StatusSnapshot> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "read")
            .await?
            .get_status(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn list_status(&self, ctx: AttemptContext, mut req: ListStatusOp) -> ClientResult<ListSnapshot> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "read")
            .await?
            .list_status(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn delete(&self, ctx: AttemptContext, mut req: DeleteOp) -> ClientResult<DeleteResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")
            .await?
            .delete(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn rename(&self, ctx: AttemptContext, mut req: RenameOp) -> ClientResult<RenameResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")
            .await?
            .rename(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn open_file(&self, ctx: AttemptContext, mut req: OpenFileOp) -> ClientResult<FileSnapshot> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "read")
            .await?
            .open_file(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn read_layout(&self, ctx: AttemptContext, mut req: GetBlockLocationsOp) -> ClientResult<LayoutSnapshot> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "read")
            .await?
            .get_block_locations(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let group_id = parse_metadata_response_header(&ctx, response.header.as_ref())?;
        LayoutSnapshot::from_proto(group_id, response)
    }

    async fn create_file(&self, ctx: AttemptContext, mut req: CreateFileOp) -> ClientResult<WriteSessionSeed> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")
            .await?
            .create_file(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(WriteSessionSeed::Create(response))
    }

    async fn append_file(&self, ctx: AttemptContext, mut req: AppendFileOp) -> ClientResult<WriteSessionSeed> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")
            .await?
            .append_file(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(WriteSessionSeed::Append(response))
    }

    async fn add_block(&self, ctx: AttemptContext, mut req: AddBlockOp) -> ClientResult<AddBlockResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")
            .await?
            .add_block(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let group_id = parse_metadata_response_header(&ctx, response.header.as_ref())?;
        let target = response
            .target
            .ok_or_else(|| side_effect_response_body_mismatch("AddBlock", "missing target"))?;
        let target = target
            .try_into()
            .map_err(|err| side_effect_response_body_mismatch("AddBlock", err))?;
        Ok(AddBlockResult { group_id, target })
    }

    async fn commit_file(&self, ctx: AttemptContext, mut req: CommitFileOp) -> ClientResult<CommitFileResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")
            .await?
            .commit_file(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn abort_file_write(
        &self,
        ctx: AttemptContext,
        mut req: AbortFileWriteOp,
    ) -> ClientResult<AbortFileWriteResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")
            .await?
            .abort_file_write(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn renew_lease(&self, ctx: AttemptContext, mut req: RenewLeaseOp) -> ClientResult<RenewLeaseResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")
            .await?
            .renew_lease(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn sync_write(&self, ctx: AttemptContext, mut req: SyncWriteOp) -> ClientResult<SyncWriteResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")
            .await?
            .sync_write(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn msync(&self, ctx: AttemptContext, mut req: MsyncOp) -> ClientResult<StateWatermark> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "refresh")
            .await?
            .msync(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        response
            .state
            .ok_or_else(|| ClientError::Metadata("MsyncResponseProto missing state".to_string()))
    }
}

fn parse_metadata_response_header(
    ctx: &AttemptContext,
    header: Option<&proto::common::ResponseHeaderProto>,
) -> ClientResult<u64> {
    let Some(header) = header else {
        return Err(ClientError::from(invalid_header_action(
            "metadata OK response missing ResponseHeader",
        )));
    };
    if header.group_id == 0 {
        return Err(ClientError::from(invalid_header_action(
            "metadata OK response invalid ResponseHeader: group_id must be non-zero",
        )));
    }
    validate_metadata_response_identity(ctx, header)?;
    let header = ResponseHeader::try_from(header.clone()).map_err(|err| {
        ClientError::from(invalid_header_action(format!(
            "metadata OK response invalid ResponseHeader: {err}"
        )))
    })?;
    validate_header_or_action(&header).map_err(ClientError::from)?;
    header.group_id.ok_or_else(|| {
        ClientError::from(invalid_header_action(
            "metadata OK response invalid ResponseHeader: group_id missing",
        ))
    })
}

fn validate_metadata_response_identity(
    ctx: &AttemptContext,
    header: &proto::common::ResponseHeaderProto,
) -> ClientResult<()> {
    if header.group_id != 0 {
        if let Some(request_group_id) = ctx.group_id() {
            if header.group_id != request_group_id {
                return Err(ClientError::from(invalid_header_action(format!(
                    "metadata OK response invalid ResponseHeader: group_id mismatch: expected {}, got {}",
                    request_group_id, header.group_id
                ))));
            }
        }
    }
    let client = header.client.as_ref().ok_or_else(|| {
        ClientError::from(invalid_header_action(
            "metadata OK response invalid ResponseHeader: missing client identity",
        ))
    })?;
    if client.client_id == 0 {
        return Err(ClientError::from(invalid_header_action(
            "metadata OK response invalid ResponseHeader: client_id must be non-zero",
        )));
    }
    if client.client_id != ctx.client_id().as_raw() {
        return Err(ClientError::from(invalid_header_action(format!(
            "metadata OK response invalid ResponseHeader: client_id mismatch: expected {}, got {}",
            ctx.client_id().as_raw(),
            client.client_id
        ))));
    }
    if client.call_id.is_empty() {
        return Err(ClientError::from(invalid_header_action(
            "metadata OK response invalid ResponseHeader: call_id must not be empty",
        )));
    }
    if client.call_id != ctx.call_id() {
        return Err(ClientError::from(invalid_header_action(
            "metadata OK response invalid ResponseHeader: call_id mismatch",
        )));
    }
    Ok(())
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

fn lazy_channel(endpoint: &str) -> ClientResult<tonic_net::Channel> {
    tonic_net::Endpoint::from_shared(endpoint.to_string())
        .map_err(|err| ClientError::Metadata(format!("invalid metadata endpoint {endpoint}: {err}")))
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
    use crate::canonical::ClientAction;
    use crate::runtime::{ErrorClass, ErrorClassifier, OperationContext, OperationIdentity, OperationKind};
    use common::error::canonical::{CanonicalError, RefreshHint as CanonicalRefreshHint, RefreshReason};
    use common::header::RpcErrorCode as HeaderRpcErrorCode;
    use common::header::RpcErrorCode;
    use proto::convert::canonical_to_error_detail;
    use std::sync::Mutex;
    use types::ClientId;

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

    #[tokio::test]
    async fn metadata_channel_pool_reuses_channel_for_same_group_endpoint() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway =
            TonicMetadataGateway::new_lazy_with_pool("127.0.0.1:18080", true, 1, metrics.clone()).expect("gateway");
        let ctx = metadata_attempt(9, None);

        let _first = gateway.client(&ctx, "read").await.expect("first client");
        let _second = gateway.client(&ctx, "read").await.expect("second client");

        let events = metrics.events();
        assert_metric(&events, ClientMetric::MetadataChannelPoolMiss);
        assert_metric(&events, ClientMetric::MetadataChannelPoolHit);
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
    }

    #[tokio::test]
    async fn concurrent_metadata_channel_requests_same_key_reuse_inserted_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway = Arc::new(
            TonicMetadataGateway::new_lazy_with_pool("127.0.0.1:18080", true, 8, metrics.clone()).expect("gateway"),
        );
        let ctx = metadata_attempt(9, None);

        let mut tasks = Vec::with_capacity(8);
        for _ in 0..8 {
            let gateway = Arc::clone(&gateway);
            let ctx = ctx.clone();
            tasks.push(tokio::spawn(async move { gateway.client(&ctx, "read").await }));
        }

        for task in tasks {
            let _client = task.await.expect("task").expect("metadata client");
        }
        let events = metrics.events();
        assert_eq!(gateway.channels.read().len(), 2);
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
    }

    #[tokio::test]
    async fn failed_metadata_channel_creation_does_not_insert() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway = Arc::new(
            TonicMetadataGateway::new_lazy_with_pool("127.0.0.1:18080", true, 8, metrics.clone()).expect("gateway"),
        );
        let ctx = metadata_attempt(9, Some("http://[invalid"));

        let mut tasks = Vec::with_capacity(4);
        for _ in 0..4 {
            let gateway = Arc::clone(&gateway);
            let ctx = ctx.clone();
            tasks.push(tokio::spawn(async move { gateway.client(&ctx, "read").await }));
        }

        for task in tasks {
            let err = task.await.expect("task").expect_err("invalid endpoint");
            assert!(matches!(err, ClientError::Metadata(msg) if msg.contains("invalid metadata endpoint")));
        }
        assert_eq!(gateway.channels.read().len(), 1);
        assert_metric(&metrics.events(), ClientMetric::ChannelPoolConnectError);
    }

    #[tokio::test]
    async fn disabled_metadata_channel_pool_does_not_reuse_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway =
            TonicMetadataGateway::new_lazy_with_pool("127.0.0.1:18080", false, 1, metrics.clone()).expect("gateway");
        let ctx = metadata_attempt(9, None);

        let _first = gateway.client(&ctx, "read").await.expect("first client");
        let _second = gateway.client(&ctx, "read").await.expect("second client");

        let events = metrics.events();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.metric == ClientMetric::MetadataChannelPoolMiss)
                .count(),
            2
        );
        assert!(events
            .iter()
            .all(|event| event.metric != ClientMetric::MetadataChannelPoolHit));
    }

    #[tokio::test]
    async fn metadata_channel_pool_connection_error_is_reported() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway =
            TonicMetadataGateway::new_lazy_with_pool("127.0.0.1:18080", true, 1, metrics.clone()).expect("gateway");
        let ctx = metadata_attempt(9, Some("http://[invalid"));

        let err = gateway.client(&ctx, "read").await.expect_err("invalid endpoint fails");

        assert!(matches!(err, ClientError::Metadata(msg) if msg.contains("invalid metadata endpoint")));
        assert_metric(&metrics.events(), ClientMetric::ChannelPoolConnectError);
    }

    #[test]
    fn metadata_response_header_preserves_need_refresh_hints() {
        let ctx = metadata_attempt(17, None);
        let canonical = CanonicalError::need_refresh_with_hint(
            RpcErrorCode::ShardMoved,
            RefreshReason::RouteEpochMismatch,
            CanonicalRefreshHint {
                leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                group_id: Some(17),
                route_epoch: Some(23),
                mount_epoch: Some(31),
                mount_prefix: Some("/mnt".to_string()),
                worker_resolve_required: true,
                ..CanonicalRefreshHint::default()
            },
            "route moved",
        );
        let header = proto::common::ResponseHeaderProto {
            client: Some(ctx.client_info()),
            error: Some(canonical_to_error_detail(&canonical)),
            state: Vec::new(),
            group_id: 17,
            mount_epoch: Some(31),
            route_epoch: Some(23),
        };

        let err = parse_metadata_response_header(&ctx, Some(&header)).expect_err("need refresh must be surfaced");
        match action(&err) {
            ClientAction::Refresh { reason, hint, .. } => {
                assert_eq!(*reason, RefreshReason::RouteEpochMismatch);
                assert_eq!(hint.leader_endpoint.as_deref(), Some("http://127.0.0.1:18081"));
                assert_eq!(hint.group_id, Some(17));
                assert_eq!(hint.route_epoch, Some(23));
                assert_eq!(hint.mount_epoch, Some(31));
                assert_eq!(hint.mount_prefix.as_deref(), Some("/mnt"));
                assert!(hint.worker_resolve_required);
            }
            other => panic!("expected refresh action, got {other:?}"),
        }
    }

    #[test]
    fn missing_metadata_response_header_is_invalid_header_action() {
        let ctx = metadata_attempt(9, None);
        let err = parse_metadata_response_header(&ctx, None).expect_err("missing response header must fail");

        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        match action(&err) {
            ClientAction::Fail { canonical } => {
                assert_eq!(canonical.class, common::error::canonical::ErrorClass::Fatal);
                assert!(matches!(
                    canonical.code,
                    Some(common::error::canonical::ErrorCode::RpcCode(
                        HeaderRpcErrorCode::InvalidHeader
                    ))
                ));
                assert!(canonical.message.contains("missing ResponseHeader"));
            }
            other => panic!("expected invalid header Fail action, got {other:?}"),
        }
    }

    #[test]
    fn malformed_metadata_response_header_is_invalid_header_action() {
        let ctx = metadata_attempt(9, None);
        let malformed = proto::common::ResponseHeaderProto::default();

        let err =
            parse_metadata_response_header(&ctx, Some(&malformed)).expect_err("malformed response header must fail");

        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        match action(&err) {
            ClientAction::Fail { canonical } => {
                assert_eq!(canonical.class, common::error::canonical::ErrorClass::Fatal);
                assert!(matches!(
                    canonical.code,
                    Some(common::error::canonical::ErrorCode::RpcCode(
                        HeaderRpcErrorCode::InvalidHeader
                    ))
                ));
                assert!(canonical.message.contains("invalid ResponseHeader"));
            }
            other => panic!("expected invalid header Fail action, got {other:?}"),
        }
    }

    #[test]
    fn metadata_response_header_with_zero_group_id_is_invalid_header_action() {
        const INVALID_GROUP_ID: u64 = 0;
        let ctx = metadata_attempt(9, None);
        let header = proto::common::ResponseHeaderProto {
            client: Some(proto::common::ClientInfoProto {
                call_id: types::CallId::new().to_string(),
                client_id: 7,
                client_name: "test".to_string(),
            }),
            error: None,
            state: Vec::new(),
            group_id: INVALID_GROUP_ID,
            mount_epoch: None,
            route_epoch: None,
        };

        let err = parse_metadata_response_header(&ctx, Some(&header)).expect_err("zero group_id must fail");

        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        match action(&err) {
            ClientAction::Fail { canonical } => {
                assert_eq!(canonical.class, common::error::canonical::ErrorClass::Fatal);
                assert!(matches!(
                    canonical.code,
                    Some(common::error::canonical::ErrorCode::RpcCode(
                        HeaderRpcErrorCode::InvalidHeader
                    ))
                ));
                assert!(canonical.message.contains("group_id"));
            }
            other => panic!("expected invalid header Fail action, got {other:?}"),
        }
    }

    #[test]
    fn metadata_response_header_with_wrong_call_id_is_invalid_header_action() {
        let ctx = metadata_attempt(9, None);
        let mut header = ok_metadata_header(&ctx);
        header.client.as_mut().expect("client").call_id = types::CallId::new().to_string();

        let err = parse_metadata_response_header(&ctx, Some(&header)).expect_err("wrong call_id must fail");

        assert_invalid_metadata_header(&err, "call_id");
    }

    #[test]
    fn metadata_response_header_with_wrong_client_id_is_invalid_header_action() {
        let ctx = metadata_attempt(9, None);
        let mut header = ok_metadata_header(&ctx);
        header.client.as_mut().expect("client").client_id = ctx.client_id().as_raw() + 1;

        let err = parse_metadata_response_header(&ctx, Some(&header)).expect_err("wrong client_id must fail");

        assert_invalid_metadata_header(&err, "client_id");
    }

    #[test]
    fn metadata_response_header_with_wrong_group_id_is_invalid_header_action() {
        let ctx = metadata_attempt(9, None);
        let mut header = ok_metadata_header(&ctx);
        header.group_id = 11;

        let err = parse_metadata_response_header(&ctx, Some(&header)).expect_err("wrong group_id must fail");

        assert_invalid_metadata_header(&err, "group_id");
    }

    #[test]
    fn metadata_response_header_with_missing_client_identity_is_invalid_header_action() {
        let ctx = metadata_attempt(9, None);
        let mut header = ok_metadata_header(&ctx);
        header.client = None;

        let err = parse_metadata_response_header(&ctx, Some(&header)).expect_err("missing client must fail");

        assert_invalid_metadata_header(&err, "client identity");
    }

    #[test]
    fn metadata_response_header_with_empty_call_id_is_invalid_header_action() {
        let ctx = metadata_attempt(9, None);
        let mut header = ok_metadata_header(&ctx);
        header.client.as_mut().expect("client").call_id.clear();

        let err = parse_metadata_response_header(&ctx, Some(&header)).expect_err("empty call_id must fail");

        assert_invalid_metadata_header(&err, "call_id");
    }

    fn action(err: &ClientError) -> &ClientAction {
        match err {
            ClientError::Action(action) => action.as_ref(),
            other => panic!("expected action error, got {other:?}"),
        }
    }

    fn assert_invalid_metadata_header(err: &ClientError, message_fragment: &str) {
        assert_eq!(ErrorClassifier.classify_error(err), ErrorClass::InvalidHeader);
        match action(err) {
            ClientAction::Fail { canonical } => {
                assert!(matches!(
                    canonical.code,
                    Some(common::error::canonical::ErrorCode::RpcCode(
                        HeaderRpcErrorCode::InvalidHeader
                    ))
                ));
                assert!(
                    canonical.message.contains(message_fragment),
                    "expected {message_fragment:?} in {:?}",
                    canonical.message
                );
            }
            other => panic!("expected invalid header Fail action, got {other:?}"),
        }
    }

    fn assert_metric(events: &[ClientMetricEvent], metric: ClientMetric) {
        assert!(
            events.iter().any(|event| event.metric == metric),
            "missing metric {metric:?}: {events:?}"
        );
    }

    fn metadata_attempt(group_id: u64, endpoint: Option<&str>) -> AttemptContext {
        let operation = OperationContext::new(
            ClientId::new(7),
            OperationKind::MetadataRead,
            "GetStatus",
            OperationIdentity::path("/alpha"),
        )
        .expect("operation");
        let ctx = AttemptContext::for_metadata(&operation, group_id, 0).expect("attempt");
        if let Some(endpoint) = endpoint {
            ctx.with_metadata_endpoint(endpoint.to_string())
        } else {
            ctx
        }
    }

    fn ok_metadata_header(ctx: &AttemptContext) -> proto::common::ResponseHeaderProto {
        let request = ctx.metadata_header().expect("metadata request header");
        proto::common::ResponseHeaderProto {
            client: request.client,
            error: None,
            state: Vec::new(),
            group_id: request.group_id,
            mount_epoch: request.mount_epoch,
            route_epoch: request.route_epoch,
        }
    }
}
