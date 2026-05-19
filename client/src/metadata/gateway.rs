// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataGateway trait and tonic implementation.

use async_trait::async_trait;
use common::header::ResponseHeader;
use proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport as tonic_net;

use crate::canonical::{invalid_header_action, validate_header_or_action};
use crate::config::ClientConfig;
use crate::error::{side_effect_response_body_mismatch, ClientError, ClientResult};
use crate::metadata::header::ensure_metadata_header;
use crate::metadata::ops::{
    AbortFileWriteOp, AddBlockOp, AppendFileOp, CommitFileOp, CreateFileOp, DeleteOp, GetBlockLocationsOp, GetStatusOp,
    ListStatusOp, MsyncOp, OpenFileOp, RenameOp, RenewLeaseOp,
};
use crate::metadata::snapshot::{
    AbortFileWriteResult, AddBlockResult, CommitFileResult, DeleteResult, FileSnapshot, LayoutSnapshot, ListSnapshot,
    RenameResult, RenewLeaseResult, StateWatermark, StatusSnapshot, WriteSessionSeed,
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
        Self::new_lazy_with_pool(
            endpoint,
            config.channel_pool.metadata_channel_pool_enabled,
            config.channel_pool.metadata_channel_pool_max_per_group,
            metrics,
        )
    }

    fn new_lazy_with_pool(
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

    fn client(
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
                let channel = lazy_channel(&key.endpoint).inspect_err(|_err| {
                    self.record_pool_metric(ClientMetric::ChannelPoolConnectError, operation, "error");
                })?;
                let mut channels = self.channels.write();
                evict_metadata_channel_if_needed(&mut channels, &key, self.max_channels_per_group);
                channels.insert(key, channel.clone());
                channel
            }
        };
        Ok(FileSystemServiceProtoClient::new(channel))
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
            .client(&ctx, "read")?
            .get_status(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(response)
    }

    async fn list_status(&self, ctx: AttemptContext, mut req: ListStatusOp) -> ClientResult<ListSnapshot> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "read")?
            .list_status(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(response)
    }

    async fn delete(&self, ctx: AttemptContext, mut req: DeleteOp) -> ClientResult<DeleteResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")?
            .delete(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(response)
    }

    async fn rename(&self, ctx: AttemptContext, mut req: RenameOp) -> ClientResult<RenameResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")?
            .rename(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(response)
    }

    async fn open_file(&self, ctx: AttemptContext, mut req: OpenFileOp) -> ClientResult<FileSnapshot> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "read")?
            .open_file(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(response)
    }

    async fn read_layout(&self, ctx: AttemptContext, mut req: GetBlockLocationsOp) -> ClientResult<LayoutSnapshot> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "read")?
            .get_block_locations(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(response)
    }

    async fn create_file(&self, ctx: AttemptContext, mut req: CreateFileOp) -> ClientResult<WriteSessionSeed> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")?
            .create_file(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(WriteSessionSeed::Create(response))
    }

    async fn append_file(&self, ctx: AttemptContext, mut req: AppendFileOp) -> ClientResult<WriteSessionSeed> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")?
            .append_file(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(WriteSessionSeed::Append(response))
    }

    async fn add_block(&self, ctx: AttemptContext, mut req: AddBlockOp) -> ClientResult<AddBlockResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")?
            .add_block(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let group_id = parse_metadata_response_header(response.header.as_ref())?;
        let target = response
            .target
            .ok_or_else(|| side_effect_response_body_mismatch("AddBlock", "missing target"))?;
        Ok(AddBlockResult { group_id, target })
    }

    async fn commit_file(&self, ctx: AttemptContext, mut req: CommitFileOp) -> ClientResult<CommitFileResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")?
            .commit_file(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(response)
    }

    async fn abort_file_write(
        &self,
        ctx: AttemptContext,
        mut req: AbortFileWriteOp,
    ) -> ClientResult<AbortFileWriteResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")?
            .abort_file_write(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(response)
    }

    async fn renew_lease(&self, ctx: AttemptContext, mut req: RenewLeaseOp) -> ClientResult<RenewLeaseResult> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "write")?
            .renew_lease(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        Ok(response)
    }

    async fn msync(&self, ctx: AttemptContext, mut req: MsyncOp) -> ClientResult<StateWatermark> {
        ensure_metadata_header(&mut req.header, &ctx)?;
        let response = self
            .client(&ctx, "refresh")?
            .msync(tonic::Request::new(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(response.header.as_ref())?;
        response
            .state
            .ok_or_else(|| ClientError::Metadata("MsyncResponseProto missing state".to_string()))
    }
}

fn parse_metadata_response_header(header: Option<&proto::common::ResponseHeaderProto>) -> ClientResult<u64> {
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

        let _first = gateway.client(&ctx, "read").expect("first client");
        let _second = gateway.client(&ctx, "read").expect("second client");

        let events = metrics.events();
        assert_metric(&events, ClientMetric::MetadataChannelPoolMiss);
        assert_metric(&events, ClientMetric::MetadataChannelPoolHit);
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
    }

    #[tokio::test]
    async fn disabled_metadata_channel_pool_does_not_reuse_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway =
            TonicMetadataGateway::new_lazy_with_pool("127.0.0.1:18080", false, 1, metrics.clone()).expect("gateway");
        let ctx = metadata_attempt(9, None);

        let _first = gateway.client(&ctx, "read").expect("first client");
        let _second = gateway.client(&ctx, "read").expect("second client");

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

        let err = gateway.client(&ctx, "read").expect_err("invalid endpoint fails");

        assert!(matches!(err, ClientError::Metadata(msg) if msg.contains("invalid metadata endpoint")));
        assert_metric(&metrics.events(), ClientMetric::ChannelPoolConnectError);
    }

    #[test]
    fn metadata_response_header_preserves_need_refresh_hints() {
        let canonical = CanonicalError::need_refresh_with_hint(
            RpcErrorCode::ShardMoved,
            RefreshReason::RouteEpochMismatch,
            CanonicalRefreshHint {
                leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                group_id: Some(17),
                route_epoch: Some(23),
                mount_epoch: Some(31),
                mount_prefix: Some("/mnt".to_string()),
                worker_epoch: Some(47),
                worker_resolve_required: true,
                ..CanonicalRefreshHint::default()
            },
            "route moved",
        );
        let header = proto::common::ResponseHeaderProto {
            client: Some(proto::common::ClientInfoProto {
                call_id: types::CallId::new().to_string(),
                client_id: 7,
                client_name: String::new(),
            }),
            error: Some(canonical_to_error_detail(&canonical)),
            state: Vec::new(),
            group_id: 17,
            mount_epoch: Some(31),
            route_epoch: Some(23),
        };

        let err = parse_metadata_response_header(Some(&header)).expect_err("need refresh must be surfaced");
        match action(&err) {
            ClientAction::Refresh { reason, hint, .. } => {
                assert_eq!(*reason, RefreshReason::RouteEpochMismatch);
                assert_eq!(hint.leader_endpoint.as_deref(), Some("http://127.0.0.1:18081"));
                assert_eq!(hint.group_id, Some(17));
                assert_eq!(hint.route_epoch, Some(23));
                assert_eq!(hint.mount_epoch, Some(31));
                assert_eq!(hint.mount_prefix.as_deref(), Some("/mnt"));
                assert_eq!(hint.worker_epoch, Some(47));
                assert!(hint.worker_resolve_required);
            }
            other => panic!("expected refresh action, got {other:?}"),
        }
    }

    #[test]
    fn missing_metadata_response_header_is_invalid_header_action() {
        let err = parse_metadata_response_header(None).expect_err("missing response header must fail");

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
        let malformed = proto::common::ResponseHeaderProto::default();

        let err = parse_metadata_response_header(Some(&malformed)).expect_err("malformed response header must fail");

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

        let err = parse_metadata_response_header(Some(&header)).expect_err("zero group_id must fail");

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

    fn action(err: &ClientError) -> &ClientAction {
        match err {
            ClientError::Action(action) => action.as_ref(),
            other => panic!("expected action error, got {other:?}"),
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
}
