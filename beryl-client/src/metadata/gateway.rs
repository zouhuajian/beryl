// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! MetadataGateway trait and tonic implementation.

use async_trait::async_trait;
use beryl_common::header::{HeaderIdentity, ResponseHeader};
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_types::GroupName;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport as tonic_net;

use crate::config::ClientConfig;
use crate::error::{side_effect_response_body_mismatch, ClientError, ClientResult};
use crate::metadata::model::{AddBlockResult, ReadLayout};
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics};
use crate::rpc_error::{invalid_header_action, validate_header_or_action};
use crate::runtime::AttemptContext;

/// Client-owned metadata control-plane adapter.
#[async_trait]
pub(crate) trait MetadataGateway: Send + Sync {
    /// Get file or directory status.
    async fn get_status(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::GetStatusRequestProto,
    ) -> ClientResult<beryl_proto::metadata::GetStatusResponseProto>;

    /// List directory status.
    async fn list_status(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::ListStatusRequestProto,
    ) -> ClientResult<beryl_proto::metadata::ListStatusResponseProto>;

    /// Create a directory.
    async fn create_directory(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::CreateDirectoryRequestProto,
    ) -> ClientResult<beryl_proto::metadata::CreateDirectoryResponseProto>;

    /// Delete a namespace entry.
    async fn delete(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::DeleteRequestProto,
    ) -> ClientResult<beryl_proto::metadata::DeleteResponseProto>;

    /// Rename a namespace entry.
    async fn rename(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::RenameRequestProto,
    ) -> ClientResult<beryl_proto::metadata::RenameResponseProto>;

    /// Open a file for read planning.
    async fn open_file(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::OpenFileRequestProto,
    ) -> ClientResult<beryl_proto::metadata::OpenFileResponseProto>;

    /// Get the file data layout for a public read.
    async fn read_layout(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::GetBlockLocationsRequestProto,
    ) -> ClientResult<ReadLayout>;

    /// Apply the durable CreateFile namespace mutation.
    async fn create_file(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::CreateFileRequestProto,
    ) -> ClientResult<beryl_proto::metadata::CreateFileResponseProto>;

    /// Open a leader-local write session.
    async fn open_write(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::OpenWriteRequestProto,
    ) -> ClientResult<beryl_proto::metadata::OpenWriteResponseProto>;

    /// Allocate a worker write target for a write session.
    async fn add_block(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::AddBlockRequestProto,
    ) -> ClientResult<AddBlockResult>;

    /// Commit a write session after worker data commit succeeds.
    async fn commit_file(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::CommitFileRequestProto,
    ) -> ClientResult<beryl_proto::metadata::CommitFileResponseProto>;

    /// Abort a write session best effort.
    async fn abort_file_write(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::AbortFileWriteRequestProto,
    ) -> ClientResult<beryl_proto::metadata::AbortFileWriteResponseProto>;

    /// Renew an active write session lease.
    async fn renew_lease(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::RenewLeaseRequestProto,
    ) -> ClientResult<beryl_proto::metadata::RenewLeaseResponseProto>;

    /// Apply a write-session visibility or durability barrier.
    async fn sync_write(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::SyncWriteRequestProto,
    ) -> ClientResult<beryl_proto::metadata::SyncWriteResponseProto>;

    /// Synchronize metadata state freshness.
    async fn msync(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::MsyncRequestProto,
    ) -> ClientResult<beryl_proto::common::GroupStateWatermarkProto>;
}

/// Tonic-backed metadata gateway.
#[derive(Clone, Debug)]
pub(crate) struct GrpcMetadataGateway {
    channels: Arc<parking_lot::RwLock<HashMap<MetadataChannelKey, tonic_net::Channel>>>,
    channel_pool_enabled: bool,
    max_channels_per_group: usize,
    metrics: Arc<dyn ClientMetrics>,
}

impl GrpcMetadataGateway {
    /// Create a lazily connecting metadata gateway from client config.
    pub(crate) fn new_lazy_with_config(config: &ClientConfig, metrics: Arc<dyn ClientMetrics>) -> ClientResult<Self> {
        Self::new_lazy_with_pool_options(
            config.connections.metadata_enabled,
            config.connections.metadata_max_per_group,
            metrics,
        )
    }

    fn new_lazy_with_pool_options(
        channel_pool_enabled: bool,
        max_channels_per_group: usize,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        Ok(Self {
            channels: Arc::new(parking_lot::RwLock::new(HashMap::new())),
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
        let endpoint = ctx.metadata_endpoint().map(normalize_endpoint).ok_or_else(|| {
            ClientError::InvalidArgument("metadata AttemptContext missing metadata_endpoint".to_string())
        })?;
        let group_name = ctx
            .group_name()
            .cloned()
            .ok_or_else(|| ClientError::InvalidArgument("metadata AttemptContext missing group_name".to_string()))?;
        let key = MetadataChannelKey { group_name, endpoint };
        if !self.channel_pool_enabled {
            self.record_pool_metric(ClientMetric::MetadataChannelPoolMiss, operation, "miss");
            return lazy_channel(&key.endpoint)
                .map(FileSystemServiceProtoClient::new)
                .inspect_err(|_err| {
                    self.record_pool_metric(ClientMetric::ChannelBuildError, operation, "error");
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
            self.record_pool_metric(ClientMetric::ChannelBuildError, operation, "error");
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
    group_name: GroupName,
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
        .filter(|existing| existing.group_name == key.group_name)
        .count();
    if count < max_per_group {
        return;
    }
    if let Some(evicted) = channels
        .keys()
        .filter(|existing| existing.group_name == key.group_name)
        .min()
        .cloned()
    {
        channels.remove(&evicted);
    }
}

#[async_trait]
impl MetadataGateway for GrpcMetadataGateway {
    async fn get_status(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::GetStatusRequestProto,
    ) -> ClientResult<beryl_proto::metadata::GetStatusResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
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

    async fn list_status(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::ListStatusRequestProto,
    ) -> ClientResult<beryl_proto::metadata::ListStatusResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
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

    async fn delete(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::DeleteRequestProto,
    ) -> ClientResult<beryl_proto::metadata::DeleteResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
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

    async fn create_directory(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::CreateDirectoryRequestProto,
    ) -> ClientResult<beryl_proto::metadata::CreateDirectoryResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .create_directory(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn rename(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::RenameRequestProto,
    ) -> ClientResult<beryl_proto::metadata::RenameResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
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

    async fn open_file(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::OpenFileRequestProto,
    ) -> ClientResult<beryl_proto::metadata::OpenFileResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
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

    async fn read_layout(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::GetBlockLocationsRequestProto,
    ) -> ClientResult<ReadLayout> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "read")
            .await?
            .get_block_locations(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let group_name = parse_metadata_response_header(&ctx, response.header.as_ref())?;
        ReadLayout::from_get_block_locations_response(group_name, response)
    }

    async fn create_file(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::CreateFileRequestProto,
    ) -> ClientResult<beryl_proto::metadata::CreateFileResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .create_file(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn open_write(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::OpenWriteRequestProto,
    ) -> ClientResult<beryl_proto::metadata::OpenWriteResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .open_write(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        parse_metadata_response_header(&ctx, response.header.as_ref())?;
        Ok(response)
    }

    async fn add_block(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::AddBlockRequestProto,
    ) -> ClientResult<AddBlockResult> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .add_block(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let group_name = parse_metadata_response_header(&ctx, response.header.as_ref())?;
        let target = response
            .target
            .ok_or_else(|| side_effect_response_body_mismatch("AddBlock", "missing target"))?;
        let target = target
            .try_into()
            .map_err(|err| side_effect_response_body_mismatch("AddBlock", err))?;
        Ok(AddBlockResult { group_name, target })
    }

    async fn commit_file(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::CommitFileRequestProto,
    ) -> ClientResult<beryl_proto::metadata::CommitFileResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
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
        mut req: beryl_proto::metadata::AbortFileWriteRequestProto,
    ) -> ClientResult<beryl_proto::metadata::AbortFileWriteResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
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

    async fn renew_lease(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::RenewLeaseRequestProto,
    ) -> ClientResult<beryl_proto::metadata::RenewLeaseResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
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

    async fn sync_write(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::SyncWriteRequestProto,
    ) -> ClientResult<beryl_proto::metadata::SyncWriteResponseProto> {
        req.header = Some(build_metadata_header(&ctx)?);
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

    async fn msync(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::MsyncRequestProto,
    ) -> ClientResult<beryl_proto::common::GroupStateWatermarkProto> {
        req.header = Some(build_metadata_header(&ctx)?);
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

fn build_metadata_header(ctx: &AttemptContext) -> ClientResult<beryl_proto::common::RequestHeaderProto> {
    ctx.metadata_header()
}

fn parse_metadata_response_header(
    ctx: &AttemptContext,
    header: Option<&beryl_proto::common::ResponseHeaderProto>,
) -> ClientResult<GroupName> {
    let Some(header) = header else {
        return Err(ClientError::from(invalid_header_action(
            "metadata OK response missing ResponseHeader",
        )));
    };
    let header = ResponseHeader::try_from(header.clone()).map_err(|err| {
        ClientError::from(invalid_header_action(format!(
            "metadata OK response invalid ResponseHeader: {err}"
        )))
    })?;
    let identity = HeaderIdentity {
        call_id: header.client.call_id,
        client_id: header.client.client_id,
        group_name: header.group_name.clone(),
    };
    let group_name = identity.group_name.clone().ok_or_else(|| {
        ClientError::from(invalid_header_action(
            "metadata OK response invalid ResponseHeader: group_name missing",
        ))
    })?;
    validate_metadata_response_identity(ctx, &identity)?;
    validate_header_or_action(&header).map_err(ClientError::from)?;
    Ok(group_name)
}

fn validate_metadata_response_identity(ctx: &AttemptContext, identity: &HeaderIdentity) -> ClientResult<()> {
    let request_identity = ctx.header_identity();
    if identity.matches_request(&request_identity) {
        return Ok(());
    }
    if let (Some(request_group_name), Some(response_group_name)) =
        (request_identity.group_name.as_ref(), identity.group_name.as_ref())
    {
        if response_group_name != request_group_name {
            return Err(ClientError::from(invalid_header_action(format!(
                "metadata OK response invalid ResponseHeader: group_name mismatch: expected {}, got {}",
                request_group_name, response_group_name
            ))));
        }
    }
    if identity.client_id != request_identity.client_id {
        return Err(ClientError::from(invalid_header_action(format!(
            "metadata OK response invalid ResponseHeader: client_id mismatch: expected {}, got {}",
            request_identity.client_id, identity.client_id
        ))));
    }
    if identity.call_id != request_identity.call_id {
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
    use crate::runtime::{OperationContext, OperationDeadline};
    use beryl_types::ClientId;
    use std::sync::Mutex;

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
    async fn concurrent_metadata_channel_requests_same_key_reuse_inserted_channel() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway =
            Arc::new(GrpcMetadataGateway::new_lazy_with_pool_options(true, 8, metrics.clone()).expect("gateway"));
        let ctx = metadata_attempt("root", Some("127.0.0.1:18080"));

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
        assert_eq!(gateway.channels.read().len(), 1);
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
    }

    fn metadata_attempt(group_name: &str, endpoint: Option<&str>) -> AttemptContext {
        let operation = OperationContext::new_named(
            ClientId::new(7),
            "test-client",
            "GetStatus",
            Some("/alpha".to_string()),
            OperationDeadline::new(5_000),
        )
        .expect("operation");
        let ctx = AttemptContext::for_metadata(&operation, GroupName::parse(group_name).unwrap(), 0).expect("attempt");
        if let Some(endpoint) = endpoint {
            ctx.with_metadata_endpoint(endpoint.to_string())
        } else {
            ctx
        }
    }
}
