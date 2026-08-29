// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata transport boundary and tonic implementation.

use async_trait::async_trait;
use beryl_common::error::rpc::{RecoveryAction, RefreshHint as RpcRefreshHint};
use beryl_common::header::{ClientInfo, ResponseHeader};
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_types::{GroupName, GroupStateWatermark};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport as tonic_net;

use crate::config::ClientConfig;
use crate::error::{side_effect_response_body_mismatch, ClientError, ClientResult};
use crate::metadata::model::{AddBlockResult, MetadataAuthorityUpdate, ReadLayout, ValidatedMetadataResponse};
use crate::metrics::{self, ClientMetric, ClientMetricLabels};
use crate::rpc_error::{invalid_header_error, validate_header};
use crate::runtime::AttemptContext;

/// Client-owned metadata control-plane adapter.
#[async_trait]
pub(crate) trait MetadataTransport: Send + Sync {
    /// Get file or directory status.
    async fn get_status(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::GetStatusRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::GetStatusResponseProto>>;

    /// List directory status.
    async fn list_status(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::ListStatusRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::ListStatusResponseProto>>;

    /// Create a directory.
    async fn create_directory(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::CreateDirectoryRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::CreateDirectoryResponseProto>>;

    /// Delete a namespace entry.
    async fn delete(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::DeleteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::DeleteResponseProto>>;

    /// Rename a namespace entry.
    async fn rename(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::RenameRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::RenameResponseProto>>;

    /// Open a file for read planning.
    async fn open_file(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::OpenFileRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::OpenFileResponseProto>>;

    /// Get the file data layout for a public read.
    async fn read_layout(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::GetBlockLocationsRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<ReadLayout>>;

    /// Apply the durable CreateFile namespace mutation.
    async fn create_file(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::CreateFileRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::CreateFileResponseProto>>;

    /// Open a leader-local write session.
    async fn open_write(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::OpenWriteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::OpenWriteResponseProto>>;

    /// Allocate a worker write target for a write session.
    async fn add_block(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::AddBlockRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<AddBlockResult>>;

    /// Commit a write session after worker data commit succeeds.
    async fn commit_file(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::CommitFileRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::CommitFileResponseProto>>;

    /// Abort a write session best effort.
    async fn abort_file_write(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::AbortFileWriteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::AbortFileWriteResponseProto>>;

    /// Renew an active write session lease.
    async fn renew_lease(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::RenewLeaseRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::RenewLeaseResponseProto>>;

    /// Apply a write-session visibility or durability barrier.
    async fn sync_write(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::SyncWriteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::SyncWriteResponseProto>>;

    /// Synchronize metadata freshness and include the returned watermark in
    /// the validated authority update.
    async fn msync(
        &self,
        ctx: AttemptContext,
        req: beryl_proto::metadata::MsyncRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::common::GroupStateWatermarkProto>>;
}

/// Tonic-backed Metadata transport for one selected-endpoint attempt.
#[derive(Clone, Debug)]
pub(crate) struct GrpcMetadataTransport {
    channels: Arc<parking_lot::RwLock<HashMap<MetadataChannelKey, tonic_net::Channel>>>,
    channel_pool_enabled: bool,
    max_channels_per_group: usize,
}

impl GrpcMetadataTransport {
    /// Creates a lazily connecting Metadata transport from sealed client configuration.
    pub(crate) fn new_lazy_with_config(config: &ClientConfig) -> ClientResult<Self> {
        Self::new_lazy_with_pool_options(config.metadata_connection_reuse(), config.metadata_connection_limit())
    }

    fn new_lazy_with_pool_options(channel_pool_enabled: bool, max_channels_per_group: usize) -> ClientResult<Self> {
        Ok(Self {
            channels: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            channel_pool_enabled,
            max_channels_per_group: max_channels_per_group.max(1),
        })
    }

    async fn client(
        &self,
        ctx: &AttemptContext,
        operation: &'static str,
    ) -> ClientResult<FileSystemServiceProtoClient<tonic_net::Channel>> {
        let endpoint = ctx.metadata_endpoint().map(normalize_endpoint).ok_or_else(|| {
            ClientError::invalid_argument("metadata AttemptContext missing metadata_endpoint".to_string())
        })?;
        let group_name = ctx
            .group_name()
            .cloned()
            .ok_or_else(|| ClientError::invalid_argument("metadata AttemptContext missing group_name".to_string()))?;
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
        metrics::record(
            metric,
            ClientMetricLabels::default()
                .with_cache("channel_pool")
                .with_target_plane("metadata")
                .with_operation_name(operation)
                .with_outcome(outcome),
        );
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
impl MetadataTransport for GrpcMetadataTransport {
    async fn get_status(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::GetStatusRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::GetStatusResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "read")
            .await?
            .get_status(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn list_status(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::ListStatusRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::ListStatusResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "read")
            .await?
            .list_status(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn create_directory(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::CreateDirectoryRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::CreateDirectoryResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .create_directory(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn delete(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::DeleteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::DeleteResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .delete(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn rename(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::RenameRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::RenameResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .rename(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn open_file(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::OpenFileRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::OpenFileResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "read")
            .await?
            .open_file(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn read_layout(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::GetBlockLocationsRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<ReadLayout>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "read")
            .await?
            .get_block_locations(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let authority = parse_metadata_response_header(&ctx, response.header.as_ref())?;
        let body = ReadLayout::from_get_block_locations_response(authority.group_name.clone(), response)?;
        Ok(ValidatedMetadataResponse::new(authority, body))
    }

    async fn create_file(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::CreateFileRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::CreateFileResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .create_file(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn open_write(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::OpenWriteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::OpenWriteResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .open_write(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn add_block(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::AddBlockRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<AddBlockResult>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .add_block(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let authority = parse_metadata_response_header(&ctx, response.header.as_ref())?;
        let target = response
            .target
            .ok_or_else(|| side_effect_response_body_mismatch("AddBlock", "missing target"))?;
        let target = target
            .try_into()
            .map_err(|err| side_effect_response_body_mismatch("AddBlock", err))?;
        let body = AddBlockResult {
            group_name: authority.group_name.clone(),
            target,
        };
        Ok(ValidatedMetadataResponse::new(authority, body))
    }

    async fn commit_file(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::CommitFileRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::CommitFileResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .commit_file(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn abort_file_write(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::AbortFileWriteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::AbortFileWriteResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .abort_file_write(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn renew_lease(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::RenewLeaseRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::RenewLeaseResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .renew_lease(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn sync_write(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::SyncWriteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::SyncWriteResponseProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "write")
            .await?
            .sync_write(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.clone(), response)
    }

    async fn msync(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::MsyncRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::common::GroupStateWatermarkProto>> {
        req.header = Some(build_metadata_header(&ctx)?);
        let response = self
            .client(&ctx, "refresh")
            .await?
            .msync(tonic_request(&ctx, req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_msync_response(&ctx, response)
    }
}

fn build_metadata_header(ctx: &AttemptContext) -> ClientResult<beryl_proto::common::RequestHeaderProto> {
    ctx.metadata_header()
}

/// Validates one successful response header and keeps its authority update
/// coupled to the body until the Metadata client applies it.
fn validated_metadata_response<T>(
    ctx: &AttemptContext,
    header: Option<beryl_proto::common::ResponseHeaderProto>,
    body: T,
) -> ClientResult<ValidatedMetadataResponse<T>> {
    let authority = parse_metadata_response_header(ctx, header.as_ref())?;
    Ok(ValidatedMetadataResponse::new(authority, body))
}

/// Binds the watermark returned by Msync to the validated response group and
/// folds it into the authority update applied by the Metadata client.
fn validated_msync_response(
    ctx: &AttemptContext,
    response: beryl_proto::metadata::MsyncResponseProto,
) -> ClientResult<ValidatedMetadataResponse<beryl_proto::common::GroupStateWatermarkProto>> {
    let mut authority = parse_metadata_response_header(ctx, response.header.as_ref())?;
    let body = response
        .state
        .ok_or_else(|| invalid_header_error("metadata Msync response missing state"))?;
    let watermark = GroupStateWatermark::try_from(body.clone())
        .map_err(|err| invalid_header_error(format!("metadata Msync response invalid state watermark: {err}")))?;
    if watermark.group_name != authority.group_name {
        return Err(invalid_header_error(format!(
            "metadata Msync response state group_name mismatch: expected {}, got {}",
            authority.group_name, watermark.group_name
        )));
    }
    authority.state.push(watermark);
    Ok(ValidatedMetadataResponse::new(authority, body))
}

/// Validates correlation before interpreting a structured error, and requires
/// successful responses to carry authority for the exact attempted group.
fn parse_metadata_response_header(
    ctx: &AttemptContext,
    header: Option<&beryl_proto::common::ResponseHeaderProto>,
) -> ClientResult<MetadataAuthorityUpdate> {
    let Some(header) = header else {
        return Err(invalid_header_error("metadata OK response missing ResponseHeader"));
    };
    let client = header
        .client
        .clone()
        .ok_or_else(|| invalid_header_error("metadata response missing client identity"))?
        .try_into()
        .map_err(|err| invalid_header_error(format!("metadata response invalid client identity: {err}")))?;
    validate_metadata_response_client(ctx, &client)?;
    let header = ResponseHeader::try_from(header.clone())
        .map_err(|err| invalid_header_error(format!("metadata OK response invalid ResponseHeader: {err}")))?;

    if header.rpc_error.is_some() {
        validate_metadata_error_scope(&header)?;
        return match validate_header(&header) {
            Err(error) => Err(error),
            Ok(()) => Err(invalid_header_error(
                "metadata response declared rpc_error without a recovery action",
            )),
        };
    }

    let group_name = header
        .group_name
        .clone()
        .ok_or_else(|| invalid_header_error("metadata OK response invalid ResponseHeader: group_name missing"))?;
    let request_group_name = ctx
        .group_name()
        .ok_or_else(|| invalid_header_error("metadata attempt missing group_name during response validation"))?;
    if &group_name != request_group_name {
        return Err(invalid_header_error(format!(
            "metadata OK response invalid ResponseHeader: group_name mismatch: expected {}, got {}",
            request_group_name, group_name
        )));
    }
    if let Some(watermark) = header.state.iter().find(|watermark| watermark.group_name != group_name) {
        return Err(invalid_header_error(format!(
            "metadata OK response invalid ResponseHeader: state group_name mismatch: expected {}, got {}",
            group_name, watermark.group_name
        )));
    }

    Ok(MetadataAuthorityUpdate {
        group_name,
        state: header.state,
        mount_epoch: header.mount_epoch,
        route_epoch: header.route_epoch,
    })
}

/// Validates the immutable request/response correlation fields independently
/// from group recovery hints carried by an error response.
fn validate_metadata_response_client(ctx: &AttemptContext, client: &ClientInfo) -> ClientResult<()> {
    let request_identity = ctx.header_identity();
    if client.client_id != request_identity.client_id {
        return Err(invalid_header_error(format!(
            "metadata OK response invalid ResponseHeader: client_id mismatch: expected {}, got {}",
            request_identity.client_id, client.client_id
        )));
    }
    if client.call_id != request_identity.call_id {
        return Err(invalid_header_error(
            "metadata OK response invalid ResponseHeader: call_id mismatch",
        ));
    }
    Ok(())
}

/// Rejects malformed or contradictory group hints before existing recovery
/// classification consumes them. Retry and terminal errors may omit a group.
fn validate_metadata_error_scope(header: &ResponseHeader) -> ClientResult<()> {
    let Some(rpc_error) = header.rpc_error.as_ref() else {
        return Ok(());
    };
    let hint = match &rpc_error.recovery {
        RecoveryAction::RefreshMetadata { hint } | RecoveryAction::ReopenWriteSession { hint } => Some(hint),
        RecoveryAction::Fail
        | RecoveryAction::Retry { .. }
        | RecoveryAction::RegisterWorker
        | RecoveryAction::SendFullBlockReport => None,
    };
    let Some(RpcRefreshHint {
        group_name: Some(raw_group_name),
        ..
    }) = hint
    else {
        return Ok(());
    };
    let hinted_group = GroupName::parse(raw_group_name)
        .map_err(|err| invalid_header_error(format!("metadata error response invalid recovery group_name: {err}")))?;
    if let Some(header_group) = header.group_name.as_ref() {
        if header_group != &hinted_group {
            return Err(invalid_header_error(format!(
                "metadata error response group_name conflicts with recovery hint: header={}, hint={}",
                header_group, hinted_group
            )));
        }
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
        .map_err(|err| ClientError::metadata(format!("invalid metadata endpoint {endpoint}: {err}")))
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
    use crate::error::ClientErrorKind;
    use crate::runtime::{retry_decision, Operation, OperationContext, OperationDeadline, RetryDecision, RetrySafety};
    use beryl_common::error::rpc::{ErrorKind, InternalErrorKind, MetadataErrorKind, RpcErrorDetail};
    use beryl_types::{CallId, ClientId};
    #[tokio::test]
    async fn concurrent_metadata_channel_requests_same_key_reuse_inserted_channel() {
        let transport = Arc::new(GrpcMetadataTransport::new_lazy_with_pool_options(true, 8).expect("transport"));
        let ctx = metadata_attempt("root", Some("127.0.0.1:18080"));

        let mut tasks = Vec::with_capacity(8);
        for _ in 0..8 {
            let transport = Arc::clone(&transport);
            let ctx = ctx.clone();
            tasks.push(tokio::spawn(async move { transport.client(&ctx, "read").await }));
        }

        for task in tasks {
            let _client = task.await.expect("task").expect("metadata client");
        }
        assert_eq!(transport.channels.read().len(), 1);
    }

    #[test]
    fn successful_responses_return_complete_authority_update() {
        let ctx = metadata_attempt("root", None);
        let mut header = success_header(&ctx);
        header.state.push(watermark("root", 9));
        header.mount_epoch = Some(31);
        header.route_epoch = Some(41);

        let update = parse_metadata_response_header(&ctx, Some(&header)).expect("valid response header");

        assert_eq!(update.group_name, GroupName::parse("root").unwrap());
        assert_eq!(update.state[0].state_id.index, 9);
        assert_eq!(update.mount_epoch, Some(31));
        assert_eq!(update.route_epoch, Some(41));

        let response = beryl_proto::metadata::MsyncResponseProto {
            header: Some(success_header(&ctx)),
            state: Some(watermark("root", 10)),
        };
        let (update, body) = validated_msync_response(&ctx, response)
            .expect("valid Msync response")
            .into_parts();
        assert_eq!(update.state[0].state_id.index, 10);
        assert_eq!(body.group_name, "root");
    }

    #[test]
    fn successful_responses_reject_missing_or_mismatched_authority_group() {
        let ctx = metadata_attempt("root", None);
        let mut missing_group = success_header(&ctx);
        missing_group.group_name.clear();
        let mut wrong_group = success_header(&ctx);
        wrong_group.group_name = "other".to_string();
        let mut wrong_state_group = success_header(&ctx);
        wrong_state_group.state.push(watermark("other", 9));

        for header in [missing_group, wrong_group, wrong_state_group] {
            let error = parse_metadata_response_header(&ctx, Some(&header)).expect_err("invalid authority group");
            assert_eq!(error.kind(), ClientErrorKind::InvalidResponse);
        }

        for state in [
            watermark("other", 9),
            beryl_proto::common::GroupStateWatermarkProto {
                group_name: "root".to_string(),
                state_id: None,
            },
        ] {
            let error = validated_msync_response(
                &ctx,
                beryl_proto::metadata::MsyncResponseProto {
                    header: Some(success_header(&ctx)),
                    state: Some(state),
                },
            )
            .expect_err("invalid Msync authority");
            assert_eq!(error.kind(), ClientErrorKind::InvalidResponse);
        }
    }

    #[test]
    fn structured_errors_allow_missing_group_but_not_identity_mismatch() {
        let ctx = metadata_attempt("root", None);
        let header = error_header_without_group(
            &ctx,
            RpcErrorDetail::retry(
                ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
                Some(10),
                "metadata not ready",
            ),
        );
        let error = parse_metadata_response_header(&ctx, Some(&header)).expect_err("structured retry");
        assert_eq!(retry_decision(&error, RetrySafety::ReadOnly), RetryDecision::Retry);

        let not_leader = error_header_without_group(
            &ctx,
            RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                RpcRefreshHint {
                    group_name: Some("root".to_string()),
                    leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                    ..RpcRefreshHint::default()
                },
                "not leader",
            ),
        );
        let error = parse_metadata_response_header(&ctx, Some(&not_leader)).expect_err("structured refresh");
        assert_eq!(
            retry_decision(&error, RetrySafety::ReadOnly),
            RetryDecision::RefreshMetadata(ErrorKind::Metadata(MetadataErrorKind::NotLeader))
        );

        let mut wrong_call = header.clone();
        wrong_call.client.as_mut().expect("client").call_id = CallId::new().to_string();
        let mut wrong_client = header;
        wrong_client.client.as_mut().expect("client").client_id = Some(ClientId::new(8).into());
        for header in [wrong_call, wrong_client] {
            let error = parse_metadata_response_header(&ctx, Some(&header)).expect_err("identity mismatch");
            assert_eq!(error.kind(), ClientErrorKind::InvalidResponse);
        }
    }

    fn metadata_attempt(group_name: &str, endpoint: Option<&str>) -> AttemptContext {
        let operation = OperationContext::new_named(
            ClientId::new(7),
            "test-client",
            Operation::GetStatus,
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

    fn success_header(ctx: &AttemptContext) -> beryl_proto::common::ResponseHeaderProto {
        let request = ctx.metadata_header().expect("request header");
        beryl_proto::common::ResponseHeaderProto {
            client: request.client,
            group_name: request.group_name,
            ..Default::default()
        }
    }

    fn error_header_without_group(
        ctx: &AttemptContext,
        error: RpcErrorDetail,
    ) -> beryl_proto::common::ResponseHeaderProto {
        let client = ctx
            .metadata_header()
            .expect("request header")
            .client
            .expect("request client")
            .try_into()
            .expect("domain client");
        let header = ResponseHeader::from_rpc_error(client, error);
        (&header).into()
    }

    fn watermark(group_name: &str, index: u64) -> beryl_proto::common::GroupStateWatermarkProto {
        beryl_proto::common::GroupStateWatermarkProto {
            group_name: group_name.to_string(),
            state_id: Some(beryl_proto::common::RaftLogIdProto {
                term: 1,
                leader_node_id: 1,
                index,
            }),
        }
    }
}
