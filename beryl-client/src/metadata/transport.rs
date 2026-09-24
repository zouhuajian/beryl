// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata transport boundary and tonic implementation.

use beryl_common::header::{ClientInfo, ResponseHeader};
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_types::GroupName;
use std::collections::HashMap;
use tonic::transport as tonic_net;

use crate::config::{normalize_endpoint, ClientConfig};
use crate::error::{side_effect_response_body_mismatch, ClientError, ClientResult};
use crate::metadata::model::{AllocateBlockResult, MetadataAuthorityUpdate, ReadLayout, ValidatedMetadataResponse};
use crate::metrics::{self, ClientMetric, ClientMetricLabels};
use crate::runtime::AttemptContext;

/// Tonic-backed Metadata transport for one selected-endpoint attempt.
#[derive(Debug)]
pub(crate) struct GrpcMetadataTransport {
    channels: parking_lot::RwLock<HashMap<String, tonic_net::Channel>>,
    channel_pool_enabled: bool,
    max_channels: usize,
}

impl GrpcMetadataTransport {
    /// Creates a lazily connecting Metadata transport from sealed client configuration.
    pub(crate) fn new_lazy_with_config(config: &ClientConfig) -> Self {
        Self {
            channels: parking_lot::RwLock::new(HashMap::new()),
            channel_pool_enabled: config.metadata_connection_reuse(),
            max_channels: config.metadata_connection_limit(),
        }
    }

    fn client(
        &self,
        ctx: &AttemptContext,
        operation: &'static str,
    ) -> ClientResult<FileSystemServiceProtoClient<tonic_net::Channel>> {
        let key = normalize_endpoint(ctx.metadata_endpoint());
        if !self.channel_pool_enabled {
            self.record_pool_metric(ClientMetric::MetadataChannelPoolMiss, operation, "miss");
            return lazy_channel(&key)
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
                self.create_metadata_channel(key, operation)?
            }
        };
        Ok(FileSystemServiceProtoClient::new(channel))
    }

    fn create_metadata_channel(&self, key: String, operation: &'static str) -> ClientResult<tonic_net::Channel> {
        if let Some(channel) = self.channels.read().get(&key).cloned() {
            self.record_pool_metric(ClientMetric::MetadataChannelPoolHit, operation, "hit");
            return Ok(channel);
        }
        let channel = lazy_channel(&key).inspect_err(|_err| {
            self.record_pool_metric(ClientMetric::ChannelBuildError, operation, "error");
        })?;
        Ok(self.insert_metadata_channel(key, channel))
    }

    fn insert_metadata_channel(&self, key: String, channel: tonic_net::Channel) -> tonic_net::Channel {
        let mut channels = self.channels.write();
        if let Some(existing) = channels.get(&key).cloned() {
            return existing;
        }
        evict_metadata_channel_if_needed(&mut channels, self.max_channels);
        channels.insert(key, channel.clone());
        channel
    }

    fn record_pool_metric(&self, metric: ClientMetric, operation: &'static str, outcome: &'static str) {
        metrics::record(
            metric,
            ClientMetricLabels::default()
                .with_cache("channel_pool")
                .with_operation(operation, "metadata")
                .with_outcome(outcome),
        );
    }
}

fn evict_metadata_channel_if_needed(channels: &mut HashMap<String, tonic_net::Channel>, limit: usize) {
    if channels.len() >= limit {
        let evicted = channels.keys().min().expect("full channel pool").clone();
        channels.remove(&evicted);
    }
}

impl GrpcMetadataTransport {
    pub(crate) async fn get_status(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::GetStatusRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::GetStatusResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "read")?
            .get_status(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn list_status(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::ListStatusRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::ListStatusResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "read")?
            .list_status(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn create_directory(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::CreateDirectoryRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::CreateDirectoryResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "write")?
            .create_directory(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn delete(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::DeleteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::DeleteResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "write")?
            .delete(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn rename(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::RenameRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::RenameResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "write")?
            .rename(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn open_file(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::OpenFileRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::OpenFileResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "read")?
            .open_file(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn read_layout(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::GetBlockLocationsRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<ReadLayout>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "read")?
            .get_block_locations(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let authority = parse_metadata_response_header(&ctx, response.header.take())?;
        let body = ReadLayout::from_response(authority.group_name.clone(), response.status, response.locations)?;
        Ok(ValidatedMetadataResponse::new(authority, body))
    }

    pub(crate) async fn create_file(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::CreateFileRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::CreateFileResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "write")?
            .create_file(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn open_write(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::OpenWriteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<(GroupName, beryl_proto::metadata::OpenWriteResponseProto)>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "write")?
            .open_write(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let authority = parse_metadata_response_header(&ctx, response.header.take())?;
        let group = authority.group_name.clone();
        Ok(ValidatedMetadataResponse::new(authority, (group, response)))
    }

    pub(crate) async fn allocate_block(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::AllocateBlockRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<AllocateBlockResult>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "write")?
            .allocate_block(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let authority = parse_metadata_response_header(&ctx, response.header.take())?;
        let block = response
            .block
            .ok_or_else(|| side_effect_response_body_mismatch("AllocateBlock", "missing block"))?;
        let block = block
            .try_into()
            .map_err(|err| side_effect_response_body_mismatch("AllocateBlock", err))?;
        let body = AllocateBlockResult {
            group_name: authority.group_name.clone(),
            block,
        };
        Ok(ValidatedMetadataResponse::new(authority, body))
    }

    pub(crate) async fn commit_file(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::CommitFileRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::CommitFileResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "write")?
            .commit_file(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn abort_file_write(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::AbortFileWriteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::AbortFileWriteResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "write")?
            .abort_file_write(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn renew_lease(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::RenewLeaseRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::RenewLeaseResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "write")?
            .renew_lease(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn sync_write(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::SyncWriteRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::SyncWriteResponseProto>> {
        req.header = Some(ctx.metadata_header());
        let mut response = self
            .client(&ctx, "write")?
            .sync_write(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        validated_metadata_response(&ctx, response.header.take(), response)
    }

    pub(crate) async fn msync(
        &self,
        ctx: AttemptContext,
        mut req: beryl_proto::metadata::MsyncRequestProto,
    ) -> ClientResult<ValidatedMetadataResponse<()>> {
        req.header = Some(ctx.metadata_header());
        let response = self
            .client(&ctx, "refresh")?
            .msync(ctx.operation_context().tonic_request(req))
            .await
            .map_err(ClientError::from)?
            .into_inner();
        let authority = parse_metadata_response_header(&ctx, response.header)?;
        if authority.state.is_none() {
            return Err(ClientError::malformed_response("metadata Msync response missing state"));
        }
        Ok(ValidatedMetadataResponse::new(authority, ()))
    }
}

/// Validates one successful response header and keeps its authority update
/// coupled to the body until the Metadata client applies it.
fn validated_metadata_response<T>(
    ctx: &AttemptContext,
    header: Option<beryl_proto::common::ResponseHeaderProto>,
    body: T,
) -> ClientResult<ValidatedMetadataResponse<T>> {
    let authority = parse_metadata_response_header(ctx, header)?;
    Ok(ValidatedMetadataResponse::new(authority, body))
}

/// Validates correlation before interpreting a structured error, and requires
/// successful responses to carry authority for the exact attempted group.
fn parse_metadata_response_header(
    ctx: &AttemptContext,
    header: Option<beryl_proto::common::ResponseHeaderProto>,
) -> ClientResult<MetadataAuthorityUpdate> {
    let Some(header) = header else {
        return Err(ClientError::malformed_response(
            "metadata OK response missing ResponseHeader",
        ));
    };
    let mut header = ResponseHeader::try_from(header).map_err(|err| {
        ClientError::malformed_response(format!("metadata OK response invalid ResponseHeader: {err}"))
    })?;

    validate_metadata_response_client(ctx, &header.client)?;
    if let Some(error) = header.rpc_error.take() {
        return Err(crate::rpc_error::metadata_error(&header, error));
    }

    let group_name = header.group_name.clone().ok_or_else(|| {
        ClientError::malformed_response("metadata OK response invalid ResponseHeader: group_name missing")
    })?;
    let request_group_name = ctx.group_name();
    if &group_name != request_group_name {
        return Err(ClientError::malformed_response(format!(
            "metadata OK response invalid ResponseHeader: group_name mismatch: expected {}, got {}",
            request_group_name, group_name
        )));
    }
    if let Some(watermark) = header
        .state
        .as_ref()
        .filter(|watermark| watermark.group_name != group_name)
    {
        return Err(ClientError::malformed_response(format!(
            "metadata OK response invalid ResponseHeader: state group_name mismatch: expected {}, got {}",
            group_name, watermark.group_name
        )));
    }

    Ok(MetadataAuthorityUpdate {
        group_name,
        state: header.state,
    })
}

/// Validates the immutable request/response correlation fields independently
/// from group recovery hints carried by an error response.
fn validate_metadata_response_client(ctx: &AttemptContext, client: &ClientInfo) -> ClientResult<()> {
    let operation = ctx.operation_context();
    if client.client_id != operation.client_id() {
        return Err(ClientError::malformed_response(format!(
            "metadata OK response invalid ResponseHeader: client_id mismatch: expected {}, got {}",
            operation.client_id(),
            client.client_id
        )));
    }
    if client.call_id != operation.call_id() {
        return Err(ClientError::malformed_response(
            "metadata OK response invalid ResponseHeader: call_id mismatch",
        ));
    }
    Ok(())
}

fn lazy_channel(endpoint: &str) -> ClientResult<tonic_net::Channel> {
    tonic_net::Endpoint::from_shared(endpoint.to_string())
        .map_err(|err| ClientError::metadata(format!("invalid metadata endpoint {endpoint}: {err}")))
        .map(|endpoint| endpoint.connect_lazy())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ClientErrorKind;
    use crate::runtime::{retry_decision, Operation, OperationContext, OperationDeadline, RetryDecision, RetrySafety};
    use beryl_common::error::rpc::{
        ErrorKind, InternalErrorKind, MetadataErrorKind, RefreshHint as RpcRefreshHint, RpcErrorDetail,
    };
    use beryl_types::{CallId, ClientId};
    // Lazy channels use Tokio's executor even though acquisition is synchronous.
    #[tokio::test]
    async fn metadata_channel_requests_cache_one_key() {
        let config = ClientConfig::builder().metadata_connection_limit(8).build().unwrap();
        let transport = GrpcMetadataTransport::new_lazy_with_config(&config);
        let ctx = metadata_attempt("root", Some("127.0.0.1:18080"));
        for _ in 0..2 {
            transport.client(&ctx, "read").expect("metadata client");
        }
        assert_eq!(transport.channels.read().len(), 1);
    }

    #[test]
    fn successful_responses_return_complete_authority_update() {
        let ctx = metadata_attempt("root", None);
        let mut header = success_header(&ctx);
        header.state = Some(watermark("root", 9));

        let update = parse_metadata_response_header(&ctx, Some(header)).expect("valid response header");

        assert_eq!(update.group_name, GroupName::parse("root").unwrap());
        assert_eq!(update.state.unwrap().state_id.index, 9);
    }

    #[test]
    fn successful_responses_reject_missing_or_mismatched_authority_group() {
        let ctx = metadata_attempt("root", None);
        let mut missing_group = success_header(&ctx);
        missing_group.group_name.clear();
        let mut wrong_group = success_header(&ctx);
        wrong_group.group_name = "other".to_string();
        let mut wrong_state_group = success_header(&ctx);
        wrong_state_group.state = Some(watermark("other", 9));

        let mut missing_state_id = success_header(&ctx);
        missing_state_id.state = Some(beryl_proto::common::GroupStateWatermarkProto {
            group_name: "root".to_string(),
            state_id: None,
        });

        for header in [missing_group, wrong_group, wrong_state_group, missing_state_id] {
            let error = parse_metadata_response_header(&ctx, Some(header)).expect_err("invalid authority group");
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
        let error = parse_metadata_response_header(&ctx, Some(header.clone())).expect_err("structured retry");
        assert_eq!(retry_decision(&error, RetrySafety::ReadOnly), RetryDecision::Retry);

        let not_leader = error_header_without_group(
            &ctx,
            RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                RpcRefreshHint {
                    group_name: Some("root".to_string()),
                    leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                },
                "not leader",
            ),
        );
        let error = parse_metadata_response_header(&ctx, Some(not_leader)).expect_err("structured refresh");
        assert_eq!(
            retry_decision(&error, RetrySafety::ReadOnly),
            RetryDecision::RefreshMetadata(ErrorKind::Metadata(MetadataErrorKind::NotLeader))
        );

        for hinted_group in ["", "other"] {
            let mut conflicting = error_header_without_group(
                &ctx,
                RpcErrorDetail::refresh_metadata(
                    ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                    RpcRefreshHint {
                        group_name: Some(hinted_group.to_string()),
                        ..RpcRefreshHint::default()
                    },
                    "invalid recovery scope",
                ),
            );
            conflicting.group_name = "root".to_string();
            let error = parse_metadata_response_header(&ctx, Some(conflicting)).unwrap_err();
            assert_eq!(error.kind(), ClientErrorKind::InvalidResponse);
        }

        let mut wrong_call = header.clone();
        wrong_call.client.as_mut().expect("client").call_id = CallId::new().to_string();
        let mut wrong_client = header;
        wrong_client.client.as_mut().expect("client").client_id = Some(ClientId::new(8).into());
        for header in [wrong_call, wrong_client] {
            let error = parse_metadata_response_header(&ctx, Some(header)).expect_err("identity mismatch");
            assert_eq!(error.kind(), ClientErrorKind::InvalidResponse);
        }
    }

    fn metadata_attempt(group_name: &str, endpoint: Option<&str>) -> AttemptContext {
        let operation = OperationContext::new_named(
            ClientId::new(7),
            "test-client",
            Operation::GetStatus,
            OperationDeadline::new(5_000),
        );
        AttemptContext::for_metadata(
            &operation,
            GroupName::parse(group_name).unwrap(),
            endpoint.unwrap_or("http://127.0.0.1:18080"),
        )
    }

    fn success_header(ctx: &AttemptContext) -> beryl_proto::common::ResponseHeaderProto {
        let request = ctx.metadata_header();
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
            .client
            .expect("request client")
            .try_into()
            .expect("domain client");
        let header = ResponseHeader {
            rpc_error: Some(error),
            ..ResponseHeader::ok(client)
        };
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
