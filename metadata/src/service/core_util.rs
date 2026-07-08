// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Core wire/context utilities shared by service adapters and FsCore.

use super::domain::{CoreFailure, CoreSuccess, PresentedFencingToken, RequestContext};
use crate::error::{to_fs_error_detail, MetadataError};
use common::error::rpc::{ErrorKind, ProtocolErrorKind, RefreshHint, RpcErrorDetail};
use common::header::{RequestHeader, ResponseHeader};
use tracing::Span;
use types::fs::FsErrorCode;
use types::ids::{BlockId, LeaseId, WorkerId};
use types::layout::FileLayout;
use types::lease::FencingToken;
use types::{FileBlockLocation, GroupName, GroupStateWatermark, WorkerEndpointInfo, WriteTarget};

#[allow(clippy::result_large_err)]
pub fn request_context_from_proto(
    req_header: &Option<proto::common::RequestHeaderProto>,
) -> Result<RequestContext, RpcErrorDetail> {
    let proto_header = req_header
        .clone()
        .ok_or_else(|| invalid_header_rpc_error("external request requires RequestHeader"))?;
    let caller = RequestHeader::try_from(proto_header)
        .map_err(|err| invalid_header_rpc_error(format!("invalid RequestHeader: {err}")))?;

    Span::current().record("call_id", caller.client.call_id.to_string());
    Span::current().record("client_id", caller.client.client_id.to_string());
    if let Some(ref client_name) = caller.client.client_name {
        Span::current().record("client_name", client_name);
    }
    if let Some(traceparent) = &caller.trace_context.traceparent {
        Span::current().record("traceparent", traceparent);
    }
    if !caller.state.is_empty() {
        Span::current().record("state", format!("{:?}", caller.state));
    }
    Ok(RequestContext {
        traceparent: caller.trace_context.traceparent.clone(),
        route_epoch: req_header.as_ref().and_then(|h| h.route_epoch),
        caller,
    })
}

#[allow(clippy::result_large_err)]
pub fn extract_and_inject_context(
    req_header: &Option<proto::common::RequestHeaderProto>,
) -> Result<RequestHeader, RpcErrorDetail> {
    request_context_from_proto(req_header).map(|ctx| ctx.caller)
}

pub fn invalid_header_rpc_error(message: impl Into<String>) -> RpcErrorDetail {
    RpcErrorDetail::fail(ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader), message)
}

fn error_detail_from_rpc_error(err: &RpcErrorDetail) -> Option<proto::common::ErrorDetailProto> {
    Some(proto::convert::rpc_error_to_proto(err))
}

fn build_base_response_header(
    ctx: &RequestContext,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermark>,
) -> proto::common::ResponseHeaderProto {
    let mut resp_header = ResponseHeader::ok(ctx.caller.client.clone());
    if let Some(group_name) = group_name {
        resp_header = resp_header.with_group_name(group_name);
    }
    resp_header.state = state;
    let mut proto_header: proto::common::ResponseHeaderProto = (&resp_header).into();
    if let Some(epoch) = mount_epoch {
        proto_header.mount_epoch = Some(epoch);
    }
    if let Some(epoch) = route_epoch {
        proto_header.route_epoch = Some(epoch);
    }
    proto_header
}

fn client_from_request_header(
    req_header: &Option<proto::common::RequestHeaderProto>,
) -> Option<common::header::ClientInfo> {
    req_header
        .as_ref()
        .and_then(|header| header.client.clone())
        .and_then(|client| common::header::ClientInfo::try_from(client).ok())
}

pub fn ok_header_from_context(
    ctx: &RequestContext,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermark>,
) -> proto::common::ResponseHeaderProto {
    build_base_response_header(ctx, group_name, mount_epoch, route_epoch, state)
}

pub fn header_from_rpc_error_with_context(
    ctx: &RequestContext,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermark>,
    err: &RpcErrorDetail,
) -> proto::common::ResponseHeaderProto {
    let mut header = build_base_response_header(ctx, group_name, mount_epoch, route_epoch, state);
    header.error = error_detail_from_rpc_error(err);
    header
}

pub fn ok_header_from_core_success<T>(
    ctx: &RequestContext,
    success: &CoreSuccess<T>,
) -> proto::common::ResponseHeaderProto {
    ok_header_from_context(
        ctx,
        success.group_name.clone(),
        success.mount_epoch,
        success.route_epoch,
        success.state.clone(),
    )
}

pub fn header_from_core_failure(ctx: &RequestContext, failure: &CoreFailure) -> proto::common::ResponseHeaderProto {
    header_from_rpc_error_with_context(
        ctx,
        failure.group_name.clone(),
        failure.mount_epoch,
        failure.route_epoch,
        failure.state.clone(),
        &failure.error,
    )
}

pub(crate) fn core_failure_from_metadata_error(
    ctx: &RequestContext,
    err: MetadataError,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
) -> CoreFailure {
    core_failure_from_rpc_error(ctx, to_fs_error_detail(err), group_name, mount_epoch, route_epoch)
}

fn core_failure_from_rpc_error(
    _ctx: &RequestContext,
    err: RpcErrorDetail,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
) -> CoreFailure {
    CoreFailure::new(err, group_name, mount_epoch, route_epoch, Vec::new())
}

// Refresh-metadata failures must keep caller and server hint fields explicit.
#[allow(clippy::too_many_arguments)]
pub(crate) fn refresh_metadata_core_failure(
    ctx: &RequestContext,
    kind: ErrorKind,
    message: impl Into<String>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    hint: Option<RefreshHint>,
) -> CoreFailure {
    let err = RpcErrorDetail::refresh_metadata(kind, hint.unwrap_or_default(), message);
    core_failure_from_rpc_error(ctx, err, group_name, mount_epoch, route_epoch)
}

pub(crate) fn fatal_fs_core_failure(
    ctx: &RequestContext,
    errno: FsErrorCode,
    message: impl Into<String>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
) -> CoreFailure {
    core_failure_from_rpc_error(ctx, RpcErrorDetail::fs(errno, message), group_name, mount_epoch, None)
}

pub fn ok_header_from_request(
    req_header: &Option<proto::common::RequestHeaderProto>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
) -> proto::common::ResponseHeaderProto {
    let mut header: proto::common::ResponseHeaderProto = client_from_request_header(req_header)
        .map(|client| {
            let mut header = ResponseHeader::ok(client);
            if let Some(group_name) = group_name.clone() {
                header = header.with_group_name(group_name);
            }
            (&header).into()
        })
        .unwrap_or_default();
    if let Some(group_name) = group_name {
        header.group_name = group_name.to_string();
    }
    header.mount_epoch = mount_epoch;
    header
}

pub fn header_from_rpc_error(
    req_header: &Option<proto::common::RequestHeaderProto>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    err: &RpcErrorDetail,
) -> proto::common::ResponseHeaderProto {
    let mut header: proto::common::ResponseHeaderProto = client_from_request_header(req_header)
        .map(|client| {
            let mut header = ResponseHeader::from_rpc_error(client, err.clone());
            if let Some(group_name) = group_name.clone() {
                header = header.with_group_name(group_name);
            }
            (&header).into()
        })
        .unwrap_or_default();
    if let Some(group_name) = group_name {
        header.group_name = group_name.to_string();
    }
    header.mount_epoch = mount_epoch;
    header.error = error_detail_from_rpc_error(err);
    header
}

pub fn file_attrs_to_proto(attrs: &types::fs::FileAttrs) -> proto::fs::FileAttrsProto {
    proto::fs::FileAttrsProto {
        mode: attrs.mode,
        uid: attrs.uid,
        gid: attrs.gid,
        size: attrs.size,
        atime_ms: attrs.atime_ms,
        mtime_ms: attrs.mtime_ms,
        ctime_ms: attrs.ctime_ms,
        nlink: attrs.nlink,
    }
}

pub fn file_attrs_from_proto(attrs: Option<proto::fs::FileAttrsProto>) -> Result<types::fs::FileAttrs, MetadataError> {
    let attrs = attrs.ok_or_else(|| MetadataError::InvalidArgument("Missing FileAttrs".to_string()))?;
    Ok(types::fs::FileAttrs {
        mode: attrs.mode,
        uid: attrs.uid,
        gid: attrs.gid,
        size: attrs.size,
        atime_ms: attrs.atime_ms,
        mtime_ms: attrs.mtime_ms,
        ctime_ms: attrs.ctime_ms,
        nlink: attrs.nlink,
    })
}

pub fn file_layout_from_proto(layout: Option<proto::common::FileLayoutProto>) -> Result<FileLayout, MetadataError> {
    let layout = layout.ok_or_else(|| MetadataError::InvalidArgument("Missing FileLayout".to_string()))?;
    FileLayout::try_from(layout).map_err(MetadataError::InvalidArgument)
}

pub fn validate_active_write_layout(layout: &FileLayout) -> Result<(), MetadataError> {
    layout
        .validate()
        .map_err(|err| MetadataError::InvalidArgument(err.to_string()))?;
    if layout.replication != 1 {
        return Err(MetadataError::InvalidArgument(
            "multi-replica write is not supported yet; replication must be 1".to_string(),
        ));
    }
    Ok(())
}

pub fn fatal_fs_header(
    req_header: &Option<proto::common::RequestHeaderProto>,
    errno: FsErrorCode,
    message: impl Into<String>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
) -> proto::common::ResponseHeaderProto {
    let err = RpcErrorDetail::fs(errno, message);
    header_from_rpc_error(req_header, group_name, mount_epoch, &err)
}

pub fn refresh_metadata_header(
    req_header: &Option<proto::common::RequestHeaderProto>,
    kind: ErrorKind,
    message: impl Into<String>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
) -> proto::common::ResponseHeaderProto {
    let hint = RefreshHint {
        group_name: group_name.as_ref().map(ToString::to_string),
        mount_epoch,
        ..Default::default()
    };
    let err = RpcErrorDetail::refresh_metadata(kind, hint, message);
    header_from_rpc_error(req_header, group_name, mount_epoch, &err)
}

pub fn retryable_header(
    req_header: &Option<proto::common::RequestHeaderProto>,
    kind: ErrorKind,
    retry_after_ms: Option<u64>,
    message: impl Into<String>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
) -> proto::common::ResponseHeaderProto {
    let err = RpcErrorDetail::retry(kind, retry_after_ms, message);
    header_from_rpc_error(req_header, group_name, mount_epoch, &err)
}

pub fn lease_id_from_proto(lease_id: Option<proto::common::LeaseIdProto>) -> Option<LeaseId> {
    lease_id.map(|lease_id_proto| {
        LeaseId::try_from(lease_id_proto).unwrap_or_else(|()| unreachable!("LeaseIdProto conversion is infallible"))
    })
}

pub fn lease_id_to_proto(lease_id: LeaseId) -> proto::common::LeaseIdProto {
    lease_id.into()
}

pub fn presented_fencing_from_proto(token: Option<proto::common::FencingTokenProto>) -> Option<PresentedFencingToken> {
    token.and_then(|token_proto| {
        let owner = proto::convert::required_client_id(token_proto.owner, "owner").ok()?;
        Some(PresentedFencingToken {
            block_id: token_proto.block_id.and_then(|block| BlockId::try_from(block).ok()),
            owner,
            epoch: token_proto.epoch,
        })
    })
}

pub fn fencing_to_proto(token: FencingToken) -> proto::common::FencingTokenProto {
    token.into()
}

pub fn worker_endpoint_from_parts(
    worker_id: WorkerId,
    endpoint: String,
    worker_net_protocol: i32,
    worker_run_id: types::WorkerRunId,
) -> Result<WorkerEndpointInfo, MetadataError> {
    proto::convert::worker_endpoint_info_from_parts(worker_id, endpoint, worker_net_protocol, worker_run_id.to_string())
        .map_err(MetadataError::InvalidArgument)
}

pub fn write_target_to_proto(target: &WriteTarget) -> proto::metadata::WriteTargetProto {
    target.into()
}

pub fn location_to_proto(location: &FileBlockLocation) -> proto::metadata::FileBlockLocationProto {
    location.into()
}

#[cfg(test)]
mod tests {
    use super::request_context_from_proto;
    use common::error::rpc::{ErrorKind, ProtocolErrorKind, RecoveryAction, RpcErrorDetail};
    use types::ClientId;

    fn assert_invalid_header_error(error: &RpcErrorDetail, expected_message: &str) {
        assert_eq!(error.kind, ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader));
        assert_eq!(error.recovery, RecoveryAction::Fail);
        assert!(
            error.message.contains(expected_message),
            "message {:?} did not contain {:?}",
            error.message,
            expected_message
        );
    }

    fn request_header_with_client(
        client: Option<proto::common::ClientInfoProto>,
    ) -> Option<proto::common::RequestHeaderProto> {
        Some(proto::common::RequestHeaderProto {
            client,
            ..Default::default()
        })
    }

    #[test]
    fn external_request_context_rejects_missing_header() {
        let error = request_context_from_proto(&None).expect_err("missing header must fail");

        assert_invalid_header_error(&error, "RequestHeader");
    }

    #[test]
    fn external_request_context_rejects_missing_client_info() {
        let header = request_header_with_client(None);

        let error = request_context_from_proto(&header).expect_err("missing client info must fail");

        assert_invalid_header_error(&error, "client");
    }

    #[test]
    fn external_request_context_rejects_missing_client_id() {
        let header = request_header_with_client(Some(proto::common::ClientInfoProto {
            call_id: types::CallId::new().to_string(),
            client_id: None,
            client_name: "worker-control".to_string(),
        }));

        let error = request_context_from_proto(&header).expect_err("missing client_id must fail");

        assert_invalid_header_error(&error, "client_id");
    }

    #[test]
    fn external_request_context_rejects_zero_client_id() {
        let header = request_header_with_client(Some(proto::common::ClientInfoProto {
            call_id: types::CallId::new().to_string(),
            client_id: Some(proto::common::ClientIdProto { high: 0, low: 0 }),
            client_name: "worker-control".to_string(),
        }));

        let error = request_context_from_proto(&header).expect_err("zero client_id must fail");

        assert_invalid_header_error(&error, "client_id");
    }

    #[test]
    fn external_request_context_rejects_invalid_call_id() {
        let header = request_header_with_client(Some(proto::common::ClientInfoProto {
            call_id: "not-a-uuid".to_string(),
            client_id: Some(ClientId::new(7).into()),
            client_name: "worker-control".to_string(),
        }));

        let error = request_context_from_proto(&header).expect_err("invalid call_id must fail");

        assert_invalid_header_error(&error, "call_id");
    }
}
