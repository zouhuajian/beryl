// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Proto/domain and response-header conversion for metadata services.

use super::filesystem::{FsFailure, FsSuccess, PresentedFencingToken, RequestContext};
use crate::error::MetadataError;
use beryl_common::error::rpc::{ErrorKind, ProtocolErrorKind, RpcErrorDetail};
use beryl_common::header::{RequestHeader, ResponseHeader};
use beryl_types::ids::{BlockId, LeaseId};
use beryl_types::layout::FileLayout;
use beryl_types::lease::FencingToken;
use beryl_types::{FileBlockLocation, GroupName, GroupStateWatermark, WriteTarget};
use tracing::Span;

#[allow(clippy::result_large_err)]
pub(crate) fn request_context_from_proto(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
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
        route_epoch: req_header.as_ref().and_then(|h| h.route_epoch),
        caller,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn extract_and_inject_context(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
) -> Result<RequestHeader, RpcErrorDetail> {
    request_context_from_proto(req_header).map(|ctx| ctx.caller)
}

pub(crate) fn invalid_header_rpc_error(message: impl Into<String>) -> RpcErrorDetail {
    RpcErrorDetail::fail(ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader), message)
}

fn error_detail_from_rpc_error(err: &RpcErrorDetail) -> Option<beryl_proto::common::ErrorDetailProto> {
    Some(beryl_proto::convert::rpc_error_to_proto(err))
}

fn build_base_response_header(
    ctx: &RequestContext,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermark>,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut resp_header = ResponseHeader::ok(ctx.caller.client.clone());
    if let Some(group_name) = group_name {
        resp_header = resp_header.with_group_name(group_name);
    }
    resp_header.state = state;
    let mut proto_header: beryl_proto::common::ResponseHeaderProto = (&resp_header).into();
    if let Some(epoch) = mount_epoch {
        proto_header.mount_epoch = Some(epoch);
    }
    if let Some(epoch) = route_epoch {
        proto_header.route_epoch = Some(epoch);
    }
    proto_header
}

fn client_from_request_header(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
) -> Option<beryl_common::header::ClientInfo> {
    req_header
        .as_ref()
        .and_then(|header| header.client.clone())
        .and_then(|client| beryl_common::header::ClientInfo::try_from(client).ok())
}

pub(crate) fn ok_header_from_context(
    ctx: &RequestContext,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermark>,
) -> beryl_proto::common::ResponseHeaderProto {
    build_base_response_header(ctx, group_name, mount_epoch, route_epoch, state)
}

pub(crate) fn header_from_rpc_error_with_context(
    ctx: &RequestContext,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermark>,
    err: &RpcErrorDetail,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut header = build_base_response_header(ctx, group_name, mount_epoch, route_epoch, state);
    header.error = error_detail_from_rpc_error(err);
    header
}

pub(crate) fn ok_header_from_fs_success<T>(
    ctx: &RequestContext,
    success: &FsSuccess<T>,
) -> beryl_proto::common::ResponseHeaderProto {
    ok_header_from_context(
        ctx,
        success.group_name.clone(),
        success.mount_epoch,
        success.route_epoch,
        success.state.clone(),
    )
}

pub(crate) fn header_from_fs_failure(
    ctx: &RequestContext,
    failure: &FsFailure,
) -> beryl_proto::common::ResponseHeaderProto {
    header_from_rpc_error_with_context(
        ctx,
        failure.group_name.clone(),
        failure.mount_epoch,
        failure.route_epoch,
        failure.state.clone(),
        &failure.error,
    )
}

pub(crate) fn ok_header_from_request(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut header: beryl_proto::common::ResponseHeaderProto = client_from_request_header(req_header)
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
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    err: &RpcErrorDetail,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut header: beryl_proto::common::ResponseHeaderProto = client_from_request_header(req_header)
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

pub(crate) fn file_attrs_to_proto(attrs: &beryl_types::fs::FileAttrs) -> beryl_proto::fs::FileAttrsProto {
    beryl_proto::fs::FileAttrsProto {
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

pub(crate) fn file_attrs_from_proto(
    attrs: Option<beryl_proto::fs::FileAttrsProto>,
) -> Result<beryl_types::fs::FileAttrs, MetadataError> {
    let attrs = attrs.ok_or_else(|| MetadataError::InvalidArgument("Missing FileAttrs".to_string()))?;
    Ok(beryl_types::fs::FileAttrs {
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

pub(crate) fn file_layout_from_proto(
    layout: Option<beryl_proto::common::FileLayoutProto>,
) -> Result<FileLayout, MetadataError> {
    let layout = layout.ok_or_else(|| MetadataError::InvalidArgument("Missing FileLayout".to_string()))?;
    FileLayout::try_from(layout).map_err(MetadataError::InvalidArgument)
}

pub(crate) fn lease_id_from_proto(lease_id: Option<beryl_proto::common::LeaseIdProto>) -> Option<LeaseId> {
    lease_id.map(|lease_id_proto| {
        LeaseId::try_from(lease_id_proto).unwrap_or_else(|()| unreachable!("LeaseIdProto conversion is infallible"))
    })
}

pub(crate) fn lease_id_to_proto(lease_id: LeaseId) -> beryl_proto::common::LeaseIdProto {
    lease_id.into()
}

pub(crate) fn presented_fencing_from_proto(
    token: Option<beryl_proto::common::FencingTokenProto>,
) -> Option<PresentedFencingToken> {
    token.and_then(|token_proto| {
        let owner = beryl_proto::convert::required_client_id(token_proto.owner, "owner").ok()?;
        Some(PresentedFencingToken {
            block_id: token_proto.block_id.and_then(|block| BlockId::try_from(block).ok()),
            owner,
            epoch: token_proto.epoch,
        })
    })
}

pub(crate) fn fencing_to_proto(token: FencingToken) -> beryl_proto::common::FencingTokenProto {
    token.into()
}

pub(crate) fn write_target_to_proto(target: &WriteTarget) -> beryl_proto::metadata::WriteTargetProto {
    target.into()
}

pub(crate) fn location_to_proto(location: &FileBlockLocation) -> beryl_proto::metadata::FileBlockLocationProto {
    location.into()
}

#[cfg(test)]
mod tests {
    use super::request_context_from_proto;
    use beryl_common::error::rpc::{ErrorKind, ProtocolErrorKind, RecoveryAction, RpcErrorDetail};
    use beryl_types::ClientId;

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
        client: Option<beryl_proto::common::ClientInfoProto>,
    ) -> Option<beryl_proto::common::RequestHeaderProto> {
        Some(beryl_proto::common::RequestHeaderProto {
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
        let header = request_header_with_client(Some(beryl_proto::common::ClientInfoProto {
            call_id: beryl_types::CallId::new().to_string(),
            client_id: None,
            client_name: "worker-control".to_string(),
        }));

        let error = request_context_from_proto(&header).expect_err("missing client_id must fail");

        assert_invalid_header_error(&error, "client_id");
    }

    #[test]
    fn external_request_context_rejects_zero_client_id() {
        let header = request_header_with_client(Some(beryl_proto::common::ClientInfoProto {
            call_id: beryl_types::CallId::new().to_string(),
            client_id: Some(beryl_proto::common::ClientIdProto { high: 0, low: 0 }),
            client_name: "worker-control".to_string(),
        }));

        let error = request_context_from_proto(&header).expect_err("zero client_id must fail");

        assert_invalid_header_error(&error, "client_id");
    }

    #[test]
    fn external_request_context_rejects_invalid_call_id() {
        let header = request_header_with_client(Some(beryl_proto::common::ClientInfoProto {
            call_id: "not-a-uuid".to_string(),
            client_id: Some(ClientId::new(7).into()),
            client_name: "worker-control".to_string(),
        }));

        let error = request_context_from_proto(&header).expect_err("invalid call_id must fail");

        assert_invalid_header_error(&error, "call_id");
    }
}
