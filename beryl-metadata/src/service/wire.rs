// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Proto/domain and response-header conversion for metadata services.

use super::filesystem::{FsFailure, FsSuccess};
use beryl_common::error::rpc::{ErrorKind, ProtocolErrorKind, RpcErrorDetail};
use beryl_common::header::{RequestHeader, ResponseHeader};
use beryl_types::{GroupName, GroupStateWatermark};
use tracing::Span;

#[allow(clippy::result_large_err)]
pub(crate) fn extract_and_inject_context(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
) -> Result<RequestHeader, RpcErrorDetail> {
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
    if let Some(state) = &caller.state {
        Span::current().record("state", format!("{state:?}"));
    }
    Ok(caller)
}

pub(crate) fn invalid_header_rpc_error(message: impl Into<String>) -> RpcErrorDetail {
    RpcErrorDetail::fail(ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader), message)
}

fn build_base_response_header(
    ctx: &RequestHeader,
    group_name: Option<GroupName>,
    state: Option<GroupStateWatermark>,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut resp_header = ResponseHeader::ok(ctx.client.clone());
    if let Some(group_name) = group_name {
        resp_header = resp_header.with_group_name(group_name);
    }
    resp_header.state = state;
    (&resp_header).into()
}

fn client_from_request_header(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
) -> Option<beryl_common::header::ClientInfo> {
    req_header
        .as_ref()
        .and_then(|header| header.client.clone())
        .and_then(|client| beryl_common::header::ClientInfo::try_from(client).ok())
}

pub(crate) fn ok_header_from_fs_success<T>(
    ctx: &RequestHeader,
    success: &FsSuccess<T>,
) -> beryl_proto::common::ResponseHeaderProto {
    build_base_response_header(ctx, success.group_name.clone(), success.state.clone())
}

pub(crate) fn header_from_fs_failure(
    ctx: &RequestHeader,
    failure: &FsFailure,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut header = build_base_response_header(ctx, failure.group_name.clone(), None);
    header.error = Some(beryl_proto::convert::rpc_error_to_proto(&failure.error));
    header
}

pub(crate) fn ok_header_from_request(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
    group_name: Option<GroupName>,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut header: beryl_proto::common::ResponseHeaderProto = client_from_request_header(req_header)
        .map(|client| (&ResponseHeader::ok(client)).into())
        .unwrap_or_default();
    if let Some(group_name) = group_name {
        header.group_name = group_name.to_string();
    }
    header
}

pub(super) fn header_from_rpc_error(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
    group_name: Option<GroupName>,
    err: &RpcErrorDetail,
) -> beryl_proto::common::ResponseHeaderProto {
    let mut header = ok_header_from_request(req_header, group_name);
    header.error = Some(beryl_proto::convert::rpc_error_to_proto(err));
    header
}
