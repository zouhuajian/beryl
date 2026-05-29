// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Core wire/context utilities shared by service adapters and FsCore.

use super::domain::{CoreFailure, CoreSuccess, PresentedFencingToken, RequestContext};
use crate::error::{to_canonical_fs, MetadataError};
use common::error::canonical::{
    CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshHint, RefreshReason,
};
use common::header::{RequestHeader, ResponseHeader, RpcErrorCode};
use tracing::Span;
use types::fs::FsErrorCode;
use types::ids::{BlockId, LeaseId, WorkerId};
use types::layout::FileLayout;
use types::lease::FencingToken;
use types::{FileBlockLocation, GroupStateWatermark, WorkerEndpointInfo, WriteTarget};

pub fn request_context_from_proto(req_header: &Option<proto::common::RequestHeaderProto>) -> RequestContext {
    let caller = if let Some(proto_header) = req_header {
        RequestHeader::try_from(proto_header.clone()).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to parse RequestHeaderProto, using default");
            RequestHeader::new(types::ClientId::new(0))
        })
    } else {
        RequestHeader::new(types::ClientId::new(0))
    };

    Span::current().record("call_id", caller.client.call_id.to_string());
    Span::current().record("client_id", caller.client.client_id.as_raw());
    if let Some(ref client_name) = caller.client.client_name {
        Span::current().record("client_name", client_name);
    }
    if let Some(traceparent) = &caller.traceparent {
        Span::current().record("traceparent", traceparent);
    }
    if !caller.state.is_empty() {
        Span::current().record("state", format!("{:?}", caller.state));
    }
    if let Some(principal) = &caller.principal {
        Span::current().record("principal", principal);
    }

    RequestContext {
        traceparent: caller.traceparent.clone(),
        route_epoch: req_header.as_ref().and_then(|h| h.route_epoch),
        principal: caller.principal.clone(),
        real_user: caller.real_user.clone(),
        doas: caller.doas.clone(),
        authn_type: caller.authn_type,
        caller,
    }
}

pub fn extract_and_inject_context(req_header: &Option<proto::common::RequestHeaderProto>) -> RequestHeader {
    request_context_from_proto(req_header).caller
}

fn error_detail_from_canonical(err: &CanonicalError) -> Option<proto::common::ErrorDetailProto> {
    debug_assert!(
        err.class != ErrorClass::Ok || (err.code.is_none() && err.reason.is_none() && err.retry_after_ms.is_none()),
        "CanonicalError invariant violated: Ok must not carry code/reason/retry_after_ms"
    );
    debug_assert!(
        err.class != ErrorClass::NeedRefresh || err.reason.is_some(),
        "CanonicalError invariant violated: NeedRefresh must have reason"
    );
    if err.class == ErrorClass::Ok {
        return None;
    }

    let mut wire_error = err.clone();
    // FileSystemService deliberately does not expose generic Moved; structural
    // encoding stays in proto::convert after this service-local policy choice.
    if wire_error.reason == Some(RefreshReason::Moved) {
        wire_error.reason = Some(RefreshReason::Unknown);
    }
    Some(proto::convert::canonical_to_error_detail(&wire_error))
}

fn build_base_response_header(
    ctx: &RequestContext,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermark>,
) -> proto::common::ResponseHeaderProto {
    let mut resp_header = ResponseHeader::ok(ctx.caller.client.clone()).with_group_id(group_id.unwrap_or(0));
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

pub fn ok_header_from_context(
    ctx: &RequestContext,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermark>,
) -> proto::common::ResponseHeaderProto {
    build_base_response_header(ctx, group_id, mount_epoch, route_epoch, state)
}

pub fn header_from_canonical_error_with_context(
    ctx: &RequestContext,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermark>,
    err: &CanonicalError,
) -> proto::common::ResponseHeaderProto {
    let mut header = build_base_response_header(ctx, group_id, mount_epoch, route_epoch, state);
    header.error = error_detail_from_canonical(err);
    header
}

pub fn ok_header_from_core_success<T>(
    ctx: &RequestContext,
    success: &CoreSuccess<T>,
) -> proto::common::ResponseHeaderProto {
    ok_header_from_context(
        ctx,
        success.group_id,
        success.mount_epoch,
        success.route_epoch,
        success.state.clone(),
    )
}

pub fn header_from_core_failure(ctx: &RequestContext, failure: &CoreFailure) -> proto::common::ResponseHeaderProto {
    header_from_canonical_error_with_context(
        ctx,
        failure.group_id,
        failure.mount_epoch,
        failure.route_epoch,
        failure.state.clone(),
        &failure.error,
    )
}

pub(crate) fn core_failure_from_metadata_error(
    ctx: &RequestContext,
    err: MetadataError,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
) -> CoreFailure {
    core_failure_from_canonical_error(ctx, to_canonical_fs(err), group_id, mount_epoch, route_epoch)
}

fn core_failure_from_canonical_error(
    _ctx: &RequestContext,
    err: CanonicalError,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
) -> CoreFailure {
    CoreFailure::new(err, group_id, mount_epoch, route_epoch, Vec::new())
}

// Refresh failures must keep caller and server hint fields explicit.
#[allow(clippy::too_many_arguments)]
pub(crate) fn need_refresh_core_failure(
    ctx: &RequestContext,
    rpc_code: RpcErrorCode,
    reason: RefreshReason,
    message: impl Into<String>,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    hint: Option<RefreshHint>,
) -> CoreFailure {
    let err = match hint {
        Some(hint) => CanonicalError::need_refresh_with_hint(rpc_code, reason, hint, message),
        None => CanonicalError::need_refresh(rpc_code, reason, message),
    };
    core_failure_from_canonical_error(ctx, err, group_id, mount_epoch, route_epoch)
}

pub(crate) fn fatal_fs_core_failure(
    ctx: &RequestContext,
    errno: FsErrorCode,
    message: impl Into<String>,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
) -> CoreFailure {
    core_failure_from_canonical_error(
        ctx,
        CanonicalError::fatal_fs(errno, message),
        group_id,
        mount_epoch,
        None,
    )
}

pub(crate) fn terminal_rpc_core_failure(
    ctx: &RequestContext,
    reason: RefreshReason,
    rpc_code: RpcErrorCode,
    message: impl Into<String>,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
) -> CoreFailure {
    core_failure_from_canonical_error(
        ctx,
        CanonicalError {
            class: ErrorClass::Fatal,
            code: Some(CanonicalErrorCode::RpcCode(rpc_code)),
            reason: Some(reason),
            retry_after_ms: None,
            message: message.into(),
            refresh_hint: None,
        },
        group_id,
        mount_epoch,
        None,
    )
}

pub fn ok_header_from_request(
    req_header: &Option<proto::common::RequestHeaderProto>,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
) -> proto::common::ResponseHeaderProto {
    let ctx = request_context_from_proto(req_header);
    ok_header_from_context(&ctx, group_id, mount_epoch, None, Vec::new())
}

pub fn header_from_canonical_error(
    req_header: &Option<proto::common::RequestHeaderProto>,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
    err: &CanonicalError,
) -> proto::common::ResponseHeaderProto {
    let ctx = request_context_from_proto(req_header);
    header_from_canonical_error_with_context(&ctx, group_id, mount_epoch, None, Vec::new(), err)
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
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
) -> proto::common::ResponseHeaderProto {
    let err = CanonicalError::fatal_fs(errno, message);
    header_from_canonical_error(req_header, group_id, mount_epoch, &err)
}

pub fn need_refresh_header(
    req_header: &Option<proto::common::RequestHeaderProto>,
    rpc_code: RpcErrorCode,
    reason: common::error::canonical::RefreshReason,
    message: impl Into<String>,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
) -> proto::common::ResponseHeaderProto {
    let hint = RefreshHint {
        group_id,
        mount_epoch,
        ..Default::default()
    };
    let err = CanonicalError::need_refresh_with_hint(rpc_code, reason, hint, message);
    header_from_canonical_error(req_header, group_id, mount_epoch, &err)
}

pub fn retryable_header(
    req_header: &Option<proto::common::RequestHeaderProto>,
    rpc_code: RpcErrorCode,
    retry_after_ms: Option<u64>,
    message: impl Into<String>,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
) -> proto::common::ResponseHeaderProto {
    let err = CanonicalError::retryable(rpc_code, retry_after_ms, message);
    header_from_canonical_error(req_header, group_id, mount_epoch, &err)
}

pub fn permission_denied_canonical_error(op: Option<&str>, detail: Option<&str>) -> CanonicalError {
    let message = match (op, detail) {
        (Some(op), Some(detail)) => format!("permission denied: op={} target={}", op, detail),
        (Some(op), None) => format!("permission denied: op={}", op),
        (None, Some(detail)) => format!("permission denied: target={}", detail),
        (None, None) => "permission denied".to_string(),
    };
    CanonicalError::fatal_fs(FsErrorCode::EAcces, message)
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
    token.map(|token_proto| PresentedFencingToken {
        block_id: token_proto.block_id.and_then(|block| BlockId::try_from(block).ok()),
        owner: token_proto.owner,
        epoch: token_proto.epoch,
    })
}

pub fn fencing_to_proto(token: FencingToken) -> proto::common::FencingTokenProto {
    token.into()
}

pub fn worker_endpoint_from_parts(
    worker_id: WorkerId,
    endpoint: String,
    worker_net_protocol: i32,
    worker_epoch: u64,
    worker_run_id: types::WorkerRunId,
) -> Result<WorkerEndpointInfo, MetadataError> {
    proto::convert::worker_endpoint_info_from_parts(
        worker_id,
        endpoint,
        worker_net_protocol,
        worker_epoch,
        worker_run_id.to_string(),
    )
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
    use super::header_from_canonical_error;
    use common::error::canonical::{CanonicalError, RefreshReason};
    use common::header::{RequestHeader, RpcErrorCode};
    use types::ClientId;

    #[test]
    fn filesystem_header_builder_does_not_emit_moved_reason() {
        let req_header: proto::common::RequestHeaderProto = (&RequestHeader::new(ClientId::new(1))).into();
        let req_header = Some(req_header);
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::ShardMoved,
            RefreshReason::Moved,
            "moved should be de-scoped",
        );

        let header = header_from_canonical_error(&req_header, Some(1), Some(7), &canonical);
        let error = header.error.expect("expected canonical error header");
        assert_eq!(
            error.refresh_reason,
            proto::common::RefreshReasonProto::RefreshReasonUnknown as i32
        );
    }
}
