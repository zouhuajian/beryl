// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Core wire/context utilities shared by service adapters and FsCore.

use super::domain::{
    CoreFailure, CoreSuccess, FileBlockLocation, PresentedFencingToken, RequestContext, WorkerHint, WriteTarget,
};
use crate::error::{to_canonical_fs, MetadataError};
use common::error::canonical::{
    CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshHint, RefreshReason, WorkerEndpointHint,
};
use common::header::{RequestHeader, ResponseHeader, RpcErrorCode};
use tracing::Span;
use types::fs::FsErrorCode;
use types::ids::{BlockId, BlockIndex, DataHandleId, LeaseId};
use types::layout::FileLayout;
use types::lease::FencingToken;
use types::GroupStateWatermark;

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

    let error_class = match err.class {
        ErrorClass::Ok => proto::common::ErrorClassProto::ErrorClassOk,
        ErrorClass::NeedRefresh => proto::common::ErrorClassProto::ErrorClassNeedRefresh,
        ErrorClass::Retryable => proto::common::ErrorClassProto::ErrorClassRetryable,
        ErrorClass::Fatal => proto::common::ErrorClassProto::ErrorClassFatal,
    };
    let code = match &err.code {
        Some(CanonicalErrorCode::FsErrno(errno)) => {
            let fs_errno = match errno {
                types::fs::FsErrorCode::Ok => proto::common::FsErrnoProto::FsErrnoOk,
                types::fs::FsErrorCode::ENoEnt => proto::common::FsErrnoProto::FsErrnoEnoent,
                types::fs::FsErrorCode::EExist => proto::common::FsErrnoProto::FsErrnoEexist,
                types::fs::FsErrorCode::ENotEmpty => proto::common::FsErrnoProto::FsErrnoEnotempty,
                types::fs::FsErrorCode::ENotDir => proto::common::FsErrnoProto::FsErrnoEnotdir,
                types::fs::FsErrorCode::EIsDir => proto::common::FsErrnoProto::FsErrnoEisdir,
                types::fs::FsErrorCode::EXDev => proto::common::FsErrnoProto::FsErrnoExdev,
                types::fs::FsErrorCode::EPerm => proto::common::FsErrnoProto::FsErrnoEperm,
                types::fs::FsErrorCode::EAcces => proto::common::FsErrnoProto::FsErrnoEacces,
                types::fs::FsErrorCode::EInval => proto::common::FsErrnoProto::FsErrnoEinval,
                types::fs::FsErrorCode::ENotsup => proto::common::FsErrnoProto::FsErrnoEnotsup,
                types::fs::FsErrorCode::ENotImpl => proto::common::FsErrnoProto::FsErrnoEnotimpl,
                types::fs::FsErrorCode::EAgain => proto::common::FsErrnoProto::FsErrnoEagain,
                types::fs::FsErrorCode::EBusy => proto::common::FsErrnoProto::FsErrnoEbusy,
            };
            Some(proto::common::error_detail_proto::Code::FsErrno(fs_errno as i32))
        }
        Some(CanonicalErrorCode::RpcCode(rpc_code)) => {
            let rpc_code_proto = match rpc_code {
                RpcErrorCode::Unspecified => proto::common::RpcErrorCodeProto::RpcErrCodeUnspecified,
                RpcErrorCode::NoSuchMethod => proto::common::RpcErrorCodeProto::RpcErrCodeNoSuchMethod,
                RpcErrorCode::InvalidHeader => proto::common::RpcErrorCodeProto::RpcErrCodeInvalidHeader,
                RpcErrorCode::VersionMismatch => proto::common::RpcErrorCodeProto::RpcErrCodeVersionMismatch,
                RpcErrorCode::DeserializeRequest => proto::common::RpcErrorCodeProto::RpcErrCodeDeserializeRequest,
                RpcErrorCode::SerializeResponse => proto::common::RpcErrorCodeProto::RpcErrCodeSerializeResponse,
                RpcErrorCode::Unauthenticated => proto::common::RpcErrorCodeProto::RpcErrCodeUnauthenticated,
                RpcErrorCode::PermissionDenied => proto::common::RpcErrorCodeProto::RpcErrCodePermissionDenied,
                RpcErrorCode::NotLeader => proto::common::RpcErrorCodeProto::RpcErrCodeNotLeader,
                RpcErrorCode::StaleState => proto::common::RpcErrorCodeProto::RpcErrCodeStaleState,
                RpcErrorCode::MountEpochMismatch => proto::common::RpcErrorCodeProto::RpcErrCodeMountEpochMismatch,
                RpcErrorCode::RouteEpochMismatch => proto::common::RpcErrorCodeProto::RpcErrCodeRouteEpochMismatch,
                RpcErrorCode::WorkerEpochMismatch => proto::common::RpcErrorCodeProto::RpcErrCodeWorkerEpochMismatch,
                RpcErrorCode::BlockStampMismatch => proto::common::RpcErrorCodeProto::RpcErrCodeBlockStampMismatch,
                RpcErrorCode::EpochMismatch => proto::common::RpcErrorCodeProto::RpcErrCodeEpochMismatch,
                RpcErrorCode::Fencing => proto::common::RpcErrorCodeProto::RpcErrCodeFencing,
                RpcErrorCode::ShardMoved => proto::common::RpcErrorCodeProto::RpcErrCodeShardMoved,
                RpcErrorCode::NodeUnavailable => proto::common::RpcErrorCodeProto::RpcErrCodeNodeUnavailable,
                RpcErrorCode::Application => proto::common::RpcErrorCodeProto::RpcErrCodeApplication,
            };
            Some(proto::common::error_detail_proto::Code::RpcCode(rpc_code_proto as i32))
        }
        None => None,
    };
    let refresh_reason = err
        .reason
        .map(|r| match r {
            common::error::canonical::RefreshReason::Unknown => proto::common::RefreshReasonProto::RefreshReasonUnknown,
            common::error::canonical::RefreshReason::NotLeader => {
                proto::common::RefreshReasonProto::RefreshReasonNotLeader
            }
            // MOVED is de-scoped for FileSystemService; do not emit it on the wire.
            common::error::canonical::RefreshReason::Moved => proto::common::RefreshReasonProto::RefreshReasonUnknown,
            common::error::canonical::RefreshReason::StaleState => {
                proto::common::RefreshReasonProto::RefreshReasonStaleState
            }
            common::error::canonical::RefreshReason::MountEpochMismatch => {
                proto::common::RefreshReasonProto::RefreshReasonMountEpochMismatch
            }
            common::error::canonical::RefreshReason::RouteEpochMismatch => {
                proto::common::RefreshReasonProto::RefreshReasonRouteEpochMismatch
            }
            common::error::canonical::RefreshReason::WorkerEpochMismatch => {
                proto::common::RefreshReasonProto::RefreshReasonWorkerEpochMismatch
            }
            common::error::canonical::RefreshReason::BlockStampMismatch => {
                proto::common::RefreshReasonProto::RefreshReasonBlockStampMismatch
            }
            common::error::canonical::RefreshReason::Fencing => proto::common::RefreshReasonProto::RefreshReasonFencing,
            common::error::canonical::RefreshReason::EpochMismatch => {
                proto::common::RefreshReasonProto::RefreshReasonEpochMismatch
            }
            common::error::canonical::RefreshReason::SessionInvalid => {
                proto::common::RefreshReasonProto::RefreshReasonSessionInvalid
            }
            common::error::canonical::RefreshReason::SessionExpired => {
                proto::common::RefreshReasonProto::RefreshReasonSessionExpired
            }
        })
        .unwrap_or(proto::common::RefreshReasonProto::RefreshReasonUnknown);

    Some(proto::common::ErrorDetailProto {
        error_class: error_class as i32,
        code,
        refresh_reason: refresh_reason as i32,
        retry_after_ms: err.retry_after_ms,
        message: err.message.clone(),
        refresh_hint: map_refresh_hint_to_proto(err.refresh_hint.as_ref()),
    })
}

fn map_refresh_hint_to_proto(hint: Option<&RefreshHint>) -> Option<proto::common::RefreshHintProto> {
    hint.map(|hint| proto::common::RefreshHintProto {
        leader_endpoint: hint.leader_endpoint.clone(),
        group_id: hint.group_id,
        mount_epoch: hint.mount_epoch,
        mount_prefix: hint.mount_prefix.clone(),
        route_epoch: hint.route_epoch,
        worker_epoch: hint.worker_epoch,
        worker_endpoints: hint
            .worker_endpoints
            .iter()
            .map(map_worker_endpoint_hint_to_proto)
            .collect(),
        worker_resolve_required: hint.worker_resolve_required,
    })
}

fn map_worker_endpoint_hint_to_proto(endpoint: &WorkerEndpointHint) -> proto::common::WorkerEndpointInfoProto {
    proto::common::WorkerEndpointInfoProto {
        worker_id: endpoint.worker_id,
        endpoint: endpoint.endpoint.clone(),
        net_transport_kind: endpoint.net_transport_kind,
        worker_epoch: endpoint.worker_epoch,
    }
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
    Ok(FileLayout::new(
        layout.block_size,
        layout.chunk_size,
        layout.replication as u8,
    ))
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
        let lease_id_raw = (lease_id_proto.high as u128) << 64 | lease_id_proto.low as u128;
        LeaseId::new(lease_id_raw)
    })
}

pub fn lease_id_to_proto(lease_id: LeaseId) -> proto::common::LeaseIdProto {
    proto::common::LeaseIdProto {
        high: (lease_id.as_raw() >> 64) as u64,
        low: lease_id.as_raw() as u64,
    }
}

pub fn presented_fencing_from_proto(token: Option<proto::common::FencingTokenProto>) -> Option<PresentedFencingToken> {
    token.map(|token_proto| PresentedFencingToken {
        block_id: token_proto.block_id.map(|block| {
            BlockId::new(
                DataHandleId::new(block.data_handle_id),
                BlockIndex::new(block.block_index),
            )
        }),
        owner: token_proto.owner,
        epoch: token_proto.epoch,
    })
}

pub fn fencing_to_proto(token: FencingToken) -> proto::common::FencingTokenProto {
    proto::common::FencingTokenProto {
        block_id: Some(proto::common::BlockIdProto {
            data_handle_id: token.block_id.data_handle_id.as_raw(),
            block_index: token.block_id.index.as_raw(),
        }),
        owner: token.owner.as_raw(),
        epoch: token.epoch,
    }
}

pub fn worker_hint_to_proto(hint: &WorkerHint) -> proto::common::WorkerEndpointInfoProto {
    proto::common::WorkerEndpointInfoProto {
        worker_id: hint.worker_id.as_raw(),
        endpoint: hint.endpoint.clone(),
        net_transport_kind: hint.net_transport_kind,
        worker_epoch: hint.worker_epoch,
    }
}

pub fn write_target_to_proto(target: &WriteTarget) -> proto::metadata::WriteTargetProto {
    proto::metadata::WriteTargetProto {
        block_id: Some(proto::common::BlockIdProto {
            data_handle_id: target.block_id.data_handle_id.as_raw(),
            block_index: target.block_id.index.as_raw(),
        }),
        file_offset: target.file_offset,
        len: target.len,
        worker_endpoints: target.worker_endpoints.iter().map(worker_hint_to_proto).collect(),
        fencing_token: Some(fencing_to_proto(target.fencing_token)),
    }
}

pub fn location_to_proto(location: &FileBlockLocation) -> proto::metadata::FileBlockLocationProto {
    proto::metadata::FileBlockLocationProto {
        block_id: Some(proto::common::BlockIdProto {
            data_handle_id: location.block_id.data_handle_id.as_raw(),
            block_index: location.block_id.index.as_raw(),
        }),
        file_offset: location.file_offset,
        len: location.len,
        workers: location.workers.iter().map(worker_hint_to_proto).collect(),
        worker_epoch: location.worker_epoch,
    }
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
