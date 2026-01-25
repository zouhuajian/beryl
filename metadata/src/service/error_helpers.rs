// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Helper functions for constructing ResponseHeaderProto from CanonicalError.
//!
//! This module provides unified helpers for converting canonical CanonicalError
//! to proto ResponseHeaderProto, ensuring consistent error semantics across
//! all metadata service handlers.
//!
//! NOTE: These helpers map CanonicalError to ErrorDetailProto (single canonical error field).
//! All error semantics are expressed through ResponseHeaderProto.error (ErrorDetailProto).

use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode};
use common::header::{ResponseHeader, RpcErrorCode};
use types::fs::FsErrorCode;

use super::extract_and_inject_context;

/// Create a successful response header from request.
pub fn ok_header_from_request(
    req_header: &Option<proto::common::RequestHeaderProto>,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
) -> proto::common::ResponseHeaderProto {
    let caller_ctx = extract_and_inject_context(req_header);
    let mut resp_header = ResponseHeader::ok(caller_ctx.client).with_group_id(group_id.unwrap_or(0));

    // Set state_id if available from request
    if let Some(header) = req_header {
        if let Some(ref state_id) = header.state_id {
            resp_header.state_id = Some(types::RaftLogId {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            });
        }
    }

    let mut proto_header: proto::common::ResponseHeaderProto = (&resp_header).into();
    if let Some(epoch) = mount_epoch {
        proto_header.mount_epoch = Some(epoch);
    }
    proto_header
}

/// Create a response header from CanonicalError.
///
/// This function maps CanonicalError to the new proto structure:
/// - `CanonicalError` -> `ResponseHeaderProto.error` (ErrorDetailProto)
/// - Only writes to the single canonical error field; no duplicate fields.
///
/// Invariants enforced:
/// - `class == Ok` => `error` field is `None`
/// - `class == NeedRefresh` => `error.refresh_reason` must be set
/// - `class == Retryable` => `error.retry_after_ms` is optional
pub fn header_from_canonical_error(
    req_header: &Option<proto::common::RequestHeaderProto>,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
    err: &CanonicalError,
) -> proto::common::ResponseHeaderProto {
    let caller_ctx = extract_and_inject_context(req_header);

    // Enforce invariant: Ok must not carry error fields
    debug_assert!(
        err.class != ErrorClass::Ok || (err.code.is_none() && err.reason.is_none() && err.retry_after_ms.is_none()),
        "CanonicalError invariant violated: Ok must not carry code/reason/retry_after_ms"
    );

    // Enforce invariant: NeedRefresh must have reason
    debug_assert!(
        err.class != ErrorClass::NeedRefresh || err.reason.is_some(),
        "CanonicalError invariant violated: NeedRefresh must have reason"
    );

    // Build ErrorDetailProto
    let error_detail = if err.class == ErrorClass::Ok {
        None
    } else {
        // Map ErrorClass to ErrorClassProto
        let error_class = match err.class {
            ErrorClass::Ok => proto::common::ErrorClassProto::ErrorClassOk,
            ErrorClass::NeedRefresh => proto::common::ErrorClassProto::ErrorClassNeedRefresh,
            ErrorClass::Retryable => proto::common::ErrorClassProto::ErrorClassRetryable,
            ErrorClass::Fatal => proto::common::ErrorClassProto::ErrorClassFatal,
        };

        // Map ErrorCode to oneof (FsErrno or RpcCode)
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
                    RpcErrorCode::WorkerEpochMismatch => {
                        proto::common::RpcErrorCodeProto::RpcErrCodeWorkerEpochMismatch
                    }
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

        // Map RefreshReason to RefreshReasonProto
        let refresh_reason = err
            .reason
            .map(|r| match r {
                common::error::canonical::RefreshReason::Unknown => {
                    proto::common::RefreshReasonProto::RefreshReasonUnknown
                }
                common::error::canonical::RefreshReason::NotLeader => {
                    proto::common::RefreshReasonProto::RefreshReasonNotLeader
                }
                common::error::canonical::RefreshReason::Moved => proto::common::RefreshReasonProto::RefreshReasonMoved,
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
                common::error::canonical::RefreshReason::Fencing => {
                    proto::common::RefreshReasonProto::RefreshReasonFencing
                }
                common::error::canonical::RefreshReason::EpochMismatch => {
                    proto::common::RefreshReasonProto::RefreshReasonEpochMismatch
                }
            })
            .unwrap_or(proto::common::RefreshReasonProto::RefreshReasonUnknown);

        Some(proto::common::ErrorDetailProto {
            error_class: error_class as i32,
            code,
            refresh_reason: refresh_reason as i32,
            retry_after_ms: err.retry_after_ms,
            message: err.message.clone(),
        })
    };

    // Build ResponseHeader (for state_id handling)
    let mut resp_header = ResponseHeader::ok(caller_ctx.client).with_group_id(group_id.unwrap_or(0));

    // Set state_id if available from request
    if let Some(header) = req_header {
        if let Some(ref state_id) = header.state_id {
            resp_header.state_id = Some(types::RaftLogId {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            });
        }
    }

    // Convert to proto and set error field (single source of truth)
    let mut proto_header: proto::common::ResponseHeaderProto = (&resp_header).into();
    proto_header.error = error_detail;

    // Set mount_epoch if provided
    if let Some(epoch) = mount_epoch {
        proto_header.mount_epoch = Some(epoch);
    }

    proto_header
}

/// Convenience helper: Create a fatal FS error response.
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

/// Convenience helper: Create a NEED_REFRESH error response.
pub fn need_refresh_header(
    req_header: &Option<proto::common::RequestHeaderProto>,
    rpc_code: RpcErrorCode,
    reason: common::error::canonical::RefreshReason,
    message: impl Into<String>,
    group_id: Option<u64>,
    mount_epoch: Option<u64>,
) -> proto::common::ResponseHeaderProto {
    let err = CanonicalError::need_refresh(rpc_code, reason, message);
    header_from_canonical_error(req_header, group_id, mount_epoch, &err)
}

/// Convenience helper: Create a RETRYABLE error response.
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
