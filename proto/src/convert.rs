// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Conversion utilities between proto messages and types crate.
//!
//! This module provides bidirectional conversion between proto messages
//! and domain types defined in the types crate.

use crate::common as proto_common;
use ::common::{
    Deadline,
    error::canonical::{
        CanonicalError, ErrorClass as CanonicalErrorClass, ErrorCode as CanonicalErrorCode,
        RefreshHint as CanonicalRefreshHint, RefreshReason, WorkerEndpointHint,
    },
    header::{AuthnType, CallerContext, ClientInfo, RequestHeader, ResponseHeader, RpcErrorCode, RpcStatus},
};
use std::str::FromStr;
use types::ids::{
    BlockId, BlockIndex, ChunkId, ChunkIndex, DataHandleId, LeaseId, MountId, ShardGroupId, ShardId, StreamId, WorkerId,
};
use types::{CallId, ClientId, GroupStateWatermark, RaftLogId};

// ============================================================================
// ID Conversions
// ============================================================================

impl From<DataHandleId> for proto_common::DataHandleIdProto {
    fn from(id: DataHandleId) -> Self {
        proto_common::DataHandleIdProto { value: id.as_raw() }
    }
}

impl TryFrom<proto_common::DataHandleIdProto> for DataHandleId {
    type Error = ();

    fn try_from(id: proto_common::DataHandleIdProto) -> Result<Self, Self::Error> {
        Ok(DataHandleId::new(id.value))
    }
}

impl From<BlockId> for proto_common::BlockIdProto {
    fn from(id: BlockId) -> Self {
        proto_common::BlockIdProto {
            data_handle_id: id.data_handle_id.as_raw(),
            block_index: id.index.as_raw(),
        }
    }
}

impl TryFrom<proto_common::BlockIdProto> for BlockId {
    type Error = ();

    fn try_from(id: proto_common::BlockIdProto) -> Result<Self, Self::Error> {
        Ok(BlockId::new(
            DataHandleId::new(id.data_handle_id),
            BlockIndex::new(id.block_index),
        ))
    }
}

impl From<ChunkId> for proto_common::ChunkIdProto {
    fn from(id: ChunkId) -> Self {
        proto_common::ChunkIdProto {
            block: Some(id.block.into()),
            chunk_index: id.index.as_raw(),
        }
    }
}

impl TryFrom<proto_common::ChunkIdProto> for ChunkId {
    type Error = ();

    fn try_from(id: proto_common::ChunkIdProto) -> Result<Self, Self::Error> {
        let block = id.block.ok_or(())?.try_into()?;
        Ok(ChunkId::new(block, ChunkIndex::new(id.chunk_index)))
    }
}

impl From<WorkerId> for proto_common::WorkerIdProto {
    fn from(id: WorkerId) -> Self {
        proto_common::WorkerIdProto { value: id.as_raw() }
    }
}

impl TryFrom<proto_common::WorkerIdProto> for WorkerId {
    type Error = ();

    fn try_from(id: proto_common::WorkerIdProto) -> Result<Self, Self::Error> {
        Ok(WorkerId::new(id.value))
    }
}

impl From<StreamId> for proto_common::StreamIdProto {
    fn from(id: StreamId) -> Self {
        let value = id.as_raw();
        proto_common::StreamIdProto {
            high: (value >> 64) as u64,
            low: value as u64,
        }
    }
}

impl TryFrom<proto_common::StreamIdProto> for StreamId {
    type Error = ();

    fn try_from(id: proto_common::StreamIdProto) -> Result<Self, Self::Error> {
        let value = ((id.high as u128) << 64) | (id.low as u128);
        Ok(StreamId::new(value))
    }
}

impl From<LeaseId> for proto_common::LeaseIdProto {
    fn from(id: LeaseId) -> Self {
        let value = id.as_raw();
        proto_common::LeaseIdProto {
            high: (value >> 64) as u64,
            low: value as u64,
        }
    }
}

impl TryFrom<proto_common::LeaseIdProto> for LeaseId {
    type Error = ();

    fn try_from(id: proto_common::LeaseIdProto) -> Result<Self, Self::Error> {
        let value = ((id.high as u128) << 64) | (id.low as u128);
        Ok(LeaseId::new(value))
    }
}

impl From<ShardId> for proto_common::ShardIdProto {
    fn from(id: ShardId) -> Self {
        proto_common::ShardIdProto { value: id.as_raw() }
    }
}

impl TryFrom<proto_common::ShardIdProto> for ShardId {
    type Error = ();

    fn try_from(id: proto_common::ShardIdProto) -> Result<Self, Self::Error> {
        Ok(ShardId::new(id.value))
    }
}

impl From<ShardGroupId> for proto_common::ShardGroupIdProto {
    fn from(id: ShardGroupId) -> Self {
        proto_common::ShardGroupIdProto { value: id.as_raw() }
    }
}

impl TryFrom<proto_common::ShardGroupIdProto> for ShardGroupId {
    type Error = ();

    fn try_from(id: proto_common::ShardGroupIdProto) -> Result<Self, Self::Error> {
        Ok(ShardGroupId::new(id.value))
    }
}

impl From<MountId> for proto_common::MountIdProto {
    fn from(id: MountId) -> Self {
        proto_common::MountIdProto { value: id.as_raw() }
    }
}

impl TryFrom<proto_common::MountIdProto> for MountId {
    type Error = ();

    fn try_from(id: proto_common::MountIdProto) -> Result<Self, Self::Error> {
        Ok(MountId::new(id.value))
    }
}

// ============================================================================
// RaftLogIdProto Conversions
// ============================================================================

impl From<&RaftLogId> for proto_common::RaftLogIdProto {
    fn from(log_id: &RaftLogId) -> Self {
        proto_common::RaftLogIdProto {
            term: log_id.term,
            leader_node_id: log_id.leader_node_id,
            index: log_id.index,
        }
    }
}

impl From<RaftLogId> for proto_common::RaftLogIdProto {
    fn from(log_id: RaftLogId) -> Self {
        proto_common::RaftLogIdProto {
            term: log_id.term,
            leader_node_id: log_id.leader_node_id,
            index: log_id.index,
        }
    }
}

impl From<proto_common::RaftLogIdProto> for RaftLogId {
    fn from(state_id: proto_common::RaftLogIdProto) -> Self {
        RaftLogId::new(state_id.term, state_id.leader_node_id, state_id.index)
    }
}

impl TryFrom<proto_common::GroupStateWatermarkProto> for GroupStateWatermark {
    type Error = String;

    fn try_from(proto: proto_common::GroupStateWatermarkProto) -> Result<Self, Self::Error> {
        let group_id = proto
            .group_id
            .ok_or_else(|| "missing group_id in GroupStateWatermarkProto".to_string())?
            .try_into()
            .map_err(|_| "invalid group_id in GroupStateWatermarkProto".to_string())?;
        let state_id = proto
            .state_id
            .ok_or_else(|| "missing state_id in GroupStateWatermarkProto".to_string())?
            .into();
        Ok(GroupStateWatermark::new(group_id, state_id))
    }
}

impl From<&GroupStateWatermark> for proto_common::GroupStateWatermarkProto {
    fn from(watermark: &GroupStateWatermark) -> Self {
        proto_common::GroupStateWatermarkProto {
            group_id: Some(watermark.group_id.into()),
            state_id: Some(watermark.state_id.into()),
        }
    }
}

// ============================================================================
// RequestHeaderProto / ResponseHeaderProto Conversions
// ============================================================================
//
// NOTE: This is the AUTHORITATIVE implementation of conversions between
// proto::common::RequestHeaderProto/ResponseHeaderProto and common::header types.
// All conversions MUST use these implementations.

impl TryFrom<proto_common::ClientInfoProto> for ClientInfo {
    type Error = String;

    fn try_from(proto: proto_common::ClientInfoProto) -> Result<Self, Self::Error> {
        let call_id = CallId::from_str(&proto.call_id).map_err(|e| format!("Invalid call_id: {}", e))?;
        let client_id = ClientId::new(proto.client_id);
        let client_name = if proto.client_name.is_empty() {
            None
        } else {
            Some(proto.client_name)
        };

        Ok(ClientInfo {
            call_id,
            client_id,
            client_name,
        })
    }
}

impl From<&ClientInfo> for proto_common::ClientInfoProto {
    fn from(info: &ClientInfo) -> Self {
        proto_common::ClientInfoProto {
            call_id: info.call_id.to_string(),
            client_id: info.client_id.as_raw(),
            client_name: info.client_name.clone().unwrap_or_default(),
        }
    }
}

impl TryFrom<proto_common::RequestHeaderProto> for RequestHeader {
    type Error = String;

    fn try_from(proto: proto_common::RequestHeaderProto) -> Result<Self, Self::Error> {
        let client = proto.client.ok_or_else(|| "missing client".to_string())?.try_into()?;
        let deadline = Deadline::from_unix_ms(proto.deadline_ms);
        let traceparent = if proto.traceparent.is_empty() {
            None
        } else {
            Some(proto.traceparent)
        };
        let caller_context = proto.caller_context.map(|cc| CallerContext {
            context: cc.context,
            signature: if cc.signature.is_empty() {
                None
            } else {
                Some(cc.signature)
            },
        });
        let state = proto
            .state
            .into_iter()
            .map(GroupStateWatermark::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let principal = if proto.principal.is_empty() {
            None
        } else {
            Some(proto.principal)
        };
        let real_user = if proto.real_user.is_empty() {
            None
        } else {
            Some(proto.real_user)
        };
        let doas = if proto.doas.is_empty() { None } else { Some(proto.doas) };
        let authn_type = match proto_common::AuthnTypeProto::try_from(proto.authn_type) {
            Ok(proto_common::AuthnTypeProto::Simple) => AuthnType::Simple,
            Ok(proto_common::AuthnTypeProto::Kerberos) => AuthnType::Kerberos,
            Ok(proto_common::AuthnTypeProto::Token) => AuthnType::Token,
            _ => AuthnType::Unspecified,
        };

        Ok(RequestHeader {
            client,
            group_id: if proto.group_id == 0 {
                None
            } else {
                Some(proto.group_id)
            },
            mount_epoch: proto.mount_epoch,
            deadline,
            traceparent,
            caller_context,
            state,
            retry_count: proto.retry_count,
            route_epoch: proto.route_epoch,
            principal,
            real_user,
            doas,
            authn_type,
        })
    }
}

impl From<&RequestHeader> for proto_common::RequestHeaderProto {
    fn from(header: &RequestHeader) -> Self {
        proto_common::RequestHeaderProto {
            client: Some((&header.client).into()),
            deadline_ms: header.deadline.as_unix_ms(),
            traceparent: header.traceparent.clone().unwrap_or_default(),
            caller_context: header
                .caller_context
                .as_ref()
                .map(|cc| proto_common::CallerContextProto {
                    context: cc.context.clone(),
                    signature: cc.signature.clone().unwrap_or_default(),
                }),
            state: header
                .state
                .iter()
                .map(proto_common::GroupStateWatermarkProto::from)
                .collect(),
            retry_count: header.retry_count,
            group_id: header.group_id.unwrap_or(0),
            mount_epoch: header.mount_epoch,
            route_epoch: header.route_epoch,
            principal: header.principal.clone().unwrap_or_default(),
            real_user: header.real_user.clone().unwrap_or_default(),
            doas: header.doas.clone().unwrap_or_default(),
            authn_type: match header.authn_type {
                AuthnType::Unspecified => proto_common::AuthnTypeProto::Unspecified as i32,
                AuthnType::Simple => proto_common::AuthnTypeProto::Simple as i32,
                AuthnType::Kerberos => proto_common::AuthnTypeProto::Kerberos as i32,
                AuthnType::Token => proto_common::AuthnTypeProto::Token as i32,
            },
        }
    }
}

impl TryFrom<proto_common::ResponseHeaderProto> for ResponseHeader {
    type Error = String;

    fn try_from(proto: proto_common::ResponseHeaderProto) -> Result<Self, Self::Error> {
        let client = proto
            .client
            .clone()
            .ok_or_else(|| "missing client".to_string())?
            .try_into()?;

        let mut canonical_error = proto.error.as_ref().map(error_detail_to_canonical);
        if matches!(
            canonical_error.as_ref().map(|err| err.class),
            Some(CanonicalErrorClass::Ok)
        ) {
            canonical_error = None;
        }

        let status = match canonical_error
            .as_ref()
            .map(|err| err.class)
            .unwrap_or(CanonicalErrorClass::Ok)
        {
            CanonicalErrorClass::Ok => RpcStatus::Ok,
            CanonicalErrorClass::NeedRefresh | CanonicalErrorClass::Retryable => RpcStatus::Error,
            CanonicalErrorClass::Fatal => RpcStatus::Fatal,
        };
        debug_assert!(
            (status == RpcStatus::Ok) == canonical_error.is_none(),
            "status must align with canonical_error presence"
        );

        let state = proto
            .state
            .into_iter()
            .map(GroupStateWatermark::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResponseHeader {
            client,
            status,
            canonical_error,
            state,
            group_id: if proto.group_id == 0 {
                None
            } else {
                Some(proto.group_id)
            },
            mount_epoch: proto.mount_epoch,
            route_epoch: proto.route_epoch,
        })
    }
}

impl From<&ResponseHeader> for proto_common::ResponseHeaderProto {
    fn from(header: &ResponseHeader) -> Self {
        let canonical_owned = match header.status {
            RpcStatus::Ok => {
                debug_assert!(
                    header.canonical_error.is_none(),
                    "status Ok must not carry canonical_error; dropping unexpected value"
                );
                None
            }
            RpcStatus::Error | RpcStatus::Fatal => header.canonical_error.clone().or_else(|| {
                debug_assert!(
                    false,
                    "non-Ok response missing canonical_error; emitting fallback fatal canonical error"
                );
                Some(CanonicalError {
                    class: CanonicalErrorClass::Fatal,
                    code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                    reason: None,
                    retry_after_ms: None,
                    message: format!("missing canonical_error for status {:?}", header.status),
                    refresh_hint: None,
                })
            }),
        };

        let error_detail = canonical_owned.as_ref().map(canonical_to_error_detail);

        proto_common::ResponseHeaderProto {
            client: Some((&header.client).into()),
            error: error_detail,
            state: header
                .state
                .iter()
                .map(proto_common::GroupStateWatermarkProto::from)
                .collect(),
            group_id: header.group_id.unwrap_or(0),
            mount_epoch: header.mount_epoch,
            route_epoch: header.route_epoch,
        }
    }
}

// ============================================================================
// CallerContextProto Conversions
// ============================================================================

impl From<&CallerContext> for proto_common::CallerContextProto {
    fn from(ctx: &CallerContext) -> Self {
        proto_common::CallerContextProto {
            context: ctx.context.clone(),
            signature: ctx.signature.clone().unwrap_or_default(),
        }
    }
}

impl From<proto_common::CallerContextProto> for CallerContext {
    fn from(proto: proto_common::CallerContextProto) -> Self {
        CallerContext {
            context: proto.context,
            signature: if proto.signature.is_empty() {
                None
            } else {
                Some(proto.signature)
            },
        }
    }
}

// ============================================================================
// DataRequestHeaderProto / DataResponseHeaderProto Conversions
// ============================================================================
//
// NOTE: These conversions are implemented in worker crate's convert module
// to avoid circular dependencies (proto should not depend on worker).

// ============================================================================
// Canonical error helpers (shared between control/data-plane conversions)
// ============================================================================

fn fs_errno_proto_to_enum(code: i32) -> types::fs::FsErrorCode {
    match code {
        x if x == proto_common::FsErrnoProto::FsErrnoOk as i32 => types::fs::FsErrorCode::Ok,
        x if x == proto_common::FsErrnoProto::FsErrnoEnoent as i32 => types::fs::FsErrorCode::ENoEnt,
        x if x == proto_common::FsErrnoProto::FsErrnoEexist as i32 => types::fs::FsErrorCode::EExist,
        x if x == proto_common::FsErrnoProto::FsErrnoEnotempty as i32 => types::fs::FsErrorCode::ENotEmpty,
        x if x == proto_common::FsErrnoProto::FsErrnoEnotdir as i32 => types::fs::FsErrorCode::ENotDir,
        x if x == proto_common::FsErrnoProto::FsErrnoEisdir as i32 => types::fs::FsErrorCode::EIsDir,
        x if x == proto_common::FsErrnoProto::FsErrnoExdev as i32 => types::fs::FsErrorCode::EXDev,
        x if x == proto_common::FsErrnoProto::FsErrnoEperm as i32 => types::fs::FsErrorCode::EPerm,
        x if x == proto_common::FsErrnoProto::FsErrnoEacces as i32 => types::fs::FsErrorCode::EAcces,
        x if x == proto_common::FsErrnoProto::FsErrnoEinval as i32 => types::fs::FsErrorCode::EInval,
        x if x == proto_common::FsErrnoProto::FsErrnoEnotsup as i32 => types::fs::FsErrorCode::ENotsup,
        x if x == proto_common::FsErrnoProto::FsErrnoEnotimpl as i32 => types::fs::FsErrorCode::ENotImpl,
        x if x == proto_common::FsErrnoProto::FsErrnoEagain as i32 => types::fs::FsErrorCode::EAgain,
        x if x == proto_common::FsErrnoProto::FsErrnoEbusy as i32 => types::fs::FsErrorCode::EBusy,
        _ => types::fs::FsErrorCode::EInval,
    }
}

fn fs_errno_enum_to_proto(code: &types::fs::FsErrorCode) -> proto_common::FsErrnoProto {
    match code {
        types::fs::FsErrorCode::Ok => proto_common::FsErrnoProto::FsErrnoOk,
        types::fs::FsErrorCode::ENoEnt => proto_common::FsErrnoProto::FsErrnoEnoent,
        types::fs::FsErrorCode::EExist => proto_common::FsErrnoProto::FsErrnoEexist,
        types::fs::FsErrorCode::ENotEmpty => proto_common::FsErrnoProto::FsErrnoEnotempty,
        types::fs::FsErrorCode::ENotDir => proto_common::FsErrnoProto::FsErrnoEnotdir,
        types::fs::FsErrorCode::EIsDir => proto_common::FsErrnoProto::FsErrnoEisdir,
        types::fs::FsErrorCode::EXDev => proto_common::FsErrnoProto::FsErrnoExdev,
        types::fs::FsErrorCode::EPerm => proto_common::FsErrnoProto::FsErrnoEperm,
        types::fs::FsErrorCode::EAcces => proto_common::FsErrnoProto::FsErrnoEacces,
        types::fs::FsErrorCode::EInval => proto_common::FsErrnoProto::FsErrnoEinval,
        types::fs::FsErrorCode::ENotsup => proto_common::FsErrnoProto::FsErrnoEnotsup,
        types::fs::FsErrorCode::ENotImpl => proto_common::FsErrnoProto::FsErrnoEnotimpl,
        types::fs::FsErrorCode::EAgain => proto_common::FsErrnoProto::FsErrnoEagain,
        types::fs::FsErrorCode::EBusy => proto_common::FsErrnoProto::FsErrnoEbusy,
    }
}

fn rpc_code_proto_to_enum(code: i32) -> RpcErrorCode {
    match code {
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeUnspecified as i32 => RpcErrorCode::Unspecified,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeNoSuchMethod as i32 => RpcErrorCode::NoSuchMethod,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeInvalidHeader as i32 => RpcErrorCode::InvalidHeader,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeVersionMismatch as i32 => RpcErrorCode::VersionMismatch,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeDeserializeRequest as i32 => {
            RpcErrorCode::DeserializeRequest
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeSerializeResponse as i32 => {
            RpcErrorCode::SerializeResponse
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeUnauthenticated as i32 => RpcErrorCode::Unauthenticated,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodePermissionDenied as i32 => RpcErrorCode::PermissionDenied,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeNotLeader as i32 => RpcErrorCode::NotLeader,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeStaleState as i32 => RpcErrorCode::StaleState,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeMountEpochMismatch as i32 => {
            RpcErrorCode::MountEpochMismatch
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeRouteEpochMismatch as i32 => {
            RpcErrorCode::RouteEpochMismatch
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeWorkerEpochMismatch as i32 => {
            RpcErrorCode::WorkerEpochMismatch
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeBlockStampMismatch as i32 => {
            RpcErrorCode::BlockStampMismatch
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeEpochMismatch as i32 => RpcErrorCode::EpochMismatch,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeFencing as i32 => RpcErrorCode::Fencing,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeShardMoved as i32 => RpcErrorCode::ShardMoved,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeNodeUnavailable as i32 => RpcErrorCode::NodeUnavailable,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeInvalidArgument as i32 => RpcErrorCode::Application,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeInternal as i32 => RpcErrorCode::Application,
        _ => RpcErrorCode::Application,
    }
}

fn rpc_code_enum_to_proto(code: RpcErrorCode) -> i32 {
    match code {
        RpcErrorCode::Unspecified => proto_common::RpcErrorCodeProto::RpcErrCodeUnspecified as i32,
        RpcErrorCode::NoSuchMethod => proto_common::RpcErrorCodeProto::RpcErrCodeNoSuchMethod as i32,
        RpcErrorCode::InvalidHeader => proto_common::RpcErrorCodeProto::RpcErrCodeInvalidHeader as i32,
        RpcErrorCode::VersionMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeVersionMismatch as i32,
        RpcErrorCode::DeserializeRequest => proto_common::RpcErrorCodeProto::RpcErrCodeDeserializeRequest as i32,
        RpcErrorCode::SerializeResponse => proto_common::RpcErrorCodeProto::RpcErrCodeSerializeResponse as i32,
        RpcErrorCode::Unauthenticated => proto_common::RpcErrorCodeProto::RpcErrCodeUnauthenticated as i32,
        RpcErrorCode::PermissionDenied => proto_common::RpcErrorCodeProto::RpcErrCodePermissionDenied as i32,
        RpcErrorCode::NotLeader => proto_common::RpcErrorCodeProto::RpcErrCodeNotLeader as i32,
        RpcErrorCode::StaleState => proto_common::RpcErrorCodeProto::RpcErrCodeStaleState as i32,
        RpcErrorCode::MountEpochMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeMountEpochMismatch as i32,
        RpcErrorCode::RouteEpochMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeRouteEpochMismatch as i32,
        RpcErrorCode::WorkerEpochMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeWorkerEpochMismatch as i32,
        RpcErrorCode::BlockStampMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeBlockStampMismatch as i32,
        RpcErrorCode::EpochMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeEpochMismatch as i32,
        RpcErrorCode::Fencing => proto_common::RpcErrorCodeProto::RpcErrCodeFencing as i32,
        RpcErrorCode::ShardMoved => proto_common::RpcErrorCodeProto::RpcErrCodeShardMoved as i32,
        RpcErrorCode::NodeUnavailable => proto_common::RpcErrorCodeProto::RpcErrCodeNodeUnavailable as i32,
        RpcErrorCode::Application => proto_common::RpcErrorCodeProto::RpcErrCodeApplication as i32,
    }
}

fn refresh_reason_proto_to_enum(reason: proto_common::RefreshReasonProto) -> RefreshReason {
    match reason {
        proto_common::RefreshReasonProto::RefreshReasonUnknown => RefreshReason::Unknown,
        proto_common::RefreshReasonProto::RefreshReasonNotLeader => RefreshReason::NotLeader,
        proto_common::RefreshReasonProto::RefreshReasonMoved => RefreshReason::Moved,
        proto_common::RefreshReasonProto::RefreshReasonStaleState => RefreshReason::StaleState,
        proto_common::RefreshReasonProto::RefreshReasonMountEpochMismatch => RefreshReason::MountEpochMismatch,
        proto_common::RefreshReasonProto::RefreshReasonRouteEpochMismatch => RefreshReason::RouteEpochMismatch,
        proto_common::RefreshReasonProto::RefreshReasonWorkerEpochMismatch => RefreshReason::WorkerEpochMismatch,
        proto_common::RefreshReasonProto::RefreshReasonBlockStampMismatch => RefreshReason::BlockStampMismatch,
        proto_common::RefreshReasonProto::RefreshReasonFencing => RefreshReason::Fencing,
        proto_common::RefreshReasonProto::RefreshReasonEpochMismatch => RefreshReason::EpochMismatch,
        proto_common::RefreshReasonProto::RefreshReasonSessionInvalid => RefreshReason::SessionInvalid,
        proto_common::RefreshReasonProto::RefreshReasonSessionExpired => RefreshReason::SessionExpired,
    }
}

fn refresh_reason_to_proto(reason: &Option<RefreshReason>) -> i32 {
    match reason.unwrap_or(RefreshReason::Unknown) {
        RefreshReason::Unknown => proto_common::RefreshReasonProto::RefreshReasonUnknown as i32,
        RefreshReason::NotLeader => proto_common::RefreshReasonProto::RefreshReasonNotLeader as i32,
        RefreshReason::Moved => proto_common::RefreshReasonProto::RefreshReasonMoved as i32,
        RefreshReason::StaleState => proto_common::RefreshReasonProto::RefreshReasonStaleState as i32,
        RefreshReason::MountEpochMismatch => proto_common::RefreshReasonProto::RefreshReasonMountEpochMismatch as i32,
        RefreshReason::RouteEpochMismatch => proto_common::RefreshReasonProto::RefreshReasonRouteEpochMismatch as i32,
        RefreshReason::WorkerEpochMismatch => proto_common::RefreshReasonProto::RefreshReasonWorkerEpochMismatch as i32,
        RefreshReason::BlockStampMismatch => proto_common::RefreshReasonProto::RefreshReasonBlockStampMismatch as i32,
        RefreshReason::Fencing => proto_common::RefreshReasonProto::RefreshReasonFencing as i32,
        RefreshReason::EpochMismatch => proto_common::RefreshReasonProto::RefreshReasonEpochMismatch as i32,
        RefreshReason::SessionInvalid => proto_common::RefreshReasonProto::RefreshReasonSessionInvalid as i32,
        RefreshReason::SessionExpired => proto_common::RefreshReasonProto::RefreshReasonSessionExpired as i32,
    }
}

fn refresh_hint_proto_to_hint(hint: Option<&proto_common::RefreshHintProto>) -> Option<CanonicalRefreshHint> {
    hint.map(|hint| CanonicalRefreshHint {
        leader_endpoint: hint.leader_endpoint.clone(),
        group_id: hint.group_id,
        mount_epoch: hint.mount_epoch,
        mount_prefix: hint.mount_prefix.clone(),
        route_epoch: hint.route_epoch,
        worker_epoch: hint.worker_epoch,
        worker_endpoints: hint
            .worker_endpoints
            .iter()
            .map(|endpoint| WorkerEndpointHint {
                worker_id: endpoint.worker_id,
                endpoint: endpoint.endpoint.clone(),
                net_transport_kind: endpoint.net_transport_kind,
                worker_epoch: endpoint.worker_epoch,
            })
            .collect(),
        worker_resolve_required: hint.worker_resolve_required,
    })
}

fn refresh_hint_to_proto(hint: Option<&CanonicalRefreshHint>) -> Option<proto_common::RefreshHintProto> {
    hint.map(|hint| proto_common::RefreshHintProto {
        leader_endpoint: hint.leader_endpoint.clone(),
        group_id: hint.group_id,
        mount_epoch: hint.mount_epoch,
        mount_prefix: hint.mount_prefix.clone(),
        route_epoch: hint.route_epoch,
        worker_epoch: hint.worker_epoch,
        worker_endpoints: hint
            .worker_endpoints
            .iter()
            .map(|endpoint| proto_common::WorkerEndpointInfoProto {
                worker_id: endpoint.worker_id,
                endpoint: endpoint.endpoint.clone(),
                net_transport_kind: endpoint.net_transport_kind,
                worker_epoch: endpoint.worker_epoch,
            })
            .collect(),
        worker_resolve_required: hint.worker_resolve_required,
    })
}

/// Convert proto ErrorDetailProto into canonical error.
pub fn error_detail_to_canonical(err_detail: &proto_common::ErrorDetailProto) -> CanonicalError {
    let class = match err_detail.error_class() {
        proto_common::ErrorClassProto::ErrorClassOk => CanonicalErrorClass::Ok,
        proto_common::ErrorClassProto::ErrorClassNeedRefresh => CanonicalErrorClass::NeedRefresh,
        proto_common::ErrorClassProto::ErrorClassRetryable => CanonicalErrorClass::Retryable,
        proto_common::ErrorClassProto::ErrorClassFatal => CanonicalErrorClass::Fatal,
    };

    let code = match &err_detail.code {
        Some(proto_common::error_detail_proto::Code::FsErrno(errno)) => {
            Some(CanonicalErrorCode::FsErrno(fs_errno_proto_to_enum(*errno)))
        }
        Some(proto_common::error_detail_proto::Code::RpcCode(code)) => {
            Some(CanonicalErrorCode::RpcCode(rpc_code_proto_to_enum(*code)))
        }
        None => None,
    };

    let refresh_proto = proto_common::RefreshReasonProto::try_from(err_detail.refresh_reason)
        .unwrap_or(proto_common::RefreshReasonProto::RefreshReasonUnknown);
    let reason =
        Some(refresh_reason_proto_to_enum(refresh_proto)).filter(|_| !matches!(class, CanonicalErrorClass::Ok));

    CanonicalError {
        class,
        code,
        reason,
        retry_after_ms: err_detail.retry_after_ms,
        message: err_detail.message.clone(),
        refresh_hint: refresh_hint_proto_to_hint(err_detail.refresh_hint.as_ref()),
    }
}

/// Convert canonical error into proto ErrorDetailProto.
pub fn canonical_to_error_detail(err: &CanonicalError) -> proto_common::ErrorDetailProto {
    let error_class = match err.class {
        CanonicalErrorClass::Ok => proto_common::ErrorClassProto::ErrorClassOk,
        CanonicalErrorClass::NeedRefresh => proto_common::ErrorClassProto::ErrorClassNeedRefresh,
        CanonicalErrorClass::Retryable => proto_common::ErrorClassProto::ErrorClassRetryable,
        CanonicalErrorClass::Fatal => proto_common::ErrorClassProto::ErrorClassFatal,
    };

    let code = match &err.code {
        Some(CanonicalErrorCode::FsErrno(errno)) => Some(proto_common::error_detail_proto::Code::FsErrno(
            fs_errno_enum_to_proto(errno) as i32,
        )),
        Some(CanonicalErrorCode::RpcCode(code)) => Some(proto_common::error_detail_proto::Code::RpcCode(
            rpc_code_enum_to_proto(*code),
        )),
        None => None,
    };

    proto_common::ErrorDetailProto {
        error_class: error_class as i32,
        code,
        refresh_reason: refresh_reason_to_proto(&err.reason),
        retry_after_ms: err.retry_after_ms,
        message: err.message.clone(),
        refresh_hint: refresh_hint_to_proto(err.refresh_hint.as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_handle_id_conversion() {
        let data_handle_id = DataHandleId::new(42);
        let proto_id: proto_common::DataHandleIdProto = data_handle_id.into();
        let back: DataHandleId = proto_id.try_into().unwrap();
        assert_eq!(data_handle_id, back);
    }

    #[test]
    fn test_block_id_conversion() {
        let block_id = BlockId::from_u64_u32(42, 7);
        let proto_id: proto_common::BlockIdProto = block_id.into();
        let back: BlockId = proto_id.try_into().unwrap();
        assert_eq!(block_id, back);
    }

    #[test]
    fn test_response_header_proto_to_canonical_need_refresh() {
        use common::error::canonical::ErrorCode as CanonicalErrorCode;
        use common::header::RpcStatus;

        let proto_header = proto_common::ResponseHeaderProto {
            client: Some(proto_common::ClientInfoProto {
                call_id: types::CallId::new().to_string(),
                client_id: 99,
                client_name: String::new(),
            }),
            error: Some(proto_common::ErrorDetailProto {
                error_class: proto_common::ErrorClassProto::ErrorClassNeedRefresh as i32,
                code: Some(proto_common::error_detail_proto::Code::RpcCode(
                    proto_common::RpcErrorCodeProto::RpcErrCodeRouteEpochMismatch as i32,
                )),
                refresh_reason: proto_common::RefreshReasonProto::RefreshReasonRouteEpochMismatch as i32,
                retry_after_ms: None,
                message: "route epoch mismatch".to_string(),
                refresh_hint: None,
            }),
            state: Vec::new(),
            group_id: 10,
            mount_epoch: Some(7),
            route_epoch: Some(9),
        };

        let header: ResponseHeader = proto_header.try_into().unwrap();
        assert_eq!(header.status, RpcStatus::Error);
        assert_eq!(header.mount_epoch, Some(7));
        assert_eq!(header.route_epoch, Some(9));
        let canonical = header
            .canonical_error
            .as_ref()
            .expect("canonical_error must be present for non-OK status");
        assert_eq!(
            canonical.code,
            Some(CanonicalErrorCode::RpcCode(RpcErrorCode::RouteEpochMismatch))
        );
        assert_eq!(
            canonical.reason,
            Some(common::error::canonical::RefreshReason::RouteEpochMismatch)
        );
    }

    #[test]
    fn test_response_header_roundtrip_need_refresh() {
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::RouteEpochMismatch,
            RefreshReason::RouteEpochMismatch,
            "route epoch mismatch",
        );
        let header = ResponseHeader::error(ClientInfo::new(ClientId::new(1)), canonical.clone());

        let proto: proto_common::ResponseHeaderProto = (&header).into();
        let decoded: ResponseHeader = proto.clone().try_into().expect("decode response header");
        let reencoded: proto_common::ResponseHeaderProto = (&decoded).into();

        let decoded_canonical = decoded
            .canonical_error
            .as_ref()
            .expect("canonical_error should persist across roundtrip");
        assert_eq!(decoded_canonical.class, CanonicalErrorClass::NeedRefresh);
        assert_eq!(
            decoded_canonical.code,
            Some(CanonicalErrorCode::RpcCode(RpcErrorCode::RouteEpochMismatch))
        );
        assert_eq!(
            decoded_canonical.reason,
            Some(common::error::canonical::RefreshReason::RouteEpochMismatch)
        );

        assert_eq!(proto.error, reencoded.error, "wire form must roundtrip");
    }

    #[test]
    fn header_state_vector_roundtrip_preserves_multiple_groups() {
        let state = vec![
            GroupStateWatermark::new(ShardGroupId::new(1), RaftLogId::new(1, 1, 10)),
            GroupStateWatermark::new(ShardGroupId::new(2), RaftLogId::new(2, 3, 20)),
        ];

        let request = RequestHeader::new(ClientId::new(42)).with_state(state.clone());
        let proto_request: proto_common::RequestHeaderProto = (&request).into();
        let decoded_request = RequestHeader::try_from(proto_request).expect("request header decode");
        assert_eq!(decoded_request.state, state);

        let response = ResponseHeader::ok(ClientInfo::new(ClientId::new(42))).with_state(state.clone());
        let proto_response: proto_common::ResponseHeaderProto = (&response).into();
        let decoded_response = ResponseHeader::try_from(proto_response).expect("response header decode");
        assert_eq!(decoded_response.state, state);
    }
}
