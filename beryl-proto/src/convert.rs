// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Conversion utilities between proto messages and types crate.
//!
//! This module provides bidirectional conversion between proto messages
//! and domain types defined in the types crate.

use crate::common as proto_common;
use crate::metadata as proto_metadata;
use ::beryl_common::{
    Deadline,
    error::rpc::{
        ErrorKind as RpcErrorKind, InternalErrorKind as RpcInternalErrorKind,
        MetadataErrorKind as RpcMetadataErrorKind, ProtocolErrorKind as RpcProtocolErrorKind,
        RecoveryAction as RpcRecoveryAction, RefreshHint as RpcRefreshHint, RpcErrorDetail, WorkerEndpointHint,
        WorkerErrorKind as RpcWorkerErrorKind,
    },
    header::{CallerContext, ClientInfo, RequestHeader, ResponseHeader, TraceContext},
};
use beryl_types::chunk::ByteRange;
use beryl_types::ids::{BlockId, BlockIndex, WorkerId};
use beryl_types::layout::{BlockShape, FileLayout};
use beryl_types::lease::FencingToken;
use beryl_types::{
    CallId, ClientId, CommittedBlock, FileAttrs, FileBlockLocation, GroupName, GroupStateWatermark, InodeId, InodeKind,
    RaftLogId, Tier, WorkerEndpointInfo, WorkerNetProtocol, WorkerRunId, WriteTarget,
};

// ============================================================================
// ID Conversions
// ============================================================================

impl From<BlockId> for proto_common::BlockIdProto {
    fn from(id: BlockId) -> Self {
        proto_common::BlockIdProto {
            inode_id: id.inode_id.as_raw(),
            block_index: id.index.as_raw(),
        }
    }
}

impl TryFrom<proto_common::BlockIdProto> for BlockId {
    type Error = String;

    fn try_from(id: proto_common::BlockIdProto) -> Result<Self, Self::Error> {
        if id.inode_id == 0 {
            return Err("BlockIdProto.inode_id must be non-zero".to_string());
        }
        Ok(BlockId::new(InodeId::new(id.inode_id), BlockIndex::new(id.block_index)))
    }
}

impl From<ClientId> for proto_common::ClientIdProto {
    fn from(id: ClientId) -> Self {
        let value = id.as_raw();
        proto_common::ClientIdProto {
            high: (value >> 64) as u64,
            low: value as u64,
        }
    }
}

impl TryFrom<proto_common::ClientIdProto> for ClientId {
    type Error = String;

    fn try_from(id: proto_common::ClientIdProto) -> Result<Self, Self::Error> {
        let value = ((id.high as u128) << 64) | (id.low as u128);
        if value == 0 {
            return Err("client_id must be non-zero".to_string());
        }
        Ok(ClientId::new(value))
    }
}

/// Parse a required block id field without choosing caller error policy.
pub fn required_block_id(proto: Option<proto_common::BlockIdProto>, field_name: &str) -> Result<BlockId, String> {
    proto
        .ok_or_else(|| format!("missing {field_name}"))?
        .try_into()
        .map_err(|error| format!("invalid {field_name}: {error}"))
}

/// Parse a required client id field without choosing caller error policy.
pub fn required_client_id(proto: Option<proto_common::ClientIdProto>, field_name: &str) -> Result<ClientId, String> {
    proto
        .ok_or_else(|| format!("missing {field_name}"))?
        .try_into()
        .map_err(|err| format!("invalid {field_name}: {err}"))
}

/// Parse a required call UUID field without choosing caller error policy.
pub fn require_call_id(value: &str, field_name: &str) -> Result<CallId, String> {
    if value.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    CallId::parse(value).map_err(|err| format!("{field_name} {err}"))
}

impl From<ByteRange> for proto_common::ByteRangeProto {
    fn from(range: ByteRange) -> Self {
        proto_common::ByteRangeProto {
            offset: range.offset,
            len: range.len,
        }
    }
}

impl TryFrom<proto_common::FileLayoutProto> for FileLayout {
    type Error = String;

    fn try_from(layout: proto_common::FileLayoutProto) -> Result<Self, Self::Error> {
        let block_format_id = beryl_types::layout::BlockFormatId::from_raw(layout.block_format_id)
            .map_err(|err| format!("FileLayoutProto.block_format_id invalid: {err}"))?;
        let layout = FileLayout::with_block_format(layout.block_size, block_format_id);
        layout
            .validate()
            .map_err(|err| format!("FileLayoutProto invalid: {err}"))?;
        Ok(layout)
    }
}

impl From<&FileLayout> for proto_common::FileLayoutProto {
    fn from(layout: &FileLayout) -> Self {
        Self {
            block_size: layout.block_size,
            block_format_id: layout.block_format_id.as_raw(),
        }
    }
}

impl From<FileLayout> for proto_common::FileLayoutProto {
    fn from(layout: FileLayout) -> Self {
        Self::from(&layout)
    }
}

// ============================================================================
// FS Domain Conversions
// ============================================================================

impl From<proto_metadata::FileAttrsProto> for FileAttrs {
    fn from(attrs: proto_metadata::FileAttrsProto) -> Self {
        Self {
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
}

impl From<&FileAttrs> for proto_metadata::FileAttrsProto {
    fn from(attrs: &FileAttrs) -> Self {
        Self {
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
}

impl From<FileAttrs> for proto_metadata::FileAttrsProto {
    fn from(attrs: FileAttrs) -> Self {
        Self::from(&attrs)
    }
}

impl TryFrom<proto_metadata::InodeKindProto> for InodeKind {
    type Error = String;

    fn try_from(kind: proto_metadata::InodeKindProto) -> Result<Self, Self::Error> {
        match kind {
            proto_metadata::InodeKindProto::InodeKindFile => Ok(Self::File),
            proto_metadata::InodeKindProto::InodeKindDir => Ok(Self::Dir),
            proto_metadata::InodeKindProto::InodeKindSymlink => Ok(Self::Symlink),
            proto_metadata::InodeKindProto::InodeKindUnspecified => {
                Err("unspecified inode kind is not a domain value".to_string())
            }
        }
    }
}

impl From<InodeKind> for proto_metadata::InodeKindProto {
    fn from(kind: InodeKind) -> Self {
        match kind {
            InodeKind::File => Self::InodeKindFile,
            InodeKind::Dir => Self::InodeKindDir,
            InodeKind::Symlink => Self::InodeKindSymlink,
        }
    }
}

impl From<FencingToken> for proto_common::FencingTokenProto {
    fn from(token: FencingToken) -> Self {
        proto_common::FencingTokenProto {
            block_id: Some(token.block_id.into()),
            owner: Some(token.owner.into()),
            epoch: token.epoch,
        }
    }
}

impl TryFrom<proto_common::FencingTokenProto> for FencingToken {
    type Error = String;

    fn try_from(token: proto_common::FencingTokenProto) -> Result<Self, Self::Error> {
        let block_id = required_block_id(token.block_id, "block_id in token")?;
        let owner = required_client_id(token.owner, "owner in token")?;
        Ok(FencingToken::new(block_id, owner, token.epoch))
    }
}

/// Parse a required fencing token field without choosing caller error policy.
pub fn required_fencing_token(
    proto: Option<proto_common::FencingTokenProto>,
    field_name: &str,
) -> Result<FencingToken, String> {
    proto.ok_or_else(|| format!("missing {field_name}"))?.try_into()
}

/// Parse a required worker process-run identifier field without choosing caller error policy.
pub fn require_worker_run_id(value: &str, field_name: &str) -> Result<WorkerRunId, String> {
    if value.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    WorkerRunId::parse(value).map_err(|err| format!("{field_name} invalid: {err}"))
}

impl From<Tier> for proto_common::TierProto {
    fn from(tier: Tier) -> Self {
        match tier {
            Tier::Mem => proto_common::TierProto::TierMem,
            Tier::Nvme => proto_common::TierProto::TierNvme,
            Tier::Ssd => proto_common::TierProto::TierSsd,
            Tier::Hdd => proto_common::TierProto::TierHdd,
        }
    }
}

impl TryFrom<proto_common::TierProto> for Tier {
    type Error = String;

    fn try_from(tier: proto_common::TierProto) -> Result<Self, Self::Error> {
        match tier {
            proto_common::TierProto::TierMem => Ok(Self::Mem),
            proto_common::TierProto::TierNvme => Ok(Self::Nvme),
            proto_common::TierProto::TierSsd => Ok(Self::Ssd),
            proto_common::TierProto::TierHdd => Ok(Self::Hdd),
            proto_common::TierProto::TierUnspecified => Err("tier must be specified".to_string()),
        }
    }
}

pub fn parse_known_tier(value: i32) -> Result<Tier, String> {
    proto_common::TierProto::try_from(value)
        .map_err(|_| format!("unknown tier value {value}"))?
        .try_into()
}

impl TryFrom<proto_common::WorkerEndpointInfoProto> for WorkerEndpointInfo {
    type Error = String;

    fn try_from(endpoint: proto_common::WorkerEndpointInfoProto) -> Result<Self, Self::Error> {
        worker_endpoint_info_from_parts(
            WorkerId::new(endpoint.worker_id),
            endpoint.endpoint,
            endpoint.worker_run_id,
        )
    }
}

/// Build a shared worker endpoint value from raw wire-shaped fields.
///
pub fn worker_endpoint_info_from_parts(
    worker_id: WorkerId,
    endpoint: String,
    worker_run_id: String,
) -> Result<WorkerEndpointInfo, String> {
    if worker_id.as_raw() == 0 {
        return Err("WorkerEndpointInfoProto.worker_id must be non-zero".to_string());
    }
    if endpoint.is_empty() {
        return Err("WorkerEndpointInfoProto.endpoint must not be empty".to_string());
    }
    let worker_run_id = require_worker_run_id(&worker_run_id, "WorkerEndpointInfoProto.worker_run_id")?;
    Ok(WorkerEndpointInfo {
        worker_id,
        endpoint,
        worker_net_protocol: WorkerNetProtocol::Grpc,
        worker_run_id,
    })
}

impl From<&WorkerEndpointInfo> for proto_common::WorkerEndpointInfoProto {
    fn from(endpoint: &WorkerEndpointInfo) -> Self {
        Self {
            worker_id: endpoint.worker_id.as_raw(),
            endpoint: endpoint.endpoint.clone(),
            worker_run_id: endpoint.worker_run_id.to_string(),
        }
    }
}

impl From<WorkerEndpointInfo> for proto_common::WorkerEndpointInfoProto {
    fn from(endpoint: WorkerEndpointInfo) -> Self {
        Self {
            worker_id: endpoint.worker_id.as_raw(),
            endpoint: endpoint.endpoint,
            worker_run_id: endpoint.worker_run_id.to_string(),
        }
    }
}

impl TryFrom<proto_metadata::WriteTargetProto> for WriteTarget {
    type Error = String;

    fn try_from(target: proto_metadata::WriteTargetProto) -> Result<Self, Self::Error> {
        let block_format_id = beryl_types::layout::BlockFormatId::from_raw(target.block_format_id)
            .map_err(|err| format!("WriteTargetProto.block_format_id invalid: {err}"))?;
        BlockShape::new(block_format_id, target.block_size, target.chunk_size, target.block_size)
            .map_err(|err| format!("WriteTargetProto invalid block shape: {err}"))?;
        if target.worker_endpoints.is_empty() {
            return Err("WriteTargetProto.worker_endpoints must not be empty".to_string());
        }
        if target.block_stamp == 0 {
            return Err("WriteTargetProto.block_stamp must be non-zero".to_string());
        }
        let tier = parse_known_tier(target.tier).map_err(|err| format!("WriteTargetProto.tier invalid: {err}"))?;
        let block_id = required_block_id(target.block_id, "WriteTargetProto.block_id")?;
        let fencing_token = required_fencing_token(target.fencing_token, "WriteTargetProto.fencing_token")?;
        if fencing_token.block_id != block_id {
            return Err("WriteTargetProto.fencing_token block_id must match block_id".to_string());
        }
        if fencing_token.owner.is_zero() || fencing_token.epoch == 0 {
            return Err("WriteTargetProto.fencing_token owner and epoch must be non-zero".to_string());
        }
        let worker_endpoints = target
            .worker_endpoints
            .into_iter()
            .map(WorkerEndpointInfo::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            block_id,
            file_offset: target.file_offset,
            block_size: target.block_size,
            worker_endpoints,
            fencing_token,
            block_stamp: target.block_stamp,
            chunk_size: target.chunk_size,
            block_format_id,
            tier,
        })
    }
}

impl From<&WriteTarget> for proto_metadata::WriteTargetProto {
    fn from(target: &WriteTarget) -> Self {
        Self {
            block_id: Some(target.block_id.into()),
            file_offset: target.file_offset,
            worker_endpoints: target.worker_endpoints.iter().map(Into::into).collect(),
            fencing_token: Some(target.fencing_token.into()),
            block_stamp: target.block_stamp,
            chunk_size: target.chunk_size,
            block_format_id: target.block_format_id.as_raw(),
            block_size: target.block_size,
            tier: proto_common::TierProto::from(target.tier) as i32,
        }
    }
}

impl From<WriteTarget> for proto_metadata::WriteTargetProto {
    fn from(target: WriteTarget) -> Self {
        Self {
            block_id: Some(target.block_id.into()),
            file_offset: target.file_offset,
            worker_endpoints: target.worker_endpoints.into_iter().map(Into::into).collect(),
            fencing_token: Some(target.fencing_token.into()),
            block_stamp: target.block_stamp,
            chunk_size: target.chunk_size,
            block_format_id: target.block_format_id.as_raw(),
            block_size: target.block_size,
            tier: proto_common::TierProto::from(target.tier) as i32,
        }
    }
}

impl TryFrom<proto_metadata::CommittedBlockProto> for CommittedBlock {
    type Error = String;

    fn try_from(block: proto_metadata::CommittedBlockProto) -> Result<Self, Self::Error> {
        let block_id = required_block_id(block.block_id, "CommittedBlockProto.block_id")?;
        Ok(Self {
            block_id,
            file_offset: block.file_offset,
            len: block.len,
        })
    }
}

impl From<&CommittedBlock> for proto_metadata::CommittedBlockProto {
    fn from(block: &CommittedBlock) -> Self {
        Self {
            block_id: Some(block.block_id.into()),
            file_offset: block.file_offset,
            len: block.len,
        }
    }
}

impl From<CommittedBlock> for proto_metadata::CommittedBlockProto {
    fn from(block: CommittedBlock) -> Self {
        Self {
            block_id: Some(block.block_id.into()),
            file_offset: block.file_offset,
            len: block.len,
        }
    }
}

impl TryFrom<proto_metadata::FileBlockLocationProto> for FileBlockLocation {
    type Error = String;

    fn try_from(location: proto_metadata::FileBlockLocationProto) -> Result<Self, Self::Error> {
        if location.len == 0 {
            return Err("FileBlockLocationProto.len must be non-zero".to_string());
        }
        let block_stamp = location
            .block_stamp
            .ok_or_else(|| "FileBlockLocationProto.block_stamp missing".to_string())?;
        if block_stamp == 0 {
            return Err("FileBlockLocationProto.block_stamp must be non-zero".to_string());
        }
        let block_format_id = beryl_types::layout::BlockFormatId::from_raw(location.block_format_id)
            .map_err(|err| format!("FileBlockLocationProto.block_format_id invalid: {err}"))?;
        BlockShape::new(
            block_format_id,
            location.block_size,
            location.chunk_size,
            location.effective_len,
        )
        .map_err(|err| format!("FileBlockLocationProto invalid block shape: {err}"))?;
        let block_id = required_block_id(location.block_id, "FileBlockLocationProto.block_id")?;
        let workers = location
            .workers
            .into_iter()
            .map(WorkerEndpointInfo::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            block_id,
            file_offset: location.file_offset,
            len: location.len,
            workers,
            block_stamp,
            block_format_id,
            block_size: location.block_size,
            chunk_size: location.chunk_size,
            effective_len: location.effective_len,
        })
    }
}

impl From<&FileBlockLocation> for proto_metadata::FileBlockLocationProto {
    fn from(location: &FileBlockLocation) -> Self {
        Self {
            block_id: Some(location.block_id.into()),
            file_offset: location.file_offset,
            len: location.len,
            workers: location.workers.iter().map(Into::into).collect(),
            block_stamp: Some(location.block_stamp),
            block_format_id: location.block_format_id.as_raw(),
            block_size: location.block_size,
            chunk_size: location.chunk_size,
            effective_len: location.effective_len,
        }
    }
}

impl From<FileBlockLocation> for proto_metadata::FileBlockLocationProto {
    fn from(location: FileBlockLocation) -> Self {
        Self {
            block_id: Some(location.block_id.into()),
            file_offset: location.file_offset,
            len: location.len,
            workers: location.workers.into_iter().map(Into::into).collect(),
            block_stamp: Some(location.block_stamp),
            block_format_id: location.block_format_id.as_raw(),
            block_size: location.block_size,
            chunk_size: location.chunk_size,
            effective_len: location.effective_len,
        }
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
        let group_name = GroupName::parse(&proto.group_name)
            .map_err(|err| format!("invalid group_name in GroupStateWatermarkProto: {err}"))?;
        let state_id = proto
            .state_id
            .ok_or_else(|| "missing state_id in GroupStateWatermarkProto".to_string())?
            .into();
        Ok(GroupStateWatermark::new(group_name, state_id))
    }
}

impl From<&GroupStateWatermark> for proto_common::GroupStateWatermarkProto {
    fn from(watermark: &GroupStateWatermark) -> Self {
        proto_common::GroupStateWatermarkProto {
            state_id: Some(watermark.state_id.into()),
            group_name: watermark.group_name.to_string(),
        }
    }
}

// ============================================================================
// RequestHeaderProto / ResponseHeaderProto Conversions
// ============================================================================
//
// NOTE: This is the AUTHORITATIVE implementation of conversions between
// beryl_proto::common::RequestHeaderProto/ResponseHeaderProto and beryl_common::header types.
// All conversions MUST use these implementations.

impl TryFrom<proto_common::ClientInfoProto> for ClientInfo {
    type Error = String;

    fn try_from(proto: proto_common::ClientInfoProto) -> Result<Self, Self::Error> {
        let call_id = require_call_id(&proto.call_id, "call_id")?;
        let client_id = required_client_id(proto.client_id, "client_id")?;
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
            client_id: Some(info.client_id.into()),
            client_name: info.client_name.clone().unwrap_or_default(),
        }
    }
}

impl From<proto_common::TraceContextProto> for TraceContext {
    fn from(proto: proto_common::TraceContextProto) -> Self {
        Self {
            traceparent: proto.traceparent.filter(|value| !value.is_empty()),
            tracestate: proto.tracestate.filter(|value| !value.is_empty()),
            baggage: proto.baggage.filter(|value| !value.is_empty()),
        }
    }
}

impl From<&TraceContext> for proto_common::TraceContextProto {
    fn from(context: &TraceContext) -> Self {
        Self {
            traceparent: context.traceparent.clone(),
            tracestate: context.tracestate.clone(),
            baggage: context.baggage.clone(),
        }
    }
}

fn optional_trace_context(proto: Option<proto_common::TraceContextProto>) -> TraceContext {
    proto.map(TraceContext::from).unwrap_or_default()
}

fn proto_trace_context(context: &TraceContext) -> Option<proto_common::TraceContextProto> {
    if context.traceparent.is_none() && context.tracestate.is_none() && context.baggage.is_none() {
        None
    } else {
        Some(context.into())
    }
}

impl TryFrom<proto_common::RequestHeaderProto> for RequestHeader {
    type Error = String;

    fn try_from(proto: proto_common::RequestHeaderProto) -> Result<Self, Self::Error> {
        let client = proto.client.ok_or_else(|| "missing client".to_string())?.try_into()?;
        let deadline = Deadline::from_unix_ms(proto.deadline_ms);
        let trace_context = optional_trace_context(proto.trace_context);
        let caller_context = proto.caller_context.map(|cc| CallerContext { context: cc.context });
        let state = proto
            .state
            .into_iter()
            .map(GroupStateWatermark::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RequestHeader {
            client,
            trace_context,
            group_name: GroupName::parse_optional(&proto.group_name)
                .map_err(|err| format!("invalid header group_name: {err}"))?,
            mount_epoch: proto.mount_epoch,
            state,
            route_epoch: proto.route_epoch,
            deadline,
            caller_context,
        })
    }
}

impl From<&RequestHeader> for proto_common::RequestHeaderProto {
    fn from(header: &RequestHeader) -> Self {
        proto_common::RequestHeaderProto {
            client: Some((&header.client).into()),
            trace_context: proto_trace_context(&header.trace_context),
            group_name: header.group_name.as_ref().map(ToString::to_string).unwrap_or_default(),
            mount_epoch: header.mount_epoch,
            state: header
                .state
                .iter()
                .map(proto_common::GroupStateWatermarkProto::from)
                .collect(),
            route_epoch: header.route_epoch,
            deadline_ms: header.deadline.as_unix_ms(),
            caller_context: header
                .caller_context
                .as_ref()
                .map(|cc| proto_common::CallerContextProto {
                    context: cc.context.clone(),
                }),
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

        let rpc_error = proto.error.as_ref().map(rpc_error_from_proto);

        let state = proto
            .state
            .into_iter()
            .map(GroupStateWatermark::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResponseHeader {
            client,
            rpc_error,
            state,
            mount_epoch: proto.mount_epoch,
            route_epoch: proto.route_epoch,
            group_name: GroupName::parse_optional(&proto.group_name)
                .map_err(|err| format!("invalid header group_name: {err}"))?,
        })
    }
}

impl From<&ResponseHeader> for proto_common::ResponseHeaderProto {
    fn from(header: &ResponseHeader) -> Self {
        let error_detail = header.rpc_error.as_ref().map(rpc_error_to_proto);

        proto_common::ResponseHeaderProto {
            client: Some((&header.client).into()),
            error: error_detail,
            state: header
                .state
                .iter()
                .map(proto_common::GroupStateWatermarkProto::from)
                .collect(),
            mount_epoch: header.mount_epoch,
            route_epoch: header.route_epoch,
            group_name: header.group_name.as_ref().map(ToString::to_string).unwrap_or_default(),
        }
    }
}

// ============================================================================
// RPC error helpers (shared between control/data-plane conversions)
// ============================================================================

fn metadata_kind_proto_to_kind(kind: proto_common::MetadataErrorKindProto) -> Option<RpcMetadataErrorKind> {
    Some(match kind {
        proto_common::MetadataErrorKindProto::MetadataErrorKindUnspecified => return None,
        proto_common::MetadataErrorKindProto::MetadataErrorKindNotFound => RpcMetadataErrorKind::NotFound,
        proto_common::MetadataErrorKindProto::MetadataErrorKindAlreadyExists => RpcMetadataErrorKind::AlreadyExists,
        proto_common::MetadataErrorKindProto::MetadataErrorKindNotDirectory => RpcMetadataErrorKind::NotDirectory,
        proto_common::MetadataErrorKindProto::MetadataErrorKindIsDirectory => RpcMetadataErrorKind::IsDirectory,
        proto_common::MetadataErrorKindProto::MetadataErrorKindDirectoryNotEmpty => {
            RpcMetadataErrorKind::DirectoryNotEmpty
        }
        proto_common::MetadataErrorKindProto::MetadataErrorKindCrossMountRename => {
            RpcMetadataErrorKind::CrossMountRename
        }
        proto_common::MetadataErrorKindProto::MetadataErrorKindBusy => RpcMetadataErrorKind::Busy,
        proto_common::MetadataErrorKindProto::MetadataErrorKindConflict => RpcMetadataErrorKind::Conflict,
        proto_common::MetadataErrorKindProto::MetadataErrorKindNotLeader => RpcMetadataErrorKind::NotLeader,
        proto_common::MetadataErrorKindProto::MetadataErrorKindStaleState => RpcMetadataErrorKind::StaleState,
        proto_common::MetadataErrorKindProto::MetadataErrorKindMountEpochMismatch => {
            RpcMetadataErrorKind::MountEpochMismatch
        }
        proto_common::MetadataErrorKindProto::MetadataErrorKindRouteEpochMismatch => {
            RpcMetadataErrorKind::RouteEpochMismatch
        }
        proto_common::MetadataErrorKindProto::MetadataErrorKindOwnerGroupMismatch => {
            RpcMetadataErrorKind::OwnerGroupMismatch
        }
        proto_common::MetadataErrorKindProto::MetadataErrorKindGroupMismatch => RpcMetadataErrorKind::GroupMismatch,
        proto_common::MetadataErrorKindProto::MetadataErrorKindFencing => RpcMetadataErrorKind::Fencing,
        proto_common::MetadataErrorKindProto::MetadataErrorKindSessionInvalid => RpcMetadataErrorKind::SessionInvalid,
        proto_common::MetadataErrorKindProto::MetadataErrorKindSessionExpired => RpcMetadataErrorKind::SessionExpired,
        proto_common::MetadataErrorKindProto::MetadataErrorKindEpochMismatch => RpcMetadataErrorKind::EpochMismatch,
        proto_common::MetadataErrorKindProto::MetadataErrorKindResourceExhausted => {
            RpcMetadataErrorKind::ResourceExhausted
        }
    })
}

fn metadata_kind_to_proto(kind: RpcMetadataErrorKind) -> proto_common::MetadataErrorKindProto {
    match kind {
        RpcMetadataErrorKind::NotFound => proto_common::MetadataErrorKindProto::MetadataErrorKindNotFound,
        RpcMetadataErrorKind::AlreadyExists => proto_common::MetadataErrorKindProto::MetadataErrorKindAlreadyExists,
        RpcMetadataErrorKind::NotDirectory => proto_common::MetadataErrorKindProto::MetadataErrorKindNotDirectory,
        RpcMetadataErrorKind::IsDirectory => proto_common::MetadataErrorKindProto::MetadataErrorKindIsDirectory,
        RpcMetadataErrorKind::DirectoryNotEmpty => {
            proto_common::MetadataErrorKindProto::MetadataErrorKindDirectoryNotEmpty
        }
        RpcMetadataErrorKind::CrossMountRename => {
            proto_common::MetadataErrorKindProto::MetadataErrorKindCrossMountRename
        }
        RpcMetadataErrorKind::Busy => proto_common::MetadataErrorKindProto::MetadataErrorKindBusy,
        RpcMetadataErrorKind::Conflict => proto_common::MetadataErrorKindProto::MetadataErrorKindConflict,
        RpcMetadataErrorKind::NotLeader => proto_common::MetadataErrorKindProto::MetadataErrorKindNotLeader,
        RpcMetadataErrorKind::StaleState => proto_common::MetadataErrorKindProto::MetadataErrorKindStaleState,
        RpcMetadataErrorKind::MountEpochMismatch => {
            proto_common::MetadataErrorKindProto::MetadataErrorKindMountEpochMismatch
        }
        RpcMetadataErrorKind::RouteEpochMismatch => {
            proto_common::MetadataErrorKindProto::MetadataErrorKindRouteEpochMismatch
        }
        RpcMetadataErrorKind::OwnerGroupMismatch => {
            proto_common::MetadataErrorKindProto::MetadataErrorKindOwnerGroupMismatch
        }
        RpcMetadataErrorKind::GroupMismatch => proto_common::MetadataErrorKindProto::MetadataErrorKindGroupMismatch,
        RpcMetadataErrorKind::Fencing => proto_common::MetadataErrorKindProto::MetadataErrorKindFencing,
        RpcMetadataErrorKind::SessionInvalid => proto_common::MetadataErrorKindProto::MetadataErrorKindSessionInvalid,
        RpcMetadataErrorKind::SessionExpired => proto_common::MetadataErrorKindProto::MetadataErrorKindSessionExpired,
        RpcMetadataErrorKind::EpochMismatch => proto_common::MetadataErrorKindProto::MetadataErrorKindEpochMismatch,
        RpcMetadataErrorKind::ResourceExhausted => {
            proto_common::MetadataErrorKindProto::MetadataErrorKindResourceExhausted
        }
    }
}

fn worker_kind_proto_to_kind(kind: proto_common::WorkerErrorKindProto) -> Option<RpcWorkerErrorKind> {
    Some(match kind {
        proto_common::WorkerErrorKindProto::WorkerErrorKindUnspecified => return None,
        proto_common::WorkerErrorKindProto::WorkerErrorKindNotRegistered => RpcWorkerErrorKind::NotRegistered,
        proto_common::WorkerErrorKindProto::WorkerErrorKindRunMismatch => RpcWorkerErrorKind::RunMismatch,
        proto_common::WorkerErrorKindProto::WorkerErrorKindDescriptorMismatch => RpcWorkerErrorKind::DescriptorMismatch,
        proto_common::WorkerErrorKindProto::WorkerErrorKindFullReportRequired => RpcWorkerErrorKind::FullReportRequired,
        proto_common::WorkerErrorKindProto::WorkerErrorKindBlockLocationUnavailable => {
            RpcWorkerErrorKind::BlockLocationUnavailable
        }
        proto_common::WorkerErrorKindProto::WorkerErrorKindBlockStampMismatch => RpcWorkerErrorKind::BlockStampMismatch,
        proto_common::WorkerErrorKindProto::WorkerErrorKindNodeUnavailable => RpcWorkerErrorKind::NodeUnavailable,
        proto_common::WorkerErrorKindProto::WorkerErrorKindTimeout => RpcWorkerErrorKind::Timeout,
        proto_common::WorkerErrorKindProto::WorkerErrorKindResourceExhausted => RpcWorkerErrorKind::ResourceExhausted,
        proto_common::WorkerErrorKindProto::WorkerErrorKindConflict => RpcWorkerErrorKind::Conflict,
        proto_common::WorkerErrorKindProto::WorkerErrorKindCorrupt => RpcWorkerErrorKind::Corrupt,
        proto_common::WorkerErrorKindProto::WorkerErrorKindFencing => RpcWorkerErrorKind::Fencing,
        proto_common::WorkerErrorKindProto::WorkerErrorKindCancelled => RpcWorkerErrorKind::Cancelled,
        proto_common::WorkerErrorKindProto::WorkerErrorKindIo => RpcWorkerErrorKind::Io,
        proto_common::WorkerErrorKindProto::WorkerErrorKindNotFound => RpcWorkerErrorKind::NotFound,
    })
}

fn worker_kind_to_proto(kind: RpcWorkerErrorKind) -> proto_common::WorkerErrorKindProto {
    match kind {
        RpcWorkerErrorKind::NotRegistered => proto_common::WorkerErrorKindProto::WorkerErrorKindNotRegistered,
        RpcWorkerErrorKind::RunMismatch => proto_common::WorkerErrorKindProto::WorkerErrorKindRunMismatch,
        RpcWorkerErrorKind::DescriptorMismatch => proto_common::WorkerErrorKindProto::WorkerErrorKindDescriptorMismatch,
        RpcWorkerErrorKind::FullReportRequired => proto_common::WorkerErrorKindProto::WorkerErrorKindFullReportRequired,
        RpcWorkerErrorKind::BlockLocationUnavailable => {
            proto_common::WorkerErrorKindProto::WorkerErrorKindBlockLocationUnavailable
        }
        RpcWorkerErrorKind::BlockStampMismatch => proto_common::WorkerErrorKindProto::WorkerErrorKindBlockStampMismatch,
        RpcWorkerErrorKind::NodeUnavailable => proto_common::WorkerErrorKindProto::WorkerErrorKindNodeUnavailable,
        RpcWorkerErrorKind::Timeout => proto_common::WorkerErrorKindProto::WorkerErrorKindTimeout,
        RpcWorkerErrorKind::ResourceExhausted => proto_common::WorkerErrorKindProto::WorkerErrorKindResourceExhausted,
        RpcWorkerErrorKind::Conflict => proto_common::WorkerErrorKindProto::WorkerErrorKindConflict,
        RpcWorkerErrorKind::Corrupt => proto_common::WorkerErrorKindProto::WorkerErrorKindCorrupt,
        RpcWorkerErrorKind::Fencing => proto_common::WorkerErrorKindProto::WorkerErrorKindFencing,
        RpcWorkerErrorKind::Cancelled => proto_common::WorkerErrorKindProto::WorkerErrorKindCancelled,
        RpcWorkerErrorKind::Io => proto_common::WorkerErrorKindProto::WorkerErrorKindIo,
        RpcWorkerErrorKind::NotFound => proto_common::WorkerErrorKindProto::WorkerErrorKindNotFound,
    }
}

fn protocol_kind_proto_to_kind(kind: proto_common::ProtocolErrorKindProto) -> Option<RpcProtocolErrorKind> {
    Some(match kind {
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindUnspecified => return None,
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindInvalidHeader => RpcProtocolErrorKind::InvalidHeader,
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindInvalidArgument => RpcProtocolErrorKind::InvalidArgument,
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindPermissionDenied => {
            RpcProtocolErrorKind::PermissionDenied
        }
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindUnsupported => RpcProtocolErrorKind::Unsupported,
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindCancelled => RpcProtocolErrorKind::Cancelled,
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindCorrupt => RpcProtocolErrorKind::Corrupt,
    })
}

fn protocol_kind_to_proto(kind: RpcProtocolErrorKind) -> proto_common::ProtocolErrorKindProto {
    match kind {
        RpcProtocolErrorKind::InvalidHeader => proto_common::ProtocolErrorKindProto::ProtocolErrorKindInvalidHeader,
        RpcProtocolErrorKind::InvalidArgument => proto_common::ProtocolErrorKindProto::ProtocolErrorKindInvalidArgument,
        RpcProtocolErrorKind::PermissionDenied => {
            proto_common::ProtocolErrorKindProto::ProtocolErrorKindPermissionDenied
        }
        RpcProtocolErrorKind::Unsupported => proto_common::ProtocolErrorKindProto::ProtocolErrorKindUnsupported,
        RpcProtocolErrorKind::Cancelled => proto_common::ProtocolErrorKindProto::ProtocolErrorKindCancelled,
        RpcProtocolErrorKind::Corrupt => proto_common::ProtocolErrorKindProto::ProtocolErrorKindCorrupt,
    }
}

fn internal_kind_proto_to_kind(kind: proto_common::InternalErrorKindProto) -> Option<RpcInternalErrorKind> {
    Some(match kind {
        proto_common::InternalErrorKindProto::InternalErrorKindUnspecified => return None,
        proto_common::InternalErrorKindProto::InternalErrorKindNodeUnavailable => RpcInternalErrorKind::NodeUnavailable,
        proto_common::InternalErrorKindProto::InternalErrorKindTimeout => RpcInternalErrorKind::Timeout,
        proto_common::InternalErrorKindProto::InternalErrorKindResourceExhausted => {
            RpcInternalErrorKind::ResourceExhausted
        }
        proto_common::InternalErrorKindProto::InternalErrorKindCancelled => RpcInternalErrorKind::Cancelled,
        proto_common::InternalErrorKindProto::InternalErrorKindCorrupt => RpcInternalErrorKind::Corrupt,
        proto_common::InternalErrorKindProto::InternalErrorKindInternal => RpcInternalErrorKind::Internal,
    })
}

fn internal_kind_to_proto(kind: RpcInternalErrorKind) -> proto_common::InternalErrorKindProto {
    match kind {
        RpcInternalErrorKind::NodeUnavailable => proto_common::InternalErrorKindProto::InternalErrorKindNodeUnavailable,
        RpcInternalErrorKind::Timeout => proto_common::InternalErrorKindProto::InternalErrorKindTimeout,
        RpcInternalErrorKind::ResourceExhausted => {
            proto_common::InternalErrorKindProto::InternalErrorKindResourceExhausted
        }
        RpcInternalErrorKind::Cancelled => proto_common::InternalErrorKindProto::InternalErrorKindCancelled,
        RpcInternalErrorKind::Corrupt => proto_common::InternalErrorKindProto::InternalErrorKindCorrupt,
        RpcInternalErrorKind::Internal => proto_common::InternalErrorKindProto::InternalErrorKindInternal,
    }
}

fn error_kind_proto_to_kind(kind: Option<&proto_common::ErrorKindProto>) -> Option<RpcErrorKind> {
    match kind.and_then(|kind| kind.kind.as_ref()) {
        Some(proto_common::error_kind_proto::Kind::Metadata(kind)) => {
            let kind = proto_common::MetadataErrorKindProto::try_from(*kind).ok()?;
            Some(RpcErrorKind::Metadata(metadata_kind_proto_to_kind(kind)?))
        }
        Some(proto_common::error_kind_proto::Kind::Worker(kind)) => {
            let kind = proto_common::WorkerErrorKindProto::try_from(*kind).ok()?;
            Some(RpcErrorKind::Worker(worker_kind_proto_to_kind(kind)?))
        }
        Some(proto_common::error_kind_proto::Kind::Protocol(kind)) => {
            let kind = proto_common::ProtocolErrorKindProto::try_from(*kind).ok()?;
            Some(RpcErrorKind::Protocol(protocol_kind_proto_to_kind(kind)?))
        }
        Some(proto_common::error_kind_proto::Kind::Internal(kind)) => {
            let kind = proto_common::InternalErrorKindProto::try_from(*kind).ok()?;
            Some(RpcErrorKind::Internal(internal_kind_proto_to_kind(kind)?))
        }
        None => None,
    }
}

fn error_kind_to_proto(kind: RpcErrorKind) -> proto_common::ErrorKindProto {
    let kind = match kind {
        RpcErrorKind::Metadata(kind) => {
            proto_common::error_kind_proto::Kind::Metadata(metadata_kind_to_proto(kind) as i32)
        }
        RpcErrorKind::Worker(kind) => proto_common::error_kind_proto::Kind::Worker(worker_kind_to_proto(kind) as i32),
        RpcErrorKind::Protocol(kind) => {
            proto_common::error_kind_proto::Kind::Protocol(protocol_kind_to_proto(kind) as i32)
        }
        RpcErrorKind::Internal(kind) => {
            proto_common::error_kind_proto::Kind::Internal(internal_kind_to_proto(kind) as i32)
        }
    };
    proto_common::ErrorKindProto { kind: Some(kind) }
}

fn refresh_hint_proto_to_hint(hint: Option<&proto_common::RefreshHintProto>) -> RpcRefreshHint {
    hint.map_or_else(RpcRefreshHint::default, |hint| RpcRefreshHint {
        leader_endpoint: hint.leader_endpoint.clone(),
        group_name: hint.group_name.clone(),
        mount_epoch: hint.mount_epoch,
        mount_prefix: hint.mount_prefix.clone(),
        route_epoch: hint.route_epoch,
        worker_endpoints: hint
            .worker_endpoints
            .iter()
            .map(|endpoint| WorkerEndpointHint {
                worker_id: endpoint.worker_id,
                endpoint: endpoint.endpoint.clone(),
            })
            .collect(),
        worker_resolve_required: hint.worker_resolve_required,
    })
}

fn refresh_hint_to_proto(hint: &RpcRefreshHint) -> proto_common::RefreshHintProto {
    proto_common::RefreshHintProto {
        leader_endpoint: hint.leader_endpoint.clone(),
        group_name: hint.group_name.clone(),
        mount_epoch: hint.mount_epoch,
        mount_prefix: hint.mount_prefix.clone(),
        route_epoch: hint.route_epoch,
        worker_endpoints: hint
            .worker_endpoints
            .iter()
            .map(|endpoint| proto_common::WorkerEndpointInfoProto {
                worker_id: endpoint.worker_id,
                endpoint: endpoint.endpoint.clone(),
                worker_run_id: String::new(),
            })
            .collect(),
        worker_resolve_required: hint.worker_resolve_required,
    }
}

fn recovery_proto_to_action(recovery: Option<&proto_common::RecoveryActionProto>) -> Option<RpcRecoveryAction> {
    match recovery.and_then(|recovery| recovery.action.as_ref()) {
        Some(proto_common::recovery_action_proto::Action::Fail(_)) => Some(RpcRecoveryAction::Fail),
        Some(proto_common::recovery_action_proto::Action::Retry(retry)) => Some(RpcRecoveryAction::Retry {
            after_ms: retry.after_ms,
        }),
        Some(proto_common::recovery_action_proto::Action::RefreshMetadata(refresh)) => {
            Some(RpcRecoveryAction::RefreshMetadata {
                hint: refresh_hint_proto_to_hint(refresh.hint.as_ref()),
            })
        }
        Some(proto_common::recovery_action_proto::Action::ReopenWriteSession(reopen)) => {
            Some(RpcRecoveryAction::ReopenWriteSession {
                hint: refresh_hint_proto_to_hint(reopen.hint.as_ref()),
            })
        }
        Some(proto_common::recovery_action_proto::Action::RegisterWorker(_)) => Some(RpcRecoveryAction::RegisterWorker),
        Some(proto_common::recovery_action_proto::Action::SendFullBlockReport(_)) => {
            Some(RpcRecoveryAction::SendFullBlockReport)
        }
        None => None,
    }
}

fn recovery_action_to_proto(action: &RpcRecoveryAction) -> proto_common::RecoveryActionProto {
    let action = match action {
        RpcRecoveryAction::Fail => {
            proto_common::recovery_action_proto::Action::Fail(proto_common::FailRecoveryProto {})
        }
        RpcRecoveryAction::Retry { after_ms } => {
            proto_common::recovery_action_proto::Action::Retry(proto_common::RetryRecoveryProto { after_ms: *after_ms })
        }
        RpcRecoveryAction::RefreshMetadata { hint } => {
            proto_common::recovery_action_proto::Action::RefreshMetadata(proto_common::RefreshMetadataRecoveryProto {
                hint: Some(refresh_hint_to_proto(hint)),
            })
        }
        RpcRecoveryAction::ReopenWriteSession { hint } => {
            proto_common::recovery_action_proto::Action::ReopenWriteSession(
                proto_common::ReopenWriteSessionRecoveryProto {
                    hint: Some(refresh_hint_to_proto(hint)),
                },
            )
        }
        RpcRecoveryAction::RegisterWorker => {
            proto_common::recovery_action_proto::Action::RegisterWorker(proto_common::RegisterWorkerRecoveryProto {})
        }
        RpcRecoveryAction::SendFullBlockReport => proto_common::recovery_action_proto::Action::SendFullBlockReport(
            proto_common::SendFullBlockReportRecoveryProto {},
        ),
    };
    proto_common::RecoveryActionProto { action: Some(action) }
}

/// Convert proto ErrorDetailProto into RPC error.
///
/// Missing or unknown failure facts and recovery actions fail closed as an
/// invalid header. Malformed input cannot retain a retry or refresh action
/// supplied by the wire payload.
pub fn rpc_error_from_proto(err_detail: &proto_common::ErrorDetailProto) -> RpcErrorDetail {
    let (Some(kind), Some(recovery)) = (
        error_kind_proto_to_kind(err_detail.kind.as_ref()),
        recovery_proto_to_action(err_detail.recovery.as_ref()),
    ) else {
        return RpcErrorDetail::fail(
            RpcErrorKind::Protocol(RpcProtocolErrorKind::InvalidHeader),
            "malformed RPC error detail",
        );
    };
    RpcErrorDetail {
        kind,
        recovery,
        message: err_detail.message.clone(),
    }
}

/// Convert RPC error into proto ErrorDetailProto.
pub fn rpc_error_to_proto(err: &RpcErrorDetail) -> proto_common::ErrorDetailProto {
    proto_common::ErrorDetailProto {
        kind: Some(error_kind_to_proto(err.kind)),
        recovery: Some(recovery_action_to_proto(&err.recovery)),
        message: err.message.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_worker_run_id() -> beryl_types::WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("valid test WorkerRunId")
    }

    #[test]
    fn malformed_rpc_error_details_fail_closed_without_recovery() {
        let retry = || proto_common::RecoveryActionProto {
            action: Some(proto_common::recovery_action_proto::Action::Retry(
                proto_common::RetryRecoveryProto { after_ms: Some(1) },
            )),
        };
        let refresh = || proto_common::RecoveryActionProto {
            action: Some(proto_common::recovery_action_proto::Action::RefreshMetadata(
                proto_common::RefreshMetadataRecoveryProto {
                    hint: Some(proto_common::RefreshHintProto::default()),
                },
            )),
        };
        let valid_kind = || proto_common::ErrorKindProto {
            kind: Some(proto_common::error_kind_proto::Kind::Metadata(
                proto_common::MetadataErrorKindProto::MetadataErrorKindNotFound as i32,
            )),
        };
        let malformed = [
            proto_common::ErrorDetailProto {
                kind: Some(proto_common::ErrorKindProto {
                    kind: Some(proto_common::error_kind_proto::Kind::Metadata(i32::MAX)),
                }),
                recovery: Some(retry()),
                message: "unknown kind with retry".to_string(),
            },
            proto_common::ErrorDetailProto {
                kind: Some(proto_common::ErrorKindProto {
                    kind: Some(proto_common::error_kind_proto::Kind::Metadata(
                        proto_common::MetadataErrorKindProto::MetadataErrorKindUnspecified as i32,
                    )),
                }),
                recovery: Some(retry()),
                message: "unspecified kind with retry".to_string(),
            },
            proto_common::ErrorDetailProto {
                kind: None,
                recovery: Some(refresh()),
                message: "missing kind with refresh".to_string(),
            },
            proto_common::ErrorDetailProto {
                kind: Some(valid_kind()),
                recovery: None,
                message: "missing recovery".to_string(),
            },
            proto_common::ErrorDetailProto {
                kind: Some(valid_kind()),
                recovery: Some(proto_common::RecoveryActionProto { action: None }),
                message: "missing recovery action".to_string(),
            },
        ];

        for encoded in malformed {
            let decoded = rpc_error_from_proto(&encoded);
            assert_eq!(
                decoded.kind,
                RpcErrorKind::Protocol(RpcProtocolErrorKind::InvalidHeader)
            );
            assert_eq!(decoded.recovery, RpcRecoveryAction::Fail);
            assert_eq!(decoded.message, "malformed RPC error detail");
        }
    }

    #[test]
    fn shared_location_conversion_rejects_malformed_required_fields() {
        let endpoint = || proto_common::WorkerEndpointInfoProto {
            worker_id: 7,
            endpoint: "127.0.0.1:19101".to_string(),
            worker_run_id: test_worker_run_id().to_string(),
        };
        let block_id = BlockId::from_u64_u32(42, 3);
        let token = FencingToken::new(block_id, ClientId::new(9), 17);

        let mut target = proto_metadata::WriteTargetProto {
            block_id: Some(block_id.into()),
            file_offset: 128,
            worker_endpoints: Vec::new(),
            fencing_token: Some(token.into()),
            block_stamp: 55,
            chunk_size: beryl_types::layout::BlockFormatId::FULL_EFFECTIVE
                .spec()
                .unwrap()
                .storage_chunk_size,
            block_format_id: beryl_types::layout::BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: 4096,
            tier: proto_common::TierProto::TierHdd as i32,
        };
        let err = beryl_types::WriteTarget::try_from(target.clone()).expect_err("empty target workers must fail");
        assert!(err.contains("worker_endpoints"));
        target.worker_endpoints.push(endpoint());
        target.block_stamp = 0;
        let err = beryl_types::WriteTarget::try_from(target).expect_err("zero target block_stamp must fail");
        assert!(err.contains("block_stamp"));

        let mut location = proto_metadata::FileBlockLocationProto {
            block_id: Some(block_id.into()),
            file_offset: 128,
            len: 4096,
            workers: Vec::new(),
            block_stamp: Some(55),
            block_format_id: beryl_types::layout::BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: 4096,
            chunk_size: beryl_types::layout::BlockFormatId::FULL_EFFECTIVE
                .spec()
                .unwrap()
                .storage_chunk_size,
            effective_len: 4096,
        };
        let decoded_empty =
            beryl_types::FileBlockLocation::try_from(location.clone()).expect("empty read location workers are valid");
        assert!(decoded_empty.workers.is_empty());
        location.workers.push(endpoint());
        location.block_stamp = None;
        let err =
            beryl_types::FileBlockLocation::try_from(location.clone()).expect_err("missing block_stamp must fail");
        assert!(err.contains("block_stamp missing"));
        location.block_stamp = Some(0);
        let err = beryl_types::FileBlockLocation::try_from(location).expect_err("zero block_stamp must fail");
        assert!(err.contains("block_stamp"));
    }
}
