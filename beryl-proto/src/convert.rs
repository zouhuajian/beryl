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
use beryl_types::ids::{BlockId, BlockIndex, DataHandleId, StreamId, WorkerId};
use beryl_types::layout::{BlockShape, FileLayout};
use beryl_types::lease::FencingToken;
use beryl_types::{
    CallId, ClientId, CommittedBlock, FileAttrs, FileBlockLocation, GroupName, GroupStateWatermark, InodeKind,
    RaftLogId, Tier, WorkerEndpointInfo, WorkerNetProtocol, WorkerRunId, WriteTarget,
};

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

/// Parse a required block id field without choosing caller error policy.
pub fn required_block_id(proto: Option<proto_common::BlockIdProto>, field_name: &str) -> Result<BlockId, String> {
    proto
        .ok_or_else(|| format!("missing {field_name}"))?
        .try_into()
        .map_err(|_| format!("invalid {field_name}"))
}

/// Parse a required stream id field without choosing caller error policy.
pub fn required_stream_id(proto: Option<proto_common::StreamIdProto>, field_name: &str) -> Result<StreamId, String> {
    proto
        .ok_or_else(|| format!("missing {field_name}"))?
        .try_into()
        .map_err(|_| format!("invalid {field_name}"))
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

impl From<&proto_common::ByteRangeProto> for ByteRange {
    fn from(range: &proto_common::ByteRangeProto) -> Self {
        ByteRange {
            offset: range.offset,
            len: range.len,
        }
    }
}

impl From<proto_common::ByteRangeProto> for ByteRange {
    fn from(range: proto_common::ByteRangeProto) -> Self {
        ByteRange::from(&range)
    }
}

impl TryFrom<proto_common::FileLayoutProto> for FileLayout {
    type Error = String;

    fn try_from(layout: proto_common::FileLayoutProto) -> Result<Self, Self::Error> {
        let replication =
            u8::try_from(layout.replication).map_err(|_| "FileLayoutProto.replication does not fit u8".to_string())?;
        let block_format_id = beryl_types::layout::BlockFormatId::from_raw(layout.block_format_id)
            .map_err(|err| format!("FileLayoutProto.block_format_id invalid: {err}"))?;
        let layout = FileLayout::with_block_format(layout.block_size, layout.chunk_size, replication, block_format_id);
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
            chunk_size: layout.chunk_size,
            replication: u32::from(layout.replication),
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
        BlockShape::new(
            block_format_id,
            target.block_size,
            target.chunk_size,
            target.effective_len,
        )
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
            effective_len: target.effective_len,
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
            effective_len: target.effective_len,
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
            effective_len: target.effective_len,
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
// CallerContextProto Conversions
// ============================================================================

impl From<&CallerContext> for proto_common::CallerContextProto {
    fn from(ctx: &CallerContext) -> Self {
        proto_common::CallerContextProto {
            context: ctx.context.clone(),
        }
    }
}

impl From<proto_common::CallerContextProto> for CallerContext {
    fn from(proto: proto_common::CallerContextProto) -> Self {
        CallerContext { context: proto.context }
    }
}

// ============================================================================
// DataRequestHeaderProto Conversions
// ============================================================================

impl From<&RequestHeader> for crate::worker::DataRequestHeaderProto {
    fn from(header: &RequestHeader) -> Self {
        crate::worker::DataRequestHeaderProto {
            client: Some((&header.client).into()),
            trace_context: proto_trace_context(&header.trace_context),
        }
    }
}

// ============================================================================
// RPC error helpers (shared between control/data-plane conversions)
// ============================================================================

fn fs_errno_proto_to_enum(code: i32) -> beryl_types::fs::FsErrorCode {
    match code {
        x if x == proto_common::FsErrnoProto::FsErrnoOk as i32 => beryl_types::fs::FsErrorCode::Ok,
        x if x == proto_common::FsErrnoProto::FsErrnoEnoent as i32 => beryl_types::fs::FsErrorCode::ENoEnt,
        x if x == proto_common::FsErrnoProto::FsErrnoEexist as i32 => beryl_types::fs::FsErrorCode::EExist,
        x if x == proto_common::FsErrnoProto::FsErrnoEnotempty as i32 => beryl_types::fs::FsErrorCode::ENotEmpty,
        x if x == proto_common::FsErrnoProto::FsErrnoEnotdir as i32 => beryl_types::fs::FsErrorCode::ENotDir,
        x if x == proto_common::FsErrnoProto::FsErrnoEisdir as i32 => beryl_types::fs::FsErrorCode::EIsDir,
        x if x == proto_common::FsErrnoProto::FsErrnoExdev as i32 => beryl_types::fs::FsErrorCode::EXDev,
        x if x == proto_common::FsErrnoProto::FsErrnoEperm as i32 => beryl_types::fs::FsErrorCode::EPerm,
        x if x == proto_common::FsErrnoProto::FsErrnoEacces as i32 => beryl_types::fs::FsErrorCode::EAcces,
        x if x == proto_common::FsErrnoProto::FsErrnoEinval as i32 => beryl_types::fs::FsErrorCode::EInval,
        x if x == proto_common::FsErrnoProto::FsErrnoEnotsup as i32 => beryl_types::fs::FsErrorCode::ENotsup,
        x if x == proto_common::FsErrnoProto::FsErrnoEnotimpl as i32 => beryl_types::fs::FsErrorCode::ENotImpl,
        x if x == proto_common::FsErrnoProto::FsErrnoEagain as i32 => beryl_types::fs::FsErrorCode::EAgain,
        x if x == proto_common::FsErrnoProto::FsErrnoEbusy as i32 => beryl_types::fs::FsErrorCode::EBusy,
        _ => beryl_types::fs::FsErrorCode::EInval,
    }
}

fn fs_errno_enum_to_proto(code: &beryl_types::fs::FsErrorCode) -> proto_common::FsErrnoProto {
    match code {
        beryl_types::fs::FsErrorCode::Ok => proto_common::FsErrnoProto::FsErrnoOk,
        beryl_types::fs::FsErrorCode::ENoEnt => proto_common::FsErrnoProto::FsErrnoEnoent,
        beryl_types::fs::FsErrorCode::EExist => proto_common::FsErrnoProto::FsErrnoEexist,
        beryl_types::fs::FsErrorCode::ENotEmpty => proto_common::FsErrnoProto::FsErrnoEnotempty,
        beryl_types::fs::FsErrorCode::ENotDir => proto_common::FsErrnoProto::FsErrnoEnotdir,
        beryl_types::fs::FsErrorCode::EIsDir => proto_common::FsErrnoProto::FsErrnoEisdir,
        beryl_types::fs::FsErrorCode::EXDev => proto_common::FsErrnoProto::FsErrnoExdev,
        beryl_types::fs::FsErrorCode::EPerm => proto_common::FsErrnoProto::FsErrnoEperm,
        beryl_types::fs::FsErrorCode::EAcces => proto_common::FsErrnoProto::FsErrnoEacces,
        beryl_types::fs::FsErrorCode::EInval => proto_common::FsErrnoProto::FsErrnoEinval,
        beryl_types::fs::FsErrorCode::ENotsup => proto_common::FsErrnoProto::FsErrnoEnotsup,
        beryl_types::fs::FsErrorCode::ENotImpl => proto_common::FsErrnoProto::FsErrnoEnotimpl,
        beryl_types::fs::FsErrorCode::EAgain => proto_common::FsErrnoProto::FsErrnoEagain,
        beryl_types::fs::FsErrorCode::EBusy => proto_common::FsErrnoProto::FsErrnoEbusy,
    }
}

fn metadata_kind_proto_to_kind(kind: proto_common::MetadataErrorKindProto) -> RpcMetadataErrorKind {
    match kind {
        proto_common::MetadataErrorKindProto::MetadataErrorKindUnspecified => RpcMetadataErrorKind::StaleState,
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
    }
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

fn worker_kind_proto_to_kind(kind: proto_common::WorkerErrorKindProto) -> RpcWorkerErrorKind {
    match kind {
        proto_common::WorkerErrorKindProto::WorkerErrorKindUnspecified => RpcWorkerErrorKind::Io,
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
    }
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
    }
}

fn protocol_kind_proto_to_kind(kind: proto_common::ProtocolErrorKindProto) -> RpcProtocolErrorKind {
    match kind {
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindUnspecified => RpcProtocolErrorKind::InvalidHeader,
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindInvalidHeader => RpcProtocolErrorKind::InvalidHeader,
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindInvalidArgument => RpcProtocolErrorKind::InvalidArgument,
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindPermissionDenied => {
            RpcProtocolErrorKind::PermissionDenied
        }
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindUnsupported => RpcProtocolErrorKind::Unsupported,
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindCancelled => RpcProtocolErrorKind::Cancelled,
        proto_common::ProtocolErrorKindProto::ProtocolErrorKindCorrupt => RpcProtocolErrorKind::Corrupt,
    }
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

fn internal_kind_proto_to_kind(kind: proto_common::InternalErrorKindProto) -> RpcInternalErrorKind {
    match kind {
        proto_common::InternalErrorKindProto::InternalErrorKindUnspecified => RpcInternalErrorKind::Internal,
        proto_common::InternalErrorKindProto::InternalErrorKindNodeUnavailable => RpcInternalErrorKind::NodeUnavailable,
        proto_common::InternalErrorKindProto::InternalErrorKindTimeout => RpcInternalErrorKind::Timeout,
        proto_common::InternalErrorKindProto::InternalErrorKindResourceExhausted => {
            RpcInternalErrorKind::ResourceExhausted
        }
        proto_common::InternalErrorKindProto::InternalErrorKindCancelled => RpcInternalErrorKind::Cancelled,
        proto_common::InternalErrorKindProto::InternalErrorKindCorrupt => RpcInternalErrorKind::Corrupt,
        proto_common::InternalErrorKindProto::InternalErrorKindInternal => RpcInternalErrorKind::Internal,
    }
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

fn error_kind_proto_to_kind(kind: Option<&proto_common::ErrorKindProto>) -> RpcErrorKind {
    match kind.and_then(|kind| kind.kind.as_ref()) {
        Some(proto_common::error_kind_proto::Kind::Fs(errno)) => RpcErrorKind::Fs(fs_errno_proto_to_enum(*errno)),
        Some(proto_common::error_kind_proto::Kind::Metadata(kind)) => {
            let kind = proto_common::MetadataErrorKindProto::try_from(*kind)
                .unwrap_or(proto_common::MetadataErrorKindProto::MetadataErrorKindUnspecified);
            RpcErrorKind::Metadata(metadata_kind_proto_to_kind(kind))
        }
        Some(proto_common::error_kind_proto::Kind::Worker(kind)) => {
            let kind = proto_common::WorkerErrorKindProto::try_from(*kind)
                .unwrap_or(proto_common::WorkerErrorKindProto::WorkerErrorKindUnspecified);
            RpcErrorKind::Worker(worker_kind_proto_to_kind(kind))
        }
        Some(proto_common::error_kind_proto::Kind::Protocol(kind)) => {
            let kind = proto_common::ProtocolErrorKindProto::try_from(*kind)
                .unwrap_or(proto_common::ProtocolErrorKindProto::ProtocolErrorKindUnspecified);
            RpcErrorKind::Protocol(protocol_kind_proto_to_kind(kind))
        }
        Some(proto_common::error_kind_proto::Kind::Internal(kind)) => {
            let kind = proto_common::InternalErrorKindProto::try_from(*kind)
                .unwrap_or(proto_common::InternalErrorKindProto::InternalErrorKindUnspecified);
            RpcErrorKind::Internal(internal_kind_proto_to_kind(kind))
        }
        None => RpcErrorKind::Internal(RpcInternalErrorKind::Internal),
    }
}

fn error_kind_to_proto(kind: RpcErrorKind) -> proto_common::ErrorKindProto {
    let kind = match kind {
        RpcErrorKind::Fs(errno) => proto_common::error_kind_proto::Kind::Fs(fs_errno_enum_to_proto(&errno) as i32),
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

fn recovery_proto_to_action(recovery: Option<&proto_common::RecoveryActionProto>) -> RpcRecoveryAction {
    match recovery.and_then(|recovery| recovery.action.as_ref()) {
        Some(proto_common::recovery_action_proto::Action::Fail(_)) => RpcRecoveryAction::Fail,
        Some(proto_common::recovery_action_proto::Action::Retry(retry)) => RpcRecoveryAction::Retry {
            after_ms: retry.after_ms,
        },
        Some(proto_common::recovery_action_proto::Action::RefreshMetadata(refresh)) => {
            RpcRecoveryAction::RefreshMetadata {
                hint: refresh_hint_proto_to_hint(refresh.hint.as_ref()),
            }
        }
        Some(proto_common::recovery_action_proto::Action::ReopenWriteSession(reopen)) => {
            RpcRecoveryAction::ReopenWriteSession {
                hint: refresh_hint_proto_to_hint(reopen.hint.as_ref()),
            }
        }
        Some(proto_common::recovery_action_proto::Action::RegisterWorker(_)) => RpcRecoveryAction::RegisterWorker,
        Some(proto_common::recovery_action_proto::Action::SendFullBlockReport(_)) => {
            RpcRecoveryAction::SendFullBlockReport
        }
        None => RpcRecoveryAction::Fail,
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
pub fn rpc_error_from_proto(err_detail: &proto_common::ErrorDetailProto) -> RpcErrorDetail {
    RpcErrorDetail {
        kind: error_kind_proto_to_kind(err_detail.kind.as_ref()),
        recovery: recovery_proto_to_action(err_detail.recovery.as_ref()),
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
    fn msync_request_proto_shape_is_header_only() {
        let request = crate::metadata::MsyncRequestProto { header: None };
        assert!(request.header.is_none());
    }

    #[test]
    fn msync_response_proto_shape_is_header_and_state() {
        let response = crate::metadata::MsyncResponseProto {
            header: None,
            state: None,
        };
        assert!(response.header.is_none());
        assert!(response.state.is_none());
    }

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
    fn file_attrs_proto_converts_to_domain_file_attrs() {
        let proto_attrs = crate::metadata::FileAttrsProto {
            mode: 0o100755,
            uid: 501,
            gid: 20,
            size: 4096,
            atime_ms: 11,
            mtime_ms: 12,
            ctime_ms: 13,
            nlink: 2,
        };

        let attrs: beryl_types::FileAttrs = proto_attrs.into();

        assert_eq!(
            attrs,
            beryl_types::FileAttrs {
                mode: 0o100755,
                uid: 501,
                gid: 20,
                size: 4096,
                atime_ms: 11,
                mtime_ms: 12,
                ctime_ms: 13,
                nlink: 2,
            }
        );
    }

    #[test]
    fn domain_file_attrs_converts_to_proto() {
        let attrs = beryl_types::FileAttrs {
            mode: 0o040755,
            uid: 502,
            gid: 21,
            size: 8192,
            atime_ms: 21,
            mtime_ms: 22,
            ctime_ms: 23,
            nlink: 3,
        };

        let proto_attrs: crate::metadata::FileAttrsProto = (&attrs).into();
        let owned_proto_attrs: crate::metadata::FileAttrsProto = attrs.into();

        let expected = crate::metadata::FileAttrsProto {
            mode: 0o040755,
            uid: 502,
            gid: 21,
            size: 8192,
            atime_ms: 21,
            mtime_ms: 22,
            ctime_ms: 23,
            nlink: 3,
        };
        assert_eq!(proto_attrs, expected);
        assert_eq!(owned_proto_attrs, expected);
    }

    #[test]
    fn file_layout_proto_roundtrip_preserves_block_format_id() {
        let layout = beryl_types::layout::FileLayout::with_block_format(
            4096,
            1024,
            1,
            beryl_types::layout::BlockFormatId::FULL_EFFECTIVE,
        );

        let proto: proto_common::FileLayoutProto = layout.into();
        assert_eq!(proto.block_format_id, 1);
        let decoded = beryl_types::layout::FileLayout::try_from(proto).expect("layout decodes");

        assert_eq!(decoded, layout);
    }

    #[test]
    fn file_layout_proto_rejects_invalid_values() {
        let valid = || proto_common::FileLayoutProto {
            block_size: 4096,
            chunk_size: 1024,
            replication: 1,
            block_format_id: beryl_types::layout::BlockFormatId::FULL_EFFECTIVE.as_raw(),
        };
        for (layout, expected) in [
            (
                proto_common::FileLayoutProto {
                    block_format_id: 0,
                    ..valid()
                },
                "block_format_id",
            ),
            (
                proto_common::FileLayoutProto {
                    block_format_id: 99,
                    ..valid()
                },
                "block_format_id",
            ),
            (
                proto_common::FileLayoutProto {
                    block_size: 0,
                    ..valid()
                },
                "block_size",
            ),
            (
                proto_common::FileLayoutProto {
                    chunk_size: 0,
                    ..valid()
                },
                "chunk_size",
            ),
            (
                proto_common::FileLayoutProto {
                    replication: 0,
                    ..valid()
                },
                "replication",
            ),
        ] {
            let err = beryl_types::layout::FileLayout::try_from(layout).expect_err("invalid layout must fail");
            assert!(err.contains(expected), "unexpected error: {err}");
        }
    }

    #[test]
    fn block_contract_proto_fields_are_normalized() {
        let header_proto = include_str!("../common/header.proto");
        assert_eq!(
            proto_message_fields(header_proto, "TraceContextProto"),
            vec![
                ("string", "traceparent", 1),
                ("string", "tracestate", 2),
                ("string", "baggage", 3),
            ]
        );
        assert_eq!(
            proto_message_fields(header_proto, "ClientInfoProto"),
            vec![
                ("string", "call_id", 1),
                ("ClientIdProto", "client_id", 2),
                ("string", "client_name", 3),
            ]
        );
        assert_eq!(
            proto_message_fields(header_proto, "RequestHeaderProto"),
            vec![
                ("ClientInfoProto", "client", 1),
                ("TraceContextProto", "trace_context", 2),
                ("string", "group_name", 3),
                ("uint64", "mount_epoch", 4),
                ("GroupStateWatermarkProto", "state", 5),
                ("uint64", "route_epoch", 6),
                ("int64", "deadline_ms", 7),
                ("CallerContextProto", "caller_context", 8),
            ]
        );
        assert_eq!(
            proto_message_fields(header_proto, "ResponseHeaderProto"),
            vec![
                ("ClientInfoProto", "client", 1),
                ("ErrorDetailProto", "error", 2),
                ("GroupStateWatermarkProto", "state", 3),
                ("uint64", "mount_epoch", 4),
                ("uint64", "route_epoch", 5),
                ("string", "group_name", 6),
            ]
        );
        let request_header = proto_message_body(header_proto, "RequestHeaderProto");
        assert!(!request_header.contains(concat!("request", "_id")));
        assert!(!request_header.contains(concat!("trace", "_id")));
        assert!(!request_header.contains("traceparent"));
        assert!(!request_header.contains("tracestate"));
        assert!(!request_header.contains("baggage"));

        let data_header_proto = include_str!("../worker/data_header.proto");
        assert_eq!(
            proto_message_fields(data_header_proto, "DataRequestHeaderProto"),
            vec![
                ("common.ClientInfoProto", "client", 1),
                ("common.TraceContextProto", "trace_context", 2),
            ]
        );
        assert_eq!(
            proto_message_fields(data_header_proto, "DataResponseHeaderProto"),
            vec![
                ("common.ClientInfoProto", "client", 1),
                ("common.ErrorDetailProto", "error", 2),
            ]
        );
        let data_request_header = proto_message_body(data_header_proto, "DataRequestHeaderProto");
        assert!(!data_request_header.contains(concat!("request", "_id")));
        assert!(!data_request_header.contains(concat!("trace", "_id")));
        assert!(!data_request_header.contains("traceparent"));
        assert!(!data_request_header.contains("tracestate"));
        assert!(!data_request_header.contains("baggage"));

        let common_proto = include_str!("../common/common.proto");
        assert_eq!(
            proto_message_fields(common_proto, "WorkerEndpointInfoProto"),
            vec![
                ("uint64", "worker_id", 1),
                ("string", "endpoint", 2),
                ("string", "worker_run_id", 3),
            ]
        );
        assert_eq!(
            proto_message_fields(common_proto, "FileLayoutProto"),
            vec![
                ("uint32", "block_size", 1),
                ("uint32", "chunk_size", 2),
                ("uint32", "replication", 3),
                ("uint32", "block_format_id", 4),
            ]
        );
        assert_eq!(
            proto_enum_values(common_proto, "TierProto"),
            vec![
                ("TIER_UNSPECIFIED", 0),
                ("TIER_MEM", 1),
                ("TIER_NVME", 2),
                ("TIER_SSD", 3),
                ("TIER_HDD", 4),
            ]
        );

        let errors_proto = include_str!("../common/errors.proto");
        assert_eq!(
            proto_message_fields(errors_proto, "ErrorDetailProto"),
            vec![
                ("ErrorKindProto", "kind", 1),
                ("RecoveryActionProto", "recovery", 2),
                ("string", "message", 3),
            ]
        );
        let error_detail = proto_message_body(errors_proto, "ErrorDetailProto");
        assert!(!error_detail.contains("error_class"));
        assert!(!error_detail.contains("refresh_reason"));
        assert!(!error_detail.contains("retry_after_ms"));
        assert_eq!(
            proto_message_fields(errors_proto, "ErrorKindProto"),
            vec![
                ("FsErrnoProto", "fs", 1),
                ("MetadataErrorKindProto", "metadata", 2),
                ("WorkerErrorKindProto", "worker", 3),
                ("ProtocolErrorKindProto", "protocol", 4),
                ("InternalErrorKindProto", "internal", 5),
            ]
        );
        assert_eq!(
            proto_message_fields(errors_proto, "RefreshHintProto"),
            vec![
                ("string", "leader_endpoint", 1),
                ("string", "group_name", 2),
                ("uint64", "mount_epoch", 3),
                ("string", "mount_prefix", 4),
                ("uint64", "route_epoch", 5),
                ("WorkerEndpointInfoProto", "worker_endpoints", 6),
                ("bool", "worker_resolve_required", 7),
            ]
        );
        assert_eq!(
            proto_enum_values(errors_proto, "MetadataErrorKindProto"),
            vec![
                ("METADATA_ERROR_KIND_UNSPECIFIED", 0),
                ("METADATA_ERROR_KIND_NOT_FOUND", 1),
                ("METADATA_ERROR_KIND_ALREADY_EXISTS", 2),
                ("METADATA_ERROR_KIND_NOT_DIRECTORY", 3),
                ("METADATA_ERROR_KIND_IS_DIRECTORY", 4),
                ("METADATA_ERROR_KIND_DIRECTORY_NOT_EMPTY", 5),
                ("METADATA_ERROR_KIND_CROSS_MOUNT_RENAME", 6),
                ("METADATA_ERROR_KIND_BUSY", 7),
                ("METADATA_ERROR_KIND_CONFLICT", 8),
                ("METADATA_ERROR_KIND_NOT_LEADER", 9),
                ("METADATA_ERROR_KIND_STALE_STATE", 10),
                ("METADATA_ERROR_KIND_MOUNT_EPOCH_MISMATCH", 11),
                ("METADATA_ERROR_KIND_ROUTE_EPOCH_MISMATCH", 12),
                ("METADATA_ERROR_KIND_OWNER_GROUP_MISMATCH", 13),
                ("METADATA_ERROR_KIND_GROUP_MISMATCH", 14),
                ("METADATA_ERROR_KIND_FENCING", 15),
                ("METADATA_ERROR_KIND_SESSION_INVALID", 16),
                ("METADATA_ERROR_KIND_SESSION_EXPIRED", 17),
                ("METADATA_ERROR_KIND_EPOCH_MISMATCH", 18),
                ("METADATA_ERROR_KIND_RESOURCE_EXHAUSTED", 19),
            ]
        );

        let metadata_proto = include_str!("../metadata/filesystem.proto");
        let write_handle = proto_message_body(metadata_proto, "WriteHandleProto");
        assert_eq!(
            proto_message_fields(metadata_proto, "WriteHandleProto"),
            vec![
                ("common.DataHandleIdProto", "data_handle_id", 1),
                ("uint64", "write_lease_epoch", 2),
            ]
        );
        assert!(!write_handle.contains("reserved"));
        assert_eq!(
            proto_message_fields(metadata_proto, "WriteTargetProto"),
            vec![
                ("common.BlockIdProto", "block_id", 1),
                ("uint64", "file_offset", 2),
                ("uint32", "block_format_id", 3),
                ("uint64", "block_size", 4),
                ("uint32", "chunk_size", 5),
                ("uint64", "block_stamp", 6),
                ("uint64", "effective_len", 7),
                ("common.WorkerEndpointInfoProto", "worker_endpoints", 8),
                ("common.FencingTokenProto", "fencing_token", 9),
                ("common.TierProto", "tier", 10),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "FileBlockLocationProto"),
            vec![
                ("common.BlockIdProto", "block_id", 1),
                ("uint64", "file_offset", 2),
                ("uint64", "len", 3),
                ("common.WorkerEndpointInfoProto", "workers", 4),
                ("uint64", "block_stamp", 5),
                ("uint32", "block_format_id", 6),
                ("uint64", "block_size", 7),
                ("uint32", "chunk_size", 8),
                ("uint64", "effective_len", 9),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "OpenFileRequestProto"),
            vec![("common.RequestHeaderProto", "header", 1), ("string", "path", 2),]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "GetStatusResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("FileAttrsProto", "attrs", 2),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "CreateDirectoryResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("FileAttrsProto", "attrs", 2),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "OpenFileResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("common.DataHandleIdProto", "data_handle_id", 2),
                ("uint64", "file_size", 3),
                ("uint64", "content_revision", 4),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "GetBlockLocationsResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("common.DataHandleIdProto", "data_handle_id", 2),
                ("uint64", "file_size", 3),
                ("FileBlockLocationProto", "locations", 4),
                ("uint64", "content_revision", 5),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "CreateFileResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("common.DataHandleIdProto", "data_handle_id", 2),
                ("common.FileLayoutProto", "layout", 3),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "OpenWriteRequestProto"),
            vec![
                ("common.RequestHeaderProto", "header", 1),
                ("string", "path", 2),
                ("OpenWriteModeProto", "mode", 3),
                ("uint64", "desired_len", 4),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "OpenWriteResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("WriteHandleProto", "write_handle", 2),
                ("uint64", "base_size", 3),
                ("uint64", "expires_at_ms", 4),
                ("common.FileLayoutProto", "layout", 5),
                ("uint64", "content_revision", 6),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "AddBlockRequestProto"),
            vec![
                ("common.RequestHeaderProto", "header", 1),
                ("WriteHandleProto", "write_handle", 2),
                ("uint64", "desired_len", 3),
                ("common.BlockIdProto", "previous_block_id", 4),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "CommitFileRequestProto"),
            vec![
                ("common.RequestHeaderProto", "header", 1),
                ("WriteHandleProto", "write_handle", 2),
                ("CommittedBlockProto", "committed_blocks", 3),
                ("uint64", "final_size", 4),
                ("uint64", "expected_content_revision", 5),
                ("OpenWriteModeProto", "write_mode", 6),
                ("uint64", "expected_file_size", 7),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "CommitFileResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("uint64", "committed_size", 2),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "SyncWriteRequestProto"),
            vec![
                ("common.RequestHeaderProto", "header", 1),
                ("WriteHandleProto", "write_handle", 2),
                ("CommittedBlockProto", "committed_blocks", 3),
                ("uint64", "target_size", 4),
                ("uint64", "expected_content_revision", 5),
                ("OpenWriteModeProto", "write_mode", 6),
                ("uint64", "expected_file_size", 7),
            ]
        );

        let metadata_worker_proto = include_str!("../metadata/worker.proto");
        assert_eq!(
            proto_message_fields(metadata_worker_proto, "RegisterWorkerRequestProto"),
            vec![
                ("common.RequestHeaderProto", "header", 1),
                ("uint64", "worker_id", 2),
                ("string", "worker_run_id", 3),
                ("common.EndpointProto", "advertised_endpoint", 4),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_worker_proto, "RegisterWorkerResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("uint64", "worker_id", 2),
                ("string", "accepted_worker_run_id", 3),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_worker_proto, "HeartbeatRequestProto"),
            vec![
                ("common.RequestHeaderProto", "header", 1),
                ("uint64", "worker_id", 2),
                ("string", "worker_run_id", 3),
                ("uint64", "heartbeat_seq", 4),
                ("common.EndpointProto", "advertised_endpoint", 5),
                ("CapacityInfoProto", "capacity", 6),
                ("LoadInfoProto", "load", 7),
                ("HealthStatusProto", "health", 8),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_worker_proto, "HeartbeatResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("uint64", "worker_id", 2),
                ("string", "accepted_worker_run_id", 3),
                ("uint32", "liveness_timeout_ms", 4),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_worker_proto, "BlockReportRequestProto"),
            vec![
                ("common.RequestHeaderProto", "header", 1),
                ("uint64", "worker_id", 2),
                ("string", "worker_run_id", 3),
                ("uint64", "report_seq", 4),
                ("FullBlockReportBatchProto", "full", 5),
                ("DeltaBlockReportProto", "delta", 6),
            ]
        );

        let worker_data_proto = include_str!("../worker/data.proto");
        assert_eq!(
            proto_message_fields(worker_data_proto, "OpenReadStreamRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 2),
                ("common.ByteRangeProto", "byte_range", 3),
                ("uint64", "block_stamp", 4),
                ("uint32", "frame_size", 5),
                ("string", "worker_run_id", 6),
                ("uint32", "block_format_id", 7),
                ("uint64", "block_size", 8),
                ("uint32", "chunk_size", 9),
                ("uint64", "effective_len", 10),
                ("string", "group_name", 11),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "OpenReadStreamResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("common.StreamIdProto", "stream_id", 2),
                ("uint32", "frame_size", 3),
                ("uint64", "block_stamp", 4),
                ("uint64", "committed_length", 5),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "OpenWriteStreamRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 2),
                ("uint32", "block_format_id", 3),
                ("uint64", "block_size", 4),
                ("uint32", "chunk_size", 5),
                ("uint64", "block_stamp", 6),
                ("common.FencingTokenProto", "token", 7),
                ("uint32", "frame_size", 8),
                ("string", "worker_run_id", 9),
                ("uint64", "effective_len", 10),
                ("string", "group_name", 11),
                ("common.TierProto", "tier", 12),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "OpenWriteStreamResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("common.StreamIdProto", "stream_id", 2),
                ("uint32", "frame_size", 3),
                ("uint64", "block_stamp", 4),
                ("uint64", "committed_length", 5),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "CommitWriteRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 2),
                ("common.StreamIdProto", "stream_id", 3),
                ("uint64", "effective_len", 4),
                ("uint64", "block_stamp", 5),
                ("common.FencingTokenProto", "token", 6),
                ("uint64", "commit_seq", 7),
                ("bool", "require_sync", 8),
                ("string", "worker_run_id", 9),
                ("uint32", "block_format_id", 10),
                ("uint64", "block_size", 11),
                ("uint32", "chunk_size", 12),
                ("string", "group_name", 13),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "CommitWriteResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("uint64", "effective_len", 2),
                ("uint64", "block_stamp", 3),
                ("uint64", "written_through", 4),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "SyncCommittedBlockRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 2),
                ("uint64", "block_stamp", 3),
                ("uint64", "expected_block_len", 4),
                ("string", "worker_run_id", 5),
                ("uint32", "block_format_id", 6),
                ("uint64", "block_size", 7),
                ("uint32", "chunk_size", 8),
                ("string", "group_name", 9),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "SyncCommittedBlockResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("uint64", "effective_len", 2),
                ("uint64", "block_stamp", 3),
            ]
        );

        let block_meta_proto = include_str!("../worker/block_meta.proto");
        assert_eq!(
            proto_message_fields(block_meta_proto, "BlockMetaPayloadProto"),
            vec![
                ("BlockIdentityProto", "identity", 1),
                ("BlockFormatProto", "format", 2),
                ("BlockSourceProto", "source", 3),
                ("BlockVisibilityProto", "visibility", 4),
                ("common.TierProto", "tier", 5),
            ]
        );
        assert_eq!(
            proto_message_fields(block_meta_proto, "BlockFormatProto"),
            vec![
                ("uint32", "format_id", 1),
                ("uint64", "block_size", 2),
                ("uint32", "chunk_size", 3),
            ]
        );
        assert_eq!(
            proto_message_fields(block_meta_proto, "BlockIdentityProto"),
            vec![("common.BlockIdProto", "block_id", 1), ("string", "group_name", 2),]
        );
        assert_eq!(
            proto_message_fields(block_meta_proto, "BlockSourceProto"),
            vec![("uint64", "effective_len", 1)]
        );
        assert_eq!(
            proto_message_fields(block_meta_proto, "BlockVisibilityProto"),
            vec![("BlockStateProto", "block_state", 1), ("uint64", "block_stamp", 2)]
        );
    }

    #[test]
    fn all_proto_message_fields_are_contiguous_and_unreserved() {
        for (path, source) in [
            ("common/common.proto", include_str!("../common/common.proto")),
            ("common/errors.proto", include_str!("../common/errors.proto")),
            ("common/header.proto", include_str!("../common/header.proto")),
            (
                "metadata/filesystem.proto",
                include_str!("../metadata/filesystem.proto"),
            ),
            ("metadata/worker.proto", include_str!("../metadata/worker.proto")),
            ("worker/block_meta.proto", include_str!("../worker/block_meta.proto")),
            ("worker/data.proto", include_str!("../worker/data.proto")),
            ("worker/data_header.proto", include_str!("../worker/data_header.proto")),
        ] {
            assert!(
                !source.lines().any(|line| line.trim_start().starts_with("reserved ")),
                "{path} still contains reserved fields"
            );
            for (message, tags) in proto_message_tag_sets(source) {
                let expected = (1..=tags.len() as u32).collect::<Vec<_>>();
                assert_eq!(tags, expected, "{path} message {message} has non-contiguous field tags");
            }
        }
    }

    #[test]
    fn inode_kind_proto_converts_to_domain_inode_kind() {
        let cases = [
            (
                crate::metadata::InodeKindProto::InodeKindFile,
                beryl_types::InodeKind::File,
            ),
            (
                crate::metadata::InodeKindProto::InodeKindDir,
                beryl_types::InodeKind::Dir,
            ),
            (
                crate::metadata::InodeKindProto::InodeKindSymlink,
                beryl_types::InodeKind::Symlink,
            ),
        ];

        for (proto_kind, domain_kind) in cases {
            let decoded: beryl_types::InodeKind = proto_kind.try_into().expect("known inode kind");
            let encoded: crate::metadata::InodeKindProto = domain_kind.into();
            assert_eq!(decoded, domain_kind);
            assert_eq!(encoded, proto_kind);
        }

        let err = beryl_types::InodeKind::try_from(crate::metadata::InodeKindProto::InodeKindUnspecified);
        assert!(err.is_err(), "unspecified inode kind is not a domain value");
    }

    #[test]
    fn client_id_proto_roundtrips_128_bit_value_and_rejects_zero() {
        let raw_client_id = 0x0102_0304_0506_0708_1112_1314_1516_1718u128;
        let client_id = ClientId::new(raw_client_id);

        let proto: proto_common::ClientIdProto = client_id.into();
        let decoded = ClientId::try_from(proto).expect("client id decode");

        assert_eq!(decoded, client_id);

        let err =
            ClientId::try_from(proto_common::ClientIdProto { high: 0, low: 0 }).expect_err("zero client id must fail");

        assert!(err.contains("client_id"));
    }

    #[test]
    fn required_client_and_call_identity_helpers_preserve_field_context() {
        let client_id =
            required_client_id(Some(ClientId::new(123).into()), "RequestHeader.client_id").expect("valid ClientId");
        assert_eq!(client_id, ClientId::new(123));
        assert!(
            required_client_id(None, "RequestHeader.client_id")
                .expect_err("missing client_id must fail")
                .contains("RequestHeader.client_id")
        );
        assert!(
            required_client_id(
                Some(proto_common::ClientIdProto { high: 0, low: 0 }),
                "RequestHeader.client_id"
            )
            .expect_err("zero client_id must fail")
            .contains("non-zero")
        );

        let call_id =
            require_call_id("550e8400-e29b-41d4-a716-446655440000", "RequestHeader.call_id").expect("valid CallId");
        assert_eq!(
            call_id,
            "550e8400-e29b-41d4-a716-446655440000"
                .parse::<beryl_types::CallId>()
                .expect("valid CallId")
        );
        assert!(
            require_call_id("", "RequestHeader.call_id")
                .expect_err("missing call_id must fail")
                .contains("RequestHeader.call_id")
        );
        assert!(
            require_call_id("not-a-uuid", "ResponseHeader.call_id")
                .expect_err("invalid call_id must fail")
                .contains("ResponseHeader.call_id")
        );
    }

    #[test]
    fn test_response_header_proto_to_rpc_refresh_metadata() {
        let proto_header = proto_common::ResponseHeaderProto {
            client: Some(proto_common::ClientInfoProto {
                call_id: beryl_types::CallId::new().to_string(),
                client_id: Some(ClientId::new(99).into()),
                client_name: String::new(),
            }),
            error: Some(proto_common::ErrorDetailProto {
                kind: Some(proto_common::ErrorKindProto {
                    kind: Some(proto_common::error_kind_proto::Kind::Metadata(
                        proto_common::MetadataErrorKindProto::MetadataErrorKindRouteEpochMismatch as i32,
                    )),
                }),
                recovery: Some(proto_common::RecoveryActionProto {
                    action: Some(proto_common::recovery_action_proto::Action::RefreshMetadata(
                        proto_common::RefreshMetadataRecoveryProto {
                            hint: Some(proto_common::RefreshHintProto {
                                leader_endpoint: None,
                                group_name: None,
                                mount_epoch: None,
                                mount_prefix: None,
                                route_epoch: Some(9),
                                worker_endpoints: Vec::new(),
                                worker_resolve_required: false,
                            }),
                        },
                    )),
                }),
                message: "route epoch mismatch".to_string(),
            }),
            state: Vec::new(),
            group_name: "root".to_string(),
            mount_epoch: Some(7),
            route_epoch: Some(9),
        };

        let header: ResponseHeader = proto_header.try_into().unwrap();
        assert_eq!(header.mount_epoch, Some(7));
        assert_eq!(header.route_epoch, Some(9));
        let rpc_error = header
            .rpc_error
            .as_ref()
            .expect("rpc_error must be present for non-OK status");
        assert_eq!(
            rpc_error.kind,
            RpcErrorKind::Metadata(RpcMetadataErrorKind::RouteEpochMismatch)
        );
        assert_eq!(
            rpc_error.recovery,
            RpcRecoveryAction::RefreshMetadata {
                hint: RpcRefreshHint {
                    route_epoch: Some(9),
                    ..RpcRefreshHint::default()
                }
            }
        );
    }

    #[test]
    fn test_response_header_roundtrip_refresh_metadata() {
        let hint = RpcRefreshHint {
            route_epoch: Some(11),
            ..RpcRefreshHint::default()
        };
        let rpc_error = RpcErrorDetail::refresh_metadata(
            RpcErrorKind::Metadata(RpcMetadataErrorKind::RouteEpochMismatch),
            hint.clone(),
            "route epoch mismatch",
        );
        let header = ResponseHeader::error(ClientInfo::new(ClientId::new(1)), rpc_error.clone());

        let proto: proto_common::ResponseHeaderProto = (&header).into();
        let decoded: ResponseHeader = proto.clone().try_into().expect("decode response header");
        let reencoded: proto_common::ResponseHeaderProto = (&decoded).into();

        let decoded_rpc_error = decoded
            .rpc_error
            .as_ref()
            .expect("rpc_error should persist across roundtrip");
        assert_eq!(
            decoded_rpc_error.kind,
            RpcErrorKind::Metadata(RpcMetadataErrorKind::RouteEpochMismatch)
        );
        assert_eq!(decoded_rpc_error.recovery, RpcRecoveryAction::RefreshMetadata { hint });

        assert_eq!(proto.error, reencoded.error, "wire form must roundtrip");
    }

    #[test]
    fn header_state_vector_roundtrip_preserves_multiple_groups() {
        let state = vec![
            GroupStateWatermark::new(GroupName::parse("root").unwrap(), RaftLogId::new(1, 1, 10)),
            GroupStateWatermark::new(GroupName::parse("analytics").unwrap(), RaftLogId::new(2, 3, 20)),
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

    #[test]
    fn request_header_trace_context_roundtrip_preserves_w3c_fields() {
        let request = RequestHeader::new(ClientId::new(42))
            .with_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string())
            .with_tracestate("vendor=state".to_string())
            .with_baggage("tenant=local".to_string());
        let call_id = request.client.call_id.to_string();

        let proto_request: proto_common::RequestHeaderProto = (&request).into();
        let trace = proto_request.trace_context.as_ref().expect("trace context");
        assert_eq!(
            trace.traceparent.as_deref(),
            request.trace_context.traceparent.as_deref()
        );
        assert_eq!(trace.tracestate.as_deref(), request.trace_context.tracestate.as_deref());
        assert_eq!(trace.baggage.as_deref(), request.trace_context.baggage.as_deref());
        assert_ne!(trace.traceparent.as_deref(), Some(call_id.as_str()));

        let decoded_request = RequestHeader::try_from(proto_request).expect("request header decode");
        assert_eq!(decoded_request.trace_context, request.trace_context);
        assert_eq!(decoded_request.client.call_id.to_string(), call_id);
    }

    #[test]
    fn request_and_data_headers_omit_trace_context_without_source() {
        let request = RequestHeader::new(ClientId::new(42));

        let proto_request: proto_common::RequestHeaderProto = (&request).into();
        let data_header: crate::worker::DataRequestHeaderProto = (&request).into();

        assert!(proto_request.trace_context.is_none());
        assert!(data_header.trace_context.is_none());
    }

    #[test]
    fn empty_inbound_trace_context_reencodes_as_absent() {
        let mut proto_request: proto_common::RequestHeaderProto = (&RequestHeader::new(ClientId::new(42))).into();
        proto_request.trace_context = Some(proto_common::TraceContextProto {
            traceparent: None,
            tracestate: None,
            baggage: None,
        });

        let decoded_request = RequestHeader::try_from(proto_request).expect("request header decode");
        let reencoded_request: proto_common::RequestHeaderProto = (&decoded_request).into();

        assert!(reencoded_request.trace_context.is_none());
    }

    #[test]
    fn data_header_trace_context_roundtrip_preserves_w3c_fields() {
        let request = RequestHeader::new(ClientId::new(42))
            .with_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string())
            .with_tracestate("vendor=state".to_string())
            .with_baggage("tenant=local".to_string());

        let data_header: crate::worker::DataRequestHeaderProto = (&request).into();
        let trace = data_header.trace_context.expect("data trace context");

        assert_eq!(
            trace.traceparent.as_deref(),
            request.trace_context.traceparent.as_deref()
        );
        assert_eq!(trace.tracestate.as_deref(), request.trace_context.tracestate.as_deref());
        assert_eq!(trace.baggage.as_deref(), request.trace_context.baggage.as_deref());
    }

    #[test]
    fn required_fencing_token_requires_token_and_block_id() {
        let missing_token = required_fencing_token(None, "token").expect_err("missing token must fail");
        assert!(missing_token.contains("missing token"));

        let missing_block = proto_common::FencingTokenProto {
            block_id: None,
            owner: Some(ClientId::new(7).into()),
            epoch: 11,
        };
        let missing_block =
            required_fencing_token(Some(missing_block), "token").expect_err("missing token block_id must fail");
        assert!(missing_block.contains("missing block_id in token"));
    }

    #[test]
    fn fencing_token_and_byte_range_conversion_round_trip() {
        let block_id = BlockId::from_u64_u32(42, 3);
        let token = beryl_types::lease::FencingToken::new(block_id, ClientId::new(9), 17);
        let proto_token: proto_common::FencingTokenProto = token.into();
        let decoded = beryl_types::lease::FencingToken::try_from(proto_token).expect("fencing token decode");
        assert_eq!(decoded, token);

        let range = beryl_types::chunk::ByteRange { offset: 128, len: 4096 };
        let proto_range: proto_common::ByteRangeProto = range.into();
        let decoded = beryl_types::chunk::ByteRange::from(proto_range);
        assert_eq!(decoded, range);
    }

    #[test]
    fn require_worker_run_id_preserves_field_context() {
        let parsed = require_worker_run_id(
            "550e8400-e29b-41d4-a716-446655440000",
            "RegisterWorkerRequest.worker_run_id",
        )
        .expect("valid WorkerRunId");
        assert_eq!(
            parsed,
            "550e8400-e29b-41d4-a716-446655440000"
                .parse::<beryl_types::WorkerRunId>()
                .expect("valid WorkerRunId")
        );

        let missing = require_worker_run_id("", "RegisterWorkerRequest.worker_run_id")
            .expect_err("missing worker_run_id must fail");
        assert!(missing.contains("RegisterWorkerRequest.worker_run_id"));
        assert!(missing.contains("must not be empty"));

        let invalid = require_worker_run_id("not-a-uuid", "HeartbeatRequest.worker_run_id")
            .expect_err("invalid worker_run_id must fail");
        assert!(invalid.contains("HeartbeatRequest.worker_run_id"));
        assert!(invalid.contains("invalid"));
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
            effective_len: 4096,
            worker_endpoints: Vec::new(),
            fencing_token: Some(token.into()),
            block_stamp: 55,
            chunk_size: 1024,
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
            chunk_size: 1024,
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

    #[test]
    fn shared_location_payloads_round_trip_through_proto() {
        let endpoint = beryl_types::WorkerEndpointInfo {
            worker_id: WorkerId::new(7),
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: beryl_types::WorkerNetProtocol::Grpc,
            worker_run_id: test_worker_run_id(),
        };
        let block_id = BlockId::from_u64_u32(42, 3);
        let token = FencingToken::new(block_id, ClientId::new(9), 17);

        let target = beryl_types::WriteTarget {
            block_id,
            file_offset: 128,
            block_size: 4096,
            effective_len: 3072,
            worker_endpoints: vec![endpoint.clone()],
            fencing_token: token,
            block_stamp: 55,
            chunk_size: 1024,
            block_format_id: beryl_types::layout::BlockFormatId::FULL_EFFECTIVE,
            tier: beryl_types::Tier::Hdd,
        };
        let decoded_target = beryl_types::WriteTarget::try_from(proto_metadata::WriteTargetProto::from(target.clone()))
            .expect("write target decodes");
        assert_eq!(decoded_target, target);

        let missing_format = proto_metadata::WriteTargetProto {
            block_id: Some(block_id.into()),
            file_offset: 128,
            effective_len: 4096,
            worker_endpoints: vec![endpoint.clone().into()],
            fencing_token: Some(token.into()),
            block_stamp: 55,
            chunk_size: 1024,
            block_format_id: 0,
            block_size: 4096,
            tier: proto_common::TierProto::TierHdd as i32,
        };
        let err = beryl_types::WriteTarget::try_from(missing_format).expect_err("missing format id must fail");
        assert!(err.contains("block_format_id"));

        let committed = beryl_types::CommittedBlock {
            block_id,
            file_offset: 128,
            len: 4096,
        };
        let decoded_committed =
            beryl_types::CommittedBlock::try_from(proto_metadata::CommittedBlockProto::from(committed.clone()))
                .expect("committed block decodes");
        assert_eq!(decoded_committed, committed);

        let location = beryl_types::FileBlockLocation {
            block_id,
            file_offset: 128,
            len: 4096,
            workers: vec![endpoint],
            block_stamp: 55,
            block_format_id: beryl_types::layout::BlockFormatId::FULL_EFFECTIVE,
            block_size: 4096,
            chunk_size: 1024,
            effective_len: 3072,
        };
        let decoded_location =
            beryl_types::FileBlockLocation::try_from(proto_metadata::FileBlockLocationProto::from(location.clone()))
                .expect("file block location decodes");
        assert_eq!(decoded_location, location);
    }

    fn proto_message_fields<'a>(source: &'a str, message: &str) -> Vec<(&'a str, &'a str, u32)> {
        proto_message_body(source, message)
            .lines()
            .filter_map(|raw_line| {
                let line = raw_line.split_once("//").map_or(raw_line, |(field, _)| field).trim();
                if line.is_empty() || line.starts_with("reserved") || !line.ends_with(';') {
                    return None;
                }

                let field = line.trim_end_matches(';');
                let (left, tag) = field.split_once(" = ")?;
                let (decl, name) = left.rsplit_once(' ')?;
                let ty = decl
                    .strip_prefix("optional ")
                    .or_else(|| decl.strip_prefix("repeated "))
                    .unwrap_or(decl);
                Some((ty, name, tag.parse().expect("numeric proto tag")))
            })
            .collect()
    }

    fn proto_message_body<'a>(source: &'a str, message: &str) -> &'a str {
        let start = format!("message {message} {{");
        let start_index = source
            .find(&start)
            .unwrap_or_else(|| panic!("missing proto message {message}"));
        let body_start = start_index + start.len();
        let body_end = source[body_start..]
            .find("\n}")
            .map(|offset| body_start + offset)
            .unwrap_or_else(|| panic!("unterminated proto message {message}"));
        &source[body_start..body_end]
    }

    fn proto_enum_values<'a>(source: &'a str, enum_name: &str) -> Vec<(&'a str, u32)> {
        proto_enum_body(source, enum_name)
            .lines()
            .filter_map(|raw_line| {
                let line = raw_line.split_once("//").map_or(raw_line, |(value, _)| value).trim();
                if line.is_empty() || line.starts_with("reserved") || !line.ends_with(';') {
                    return None;
                }

                let value = line.trim_end_matches(';');
                let (name, tag) = value.split_once(" = ")?;
                Some((name.trim(), tag.trim().parse().expect("numeric proto enum tag")))
            })
            .collect()
    }

    fn proto_enum_body<'a>(source: &'a str, enum_name: &str) -> &'a str {
        let start = format!("enum {enum_name} {{");
        let start_index = source
            .find(&start)
            .unwrap_or_else(|| panic!("missing proto enum {enum_name}"));
        let body_start = start_index + start.len();
        let body_end = source[body_start..]
            .find("\n}")
            .map(|offset| body_start + offset)
            .unwrap_or_else(|| panic!("unterminated proto enum {enum_name}"));
        &source[body_start..body_end]
    }

    fn proto_message_tag_sets(source: &str) -> Vec<(String, Vec<u32>)> {
        let mut messages = Vec::new();
        let mut current: Option<(String, Vec<u32>)> = None;
        let mut depth = 0usize;

        for raw_line in source.lines() {
            let line = raw_line.split_once("//").map_or(raw_line, |(code, _)| code).trim();
            if current.is_none() {
                if let Some(name) = line.strip_prefix("message ").and_then(|decl| decl.strip_suffix(" {")) {
                    current = Some((name.to_string(), Vec::new()));
                    depth = 1;
                }
                continue;
            }

            if line.ends_with(';')
                && let Some((_, tag)) = line.trim_end_matches(';').split_once(" = ")
                && let Ok(tag) = tag.parse::<u32>()
            {
                current.as_mut().expect("message state").1.push(tag);
            }

            depth = depth
                .saturating_add(line.chars().filter(|ch| *ch == '{').count())
                .saturating_sub(line.chars().filter(|ch| *ch == '}').count());
            if depth == 0 {
                let mut message = current.take().expect("message state");
                message.1.sort_unstable();
                messages.push(message);
            }
        }
        assert!(current.is_none(), "unterminated proto message");
        messages
    }
}
