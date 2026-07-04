// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Conversion utilities between proto messages and types crate.
//!
//! This module provides bidirectional conversion between proto messages
//! and domain types defined in the types crate.

use crate::common as proto_common;
use crate::fs as proto_fs;
use crate::metadata as proto_metadata;
use ::common::{
    Deadline,
    error::canonical::{
        CanonicalError, ErrorClass as CanonicalErrorClass, ErrorCode as CanonicalErrorCode,
        RefreshHint as CanonicalRefreshHint, RefreshReason, WorkerEndpointHint,
    },
    header::{AuthnType, CallerContext, ClientInfo, RequestHeader, ResponseHeader, RpcErrorCode, TraceContext},
};
use types::chunk::ByteRange;
use types::ids::{
    BlockId, BlockIndex, ChunkId, ChunkIndex, DataHandleId, LeaseId, MountId, ShardId, StreamId, WorkerId,
};
use types::layout::{BlockShape, FileLayout};
use types::lease::FencingToken;
use types::{
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
        let block_format_id = types::layout::BlockFormatId::from_raw(layout.block_format_id)
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

impl From<proto_fs::FileAttrsProto> for FileAttrs {
    fn from(attrs: proto_fs::FileAttrsProto) -> Self {
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

impl From<&FileAttrs> for proto_fs::FileAttrsProto {
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

impl From<FileAttrs> for proto_fs::FileAttrsProto {
    fn from(attrs: FileAttrs) -> Self {
        Self::from(&attrs)
    }
}

impl TryFrom<proto_fs::InodeKindProto> for InodeKind {
    type Error = String;

    fn try_from(kind: proto_fs::InodeKindProto) -> Result<Self, Self::Error> {
        match kind {
            proto_fs::InodeKindProto::InodeKindFile => Ok(Self::File),
            proto_fs::InodeKindProto::InodeKindDir => Ok(Self::Dir),
            proto_fs::InodeKindProto::InodeKindSymlink => Ok(Self::Symlink),
            proto_fs::InodeKindProto::InodeKindUnspecified => {
                Err("unspecified inode kind is not a domain value".to_string())
            }
        }
    }
}

impl From<InodeKind> for proto_fs::InodeKindProto {
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

/// Parse a known, explicitly specified worker network protocol value.
///
/// Caller-owned policy still decides whether a known protocol is supported or
/// whether unspecified/unknown values should default, reject, or trigger refresh.
pub fn parse_known_worker_net_protocol(value: i32) -> Result<proto_common::WorkerNetProtocolProto, String> {
    let protocol = proto_common::WorkerNetProtocolProto::try_from(value)
        .map_err(|_| format!("unknown worker_net_protocol value {value}"))?;
    if protocol == proto_common::WorkerNetProtocolProto::WorkerNetProtocolUnspecified {
        return Err("unspecified worker_net_protocol must not default to gRPC".to_string());
    }
    Ok(protocol)
}

impl From<WorkerNetProtocol> for proto_common::WorkerNetProtocolProto {
    fn from(protocol: WorkerNetProtocol) -> Self {
        match protocol {
            WorkerNetProtocol::Grpc => proto_common::WorkerNetProtocolProto::WorkerNetProtocolGrpc,
        }
    }
}

impl TryFrom<proto_common::WorkerNetProtocolProto> for WorkerNetProtocol {
    type Error = String;

    fn try_from(protocol: proto_common::WorkerNetProtocolProto) -> Result<Self, Self::Error> {
        match protocol {
            proto_common::WorkerNetProtocolProto::WorkerNetProtocolGrpc => Ok(Self::Grpc),
            proto_common::WorkerNetProtocolProto::WorkerNetProtocolQuic => {
                Err("QUIC worker_net_protocol is not supported by the Rust runtime".to_string())
            }
            proto_common::WorkerNetProtocolProto::WorkerNetProtocolRdma => {
                Err("RDMA worker_net_protocol is not supported by the Rust runtime".to_string())
            }
            proto_common::WorkerNetProtocolProto::WorkerNetProtocolUnspecified => {
                Err("unspecified worker_net_protocol must not default to gRPC".to_string())
            }
        }
    }
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
            endpoint.worker_net_protocol,
            endpoint.worker_run_id,
        )
    }
}

/// Build a shared worker endpoint value from raw wire-shaped fields.
///
/// This performs structural parsing only; protocol support and cache policy
/// remain caller-owned decisions.
pub fn worker_endpoint_info_from_parts(
    worker_id: WorkerId,
    endpoint: String,
    worker_net_protocol: i32,
    worker_run_id: String,
) -> Result<WorkerEndpointInfo, String> {
    if worker_id.as_raw() == 0 {
        return Err("WorkerEndpointInfoProto.worker_id must be non-zero".to_string());
    }
    if endpoint.is_empty() {
        return Err("WorkerEndpointInfoProto.endpoint must not be empty".to_string());
    }
    let worker_run_id = require_worker_run_id(&worker_run_id, "WorkerEndpointInfoProto.worker_run_id")?;
    let protocol = parse_known_worker_net_protocol(worker_net_protocol)?;
    Ok(WorkerEndpointInfo {
        worker_id,
        endpoint,
        worker_net_protocol: protocol.try_into()?,
        worker_run_id,
    })
}

impl From<&WorkerEndpointInfo> for proto_common::WorkerEndpointInfoProto {
    fn from(endpoint: &WorkerEndpointInfo) -> Self {
        Self {
            worker_id: endpoint.worker_id.as_raw(),
            endpoint: endpoint.endpoint.clone(),
            worker_net_protocol: proto_common::WorkerNetProtocolProto::from(endpoint.worker_net_protocol) as i32,
            worker_run_id: endpoint.worker_run_id.to_string(),
        }
    }
}

impl From<WorkerEndpointInfo> for proto_common::WorkerEndpointInfoProto {
    fn from(endpoint: WorkerEndpointInfo) -> Self {
        Self {
            worker_id: endpoint.worker_id.as_raw(),
            endpoint: endpoint.endpoint,
            worker_net_protocol: proto_common::WorkerNetProtocolProto::from(endpoint.worker_net_protocol) as i32,
            worker_run_id: endpoint.worker_run_id.to_string(),
        }
    }
}

impl TryFrom<proto_metadata::WriteTargetProto> for WriteTarget {
    type Error = String;

    fn try_from(target: proto_metadata::WriteTargetProto) -> Result<Self, Self::Error> {
        let block_format_id = types::layout::BlockFormatId::from_raw(target.block_format_id)
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
            checksum: block.checksum,
        })
    }
}

impl From<&CommittedBlock> for proto_metadata::CommittedBlockProto {
    fn from(block: &CommittedBlock) -> Self {
        Self {
            block_id: Some(block.block_id.into()),
            file_offset: block.file_offset,
            len: block.len,
            checksum: block.checksum.clone(),
        }
    }
}

impl From<CommittedBlock> for proto_metadata::CommittedBlockProto {
    fn from(block: CommittedBlock) -> Self {
        Self {
            block_id: Some(block.block_id.into()),
            file_offset: block.file_offset,
            len: block.len,
            checksum: block.checksum,
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
        let block_format_id = types::layout::BlockFormatId::from_raw(location.block_format_id)
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
// proto::common::RequestHeaderProto/ResponseHeaderProto and common::header types.
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
            trace_context,
            group_name: GroupName::parse_optional(&proto.group_name)
                .map_err(|err| format!("invalid header group_name: {err}"))?,
            mount_epoch: proto.mount_epoch,
            state,
            route_epoch: proto.route_epoch,
            principal,
            real_user,
            doas,
            authn_type,
            deadline,
            caller_context,
            retry_count: proto.retry_count,
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
            principal: header.principal.clone().unwrap_or_default(),
            real_user: header.real_user.clone().unwrap_or_default(),
            doas: header.doas.clone().unwrap_or_default(),
            authn_type: match header.authn_type {
                AuthnType::Unspecified => proto_common::AuthnTypeProto::Unspecified as i32,
                AuthnType::Simple => proto_common::AuthnTypeProto::Simple as i32,
                AuthnType::Kerberos => proto_common::AuthnTypeProto::Kerberos as i32,
                AuthnType::Token => proto_common::AuthnTypeProto::Token as i32,
            },
            deadline_ms: header.deadline.as_unix_ms(),
            caller_context: header
                .caller_context
                .as_ref()
                .map(|cc| proto_common::CallerContextProto {
                    context: cc.context.clone(),
                    signature: cc.signature.clone().unwrap_or_default(),
                }),
            retry_count: header.retry_count,
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

        debug_assert!(
            canonical_error
                .as_ref()
                .is_none_or(|err| !matches!(err.class, CanonicalErrorClass::Ok)),
            "Ok canonical error detail must be normalized away"
        );

        let state = proto
            .state
            .into_iter()
            .map(GroupStateWatermark::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResponseHeader {
            client,
            canonical_error,
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
        let canonical_owned = header
            .canonical_error
            .as_ref()
            .filter(|err| !matches!(err.class, CanonicalErrorClass::Ok))
            .cloned();

        let error_detail = canonical_owned.as_ref().map(canonical_to_error_detail);

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
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeInvalidHeader as i32 => RpcErrorCode::InvalidHeader,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeNotLeader as i32 => RpcErrorCode::NotLeader,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeStaleState as i32 => RpcErrorCode::StaleState,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeMountEpochMismatch as i32 => {
            RpcErrorCode::MountEpochMismatch
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeRouteEpochMismatch as i32 => {
            RpcErrorCode::RouteEpochMismatch
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeWorkerNotRegistered as i32 => {
            RpcErrorCode::WorkerNotRegistered
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeWorkerRunMismatch as i32 => {
            RpcErrorCode::WorkerRunMismatch
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeWorkerDescriptorMismatch as i32 => {
            RpcErrorCode::WorkerDescriptorMismatch
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeFullReportRequired as i32 => {
            RpcErrorCode::FullReportRequired
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeBlockLocationUnavailable as i32 => {
            RpcErrorCode::BlockLocationUnavailable
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeBlockStampMismatch as i32 => {
            RpcErrorCode::BlockStampMismatch
        }
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeEpochMismatch as i32 => RpcErrorCode::EpochMismatch,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeFencing as i32 => RpcErrorCode::Fencing,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeShardMoved as i32 => RpcErrorCode::ShardMoved,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeNodeUnavailable as i32 => RpcErrorCode::NodeUnavailable,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeInvalidArgument as i32 => RpcErrorCode::InvalidArgument,
        x if x == proto_common::RpcErrorCodeProto::RpcErrCodeInternal as i32 => RpcErrorCode::Application,
        _ => RpcErrorCode::Application,
    }
}

fn rpc_code_enum_to_proto(code: RpcErrorCode) -> i32 {
    match code {
        RpcErrorCode::Unspecified => proto_common::RpcErrorCodeProto::RpcErrCodeUnspecified as i32,
        RpcErrorCode::InvalidHeader => proto_common::RpcErrorCodeProto::RpcErrCodeInvalidHeader as i32,
        RpcErrorCode::NotLeader => proto_common::RpcErrorCodeProto::RpcErrCodeNotLeader as i32,
        RpcErrorCode::StaleState => proto_common::RpcErrorCodeProto::RpcErrCodeStaleState as i32,
        RpcErrorCode::MountEpochMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeMountEpochMismatch as i32,
        RpcErrorCode::RouteEpochMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeRouteEpochMismatch as i32,
        RpcErrorCode::WorkerNotRegistered => proto_common::RpcErrorCodeProto::RpcErrCodeWorkerNotRegistered as i32,
        RpcErrorCode::WorkerRunMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeWorkerRunMismatch as i32,
        RpcErrorCode::WorkerDescriptorMismatch => {
            proto_common::RpcErrorCodeProto::RpcErrCodeWorkerDescriptorMismatch as i32
        }
        RpcErrorCode::FullReportRequired => proto_common::RpcErrorCodeProto::RpcErrCodeFullReportRequired as i32,
        RpcErrorCode::BlockLocationUnavailable => {
            proto_common::RpcErrorCodeProto::RpcErrCodeBlockLocationUnavailable as i32
        }
        RpcErrorCode::BlockStampMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeBlockStampMismatch as i32,
        RpcErrorCode::EpochMismatch => proto_common::RpcErrorCodeProto::RpcErrCodeEpochMismatch as i32,
        RpcErrorCode::Fencing => proto_common::RpcErrorCodeProto::RpcErrCodeFencing as i32,
        RpcErrorCode::ShardMoved => proto_common::RpcErrorCodeProto::RpcErrCodeShardMoved as i32,
        RpcErrorCode::NodeUnavailable => proto_common::RpcErrorCodeProto::RpcErrCodeNodeUnavailable as i32,
        RpcErrorCode::InvalidArgument => proto_common::RpcErrorCodeProto::RpcErrCodeInvalidArgument as i32,
        RpcErrorCode::Application => proto_common::RpcErrorCodeProto::RpcErrCodeApplication as i32,
    }
}

fn refresh_reason_proto_to_enum(reason: proto_common::RefreshReasonProto) -> RefreshReason {
    match reason {
        proto_common::RefreshReasonProto::RefreshReasonUnknown => RefreshReason::Unknown,
        proto_common::RefreshReasonProto::RefreshReasonNotLeader => RefreshReason::NotLeader,
        proto_common::RefreshReasonProto::RefreshReasonOwnerGroupMismatch => RefreshReason::OwnerGroupMismatch,
        proto_common::RefreshReasonProto::RefreshReasonMoved => RefreshReason::Moved,
        proto_common::RefreshReasonProto::RefreshReasonStaleState => RefreshReason::StaleState,
        proto_common::RefreshReasonProto::RefreshReasonMountEpochMismatch => RefreshReason::MountEpochMismatch,
        proto_common::RefreshReasonProto::RefreshReasonRouteEpochMismatch => RefreshReason::RouteEpochMismatch,
        proto_common::RefreshReasonProto::RefreshReasonGroupMismatch => RefreshReason::GroupMismatch,
        proto_common::RefreshReasonProto::RefreshReasonNeedRegister => RefreshReason::NeedRegister,
        proto_common::RefreshReasonProto::RefreshReasonWorkerRunMismatch => RefreshReason::WorkerRunMismatch,
        proto_common::RefreshReasonProto::RefreshReasonFullReportRequired => RefreshReason::FullReportRequired,
        proto_common::RefreshReasonProto::RefreshReasonBlockLocationUnavailable => {
            RefreshReason::BlockLocationUnavailable
        }
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
        RefreshReason::OwnerGroupMismatch => proto_common::RefreshReasonProto::RefreshReasonOwnerGroupMismatch as i32,
        RefreshReason::Moved => proto_common::RefreshReasonProto::RefreshReasonMoved as i32,
        RefreshReason::StaleState => proto_common::RefreshReasonProto::RefreshReasonStaleState as i32,
        RefreshReason::MountEpochMismatch => proto_common::RefreshReasonProto::RefreshReasonMountEpochMismatch as i32,
        RefreshReason::RouteEpochMismatch => proto_common::RefreshReasonProto::RefreshReasonRouteEpochMismatch as i32,
        RefreshReason::GroupMismatch => proto_common::RefreshReasonProto::RefreshReasonGroupMismatch as i32,
        RefreshReason::NeedRegister => proto_common::RefreshReasonProto::RefreshReasonNeedRegister as i32,
        RefreshReason::WorkerRunMismatch => proto_common::RefreshReasonProto::RefreshReasonWorkerRunMismatch as i32,
        RefreshReason::FullReportRequired => proto_common::RefreshReasonProto::RefreshReasonFullReportRequired as i32,
        RefreshReason::BlockLocationUnavailable => {
            proto_common::RefreshReasonProto::RefreshReasonBlockLocationUnavailable as i32
        }
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
                worker_net_protocol: endpoint.worker_net_protocol,
            })
            .collect(),
        worker_resolve_required: hint.worker_resolve_required,
    })
}

fn refresh_hint_to_proto(hint: Option<&CanonicalRefreshHint>) -> Option<proto_common::RefreshHintProto> {
    hint.map(|hint| proto_common::RefreshHintProto {
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
                worker_net_protocol: endpoint.worker_net_protocol,
                worker_run_id: String::new(),
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

    fn test_worker_run_id() -> types::WorkerRunId {
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
        let proto_attrs = crate::fs::FileAttrsProto {
            mode: 0o100755,
            uid: 501,
            gid: 20,
            size: 4096,
            atime_ms: 11,
            mtime_ms: 12,
            ctime_ms: 13,
            nlink: 2,
        };

        let attrs: types::FileAttrs = proto_attrs.into();

        assert_eq!(
            attrs,
            types::FileAttrs {
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
        let attrs = types::FileAttrs {
            mode: 0o040755,
            uid: 502,
            gid: 21,
            size: 8192,
            atime_ms: 21,
            mtime_ms: 22,
            ctime_ms: 23,
            nlink: 3,
        };

        let proto_attrs: crate::fs::FileAttrsProto = (&attrs).into();
        let owned_proto_attrs: crate::fs::FileAttrsProto = attrs.into();

        let expected = crate::fs::FileAttrsProto {
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
        let layout =
            types::layout::FileLayout::with_block_format(4096, 1024, 1, types::layout::BlockFormatId::FULL_EFFECTIVE);

        let proto: proto_common::FileLayoutProto = layout.into();
        assert_eq!(proto.block_format_id, 1);
        let decoded = types::layout::FileLayout::try_from(proto).expect("layout decodes");

        assert_eq!(decoded, layout);
    }

    #[test]
    fn file_layout_proto_rejects_missing_or_unknown_block_format_id() {
        let missing = proto_common::FileLayoutProto {
            block_size: 4096,
            chunk_size: 1024,
            replication: 1,
            block_format_id: 0,
        };
        let err = types::layout::FileLayout::try_from(missing).expect_err("missing format must fail");
        assert!(err.contains("block_format_id"));

        let unknown = proto_common::FileLayoutProto {
            block_format_id: 99,
            ..missing
        };
        let err = types::layout::FileLayout::try_from(unknown).expect_err("unknown format must fail");
        assert!(err.contains("block_format_id"));
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
                ("string", "principal", 7),
                ("string", "real_user", 8),
                ("string", "doas", 9),
                ("AuthnTypeProto", "authn_type", 10),
                ("int64", "deadline_ms", 11),
                ("CallerContextProto", "caller_context", 12),
                ("int32", "retry_count", 13),
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
        assert!(!proto_message_has_reserved_statement(request_header));
        assert!(!request_header.contains(concat!("request", "_id")));
        assert!(!request_header.contains(concat!("trace", "_id")));
        assert!(!request_header.contains("traceparent"));
        assert!(!request_header.contains("tracestate"));
        assert!(!request_header.contains("baggage"));
        let response_header = proto_message_body(header_proto, "ResponseHeaderProto");
        assert!(!proto_message_has_reserved_statement(response_header));

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
        assert!(!proto_message_has_reserved_statement(data_request_header));
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
                ("WorkerNetProtocolProto", "worker_net_protocol", 3),
                ("string", "worker_run_id", 4),
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
            proto_enum_values(errors_proto, "RefreshReasonProto"),
            vec![
                ("REFRESH_REASON_UNKNOWN", 0),
                ("REFRESH_REASON_NOT_LEADER", 1),
                ("REFRESH_REASON_MOVED", 2),
                ("REFRESH_REASON_STALE_STATE", 3),
                ("REFRESH_REASON_MOUNT_EPOCH_MISMATCH", 4),
                ("REFRESH_REASON_ROUTE_EPOCH_MISMATCH", 5),
                ("REFRESH_REASON_BLOCK_STAMP_MISMATCH", 6),
                ("REFRESH_REASON_FENCING", 7),
                ("REFRESH_REASON_EPOCH_MISMATCH", 8),
                ("REFRESH_REASON_SESSION_INVALID", 9),
                ("REFRESH_REASON_SESSION_EXPIRED", 10),
                ("REFRESH_REASON_OWNER_GROUP_MISMATCH", 11),
                ("REFRESH_REASON_GROUP_MISMATCH", 12),
                ("REFRESH_REASON_NEED_REGISTER", 13),
                ("REFRESH_REASON_WORKER_RUN_MISMATCH", 14),
                ("REFRESH_REASON_FULL_REPORT_REQUIRED", 15),
                ("REFRESH_REASON_BLOCK_LOCATION_UNAVAILABLE", 16),
            ]
        );
        assert_eq!(
            proto_message_fields(errors_proto, "RefreshHintProto"),
            vec![
                ("string", "leader_endpoint", 1),
                ("uint64", "mount_epoch", 3),
                ("string", "mount_prefix", 4),
                ("uint64", "route_epoch", 5),
                ("WorkerEndpointInfoProto", "worker_endpoints", 6),
                ("bool", "worker_resolve_required", 7),
                ("string", "group_name", 8),
            ]
        );
        assert_eq!(
            proto_enum_values(errors_proto, "RpcErrorCodeProto"),
            vec![
                ("RPC_ERR_CODE_UNSPECIFIED", 0),
                ("RPC_ERR_CODE_INVALID_HEADER", 2),
                ("RPC_ERR_CODE_NOT_LEADER", 40),
                ("RPC_ERR_CODE_STALE_STATE", 41),
                ("RPC_ERR_CODE_FENCING", 42),
                ("RPC_ERR_CODE_SHARD_MOVED", 43),
                ("RPC_ERR_CODE_NODE_UNAVAILABLE", 44),
                ("RPC_ERR_CODE_BLOCK_LOCATION_UNAVAILABLE", 45),
                ("RPC_ERR_CODE_MOUNT_EPOCH_MISMATCH", 50),
                ("RPC_ERR_CODE_ROUTE_EPOCH_MISMATCH", 51),
                ("RPC_ERR_CODE_BLOCK_STAMP_MISMATCH", 52),
                ("RPC_ERR_CODE_EPOCH_MISMATCH", 53),
                ("RPC_ERR_CODE_WORKER_NOT_REGISTERED", 54),
                ("RPC_ERR_CODE_WORKER_RUN_MISMATCH", 55),
                ("RPC_ERR_CODE_WORKER_DESCRIPTOR_MISMATCH", 56),
                ("RPC_ERR_CODE_FULL_REPORT_REQUIRED", 57),
                ("RPC_ERR_CODE_INVALID_ARGUMENT", 100),
                ("RPC_ERR_CODE_INTERNAL", 101),
                ("RPC_ERR_CODE_APPLICATION", 102),
            ]
        );

        let metadata_proto = include_str!("../metadata/filesystem.proto");
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
                ("fs.FileAttrsProto", "attrs", 2),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "CreateDirectoryResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("fs.FileAttrsProto", "attrs", 2),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "OpenFileResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("common.DataHandleIdProto", "data_handle_id", 2),
                ("uint64", "file_size", 3),
                ("uint64", "file_version", 4),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "GetBlockLocationsResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("common.DataHandleIdProto", "data_handle_id", 2),
                ("uint64", "file_size", 3),
                ("FileBlockLocationProto", "locations", 4),
                ("uint64", "file_version", 5),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "CreateFileResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("WriteHandleProto", "write_handle", 2),
                ("common.DataHandleIdProto", "data_handle_id", 3),
                ("uint64", "base_size", 4),
                ("WriteTargetProto", "initial_targets", 5),
                ("uint64", "expires_at_ms", 6),
                ("common.FileLayoutProto", "layout", 7),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_proto, "AppendFileResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("WriteHandleProto", "write_handle", 2),
                ("common.DataHandleIdProto", "data_handle_id", 3),
                ("uint64", "base_size", 4),
                ("WriteTargetProto", "initial_targets", 5),
                ("uint64", "expires_at_ms", 6),
                ("common.FileLayoutProto", "layout", 7),
            ]
        );

        let metadata_worker_proto = include_str!("../metadata/worker.proto");
        assert_eq!(
            proto_message_fields(metadata_worker_proto, "RegisterWorkerRequestProto"),
            vec![
                ("common.RequestHeaderProto", "header", 1),
                ("uint64", "worker_id", 3),
                ("string", "worker_run_id", 4),
                ("common.EndpointProto", "advertised_endpoint", 5),
                ("common.WorkerNetProtocolProto", "worker_net_protocol", 9),
                ("string", "group_name", 10),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_worker_proto, "RegisterWorkerResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("uint64", "worker_id", 3),
                ("string", "accepted_worker_run_id", 4),
                ("string", "group_name", 5),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_worker_proto, "HeartbeatRequestProto"),
            vec![
                ("common.RequestHeaderProto", "header", 1),
                ("uint64", "worker_id", 3),
                ("string", "worker_run_id", 4),
                ("uint64", "heartbeat_seq", 5),
                ("common.EndpointProto", "advertised_endpoint", 6),
                ("common.WorkerNetProtocolProto", "worker_net_protocol", 7),
                ("CapacityInfoProto", "capacity", 8),
                ("LoadInfoProto", "load", 9),
                ("HealthStatusProto", "health", 10),
                ("TaskAckProto", "acks", 11),
                ("string", "group_name", 12),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_worker_proto, "HeartbeatResponseProto"),
            vec![
                ("common.ResponseHeaderProto", "header", 1),
                ("WorkerCommandProto", "commands", 2),
                ("uint64", "worker_id", 7),
                ("string", "accepted_worker_run_id", 8),
                ("uint32", "heartbeat_interval_ms", 9),
                ("uint32", "liveness_timeout_ms", 10),
                ("MetadataServerRoleProto", "server_role", 11),
                ("common.EndpointProto", "leader_hint", 12),
                ("string", "group_name", 13),
            ]
        );
        assert_eq!(
            proto_message_fields(metadata_worker_proto, "BlockReportRequestProto"),
            vec![
                ("common.RequestHeaderProto", "header", 1),
                ("uint64", "worker_id", 3),
                ("string", "worker_run_id", 4),
                ("uint64", "report_seq", 5),
                ("FullBlockReportBatchProto", "full", 6),
                ("DeltaBlockReportProto", "delta", 7),
                ("string", "group_name", 8),
            ]
        );

        let worker_data_proto = include_str!("../worker/data.proto");
        assert_eq!(
            proto_message_fields(worker_data_proto, "OpenReadStreamRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 3),
                ("common.ByteRangeProto", "byte_range", 4),
                ("uint64", "block_stamp", 5),
                ("uint32", "frame_size", 6),
                ("string", "worker_run_id", 7),
                ("uint32", "block_format_id", 8),
                ("uint64", "block_size", 9),
                ("uint32", "chunk_size", 10),
                ("uint64", "effective_len", 11),
                ("string", "group_name", 12),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "OpenReadStreamResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("common.StreamIdProto", "stream_id", 2),
                ("uint32", "frame_size", 3),
                ("uint32", "window_bytes", 4),
                ("uint64", "block_stamp", 5),
                ("uint64", "committed_length", 6),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "OpenWriteStreamRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 3),
                ("uint32", "block_format_id", 4),
                ("uint64", "block_size", 5),
                ("uint32", "chunk_size", 6),
                ("worker.ChecksumKindProto", "checksum_kind", 7),
                ("uint64", "block_stamp", 8),
                ("common.FencingTokenProto", "token", 9),
                ("uint32", "frame_size", 10),
                ("string", "worker_run_id", 11),
                ("uint64", "effective_len", 12),
                ("string", "group_name", 13),
                ("common.TierProto", "tier", 14),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "OpenWriteStreamResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("common.StreamIdProto", "stream_id", 2),
                ("uint32", "frame_size", 3),
                ("uint32", "window_bytes", 4),
                ("uint64", "block_stamp", 5),
                ("uint64", "committed_length", 6),
            ]
        );
        assert_eq!(
            proto_message_fields(worker_data_proto, "CommitWriteRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 3),
                ("common.StreamIdProto", "stream_id", 4),
                ("uint64", "effective_len", 5),
                ("uint64", "block_stamp", 6),
                ("common.FencingTokenProto", "token", 7),
                ("uint64", "commit_seq", 8),
                ("bool", "require_sync", 9),
                ("string", "worker_run_id", 10),
                ("uint32", "block_format_id", 11),
                ("uint64", "block_size", 12),
                ("uint32", "chunk_size", 13),
                ("string", "group_name", 14),
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
                ("common.BlockIdProto", "block_id", 3),
                ("uint64", "block_stamp", 4),
                ("uint64", "expected_block_len", 5),
                ("string", "worker_run_id", 6),
                ("uint32", "block_format_id", 7),
                ("uint64", "block_size", 8),
                ("uint32", "chunk_size", 9),
                ("string", "group_name", 10),
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
                ("ChecksumKindProto", "checksum_kind", 4),
            ]
        );
        assert_eq!(
            proto_message_fields(block_meta_proto, "BlockIdentityProto"),
            vec![("common.BlockIdProto", "block_id", 1), ("string", "group_name", 3),]
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
    fn inode_kind_proto_converts_to_domain_inode_kind() {
        let cases = [
            (crate::fs::InodeKindProto::InodeKindFile, types::InodeKind::File),
            (crate::fs::InodeKindProto::InodeKindDir, types::InodeKind::Dir),
            (crate::fs::InodeKindProto::InodeKindSymlink, types::InodeKind::Symlink),
        ];

        for (proto_kind, domain_kind) in cases {
            let decoded: types::InodeKind = proto_kind.try_into().expect("known inode kind");
            let encoded: crate::fs::InodeKindProto = domain_kind.into();
            assert_eq!(decoded, domain_kind);
            assert_eq!(encoded, proto_kind);
        }

        let err = types::InodeKind::try_from(crate::fs::InodeKindProto::InodeKindUnspecified);
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
                .parse::<types::CallId>()
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
    fn test_response_header_proto_to_canonical_need_refresh() {
        use common::error::canonical::ErrorCode as CanonicalErrorCode;
        use common::header::RpcStatus;

        let proto_header = proto_common::ResponseHeaderProto {
            client: Some(proto_common::ClientInfoProto {
                call_id: types::CallId::new().to_string(),
                client_id: Some(ClientId::new(99).into()),
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
            group_name: "root".to_string(),
            mount_epoch: Some(7),
            route_epoch: Some(9),
        };

        let header: ResponseHeader = proto_header.try_into().unwrap();
        assert_eq!(header.status(), RpcStatus::Error);
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
        let token = types::lease::FencingToken::new(block_id, ClientId::new(9), 17);
        let proto_token: proto_common::FencingTokenProto = token.into();
        let decoded = types::lease::FencingToken::try_from(proto_token).expect("fencing token decode");
        assert_eq!(decoded, token);

        let range = types::chunk::ByteRange { offset: 128, len: 4096 };
        let proto_range: proto_common::ByteRangeProto = range.into();
        let decoded = types::chunk::ByteRange::from(proto_range);
        assert_eq!(decoded, range);
    }

    #[test]
    fn worker_net_protocol_parser_rejects_unspecified_and_unknown_but_accepts_known_values() {
        let unspecified =
            parse_known_worker_net_protocol(proto_common::WorkerNetProtocolProto::WorkerNetProtocolUnspecified as i32)
                .expect_err("unspecified must fail");
        assert!(unspecified.contains("unspecified worker_net_protocol"));

        let unknown = parse_known_worker_net_protocol(99).expect_err("unknown must fail");
        assert!(unknown.contains("unknown worker_net_protocol value 99"));

        assert_eq!(
            parse_known_worker_net_protocol(proto_common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32)
                .expect("grpc must parse"),
            proto_common::WorkerNetProtocolProto::WorkerNetProtocolGrpc
        );
        assert_eq!(
            parse_known_worker_net_protocol(proto_common::WorkerNetProtocolProto::WorkerNetProtocolQuic as i32)
                .expect("quic must parse"),
            proto_common::WorkerNetProtocolProto::WorkerNetProtocolQuic
        );
        assert_eq!(
            parse_known_worker_net_protocol(proto_common::WorkerNetProtocolProto::WorkerNetProtocolRdma as i32)
                .expect("rdma must parse"),
            proto_common::WorkerNetProtocolProto::WorkerNetProtocolRdma
        );
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
                .parse::<types::WorkerRunId>()
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
    fn worker_net_protocol_domain_conversion_rejects_unsupported_wire_values() {
        assert_eq!(
            types::WorkerNetProtocol::try_from(proto_common::WorkerNetProtocolProto::WorkerNetProtocolGrpc)
                .expect("grpc is supported"),
            types::WorkerNetProtocol::Grpc
        );
        let quic = types::WorkerNetProtocol::try_from(proto_common::WorkerNetProtocolProto::WorkerNetProtocolQuic)
            .expect_err("quic wire value is not a supported runtime protocol");
        assert!(quic.contains("QUIC worker_net_protocol is not supported"));
        let rdma = types::WorkerNetProtocol::try_from(proto_common::WorkerNetProtocolProto::WorkerNetProtocolRdma)
            .expect_err("rdma wire value is not a supported runtime protocol");
        assert!(rdma.contains("RDMA worker_net_protocol is not supported"));
    }

    #[test]
    fn worker_endpoint_info_conversion_rejects_unspecified_protocol() {
        let endpoint = proto_common::WorkerEndpointInfoProto {
            worker_id: 7,
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: proto_common::WorkerNetProtocolProto::WorkerNetProtocolUnspecified as i32,
            worker_run_id: test_worker_run_id().to_string(),
        };

        let err = types::WorkerEndpointInfo::try_from(endpoint).expect_err("unspecified protocol must fail");

        assert!(err.contains("unspecified worker_net_protocol"));
    }

    #[test]
    fn shared_location_conversion_rejects_malformed_required_fields() {
        let endpoint = || proto_common::WorkerEndpointInfoProto {
            worker_id: 7,
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: proto_common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
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
            block_format_id: types::layout::BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: 4096,
            tier: proto_common::TierProto::TierHdd as i32,
        };
        let err = types::WriteTarget::try_from(target.clone()).expect_err("empty target workers must fail");
        assert!(err.contains("worker_endpoints"));
        target.worker_endpoints.push(endpoint());
        target.block_stamp = 0;
        let err = types::WriteTarget::try_from(target).expect_err("zero target block_stamp must fail");
        assert!(err.contains("block_stamp"));

        let mut location = proto_metadata::FileBlockLocationProto {
            block_id: Some(block_id.into()),
            file_offset: 128,
            len: 4096,
            workers: Vec::new(),
            block_stamp: Some(55),
            block_format_id: types::layout::BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: 4096,
            chunk_size: 1024,
            effective_len: 4096,
        };
        let decoded_empty =
            types::FileBlockLocation::try_from(location.clone()).expect("empty read location workers are valid");
        assert!(decoded_empty.workers.is_empty());
        location.workers.push(endpoint());
        location.block_stamp = None;
        let err = types::FileBlockLocation::try_from(location.clone()).expect_err("missing block_stamp must fail");
        assert!(err.contains("block_stamp missing"));
        location.block_stamp = Some(0);
        let err = types::FileBlockLocation::try_from(location).expect_err("zero block_stamp must fail");
        assert!(err.contains("block_stamp"));
    }

    #[test]
    fn shared_location_payloads_round_trip_through_proto() {
        let endpoint = types::WorkerEndpointInfo {
            worker_id: WorkerId::new(7),
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: types::WorkerNetProtocol::Grpc,
            worker_run_id: test_worker_run_id(),
        };
        let block_id = BlockId::from_u64_u32(42, 3);
        let token = FencingToken::new(block_id, ClientId::new(9), 17);

        let target = types::WriteTarget {
            block_id,
            file_offset: 128,
            block_size: 4096,
            effective_len: 3072,
            worker_endpoints: vec![endpoint.clone()],
            fencing_token: token,
            block_stamp: 55,
            chunk_size: 1024,
            block_format_id: types::layout::BlockFormatId::FULL_EFFECTIVE,
            tier: types::Tier::Hdd,
        };
        let decoded_target = types::WriteTarget::try_from(proto_metadata::WriteTargetProto::from(target.clone()))
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
        let err = types::WriteTarget::try_from(missing_format).expect_err("missing format id must fail");
        assert!(err.contains("block_format_id"));

        let committed = types::CommittedBlock {
            block_id,
            file_offset: 128,
            len: 4096,
            checksum: Some(vec![1, 2, 3]),
        };
        let decoded_committed =
            types::CommittedBlock::try_from(proto_metadata::CommittedBlockProto::from(committed.clone()))
                .expect("committed block decodes");
        assert_eq!(decoded_committed, committed);

        let location = types::FileBlockLocation {
            block_id,
            file_offset: 128,
            len: 4096,
            workers: vec![endpoint],
            block_stamp: 55,
            block_format_id: types::layout::BlockFormatId::FULL_EFFECTIVE,
            block_size: 4096,
            chunk_size: 1024,
            effective_len: 3072,
        };
        let decoded_location =
            types::FileBlockLocation::try_from(proto_metadata::FileBlockLocationProto::from(location.clone()))
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

    fn proto_message_has_reserved_statement(body: &str) -> bool {
        body.lines().any(|raw_line| {
            raw_line
                .split_once("//")
                .map_or(raw_line, |(statement, _)| statement)
                .trim_start()
                .starts_with("reserved")
        })
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
}
