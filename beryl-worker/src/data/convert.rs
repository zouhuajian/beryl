// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Explicit conversion between worker wire messages and core domain types.

use beryl_common::header::RequestHeader;
use beryl_proto::common::{BlockIdProto, ByteRangeProto, FencingTokenProto, StreamIdProto, TierProto};
use beryl_proto::convert as proto_convert;
use beryl_proto::worker::{
    AbortWriteRequestProto, ChecksumKindProto, CommitWriteRequestProto, DataRequestHeaderProto,
    OpenReadStreamRequestProto, OpenWriteStreamRequestProto, ReadStreamResponseProto, SyncCommittedBlockRequestProto,
    WriteStreamRequestProto, WriteStreamResponseProto,
};
use beryl_types::chunk::ByteRange;
use beryl_types::ids::{BlockId, StreamId};
use beryl_types::layout::BlockFormatId;
use beryl_types::lease::FencingToken;
use beryl_types::{GroupName, WorkerRunId};

use crate::data::core::{
    AbortWriteRequest, CommitWriteRequest, ReadFrame, ReadOpenRequest, SyncCommittedBlockRequest, WorkerCoreResult,
    WriteFrame, WriteFrameResult, WriteOpenRequest,
};
use crate::error::WorkerError;
use crate::store::block::ChecksumKind;

pub fn proto_to_block_id(proto: Option<BlockIdProto>, field_name: &str) -> WorkerCoreResult<BlockId> {
    proto_convert::required_block_id(proto, field_name).map_err(WorkerError::InvalidArgument)
}

pub fn proto_to_stream_id(proto: Option<StreamIdProto>, field_name: &str) -> WorkerCoreResult<StreamId> {
    proto_convert::required_stream_id(proto, field_name).map_err(WorkerError::InvalidArgument)
}

pub fn stream_id_to_proto(stream_id: StreamId) -> StreamIdProto {
    stream_id.into()
}

pub fn proto_to_byte_range(proto: &ByteRangeProto) -> ByteRange {
    proto.into()
}

pub fn block_id_to_proto(block_id: BlockId) -> BlockIdProto {
    block_id.into()
}

pub fn byte_range_to_proto(byte_range: ByteRange) -> ByteRangeProto {
    byte_range.into()
}

pub fn fencing_token_to_proto(token: FencingToken) -> FencingTokenProto {
    token.into()
}

pub fn request_header_to_data_proto(ctx: &RequestHeader) -> DataRequestHeaderProto {
    ctx.into()
}

pub fn read_open_request_to_proto(req: ReadOpenRequest, ctx: &RequestHeader) -> OpenReadStreamRequestProto {
    OpenReadStreamRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_name: req.group_name.to_string(),
        block_id: Some(block_id_to_proto(req.block_id)),
        byte_range: Some(byte_range_to_proto(req.byte_range)),
        block_stamp: req.block_stamp,
        frame_size: req.frame_size,
        worker_run_id: req.worker_run_id.to_string(),
        block_format_id: req.block_format_id.as_raw(),
        block_size: req.block_size,
        chunk_size: req.chunk_size,
        effective_len: req.effective_len,
    }
}

pub fn write_open_request_to_proto(req: WriteOpenRequest, ctx: &RequestHeader) -> OpenWriteStreamRequestProto {
    OpenWriteStreamRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_name: req.group_name.to_string(),
        block_id: Some(block_id_to_proto(req.block_id)),
        block_size: req.block_size,
        block_stamp: req.block_stamp,
        chunk_size: req.chunk_size,
        checksum_kind: checksum_kind_to_proto(req.checksum_kind),
        token: Some(fencing_token_to_proto(req.token)),
        frame_size: req.frame_size,
        block_format_id: req.block_format_id.as_raw(),
        worker_run_id: req.worker_run_id.to_string(),
        effective_len: req.effective_len,
        tier: TierProto::from(req.tier) as i32,
    }
}

pub fn write_frame_to_proto(frame: WriteFrame) -> WriteStreamRequestProto {
    WriteStreamRequestProto {
        stream_id: Some(stream_id_to_proto(frame.stream_id)),
        seq: frame.seq,
        offset_in_block: frame.offset_in_block,
        data: frame.data,
        checksum32: frame.checksum32,
    }
}

pub fn commit_write_request_to_proto(req: CommitWriteRequest, ctx: &RequestHeader) -> CommitWriteRequestProto {
    CommitWriteRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_name: req.group_name.to_string(),
        block_id: Some(block_id_to_proto(req.block_id)),
        stream_id: Some(stream_id_to_proto(req.stream_id)),
        effective_len: req.effective_len,
        block_stamp: req.block_stamp,
        token: Some(fencing_token_to_proto(req.token)),
        commit_seq: req.commit_seq,
        require_sync: req.require_sync,
        worker_run_id: req.worker_run_id.to_string(),
        block_format_id: req.block_format_id.as_raw(),
        block_size: req.block_size,
        chunk_size: req.chunk_size,
    }
}

pub fn sync_committed_block_request_to_proto(
    req: SyncCommittedBlockRequest,
    ctx: &RequestHeader,
) -> SyncCommittedBlockRequestProto {
    SyncCommittedBlockRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_name: req.group_name.to_string(),
        block_id: Some(block_id_to_proto(req.block_id)),
        block_stamp: req.block_stamp,
        expected_block_len: req.expected_block_len,
        worker_run_id: req.worker_run_id.to_string(),
        block_format_id: req.block_format_id.as_raw(),
        block_size: req.block_size,
        chunk_size: req.chunk_size,
    }
}

pub fn abort_write_request_to_proto(req: AbortWriteRequest, ctx: &RequestHeader) -> AbortWriteRequestProto {
    AbortWriteRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_name: req.group_name.to_string(),
        block_id: Some(block_id_to_proto(req.block_id)),
        stream_id: Some(stream_id_to_proto(req.stream_id)),
        token: Some(fencing_token_to_proto(req.token)),
    }
}

pub fn proto_to_read_frame(proto: ReadStreamResponseProto) -> ReadFrame {
    ReadFrame {
        offset_in_block: proto.offset_in_block,
        data: proto.data,
        checksum32: proto.checksum32,
        eos: proto.eos,
    }
}

pub fn proto_to_write_frame_result(proto: WriteStreamResponseProto) -> WriteFrameResult {
    WriteFrameResult {
        accepted: proto.accepted,
        last_acked_seq: proto.last_acked_seq,
        written_through: proto.written_through,
    }
}

pub fn proto_to_read_open_request(proto: OpenReadStreamRequestProto) -> WorkerCoreResult<ReadOpenRequest> {
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let byte_range = proto
        .byte_range
        .ok_or_else(|| WorkerError::InvalidArgument("missing byte_range".to_string()))?;
    let worker_run_id = proto_to_worker_run_id(&proto.worker_run_id)?;
    let block_format_id = BlockFormatId::from_raw(proto.block_format_id)
        .map_err(|err| WorkerError::InvalidArgument(format!("block_format_id invalid: {err}")))?;

    Ok(ReadOpenRequest {
        group_name,
        block_id,
        worker_run_id,
        byte_range: proto_to_byte_range(&byte_range),
        block_stamp: proto.block_stamp,
        block_format_id,
        block_size: proto.block_size,
        chunk_size: proto.chunk_size,
        effective_len: proto.effective_len,
        frame_size: proto.frame_size,
    })
}

pub fn proto_to_write_open_request(proto: OpenWriteStreamRequestProto) -> WorkerCoreResult<WriteOpenRequest> {
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;
    let checksum_kind = proto_to_checksum_kind(proto.checksum_kind)?;
    let block_format_id = BlockFormatId::from_raw(proto.block_format_id)
        .map_err(|err| WorkerError::InvalidArgument(format!("block_format_id invalid: {err}")))?;
    let worker_run_id = proto_to_worker_run_id(&proto.worker_run_id)?;
    let tier = proto_convert::parse_known_tier(proto.tier)
        .map_err(|err| WorkerError::InvalidArgument(format!("tier invalid: {err}")))?;

    Ok(WriteOpenRequest {
        group_name,
        block_id,
        worker_run_id,
        token,
        block_stamp: proto.block_stamp,
        frame_size: proto.frame_size,
        block_size: proto.block_size,
        block_format_id,
        chunk_size: proto.chunk_size,
        effective_len: proto.effective_len,
        checksum_kind,
        tier,
    })
}

pub fn proto_to_write_frame(proto: WriteStreamRequestProto) -> WorkerCoreResult<WriteFrame> {
    let stream_id = proto_to_stream_id(proto.stream_id, "stream_id")?;

    Ok(WriteFrame {
        stream_id,
        seq: proto.seq,
        offset_in_block: proto.offset_in_block,
        data: proto.data,
        checksum32: proto.checksum32,
    })
}

pub fn proto_to_commit_write_request(proto: CommitWriteRequestProto) -> WorkerCoreResult<CommitWriteRequest> {
    let stream_id = proto_to_stream_id(proto.stream_id, "stream_id")?;
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;
    let worker_run_id = proto_to_worker_run_id(&proto.worker_run_id)?;
    let block_format_id = BlockFormatId::from_raw(proto.block_format_id)
        .map_err(|err| WorkerError::InvalidArgument(format!("block_format_id invalid: {err}")))?;

    Ok(CommitWriteRequest {
        stream_id,
        group_name,
        block_id,
        worker_run_id,
        token,
        commit_seq: proto.commit_seq,
        effective_len: proto.effective_len,
        block_stamp: proto.block_stamp,
        block_format_id,
        block_size: proto.block_size,
        chunk_size: proto.chunk_size,
        require_sync: proto.require_sync,
    })
}

pub fn proto_to_sync_committed_block_request(
    proto: SyncCommittedBlockRequestProto,
) -> WorkerCoreResult<SyncCommittedBlockRequest> {
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let worker_run_id = proto_to_worker_run_id(&proto.worker_run_id)?;
    let block_format_id = BlockFormatId::from_raw(proto.block_format_id)
        .map_err(|err| WorkerError::InvalidArgument(format!("block_format_id invalid: {err}")))?;

    Ok(SyncCommittedBlockRequest {
        group_name,
        block_id,
        worker_run_id,
        block_stamp: proto.block_stamp,
        expected_block_len: proto.expected_block_len,
        block_format_id,
        block_size: proto.block_size,
        chunk_size: proto.chunk_size,
    })
}

pub fn proto_to_abort_write_request(proto: AbortWriteRequestProto) -> WorkerCoreResult<AbortWriteRequest> {
    let stream_id = proto_to_stream_id(proto.stream_id, "stream_id")?;
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;

    Ok(AbortWriteRequest {
        stream_id,
        group_name,
        block_id,
        token,
    })
}

pub fn proto_to_fencing_token(proto: &FencingTokenProto) -> WorkerCoreResult<FencingToken> {
    FencingToken::try_from(*proto).map_err(WorkerError::InvalidArgument)
}

fn proto_to_required_fencing_token(
    proto: Option<FencingTokenProto>,
    field_name: &str,
) -> WorkerCoreResult<FencingToken> {
    proto_convert::required_fencing_token(proto, field_name).map_err(WorkerError::InvalidArgument)
}

fn proto_to_worker_run_id(value: &str) -> WorkerCoreResult<WorkerRunId> {
    proto_convert::require_worker_run_id(value, "worker_run_id").map_err(WorkerError::InvalidArgument)
}

fn proto_to_group_name(value: &str, field_name: &str) -> WorkerCoreResult<GroupName> {
    if value.is_empty() {
        return Err(WorkerError::InvalidArgument(format!("missing {field_name}")));
    }
    GroupName::parse(value).map_err(|err| WorkerError::InvalidArgument(format!("{field_name} invalid: {err}")))
}

fn proto_to_checksum_kind(checksum_kind: i32) -> WorkerCoreResult<ChecksumKind> {
    match ChecksumKindProto::try_from(checksum_kind)
        .map_err(|_| WorkerError::InvalidArgument("unsupported checksum_kind".to_string()))?
    {
        ChecksumKindProto::ChecksumKindNone => Ok(ChecksumKind::None),
        ChecksumKindProto::ChecksumKindUnspecified => Err(WorkerError::InvalidArgument(
            "checksum_kind must be specified".to_string(),
        )),
    }
}

fn checksum_kind_to_proto(checksum_kind: ChecksumKind) -> i32 {
    match checksum_kind {
        ChecksumKind::None => ChecksumKindProto::ChecksumKindNone as i32,
    }
}
