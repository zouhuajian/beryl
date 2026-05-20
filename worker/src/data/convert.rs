// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Explicit conversion between worker wire messages and core domain types.

use common::header::RequestHeader;
use proto::common::{BlockIdProto, ByteRangeProto, FencingTokenProto, ShardGroupIdProto, StreamIdProto};
use proto::convert as proto_convert;
use proto::worker::{
    AbortWriteRequestProto, ChecksumKindProto, CommitWriteRequestProto, DataRequestHeaderProto,
    OpenReadStreamRequestProto, OpenWriteStreamRequestProto, ReadStreamResponseProto, WriteStreamRequestProto,
    WriteStreamResponseProto,
};
use types::chunk::ByteRange;
use types::ids::{BlockId, ShardGroupId, StreamId};
use types::lease::FencingToken;

use crate::data::core::{
    AbortWriteRequest, CommitWriteRequest, ReadFrame, ReadOpenRequest, WorkerCoreResult, WriteFrame, WriteFrameResult,
    WriteOpenRequest,
};
use crate::error::WorkerError;
use crate::store::block::ChecksumKind;

pub fn proto_to_block_id(proto: Option<BlockIdProto>, field_name: &str) -> WorkerCoreResult<BlockId> {
    proto_convert::required_block_id(proto, field_name).map_err(WorkerError::InvalidArgument)
}

pub fn proto_to_stream_id(proto: Option<StreamIdProto>, field_name: &str) -> WorkerCoreResult<StreamId> {
    proto_convert::required_stream_id(proto, field_name).map_err(WorkerError::InvalidArgument)
}

pub fn proto_to_group_id(proto: Option<ShardGroupIdProto>, field_name: &str) -> WorkerCoreResult<ShardGroupId> {
    proto_convert::required_group_id(proto, field_name).map_err(WorkerError::InvalidArgument)
}

pub fn stream_id_to_proto(stream_id: StreamId) -> StreamIdProto {
    stream_id.into()
}

pub fn proto_to_byte_range(proto: &ByteRangeProto) -> ByteRange {
    proto.into()
}

pub fn group_id_to_proto(group_id: ShardGroupId) -> ShardGroupIdProto {
    group_id.into()
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
        group_id: Some(group_id_to_proto(req.group_id)),
        block_id: Some(block_id_to_proto(req.block_id)),
        byte_range: Some(byte_range_to_proto(req.byte_range)),
        block_stamp: req.block_stamp,
        frame_size: req.frame_size,
    }
}

pub fn write_open_request_to_proto(req: WriteOpenRequest, ctx: &RequestHeader) -> OpenWriteStreamRequestProto {
    OpenWriteStreamRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_id: Some(group_id_to_proto(req.group_id)),
        block_id: Some(block_id_to_proto(req.block_id)),
        block_size: req.block_size,
        block_stamp: req.block_stamp,
        chunk_size: req.chunk_size,
        checksum_kind: checksum_kind_to_proto(req.checksum_kind),
        token: Some(fencing_token_to_proto(req.token)),
        frame_size: req.frame_size,
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
        group_id: Some(group_id_to_proto(req.group_id)),
        block_id: Some(block_id_to_proto(req.block_id)),
        stream_id: Some(stream_id_to_proto(req.stream_id)),
        effective_block_len: req.effective_block_len,
        block_stamp: req.block_stamp,
        token: Some(fencing_token_to_proto(req.token)),
        commit_seq: req.commit_seq,
        require_sync: req.require_sync,
    }
}

pub fn abort_write_request_to_proto(req: AbortWriteRequest, ctx: &RequestHeader) -> AbortWriteRequestProto {
    AbortWriteRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_id: Some(group_id_to_proto(req.group_id)),
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
    let group_id = proto_to_group_id(proto.group_id, "group_id")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let byte_range = proto
        .byte_range
        .ok_or_else(|| WorkerError::InvalidArgument("missing byte_range".to_string()))?;

    Ok(ReadOpenRequest {
        group_id,
        block_id,
        byte_range: proto_to_byte_range(&byte_range),
        block_stamp: proto.block_stamp,
        frame_size: proto.frame_size,
    })
}

pub fn proto_to_write_open_request(proto: OpenWriteStreamRequestProto) -> WorkerCoreResult<WriteOpenRequest> {
    let group_id = proto_to_group_id(proto.group_id, "group_id")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;
    let checksum_kind = proto_to_checksum_kind(proto.checksum_kind)?;

    Ok(WriteOpenRequest {
        group_id,
        block_id,
        token,
        block_stamp: proto.block_stamp,
        frame_size: proto.frame_size,
        block_size: proto.block_size,
        chunk_size: proto.chunk_size,
        checksum_kind,
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
    let group_id = proto_to_group_id(proto.group_id, "group_id")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;

    Ok(CommitWriteRequest {
        stream_id,
        group_id,
        block_id,
        token,
        commit_seq: proto.commit_seq,
        effective_block_len: proto.effective_block_len,
        block_stamp: proto.block_stamp,
        require_sync: proto.require_sync,
    })
}

pub fn proto_to_abort_write_request(proto: AbortWriteRequestProto) -> WorkerCoreResult<AbortWriteRequest> {
    let stream_id = proto_to_stream_id(proto.stream_id, "stream_id")?;
    let group_id = proto_to_group_id(proto.group_id, "group_id")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;

    Ok(AbortWriteRequest {
        stream_id,
        group_id,
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
