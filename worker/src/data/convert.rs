// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Explicit conversion between worker wire messages and core domain types.

use proto::common::{BlockIdProto, ByteRangeProto, FencingTokenProto, ShardGroupIdProto, StreamIdProto};
use proto::worker::{
    AbortWriteRequestProto, CommitWriteRequestProto, OpenReadStreamRequestProto, OpenWriteStreamRequestProto,
    WriteStreamRequestProto,
};
use types::chunk::ByteRange;
use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, ShardGroupId, StreamId};
use types::lease::FencingToken;

use crate::data::core::{
    AbortWriteRequest, CommitWriteRequest, ReadOpenRequest, WorkerCoreResult, WriteFrame, WriteOpenRequest,
};
use crate::error::WorkerError;

pub fn proto_to_block_id(proto: Option<BlockIdProto>, field_name: &str) -> WorkerCoreResult<BlockId> {
    let proto = proto.ok_or_else(|| WorkerError::InvalidArgument(format!("missing {field_name}")))?;
    Ok(BlockId::new(
        DataHandleId::new(proto.data_handle_id),
        BlockIndex::new(proto.block_index),
    ))
}

pub fn proto_to_stream_id(proto: Option<StreamIdProto>, field_name: &str) -> WorkerCoreResult<StreamId> {
    let proto = proto.ok_or_else(|| WorkerError::InvalidArgument(format!("missing {field_name}")))?;
    let value = ((proto.high as u128) << 64) | proto.low as u128;
    Ok(StreamId::new(value))
}

pub fn proto_to_group_id(proto: Option<ShardGroupIdProto>, field_name: &str) -> WorkerCoreResult<ShardGroupId> {
    let proto = proto.ok_or_else(|| WorkerError::InvalidArgument(format!("missing {field_name}")))?;
    Ok(ShardGroupId::new(proto.value))
}

pub fn stream_id_to_proto(stream_id: StreamId) -> StreamIdProto {
    let value = stream_id.as_raw();
    StreamIdProto {
        high: (value >> 64) as u64,
        low: value as u64,
    }
}

pub fn proto_to_byte_range(proto: &ByteRangeProto) -> ByteRange {
    ByteRange {
        offset: proto.offset,
        len: proto.len,
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
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;

    Ok(WriteOpenRequest {
        block_id,
        token,
        block_stamp: proto.block_stamp,
        frame_size: proto.frame_size,
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
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;

    Ok(CommitWriteRequest {
        stream_id,
        block_id,
        token,
        commit_seq: proto.commit_seq,
        committed_length: proto.committed_length,
        require_sync: proto.require_sync,
    })
}

pub fn proto_to_abort_write_request(proto: AbortWriteRequestProto) -> WorkerCoreResult<AbortWriteRequest> {
    let stream_id = proto_to_stream_id(proto.stream_id, "stream_id")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;

    Ok(AbortWriteRequest {
        stream_id,
        block_id,
        token,
        reason: proto.reason,
    })
}

pub fn proto_to_fencing_token(proto: &FencingTokenProto) -> WorkerCoreResult<FencingToken> {
    let block_id = proto
        .block_id
        .as_ref()
        .ok_or_else(|| WorkerError::InvalidArgument("missing block_id in token".to_string()))?;
    Ok(FencingToken::new(
        BlockId::new(
            DataHandleId::new(block_id.data_handle_id),
            BlockIndex::new(block_id.block_index),
        ),
        ClientId::new(proto.owner),
        proto.epoch,
    ))
}

fn proto_to_required_fencing_token(
    proto: Option<FencingTokenProto>,
    field_name: &str,
) -> WorkerCoreResult<FencingToken> {
    let proto = proto.ok_or_else(|| WorkerError::InvalidArgument(format!("missing {field_name}")))?;
    proto_to_fencing_token(&proto)
}
