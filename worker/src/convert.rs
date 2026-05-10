// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Proto to domain type conversions.
//!
//! This module provides unified conversion logic between proto messages and domain types.
//! Uses functions instead of trait implementations to avoid orphan rule issues.

use proto::common::{BlockIdProto as ProtoBlockId, ChunkIdProto as ProtoChunkId, StreamIdProto as ProtoStreamId};
use proto::common::{
    BlockMetaProto as ProtoBlockMeta, BlockPlacementProto as ProtoBlockPlacement, ByteRangeProto as ProtoByteRange,
    FencingTokenProto as ProtoFencingToken, FileLayoutProto as ProtoFileLayout,
};
use proto::worker::{
    AbortWriteRequestProto, CommitWriteRequestProto, OpenReadStreamRequestProto, OpenWriteStreamRequestProto,
    WriteStreamRequestProto,
};
use tonic::Status;

use crate::core::{
    AbortWriteRequest, CommitWriteRequest, ReadOpenRequest, WorkerCoreResult, WriteFrame, WriteOpenRequest,
};
use crate::error::WorkerError;
use types::block::{BlockMeta, BlockPlacement, BlockState};
use types::chunk::{ByteRange, ChunkRef};
use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, StreamId, WorkerId};
use types::layout::FileLayout;
use types::lease::FencingToken;

// ========== ChunkId ==========

pub fn proto_to_chunk_ref(proto: &ProtoChunkId) -> Result<ChunkRef, Status> {
    let block_id_proto = proto
        .block
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing block in ChunkIdProto"))?;
    let block_id = BlockId::new(
        DataHandleId::new(block_id_proto.data_handle_id),
        BlockIndex::new(block_id_proto.block_index),
    );
    Ok(ChunkRef::new(block_id, proto.chunk_index))
}

pub fn proto_to_block_id(proto: Option<ProtoBlockId>, field_name: &str) -> WorkerCoreResult<BlockId> {
    let proto = proto.ok_or_else(|| WorkerError::InvalidArgument(format!("missing {field_name}")))?;
    Ok(BlockId::new(
        DataHandleId::new(proto.data_handle_id),
        BlockIndex::new(proto.block_index),
    ))
}

pub fn block_id_to_proto(block_id: BlockId) -> ProtoBlockId {
    ProtoBlockId {
        data_handle_id: block_id.data_handle_id.as_raw(),
        block_index: block_id.index.as_raw(),
    }
}

pub fn proto_to_stream_id(proto: Option<ProtoStreamId>, field_name: &str) -> WorkerCoreResult<StreamId> {
    let proto = proto.ok_or_else(|| WorkerError::InvalidArgument(format!("missing {field_name}")))?;
    let value = ((proto.high as u128) << 64) | proto.low as u128;
    Ok(StreamId::new(value))
}

pub fn stream_id_to_proto(stream_id: StreamId) -> ProtoStreamId {
    let value = stream_id.as_raw();
    ProtoStreamId {
        high: (value >> 64) as u64,
        low: value as u64,
    }
}

pub fn chunk_ref_to_proto(chunk_ref: &ChunkRef) -> ProtoChunkId {
    use proto::common::BlockIdProto as ProtoBlockId;
    ProtoChunkId {
        block: Some(ProtoBlockId {
            data_handle_id: chunk_ref.block_id.data_handle_id.as_raw(),
            block_index: chunk_ref.block_id.index.as_raw(),
        }),
        chunk_index: chunk_ref.chunk_idx,
    }
}

// ========== ByteRange ==========

pub fn proto_to_byte_range(proto: &ProtoByteRange) -> ByteRange {
    ByteRange {
        offset: proto.offset,
        len: proto.len,
    }
}

pub fn proto_to_read_open_request(proto: OpenReadStreamRequestProto) -> WorkerCoreResult<ReadOpenRequest> {
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let byte_range = proto
        .byte_range
        .ok_or_else(|| WorkerError::InvalidArgument("missing byte_range".to_string()))?;

    Ok(ReadOpenRequest {
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

pub fn byte_range_to_proto(range: &ByteRange) -> ProtoByteRange {
    ProtoByteRange {
        offset: range.offset,
        len: range.len,
    }
}

// ========== FileLayout ==========

pub fn proto_to_file_layout(proto: &ProtoFileLayout) -> FileLayout {
    FileLayout::new(proto.block_size, proto.chunk_size, proto.replication as u8)
}

pub fn file_layout_to_proto(layout: &FileLayout) -> ProtoFileLayout {
    ProtoFileLayout {
        block_size: layout.block_size,
        chunk_size: layout.chunk_size,
        replication: layout.replication as u32,
    }
}

// ========== BlockState ==========

pub fn proto_to_block_state(proto: i32) -> BlockState {
    // BlockState enum values from common.proto:
    // BLOCK_STATE_UNSPECIFIED = 0
    // BLOCK_STATE_OPEN = 1
    // BLOCK_STATE_SEALED = 2
    // BLOCK_STATE_ABORTED = 3
    match proto {
        1 => BlockState::Open,
        2 => BlockState::Sealed,
        3 => BlockState::Aborted,
        _ => BlockState::Open, // Default to Open
    }
}

pub fn block_state_to_proto(state: &BlockState) -> i32 {
    // BlockState enum values from common.proto:
    // BLOCK_STATE_UNSPECIFIED = 0
    // BLOCK_STATE_OPEN = 1
    // BLOCK_STATE_SEALED = 2
    // BLOCK_STATE_ABORTED = 3
    match state {
        BlockState::Open => 1,
        BlockState::Sealed => 2,
        BlockState::Aborted => 3,
        BlockState::Deleted => 3, // Map Deleted to Aborted for now (proto doesn't have Deleted yet)
        BlockState::Compacted => 3, // Map Compacted to Aborted for now (proto doesn't have Compacted yet)
    }
}

// ========== BlockPlacement ==========

pub fn proto_to_block_placement(proto: &ProtoBlockPlacement) -> BlockPlacement {
    BlockPlacement {
        primary: WorkerId::new(proto.primary_worker_id),
        replicas: proto.replica_worker_ids.iter().map(|&id| WorkerId::new(id)).collect(),
    }
}

pub fn block_placement_to_proto(placement: &BlockPlacement) -> ProtoBlockPlacement {
    ProtoBlockPlacement {
        primary_worker_id: placement.primary.as_raw(),
        replica_worker_ids: placement.replicas.iter().map(|id| id.as_raw()).collect(),
    }
}

// ========== FencingToken ==========

pub fn proto_to_fencing_token(proto: &ProtoFencingToken) -> Result<FencingToken, Status> {
    let block_id = proto
        .block_id
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing block_id in FencingToken"))?;
    let block_id = BlockId::new(
        DataHandleId::new(block_id.data_handle_id),
        BlockIndex::new(block_id.block_index),
    );
    Ok(FencingToken::new(block_id, ClientId::new(proto.owner), proto.epoch))
}

fn proto_to_required_fencing_token(
    proto: Option<ProtoFencingToken>,
    field_name: &str,
) -> WorkerCoreResult<FencingToken> {
    let proto = proto.ok_or_else(|| WorkerError::InvalidArgument(format!("missing {field_name}")))?;
    proto_to_fencing_token(&proto).map_err(|status| WorkerError::InvalidArgument(status.message().to_string()))
}

pub fn fencing_token_to_proto(token: &FencingToken) -> ProtoFencingToken {
    use proto::common::BlockIdProto as ProtoBlockId;
    ProtoFencingToken {
        block_id: Some(ProtoBlockId {
            data_handle_id: token.block_id.data_handle_id.as_raw(),
            block_index: token.block_id.index.as_raw(),
        }),
        owner: token.owner.as_raw(),
        epoch: token.epoch,
    }
}

// Note: FencingToken is now unified in common.proto, so we only need one set of conversion functions.
// The proto_meta_to_fencing_token and fencing_token_to_proto_meta functions are kept for compatibility
// but now use the same common.FencingToken type.
pub fn proto_meta_to_fencing_token(proto: &ProtoFencingToken) -> Result<FencingToken, Status> {
    proto_to_fencing_token(proto)
}

pub fn fencing_token_to_proto_meta(token: &FencingToken) -> ProtoFencingToken {
    fencing_token_to_proto(token)
}

// ========== BlockMeta ==========

pub fn proto_to_block_meta(proto: &ProtoBlockMeta) -> Result<BlockMeta, Status> {
    let block_id = BlockId::new(
        DataHandleId::new(proto.data_handle_id),
        BlockIndex::new(proto.block_index),
    );
    Ok(BlockMeta {
        block_id,
        inode_id: types::fs::InodeId::new(proto.inode_id),
        data_handle_id: DataHandleId::new(proto.data_handle_id),
        block_index: proto.block_index,
        start_offset: proto.start_offset,
        state: proto_to_block_state(proto.state() as i32),
        placement: proto_to_block_placement(
            proto
                .placement
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing placement"))?,
        ),
        committed_length: proto.committed_length,
    })
}

pub fn block_meta_to_proto(meta: &BlockMeta) -> ProtoBlockMeta {
    ProtoBlockMeta {
        inode_id: meta.inode_id.as_raw(),
        data_handle_id: meta.data_handle_id.as_raw(),
        block_index: meta.block_index,
        start_offset: meta.start_offset,
        state: block_state_to_proto(&meta.state),
        placement: Some(block_placement_to_proto(&meta.placement)),
        committed_length: meta.committed_length,
        block_stamp: 0, // TODO: implement block_stamp tracking
    }
}
