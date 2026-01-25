// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Proto to domain type conversions.
//!
//! This module provides unified conversion logic between proto messages and domain types.
//! Uses functions instead of trait implementations to avoid orphan rule issues.

use proto::common::ChunkIdProto as ProtoChunkId;
use proto::common::{
    BlockMetaProto as ProtoBlockMeta, BlockPlacementProto as ProtoBlockPlacement, ByteRangeProto as ProtoByteRange,
    FencingTokenProto as ProtoFencingToken, FileLayoutProto as ProtoFileLayout,
};
use proto::worker::ChunkSliceProto as ProtoChunkSlice;
use tonic::Status;

use types::block::{BlockMeta, BlockPlacement, BlockState};
use types::chunk::{ByteRange, ChunkRef, ChunkSlice};
use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, WorkerId};
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

pub fn byte_range_to_proto(range: &ByteRange) -> ProtoByteRange {
    ProtoByteRange {
        offset: range.offset,
        len: range.len,
    }
}

// ========== ChunkSlice ==========

pub fn proto_to_chunk_slice(proto: &ProtoChunkSlice) -> Result<ChunkSlice, Status> {
    let chunk = proto
        .chunk
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing chunk in ChunkSlice"))?;
    let chunk_ref = proto_to_chunk_ref(chunk)?;
    Ok(ChunkSlice {
        chunk: chunk_ref,
        offset_in_chunk: proto.offset_in_chunk,
        len: proto.len,
    })
}

pub fn chunk_slice_to_proto(slice: &ChunkSlice) -> ProtoChunkSlice {
    ProtoChunkSlice {
        chunk: Some(chunk_ref_to_proto(&slice.chunk)),
        offset_in_chunk: slice.offset_in_chunk,
        len: slice.len,
    }
}

// Legacy alias for backward compatibility during migration
pub use chunk_ref_to_proto as chunk_ref_to_proto_legacy;

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
