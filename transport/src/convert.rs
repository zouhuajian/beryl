// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Conversion utilities between domain types and proto types.
//!
//! This module handles conversions for transport layer, focusing on zero-copy
//! or minimal-copy strategies.

use crate::error::TransportResult;
use bytes::Bytes;
use proto::common::ChunkIdProto as ProtoChunkId;
use proto::worker::{ChunkDataProto as ProtoChunkData, ChunkSliceProto as ProtoChunkSlice};
use types::chunk::{ChunkData, ChunkSlice};
use types::ids::{BlockId, DataHandleId};

/// Convert domain ChunkData to proto ChunkData.
///
/// Note: This requires one copy because proto uses Vec<u8> while domain uses Bytes.
/// This is tracked via metrics for observability.
pub fn chunk_data_to_proto(chunk: &ChunkData) -> ProtoChunkData {
    // TODO: metrics::counter!(transport::BYTES_COPIED_TOTAL).increment(chunk.data.len() as u64);
    // TODO: metrics::counter!(transport::CHUNK_CONVERSIONS_TOTAL, "direction" => "to_proto").increment(1);

    ProtoChunkData {
        slice: Some(chunk_slice_to_proto(&chunk.slice)),
        data: chunk.data.clone(), // Clone Bytes for proto (Bytes is cheap to clone)
        checksum32: chunk.checksum32,
    }
}

/// Convert proto ChunkData to domain ChunkData.
///
/// This uses Bytes::from() which creates a single allocation (no extra copy).
pub fn chunk_data_from_proto(proto: ProtoChunkData) -> TransportResult<ChunkData> {
    let slice = proto
        .slice
        .ok_or_else(|| crate::error::TransportError::Protocol("missing slice in ChunkData".to_string()))?;

    // TODO: metrics::counter!(transport::CHUNK_CONVERSIONS_TOTAL, "direction" => "from_proto").increment(1);

    Ok(ChunkData {
        slice: chunk_slice_from_proto(&slice)?,
        data: Bytes::from(proto.data), // Single allocation: Vec<u8> -> Bytes
        checksum32: proto.checksum32,
    })
}

/// Convert domain ChunkSlice to proto ChunkSlice.
pub fn chunk_slice_to_proto(slice: &ChunkSlice) -> ProtoChunkSlice {
    use proto::common::BlockIdProto as ProtoBlockId;

    ProtoChunkSlice {
        chunk: Some(ProtoChunkId {
            block: Some(ProtoBlockId {
                data_handle_id: slice.chunk.block_id.data_handle_id.as_raw(),
                block_index: slice.chunk.block_id.index.as_raw(),
            }),
            chunk_index: slice.chunk.chunk_idx,
        }),
        offset_in_chunk: slice.offset_in_chunk,
        len: slice.len,
    }
}

/// Convert proto ChunkSlice to domain ChunkSlice.
pub fn chunk_slice_from_proto(proto: &ProtoChunkSlice) -> TransportResult<ChunkSlice> {
    use types::ids::BlockIndex;

    let chunk_id = proto
        .chunk
        .as_ref()
        .ok_or_else(|| crate::error::TransportError::Protocol("missing chunk in ChunkSliceProto".to_string()))?;

    let block_id_proto = chunk_id
        .block
        .as_ref()
        .ok_or_else(|| crate::error::TransportError::Protocol("missing block in ChunkIdProto".to_string()))?;

    Ok(ChunkSlice {
        chunk: types::chunk::ChunkRef {
            block_id: BlockId::new(
                DataHandleId::new(block_id_proto.data_handle_id),
                BlockIndex::new(block_id_proto.block_index),
            ),
            chunk_idx: chunk_id.chunk_index,
        },
        offset_in_chunk: proto.offset_in_chunk,
        len: proto.len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::chunk::ChunkRef;
    use types::ids::{BlockId, BlockIndex, DataHandleId};

    #[test]
    fn test_chunk_data_conversion_empty() {
        let slice = ChunkSlice {
            chunk: ChunkRef {
                block_id: BlockId::new(DataHandleId::new(1), BlockIndex::new(2)),
                chunk_idx: 0,
            },
            offset_in_chunk: 0,
            len: 0,
        };
        let chunk = ChunkData {
            slice,
            data: Bytes::new(),
            checksum32: 0,
        };

        let proto = chunk_data_to_proto(&chunk);
        let back = chunk_data_from_proto(proto).unwrap();

        assert_eq!(chunk.slice.chunk.block_id, back.slice.chunk.block_id);
        assert_eq!(chunk.slice.chunk.chunk_idx, back.slice.chunk.chunk_idx);
        assert_eq!(chunk.data, back.data);
        assert_eq!(chunk.checksum32, back.checksum32);
    }

    #[test]
    fn test_chunk_data_conversion_1mb() {
        let slice = ChunkSlice {
            chunk: ChunkRef {
                block_id: BlockId::new(DataHandleId::new(1), BlockIndex::new(2)),
                chunk_idx: 0,
            },
            offset_in_chunk: 0,
            len: 1024 * 1024,
        };
        let data = Bytes::from(vec![0x42u8; 1024 * 1024]);
        let chunk = ChunkData {
            slice,
            data,
            checksum32: 12345,
        };

        let proto = chunk_data_to_proto(&chunk);
        assert_eq!(proto.data.len(), 1024 * 1024);
        assert_eq!(proto.checksum32, 12345);

        let back = chunk_data_from_proto(proto).unwrap();
        assert_eq!(chunk.data.len(), back.data.len());
        assert_eq!(chunk.data, back.data);
        assert_eq!(chunk.checksum32, back.checksum32);
    }

    #[test]
    fn test_chunk_data_conversion_tail_chunk() {
        // Test boundary case: last chunk might be smaller
        let slice = ChunkSlice {
            chunk: ChunkRef {
                block_id: BlockId::new(DataHandleId::new(100), BlockIndex::new(5)),
                chunk_idx: 15, // Last chunk in block
            },
            offset_in_chunk: 0,
            len: 512, // Smaller than full chunk size
        };
        let data = Bytes::from(vec![0xAAu8; 512]);
        let chunk = ChunkData {
            slice,
            data,
            checksum32: 0xDEADBEEF,
        };

        let proto = chunk_data_to_proto(&chunk);
        let back = chunk_data_from_proto(proto).unwrap();

        assert_eq!(chunk.data.len(), back.data.len());
        assert_eq!(chunk.data, back.data);
        assert_eq!(chunk.checksum32, back.checksum32);
        assert_eq!(back.slice.chunk.chunk_idx, 15);
    }
}
