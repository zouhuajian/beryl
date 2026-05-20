// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Protobuf payload codec for worker-local block metadata.

use prost::Message;
use proto::worker::{
    BlockFormatProto, BlockIdentityProto, BlockMetaPayloadProto, BlockSourceProto, BlockStateProto,
    BlockVisibilityProto, ChecksumKindProto,
};
use types::ids::{BlockId, ShardGroupId};

use super::block::{
    BlockFormat, BlockIdentity, BlockMetaPayload, BlockSource, BlockState, BlockVisibility, ChecksumKind, StoreResult,
};
use crate::error::WorkerError;

pub(super) fn encode_meta_payload(meta: &BlockMetaPayload) -> StoreResult<Vec<u8>> {
    let proto = meta_to_proto(meta)?;
    encode_proto_payload(proto)
}

pub(super) fn decode_meta_payload(encoded: &[u8]) -> StoreResult<BlockMetaPayload> {
    let proto = BlockMetaPayloadProto::decode(encoded).map_err(|err| corrupt(err.to_string()))?;
    meta_from_proto(proto)
}

pub(super) fn encode_staging_meta_payload(meta: &BlockMetaPayload) -> StoreResult<Vec<u8>> {
    let proto = staging_meta_to_proto(meta)?;
    encode_proto_payload(proto)
}

pub(super) fn decode_staging_meta_payload(encoded: &[u8]) -> StoreResult<BlockMetaPayload> {
    let proto = BlockMetaPayloadProto::decode(encoded).map_err(|err| corrupt(err.to_string()))?;
    staging_meta_from_proto(proto)
}

fn meta_to_proto(meta: &BlockMetaPayload) -> StoreResult<BlockMetaPayloadProto> {
    let mut proto = meta_to_proto_without_visibility(meta)?;
    proto.visibility = Some(BlockVisibilityProto {
        block_state: block_state_to_proto(meta.visibility.block_state)? as i32,
        block_stamp: meta.visibility.block_stamp,
    });
    Ok(proto)
}

fn staging_meta_to_proto(meta: &BlockMetaPayload) -> StoreResult<BlockMetaPayloadProto> {
    if meta.visibility.block_state != BlockState::Loading {
        return Err(WorkerError::InvalidArgument(
            "published block state is not valid staging metadata".to_string(),
        ));
    }

    meta_to_proto_without_visibility(meta)
}

fn meta_to_proto_without_visibility(meta: &BlockMetaPayload) -> StoreResult<BlockMetaPayloadProto> {
    let chunk_size = u32::try_from(meta.format.chunk_size)
        .map_err(|_| WorkerError::InvalidArgument("chunk size does not fit block metadata format".to_string()))?;

    Ok(BlockMetaPayloadProto {
        identity: Some(BlockIdentityProto {
            block_id: Some(meta.identity.block_id.into()),
            group_id: Some(meta.identity.group_id.into()),
        }),
        format: Some(BlockFormatProto {
            format_id: meta.format.format_id,
            block_size: meta.format.block_size,
            chunk_size,
            checksum_kind: checksum_kind_to_proto(meta.format.checksum_kind) as i32,
        }),
        source: Some(BlockSourceProto {
            effective_block_len: meta.source.effective_block_len,
        }),
        visibility: None,
    })
}

fn meta_from_proto(proto: BlockMetaPayloadProto) -> StoreResult<BlockMetaPayload> {
    let BlockMetaPayloadProto {
        identity,
        format,
        source,
        visibility,
    } = proto;
    let fields = meta_fields_from_proto(identity, format, source)?;
    let visibility = visibility.ok_or_else(|| corrupt("block meta payload missing visibility"))?;

    Ok(BlockMetaPayload {
        identity: fields.identity,
        format: fields.format,
        source: fields.source,
        visibility: BlockVisibility {
            block_state: block_state_from_proto(visibility.block_state)?,
            block_stamp: visibility.block_stamp,
        },
    })
}

fn staging_meta_from_proto(proto: BlockMetaPayloadProto) -> StoreResult<BlockMetaPayload> {
    let BlockMetaPayloadProto {
        identity,
        format,
        source,
        visibility,
    } = proto;
    if visibility.is_some() {
        return Err(corrupt("staging block metadata must not encode final visibility"));
    }

    let fields = meta_fields_from_proto(identity, format, source)?;
    Ok(BlockMetaPayload {
        identity: fields.identity,
        format: fields.format,
        source: fields.source,
        visibility: BlockVisibility {
            block_state: BlockState::Loading,
            block_stamp: 0,
        },
    })
}

struct MetaFields {
    identity: BlockIdentity,
    format: BlockFormat,
    source: BlockSource,
}

fn meta_fields_from_proto(
    identity: Option<BlockIdentityProto>,
    format: Option<BlockFormatProto>,
    source: Option<BlockSourceProto>,
) -> StoreResult<MetaFields> {
    let identity = identity.ok_or_else(|| corrupt("block meta payload missing identity"))?;
    let block_id = identity
        .block_id
        .ok_or_else(|| corrupt("block meta payload missing block id"))?;
    let group_id = identity
        .group_id
        .ok_or_else(|| corrupt("block meta payload missing group id"))?;
    let format = format.ok_or_else(|| corrupt("block meta payload missing format"))?;
    let source = source.ok_or_else(|| corrupt("block meta payload missing source"))?;

    Ok(MetaFields {
        identity: BlockIdentity {
            block_id: BlockId::try_from(block_id)
                .unwrap_or_else(|()| unreachable!("BlockIdProto conversion is infallible")),
            group_id: ShardGroupId::try_from(group_id)
                .unwrap_or_else(|()| unreachable!("ShardGroupIdProto conversion is infallible")),
        },
        format: BlockFormat {
            format_id: format.format_id,
            block_size: format.block_size,
            chunk_size: u64::from(format.chunk_size),
            checksum_kind: checksum_kind_from_proto(format.checksum_kind)?,
        },
        source: BlockSource {
            effective_block_len: source.effective_block_len,
        },
    })
}

fn block_state_to_proto(block_state: BlockState) -> StoreResult<BlockStateProto> {
    match block_state {
        BlockState::Loading => Err(WorkerError::InvalidArgument(
            "loading block metadata is not valid final metadata".to_string(),
        )),
        BlockState::Ready => Ok(BlockStateProto::BlockStateReady),
        BlockState::Corrupt => Ok(BlockStateProto::BlockStateCorrupt),
    }
}

fn block_state_from_proto(block_state: i32) -> StoreResult<BlockState> {
    match BlockStateProto::try_from(block_state).map_err(|_| corrupt("unsupported block state"))? {
        BlockStateProto::BlockStateUnspecified => Err(corrupt("block state must be specified")),
        BlockStateProto::BlockStateReady => Ok(BlockState::Ready),
        BlockStateProto::BlockStateCorrupt => Ok(BlockState::Corrupt),
    }
}

fn checksum_kind_to_proto(checksum_kind: ChecksumKind) -> ChecksumKindProto {
    match checksum_kind {
        ChecksumKind::None => ChecksumKindProto::ChecksumKindNone,
    }
}

fn checksum_kind_from_proto(checksum_kind: i32) -> StoreResult<ChecksumKind> {
    match ChecksumKindProto::try_from(checksum_kind).map_err(|_| corrupt("unsupported checksum kind"))? {
        ChecksumKindProto::ChecksumKindUnspecified => Err(corrupt("checksum kind must be specified")),
        ChecksumKindProto::ChecksumKindNone => Ok(ChecksumKind::None),
    }
}

fn encode_proto_payload(proto: BlockMetaPayloadProto) -> StoreResult<Vec<u8>> {
    let mut encoded = Vec::with_capacity(proto.encoded_len());
    proto
        .encode(&mut encoded)
        .map_err(|err| WorkerError::Internal(err.to_string()))?;
    Ok(encoded)
}

fn corrupt(message: impl Into<String>) -> WorkerError {
    WorkerError::Corrupt(message.into())
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) fn payload_has_generated_protobuf_shape(encoded: &[u8]) -> bool {
        BlockMetaPayloadProto::decode(encoded)
            .map(|payload| {
                payload.identity.is_some()
                    && payload.format.is_some()
                    && payload.source.is_some()
                    && payload.visibility.is_some()
            })
            .unwrap_or(false)
    }

    pub(crate) fn protobuf_payload_missing_identity(meta: &BlockMetaPayload) -> Vec<u8> {
        let mut proto = meta_to_proto(meta).expect("domain meta converts to proto");
        proto.identity = None;
        encode_proto(proto)
    }

    pub(crate) fn protobuf_payload_missing_block_id(meta: &BlockMetaPayload) -> Vec<u8> {
        let mut proto = meta_to_proto(meta).expect("domain meta converts to proto");
        proto.identity.as_mut().expect("identity").block_id = None;
        encode_proto(proto)
    }

    pub(crate) fn protobuf_payload_missing_group_id(meta: &BlockMetaPayload) -> Vec<u8> {
        let mut proto = meta_to_proto(meta).expect("domain meta converts to proto");
        proto.identity.as_mut().expect("identity").group_id = None;
        encode_proto(proto)
    }

    pub(crate) fn protobuf_payload_missing_format(meta: &BlockMetaPayload) -> Vec<u8> {
        let mut proto = meta_to_proto(meta).expect("domain meta converts to proto");
        proto.format = None;
        encode_proto(proto)
    }

    pub(crate) fn protobuf_payload_missing_source(meta: &BlockMetaPayload) -> Vec<u8> {
        let mut proto = meta_to_proto(meta).expect("domain meta converts to proto");
        proto.source = None;
        encode_proto(proto)
    }

    pub(crate) fn protobuf_payload_missing_visibility(meta: &BlockMetaPayload) -> Vec<u8> {
        let mut proto = meta_to_proto(meta).expect("domain meta converts to proto");
        proto.visibility = None;
        encode_proto(proto)
    }

    pub(crate) fn protobuf_payload_with_block_state(meta: &BlockMetaPayload, block_state: i32) -> Vec<u8> {
        let mut proto = meta_to_proto(meta).expect("domain meta converts to proto");
        proto.visibility.as_mut().expect("visibility").block_state = block_state;
        encode_proto(proto)
    }

    pub(crate) fn protobuf_payload_with_checksum_kind(meta: &BlockMetaPayload, checksum_kind: i32) -> Vec<u8> {
        let mut proto = meta_to_proto(meta).expect("domain meta converts to proto");
        proto.format.as_mut().expect("format").checksum_kind = checksum_kind;
        encode_proto(proto)
    }

    fn encode_proto(proto: BlockMetaPayloadProto) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(proto.encoded_len());
        proto.encode(&mut encoded).expect("encode proto payload");
        encoded
    }
}
