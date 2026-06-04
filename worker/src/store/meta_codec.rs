// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Protobuf payload codec for worker-local block metadata.

use prost::Message;
use proto::worker::{
    BlockFormatProto, BlockIdentityProto, BlockMetaPayloadProto, BlockSourceProto, BlockStateProto,
    BlockVisibilityProto, ChecksumKindProto,
};
use types::ids::BlockId;
use types::layout::BlockFormatId;
use types::GroupName;

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
            group_name: meta.identity.group_name.to_string(),
        }),
        format: Some(BlockFormatProto {
            format_id: meta.format.format_id.as_raw(),
            block_size: meta.format.block_size,
            chunk_size,
            checksum_kind: checksum_kind_to_proto(meta.format.checksum_kind) as i32,
        }),
        source: Some(BlockSourceProto {
            effective_len: meta.source.effective_len,
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
    let group_name = GroupName::parse(&identity.group_name)
        .map_err(|err| corrupt(format!("block meta payload invalid group name: {err}")))?;
    let format = format.ok_or_else(|| corrupt("block meta payload missing format"))?;
    let source = source.ok_or_else(|| corrupt("block meta payload missing source"))?;

    Ok(MetaFields {
        identity: BlockIdentity {
            block_id: BlockId::try_from(block_id)
                .unwrap_or_else(|()| unreachable!("BlockIdProto conversion is infallible")),
            group_name,
        },
        format: BlockFormat {
            format_id: BlockFormatId::from_raw(format.format_id)
                .map_err(|err| corrupt(format!("unsupported block format id: {err}")))?,
            block_size: format.block_size,
            chunk_size: u64::from(format.chunk_size),
            checksum_kind: checksum_kind_from_proto(format.checksum_kind)?,
        },
        source: BlockSource {
            effective_len: source.effective_len,
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
