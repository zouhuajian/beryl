// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Protobuf payload codec for worker-local block metadata.

use super::block::{BlockMetaPayload, BlockState};
use crate::error::{WorkerError, WorkerResult};
use beryl_proto::common::TierProto;
use beryl_proto::convert::{parse_known_tier, required_block_id};
use beryl_proto::worker::{BlockMetaPayloadProto, BlockStateProto};
use beryl_types::GroupName;
use prost::Message;

/// Encode the payload inside the versioned local header; validation belongs to the store.
pub(super) fn encode_meta_payload(meta: &BlockMetaPayload) -> Vec<u8> {
    meta_to_proto(meta).encode_to_vec()
}

/// Decode known payload fields after the enclosing header version has been checked.
pub(super) fn decode_meta_payload(encoded: &[u8]) -> WorkerResult<BlockMetaPayload> {
    let proto = BlockMetaPayloadProto::decode(encoded).map_err(|err| corrupt(err.to_string()))?;
    meta_from_proto(proto)
}

fn meta_to_proto(meta: &BlockMetaPayload) -> BlockMetaPayloadProto {
    BlockMetaPayloadProto {
        group_name: meta.group_name.to_string(),
        block_id: Some(meta.block_id.into()),
        block_size: meta.block_size,
        durable_len: meta.durable_len,
        block_state: block_state_to_proto(meta.block_state) as i32,
        fencing_token: Some(meta.fencing_token.into()),
        tier: TierProto::from(meta.tier) as i32,
    }
}

fn meta_from_proto(proto: BlockMetaPayloadProto) -> WorkerResult<BlockMetaPayload> {
    let BlockMetaPayloadProto {
        group_name,
        block_id,
        block_size,
        durable_len,
        block_state,
        fencing_token,
        tier,
    } = proto;
    let group_name = GroupName::parse(&group_name)
        .map_err(|err| corrupt(format!("block meta payload invalid group name: {err}")))?;

    let tier = parse_known_tier(tier).map_err(|err| corrupt(format!("block meta payload invalid tier: {err}")))?;
    Ok(BlockMetaPayload {
        block_id: required_block_id(block_id, "block meta payload block_id").map_err(corrupt)?,
        block_state: block_state_from_proto(block_state)?,
        fencing_token: beryl_proto::convert::required_fencing_token(fencing_token, "block writer token")
            .map_err(corrupt)?,
        tier,
        group_name,
        block_size,
        durable_len,
    })
}

fn block_state_to_proto(block_state: BlockState) -> BlockStateProto {
    match block_state {
        BlockState::Deleting => BlockStateProto::BlockStateDeleting,
        BlockState::Ready => BlockStateProto::BlockStateReady,
    }
}

fn block_state_from_proto(block_state: i32) -> WorkerResult<BlockState> {
    match BlockStateProto::try_from(block_state).map_err(|_| corrupt("unsupported block state"))? {
        BlockStateProto::BlockStateUnspecified => Err(corrupt("block state must be specified")),
        BlockStateProto::BlockStateDeleting => Ok(BlockState::Deleting),
        BlockStateProto::BlockStateReady => Ok(BlockState::Ready),
    }
}

fn corrupt(message: impl Into<String>) -> WorkerError {
    WorkerError::Corrupt(message.into())
}
