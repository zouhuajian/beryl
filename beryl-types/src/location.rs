// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared read/write location value objects.

use crate::ids::BlockId;
use crate::lease::FencingToken;
use crate::tier::Tier;
use crate::worker::WorkerEndpointInfo;
use serde::{Deserialize, Serialize};

/// Metadata-issued block identity, write locations, layout, and fencing authority.
///
/// Worker locations designate where the client may write; they do not prove that
/// data exists, is durable, or has been published in the file's visible layout.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocatedBlock {
    pub block_id: BlockId,
    /// Start of the block in the file, independent of its allocation index.
    pub file_offset: u64,

    /// Maximum writable capacity authorized by the file inode.
    ///
    /// Workers reserve and enforce this bound before the final effective length
    /// is known, then persist it in `BlockMetaPayload.block_size`.
    pub block_size: u64,

    /// Block-local start of the next write: zero for allocation, the visible
    /// prefix for OpenWrite, or a locally confirmed checkpoint for continuation.
    pub write_offset: u64,

    /// Selected Worker process identities retained unchanged when allocation replays.
    pub worker_endpoints: Vec<WorkerEndpointInfo>,
    /// Worker-local storage tier requested for this replica.
    pub tier: Tier,
    pub fencing_token: FencingToken,
}

/// Changed tail or new block included in a Metadata content publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedBlock {
    pub block_id: BlockId,
    /// Total block length, including the previously visible tail prefix.
    pub len: u64,
}

/// Metadata-authoritative readable location for one file range backed by a block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileBlockLocation {
    pub block_id: BlockId,
    pub file_offset: u64,
    pub len: u64,

    /// Immutable logical capacity from the file inode.
    pub block_size: u64,
    /// Block-local readable prefix expected by metadata.
    pub effective_len: u64,

    /// Metadata-issued read candidates. Empty means the authoritative layout has
    /// this block range but no live reported replica is currently eligible.
    pub workers: Vec<WorkerEndpointInfo>,
}
