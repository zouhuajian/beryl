// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Shared read/write location value objects.

use serde::{Deserialize, Serialize};

use crate::ids::BlockId;
use crate::layout::BlockFormatId;
use crate::lease::FencingToken;
use crate::worker::WorkerEndpointInfo;

/// Metadata-issued target for writing one block to worker data-plane storage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteTarget {
    pub block_id: BlockId,
    pub file_offset: u64,
    pub len: u64,
    pub worker_endpoints: Vec<WorkerEndpointInfo>,
    pub fencing_token: FencingToken,
    pub block_stamp: u64,
    pub chunk_size: u32,
    /// Metadata-selected Vecton block data/meta interpretation format.
    pub block_format_id: BlockFormatId,
}

/// Metadata commit payload for one worker-published block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedBlock {
    pub block_id: BlockId,
    pub file_offset: u64,
    pub len: u64,
    pub checksum: Option<Vec<u8>>,
}

/// Metadata-authoritative readable location for one file range backed by a block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileBlockLocation {
    pub block_id: BlockId,
    pub file_offset: u64,
    pub len: u64,
    pub workers: Vec<WorkerEndpointInfo>,
    pub worker_epoch: Option<u64>,
    pub block_stamp: u64,
}
