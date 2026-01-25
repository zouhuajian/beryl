// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use crate::block::{BlockMeta, BlockPlacement};
use crate::chunk::{ByteRange, ChunkBitmap, ChunkData, ChunkRef};
use crate::fs::InodeId;
use crate::ids::{BlockId, ClientId, DataHandleId, WorkerId};
use crate::layout::FileLayout;
use crate::lease::FencingToken;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReqRespError {
    #[error("not found: {0}")]
    NotFound(&'static str),

    #[error("lease fenced: expected epoch >= {expected}, got {got}")]
    LeaseFenced { expected: u64, got: u64 },

    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),

    #[error("type parse error: {0}")]
    TypeParse(&'static str),

    #[error("io error: {0}")]
    Io(&'static str),
}

#[derive(Debug, Error)]
#[error("type parse error: {ty}")]
pub struct TypeParseError {
    ty: &'static str,
}
impl TypeParseError {
    pub const fn new(ty: &'static str) -> Self {
        Self { ty }
    }
}

/// ---------------- Control Plane (Meta) ----------------

#[derive(Clone, Debug)]
pub struct CreateFileReq {
    pub path: String,
    pub layout: FileLayout,
}

#[derive(Clone, Debug)]
pub struct CreateFileResp {
    pub inode_id: InodeId,
    pub data_handle_id: DataHandleId,
}

#[derive(Clone, Debug)]
pub struct LocateReq {
    pub data_handle_id: DataHandleId,
    pub range: ByteRange,
}

/// For each touched block, return its meta and an optional presence summary hint.
#[derive(Clone, Debug)]
pub struct LocateResp {
    pub blocks: Vec<BlockMeta>,
    pub presence_hints: Vec<(BlockId, ChunkBitmap)>,
}

#[derive(Clone, Debug)]
pub struct AcquireLeaseReq {
    pub block_id: BlockId,
    pub client_id: ClientId,
}

#[derive(Clone, Debug)]
pub struct AcquireLeaseResp {
    pub token: FencingToken,
    pub placement: BlockPlacement,
}

#[derive(Clone, Debug)]
pub struct SealBlockReq {
    pub block_id: BlockId,
    pub token: FencingToken,
}

#[derive(Clone, Debug)]
pub struct SealBlockResp {
    pub sealed: bool,
}

/// Commit file-visible length; readers should not read beyond committed_length.
#[derive(Clone, Debug)]
pub struct CommitLengthReq {
    pub data_handle_id: DataHandleId,
    pub committed_length: u64,
}

#[derive(Clone, Debug)]
pub struct CommitLengthResp {
    pub committed_length: u64,
}

/// Optional: worker reports presence summary to meta (weakly consistent).
#[derive(Clone, Debug)]
pub struct ReportPresenceReq {
    pub block_id: BlockId,
    pub worker: WorkerId,
    pub summary: ChunkBitmap,
}

#[derive(Clone, Debug)]
pub struct ReportPresenceResp {}

/// ---------------- Data Plane (Worker) ----------------

#[derive(Clone, Debug)]
pub struct ReadChunkReq {
    pub chunk: ChunkRef,
    pub offset_in_chunk: u32,
    pub len: u32,
}

#[derive(Clone, Debug)]
pub struct ReadChunkResp {
    pub data: ChunkData,
}

#[derive(Clone, Debug)]
pub struct WriteChunkReq {
    pub token: FencingToken,
    pub data: ChunkData,
    /// Optional: monotonically increasing per-stream idempotency key
    pub write_id: u64,
}

#[derive(Clone, Debug)]
pub struct WriteChunkResp {
    pub stored: bool,
}

#[derive(Clone, Debug)]
pub struct ReadRangeReq {
    pub data_handle_id: DataHandleId,
    pub range: ByteRange,
    /// Hints to route without re-locating every time; client can omit.
    pub prefer_workers: Vec<WorkerId>,
}

#[derive(Clone, Debug)]
pub struct ReadRangeResp {
    pub chunks: Vec<ChunkData>,
}
