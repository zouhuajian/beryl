// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Block runtime metadata and validation boundary.

use common::error::canonical::RefreshReason;
use common::header::RpcErrorCode;
use types::ids::{BlockId, ShardGroupId};

use crate::data::core::{ReadOpenRequest, WorkerCoreResult};
use crate::error::WorkerError;
use crate::store::block::{BlockState, LocalBlockStore};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadBlockSnapshot {
    pub group_id: ShardGroupId,
    pub block_id: BlockId,
    pub effective_block_len: u64,
    pub block_stamp: u64,
}

/// Block-level facade for open and commit decisions.
///
/// The manager owns block metadata checks, stamp validation, range validation,
/// and fencing decisions. It does not perform block data reads or writes.
#[derive(Clone, Debug)]
pub struct BlockManager {
    /// Transport frame payload size used when a caller does not request one.
    /// This controls network batching and does not define StorageChunk size.
    default_frame_size: u32,
    /// Upper bound for negotiated transport frame payload size.
    max_frame_size: u32,
    /// Per-stream application-level in-flight byte window.
    /// This is independent from protocol-native flow control.
    window_bytes: u32,
}

impl BlockManager {
    pub const DEFAULT_FRAME_SIZE: u32 = 1024 * 1024;
    pub const MAX_FRAME_SIZE: u32 = 4 * 1024 * 1024;
    pub const DEFAULT_WINDOW_BYTES: u32 = 8 * 1024 * 1024;

    pub const fn new(default_frame_size: u32, max_frame_size: u32, window_bytes: u32) -> Self {
        Self {
            default_frame_size,
            max_frame_size,
            window_bytes,
        }
    }

    pub const fn default_frame_size(&self) -> u32 {
        self.default_frame_size
    }

    pub const fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    pub const fn window_bytes(&self) -> u32 {
        self.window_bytes
    }

    pub fn validate_read(
        &self,
        store: &(dyn LocalBlockStore + Send + Sync),
        req: &ReadOpenRequest,
    ) -> WorkerCoreResult<ReadBlockSnapshot> {
        if req.block_stamp == 0 {
            return Err(WorkerError::InvalidArgument(
                "block_stamp must be metadata-assigned and non-zero".to_string(),
            ));
        }

        let meta = match store.load_meta(req.group_id, req.block_id) {
            Ok(meta) => meta,
            Err(WorkerError::NotFound(message)) => {
                return Err(Self::need_refresh(
                    RpcErrorCode::ShardMoved,
                    RefreshReason::Moved,
                    format!("local block is not available for read: {message}"),
                ));
            }
            Err(error) => return Err(error),
        };

        if meta.visibility.block_state != BlockState::Ready {
            return Err(Self::need_refresh(
                RpcErrorCode::ShardMoved,
                RefreshReason::Moved,
                format!(
                    "local block is not Ready: group_id={}, block_id={}, state={:?}",
                    req.group_id, req.block_id, meta.visibility.block_state
                ),
            ));
        }
        if req.block_stamp != meta.visibility.block_stamp {
            return Err(Self::need_refresh(
                RpcErrorCode::BlockStampMismatch,
                RefreshReason::BlockStampMismatch,
                format!(
                    "block stamp mismatch: group_id={}, block_id={}, requested={}, local={}",
                    req.group_id, req.block_id, req.block_stamp, meta.visibility.block_stamp
                ),
            ));
        }

        let range_end = req
            .byte_range
            .offset
            .checked_add(u64::from(req.byte_range.len))
            .ok_or_else(|| WorkerError::InvalidArgument("byte range offset overflow".to_string()))?;
        if req.byte_range.offset > meta.source.effective_block_len || range_end > meta.source.effective_block_len {
            return Err(WorkerError::InvalidArgument(format!(
                "byte range exceeds effective block length: group_id={}, block_id={}, offset={}, len={}, effective_block_len={}",
                req.group_id, req.block_id, req.byte_range.offset, req.byte_range.len, meta.source.effective_block_len
            )));
        }

        Ok(ReadBlockSnapshot {
            group_id: req.group_id,
            block_id: req.block_id,
            effective_block_len: meta.source.effective_block_len,
            block_stamp: meta.visibility.block_stamp,
        })
    }

    fn need_refresh(code: RpcErrorCode, reason: RefreshReason, message: String) -> WorkerError {
        WorkerError::NeedRefresh { code, reason, message }
    }
}

impl Default for BlockManager {
    fn default() -> Self {
        Self::new(
            Self::DEFAULT_FRAME_SIZE,
            Self::MAX_FRAME_SIZE,
            Self::DEFAULT_WINDOW_BYTES,
        )
    }
}
