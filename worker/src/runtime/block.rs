// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Block runtime metadata and validation boundary.

use crate::data::core::{
    AbortWriteRequest, AbortWriteResult, CommitWriteRequest, CommitWriteResult, ReadOpenRequest, ReadOpenResult,
    WorkerCoreResult, WriteOpenRequest, WriteOpenResult,
};
use crate::error::WorkerError;

/// Block-level facade for open and commit decisions.
///
/// The manager will own block existence checks, stamp validation, committed
/// prefix validation, and fencing decisions. It intentionally does no file IO.
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

    pub async fn open_read(&self, _req: ReadOpenRequest) -> WorkerCoreResult<ReadOpenResult> {
        Err(Self::not_implemented("OpenReadStream"))
    }

    pub async fn open_write(&self, _req: WriteOpenRequest) -> WorkerCoreResult<WriteOpenResult> {
        Err(Self::not_implemented("OpenWriteStream"))
    }

    pub async fn commit_write(&self, _req: CommitWriteRequest) -> WorkerCoreResult<CommitWriteResult> {
        Err(Self::not_implemented("CommitWrite"))
    }

    pub async fn abort_write(&self, _req: AbortWriteRequest) -> WorkerCoreResult<AbortWriteResult> {
        Err(Self::not_implemented("AbortWrite"))
    }

    fn not_implemented(operation: &'static str) -> WorkerError {
        WorkerError::Unimplemented(format!("{operation} worker core is not implemented"))
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
