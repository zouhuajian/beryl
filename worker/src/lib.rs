// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Vecton worker data-plane skeleton.

pub mod config;
pub mod control;
pub mod data;
pub mod error;
pub mod net;
pub mod observe;
pub mod runtime;
pub mod store;

#[cfg(test)]
mod tests;

pub use data::core::{
    AbortWriteRequest, AbortWriteResult, CommitWriteRequest, CommitWriteResult, RangeMapper, ReadFrame,
    ReadOpenRequest, ReadOpenResult, StorageChunkSlice, StreamContext, StreamMode, WorkerCore, WorkerCoreResult,
    WriteFrame, WriteOpenRequest, WriteOpenResult,
};
pub use error::{ErrorMetadata, WorkerError};
pub use runtime::block::BlockManager;
pub use runtime::stream::{StreamManager, StreamState};
