// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Internal worker data-plane boundary.
//!
//! This module stays private to the crate, so stream handles and block-local
//! worker operations do not appear in the public API.

mod channel_pool;
mod protocol;
mod worker;

use async_trait::async_trait;
use beryl_types::{GroupName, WriteTarget};
use bytes::Bytes;

use crate::error::ClientResult;
use crate::planner::PlannedBlockRead;
use crate::runtime::AttemptContext;

/// Internal boundary that isolates Worker RPC transport from client runtime
/// and provides a narrow seam for orchestration tests.
#[async_trait]
pub(crate) trait WorkerDataClient: Send + Sync {
    /// Reads one metadata-planned block-local range with exact-length semantics.
    async fn read_block_range(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        block_read: &PlannedBlockRead,
    ) -> ClientResult<WorkerReadResult>;

    /// Writes one complete block through one bidirectional RPC.
    async fn write_block(&self, attempt: AttemptContext, target: WorkerWriteTarget, data: Bytes) -> ClientResult<()>;
}

/// Internal worker write target derived from metadata AddBlock.
#[derive(Clone, Debug)]
pub(crate) struct WorkerWriteTarget {
    /// Metadata owner group for the target block.
    pub(crate) group_name: GroupName,
    /// Metadata AddBlock target.
    pub(crate) target: WriteTarget,
}

/// Exact bytes returned for one metadata-planned block-local range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkerReadResult {
    pub(crate) bytes: Bytes,
}

pub(crate) use worker::WorkerDataPlane;
