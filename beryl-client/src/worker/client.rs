// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client entry points for Metadata-authorized Worker block IO.

use std::fmt;

use super::transport::GrpcWorkerTransport;
use super::{BlockWrite, WorkerWriteTarget};
use crate::config::ClientConfig;
use crate::error::ClientResult;
use crate::planner::PlannedBlockRead;
use crate::runtime::AttemptContext;
use beryl_types::{GroupName, LocatedBlock};
use bytes::Bytes;

/// Provides block read and write operations through the Worker transport.
pub(crate) struct WorkerClient {
    transport: GrpcWorkerTransport,
}

impl WorkerClient {
    /// Builds the production Worker client and its gRPC transport.
    pub(crate) fn from_config(config: &ClientConfig) -> Self {
        Self {
            transport: GrpcWorkerTransport::from_config(config),
        }
    }

    /// Returns one owned block range only after exact response completion.
    pub(crate) async fn read_block_range(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        block_read: &PlannedBlockRead,
    ) -> ClientResult<Bytes> {
        self.transport.read_block_range(attempt, group_name, block_read).await
    }

    /// Opens one Metadata-authorized block RPC and returns only after the
    /// transport has crossed Worker's block-open acknowledgement boundary.
    pub(crate) async fn open_write_block(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        target: LocatedBlock,
        lease_expires_at_ms: u64,
    ) -> ClientResult<BlockWrite> {
        self.transport
            .open_write_block(attempt, WorkerWriteTarget { group_name, target }, lease_expires_at_ms)
            .await
    }
}

impl fmt::Debug for WorkerClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("WorkerClient").finish_non_exhaustive()
    }
}
