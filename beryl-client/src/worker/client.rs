// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker operation orchestration above the block-local transport boundary.

use std::fmt;
use std::sync::Arc;

use beryl_types::{GroupName, WriteTarget};
use bytes::Bytes;

use super::transport::GrpcWorkerTransport;
use super::{BlockWrite, WorkerTransport, WorkerWriteTarget};
use crate::config::ClientConfig;
use crate::error::{read_buffer_reservation_failed, ClientError, ClientResult};
use crate::metrics::ClientMetrics;
use crate::planner::PlannedBlockRead;
use crate::runtime::AttemptContext;

/// Owns file-level Worker orchestration while delegating each block-local IO
/// operation to a transport implementation.
#[derive(Clone)]
pub(crate) struct WorkerClient {
    transport: Arc<dyn WorkerTransport>,
}

impl WorkerClient {
    /// Takes ownership of the transport used for all block-local Worker IO.
    pub(crate) fn new(transport: Arc<dyn WorkerTransport>) -> Self {
        Self { transport }
    }

    /// Builds the production Worker client and its gRPC transport.
    pub(crate) fn from_config(config: &ClientConfig, metrics: Arc<dyn ClientMetrics>) -> Self {
        Self::new(Arc::new(GrpcWorkerTransport::from_config(config, metrics)))
    }

    /// Reads all Metadata-planned block-local ranges in file order and rejects
    /// any response that does not exactly cover its authorized range.
    pub(crate) async fn read_block_ranges(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        block_reads: &[PlannedBlockRead],
    ) -> ClientResult<Bytes> {
        let total_len = block_reads.iter().try_fold(0usize, |total, block_read| {
            total
                .checked_add(block_read.len as usize)
                .ok_or_else(|| ClientError::invalid_layout("planned read length overflow".to_string()))
        })?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(total_len)
            .map_err(|error| read_buffer_reservation_failed("read_at", total_len, error))?;
        for block_read in block_reads {
            if block_read.block_stamp == 0 {
                return Err(ClientError::invalid_layout(
                    "planned block read has zero block_stamp".to_string(),
                ));
            }
            let expected_end = block_read
                .file_offset
                .checked_add(u64::from(block_read.len))
                .ok_or_else(|| ClientError::invalid_layout("planned block read end overflow".to_string()))?;
            if expected_end != block_read.end_file_offset {
                return Err(ClientError::invalid_layout(
                    "planned block read coverage is inconsistent".to_string(),
                ));
            }
            let result = self
                .transport
                .read_block_range(attempt.clone(), group_name.clone(), block_read)
                .await?;
            if result.bytes.len() != block_read.len as usize {
                return Err(ClientError::invalid_response(
                    "ReadBlock",
                    format!(
                        "worker read returned {} bytes for {} byte block range",
                        result.bytes.len(),
                        block_read.len
                    ),
                ));
            }
            output.extend_from_slice(&result.bytes);
        }
        Ok(Bytes::from(output))
    }

    /// Opens one Metadata-authorized block RPC and returns only after the
    /// transport has crossed Worker's staging acknowledgement boundary.
    pub(crate) async fn open_write_block(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        target: WriteTarget,
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
