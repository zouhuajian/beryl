// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Internal worker data-plane boundary.
//!
//! This module stays private to the crate, so stream handles and block-local
//! worker operations do not appear in the public API.

mod worker;

use async_trait::async_trait;
use bytes::Bytes;
use proto::worker::WriteStreamResponseProto;
use types::{GroupName, WorkerEndpointInfo, WriteTarget};

use crate::error::ClientResult;
use crate::planner::read_planner::PlannedReadSegment;
use crate::runtime::AttemptContext;

/// Internal worker data client boundary.
/// Stream identifiers and endpoint details stay inside the implementation.
#[async_trait]
pub(crate) trait WorkerDataClient: Send + Sync {
    async fn read_segment(
        &self,
        ctx: AttemptContext,
        group_name: GroupName,
        segment: &PlannedReadSegment,
    ) -> ClientResult<WorkerReadResult>;

    async fn open_write(&self, ctx: AttemptContext, target: WorkerWriteTarget) -> ClientResult<WorkerWriteBlock>;

    async fn write_stream(&self, block: &WorkerWriteBlock, data: Bytes) -> ClientResult<WriteStreamResponseProto>;

    async fn commit_write(
        &self,
        ctx: AttemptContext,
        block: &WorkerWriteBlock,
        effective_len: u64,
        commit_seq: u64,
        require_sync: bool,
    ) -> ClientResult<WorkerCommitResult>;

    async fn sync_committed_block(
        &self,
        ctx: AttemptContext,
        block: &WorkerWriteBlock,
        expected_len: u64,
    ) -> ClientResult<WorkerBlockSyncResult>;

    async fn abort_write(&self, ctx: AttemptContext, block: &WorkerWriteBlock) -> ClientResult<()>;
}

/// Internal worker write target derived from metadata AddBlock.
#[derive(Clone, Debug)]
pub(crate) struct WorkerWriteTarget {
    /// Metadata owner group for the target block.
    pub(crate) group_name: GroupName,
    /// Metadata AddBlock target.
    pub(crate) target: WriteTarget,
}

/// Worker OpenReadStream evidence plus the bytes read from that stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkerReadResult {
    /// Bytes returned for the planned segment.
    pub(crate) bytes: Bytes,
    /// Worker-observed block stamp from OpenReadStream.
    pub(crate) block_stamp: u64,
    /// Worker-observed readable committed prefix from OpenReadStream.
    pub(crate) committed_length: u64,
}

/// Internal worker write stream state.
#[derive(Clone, Debug)]
pub(crate) struct WorkerWriteBlock {
    /// Metadata owner group for the block.
    pub(crate) group_name: GroupName,
    /// Stable worker identity selected by metadata.
    pub(crate) worker: WorkerEndpointInfo,
    /// Metadata AddBlock target.
    pub(crate) target: WriteTarget,
    /// Worker stream identifier.
    pub(crate) stream_id: proto::common::StreamIdProto,
    /// Worker-accepted frame size.
    pub(crate) frame_size: u32,
    /// Next frame sequence number.
    pub(crate) next_seq: u64,
}

/// Worker CommitWrite result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerCommitResult {
    /// Effective block length published by the worker.
    pub(crate) effective_len: u64,
    /// Metadata-assigned block stamp persisted by the worker.
    pub(crate) block_stamp: u64,
    /// Contiguous byte prefix written into the staging block.
    pub(crate) written_through: u64,
}

/// Worker block-level durable sync result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerBlockSyncResult {
    /// Effective block length validated by the worker.
    pub(crate) effective_len: u64,
    /// Metadata-assigned block stamp persisted by the worker.
    pub(crate) block_stamp: u64,
}

pub(crate) use worker::WorkerDataPlane;
