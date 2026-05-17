// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker-internal peer client trait.

use async_trait::async_trait;
use common::header::RequestHeader;
use types::ids::StreamId;

use crate::data::core::{
    AbortWriteRequest, AbortWriteResult, CommitWriteRequest, CommitWriteResult, ReadFrame, ReadOpenRequest,
    ReadOpenResult, WorkerCoreResult, WriteFrame, WriteFrameResult, WriteOpenRequest, WriteOpenResult,
};
use crate::net::endpoint::WorkerNetEndpoint;

#[async_trait]
pub trait WorkerPeerClient: Send + Sync {
    async fn open_read(
        &self,
        endpoint: &WorkerNetEndpoint,
        req: ReadOpenRequest,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<ReadOpenResult>;

    async fn read_stream(
        &self,
        endpoint: &WorkerNetEndpoint,
        stream_id: StreamId,
        max_bytes: u32,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<Vec<ReadFrame>>;

    async fn open_write(
        &self,
        endpoint: &WorkerNetEndpoint,
        req: WriteOpenRequest,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<WriteOpenResult>;

    async fn write_stream(
        &self,
        endpoint: &WorkerNetEndpoint,
        frame: WriteFrame,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<WriteFrameResult>;

    async fn commit_write(
        &self,
        endpoint: &WorkerNetEndpoint,
        req: CommitWriteRequest,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<CommitWriteResult>;

    async fn abort_write(
        &self,
        endpoint: &WorkerNetEndpoint,
        req: AbortWriteRequest,
        ctx: RequestHeader,
    ) -> WorkerCoreResult<AbortWriteResult>;
}
