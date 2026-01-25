// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Network transport abstraction trait.

use crate::buffer::Buffer;
use crate::connection::Connection;
use crate::error::TransportResult;
use async_trait::async_trait;
use common::header::RequestHeader;

// RequestHeader is available from common::header::RequestHeader

/// Request idempotency classification for retry policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Idempotency {
    /// Request is idempotent and can be safely retried.
    Idempotent,
    /// Request is not idempotent and should not be retried.
    NonIdempotent,
}

/// Network transport trait for different network transport implementations.
///
/// This allows switching between gRPC, QUIC, RDMA, etc.
/// without changing the application code.
///
/// Note: This trait is for network I/O only. For local file/device I/O,
/// see the `LocalIoEngine` trait.
#[async_trait]
pub trait NetTransport: Send + Sync {
    /// The connection type for this transport.
    type Connection: Connection;

    /// The buffer type for zero-copy operations.
    type Buffer: Buffer;

    /// Create a new connection to the given address.
    async fn connect(&self, addr: &str) -> TransportResult<Self::Connection>;

    /// Send a request and receive a response (unary RPC).
    async fn unary_call<Req, Resp>(
        &self,
        connection: &Self::Connection,
        method: &str,
        request: Req,
        ctx: RequestHeader,
    ) -> TransportResult<Resp>
    where
        Req: Send + Sync,
        Resp: Send + Sync;

    /// Send a request and receive a streaming response.
    async fn server_streaming<Req, Resp>(
        &self,
        connection: &Self::Connection,
        method: &str,
        request: Req,
        ctx: RequestHeader,
    ) -> TransportResult<Box<dyn futures::Stream<Item = TransportResult<Resp>> + Send + Unpin>>
    where
        Req: Send + Sync,
        Resp: Send + Sync;

    /// Send a streaming request and receive a response.
    async fn client_streaming<Req, Resp>(
        &self,
        connection: &Self::Connection,
        method: &str,
        request_stream: Box<dyn futures::Stream<Item = TransportResult<Req>> + Send + Unpin>,
        ctx: RequestHeader,
    ) -> TransportResult<Resp>
    where
        Req: Send + Sync,
        Resp: Send + Sync;

    /// Bidirectional streaming.
    async fn bidi_streaming<Req, Resp>(
        &self,
        connection: &Self::Connection,
        method: &str,
        request_stream: Box<dyn futures::Stream<Item = TransportResult<Req>> + Send + Unpin>,
        ctx: RequestHeader,
    ) -> TransportResult<Box<dyn futures::Stream<Item = TransportResult<Resp>> + Send + Unpin>>
    where
        Req: Send + Sync,
        Resp: Send + Sync;
}

/// Transport capability declaration (zero-copy, reliability, etc.)
pub trait NetTransportCapability: Send + Sync {
    /// Whether this transport supports zero-copy payload operations.
    /// Returns true if payloads can be passed without copying,
    /// false if copying may occur (e.g., TCP/HTTP2 may copy).
    fn zero_copy_payload(&self) -> bool;

    /// Whether this transport relies on underlying protocol reliability
    /// (e.g., TCP provides reliability, QUIC provides its own).
    fn reliability_provided_by(&self) -> ReliabilitySource;
}

/// Source of transport reliability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReliabilitySource {
    /// Reliability provided by underlying protocol (e.g., TCP)
    UnderlyingProtocol,
    /// Reliability provided by transport itself (e.g., QUIC)
    Transport,
    /// No reliability guarantee
    None,
}
