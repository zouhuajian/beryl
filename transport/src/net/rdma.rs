// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! RDMA transport placeholder implementation.

use crate::connection::{Connection, ConnectionMetadata};
use crate::error::TransportResult;
use async_trait::async_trait;

/// RDMA connection placeholder.
pub struct RdmaConnection {
    metadata: ConnectionMetadata,
}

#[async_trait]
impl Connection for RdmaConnection {
    fn remote_addr(&self) -> &str {
        &self.metadata.remote_addr
    }

    async fn is_healthy(&self) -> bool {
        false
    }

    async fn close(&mut self) -> TransportResult<()> {
        Ok(())
    }
}

/// RDMA transport placeholder implementation.
///
/// This is a placeholder that returns `NotImplemented` errors.
/// A full implementation would use rdma-sys or similar RDMA libraries.
#[cfg(feature = "rdma")]
pub struct RdmaTransport {
    _config: crate::net::config::NetTransportConfig,
}

#[cfg(feature = "rdma")]
impl RdmaTransport {
    pub fn new(config: crate::net::config::NetTransportConfig) -> Self {
        Self { _config: config }
    }

    pub fn with_default_config() -> Self {
        Self::new(crate::net::config::NetTransportConfig::default())
    }
}

#[cfg(feature = "rdma")]
#[async_trait]
impl NetTransport for RdmaTransport {
    type Connection = RdmaConnection;
    type Buffer = ZeroCopyBuffer;

    async fn connect(&self, _addr: &str) -> TransportResult<Self::Connection> {
        Err(crate::error::TransportError::NotImplemented(
            "RDMA transport not yet implemented".to_string(),
        ))
    }

    async fn unary_call<Req, Resp>(
        &self,
        _connection: &Self::Connection,
        _method: &str,
        _request: Req,
        _ctx: RequestContext,
    ) -> TransportResult<Resp>
    where
        Req: Send + Sync,
        Resp: Send + Sync,
    {
        Err(crate::error::TransportError::NotImplemented(
            "RDMA transport not yet implemented".to_string(),
        ))
    }

    async fn server_streaming<Req, Resp>(
        &self,
        _connection: &Self::Connection,
        _method: &str,
        _request: Req,
        _ctx: RequestContext,
    ) -> TransportResult<Box<dyn futures::Stream<Item = TransportResult<Resp>> + Send + Unpin>>
    where
        Req: Send + Sync,
        Resp: Send + Sync,
    {
        Err(crate::error::TransportError::NotImplemented(
            "RDMA transport not yet implemented".to_string(),
        ))
    }

    async fn client_streaming<Req, Resp>(
        &self,
        _connection: &Self::Connection,
        _method: &str,
        _request_stream: Box<dyn futures::Stream<Item = TransportResult<Req>> + Send + Unpin>,
        _ctx: RequestContext,
    ) -> TransportResult<Resp>
    where
        Req: Send + Sync,
        Resp: Send + Sync,
    {
        Err(crate::error::TransportError::NotImplemented(
            "RDMA transport not yet implemented".to_string(),
        ))
    }

    async fn bidi_streaming<Req, Resp>(
        &self,
        _connection: &Self::Connection,
        _method: &str,
        _request_stream: Box<dyn futures::Stream<Item = TransportResult<Req>> + Send + Unpin>,
        _ctx: RequestContext,
    ) -> TransportResult<Box<dyn futures::Stream<Item = TransportResult<Resp>> + Send + Unpin>>
    where
        Req: Send + Sync,
        Resp: Send + Sync,
    {
        Err(crate::error::TransportError::NotImplemented(
            "RDMA transport not yet implemented".to_string(),
        ))
    }
}

#[cfg(feature = "rdma")]
impl crate::transport::NetTransportCapability for RdmaTransport {
    fn zero_copy_payload(&self) -> bool {
        // RDMA can support true zero-copy via DMA (future implementation)
        true
    }

    fn reliability_provided_by(&self) -> crate::transport::ReliabilitySource {
        // RDMA reliability depends on configuration
        // Note: config.rdma_congestion_control and config.rdma_retransmission_enabled
        // are reserved for future configuration
        crate::transport::ReliabilitySource::Transport
    }
}
