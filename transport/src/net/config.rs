// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Network transport configuration and factory.

use crate::error::{TransportError, TransportResult};
use crate::retry::RetryPolicy;
use std::time::Duration;

#[cfg(feature = "grpc")]
use crate::net::grpc::GrpcTransport;

#[cfg(feature = "quic")]
use crate::net::quic::QuicTransport;

#[cfg(feature = "rdma")]
use crate::net::rdma::RdmaTransport;

/// Network transport kind selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NetTransportKind {
    /// gRPC transport (default, HTTP/2 based)
    #[default]
    Grpc,
    /// QUIC transport
    Quic,
    /// RDMA transport
    Rdma,
}

/// Network transport configuration.
#[derive(Clone, Debug)]
pub struct NetTransportConfig {
    /// Transport kind to use
    pub kind: NetTransportKind,
    /// Connection-level timeout (for establishing connections)
    pub connect_timeout: Duration,
    /// Request-level default timeout (can be overridden by RequestContext)
    pub request_timeout: Duration,
    /// Maximum number of concurrent inflight requests (client-side backpressure)
    pub max_inflight_requests: usize,
    /// Maximum number of concurrent streams (for streaming RPCs)
    pub max_inflight_streams: usize,
    /// Keep-alive interval
    pub keepalive_interval: Option<Duration>,
    /// Keep-alive timeout
    pub keepalive_timeout: Option<Duration>,
    /// Retry policy (default: disabled)
    pub retry_policy: RetryPolicy,
    /// gRPC-specific: maximum message size
    pub grpc_max_msg_size: Option<usize>,
    /// gRPC-specific: initial window size
    pub grpc_initial_window_size: Option<u32>,
    /// QUIC-specific: congestion control algorithm (placeholder for future)
    pub quic_congestion_control: Option<String>,
    /// QUIC-specific: retransmission enabled (placeholder for future)
    pub quic_retransmission_enabled: Option<bool>,
    /// RDMA-specific: congestion control (placeholder for future)
    pub rdma_congestion_control: Option<String>,
    /// RDMA-specific: retransmission enabled (placeholder for future)
    pub rdma_retransmission_enabled: Option<bool>,
}

impl Default for NetTransportConfig {
    fn default() -> Self {
        Self {
            kind: NetTransportKind::Grpc,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_inflight_requests: 100,
            max_inflight_streams: 10,
            keepalive_interval: Some(Duration::from_secs(30)),
            keepalive_timeout: Some(Duration::from_secs(5)),
            retry_policy: RetryPolicy::disabled(),
            grpc_max_msg_size: Some(4 * 1024 * 1024), // 4MB default
            grpc_initial_window_size: None,
            quic_congestion_control: None,
            quic_retransmission_enabled: None,
            rdma_congestion_control: None,
            rdma_retransmission_enabled: None,
        }
    }
}

impl NetTransportConfig {
    pub fn new(kind: NetTransportKind) -> Self {
        Self {
            kind,
            ..Default::default()
        }
    }

    pub fn grpc() -> Self {
        Self::new(NetTransportKind::Grpc)
    }

    pub fn quic() -> Self {
        Self::new(NetTransportKind::Quic)
    }

    pub fn rdma() -> Self {
        Self::new(NetTransportKind::Rdma)
    }

    /// Set connection timeout
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set request timeout
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set max inflight requests
    pub fn with_max_inflight_requests(mut self, max: usize) -> Self {
        self.max_inflight_requests = max;
        self
    }

    /// Set retry policy
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }
}

/// Type-erased network transport wrapper.
///
/// Since NetTransport has associated types, we can't directly create
/// `Arc<dyn NetTransport>`. This enum provides type erasure by wrapping
/// concrete transport types.
pub enum NetTransportBox {
    #[cfg(feature = "grpc")]
    Grpc(GrpcTransport),
    #[cfg(feature = "quic")]
    Quic(QuicTransport),
    #[cfg(feature = "rdma")]
    Rdma(RdmaTransport),
}

/// Build a network transport from configuration.
///
/// Returns a boxed enum that can hold any transport type.
/// Use the convenience functions (build_grpc_transport, etc.) if you need
/// a specific concrete type.
pub fn build_net_transport(cfg: &NetTransportConfig) -> TransportResult<NetTransportBox> {
    match cfg.kind {
        #[cfg(feature = "grpc")]
        NetTransportKind::Grpc => Ok(NetTransportBox::Grpc(GrpcTransport::new(cfg.clone()))),
        #[cfg(not(feature = "grpc"))]
        NetTransportKind::Grpc => Err(TransportError::NotSupported(
            "gRPC transport requires the 'grpc' feature to be enabled".to_string(),
        )),
        #[cfg(feature = "quic")]
        NetTransportKind::Quic => Ok(NetTransportBox::Quic(QuicTransport::new(cfg.clone()))),
        #[cfg(not(feature = "quic"))]
        NetTransportKind::Quic => Err(TransportError::NotSupported(
            "QUIC transport requires the 'quic' feature to be enabled".to_string(),
        )),
        #[cfg(feature = "rdma")]
        NetTransportKind::Rdma => Ok(NetTransportBox::Rdma(RdmaTransport::new(cfg.clone()))),
        #[cfg(not(feature = "rdma"))]
        NetTransportKind::Rdma => Err(TransportError::NotSupported(
            "RDMA transport requires the 'rdma' feature to be enabled".to_string(),
        )),
    }
}

// Actually, let's provide a simpler factory that returns concrete types.
// The caller can handle the type erasure if needed.

/// Build a gRPC transport (convenience function).
#[cfg(feature = "grpc")]
pub fn build_grpc_transport(cfg: Option<NetTransportConfig>) -> GrpcTransport {
    match cfg {
        Some(c) => GrpcTransport::new(c),
        None => GrpcTransport::with_default_config(),
    }
}

/// Build a QUIC transport (convenience function).
#[cfg(feature = "quic")]
pub fn build_quic_transport(cfg: Option<NetTransportConfig>) -> QuicTransport {
    match cfg {
        Some(c) => QuicTransport::new(c),
        None => QuicTransport::with_default_config(),
    }
}

/// Build an RDMA transport (convenience function).
#[cfg(feature = "rdma")]
pub fn build_rdma_transport(cfg: Option<NetTransportConfig>) -> RdmaTransport {
    match cfg {
        Some(c) => RdmaTransport::new(c),
        None => RdmaTransport::with_default_config(),
    }
}
