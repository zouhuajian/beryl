// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Transport abstraction layer for Vecton.
//!
//! This crate provides abstractions for:
//! - Network transport (NetTransport): gRPC, QUIC, RDMA
//! - Local I/O engine (LocalIoEngine): File system, io_uring, SPDK

pub mod buffer;
pub mod connection;
pub mod convert;
pub mod ctx_adapter;
pub mod error;
pub mod pool;
pub mod retry;
pub mod transport;

// Network transport implementations
pub mod net;

// Local I/O engine implementations
pub mod local_io;

// Core exports
pub use buffer::{Buffer, ZeroCopyBuffer};
pub use connection::{Connection, ConnectionConfig, ConnectionMetadata};
pub use error::{IoError, IoResult, TransportError, TransportResult};
pub use pool::{ConnectionPool, PoolStats};
pub use retry::RetryPolicy;
pub use transport::{Idempotency, NetTransport, NetTransportCapability, ReliabilitySource};

// Network transport exports
pub use net::grpc::{GrpcConnection, GrpcTransport};
pub use net::{build_net_transport, NetTransportBox, NetTransportConfig, NetTransportKind};

#[cfg(feature = "quic")]
pub use net::quic::QuicTransport;

#[cfg(feature = "rdma")]
pub use net::rdma::RdmaTransport;

// Local I/O engine exports
pub use local_io::{build_local_io, FsIoEngine, LocalIoConfig, LocalIoEngine, LocalIoKind};

#[cfg(all(feature = "io_uring", target_os = "linux"))]
pub use local_io::IoUringIoEngine;

#[cfg(feature = "spdk")]
pub use local_io::SpdkIoEngine;
