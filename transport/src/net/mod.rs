// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Network transport implementations.

pub mod config;
pub mod grpc;
pub mod methods;
pub mod quic;
pub mod rdma;

pub use config::{build_net_transport, NetTransportBox, NetTransportConfig, NetTransportKind};
pub use grpc::GrpcTransport;

#[cfg(feature = "quic")]
pub use quic::QuicTransport;

#[cfg(feature = "rdma")]
pub use rdma::RdmaTransport;
