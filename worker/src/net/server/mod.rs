// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker data-plane server entry points.

use std::sync::Arc;

use anyhow::{bail, Context};

use crate::data::core::WorkerCore;
use crate::net::config::WorkerNetConfig;
use crate::net::protocol::WorkerNetProtocol;

pub mod grpc;
pub mod quic;
pub mod rdma;

/// Serve worker data-plane RPCs using the configured worker-owned net layer.
pub async fn serve_worker_data(config: &WorkerNetConfig, core: Arc<WorkerCore>) -> anyhow::Result<()> {
    if config.listeners.is_empty() {
        bail!("worker net listeners must not be empty");
    }
    if config.listeners.len() > 1 {
        bail!("multiple worker net listeners are not implemented in this task");
    }

    let listener = &config.listeners[0];
    match listener.protocol {
        WorkerNetProtocol::Grpc => {
            let bind = listener
                .bind
                .parse()
                .with_context(|| format!("invalid worker gRPC listener bind address: {}", listener.bind))?;
            grpc::serve_grpc_worker_data(bind, listener.max_inflight, core).await
        }
        WorkerNetProtocol::Quic => quic::serve_quic_worker_data(&listener.bind, core).await,
        WorkerNetProtocol::Rdma => rdma::serve_rdma_worker_data(&listener.bind, core).await,
    }
}
