// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker data-plane server entry points.

use std::sync::Arc;

use anyhow::{bail, Context};

use crate::control::RegistrationSet;
use crate::data::core::WorkerCore;
use crate::net::config::WorkerNetConfig;
use crate::net::protocol::WorkerNetProtocol;

pub mod grpc;
pub mod quic;
pub mod rdma;

/// Serve worker data-plane RPCs after metadata registration, with an active readiness guard.
pub async fn serve_worker_data_with_registration(
    config: &WorkerNetConfig,
    core: Arc<WorkerCore>,
    registration_state: Arc<RegistrationSet>,
) -> anyhow::Result<()> {
    serve_worker_data_inner(config, core, registration_state).await
}

async fn serve_worker_data_inner(
    config: &WorkerNetConfig,
    core: Arc<WorkerCore>,
    registration_state: Arc<RegistrationSet>,
) -> anyhow::Result<()> {
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
            grpc::serve_grpc_worker_data_with_registration(bind, listener.max_inflight, core, registration_state).await
        }
        WorkerNetProtocol::Quic => bail!("QUIC worker data service requires a registration readiness guard"),
        WorkerNetProtocol::Rdma => bail!("RDMA worker data service requires a registration readiness guard"),
    }
}
