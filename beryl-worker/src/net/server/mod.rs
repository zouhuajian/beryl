// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker data-plane server entry points.

use std::sync::Arc;

use anyhow::{bail, Context};
use beryl_common::grpc_server::{spawn_grpc_server, GrpcServerHandle};

use crate::control::RegistrationSet;
use crate::data::core::WorkerCore;
use crate::net::config::WorkerNetConfig;

pub mod grpc;

/// Binds the Worker data plane under the process-owned connection tracker.
pub fn spawn_worker_data_with_registration(
    config: &WorkerNetConfig,
    core: Arc<WorkerCore>,
    registration_state: Arc<RegistrationSet>,
) -> anyhow::Result<GrpcServerHandle> {
    let (bind, max_inflight) = grpc_listener(config)?;
    let routes = grpc::worker_data_routes(core, registration_state);
    spawn_grpc_server(bind, routes, Some(max_inflight)).context("failed to bind Worker gRPC listener")
}

fn grpc_listener(config: &WorkerNetConfig) -> anyhow::Result<(std::net::SocketAddr, usize)> {
    if config.listeners.is_empty() {
        bail!("worker net listeners must not be empty");
    }
    if config.listeners.len() > 1 {
        bail!("multiple worker net listeners are not implemented in this task");
    }

    let listener = &config.listeners[0];
    let bind = listener
        .bind
        .parse()
        .with_context(|| format!("invalid worker gRPC listener bind address: {}", listener.bind))?;
    Ok((bind, listener.max_inflight))
}
