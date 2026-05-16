// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker main entry point.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use common::observe::{init_observability, ObservabilityConfig, ServiceInfo};
use proto::worker::worker_data_service_server::WorkerDataServiceServer;
use tonic::transport::Server;
use tracing::{error, info};
use worker::{config::WorkerConfig, WorkerCore, WorkerDataServiceImpl};

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "conf/core-site.yaml".to_string());

    let config = WorkerConfig::load(&config_path).context("Failed to load worker configuration")?;

    let obs_config = ObservabilityConfig::default();
    let service_info = ServiceInfo {
        name: "vecton-worker".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        environment: "development".to_string(),
        instance_id: format!("worker-{}", std::process::id()),
        node_name: Some(format!("worker-node-{}", std::process::id())),
    };
    let _obs_guard = init_observability(&obs_config, service_info)
        .map_err(|e| anyhow::anyhow!("Failed to initialize observability: {}", e))?;

    info!(
        rpc_bind = %config.rpc_bind,
        rpc_max_inflight = config.rpc_max_inflight,
        default_frame_size = config.default_frame_size,
        max_frame_size = config.max_frame_size,
        window_bytes = config.window_bytes,
        chunk_size = config.chunk_size,
        storage_root = ?config.storage_root,
        "Starting worker data service skeleton"
    );

    let core = Arc::new(WorkerCore::with_options(
        config.chunk_size,
        config.default_frame_size,
        config.max_frame_size,
        config.window_bytes,
        Duration::from_millis(config.stream_idle_timeout_ms),
        config.storage_root.clone(),
    ));
    let service = WorkerDataServiceImpl::new(core);
    let bind_addr = config.rpc_bind.parse().context("Invalid worker RPC bind address")?;

    if let Err(error) = Server::builder()
        .add_service(WorkerDataServiceServer::new(service))
        .serve(bind_addr)
        .await
        .context("Worker data service server failed")
    {
        error!(%error, "Worker RPC server failed");
        return Err(error);
    }

    Ok(())
}
