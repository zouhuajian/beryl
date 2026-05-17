// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker main entry point.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use common::observe::{init_observability, ObservabilityConfig, ServiceInfo};
use tracing::{error, info};
use worker::{config::WorkerConfig, net, WorkerCore};

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
        net_listeners = config.net.listeners.len(),
        "Starting worker data service skeleton"
    );
    for listener in &config.net.listeners {
        info!(
            protocol = %listener.protocol,
            bind = %listener.bind,
            max_inflight = listener.max_inflight,
            max_frame_size = listener.max_frame_size,
            roles = ?listener.role,
            "Configured worker net listener"
        );
    }

    let core = Arc::new(WorkerCore::with_options(
        config.chunk_size,
        config.default_frame_size,
        config.max_frame_size,
        config.window_bytes,
        Duration::from_millis(config.stream_idle_timeout_ms),
        config.storage_root.clone(),
    ));

    if let Err(error) = net::server::serve_worker_data(&config.net, core)
        .await
        .context("Worker data service server failed")
    {
        error!(%error, "Worker RPC server failed");
        return Err(error);
    }

    Ok(())
}
