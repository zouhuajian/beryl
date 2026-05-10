// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker main entry point.

use anyhow::{Context, Result};
use common::observe::{init_observability, ObservabilityConfig, ServiceInfo};
use std::sync::Arc;
use tracing::{error, info};
use transport::{GrpcTransport, NetTransportConfig};
use types::ids::WorkerId;
use types::layout::FileLayout;
use worker::{
    block_manager::BlockManager,
    block_store::BlockStore,
    config::WorkerConfig,
    lifecycle::{Lifecycle, WorkerState},
    replication::GrpcReplicationClient,
    rpc_server::RpcServer,
    service::WorkerDataServiceImpl,
    volume_manager::VolumeManager,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "conf/core-site.yaml".to_string());

    info!(config_path = %config_path, "Loading configuration");

    // Load configuration
    let config = WorkerConfig::load(&config_path).context("Failed to load worker configuration")?;

    // Print configuration summary (redacted for sensitive values)
    info!(
        rpc_bind = %config.rpc_bind,
        storage_dirs_count = config.storage_dirs.len(),
        block_size = config.block_size,
        chunk_size = config.chunk_size,
        transport_kind = %config.transport.kind,
        storage_kind = %config.storage.kind,
        max_read_ops = config.max_read_ops,
        max_write_ops = config.max_write_ops,
        metadata_groups_count = config.metadata.groups.len(),
        replication_peers_count = config.replication.peer_endpoints.len(),
        "Configuration loaded (sensitive values redacted)"
    );

    // Initialize observability (logging, metrics, tracing)
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

    info!("Starting Vecton Worker");

    // Initialize lifecycle
    let lifecycle = Arc::new(Lifecycle::new());
    lifecycle
        .transition(WorkerState::Bootstrapping)
        .map_err(|e| anyhow::anyhow!("Failed to transition to Bootstrapping: {}", e))?;

    // Open volumes
    let volume_manager = Arc::new(VolumeManager::new());
    volume_manager
        .open_volumes(&config.storage_dirs)
        .map_err(|e| anyhow::anyhow!("Failed to open volumes: {}", e))?;
    lifecycle
        .transition(WorkerState::VolumesReady)
        .map_err(|e| anyhow::anyhow!("Failed to transition to VolumesReady: {}", e))?;

    // Initialize BlockStore
    let manifest_path = config.storage_dirs[0].join("manifest.json");
    let layout = FileLayout::new(config.block_size, config.chunk_size, 1);
    let block_store = Arc::new(BlockStore::new(
        Arc::clone(&volume_manager),
        manifest_path,
        config.block_size,
        config.chunk_size,
    ));
    block_store
        .init()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize BlockStore: {}", e))?;

    // Initialize BlockManager
    let block_manager = Arc::new(BlockManager::new(Arc::clone(&block_store), layout.clone()));

    // Validate and build transport × storage combo
    use worker::combo_validator::build_and_validate_combo;
    let (_transport_box, _storage, effective_transport) = build_and_validate_combo(
        &config.transport.kind,
        &config.storage.kind,
        config.transport.zero_copy_required,
        config.transport.combo_allow_fallback,
        config.transport.fallback_transport.as_deref(),
    )
    .map_err(|e| anyhow::anyhow!("Failed to validate transport×storage combo: {}", e))?;

    // Build NetTransportConfig from worker transport config
    use std::time::Duration;
    use transport::net::NetTransportKind;
    let net_kind = match config.transport.kind.as_str() {
        "grpc" => NetTransportKind::Grpc,
        "quic" => NetTransportKind::Quic,
        "rdma" => NetTransportKind::Rdma,
        _ => NetTransportKind::Grpc, // Default fallback
    };
    let transport_config = NetTransportConfig::new(net_kind)
        .with_connect_timeout(Duration::from_millis(config.transport.connect_timeout_ms))
        .with_request_timeout(Duration::from_millis(config.transport.request_timeout_ms))
        .with_max_inflight_requests(config.transport.max_inflight_requests);
    let transport = Arc::new(GrpcTransport::new(transport_config));

    // Initialize replication client if replication is configured
    let replication_client: Option<Arc<dyn worker::block_manager::ReplicationClient + Send + Sync>> =
        if !config.replication.peer_endpoints.is_empty() {
            info!(
                peer_count = config.replication.peer_endpoints.len(),
                pool_size = config.replication.peer_connection_pool_size,
                max_concurrent_blocks = config.replication.max_concurrent_blocks,
                effective_transport = %effective_transport,
                "Initializing replication client"
            );

            let replication_client = Arc::new(GrpcReplicationClient::new(transport, config.replication.clone()));

            Some(replication_client as Arc<dyn worker::block_manager::ReplicationClient + Send + Sync>)
        } else {
            info!("No replication peers configured, replication disabled");
            None
        };

    lifecycle
        .transition(WorkerState::RpcServing)
        .map_err(|e| anyhow::anyhow!("Failed to transition to RpcServing: {}", e))?;

    // Worker identity is used by metadata registration.
    let worker_id = WorkerId::new(1); // TODO(worker): obtain worker_id from metadata instead of hardcoding

    // Create CommandExecutor with replication if available
    use worker::command_executor::CommandExecutor;
    let command_executor = if let Some(ref replication_client) = replication_client {
        Arc::new(CommandExecutor::with_replication(
            Arc::clone(&block_store),
            Arc::clone(replication_client),
        ))
    } else {
        Arc::new(CommandExecutor::new(Arc::clone(&block_store)))
    };

    // Generate worker_epoch (use startup timestamp in seconds)
    use std::time::{SystemTime, UNIX_EPOCH};
    let worker_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    // Convert transport kind to proto enum value (0=unspecified, 1=grpc, 2=quic, 3=rdma)
    let net_transport_kind = match net_kind {
        NetTransportKind::Grpc => 1,
        NetTransportKind::Quic => 2,
        NetTransportKind::Rdma => 3,
    };

    // Create metadata client if metadata groups are configured
    if !config.metadata.groups.is_empty() {
        use worker::metadata_client::MetadataClient;
        let metadata_client = Arc::new(MetadataClient::new(
            worker_id,
            config.rpc_bind.clone(),
            net_transport_kind,
            worker_epoch,
            Arc::clone(&block_store),
            Arc::clone(&command_executor),
            Duration::from_secs(config.metadata.heartbeat_interval_sec),
            Duration::from_secs(config.metadata.block_report_interval_sec),
            Duration::from_secs(config.metadata.backoff_duration_sec),
        ));

        // Add metadata groups
        for group in &config.metadata.groups {
            metadata_client
                .add_group(types::ids::ShardGroupId::new(group.group_id), group.endpoint.clone())
                .await;
        }

        // Register with all groups
        if let Err(e) = metadata_client.register_all().await {
            error!(error = %e, "Failed to register with metadata groups");
            return Err(anyhow::anyhow!("Metadata registration failed: {}", e));
        }

        // Start heartbeat and block report loops
        let heartbeat_client = Arc::clone(&metadata_client);
        tokio::spawn(async move {
            heartbeat_client.start_heartbeat_loop().await;
        });

        let block_report_client = Arc::clone(&metadata_client);
        tokio::spawn(async move {
            block_report_client.start_block_report_loop().await;
        });

        info!("Metadata client started");
    }

    // Initialize RebalanceManager if replication is enabled (before creating service)
    if let Some(ref replication_client) = replication_client {
        use worker::rebalance::RebalanceManager;
        let rebalance_mgr = RebalanceManager::with_volume_manager(
            Arc::clone(&block_manager),
            Arc::clone(replication_client),
            Arc::clone(&volume_manager),
        );
        let rebalance_mgr_arc = Arc::new(rebalance_mgr);

        // Start rebalance loop in background
        let rebalance_loop = Arc::clone(&rebalance_mgr_arc);
        tokio::spawn(async move {
            if let Err(e) = rebalance_loop.start_rebalance_loop().await {
                error!(error = %e, "Rebalance loop error");
            }
        });

        info!("RebalanceManager started");
    }

    let service = Arc::new(WorkerDataServiceImpl::new(layout));

    // Start RPC server
    let rpc_server = RpcServer::new(config.rpc_bind.clone(), Arc::clone(&service));

    lifecycle
        .transition(WorkerState::Serving)
        .map_err(|e| anyhow::anyhow!("Failed to transition to Serving: {}", e))?;

    info!(
        rpc_bind = %config.rpc_bind,
        "Worker is serving requests"
    );

    // Run RPC server (blocks until shutdown)
    if let Err(e) = rpc_server.start().await {
        error!(error = %e, "RPC server error");
        lifecycle.transition(WorkerState::Draining).ok();
        return Err(anyhow::anyhow!("RPC server failed: {}", e));
    }

    // Graceful shutdown
    lifecycle.transition(WorkerState::Draining).ok();
    lifecycle.transition(WorkerState::Stopped).ok();

    info!("Worker stopped");
    Ok(())
}
