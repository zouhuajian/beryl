// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service main entry point.

use common::observe::{init_observability, ObservabilityConfig, ServiceInfo};
use metadata::ensure_root_mount;
use metadata::lease_runtime::LeaseRuntimeTable;
use metadata::maintenance::MaintenanceService;
use metadata::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use metadata::readiness::{wait_for_root_ready_with_metrics, RootReadinessGate};
use metadata::service::{
    cached_static_group_resolver, filesystem_authz_provider, inode_authz_provider, AuthzProviderDeps,
    MetadataFileSystemServiceImpl, MetadataFsServiceImpl, RocksDbInodePermReader,
};
use metadata::state::RaftStateStore;
use metadata::ufs_proxy::UfsMetadataProxy;
use metadata::worker::{
    DeleteExecutor, MetadataWorkerServiceImpl, OrphanQueue, RepairPlanner, RepairQueue, WorkerManager,
};
use metadata::{MetadataConfig, MountTable};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProtoServer;
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProtoServer;
use proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProtoServer;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{info, warn};
use types::ids::ShardGroupId;
use ufs::UfsRegistry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize observability
    let obs_config = ObservabilityConfig::default();
    let service_info = ServiceInfo {
        name: "metadata".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        environment: "development".to_string(),
        instance_id: uuid::Uuid::new_v4().to_string(),
        node_name: None,
    };
    let _guard = init_observability(&obs_config, service_info)?;

    // Load configuration
    let config_path = std::env::var("VECTON_CONFIG").unwrap_or_else(|_| "conf/core-site.yaml".to_string());
    info!(config_path = %config_path, "Loading configuration");

    let config = MetadataConfig::load(&config_path)?;
    let serve_inode_service = config.inode_service.enable;
    if serve_inode_service && config.inode_service.require_loopback_bind && !config.rpc_addr.ip().is_loopback() {
        return Err(format!(
            "metadata.inode_service.enable=true requires loopback metadata.rpc.addr when \
             metadata.inode_service.require_loopback_bind=true; got {}",
            config.rpc_addr
        )
        .into());
    }

    // Print configuration summary (redacted for sensitive values)
    info!(
        rpc_addr = %config.rpc_addr,
        inode_service_enabled = serve_inode_service,
        inode_service_require_loopback_bind = config.inode_service.require_loopback_bind,
        authz_filesystem_mode = ?config.authz.filesystem.mode,
        authz_inode_mode = ?config.authz.inode.mode,
        node_id = config.raft.node_id,
        cluster_id = %config.raft.cluster_id,
        peers_count = config.raft.peers.len(),
        shard_num_shards = config.shard.num_shards,
        shard_group_id = config.shard.shard_group_id,
        "Configuration loaded (sensitive values redacted)"
    );

    // Initialize RocksDB storage
    let db_path = std::env::var("VECTON_METADATA_DB_PATH").unwrap_or_else(|_| "data/metadata".to_string());
    let storage = Arc::new(RocksDBStorage::open(&db_path).map_err(|e| format!("Failed to initialize RocksDB: {}", e))?);

    // Load mount table from RocksDB first (needed for state machine)
    let mount_table = Arc::new(
        MountTable::load_from_storage(&storage)
            .map_err(|e| format!("Failed to load mount table from storage: {}", e))?,
    );

    // Initialize Raft state machine (with mount_table for synchronization)
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));

    // Initialize Raft node
    let raft_node = Arc::new(
        AppRaftNode::new(
            config.raft.node_id,
            Arc::clone(&storage),
            Arc::clone(&state_machine),
            &config.raft,
        )
        .await
        .map_err(|e| format!("Failed to initialize Raft node: {}", e))?,
    );

    // Ensure root mount exists (durable via Raft apply)
    let shard_group_id = ShardGroupId::new(config.shard.shard_group_id);
    ensure_root_mount(Arc::clone(&raft_node), Arc::clone(&mount_table), shard_group_id)
        .await
        .map_err(|e| format!("Failed to ensure root mount: {}", e))?;

    // Create Raft-based state store
    let state_store = Arc::new(RaftStateStore::new(Arc::clone(&raft_node)));

    // Create UFS registry and proxy
    let ufs_registry = Arc::new(UfsRegistry::new());
    let _ufs_metadata_proxy = Arc::new(UfsMetadataProxy::new(
        Arc::clone(&mount_table),
        Arc::clone(&ufs_registry),
    ));

    // Create worker manager
    let worker_manager = Arc::new(WorkerManager::new(60)); // 60s heartbeat timeout

    // Initialize metadata epoch (increment on startup to simulate restart)
    // This ensures all workers will be requested to send FULL block reports
    worker_manager.increment_metadata_epoch();
    info!("Metadata epoch initialized: {}", worker_manager.get_metadata_epoch());

    // Create metadata metrics
    use metadata::metrics::MetadataMetrics;
    let metadata_metrics = Arc::new(MetadataMetrics::new());

    // Create readiness gate and health reporter
    let readiness_gate = Arc::new(RootReadinessGate::new(Some(Arc::clone(&metadata_metrics))));
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_not_serving::<FileSystemServiceProtoServer<MetadataFileSystemServiceImpl>>()
        .await;
    if serve_inode_service {
        health_reporter
            .set_not_serving::<MetadataFsServiceProtoServer<MetadataFsServiceImpl>>()
            .await;
    }

    // Create repair metrics
    use metadata::worker::RepairMetrics;
    let repair_metrics = Arc::new(RepairMetrics::new());

    // Create repair queues with config
    let repair_config = &config.worker.repair;
    let mut repair_queue = RepairQueue::with_config_and_metrics(
        repair_config.max_queue_size,
        repair_config.max_attempts,
        repair_config.inflight_timeout_ms,
        repair_config.initial_backoff_ms,
        repair_config.max_backoff_ms,
        repair_config.worker_inflight_limit,
        Some(Arc::clone(&repair_metrics)),
    );

    // Create inflight registry for cross-operation mutual exclusion
    // This will be shared between RepairQueue and MaintenanceService
    let shared_inflight_registry = Arc::new(metadata::inflight_registry::InflightRegistry::new(5 * 60 * 1000));
    repair_queue.set_inflight_registry(Arc::clone(&shared_inflight_registry));
    let repair_queue = Arc::new(repair_queue);
    let orphan_queue = Arc::new(OrphanQueue::new(10000)); // Max 10000 orphan blocks

    // Create repair scheduler
    let repair_planner = Arc::new(RepairPlanner::new(Arc::clone(&repair_queue), Arc::clone(&orphan_queue)));

    // Create lease runtime table (leader-only, in-memory)
    // Hard TTL: 30 seconds, Soft TTL: 10 seconds, Warmup window: 30 seconds
    let lease_runtime = Arc::new(LeaseRuntimeTable::new(30_000, 10_000, 30_000));

    // Start warmup when leader becomes ready
    // Note: In production, this should be triggered by leader election callback
    if raft_node.is_leader() {
        lease_runtime.start_warmup();
        // Warmup: load active leases from storage
        // This is a simplified version - in production, load from Raft state machine
        // For now, we'll complete warmup immediately (runtime will be populated by RenewLease calls)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        lease_runtime.complete_warmup();
    }

    // Create maintenance service with shared inflight registry
    let maintenance_service = Arc::new(MaintenanceService::new_with_inflight_registry(
        Arc::clone(&raft_node),
        Arc::clone(&storage),
        Arc::clone(&worker_manager),
        Arc::clone(&repair_queue),
        Arc::clone(&orphan_queue),
        Arc::clone(&repair_planner),
        Arc::clone(&metadata_metrics),
        Some(Arc::clone(&shared_inflight_registry)), // Share the same instance
        Arc::clone(&mount_table),                    // Add mount_table for destructive gate
    ));
    // TODO: Set lease_runtime in maintenance_service (requires refactoring LeaseCleanupService)
    maintenance_service.start();

    // Create delete executor
    let delete_executor = Arc::new(DeleteExecutor::new(
        Arc::clone(&raft_node),
        Arc::clone(&storage),
        Arc::clone(&worker_manager),
        Arc::clone(&metadata_metrics),
        Arc::clone(&mount_table), // Add mount_table for destructive gate
    ));
    delete_executor.start(); // start() now takes &Arc<Self>

    // Create worker service
    let mut worker_service = MetadataWorkerServiceImpl::new(
        Arc::clone(&raft_node),
        Arc::clone(&worker_manager),
        Arc::clone(&repair_queue),
        Arc::clone(&orphan_queue),
        Arc::clone(&mount_table), // TODO-2: Add mount_table
    );
    worker_service.set_delete_executor(Arc::clone(&delete_executor));
    worker_service.set_slot_metrics(Arc::clone(&metadata_metrics));
    worker_service.start_background_tasks();

    let authz_deps = AuthzProviderDeps::new(
        cached_static_group_resolver(
            config.authz.groups.static_mappings.clone(),
            config.authz.groups.cache_ttl_secs,
            config.authz.groups.stale_while_error,
        ),
        Arc::new(RocksDbInodePermReader::new(Arc::clone(&storage), 2)),
    );

    // Note: state_store is Arc<RaftStateStore>, which implements StateStore trait
    let mount_table_for_readiness = Arc::clone(&mount_table);
    let fs_service = MetadataFsServiceImpl::new(
        state_store.clone() as Arc<dyn metadata::state::StateStore>,
        Arc::clone(&mount_table),
    )
    .with_storage(Arc::clone(&storage))
    .with_raft_node(Arc::clone(&raft_node))
    .with_metrics(Arc::clone(&metadata_metrics))
    .with_authz_provider(inode_authz_provider(config.authz.inode.mode, &authz_deps))
    .with_inode_perm_reader(Arc::clone(&authz_deps.inode_perm_reader))
    .with_readiness_gate(Arc::clone(&readiness_gate));

    let fs_service_for_filesystem = MetadataFsServiceImpl::new(
        state_store.clone() as Arc<dyn metadata::state::StateStore>,
        Arc::clone(&mount_table),
    )
    .with_storage(Arc::clone(&storage))
    .with_raft_node(Arc::clone(&raft_node))
    .with_metrics(Arc::clone(&metadata_metrics))
    .with_authz_provider(inode_authz_provider(config.authz.inode.mode, &authz_deps))
    .with_inode_perm_reader(Arc::clone(&authz_deps.inode_perm_reader))
    .with_readiness_gate(Arc::clone(&readiness_gate));
    let fs_core_for_filesystem = fs_service_for_filesystem.fs_core();

    let filesystem_service =
        MetadataFileSystemServiceImpl::new(Arc::clone(&mount_table), Arc::clone(&storage), fs_core_for_filesystem)
            .with_metrics(Arc::clone(&metadata_metrics))
            .with_authz_provider(filesystem_authz_provider(config.authz.filesystem.mode, &authz_deps))
            .with_inode_perm_reader(Arc::clone(&authz_deps.inode_perm_reader))
            .with_readiness_gate(Arc::clone(&readiness_gate));

    let readiness_config = config.bootstrap.root_readiness.clone();
    let readiness_gate_clone = Arc::clone(&readiness_gate);
    let mount_table_clone = Arc::clone(&mount_table_for_readiness);
    let raft_node_clone = Arc::clone(&raft_node);
    let shard_group_id_clone = shard_group_id;
    let metrics_clone = Arc::clone(&metadata_metrics);
    let serve_inode_service_for_health = serve_inode_service;
    tokio::spawn(async move {
        let result = wait_for_root_ready_with_metrics(
            raft_node_clone,
            mount_table_clone,
            shard_group_id_clone,
            readiness_gate_clone,
            readiness_config,
            Some(metrics_clone),
        )
        .await;
        match result {
            Ok(()) => {
                health_reporter
                    .set_serving::<FileSystemServiceProtoServer<MetadataFileSystemServiceImpl>>()
                    .await;
                if serve_inode_service_for_health {
                    health_reporter
                        .set_serving::<MetadataFsServiceProtoServer<MetadataFsServiceImpl>>()
                        .await;
                }
            }
            Err(err) => {
                tracing::error!(error = %err, "Root readiness watcher failed");
            }
        }
    });

    // Start gRPC server
    let addr = config.rpc_addr;
    if serve_inode_service {
        warn!(
            addr = %addr,
            "Inode service is enabled on the shared metadata RPC endpoint; keep this endpoint privileged-only"
        );
        info!(addr = %addr, "Listening on");
        Server::builder()
            // DEPRECATED: MetadataClientService removed
            // .add_service(MetadataClientServiceServer::new(client_service))
            .add_service(FileSystemServiceProtoServer::new(filesystem_service))
            .add_service(MetadataWorkerServiceProtoServer::new(worker_service))
            .add_service(MetadataFsServiceProtoServer::new(fs_service))
            .add_service(health_service)
            .serve_with_shutdown(addr, shutdown_signal())
            .await?;
    } else {
        info!(
            addr = %addr,
            "Listening on (inode service disabled by default; external FS entrypoint is FileSystemService only)"
        );
        Server::builder()
            // DEPRECATED: MetadataClientService removed
            // .add_service(MetadataClientServiceServer::new(client_service))
            .add_service(FileSystemServiceProtoServer::new(filesystem_service))
            .add_service(MetadataWorkerServiceProtoServer::new(worker_service))
            .add_service(health_service)
            .serve_with_shutdown(addr, shutdown_signal())
            .await?;
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received");
}
