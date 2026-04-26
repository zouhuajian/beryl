// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Startup staging helpers for the metadata binary.

use common::observe::{init_observability, ObservabilityConfig, ObservabilityGuard, ServiceInfo};
use metadata::ensure_root_mount;
use metadata::inflight_registry::InflightRegistry;
use metadata::lease_runtime::LeaseRuntimeTable;
use metadata::maintenance::MaintenanceService;
use metadata::metrics::MetadataMetrics;
use metadata::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use metadata::readiness::{wait_for_root_ready_with_metrics, RootReadinessGate};
use metadata::service::{
    cached_static_group_resolver, filesystem_authz_provider, AuthzProviderDeps, MetadataFileSystemServiceDeps,
    MetadataFileSystemServiceImpl, RocksDbInodePermReader, SharedWorkerCommitHook,
};
use metadata::state::RaftStateStore;
use metadata::ufs_proxy::UfsMetadataProxy;
use metadata::worker::{
    DeleteExecutor, MetadataWorkerServiceImpl, OrphanQueue, RepairPlanner, RepairQueue, WorkerManager,
};
use metadata::{MetadataConfig, MountTable};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProtoServer;
use proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProtoServer;
use std::sync::{Arc, Mutex};
use tokio::signal;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic_health::pb::health_server::HealthServer;
use tonic_health::server::{HealthReporter, HealthService};
use tracing::info;
use types::ids::ShardGroupId;
use ufs::UfsRegistry;

pub type DynError = Box<dyn std::error::Error>;

type MetadataHealthServer = HealthServer<HealthService>;

/// Process-wide bootstrap state that must stay alive for the metadata binary lifetime.
pub struct CoreStage {
    pub config: Arc<MetadataConfig>,
    _observability_guard: ObservabilityGuard,
}

/// Authoritative metadata dependencies built before any public service is exposed.
pub struct AuthorityStage {
    pub storage: Arc<RocksDBStorage>,
    pub mount_table: Arc<MountTable>,
    pub raft_node: Arc<AppRaftNode>,
    pub state_store: Arc<dyn metadata::state::StateStore>,
    pub worker_manager: Arc<WorkerManager>,
    pub metadata_metrics: Arc<MetadataMetrics>,
    pub shard_group_id: ShardGroupId,
    _ufs_registry: Arc<UfsRegistry>,
    _ufs_metadata_proxy: Arc<UfsMetadataProxy>,
}

/// Worker-facing RPC surface plus the worker-owned handles shared with background startup.
pub struct WorkerRuntime {
    pub service: MetadataWorkerServiceImpl,
    owned: WorkerOwnedRuntime,
}

/// Runtime objects owned by the worker side, even when background services need cloned handles.
struct WorkerOwnedRuntime {
    repair_queue: Arc<RepairQueue>,
    orphan_queue: Arc<OrphanQueue>,
    repair_planner: Arc<RepairPlanner>,
    shared_inflight_registry: Arc<InflightRegistry>,
}

/// Filesystem-facing RPC surface and readiness state exposed through tonic health.
pub struct FileSystemRuntime {
    pub service: MetadataFileSystemServiceImpl,
    pub health_service: MetadataHealthServer,
    _readiness: ReadinessRuntime,
}

/// Keeps the readiness gate and watcher task alive for as long as the filesystem runtime lives.
struct ReadinessRuntime {
    _gate: Arc<RootReadinessGate>,
    _watcher: JoinHandle<()>,
}

/// Started background side effects that must stay alive for the server lifetime.
pub struct BackgroundRuntime {
    _shared_inflight_registry: Arc<InflightRegistry>,
    _lease_runtime: Arc<LeaseRuntimeTable>,
    _maintenance_service: Arc<MaintenanceService>,
    _delete_executor: Arc<DeleteExecutor>,
}

/// Cloned worker-owned handles consumed by background services without transferring ownership.
struct WorkerBackgroundInputs {
    repair_queue: Arc<RepairQueue>,
    orphan_queue: Arc<OrphanQueue>,
    repair_planner: Arc<RepairPlanner>,
    shared_inflight_registry: Arc<InflightRegistry>,
}

impl WorkerOwnedRuntime {
    /// Builds worker-owned queues and shared registries before worker service construction.
    fn new(config: &MetadataConfig) -> Self {
        let repair_metrics = Arc::new(metadata::worker::RepairMetrics::new());
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
        let shared_inflight_registry = Arc::new(InflightRegistry::new(5 * 60 * 1000));
        repair_queue.set_inflight_registry(Arc::clone(&shared_inflight_registry));
        let repair_queue = Arc::new(repair_queue);
        let orphan_queue = Arc::new(OrphanQueue::new(10_000));
        let repair_planner = Arc::new(RepairPlanner::new(Arc::clone(&repair_queue), Arc::clone(&orphan_queue)));

        Self {
            repair_queue,
            orphan_queue,
            repair_planner,
            shared_inflight_registry,
        }
    }

    /// Produces the background wiring inputs while keeping ownership anchored in `WorkerRuntime`.
    fn background_inputs(&self) -> WorkerBackgroundInputs {
        WorkerBackgroundInputs {
            repair_queue: Arc::clone(&self.repair_queue),
            orphan_queue: Arc::clone(&self.orphan_queue),
            repair_planner: Arc::clone(&self.repair_planner),
            shared_inflight_registry: Arc::clone(&self.shared_inflight_registry),
        }
    }
}

impl WorkerRuntime {
    /// Exposes only the worker-owned handles background services need to be started.
    fn background_inputs(&self) -> WorkerBackgroundInputs {
        self.owned.background_inputs()
    }

    /// Performs the single handoff from started background runtime back into the worker service.
    fn connect_background_runtime(&mut self, delete_executor: Arc<DeleteExecutor>) {
        self.service.set_delete_executor(delete_executor);
        self.service.start_background_tasks();
    }
}

/// Loads process configuration and initializes observability before metadata authority is built.
pub fn bootstrap_core() -> Result<CoreStage, DynError> {
    let obs_config = ObservabilityConfig::default();
    let service_info = ServiceInfo {
        name: "metadata".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        environment: "development".to_string(),
        instance_id: uuid::Uuid::new_v4().to_string(),
        node_name: None,
    };
    let observability_guard = init_observability(&obs_config, service_info)?;

    let config_path = std::env::var("VECTON_CONFIG").unwrap_or_else(|_| "conf/core-site.yaml".to_string());
    info!(config_path = %config_path, "Loading configuration");

    let config = Arc::new(MetadataConfig::load(&config_path)?);

    info!(
        rpc_addr = %config.rpc_addr,
        authz_filesystem_mode = ?config.authz.filesystem.mode,
        node_id = config.raft.node_id,
        cluster_id = %config.raft.cluster_id,
        peers_count = config.raft.peers.len(),
        shard_num_shards = config.shard.num_shards,
        shard_group_id = config.shard.shard_group_id,
        "Configuration loaded (sensitive values redacted)"
    );

    Ok(CoreStage {
        config,
        _observability_guard: observability_guard,
    })
}

/// Builds authoritative storage, mount, raft, and state-store dependencies in startup order.
pub async fn bootstrap_authority(config: &MetadataConfig) -> Result<AuthorityStage, DynError> {
    // TODO: use path from config
    let db_path = std::env::var("VECTON_METADATA_DB_PATH").unwrap_or_else(|_| "data/metadata".to_string());
    let storage = Arc::new(RocksDBStorage::open(&db_path).map_err(|e| format!("Failed to initialize RocksDB: {e}"))?);

    let mount_table = Arc::new(
        MountTable::load_from_storage(storage.as_ref())
            .map_err(|e| format!("Failed to load mount table from storage: {e}"))?,
    );

    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));

    let raft_node = Arc::new(
        AppRaftNode::new(
            config.raft.node_id,
            Arc::clone(&storage),
            Arc::clone(&state_machine),
            &config.raft,
        )
        .await
        .map_err(|e| format!("Failed to initialize Raft node: {e}"))?,
    );

    let shard_group_id = ShardGroupId::new(config.shard.shard_group_id);
    ensure_root_mount(Arc::clone(&raft_node), Arc::clone(&mount_table), shard_group_id)
        .await
        .map_err(|e| format!("Failed to ensure root mount: {e}"))?;

    let state_store: Arc<dyn metadata::state::StateStore> = Arc::new(RaftStateStore::new(Arc::clone(&raft_node)));
    let ufs_registry = Arc::new(UfsRegistry::new());
    let ufs_metadata_proxy = Arc::new(UfsMetadataProxy::new(
        Arc::clone(&mount_table),
        Arc::clone(&ufs_registry),
    ));
    let worker_manager = Arc::new(WorkerManager::new(60));
    worker_manager.increment_metadata_epoch();
    info!("Metadata epoch initialized: {}", worker_manager.get_metadata_epoch());

    Ok(AuthorityStage {
        storage,
        mount_table,
        raft_node,
        state_store,
        worker_manager,
        metadata_metrics: Arc::new(MetadataMetrics::new()),
        shard_group_id,
        _ufs_registry: ufs_registry,
        _ufs_metadata_proxy: ufs_metadata_proxy,
    })
}

/// Constructs the worker RPC service without starting background side effects.
pub fn build_worker_runtime(config: &MetadataConfig, authority: &AuthorityStage) -> WorkerRuntime {
    let owned = WorkerOwnedRuntime::new(config);

    let mut worker_service = MetadataWorkerServiceImpl::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(&authority.worker_manager),
        Arc::clone(&owned.repair_queue),
        Arc::clone(&owned.orphan_queue),
        Arc::clone(&authority.mount_table),
    );
    worker_service.set_slot_metrics(Arc::clone(&authority.metadata_metrics));

    WorkerRuntime {
        service: worker_service,
        owned,
    }
}

/// Starts background maintenance side effects after authority and worker runtime exist.
pub async fn build_background_runtime(authority: &AuthorityStage, worker: &mut WorkerRuntime) -> BackgroundRuntime {
    let worker_inputs = worker.background_inputs();
    let lease_runtime = start_lease_runtime(authority).await;

    let maintenance_service = Arc::new(MaintenanceService::new_with_inflight_registry(
        Arc::clone(&authority.raft_node),
        Arc::clone(&authority.storage),
        Arc::clone(&authority.worker_manager),
        Arc::clone(&worker_inputs.repair_queue),
        Arc::clone(&worker_inputs.orphan_queue),
        Arc::clone(&worker_inputs.repair_planner),
        Arc::clone(&authority.metadata_metrics),
        Some(Arc::clone(&worker_inputs.shared_inflight_registry)),
        Arc::clone(&authority.mount_table),
    ));
    maintenance_service.start();

    let delete_executor = Arc::new(DeleteExecutor::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(&authority.storage),
        Arc::clone(&authority.worker_manager),
        Arc::clone(&authority.metadata_metrics),
        Arc::clone(&authority.mount_table),
    ));
    delete_executor.start();

    worker.connect_background_runtime(Arc::clone(&delete_executor));

    BackgroundRuntime {
        _shared_inflight_registry: worker_inputs.shared_inflight_registry,
        _lease_runtime: lease_runtime,
        _maintenance_service: maintenance_service,
        _delete_executor: delete_executor,
    }
}

/// Preserves lease warmup behavior behind a named startup step.
async fn start_lease_runtime(authority: &AuthorityStage) -> Arc<LeaseRuntimeTable> {
    let lease_runtime = Arc::new(LeaseRuntimeTable::new(30_000, 10_000, 30_000));
    if authority.raft_node.is_leader() {
        lease_runtime.start_warmup();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        lease_runtime.complete_warmup();
    }
    lease_runtime
}

/// Constructs the filesystem RPC service and its readiness/health reporting runtime.
pub async fn build_filesystem_runtime(
    config: &MetadataConfig,
    authority: &AuthorityStage,
) -> Result<FileSystemRuntime, DynError> {
    let readiness_gate = Arc::new(RootReadinessGate::new(Some(Arc::clone(&authority.metadata_metrics))));
    let health_reporter = HealthReporter::new();
    health_reporter
        .set_not_serving::<FileSystemServiceProtoServer<MetadataFileSystemServiceImpl>>()
        .await;
    let health_service = HealthServer::new(HealthService::from_health_reporter(health_reporter.clone()));

    let authz_deps = AuthzProviderDeps::new(
        cached_static_group_resolver(
            config.authz.groups.static_mappings.clone(),
            config.authz.groups.cache_ttl_secs,
            config.authz.groups.stale_while_error,
        ),
        Arc::new(RocksDbInodePermReader::new(Arc::clone(&authority.storage), 2)),
    );

    let write_session_manager = Arc::new(metadata::write_session::WriteSessionManager::default());
    let inode_lease_manager = Arc::new(metadata::inode_lease::InodeLeaseManager::default());
    let worker_commit_hook: SharedWorkerCommitHook = Arc::new(Mutex::new(None));
    let filesystem_service = MetadataFileSystemServiceImpl::new(MetadataFileSystemServiceDeps {
        state_store: Arc::clone(&authority.state_store),
        mount_table: Arc::clone(&authority.mount_table),
        storage: Arc::clone(&authority.storage),
        write_session_manager,
        inode_lease_manager,
        worker_commit_hook,
        raft_node: Some(Arc::clone(&authority.raft_node)),
        worker_manager: Some(Arc::clone(&authority.worker_manager)),
        metrics: Some(Arc::clone(&authority.metadata_metrics)),
        readiness_gate: Some(Arc::clone(&readiness_gate)),
        leadership_checker: None,
        authz_provider: filesystem_authz_provider(config.authz.filesystem.mode, &authz_deps),
        inode_perm_reader: Some(Arc::clone(&authz_deps.inode_perm_reader)),
    });

    let readiness_config = config.bootstrap.root_readiness.clone();
    let readiness_gate_clone = Arc::clone(&readiness_gate);
    let mount_table_clone = Arc::clone(&authority.mount_table);
    let raft_node_clone = Arc::clone(&authority.raft_node);
    let shard_group_id = authority.shard_group_id;
    let metrics = Arc::clone(&authority.metadata_metrics);
    let readiness_watcher = tokio::spawn(async move {
        let result = wait_for_root_ready_with_metrics(
            raft_node_clone,
            mount_table_clone,
            shard_group_id,
            readiness_gate_clone,
            readiness_config,
            Some(metrics),
        )
        .await;
        match result {
            Ok(()) => {
                health_reporter
                    .set_serving::<FileSystemServiceProtoServer<MetadataFileSystemServiceImpl>>()
                    .await;
            }
            Err(err) => {
                tracing::error!(error = %err, "Root readiness watcher failed");
            }
        }
    });

    Ok(FileSystemRuntime {
        service: filesystem_service,
        health_service,
        _readiness: ReadinessRuntime {
            _gate: readiness_gate,
            _watcher: readiness_watcher,
        },
    })
}

/// Exposes the filesystem, worker, and health services using the staged runtime objects.
pub async fn serve(
    config: &MetadataConfig,
    filesystem: FileSystemRuntime,
    worker: WorkerRuntime,
    _background: BackgroundRuntime,
) -> Result<(), DynError> {
    let FileSystemRuntime {
        service: filesystem_service,
        health_service,
        _readiness,
    } = filesystem;
    let addr = config.rpc_addr;
    info!(addr = %addr, "Listening on (path/filesystem + worker services)");
    Server::builder()
        .add_service(FileSystemServiceProtoServer::new(filesystem_service))
        .add_service(MetadataWorkerServiceProtoServer::new(worker.service))
        .add_service(health_service)
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use metadata::config::{BootstrapConfig, MetadataAuthzConfig, RaftConfig, ShardConfig, WorkerConfig};
    use tempfile::TempDir;

    async fn test_authority_stage(dir: &TempDir) -> AuthorityStage {
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
        let raft_config = RaftConfig {
            node_id: 1,
            cluster_id: "test".to_string(),
            peers: vec!["127.0.0.1:0".to_string()],
        };
        let raft_node = Arc::new(
            AppRaftNode::new(
                raft_config.node_id,
                Arc::clone(&storage),
                Arc::clone(&state_machine),
                &raft_config,
            )
            .await
            .unwrap(),
        );

        let shard_group_id = ShardGroupId::new(1);
        ensure_root_mount(Arc::clone(&raft_node), Arc::clone(&mount_table), shard_group_id)
            .await
            .unwrap();
        let ufs_registry = Arc::new(UfsRegistry::new());
        let worker_manager = Arc::new(WorkerManager::new(60));
        worker_manager.increment_metadata_epoch();

        AuthorityStage {
            storage,
            mount_table: Arc::clone(&mount_table),
            raft_node: Arc::clone(&raft_node),
            state_store: Arc::new(RaftStateStore::new(raft_node)),
            worker_manager,
            metadata_metrics: Arc::new(MetadataMetrics::new()),
            shard_group_id,
            _ufs_registry: Arc::clone(&ufs_registry),
            _ufs_metadata_proxy: Arc::new(UfsMetadataProxy::new(mount_table, ufs_registry)),
        }
    }

    fn test_config() -> MetadataConfig {
        MetadataConfig {
            rpc_addr: "127.0.0.1:18080".parse().unwrap(),
            authz: MetadataAuthzConfig::default(),
            raft: RaftConfig {
                cluster_id: "test".to_string(),
                node_id: 1,
                peers: vec!["127.0.0.1:0".to_string()],
            },
            shard: ShardConfig {
                num_shards: 1,
                shard_group_id: 1,
            },
            worker: WorkerConfig::default(),
            bootstrap: BootstrapConfig {
                root_readiness: metadata::readiness::RootReadinessConfig::default(),
            },
        }
    }

    #[tokio::test]
    async fn staged_startup_builds_worker_background_and_filesystem() {
        let dir = TempDir::new().unwrap();
        let config = test_config();
        let authority = test_authority_stage(&dir).await;
        let mut worker = build_worker_runtime(&config, &authority);
        let background = build_background_runtime(&authority, &mut worker).await;
        assert!(Arc::ptr_eq(
            &background._shared_inflight_registry,
            &worker.owned.shared_inflight_registry
        ));
        assert!(Arc::strong_count(&background._delete_executor) >= 3);
        let _filesystem = build_filesystem_runtime(&config, &authority).await.unwrap();
    }

    #[tokio::test]
    async fn worker_owned_runtime_supplies_background_inputs_without_moving_ownership() {
        let dir = TempDir::new().unwrap();
        let config = test_config();
        let authority = test_authority_stage(&dir).await;
        let worker = build_worker_runtime(&config, &authority);
        let inputs = worker.background_inputs();

        assert!(Arc::ptr_eq(&worker.owned.repair_queue, &inputs.repair_queue));
        assert!(Arc::ptr_eq(&worker.owned.orphan_queue, &inputs.orphan_queue));
        assert!(Arc::ptr_eq(&worker.owned.repair_planner, &inputs.repair_planner));
        assert!(Arc::ptr_eq(
            &worker.owned.shared_inflight_registry,
            &inputs.shared_inflight_registry
        ));
    }
}
