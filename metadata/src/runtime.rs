// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Runtime composition root for the metadata binary.

use crate::ensure_root_mount;
use crate::inflight_registry::InflightRegistry;
use crate::maintenance::delete::{DeleteExecutor, DeleteExecutorHandle};
use crate::maintenance::repair::{
    OrphanQueue, RepairPlanner, RepairPolicy, RepairQueue, RepairSignalHandler, RepairSignalHandlerDeps,
    RepairSignalSink,
};
use crate::maintenance::{MaintenanceHandle, MaintenanceService};
use crate::metrics::MetadataMetrics;
use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use crate::readiness::{wait_for_root_ready_with_metrics, RootReadinessGate};
use crate::service::{
    filesystem_permission_checker, FileSystemAuthorityDeps, FileSystemPolicyDeps, FileSystemRuntimeDeps,
    MetadataFileSystemServiceDeps, MetadataFileSystemServiceImpl, SharedWorkerCommitHook,
};
use crate::state::RaftStateStore;
use crate::worker::{
    DeleteCommandSource, MetadataWorkerServiceImpl, RepairCommandSource, WorkerBackgroundHandle, WorkerCommandRouter,
    WorkerManager,
};
use crate::{MetadataConfig, MountTable};
use common::observe::{
    init_observability as init_common_observability, ObservabilityConfig, ObservabilityGuard, ServiceInfo,
};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProtoServer;
use proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProtoServer;
use std::sync::{Arc, Mutex};
use tokio::signal;
use tokio::task::JoinHandle;
use tonic::transport as tonic_net;
use tonic_health::pb::health_server::HealthServer;
use tonic_health::server::{HealthReporter, HealthService};
use tracing::info;
use types::ids::ShardGroupId;

pub type DynError = Box<dyn std::error::Error>;

type MetadataHealthServer = HealthServer<HealthService>;

/// Keeps the tracing and metrics provider alive for the process lifetime.
pub struct Observability {
    _observability_guard: ObservabilityGuard,
}

/// Authoritative metadata dependencies built before public services are exposed.
pub struct MetadataAuthority {
    pub storage: Arc<RocksDBStorage>,
    pub mount_table: Arc<MountTable>,
    pub raft_node: Arc<AppRaftNode>,
    pub state_store: Arc<dyn crate::state::StateStore>,
    pub metadata_metrics: Arc<MetadataMetrics>,
    pub shard_group_id: ShardGroupId,
}

/// Required worker runtime soft state shared by worker RPC and background work.
pub struct WorkerRuntime {
    pub manager: Arc<WorkerManager>,
}

/// Maintenance repair state shared by maintenance tasks, repair signals, and command routing.
#[derive(Clone)]
pub(crate) struct MaintenanceRepairState {
    repair_queue: Arc<RepairQueue>,
    orphan_queue: Arc<OrphanQueue>,
    repair_planner: Arc<RepairPlanner>,
    repair_policy: RepairPolicy,
    shared_inflight_registry: Arc<InflightRegistry>,
}

/// Worker-owned background lifecycle started after authority and maintenance are available.
pub struct WorkerBackground {
    _command_router: Arc<WorkerCommandRouter>,
    _handle: WorkerBackgroundHandle,
}

/// Metadata maintenance lifecycle independent of worker RPC serving.
pub struct Maintenance {
    _repair_state: MaintenanceRepairState,
    _maintenance_service: Arc<MaintenanceService>,
    delete_executor: Arc<DeleteExecutor>,
    _maintenance_handle: MaintenanceHandle,
    _delete_executor_handle: DeleteExecutorHandle,
}

/// Readiness gate, watcher task, and health service state.
pub struct Readiness {
    pub health_service: MetadataHealthServer,
    handle: ReadinessHandle,
}

/// Root readiness task handle and gate retained for request guards.
pub struct ReadinessHandle {
    gate: Arc<RootReadinessGate>,
    _watcher: JoinHandle<()>,
}

/// Services registered on the tonic server.
pub struct RpcServices {
    filesystem: MetadataFileSystemServiceImpl,
    worker: MetadataWorkerServiceImpl,
    health: MetadataHealthServer,
}

/// Long-lived handles retained by `serve()` for the server lifetime.
pub struct RuntimeHandles {
    _worker_background: WorkerBackground,
    _maintenance: Maintenance,
    _readiness: ReadinessHandle,
}

impl MaintenanceRepairState {
    fn new(config: &MetadataConfig) -> Self {
        let repair_metrics = Arc::new(crate::maintenance::repair::RepairMetrics::new());
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
        let repair_planner = Arc::new(RepairPlanner::new(Arc::clone(&orphan_queue)));
        let repair_policy = RepairPolicy::default();

        Self {
            repair_queue,
            orphan_queue,
            repair_planner,
            repair_policy,
            shared_inflight_registry,
        }
    }
}

impl WorkerRuntime {
    /// Builds required worker soft state before worker RPC registration.
    fn new() -> Self {
        let manager = Arc::new(WorkerManager::new(60));
        manager.increment_metadata_epoch();
        info!("Metadata epoch initialized: {}", manager.get_metadata_epoch());

        Self { manager }
    }

    /// Builds the worker RPC service from required runtime state.
    fn service(&self, authority: &MetadataAuthority, repair: &MaintenanceRepairState) -> MetadataWorkerServiceImpl {
        let repair_signal_handler: Arc<dyn RepairSignalSink> =
            Arc::new(RepairSignalHandler::new(RepairSignalHandlerDeps {
                raft_node: Arc::clone(&authority.raft_node),
                worker_manager: Arc::clone(&self.manager),
                repair_queue: Arc::clone(&repair.repair_queue),
                orphan_queue: Arc::clone(&repair.orphan_queue),
                repair_planner: Arc::clone(&repair.repair_planner),
                repair_policy: repair.repair_policy,
            }));
        let mut service = MetadataWorkerServiceImpl::new(
            Arc::clone(&authority.raft_node),
            Arc::clone(&self.manager),
            repair_signal_handler,
            Arc::clone(&authority.mount_table),
        );
        service.set_slot_metrics(Arc::clone(&authority.metadata_metrics));

        service
    }

    /// Builds the worker command router after maintenance-owned command sources exist.
    fn command_router(&self, maintenance: &Maintenance) -> Arc<WorkerCommandRouter> {
        const MAX_DELETE_COMMANDS_PER_HEARTBEAT: usize = 4;
        const MAX_REPAIR_COMMANDS_PER_HEARTBEAT: usize = 8;

        let mut router = WorkerCommandRouter::new();
        router.register_source(
            Arc::new(DeleteCommandSource::new(Arc::clone(&maintenance.delete_executor))),
            MAX_DELETE_COMMANDS_PER_HEARTBEAT,
        );
        router.register_source(
            Arc::new(RepairCommandSource::new(Arc::clone(
                &maintenance._repair_state.repair_queue,
            ))),
            MAX_REPAIR_COMMANDS_PER_HEARTBEAT,
        );
        Arc::new(router)
    }

    /// Connects command routing before starting worker background tasks.
    fn start_background(
        &self,
        service: &mut MetadataWorkerServiceImpl,
        command_router: Arc<WorkerCommandRouter>,
    ) -> WorkerBackgroundHandle {
        service.set_command_router(command_router);
        service.start_background_tasks()
    }
}

/// Final server composition object for metadata.
pub struct MetadataServer {
    config: Arc<MetadataConfig>,
    authority: MetadataAuthority,
    worker: WorkerRuntime,
    services: RpcServices,
    handles: RuntimeHandles,
}

impl MetadataServer {
    /// Builds long-lived metadata runtime objects in startup dependency order.
    pub async fn build(config: Arc<MetadataConfig>) -> Result<Self, DynError> {
        let authority = build_authority(config.as_ref()).await?;
        let maintenance_repair = build_maintenance_repair_state(config.as_ref());
        let (worker, mut worker_service) = build_worker_runtime(&authority, &maintenance_repair);
        let readiness = build_readiness(config.as_ref(), &authority).await;
        let filesystem =
            build_filesystem_service(config.as_ref(), &authority, Arc::clone(&worker.manager), &readiness).await?;
        let maintenance = build_maintenance(&authority, &worker, maintenance_repair).await;
        let worker_background = build_worker_background(&worker, &mut worker_service, &maintenance);
        let (services, handles) =
            compose_services(filesystem, worker_service, readiness, worker_background, maintenance);

        Ok(Self {
            config,
            authority,
            worker,
            services,
            handles,
        })
    }

    /// Runs the registered RPC services while retaining runtime handles.
    pub async fn serve(self) -> Result<(), DynError> {
        let Self {
            config,
            authority,
            worker,
            services,
            handles,
        } = self;
        let _keep_alive = (authority, worker);

        serve(config.as_ref(), services, handles).await
    }
}

/// Loads metadata configuration from the configured path.
pub fn load_config() -> Result<Arc<MetadataConfig>, DynError> {
    let config_path = std::env::var("VECTON_CONFIG").unwrap_or_else(|_| "conf/core-site.yaml".to_string());
    let config = Arc::new(MetadataConfig::load(&config_path)?);

    Ok(config)
}

/// Initializes process-wide observability after configuration has been loaded.
pub fn init_observability(config: &MetadataConfig) -> Result<Observability, DynError> {
    let obs_config = ObservabilityConfig::default();
    let service_info = ServiceInfo {
        name: "metadata".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        environment: "development".to_string(),
        instance_id: uuid::Uuid::new_v4().to_string(),
        node_name: None,
    };
    let observability_guard = init_common_observability(&obs_config, service_info)?;

    info!(
        rpc_addr = %config.rpc_addr,
        storage_dir = %config.storage_dir.display(),
        authz_filesystem_mode = ?config.authz.filesystem.mode,
        node_id = config.raft.node_id,
        peers_count = config.raft.peers.len(),
        authority_group_id = config.authority.group_id,
        "Configuration loaded (sensitive values redacted)"
    );

    Ok(Observability {
        _observability_guard: observability_guard,
    })
}

/// Builds authoritative storage, mount, raft, and state-store dependencies in startup order.
pub async fn build_authority(config: &MetadataConfig) -> Result<MetadataAuthority, DynError> {
    let db_path = effective_storage_dir(config);
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

    let authority_group_id = ShardGroupId::new(config.authority.group_id);
    ensure_root_mount(Arc::clone(&raft_node), Arc::clone(&mount_table), authority_group_id)
        .await
        .map_err(|e| format!("Failed to ensure root mount: {e}"))?;

    let state_store: Arc<dyn crate::state::StateStore> = Arc::new(RaftStateStore::new(Arc::clone(&raft_node)));

    Ok(MetadataAuthority {
        storage,
        mount_table,
        raft_node,
        state_store,
        metadata_metrics: Arc::new(MetadataMetrics::new()),
        shard_group_id: authority_group_id,
    })
}

fn effective_storage_dir(config: &MetadataConfig) -> std::path::PathBuf {
    std::env::var_os("VECTON_METADATA_DB_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| config.storage_dir.clone())
}

/// Builds maintenance-owned repair state before worker RPC and maintenance tasks are wired.
pub(crate) fn build_maintenance_repair_state(config: &MetadataConfig) -> MaintenanceRepairState {
    MaintenanceRepairState::new(config)
}

/// Builds the required worker runtime without starting heavy background work.
pub(crate) fn build_worker_runtime(
    authority: &MetadataAuthority,
    repair: &MaintenanceRepairState,
) -> (WorkerRuntime, MetadataWorkerServiceImpl) {
    let worker = WorkerRuntime::new();
    let service = worker.service(authority, repair);
    (worker, service)
}

/// Starts metadata maintenance side effects after authority and worker state exist.
pub(crate) async fn build_maintenance(
    authority: &MetadataAuthority,
    worker: &WorkerRuntime,
    repair: MaintenanceRepairState,
) -> Maintenance {
    let maintenance_service = Arc::new(MaintenanceService::new_with_inflight_registry(
        Arc::clone(&authority.raft_node),
        Arc::clone(&authority.storage),
        Arc::clone(&worker.manager),
        Arc::clone(&repair.repair_queue),
        Arc::clone(&repair.orphan_queue),
        Arc::clone(&repair.repair_planner),
        Arc::clone(&authority.metadata_metrics),
        Some(Arc::clone(&repair.shared_inflight_registry)),
        Arc::clone(&authority.mount_table),
        repair.repair_policy,
    ));
    let maintenance_handle = maintenance_service.start();

    let delete_executor = Arc::new(DeleteExecutor::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(&authority.storage),
        Arc::clone(&worker.manager),
        Arc::clone(&authority.metadata_metrics),
        Arc::clone(&authority.mount_table),
        Arc::clone(&repair.shared_inflight_registry),
    ));
    let delete_executor_handle = delete_executor.start();

    Maintenance {
        _repair_state: repair,
        _maintenance_service: maintenance_service,
        delete_executor,
        _maintenance_handle: maintenance_handle,
        _delete_executor_handle: delete_executor_handle,
    }
}

/// Starts worker-owned background work after authority and maintenance are available.
pub fn build_worker_background(
    worker: &WorkerRuntime,
    service: &mut MetadataWorkerServiceImpl,
    maintenance: &Maintenance,
) -> WorkerBackground {
    let command_router = worker.command_router(maintenance);
    let handle = worker.start_background(service, Arc::clone(&command_router));

    WorkerBackground {
        _command_router: command_router,
        _handle: handle,
    }
}

/// Starts the root readiness watcher and owns health serving state.
pub async fn build_readiness(config: &MetadataConfig, authority: &MetadataAuthority) -> Readiness {
    let readiness_gate = Arc::new(RootReadinessGate::new(Some(Arc::clone(&authority.metadata_metrics))));
    let health_reporter = HealthReporter::new();
    health_reporter
        .set_not_serving::<FileSystemServiceProtoServer<MetadataFileSystemServiceImpl>>()
        .await;
    let health_service = HealthServer::new(HealthService::from_health_reporter(health_reporter.clone()));

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

    Readiness {
        health_service,
        handle: ReadinessHandle {
            gate: readiness_gate,
            _watcher: readiness_watcher,
        },
    }
}

impl Readiness {
    fn gate(&self) -> Arc<RootReadinessGate> {
        Arc::clone(&self.handle.gate)
    }
}

/// Constructs the filesystem RPC service without owning readiness lifecycle.
pub async fn build_filesystem_service(
    config: &MetadataConfig,
    authority: &MetadataAuthority,
    worker_manager: Arc<WorkerManager>,
    readiness: &Readiness,
) -> Result<MetadataFileSystemServiceImpl, DynError> {
    let write_session_manager = Arc::new(crate::write_session::WriteSessionManager::default());
    let inode_lease_manager = Arc::new(crate::inode_lease::InodeLeaseManager::default());
    let worker_commit_hook: SharedWorkerCommitHook = Arc::new(Mutex::new(None));
    let filesystem_service = MetadataFileSystemServiceImpl::new(MetadataFileSystemServiceDeps {
        authority: FileSystemAuthorityDeps {
            state_store: Arc::clone(&authority.state_store),
            mount_table: Arc::clone(&authority.mount_table),
            storage: Arc::clone(&authority.storage),
            raft_node: Some(Arc::clone(&authority.raft_node)),
            shard_group_id: authority.shard_group_id,
        },
        runtime: FileSystemRuntimeDeps {
            write_session_manager,
            inode_lease_manager,
            worker_commit_hook,
            worker_manager: Some(worker_manager),
            metrics: Some(Arc::clone(&authority.metadata_metrics)),
            readiness_gate: Some(readiness.gate()),
        },
        policy: FileSystemPolicyDeps {
            leadership_checker: None,
            permission_checker: filesystem_permission_checker(config.authz.filesystem.mode)?,
        },
    });

    Ok(filesystem_service)
}

/// Separates RPC service values from lifecycle handles before entering server code.
pub fn compose_services(
    filesystem: MetadataFileSystemServiceImpl,
    worker: MetadataWorkerServiceImpl,
    readiness: Readiness,
    worker_background: WorkerBackground,
    maintenance: Maintenance,
) -> (RpcServices, RuntimeHandles) {
    let Readiness {
        health_service,
        handle: readiness,
    } = readiness;

    (
        RpcServices {
            filesystem,
            worker,
            health: health_service,
        },
        RuntimeHandles {
            _worker_background: worker_background,
            _maintenance: maintenance,
            _readiness: readiness,
        },
    )
}

/// Registers the filesystem, worker, and health services and holds runtime handles.
pub async fn serve(config: &MetadataConfig, services: RpcServices, _handles: RuntimeHandles) -> Result<(), DynError> {
    let addr = config.rpc_addr;
    info!(addr = %addr, "Listening on (path/filesystem + worker services)");
    tonic_net::Server::builder()
        .add_service(FileSystemServiceProtoServer::new(services.filesystem))
        .add_service(MetadataWorkerServiceProtoServer::new(services.worker))
        .add_service(services.health)
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
    use crate::config::{BootstrapConfig, MetadataAuthorityConfig, MetadataAuthzConfig, RaftConfig, WorkerConfig};
    use common::error::canonical::{ErrorClass, ErrorCode, RefreshReason};
    use common::header::{RequestHeader, ResponseHeader, RpcErrorCode};
    use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
    use proto::metadata::{MsyncRequestProto, MsyncResponseProto};
    use std::sync::OnceLock;
    use tempfile::TempDir;
    use tokio::sync::Mutex as AsyncMutex;
    use types::{ClientId, GroupStateWatermark, RaftLogId};

    async fn test_authority(dir: &TempDir) -> MetadataAuthority {
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
        let raft_config = RaftConfig {
            node_id: 1,
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

        MetadataAuthority {
            storage,
            mount_table: Arc::clone(&mount_table),
            raft_node: Arc::clone(&raft_node),
            state_store: Arc::new(RaftStateStore::new(raft_node)),
            metadata_metrics: Arc::new(MetadataMetrics::new()),
            shard_group_id,
        }
    }

    async fn wait_for_leader_state(authority: &MetadataAuthority) -> RaftLogId {
        for _ in 0..100 {
            if authority.raft_node.is_leader() {
                if let Some(state_id) = authority.raft_node.get_last_applied_state_id() {
                    return state_id;
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
        panic!("single-node test authority did not expose leader last_applied state");
    }

    async fn nonleader_filesystem_service(dir: &TempDir) -> MetadataFileSystemServiceImpl {
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
        let raft_config = RaftConfig {
            node_id: 1,
            peers: Vec::new(),
        };
        let raft_node = Arc::new(
            AppRaftNode::new(raft_config.node_id, Arc::clone(&storage), state_machine, &raft_config)
                .await
                .unwrap(),
        );
        MetadataFileSystemServiceImpl::new(MetadataFileSystemServiceDeps {
            authority: FileSystemAuthorityDeps {
                state_store: Arc::new(RaftStateStore::new(Arc::clone(&raft_node))),
                mount_table,
                storage,
                raft_node: Some(raft_node),
                shard_group_id: ShardGroupId::new(1),
            },
            runtime: FileSystemRuntimeDeps {
                write_session_manager: Arc::new(crate::write_session::WriteSessionManager::default()),
                inode_lease_manager: Arc::new(crate::inode_lease::InodeLeaseManager::default()),
                worker_commit_hook: Arc::new(Mutex::new(None)),
                worker_manager: None,
                metrics: None,
                readiness_gate: None,
            },
            policy: FileSystemPolicyDeps {
                leadership_checker: None,
                permission_checker: Arc::new(crate::service::NonePermissionChecker),
            },
        })
    }

    async fn call_msync(service: &MetadataFileSystemServiceImpl, header: RequestHeader) -> MsyncResponseProto {
        <MetadataFileSystemServiceImpl as FileSystemServiceProto>::msync(
            service,
            tonic::Request::new(MsyncRequestProto {
                header: Some((&header).into()),
            }),
        )
        .await
        .expect("msync must use gRPC OK for application outcomes")
        .into_inner()
    }

    fn parse_msync_header(response: &MsyncResponseProto) -> ResponseHeader {
        response
            .header
            .clone()
            .expect("msync response header")
            .try_into()
            .expect("valid response header")
    }

    fn test_config() -> MetadataConfig {
        MetadataConfig {
            rpc_addr: "127.0.0.1:18080".parse().unwrap(),
            storage_dir: std::path::PathBuf::from("data/metadata"),
            authz: MetadataAuthzConfig::default(),
            raft: RaftConfig {
                node_id: 1,
                peers: vec!["127.0.0.1:0".to_string()],
            },
            authority: MetadataAuthorityConfig { group_id: 1 },
            worker: WorkerConfig::default(),
            bootstrap: BootstrapConfig {
                root_readiness: crate::readiness::RootReadinessConfig::default(),
            },
        }
    }

    #[tokio::test]
    async fn runtime_composition_separates_worker_maintenance_and_readiness() {
        let dir = TempDir::new().unwrap();
        let config = test_config();
        let authority = test_authority(&dir).await;
        let maintenance_repair = build_maintenance_repair_state(&config);
        let (worker_runtime, mut worker_service) = build_worker_runtime(&authority, &maintenance_repair);
        let maintenance = build_maintenance(&authority, &worker_runtime, maintenance_repair).await;
        let worker_background = build_worker_background(&worker_runtime, &mut worker_service, &maintenance);
        assert_eq!(worker_background._command_router.source_count(), 2);
        assert!(Arc::strong_count(&maintenance.delete_executor) >= 3);
        assert_eq!(maintenance._maintenance_handle.task_count(), 8);
        assert!(!maintenance._delete_executor_handle.is_finished());
        let readiness = build_readiness(&config, &authority).await;
        let _filesystem =
            build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
                .await
                .unwrap();
    }

    #[tokio::test]
    async fn runtime_handles_hold_started_background_tasks() {
        let dir = TempDir::new().unwrap();
        let config = test_config();
        let authority = test_authority(&dir).await;
        let readiness = build_readiness(&config, &authority).await;
        let maintenance_repair = build_maintenance_repair_state(&config);
        let (worker_runtime, mut worker_service) = build_worker_runtime(&authority, &maintenance_repair);
        let filesystem = build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
            .await
            .unwrap();
        let maintenance = build_maintenance(&authority, &worker_runtime, maintenance_repair).await;
        let worker_background = build_worker_background(&worker_runtime, &mut worker_service, &maintenance);
        let (_services, handles) =
            compose_services(filesystem, worker_service, readiness, worker_background, maintenance);

        assert_eq!(handles._worker_background._handle.task_count(), 1);
        assert_eq!(handles._maintenance._maintenance_handle.task_count(), 8);
        assert!(!handles._maintenance._delete_executor_handle.is_finished());
        assert!(Arc::strong_count(&handles._readiness.gate) >= 1);
        let _readiness_watcher_finished = handles._readiness._watcher.is_finished();
    }

    #[tokio::test]
    async fn msync_success_on_leader_returns_authoritative_watermark() {
        let dir = TempDir::new().unwrap();
        let authority = test_authority(&dir).await;
        let expected_state_id = wait_for_leader_state(&authority).await;
        let config = test_config();
        let readiness = build_readiness(&config, &authority).await;
        let maintenance_repair = build_maintenance_repair_state(&config);
        let (worker_runtime, _worker_service) = build_worker_runtime(&authority, &maintenance_repair);
        let service = build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
            .await
            .unwrap();
        let group_id = ShardGroupId::new(1);

        let response = call_msync(
            &service,
            RequestHeader::new(ClientId::new(7)).with_group_id(group_id.as_raw()),
        )
        .await;
        let header = parse_msync_header(&response);

        assert_eq!(header.group_id, Some(group_id.as_raw()));
        assert!(header.canonical_error.is_none());
        assert!(header.state.is_empty());
        assert_eq!(
            response.state,
            Some((&GroupStateWatermark::new(group_id, expected_state_id)).into())
        );
    }

    #[tokio::test]
    async fn msync_does_not_compare_client_header_state() {
        let dir = TempDir::new().unwrap();
        let authority = test_authority(&dir).await;
        let expected_state_id = wait_for_leader_state(&authority).await;
        let config = test_config();
        let readiness = build_readiness(&config, &authority).await;
        let maintenance_repair = build_maintenance_repair_state(&config);
        let (worker_runtime, _worker_service) = build_worker_runtime(&authority, &maintenance_repair);
        let service = build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
            .await
            .unwrap();
        let group_id = ShardGroupId::new(1);
        let mut header = RequestHeader::new(ClientId::new(7)).with_group_id(group_id.as_raw());
        header.state = vec![GroupStateWatermark::new(group_id, RaftLogId::new(99, 99, u64::MAX))];

        let response = call_msync(&service, header).await;
        let response_header = parse_msync_header(&response);

        assert!(response_header.canonical_error.is_none());
        assert_eq!(
            response.state,
            Some((&GroupStateWatermark::new(group_id, expected_state_id)).into())
        );
    }

    #[tokio::test]
    async fn msync_rejects_missing_header_group_id() {
        let dir = TempDir::new().unwrap();
        let authority = test_authority(&dir).await;
        wait_for_leader_state(&authority).await;
        let config = test_config();
        let readiness = build_readiness(&config, &authority).await;
        let maintenance_repair = build_maintenance_repair_state(&config);
        let (worker_runtime, _worker_service) = build_worker_runtime(&authority, &maintenance_repair);
        let service = build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
            .await
            .unwrap();

        let response = call_msync(&service, RequestHeader::new(ClientId::new(7))).await;
        let header = parse_msync_header(&response);
        let canonical = header.canonical_error.expect("missing header group error");

        assert!(header.state.is_empty());
        assert!(response.state.is_none());
        assert_eq!(canonical.class, ErrorClass::Fatal);
        assert_eq!(canonical.code, Some(ErrorCode::RpcCode(RpcErrorCode::InvalidHeader)));
    }

    #[tokio::test]
    async fn msync_rejects_non_local_group_with_structured_error() {
        let dir = TempDir::new().unwrap();
        let authority = test_authority(&dir).await;
        wait_for_leader_state(&authority).await;
        let config = test_config();
        let readiness = build_readiness(&config, &authority).await;
        let maintenance_repair = build_maintenance_repair_state(&config);
        let (worker_runtime, _worker_service) = build_worker_runtime(&authority, &maintenance_repair);
        let service = build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
            .await
            .unwrap();
        let group_id = ShardGroupId::new(2);

        let response = call_msync(
            &service,
            RequestHeader::new(ClientId::new(7)).with_group_id(group_id.as_raw()),
        )
        .await;
        let header = parse_msync_header(&response);
        let canonical = header.canonical_error.expect("non-local group error");

        assert!(header.state.is_empty());
        assert!(response.state.is_none());
        assert_eq!(canonical.class, ErrorClass::NeedRefresh);
        assert_eq!(canonical.code, Some(ErrorCode::RpcCode(RpcErrorCode::ShardMoved)));
        assert_eq!(canonical.reason, Some(RefreshReason::OwnerGroupMismatch));
    }

    #[tokio::test]
    async fn msync_nonleader_returns_need_refresh_not_leader() {
        let dir = TempDir::new().unwrap();
        let service = nonleader_filesystem_service(&dir).await;

        let response = call_msync(&service, RequestHeader::new(ClientId::new(7)).with_group_id(1)).await;
        let header = parse_msync_header(&response);
        let canonical = header.canonical_error.expect("not-leader error");

        assert!(header.state.is_empty());
        assert!(response.state.is_none());
        assert_eq!(canonical.class, ErrorClass::NeedRefresh);
        assert_eq!(canonical.code, Some(ErrorCode::RpcCode(RpcErrorCode::NotLeader)));
        assert_eq!(canonical.reason, Some(RefreshReason::NotLeader));
    }

    #[tokio::test]
    async fn maintenance_repair_state_is_maintenance_owned() {
        let dir = TempDir::new().unwrap();
        let config = test_config();
        let authority = test_authority(&dir).await;
        let repair = build_maintenance_repair_state(&config);
        let expected_registry = Arc::clone(&repair.shared_inflight_registry);
        let (worker_runtime, _worker_service) = build_worker_runtime(&authority, &repair);
        let maintenance = build_maintenance(&authority, &worker_runtime, repair).await;

        assert!(Arc::ptr_eq(
            &maintenance._repair_state.shared_inflight_registry,
            &expected_registry
        ));
        assert_eq!(maintenance._maintenance_handle.task_count(), 8);
    }

    #[tokio::test]
    async fn metadata_server_build_composes_required_runtime() {
        let _guard = metadata_db_env_lock().lock().await;
        let dir = TempDir::new().unwrap();
        std::env::remove_var("VECTON_METADATA_DB_PATH");
        let mut config = test_config();
        config.storage_dir = dir.path().to_path_buf();

        let server = MetadataServer::build(Arc::new(config)).await.unwrap();

        assert_eq!(server.config.rpc_addr, "127.0.0.1:18080".parse().unwrap());
        assert_eq!(server.authority.shard_group_id, ShardGroupId::new(1));
        assert!(dir.path().join("CURRENT").exists());
        assert!(server
            .authority
            .mount_table
            .list_mounts()
            .iter()
            .any(|entry| entry.mount_prefix == "/"));
        assert!(server.worker.manager.get_metadata_epoch() > 0);
        assert_eq!(server.handles._worker_background._handle.task_count(), 1);
        assert_eq!(server.handles._maintenance._maintenance_handle.task_count(), 8);
        assert!(!server.handles._maintenance._delete_executor_handle.is_finished());
        assert!(Arc::strong_count(&server.handles._readiness.gate) >= 1);

        std::env::remove_var("VECTON_METADATA_DB_PATH");
    }

    #[tokio::test]
    async fn vecton_metadata_db_path_overrides_config_storage_dir_for_legacy_runtime() {
        let _guard = metadata_db_env_lock().lock().await;
        let configured = TempDir::new().unwrap();
        let legacy_env = TempDir::new().unwrap();
        std::env::set_var("VECTON_METADATA_DB_PATH", legacy_env.path());
        let mut config = test_config();
        config.storage_dir = configured.path().to_path_buf();

        let authority = build_authority(&config).await.unwrap();

        assert!(legacy_env.path().join("CURRENT").exists());
        assert!(!configured.path().join("CURRENT").exists());
        drop(authority);
        std::env::remove_var("VECTON_METADATA_DB_PATH");
    }

    #[test]
    fn binary_entrypoint_delegates_to_metadata_server() {
        let source = include_str!("bin/main.rs");

        assert!(source.contains("MetadataServer::build(config)"));
        assert!(source.contains("server.serve().await"));
        for forbidden in [
            "build_authority(",
            "build_worker_manager(",
            "build_worker_service(",
            "build_maintenance(",
            "build_worker_background(",
            "build_filesystem_service(",
            "compose_services(",
            "serve(config.as_ref()",
        ] {
            assert!(
                !source.contains(forbidden),
                "main.rs must not perform runtime wiring with {forbidden}"
            );
        }
    }

    fn metadata_db_env_lock() -> &'static AsyncMutex<()> {
        static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| AsyncMutex::new(()))
    }
}
