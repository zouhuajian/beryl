// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Runtime composition root for the metadata binary.

use crate::inflight_registry::InflightRegistry;
use crate::maintenance::repair::{RepairPlanner, RepairPolicy, RepairQueue};
use crate::maintenance::{MaintenanceHandle, MaintenanceService};
use crate::metrics::MetadataMetrics;
use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use crate::readiness::{wait_for_root_ready_with_inputs, RootReadinessGate, RootReadinessLogFields, RootReadyInputs};
use crate::service::{MetadataFileSystem, MetadataFileSystemDeps, MetadataFileSystemServiceImpl, MsyncHandler};
use crate::state::RaftStateStore;
use crate::worker::{MetadataWorkerServiceImpl, WorkerBackgroundHandle, WorkerManager};
use crate::{observe, MetadataConfig, MountTable};
use beryl_common::observe::{init_observability as init_common_observability, ObservabilityGuard, ServiceInfo};
use beryl_proto::metadata::file_system_service_proto_server::FileSystemServiceProtoServer;
use beryl_proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProtoServer;
use beryl_types::GroupName;
use std::sync::Arc;
use tokio::signal;
use tokio::task::JoinHandle;
use tonic::transport as tonic_net;
use tonic_health::pb::health_server::HealthServer;
use tonic_health::server::{HealthReporter, HealthService};
use tracing::info;

pub type DynError = Box<dyn std::error::Error>;

type MetadataHealthServer = HealthServer<HealthService>;

/// Keeps the tracing and metrics provider alive for the process lifetime.
pub struct Observability {
    _observability_guard: ObservabilityGuard,
}

/// Authoritative metadata dependencies built before public services are exposed.
pub struct MetadataAuthority {
    pub(crate) storage: Arc<RocksDBStorage>,
    pub(crate) mount_table: Arc<MountTable>,
    pub(crate) raft_node: Arc<AppRaftNode>,
    pub(crate) state_store: Arc<dyn crate::state::StateStore>,
    pub(crate) metadata_metrics: Arc<MetadataMetrics>,
    pub(crate) group_name: GroupName,
}

impl MetadataAuthority {
    /// Build the worker control-plane service without exposing Raft/storage internals.
    pub fn worker_service(&self, manager: Arc<WorkerManager>) -> MetadataWorkerServiceImpl {
        MetadataWorkerServiceImpl::new(
            Arc::clone(&self.raft_node),
            manager,
            Arc::clone(&self.mount_table),
            self.group_name.clone(),
        )
    }

    /// Return durable worker descriptors needed to rebuild process-local soft state.
    pub fn registered_workers(&self) -> crate::MetadataResult<Vec<crate::worker::WorkerInfo>> {
        self.storage.list_workers()
    }

    /// Stop the authority's Raft runtime.
    pub async fn shutdown(&self) -> crate::MetadataResult<()> {
        self.raft_node.shutdown().await
    }
}

/// Required worker runtime soft state shared by worker RPC and background work.
pub struct WorkerRuntime {
    pub manager: Arc<WorkerManager>,
}

/// Maintenance repair state shared by maintenance tasks, repair signals, and command routing.
#[derive(Clone)]
pub(crate) struct MaintenanceRepairState {
    repair_queue: Arc<RepairQueue>,
    repair_planner: Arc<RepairPlanner>,
    repair_policy: RepairPolicy,
}

/// Worker-owned background lifecycle started after authority and maintenance are available.
pub struct WorkerBackground {
    _handle: WorkerBackgroundHandle,
}

/// Metadata maintenance lifecycle independent of worker RPC serving.
pub struct Maintenance {
    _repair_state: MaintenanceRepairState,
    _maintenance_service: Arc<MaintenanceService>,
    _maintenance_handle: MaintenanceHandle,
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
        let repair_planner = Arc::new(RepairPlanner::new());
        let repair_policy = RepairPolicy::default();

        Self {
            repair_queue,
            repair_planner,
            repair_policy,
        }
    }
}

impl WorkerRuntime {
    /// Builds required worker soft state before worker RPC registration.
    fn new() -> Self {
        let manager = Arc::new(WorkerManager::new(60));
        manager.reset_worker_soft_state();
        info!(event = "worker_soft_state_reset", "worker soft state reset");

        Self { manager }
    }

    /// Builds the worker RPC service from required runtime state.
    fn service(&self, authority: &MetadataAuthority) -> MetadataWorkerServiceImpl {
        let mut service = MetadataWorkerServiceImpl::new(
            Arc::clone(&authority.raft_node),
            Arc::clone(&self.manager),
            Arc::clone(&authority.mount_table),
            authority.group_name.clone(),
        );
        service.set_slot_metrics(Arc::clone(&authority.metadata_metrics));

        service
    }

    /// Starts worker-service background tasks.
    fn start_background(&self, service: &MetadataWorkerServiceImpl) -> WorkerBackgroundHandle {
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
        crate::lifecycle::prepare_metadata_start(config.as_ref()).await?;
        let authority = build_authority(config.as_ref()).await?;
        let maintenance_repair = build_maintenance_repair_state(config.as_ref());
        let (worker, mut worker_service) = build_worker_runtime(&authority, &maintenance_repair)?;
        let readiness = build_readiness(config.as_ref(), &authority).await;
        let filesystem =
            build_filesystem_service(config.as_ref(), &authority, Arc::clone(&worker.manager), &readiness).await?;
        let maintenance = build_maintenance(&authority, &worker, &readiness, maintenance_repair).await;
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
    let config_path = std::env::var("BERYL_CONFIG").unwrap_or_else(|_| "conf/metadata.yaml".to_string());
    let config = Arc::new(MetadataConfig::load(&config_path)?);

    Ok(config)
}

/// Initializes process-wide observability after configuration has been loaded.
pub fn init_observability(config: &MetadataConfig) -> Result<Observability, DynError> {
    let obs_config = config.observability.clone();
    let service_info = ServiceInfo {
        name: "metadata".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        environment: "development".to_string(),
        instance_id: uuid::Uuid::new_v4().to_string(),
        node_name: None,
    };
    let observability_guard = init_common_observability(&obs_config, service_info)?;
    observe::record_metadata_started("metadata", env!("CARGO_PKG_VERSION"));

    info!(
        event = "metadata_configuration_loaded",
        rpc_addr = %config.rpc_addr,
        metrics_bind = %config.observability.metrics.prometheus.bind,
        storage_dir = %config.storage_dir.display(),
        node_id = config.raft.node_id,
        raft_mode = ?config.raft.mode,
        authority_group_name = %config.authority.group_name,
        "Configuration loaded (sensitive values redacted)"
    );

    Ok(Observability {
        _observability_guard: observability_guard,
    })
}

/// Builds authoritative storage, mount, raft, and state-store dependencies in startup order.
pub async fn build_authority(config: &MetadataConfig) -> Result<MetadataAuthority, DynError> {
    let db_path = effective_storage_dir(config);
    let storage = Arc::new(
        RocksDBStorage::open_existing_for_start(&db_path).map_err(|e| format!("Failed to initialize RocksDB: {e}"))?,
    );

    let mount_table = Arc::new(
        MountTable::load_from_storage(storage.as_ref())
            .map_err(|e| format!("Failed to load mount table from storage: {e}"))?,
    );
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));

    let raft_node = Arc::new(
        AppRaftNode::new(
            config.raft.node_id,
            Arc::clone(&storage),
            Arc::clone(&state_machine),
            Arc::clone(&mount_table),
            &config.raft,
        )
        .await
        .map_err(|e| format!("Failed to initialize Raft node: {e}"))?,
    );
    let state_store: Arc<dyn crate::state::StateStore> = Arc::new(RaftStateStore::new(Arc::clone(&raft_node)));

    Ok(MetadataAuthority {
        storage,
        mount_table,
        raft_node,
        state_store,
        metadata_metrics: Arc::new(MetadataMetrics::new()),
        group_name: config.authority.group_name.clone(),
    })
}

fn effective_storage_dir(config: &MetadataConfig) -> std::path::PathBuf {
    config.storage_dir.clone()
}

/// Builds maintenance-owned repair state before worker RPC and maintenance tasks are wired.
pub(crate) fn build_maintenance_repair_state(config: &MetadataConfig) -> MaintenanceRepairState {
    MaintenanceRepairState::new(config)
}

/// Builds the required worker runtime without starting heavy background work.
pub(crate) fn build_worker_runtime(
    authority: &MetadataAuthority,
    _repair: &MaintenanceRepairState,
) -> Result<(WorkerRuntime, MetadataWorkerServiceImpl), DynError> {
    let worker = WorkerRuntime::new();
    worker
        .manager
        .load_registered_workers(authority.storage.list_workers()?)?;
    let service = worker.service(authority);
    Ok((worker, service))
}

/// Starts metadata maintenance side effects after authority and worker state exist.
pub(crate) async fn build_maintenance(
    authority: &MetadataAuthority,
    worker: &WorkerRuntime,
    _readiness: &Readiness,
    repair: MaintenanceRepairState,
) -> Maintenance {
    let maintenance_service = Arc::new(MaintenanceService::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(&worker.manager),
        Arc::clone(&repair.repair_queue),
        Arc::clone(&repair.repair_planner),
        repair.repair_policy,
    ));
    let maintenance_handle = maintenance_service.start();

    Maintenance {
        _repair_state: repair,
        _maintenance_service: maintenance_service,
        _maintenance_handle: maintenance_handle,
    }
}

/// Starts worker-owned background work after authority and maintenance are available.
pub fn build_worker_background(
    worker: &WorkerRuntime,
    service: &mut MetadataWorkerServiceImpl,
    _maintenance: &Maintenance,
) -> WorkerBackground {
    let handle = worker.start_background(service);

    WorkerBackground { _handle: handle }
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
    let storage_clone = Arc::clone(&authority.storage);
    let group_name = authority.group_name.clone();
    let metrics = Arc::clone(&authority.metadata_metrics);
    let fail_fast = config.bootstrap.root_readiness.fail_fast;
    let log_fields = RootReadinessLogFields {
        cluster_id: config.cluster_id.clone(),
        group_name: config.authority.group_name.to_string(),
        node_id: config.raft.node_id,
        storage_dir: config.storage_dir.display().to_string(),
    };
    let readiness_watcher = tokio::spawn(async move {
        let result = wait_for_root_ready_with_inputs(RootReadyInputs {
            raft_node: raft_node_clone,
            mount_table: mount_table_clone,
            storage: Some(storage_clone),
            namespace_owner_group_name: group_name,
            readiness_gate: readiness_gate_clone,
            config: readiness_config,
            metrics: Some(metrics),
            log_fields,
        })
        .await;
        match result {
            Ok(()) => {
                health_reporter
                    .set_serving::<FileSystemServiceProtoServer<MetadataFileSystemServiceImpl>>()
                    .await;
            }
            Err(err) => {
                tracing::error!(error = %err, "Root readiness watcher failed");
                if fail_fast {
                    std::process::exit(1);
                }
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
    _config: &MetadataConfig,
    authority: &MetadataAuthority,
    worker_manager: Arc<WorkerManager>,
    readiness: &Readiness,
) -> Result<MetadataFileSystemServiceImpl, DynError> {
    let session_registry = Arc::new(crate::session_registry::SessionRegistry::default());
    let lease_manager = Arc::new(crate::inode_lease::LeaseManager::default());
    let filesystem = Arc::new(MetadataFileSystem::new(MetadataFileSystemDeps {
        state_store: Arc::clone(&authority.state_store),
        mount_table: Arc::clone(&authority.mount_table),
        storage: Arc::clone(&authority.storage),
        raft_node: Some(Arc::clone(&authority.raft_node)),
        session_registry,
        lease_manager,
        worker_manager: Some(worker_manager),
        metrics: Some(Arc::clone(&authority.metadata_metrics)),
        readiness_gate: Some(readiness.gate()),
    }));
    let msync = Some(MsyncHandler::new(
        Arc::clone(&authority.raft_node),
        authority.group_name.clone(),
    ));

    Ok(MetadataFileSystemServiceImpl::new(filesystem, msync))
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
    use crate::config::{BootstrapConfig, MetadataAuthorityConfig, RaftConfig, WorkerConfig};
    use crate::raft::Command;
    use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction};
    use beryl_common::header::{RequestHeader, ResponseHeader};
    use beryl_proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
    use beryl_proto::metadata::{MsyncRequestProto, MsyncResponseProto};
    use beryl_types::ids::WorkerId;
    use beryl_types::{ClientId, GroupName, GroupStateWatermark, RaftLogId};
    use std::time::Duration;
    use tempfile::TempDir;

    async fn test_authority(dir: &TempDir) -> MetadataAuthority {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(
                raft_config.node_id,
                Arc::clone(&storage),
                Arc::clone(&state_machine),
                Arc::clone(&mount_table),
                &raft_config,
            )
            .await
            .unwrap(),
        );
        raft_node
            .initialize_single_node("127.0.0.1:0".to_string())
            .await
            .unwrap();

        let group_name = GroupName::parse("root").unwrap();
        for _ in 0..100 {
            if raft_node.is_leader() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        raft_node
            .propose(Command::BootstrapNamespace {
                proposed_at_ms: 1,
                group_name: group_name.clone(),
            })
            .await
            .unwrap();

        MetadataAuthority {
            storage,
            mount_table: Arc::clone(&mount_table),
            raft_node: Arc::clone(&raft_node),
            state_store: Arc::new(RaftStateStore::new(raft_node)),
            metadata_metrics: Arc::new(MetadataMetrics::new()),
            group_name,
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
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(
                raft_config.node_id,
                Arc::clone(&storage),
                state_machine,
                Arc::clone(&mount_table),
                &raft_config,
            )
            .await
            .unwrap(),
        );
        let group_name = GroupName::parse("root").unwrap();
        let filesystem = Arc::new(MetadataFileSystem::new(MetadataFileSystemDeps {
            state_store: Arc::new(RaftStateStore::new(Arc::clone(&raft_node))),
            mount_table,
            storage,
            raft_node: Some(Arc::clone(&raft_node)),
            session_registry: Arc::new(crate::session_registry::SessionRegistry::default()),
            lease_manager: Arc::new(crate::inode_lease::LeaseManager::default()),
            worker_manager: None,
            metrics: None,
            readiness_gate: None,
        }));
        let msync = Some(MsyncHandler::new(raft_node, group_name));
        MetadataFileSystemServiceImpl::new(filesystem, msync)
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
            cluster_id: "local".to_string(),
            rpc_addr: "127.0.0.1:18080".parse().unwrap(),
            storage_dir: std::path::PathBuf::from("data/metadata"),
            raft: RaftConfig::default(),
            authority: MetadataAuthorityConfig {
                group_name: GroupName::parse("root").unwrap(),
            },
            worker: WorkerConfig::default(),
            bootstrap: BootstrapConfig {
                root_readiness: crate::readiness::RootReadinessConfig::default(),
            },
            observability: test_observability_config(),
        }
    }

    fn test_observability_config() -> beryl_common::observe::ObservabilityConfig {
        let mut flat = beryl_common::config::FlatConfig::new();
        flat.set("observe.log.format", "compact");
        flat.set("observe.log.output", "stderr");
        flat.set(
            "observe.log.level",
            "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn",
        );
        flat.set("observe.metrics.prometheus.bind", "127.0.0.1:18081");
        flat.set("observe.metrics.prometheus.path", "/metrics");
        beryl_common::observe::ObservabilityConfig::from_flat(&flat).expect("test observe config")
    }

    #[tokio::test]
    async fn runtime_composition_separates_worker_maintenance_and_readiness() {
        let dir = TempDir::new().unwrap();
        let config = test_config();
        let authority = test_authority(&dir).await;
        let maintenance_repair = build_maintenance_repair_state(&config);
        let (worker_runtime, mut worker_service) = build_worker_runtime(&authority, &maintenance_repair).unwrap();
        let readiness = build_readiness(&config, &authority).await;
        let maintenance = build_maintenance(&authority, &worker_runtime, &readiness, maintenance_repair).await;
        let worker_background = build_worker_background(&worker_runtime, &mut worker_service, &maintenance);
        let _worker_background = worker_background;
        assert_eq!(maintenance._maintenance_handle.task_count(), 3);
        let _filesystem =
            build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
                .await
                .unwrap();
    }

    #[tokio::test]
    async fn worker_runtime_loads_durable_descriptors_without_live_registration() {
        let dir = TempDir::new().unwrap();
        let config = test_config();
        let authority = test_authority(&dir).await;
        let worker_id = WorkerId::new(91);
        authority
            .storage
            .put_worker(&crate::worker::WorkerInfo {
                group_name: authority.group_name.clone(),
                worker_id,
                address: "127.0.0.1:19091".to_string(),
                worker_net_protocol: 1,
                capacity_total: 0,
                capacity_used: 0,
                capacity_available: 0,
                active_reads: 0,
                active_writes: 0,
                health: crate::worker::HealthStatus::Healthy,
                last_heartbeat: 0,
                fault_domain: Some("rack-a".to_string()),
            })
            .unwrap();

        let repair = build_maintenance_repair_state(&config);
        let (worker, _service) = build_worker_runtime(&authority, &repair).unwrap();

        let descriptor = worker
            .manager
            .get_descriptor(&authority.group_name, worker_id)
            .expect("durable descriptor");
        assert_eq!(descriptor.address, "127.0.0.1:19091");
        assert!(worker
            .manager
            .get_registration(&authority.group_name, worker_id)
            .is_none());
    }

    #[tokio::test]
    async fn runtime_handles_hold_started_background_tasks() {
        let dir = TempDir::new().unwrap();
        let config = test_config();
        let authority = test_authority(&dir).await;
        let readiness = build_readiness(&config, &authority).await;
        let maintenance_repair = build_maintenance_repair_state(&config);
        let (worker_runtime, mut worker_service) = build_worker_runtime(&authority, &maintenance_repair).unwrap();
        let filesystem = build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
            .await
            .unwrap();
        let maintenance = build_maintenance(&authority, &worker_runtime, &readiness, maintenance_repair).await;
        let worker_background = build_worker_background(&worker_runtime, &mut worker_service, &maintenance);
        let (_services, handles) =
            compose_services(filesystem, worker_service, readiness, worker_background, maintenance);

        assert_eq!(handles._worker_background._handle.task_count(), 0);
        assert_eq!(handles._maintenance._maintenance_handle.task_count(), 3);
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
        let (worker_runtime, _worker_service) = build_worker_runtime(&authority, &maintenance_repair).unwrap();
        let service = build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
            .await
            .unwrap();
        let group_name = GroupName::parse("root").unwrap();

        let response = call_msync(
            &service,
            RequestHeader::new(ClientId::new(7)).with_group_name(group_name.clone()),
        )
        .await;
        let header = parse_msync_header(&response);

        assert_eq!(header.group_name, Some(group_name.clone()));
        assert!(header.rpc_error.is_none());
        assert!(header.state.is_empty());
        assert_eq!(
            response.state,
            Some((&GroupStateWatermark::new(group_name, expected_state_id)).into())
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
        let (worker_runtime, _worker_service) = build_worker_runtime(&authority, &maintenance_repair).unwrap();
        let service = build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
            .await
            .unwrap();
        let group_name = GroupName::parse("root").unwrap();
        let mut header = RequestHeader::new(ClientId::new(7)).with_group_name(group_name.clone());
        header.state = vec![GroupStateWatermark::new(
            group_name.clone(),
            RaftLogId::new(99, 99, u64::MAX),
        )];

        let response = call_msync(&service, header).await;
        let response_header = parse_msync_header(&response);

        assert!(response_header.rpc_error.is_none());
        assert_eq!(
            response.state,
            Some((&GroupStateWatermark::new(group_name, expected_state_id)).into())
        );
    }

    #[tokio::test]
    async fn msync_rejects_missing_header_group_name() {
        let dir = TempDir::new().unwrap();
        let authority = test_authority(&dir).await;
        wait_for_leader_state(&authority).await;
        let config = test_config();
        let readiness = build_readiness(&config, &authority).await;
        let maintenance_repair = build_maintenance_repair_state(&config);
        let (worker_runtime, _worker_service) = build_worker_runtime(&authority, &maintenance_repair).unwrap();
        let service = build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
            .await
            .unwrap();

        let response = call_msync(&service, RequestHeader::new(ClientId::new(7))).await;
        let header = parse_msync_header(&response);
        let rpc_error = header.rpc_error.expect("missing header group error");

        assert!(header.state.is_empty());
        assert!(response.state.is_none());
        assert_eq!(rpc_error.kind, ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader));
        assert_eq!(rpc_error.recovery, RecoveryAction::Fail);
    }

    #[tokio::test]
    async fn msync_rejects_non_local_group_with_structured_error() {
        let dir = TempDir::new().unwrap();
        let authority = test_authority(&dir).await;
        wait_for_leader_state(&authority).await;
        let config = test_config();
        let readiness = build_readiness(&config, &authority).await;
        let maintenance_repair = build_maintenance_repair_state(&config);
        let (worker_runtime, _worker_service) = build_worker_runtime(&authority, &maintenance_repair).unwrap();
        let service = build_filesystem_service(&config, &authority, Arc::clone(&worker_runtime.manager), &readiness)
            .await
            .unwrap();
        let group_name = GroupName::parse("other").unwrap();

        let response = call_msync(
            &service,
            RequestHeader::new(ClientId::new(7)).with_group_name(group_name),
        )
        .await;
        let header = parse_msync_header(&response);
        let rpc_error = header.rpc_error.expect("non-local group error");

        assert!(header.state.is_empty());
        assert!(response.state.is_none());
        assert_eq!(
            rpc_error.kind,
            ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch)
        );
        assert!(matches!(rpc_error.recovery, RecoveryAction::RefreshMetadata { .. }));
    }

    #[tokio::test]
    async fn msync_nonleader_returns_refresh_metadata_not_leader() {
        let dir = TempDir::new().unwrap();
        let service = nonleader_filesystem_service(&dir).await;

        let response = call_msync(
            &service,
            RequestHeader::new(ClientId::new(7)).with_group_name(GroupName::parse("root").unwrap()),
        )
        .await;
        let header = parse_msync_header(&response);
        let rpc_error = header.rpc_error.expect("not-leader error");

        assert!(header.state.is_empty());
        assert!(response.state.is_none());
        assert_eq!(rpc_error.kind, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
        assert!(matches!(rpc_error.recovery, RecoveryAction::RefreshMetadata { .. }));
    }

    #[tokio::test]
    async fn metadata_server_build_composes_required_runtime() {
        let dir = TempDir::new().unwrap();
        let mut config = test_config();
        config.storage_dir = dir.path().to_path_buf();
        crate::lifecycle::format_metadata_storage(&config).await.unwrap();

        let server = MetadataServer::build(Arc::new(config)).await.unwrap();

        assert_eq!(server.config.rpc_addr, "127.0.0.1:18080".parse().unwrap());
        assert_eq!(server.authority.group_name, GroupName::parse("root").unwrap());
        assert!(dir.path().join("CURRENT").exists());
        assert!(server
            .authority
            .mount_table
            .list_mounts()
            .iter()
            .any(|entry| entry.mount_prefix == "/"));
        assert!(server.worker.manager.is_blockreport_converged(0).converged);
        assert_eq!(server.handles._worker_background._handle.task_count(), 0);
        assert_eq!(server.handles._maintenance._maintenance_handle.task_count(), 3);
        assert!(Arc::strong_count(&server.handles._readiness.gate) >= 1);
    }

    #[tokio::test]
    async fn build_authority_uses_configured_storage_dir() {
        let configured = TempDir::new().unwrap();
        let mut config = test_config();
        config.storage_dir = configured.path().to_path_buf();
        crate::lifecycle::format_metadata_storage(&config).await.unwrap();

        let authority = build_authority(&config).await.unwrap();

        assert!(configured.path().join("CURRENT").exists());
        drop(authority);
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
}
