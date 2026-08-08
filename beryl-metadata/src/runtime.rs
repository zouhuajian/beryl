// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Runtime composition root for the metadata binary.

use crate::maintenance::{BlockCleanupCoordinator, DetachedRootReclaimer, MaintenanceHandle, MaintenanceService};
use crate::metrics::MetadataMetrics;
use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use crate::readiness::{wait_for_root_ready_with_inputs, RootReadinessGate, RootReadinessLogFields, RootReadyInputs};
use crate::service::{MetadataFileSystem, MetadataFileSystemDeps, MetadataFileSystemServiceImpl, MsyncHandler};
use crate::state::RaftStateStore;
use crate::worker::{MetadataWorkerServiceImpl, WorkerBackgroundHandle, WorkerManager};
use crate::{observe, MetadataConfig, MountTable};
use beryl_common::grpc_server::spawn_grpc_server;
use beryl_common::observe::{init_observability as init_common_observability, ObservabilityGuard, ServiceInfo};
use beryl_common::service_http::spawn_service_http;
use beryl_common::termination::TerminationMonitor;
use beryl_proto::metadata::file_system_service_proto_server::FileSystemServiceProtoServer;
use beryl_proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProtoServer;
use beryl_types::GroupName;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;
use tonic_health::pb::health_server::HealthServer;
use tonic_health::server::{HealthReporter, HealthService};
use tracing::info;

/// Largest protobuf request accepted by either Metadata gRPC service.
///
/// Repeated-field limits remain the semantic authority. This transport bound
/// prevents protobuf decoding from allocating an arbitrarily large request
/// before service handlers can enforce those limits.
const MAX_REQUEST_SIZE: usize = 4 * 1024 * 1024;

pub type DynError = Box<dyn std::error::Error>;

type MetadataHealthServer = HealthServer<HealthService>;

/// Keeps the tracing and metrics provider alive for the process lifetime.
pub struct Observability {
    observability_guard: ObservabilityGuard,
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

/// Worker-owned background lifecycle started after authority and maintenance are available.
pub struct WorkerBackground {
    _handle: WorkerBackgroundHandle,
}

/// Metadata maintenance lifecycle independent of worker RPC serving.
pub struct Maintenance {
    cleanup: Arc<BlockCleanupCoordinator>,
    _maintenance_service: Arc<MaintenanceService>,
    maintenance_handle: MaintenanceHandle,
}

/// Readiness gate, watcher task, and health service state.
pub struct Readiness {
    pub health_service: MetadataHealthServer,
    handle: ReadinessHandle,
}

/// Root readiness task handle and gate retained for request guards.
pub struct ReadinessHandle {
    gate: Arc<RootReadinessGate>,
    health_reporter: HealthReporter,
    watcher: Option<JoinHandle<()>>,
    fatal: Option<oneshot::Receiver<crate::MetadataError>>,
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
    maintenance: Maintenance,
    readiness: ReadinessHandle,
}

impl ReadinessHandle {
    /// Closes readiness and prevents the startup watcher from racing it open.
    async fn begin_shutdown(&mut self) {
        self.gate.begin_shutdown();
        if let Some(watcher) = self.watcher.take() {
            watcher.abort();
            if let Err(error) = watcher.await {
                if !error.is_cancelled() {
                    tracing::warn!(%error, "Root readiness watcher terminated unexpectedly");
                }
            }
        }
        self.health_reporter
            .set_not_serving::<FileSystemServiceProtoServer<MetadataFileSystemServiceImpl>>()
            .await;
    }

    /// Transfers the single fail-fast readiness receiver to the server event loop.
    fn take_fatal(&mut self) -> oneshot::Receiver<crate::MetadataError> {
        self.fatal.take().expect("readiness failure receiver is owned")
    }
}

impl Drop for ReadinessHandle {
    fn drop(&mut self) {
        self.gate.begin_shutdown();
        if let Some(watcher) = self.watcher.take() {
            watcher.abort();
        }
    }
}

impl Maintenance {
    /// Cancels and awaits all Metadata maintenance loops without a deadline.
    async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.maintenance_handle.shutdown().await
    }

    /// Drains Metadata maintenance loops until the shared process deadline.
    async fn shutdown_until(self, deadline: Instant) -> Result<bool, tokio::task::JoinError> {
        self.maintenance_handle.shutdown_until(deadline).await
    }
}

impl RuntimeHandles {
    /// Publishes not-ready state before any listener begins draining.
    async fn begin_shutdown(&mut self) {
        self.readiness.begin_shutdown().await;
    }

    /// Cancels and awaits Metadata-owned background loops.
    async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        self.maintenance.shutdown().await
    }

    /// Drains Metadata-owned background loops until the shared process deadline.
    async fn shutdown_until(self, deadline: Instant) -> Result<bool, tokio::task::JoinError> {
        self.maintenance.shutdown_until(deadline).await
    }
}

impl WorkerRuntime {
    /// Builds required worker soft state before worker RPC registration.
    fn new(heartbeat_timeout_ms: u32) -> Self {
        let manager = Arc::new(WorkerManager::new(heartbeat_timeout_ms));
        manager.reset_worker_soft_state();
        info!(event = "worker_soft_state_reset", "worker soft state reset");

        Self { manager }
    }

    /// Builds the worker RPC service from required runtime state.
    fn service(
        &self,
        authority: &MetadataAuthority,
        cleanup: Arc<BlockCleanupCoordinator>,
    ) -> MetadataWorkerServiceImpl {
        let mut service = MetadataWorkerServiceImpl::new_with_cleanup(
            Arc::clone(&authority.raft_node),
            Arc::clone(&self.manager),
            Arc::clone(&authority.mount_table),
            authority.group_name.clone(),
            cleanup,
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
    ///
    /// Filesystem writes and cleanup observation share one session registry so
    /// the coordinator sees the same active-write authority as the RPC path.
    pub async fn build(
        config: Arc<MetadataConfig>,
        startup_shutdown: CancellationToken,
    ) -> Result<Option<Self>, DynError> {
        tokio::select! {
            _ = startup_shutdown.cancelled() => return Ok(None),
            result = crate::lifecycle::prepare_metadata_start(config.as_ref()) => result?,
        }
        if startup_shutdown.is_cancelled() {
            return Ok(None);
        }
        let authority = build_authority(config.as_ref()).await?;
        if startup_shutdown.is_cancelled() {
            authority.shutdown().await?;
            return Ok(None);
        }
        let worker = match build_worker_runtime(&authority, config.worker_liveness.heartbeat_timeout_ms) {
            Ok(worker) => worker,
            Err(error) => {
                authority.shutdown().await?;
                return Err(error);
            }
        };
        let mut readiness = build_readiness(config.as_ref(), &authority).await;
        if startup_shutdown.is_cancelled() {
            readiness.handle.begin_shutdown().await;
            authority.shutdown().await?;
            return Ok(None);
        }
        let session_registry = Arc::new(crate::session_registry::SessionRegistry::default());
        let filesystem = match build_filesystem_service_with_sessions(
            config.as_ref(),
            &authority,
            Arc::clone(&worker.manager),
            Arc::clone(&session_registry),
            &readiness,
        )
        .await
        {
            Ok(filesystem) => filesystem,
            Err(error) => {
                readiness.handle.begin_shutdown().await;
                authority.shutdown().await?;
                return Err(error);
            }
        };
        let maintenance = build_maintenance(config.as_ref(), &authority, &worker, &readiness, session_registry).await;
        let worker_service = worker.service(&authority, Arc::clone(&maintenance.cleanup));
        let worker_background = build_worker_background(&worker, &worker_service);
        let (services, handles) =
            compose_services(filesystem, worker_service, readiness, worker_background, maintenance);

        let mut server = Self {
            config,
            authority,
            worker,
            services,
            handles,
        };
        if startup_shutdown.is_cancelled() {
            server.handles.begin_shutdown().await;
            server.handles.shutdown().await?;
            server.authority.shutdown().await?;
            return Ok(None);
        }

        Ok(Some(server))
    }

    /// Runs the registered RPC services while retaining runtime handles.
    pub async fn serve(
        self,
        observability: Observability,
        termination: &mut TerminationMonitor,
    ) -> Result<(), DynError> {
        let Self {
            config,
            authority,
            worker,
            services,
            mut handles,
        } = self;
        let readiness_gate = Arc::clone(&handles.readiness.gate);
        let http = match spawn_service_http(
            config.http_addr(),
            observability.observability_guard.prometheus_handle(),
            Arc::new(move || readiness_gate.is_ready()),
        ) {
            Ok(http) => http,
            Err(error) => {
                handles.begin_shutdown().await;
                handles.shutdown().await?;
                authority.shutdown().await?;
                return Err(Box::new(error));
            }
        };
        let routes = Routes::new(
            FileSystemServiceProtoServer::new(services.filesystem).max_decoding_message_size(MAX_REQUEST_SIZE),
        )
        .add_service(MetadataWorkerServiceProtoServer::new(services.worker).max_decoding_message_size(MAX_REQUEST_SIZE))
        .add_service(services.health);
        let mut rpc = match spawn_grpc_server(config.rpc_addr(), routes, None) {
            Ok(rpc) => rpc,
            Err(error) => {
                handles.begin_shutdown().await;
                let deadline = Instant::now() + Duration::from_millis(config.shutdown_timeout_ms);
                let (background_result, http_result) =
                    tokio::join!(handles.shutdown_until(deadline), http.shutdown_until(deadline),);
                let raft_result = authority.shutdown().await;
                background_result.map_err(|error| Box::new(error) as DynError)?;
                http_result.map_err(|error| Box::new(error) as DynError)?;
                raft_result.map_err(|error| Box::new(error) as DynError)?;
                return Err(Box::new(error));
            }
        };
        info!(addr = %rpc.local_addr(), "Listening on (path/filesystem + worker services)");
        let readiness_failure = handles.readiness.take_fatal();
        let mut stop_error = None;
        tokio::select! {
            signal = termination.recv() => {
                match signal {
                    Ok(signal) => info!(?signal, "Shutdown signal received"),
                    Err(error) => stop_error = Some(Box::new(error) as DynError),
                }
            }
            result = rpc.wait() => {
                stop_error = Some(match result {
                    Ok(()) => "Metadata RPC server stopped unexpectedly".into(),
                    Err(error) => Box::new(error) as DynError,
                });
            }
            error = wait_for_readiness_failure(readiness_failure) => {
                stop_error = Some(Box::new(error) as DynError);
            }
        }

        handles.begin_shutdown().await;
        let deadline = Instant::now() + Duration::from_millis(config.shutdown_timeout_ms);
        let (rpc_result, background_result, http_result) = tokio::join!(
            rpc.shutdown_until(deadline),
            handles.shutdown_until(deadline),
            http.shutdown_until(deadline),
        );
        let raft_result = authority.shutdown().await;
        let _keep_alive = (worker, observability);

        let rpc_forced = rpc_result.as_ref().copied().unwrap_or(false);
        let background_forced = background_result.as_ref().copied().unwrap_or(false);
        let http_forced = http_result.as_ref().copied().unwrap_or(false);
        if rpc_forced || background_forced || http_forced {
            tracing::warn!(
                rpc_forced,
                background_forced,
                http_forced,
                timeout_ms = config.shutdown_timeout_ms,
                "Metadata forced remaining work after the graceful drain deadline"
            );
        }

        rpc_result.map_err(|error| Box::new(error) as DynError)?;
        background_result.map_err(|error| Box::new(error) as DynError)?;
        http_result.map_err(|error| Box::new(error) as DynError)?;
        raft_result.map_err(|error| Box::new(error) as DynError)?;
        if let Some(error) = stop_error {
            return Err(error);
        }
        Ok(())
    }
}

/// Waits only for a configured fail-fast readiness error.
///
/// Normal watcher completion closes the channel and must not stop the server.
async fn wait_for_readiness_failure(failure: oneshot::Receiver<crate::MetadataError>) -> crate::MetadataError {
    match failure.await {
        Ok(error) => error,
        Err(_) => std::future::pending().await,
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
        rpc_addr = %config.rpc_addr(),
        http_addr = %config.http_addr(),
        storage_dir = %config.storage_dir.display(),
        node_id = config.raft.node_id,
        raft_mode = ?config.raft.mode,
        authority_group_name = %config.authority.group_name,
        "Configuration loaded (sensitive values redacted)"
    );

    Ok(Observability { observability_guard })
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

/// Builds the required worker runtime without starting heavy background work.
pub(crate) fn build_worker_runtime(
    authority: &MetadataAuthority,
    heartbeat_timeout_ms: u32,
) -> Result<WorkerRuntime, DynError> {
    let worker = WorkerRuntime::new(heartbeat_timeout_ms);
    worker
        .manager
        .load_registered_workers(authority.storage.list_workers()?)?;
    Ok(worker)
}

/// Starts metadata maintenance after authority and worker state exist.
///
/// `session_registry` must be the same registry owned by the filesystem service;
/// cleanup classification would otherwise miss active writes.
pub(crate) async fn build_maintenance(
    config: &MetadataConfig,
    authority: &MetadataAuthority,
    worker: &WorkerRuntime,
    _readiness: &Readiness,
    session_registry: Arc<crate::session_registry::SessionRegistry>,
) -> Maintenance {
    let cleanup = Arc::new(BlockCleanupCoordinator::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(&authority.storage),
        Arc::clone(&worker.manager),
        session_registry,
        authority.group_name.clone(),
        &config.block_cleanup,
    ));
    let detached_root_reclaimer = Arc::new(DetachedRootReclaimer::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(&authority.storage),
        config.namespace_delete.clone(),
    ));
    let maintenance_service = Arc::new(MaintenanceService::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(&worker.manager),
        Arc::clone(&cleanup),
        detached_root_reclaimer,
        Duration::from_millis(config.worker_liveness.scan_interval_ms),
    ));
    let maintenance_handle = maintenance_service.start();

    Maintenance {
        cleanup,
        _maintenance_service: maintenance_service,
        maintenance_handle,
    }
}

/// Starts worker-owned background work after authority and maintenance are available.
pub fn build_worker_background(worker: &WorkerRuntime, service: &MetadataWorkerServiceImpl) -> WorkerBackground {
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
    let watcher_health_reporter = health_reporter.clone();
    let (fatal_sender, fatal_receiver) = oneshot::channel();

    let readiness_config = config.startup.root_readiness.clone();
    let readiness_gate_clone = Arc::clone(&readiness_gate);
    let mount_table_clone = Arc::clone(&authority.mount_table);
    let raft_node_clone = Arc::clone(&authority.raft_node);
    let storage_clone = Arc::clone(&authority.storage);
    let group_name = authority.group_name.clone();
    let metrics = Arc::clone(&authority.metadata_metrics);
    let fail_fast = config.startup.root_readiness.fail_fast;
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
                watcher_health_reporter
                    .set_serving::<FileSystemServiceProtoServer<MetadataFileSystemServiceImpl>>()
                    .await;
            }
            Err(err) => {
                tracing::error!(error = %err, "Root readiness watcher failed");
                if fail_fast {
                    let _ = fatal_sender.send(err);
                }
            }
        }
    });

    Readiness {
        health_service,
        handle: ReadinessHandle {
            gate: readiness_gate,
            health_reporter,
            watcher: Some(readiness_watcher),
            fatal: Some(fatal_receiver),
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
    build_filesystem_service_with_sessions(
        config,
        authority,
        worker_manager,
        Arc::new(crate::session_registry::SessionRegistry::default()),
        readiness,
    )
    .await
}

/// Constructs the filesystem service with a caller-owned session registry.
///
/// Production startup uses this path to share active-write authority with
/// maintenance cleanup observation.
async fn build_filesystem_service_with_sessions(
    config: &MetadataConfig,
    authority: &MetadataAuthority,
    worker_manager: Arc<WorkerManager>,
    session_registry: Arc<crate::session_registry::SessionRegistry>,
    readiness: &Readiness,
) -> Result<MetadataFileSystemServiceImpl, DynError> {
    let lease_manager = Arc::new(crate::inode_lease::LeaseManager::new(
        config.write_lease_timeout_ms,
        10_000,
    ));
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

    Ok(MetadataFileSystemServiceImpl::new(
        filesystem,
        msync,
        config.namespace_list,
    ))
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
            maintenance,
            readiness,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BlockCleanupConfig, MetadataAuthorityConfig, RaftConfig, StartupConfig, WorkerLivenessConfig};
    use crate::mount::{DataIoPolicy, MountEntry, MountKind, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
    use crate::raft::Command;
    use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction};
    use beryl_common::header::{RequestHeader, ResponseHeader};
    use beryl_proto::common::BlockIdProto;
    use beryl_proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
    use beryl_proto::metadata::{CommitFileRequestProto, CommittedBlockProto, MsyncRequestProto, MsyncResponseProto};
    use beryl_types::ids::{MountId, WorkerId};
    use beryl_types::{ClientId, GroupName, GroupStateWatermark, RaftLogId, MAX_FILE_EXTENTS};
    use prost::Message;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn maximum_commit_body_fits_metadata_request_limit() {
        let committed_blocks = (0..MAX_FILE_EXTENTS)
            .map(|index| CommittedBlockProto {
                block_id: Some(BlockIdProto {
                    inode_id: u64::MAX,
                    block_index: index as u32,
                }),
                file_offset: u64::MAX,
                len: u64::MAX,
            })
            .collect();
        let request = CommitFileRequestProto {
            committed_blocks,
            final_size: u64::MAX,
            expected_content_revision: u64::MAX,
            expected_file_size: u64::MAX,
            ..Default::default()
        };

        assert!(
            request.encoded_len() < MAX_REQUEST_SIZE,
            "maximum legal CommitFile body must fit the transport request ceiling"
        );
    }

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
        MetadataFileSystemServiceImpl::new(filesystem, msync, crate::config::NamespaceListConfig::default())
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
            host: "127.0.0.1".to_string(),
            bind_host: "127.0.0.1".parse().unwrap(),
            rpc_port: 18080,
            http_port: 18081,
            storage_dir: std::path::PathBuf::from("data/metadata"),
            raft: RaftConfig::default(),
            authority: MetadataAuthorityConfig {
                group_name: GroupName::parse("root").unwrap(),
            },
            namespace_list: crate::config::NamespaceListConfig::default(),
            block_cleanup: BlockCleanupConfig::default(),
            namespace_delete: crate::config::NamespaceDeleteConfig::default(),
            worker_liveness: WorkerLivenessConfig::default(),
            startup: StartupConfig {
                root_readiness: crate::readiness::RootReadinessConfig::default(),
            },
            write_lease_timeout_ms: 60_000,
            shutdown_timeout_ms: 30_000,
            observability: test_observability_config(),
        }
    }

    fn test_observability_config() -> beryl_common::observe::ObservabilityConfig {
        let mut flat = beryl_common::config::FlatConfig::new();
        flat.set("beryl.logging.format", "compact");
        flat.set("beryl.logging.output", "stderr");
        flat.set(
            "beryl.logging.level",
            "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn",
        );
        beryl_common::observe::ObservabilityConfig::from_flat(&flat).expect("test observe config")
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

        let worker = build_worker_runtime(&authority, config.worker_liveness.heartbeat_timeout_ms).unwrap();

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
    async fn fail_fast_readiness_returns_to_lifecycle_owner_before_raft_shutdown() {
        let dir = TempDir::new().unwrap();
        let authority = test_authority(&dir).await;
        authority
            .mount_table
            .upsert(MountEntry {
                mount_id: MountId::new(1),
                mount_prefix: ROOT_MOUNT_PREFIX.to_string(),
                mount_kind: MountKind::Internal,
                ufs_uri: None,
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch: 2,
                namespace_owner_group_name: GroupName::parse("other").unwrap(),
                root_inode_id: ROOT_INODE_ID,
            })
            .unwrap();
        let mut config = test_config();
        config.startup.root_readiness = crate::readiness::RootReadinessConfig {
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
            warn_after_ms: 1,
            timeout_ms: 5,
            fail_fast: true,
        };
        let mut readiness = build_readiness(&config, &authority).await;
        let fatal = readiness.handle.take_fatal();

        let error = tokio::time::timeout(Duration::from_secs(1), fatal)
            .await
            .expect("readiness failure must be bounded")
            .expect("lifecycle owner must receive readiness failure");

        assert!(
            error.to_string().contains("root mount owner group mismatch"),
            "unexpected readiness error: {error}"
        );
        readiness.handle.begin_shutdown().await;
        assert!(!readiness.handle.gate.is_ready());
        authority.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn msync_success_on_leader_returns_authoritative_watermark() {
        let dir = TempDir::new().unwrap();
        let authority = test_authority(&dir).await;
        let expected_state_id = wait_for_leader_state(&authority).await;
        let config = test_config();
        let readiness = build_readiness(&config, &authority).await;
        let worker_runtime = build_worker_runtime(&authority, config.worker_liveness.heartbeat_timeout_ms).unwrap();
        let service = build_filesystem_service_with_sessions(
            &config,
            &authority,
            Arc::clone(&worker_runtime.manager),
            Arc::new(crate::session_registry::SessionRegistry::default()),
            &readiness,
        )
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
        let worker_runtime = build_worker_runtime(&authority, config.worker_liveness.heartbeat_timeout_ms).unwrap();
        let service = build_filesystem_service_with_sessions(
            &config,
            &authority,
            Arc::clone(&worker_runtime.manager),
            Arc::new(crate::session_registry::SessionRegistry::default()),
            &readiness,
        )
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
        let worker_runtime = build_worker_runtime(&authority, config.worker_liveness.heartbeat_timeout_ms).unwrap();
        let service = build_filesystem_service_with_sessions(
            &config,
            &authority,
            Arc::clone(&worker_runtime.manager),
            Arc::new(crate::session_registry::SessionRegistry::default()),
            &readiness,
        )
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
        let worker_runtime = build_worker_runtime(&authority, config.worker_liveness.heartbeat_timeout_ms).unwrap();
        let service = build_filesystem_service_with_sessions(
            &config,
            &authority,
            Arc::clone(&worker_runtime.manager),
            Arc::new(crate::session_registry::SessionRegistry::default()),
            &readiness,
        )
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
    async fn build_authority_uses_configured_storage_dir() {
        let configured = TempDir::new().unwrap();
        let mut config = test_config();
        config.storage_dir = configured.path().to_path_buf();
        crate::lifecycle::format_metadata_storage(&config).await.unwrap();

        let authority = build_authority(&config).await.unwrap();

        assert!(configured.path().join("CURRENT").exists());
        drop(authority);
    }
}
