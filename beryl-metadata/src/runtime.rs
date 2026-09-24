// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Runtime composition root for the metadata binary.

use crate::maintenance::{BlockCleanupCoordinator, DetachedRootReclaimer, MaintenanceHandle, MaintenanceService};
use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use crate::readiness::RootReadinessGate;
use crate::service::{MetadataFileSystem, MetadataFileSystemDeps, MetadataFileSystemServiceImpl};
use crate::worker::{MetadataWorkerServiceImpl, WorkerManager};
use crate::{observe, MetadataConfig, MountTable};
use beryl_common::grpc_server::{
    spawn_grpc_server_with_concurrency_limits, GrpcRequestConcurrencyConfig, RpcRequestClass,
};
use beryl_common::observe::{init_observability as init_common_observability, ServiceInfo};
use beryl_common::service_http::spawn_service_http;
use beryl_common::termination::TerminationMonitor;
use beryl_proto::metadata::file_system_service_proto_server::FileSystemServiceProtoServer;
use beryl_proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProtoServer;
use beryl_types::GroupName;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::server::NamedService;
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
/// Period between bounded leader-local write-session expiry passes.
const WRITE_SESSION_EXPIRY_SCAN_INTERVAL: Duration = Duration::from_secs(1);

pub type DynError = Box<dyn std::error::Error>;

type MetadataHealthServer = HealthServer<HealthService>;

/// Classifies raw gRPC paths without decoding request bodies.
///
/// Unknown services remain regular traffic so only explicitly named internal
/// control services can consume reserved capacity.
fn classify_metadata_rpc(path: &str) -> RpcRequestClass {
    let service_name = path
        .strip_prefix('/')
        .and_then(|path| path.split_once('/'))
        .map(|(service, _)| service);
    if service_name == Some(<MetadataWorkerServiceProtoServer<MetadataWorkerServiceImpl> as NamedService>::NAME)
        || service_name == Some(<MetadataHealthServer as NamedService>::NAME)
    {
        RpcRequestClass::Control
    } else {
        RpcRequestClass::Regular
    }
}

/// Authoritative metadata dependencies built before public services are exposed.
struct MetadataAuthority {
    storage: Arc<RocksDBStorage>,
    mount_table: Arc<MountTable>,
    raft_node: Arc<AppRaftNode>,
    group_name: GroupName,
}

impl MetadataAuthority {
    /// Stop the authority's Raft runtime.
    async fn shutdown(&self) -> crate::MetadataResult<()> {
        self.raft_node.shutdown().await
    }
}

/// Readiness gate and health service state.
struct Readiness {
    health_service: MetadataHealthServer,
    handle: ReadinessHandle,
}

/// Readiness ownership retained for request guards and shutdown.
struct ReadinessHandle {
    gate: Arc<RootReadinessGate>,
    health_reporter: HealthReporter,
}

/// Services registered on the tonic server.
struct RpcServices {
    filesystem: MetadataFileSystemServiceImpl,
    worker: MetadataWorkerServiceImpl,
    health: MetadataHealthServer,
}

/// Long-lived handles retained by `serve()` for the server lifetime.
struct RuntimeHandles {
    maintenance: MaintenanceHandle,
    readiness: ReadinessHandle,
}

impl ReadinessHandle {
    /// Closes request admission before publishing the health shutdown state.
    async fn begin_shutdown(&mut self) {
        self.gate.begin_shutdown();
        self.health_reporter
            .set_not_serving::<FileSystemServiceProtoServer<MetadataFileSystemServiceImpl>>()
            .await;
    }
}

impl Drop for ReadinessHandle {
    fn drop(&mut self) {
        self.gate.begin_shutdown();
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

/// Final server composition object for metadata.
pub struct MetadataServer {
    config: Arc<MetadataConfig>,
    authority: MetadataAuthority,
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
        if startup_shutdown.is_cancelled() {
            return Ok(None);
        }
        let authority = build_authority(config.as_ref()).await?;
        if startup_shutdown.is_cancelled() {
            authority.shutdown().await?;
            return Ok(None);
        }
        let worker = match build_worker_manager(&authority, config.worker_liveness.heartbeat_timeout_ms) {
            Ok(worker) => worker,
            Err(error) => {
                authority.shutdown().await?;
                return Err(error);
            }
        };
        let mut readiness = build_readiness().await;
        if startup_shutdown.is_cancelled() {
            readiness.handle.begin_shutdown().await;
            authority.shutdown().await?;
            return Ok(None);
        }
        let write_sessions = config.write_session_limits;
        let write_targets = config.write_target_limits;
        let session_registry = Arc::new(crate::session_registry::SessionRegistry::new(
            write_sessions.max_active,
            write_sessions.max_active_per_client,
            write_targets.max_outstanding,
            write_targets.max_outstanding_per_session,
            config.write_lease_timeout_ms,
        ));
        let filesystem = match build_filesystem_service(
            config.as_ref(),
            &authority,
            Arc::clone(&worker),
            Arc::clone(&session_registry),
            &readiness,
        ) {
            Ok(filesystem) => filesystem,
            Err(error) => {
                readiness.handle.begin_shutdown().await;
                authority.shutdown().await?;
                return Err(error);
            }
        };
        let (cleanup, maintenance) = build_maintenance(config.as_ref(), &authority, &worker, session_registry);
        let worker_service = MetadataWorkerServiceImpl::new(
            Arc::clone(&authority.raft_node),
            Arc::clone(&worker),
            authority.group_name.clone(),
            cleanup,
        );
        let (services, handles) = compose_services(filesystem, worker_service, readiness, maintenance);

        let mut server = Self {
            config,
            authority,
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
        prometheus_handle: PrometheusHandle,
        termination: &mut TerminationMonitor,
    ) -> Result<(), DynError> {
        let Self {
            config,
            authority,
            services,
            mut handles,
        } = self;
        let readiness_gate = Arc::clone(&handles.readiness.gate);
        let http = match spawn_service_http(
            config.http_addr(),
            prometheus_handle,
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
        let rpc_concurrency = config.rpc_concurrency;
        let mut rpc = match spawn_grpc_server_with_concurrency_limits(
            config.rpc_addr(),
            routes,
            GrpcRequestConcurrencyConfig {
                server_name: "metadata",
                max_concurrent_requests: rpc_concurrency.max_concurrent_requests,
                max_concurrent_requests_per_connection: rpc_concurrency.max_concurrent_requests_per_connection,
                reserved_control_requests: rpc_concurrency.reserved_control_requests,
                classify: classify_metadata_rpc,
            },
        ) {
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
        }

        handles.begin_shutdown().await;
        let deadline = Instant::now() + Duration::from_millis(config.shutdown_timeout_ms);
        let (rpc_result, background_result, http_result) = tokio::join!(
            rpc.shutdown_until(deadline),
            handles.shutdown_until(deadline),
            http.shutdown_until(deadline),
        );
        let raft_result = authority.shutdown().await;

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

/// Initializes process-wide observability after configuration has been loaded.
pub fn init_observability(config: &MetadataConfig) -> Result<PrometheusHandle, DynError> {
    let obs_config = config.observability.clone();
    let service_info = ServiceInfo {
        name: "metadata".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        environment: "development".to_string(),
    };
    let prometheus_handle = init_common_observability(&obs_config, service_info)?;
    observe::record_metadata_started("metadata", env!("CARGO_PKG_VERSION"));

    info!(
        event = "metadata_configuration_loaded",
        rpc_addr = %config.rpc_addr(),
        http_addr = %config.http_addr(),
        storage_dir = %config.storage_dir.display(),
        node_id = config.raft.node_id,
        authority_group_name = %config.authority.group_name,
        "Configuration loaded (sensitive values redacted)"
    );

    Ok(prometheus_handle)
}

/// Builds authoritative storage, mount, and Raft dependencies in startup order.
async fn build_authority(config: &MetadataConfig) -> Result<MetadataAuthority, DynError> {
    let (storage, mount_table) = crate::lifecycle::open_metadata_storage(config)?;
    let storage = Arc::new(storage);
    let mount_table = Arc::new(mount_table);
    let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));

    let raft_node = Arc::new(
        AppRaftNode::new(
            config.raft.node_id,
            Arc::clone(&storage),
            state_machine,
            Arc::clone(&mount_table),
        )
        .await
        .map_err(|e| format!("Failed to initialize Raft node: {e}"))?,
    );

    Ok(MetadataAuthority {
        storage,
        mount_table,
        raft_node,
        group_name: config.authority.group_name.clone(),
    })
}

/// Builds the required worker runtime without starting heavy background work.
fn build_worker_manager(
    authority: &MetadataAuthority,
    heartbeat_timeout_ms: u32,
) -> Result<Arc<WorkerManager>, DynError> {
    let worker = Arc::new(WorkerManager::new(heartbeat_timeout_ms));
    worker.load_registered_workers(authority.storage.list_workers()?);
    Ok(worker)
}

/// Starts metadata maintenance after authority and worker state exist.
///
/// `session_registry` must be the same registry owned by the filesystem service;
/// cleanup classification would otherwise miss active writes.
fn build_maintenance(
    config: &MetadataConfig,
    authority: &MetadataAuthority,
    worker: &Arc<WorkerManager>,
    session_registry: Arc<crate::session_registry::SessionRegistry>,
) -> (Arc<BlockCleanupCoordinator>, MaintenanceHandle) {
    let cleanup = Arc::new(BlockCleanupCoordinator::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(&authority.storage),
        Arc::clone(worker),
        Arc::clone(&session_registry),
        authority.group_name.clone(),
        &config.block_cleanup,
    ));
    let detached_root_reclaimer = Arc::new(DetachedRootReclaimer::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(&authority.storage),
        config.namespace_delete.clone(),
    ));
    let maintenance_service = MaintenanceService::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(worker),
        Arc::clone(&cleanup),
        detached_root_reclaimer,
        Duration::from_millis(config.worker_liveness.scan_interval_ms),
        session_registry,
        WRITE_SESSION_EXPIRY_SCAN_INTERVAL,
    );
    let maintenance_handle = maintenance_service.start();

    (cleanup, maintenance_handle)
}

/// Publish readiness only after the retained authority has passed startup validation.
async fn build_readiness() -> Readiness {
    let health_reporter = HealthReporter::new();
    health_reporter
        .set_serving::<FileSystemServiceProtoServer<MetadataFileSystemServiceImpl>>()
        .await;
    let gate = Arc::new(RootReadinessGate::new());
    let health_service = HealthServer::new(HealthService::from_health_reporter(health_reporter.clone()));
    Readiness {
        health_service,
        handle: ReadinessHandle { gate, health_reporter },
    }
}

impl Readiness {
    fn gate(&self) -> Arc<RootReadinessGate> {
        Arc::clone(&self.handle.gate)
    }
}

/// Constructs the filesystem service with a caller-owned session registry.
///
/// Production startup uses this path to share active-write authority with
/// maintenance cleanup observation.
fn build_filesystem_service(
    config: &MetadataConfig,
    authority: &MetadataAuthority,
    worker_manager: Arc<WorkerManager>,
    session_registry: Arc<crate::session_registry::SessionRegistry>,
    readiness: &Readiness,
) -> Result<MetadataFileSystemServiceImpl, DynError> {
    // Revalidate public mutable configuration at the runtime construction boundary.
    let file_block_size = config.file_block_size;
    beryl_types::validate_block_size(u64::from(file_block_size))?;
    let filesystem = Arc::new(MetadataFileSystem::new(MetadataFileSystemDeps {
        mount_table: Arc::clone(&authority.mount_table),
        storage: Arc::clone(&authority.storage),
        raft_node: Arc::clone(&authority.raft_node),
        session_registry,
        worker_manager,
        readiness_gate: readiness.gate(),
        file_block_size,
    }));

    Ok(MetadataFileSystemServiceImpl::new(
        filesystem,
        Arc::clone(&authority.raft_node),
        authority.group_name.clone(),
        config.namespace_list,
    ))
}

/// Separates RPC service values from lifecycle handles before entering server code.
fn compose_services(
    filesystem: MetadataFileSystemServiceImpl,
    worker: MetadataWorkerServiceImpl,
    readiness: Readiness,
    maintenance: MaintenanceHandle,
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
        RuntimeHandles { maintenance, readiness },
    )
}
