// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use client::{ClientConfig, FsClient};
use common::observe::ObservabilityConfig;
use common::FlatConfig;
use metadata::config::{
    BootstrapConfig, MetadataAuthorityConfig, MetadataAuthzConfig, MetadataConfig, RaftConfig, WorkerConfig,
};
use metadata::lifecycle::format_metadata_storage;
use metadata::runtime::{build_authority, build_filesystem_service, build_readiness};
use metadata::worker::{MetadataWorkerServiceImpl, WorkerManager};
use tokio::net::TcpListener;
use types::{GroupName, Tier, WorkerId, WorkerRunId};
use worker::config::{
    StoreDirConfig, WorkerConfig as WorkerServiceConfig, WorkerRegistrationConfig, WorkerStoreConfig,
};
use worker::control::{prepare_worker_start, MetadataBlockReportLoop, MetadataHeartbeatLoop, MetadataRegistrar};
use worker::net::config::WorkerNetConfig;
use worker::store::dirs::StoreDirs;
use worker::WorkerCore;

use crate::ports::PortReservation;
use crate::readiness;
use crate::services::{MetadataServiceInstance, WorkerServiceInstance};
use crate::temp_state::TempState;
use crate::TestResult;

const GROUP_NAME: &str = "root";
const CLUSTER_ID: &str = "local-vecton-e2e";

pub struct TestCluster {
    _temp_state: TempState,
    client: FsClient,
    group_name: GroupName,
    worker_id: WorkerId,
    worker_addr: SocketAddr,
    metadata_addr: SocketAddr,
    metadata_config: MetadataConfig,
    worker_config: WorkerServiceConfig,
    worker_manager: Arc<WorkerManager>,
    registrar: MetadataRegistrar,
    registration_state: Arc<worker::control::RegistrationSet>,
    block_report: MetadataBlockReportLoop,
    heartbeat: MetadataHeartbeatLoop,
    block_store: Arc<StoreDirs>,
    metadata_server: MetadataServiceInstance,
    worker_server: WorkerServiceInstance,
}

impl TestCluster {
    pub async fn start() -> TestResult<Self> {
        let temp_state = TempState::new()?;
        let group_name = GroupName::parse(GROUP_NAME)?;
        let metadata_port = PortReservation::reserve_localhost().await?;
        let metadata_addr = metadata_port.addr();
        let worker_port = PortReservation::reserve_localhost().await?;
        let worker_addr = worker_port.addr();

        let metadata_config = metadata_config(temp_state.metadata_dir(), metadata_addr, group_name.clone())?;
        format_metadata_storage(&metadata_config).await?;
        let (metadata_server, worker_manager) =
            start_metadata_instance(&metadata_config, metadata_port.into_listener(), &group_name).await?;

        let client = client_for(metadata_addr, group_name.clone())?;
        readiness::wait_for_metadata_filesystem(&client).await?;

        let worker_config = worker_config(temp_state.worker_root(), worker_addr, metadata_addr, group_name.clone())?;
        let worker = start_worker_instance(&worker_config, worker_port.into_listener())?;

        worker.registrar.register_once().await?;
        readiness::wait_for_worker_registration(
            &worker.registration_state,
            &worker_manager,
            &group_name,
            worker.worker_id,
        )
        .await?;

        readiness::send_heartbeat(&worker.heartbeat, &worker.block_store).await?;
        readiness::wait_for_worker_heartbeat(
            &worker.registration_state,
            &worker_manager,
            &group_name,
            worker.worker_id,
        )
        .await?;

        let cluster = Self {
            _temp_state: temp_state,
            client,
            group_name,
            worker_id: worker.worker_id,
            worker_addr,
            metadata_addr,
            metadata_config,
            worker_config,
            worker_manager,
            registrar: worker.registrar,
            registration_state: worker.registration_state,
            block_report: worker.block_report,
            heartbeat: worker.heartbeat,
            block_store: worker.block_store,
            metadata_server,
            worker_server: worker.worker_server,
        };
        cluster.converge_block_reports().await?;
        Ok(cluster)
    }

    pub fn client(&self) -> &FsClient {
        &self.client
    }

    pub fn metadata_endpoint(&self) -> String {
        format!("http://{}", self.metadata_addr)
    }

    pub fn ready_block_count(&self) -> TestResult<usize> {
        Ok(self.block_store.scan_group_blocks(&self.group_name)?.len())
    }

    pub fn current_worker_run_id(&self) -> Option<WorkerRunId> {
        self.registration_state
            .registration_for_group(&self.group_name)
            .map(|registration| registration.worker_run_id)
    }

    pub async fn restart_worker(&mut self) -> TestResult<()> {
        self.restart_worker_until_heartbeat().await?;
        self.converge_block_reports().await
    }

    pub async fn restart_worker_until_heartbeat(&mut self) -> TestResult<()> {
        self.worker_server.shutdown().await?;
        let listener = TcpListener::bind(self.worker_addr).await?;
        let worker = start_worker_instance(&self.worker_config, listener)?;
        let worker_id = worker.worker_id;

        self.worker_id = worker_id;
        self.registrar = worker.registrar;
        self.registration_state = worker.registration_state;
        self.block_report = worker.block_report;
        self.heartbeat = worker.heartbeat;
        self.block_store = worker.block_store;
        self.worker_server = worker.worker_server;

        self.registrar.register_once().await?;
        readiness::wait_for_worker_registration(
            &self.registration_state,
            &self.worker_manager,
            &self.group_name,
            worker_id,
        )
        .await?;
        readiness::send_heartbeat(&self.heartbeat, &self.block_store).await?;
        readiness::wait_for_worker_heartbeat(
            &self.registration_state,
            &self.worker_manager,
            &self.group_name,
            worker_id,
        )
        .await
    }

    pub async fn restart_metadata(&mut self) -> TestResult<()> {
        self.metadata_server.shutdown().await?;
        let listener = TcpListener::bind(self.metadata_addr).await?;
        let (metadata_server, worker_manager) =
            start_metadata_instance(&self.metadata_config, listener, &self.group_name).await?;
        self.metadata_server = metadata_server;
        self.worker_manager = worker_manager;

        readiness::wait_for_metadata_filesystem(&self.client).await?;
        self.registration_state.mark_needs_register(&self.group_name);
        self.registrar.register_once().await?;
        readiness::wait_for_worker_registration(
            &self.registration_state,
            &self.worker_manager,
            &self.group_name,
            self.worker_id,
        )
        .await?;
        readiness::send_heartbeat(&self.heartbeat, &self.block_store).await?;
        readiness::wait_for_worker_heartbeat(
            &self.registration_state,
            &self.worker_manager,
            &self.group_name,
            self.worker_id,
        )
        .await?;
        self.converge_block_reports().await
    }

    pub async fn converge_block_reports(&self) -> TestResult<()> {
        readiness::converge_block_reports(
            &self.heartbeat,
            &self.block_report,
            &self.block_store,
            &self.registration_state,
            &self.worker_manager,
            &self.group_name,
            self.worker_id,
        )
        .await
    }

    pub async fn shutdown(&mut self) -> TestResult<()> {
        self.worker_server.shutdown().await?;
        self.metadata_server.shutdown().await?;
        Ok(())
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        self.worker_server.abort();
        self.metadata_server.abort();
    }
}

async fn start_metadata_instance(
    metadata_config: &MetadataConfig,
    listener: TcpListener,
    group_name: &GroupName,
) -> TestResult<(MetadataServiceInstance, Arc<WorkerManager>)> {
    let authority = build_authority(metadata_config)
        .await
        .map_err(|err| io::Error::other(err.to_string()))?;
    let worker_manager = Arc::new(WorkerManager::new(60));
    worker_manager.reset_worker_soft_state();
    authority.raft_node.set_worker_manager(Arc::clone(&worker_manager))?;
    let readiness_state = build_readiness(metadata_config, &authority).await;
    let filesystem = build_filesystem_service(
        metadata_config,
        &authority,
        Arc::clone(&worker_manager),
        &readiness_state,
    )
    .await
    .map_err(|err| io::Error::other(err.to_string()))?;
    let worker_control = MetadataWorkerServiceImpl::new(
        Arc::clone(&authority.raft_node),
        Arc::clone(&worker_manager),
        Arc::clone(&authority.mount_table),
        group_name.clone(),
    );
    let metadata_server =
        MetadataServiceInstance::start(listener, filesystem, worker_control, Arc::clone(&authority.raft_node));
    Ok((metadata_server, worker_manager))
}

struct StartedWorkerService {
    worker_id: WorkerId,
    registrar: MetadataRegistrar,
    registration_state: Arc<worker::control::RegistrationSet>,
    block_report: MetadataBlockReportLoop,
    heartbeat: MetadataHeartbeatLoop,
    block_store: Arc<StoreDirs>,
    worker_server: WorkerServiceInstance,
}

fn start_worker_instance(
    worker_config: &WorkerServiceConfig,
    listener: TcpListener,
) -> TestResult<StartedWorkerService> {
    std::fs::create_dir_all(worker_config.identity_path.parent().expect("identity path has parent"))?;
    let worker_id = prepare_worker_start(worker_config)?;
    let registration_state = readiness::shared_registration_state();
    let descriptor = MetadataRegistrar::descriptor_from_config(worker_config, worker_id)?;
    let registrar = MetadataRegistrar::new(
        worker_config.metadata.clone(),
        descriptor.clone(),
        Arc::clone(&registration_state),
    )?;
    let heartbeat = MetadataHeartbeatLoop::new(
        worker_config.metadata.clone(),
        descriptor.clone(),
        Arc::clone(&registration_state),
    )?;
    let block_store = Arc::new(StoreDirs::open(
        worker_config.store.dirs.clone(),
        worker_config.store.reserve_space_bytes,
        worker_config.store.check_interval_ms,
    )?);
    let block_report = MetadataBlockReportLoop::new(
        worker_config.metadata.clone(),
        descriptor,
        Arc::clone(&registration_state),
        Arc::clone(&block_store),
    )?;
    let worker_core = Arc::new(WorkerCore::with_local_store(
        worker_config.default_frame_size,
        worker_config.max_frame_size,
        worker_config.window_bytes,
        Duration::from_millis(worker_config.stream_idle_timeout_ms),
        Arc::clone(&block_store) as Arc<dyn worker::store::block::LocalBlockStore + Send + Sync>,
    ));
    let worker_server = WorkerServiceInstance::start(listener, worker_core, Arc::clone(&registration_state));
    Ok(StartedWorkerService {
        worker_id,
        registrar,
        registration_state,
        block_report,
        heartbeat,
        block_store,
        worker_server,
    })
}

fn metadata_config(
    storage_dir: std::path::PathBuf,
    rpc_addr: SocketAddr,
    group_name: GroupName,
) -> TestResult<MetadataConfig> {
    Ok(MetadataConfig {
        cluster_id: CLUSTER_ID.to_string(),
        rpc_addr,
        storage_dir,
        authz: MetadataAuthzConfig::default(),
        raft: RaftConfig::default(),
        authority: MetadataAuthorityConfig { group_name },
        worker: WorkerConfig::default(),
        bootstrap: BootstrapConfig {
            root_readiness: metadata::RootReadinessConfig {
                initial_backoff_ms: 10,
                max_backoff_ms: 100,
                warn_after_ms: 1_000,
                timeout_ms: 10_000,
                fail_fast: false,
            },
        },
        observability: observability_config()?,
    })
}

fn worker_config(
    root: std::path::PathBuf,
    rpc_addr: SocketAddr,
    metadata_addr: SocketAddr,
    group_name: GroupName,
) -> TestResult<WorkerServiceConfig> {
    let store_dir = root.join("hdd0");
    let identity_path = root.join("worker.identity");
    let mut dirs = BTreeMap::new();
    dirs.insert(
        "hdd0".to_string(),
        StoreDirConfig {
            path: store_dir,
            tier: Tier::Hdd,
            capacity_bytes: 64 * 1024 * 1024,
        },
    );
    let rpc_endpoint = format!("http://{rpc_addr}");
    let config = WorkerServiceConfig {
        cluster_id: CLUSTER_ID.to_string(),
        identity_path,
        rpc_bind: rpc_addr.to_string(),
        rpc_advertised_endpoint: rpc_endpoint,
        rpc_max_inflight: 100,
        default_frame_size: 1024 * 1024,
        max_frame_size: 4 * 1024 * 1024,
        window_bytes: 8 * 1024 * 1024,
        stream_idle_timeout_ms: 60_000,
        store: WorkerStoreConfig {
            dirs,
            reserve_space_bytes: 0,
            selection_policy: "round_robin".to_string(),
            check_interval_ms: 30_000,
        },
        net: WorkerNetConfig::grpc_from_rpc(rpc_addr.to_string(), 100, 4 * 1024 * 1024),
        metadata: WorkerRegistrationConfig {
            group_name,
            endpoints: vec![format!("http://{metadata_addr}")],
            register_timeout_ms: 2_000,
            register_retry_initial_backoff_ms: 10,
            register_retry_max_backoff_ms: 100,
        },
        observability: observability_config()?,
    };
    config.validate()?;
    Ok(config)
}

fn client_for(metadata_addr: SocketAddr, group_name: GroupName) -> TestResult<FsClient> {
    let mut flat = FlatConfig::new();
    flat.set("client.name", "local_crud_e2e");
    flat.set("client.metadata.group.names", group_name.as_str());
    flat.set(
        &format!("client.metadata.group.{}.endpoints", group_name.as_str()),
        metadata_addr.to_string(),
    );
    flat.set("client.retry.max_retry_attempts", 3i64);
    flat.set("client.refresh.max_attempts", 3i64);
    flat.set("client.operation.timeout_ms", 2_000i64);
    flat.set("client.backoff.initial_ms", 10i64);
    flat.set("client.backoff.max_ms", 100i64);
    flat.set("client.backoff.multiplier", "2.0");
    Ok(FsClient::try_new(ClientConfig::from_flat(flat)?)?)
}

fn observability_config() -> Result<ObservabilityConfig, common::CommonError> {
    let mut flat = FlatConfig::new();
    flat.set("observe.log.format", "compact");
    flat.set("observe.log.output", "stderr");
    flat.set("observe.log.level", "warn");
    flat.set("observe.metrics.prometheus.bind", "127.0.0.1:0");
    flat.set("observe.metrics.prometheus.path", "/metrics");
    ObservabilityConfig::from_flat(&flat)
}
