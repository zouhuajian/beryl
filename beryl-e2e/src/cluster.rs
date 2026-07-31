// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use beryl_client::{ClientConfig, FsClient};
use beryl_common::observe::ObservabilityConfig;
use beryl_common::FlatConfig;
use beryl_metadata::config::{
    BootstrapConfig, CleanupConfig, DetachedRootReclamationConfig, MetadataAuthorityConfig, MetadataConfig, RaftConfig,
    WorkerConfig,
};
use beryl_metadata::lifecycle::format_metadata_storage;
use beryl_metadata::lifecycle::prepare_metadata_start;
use beryl_metadata::runtime::{build_authority, build_filesystem_service, build_readiness};
use beryl_metadata::worker::WorkerManager;
use beryl_types::{GroupName, Tier, WorkerId, WorkerRunId};
use beryl_worker::config::{
    StoreDirConfig, WorkerConfig as WorkerServiceConfig, WorkerRegistrationConfig, WorkerStoreConfig,
};
use beryl_worker::control::{
    prepare_worker_start, BlockCleanupExecutor, BlockCleanupOptions, MetadataBlockReportLoop, MetadataHeartbeatLoop,
    MetadataRegistrar,
};
use beryl_worker::net::config::WorkerNetConfig;
use beryl_worker::store::dirs::StoreDirs;
use beryl_worker::WorkerCore;
use tokio::net::TcpListener;

use crate::ports::PortReservation;
use crate::readiness;
use crate::services::{MetadataProcessInstance, MetadataServiceInstance, WorkerServiceInstance};
use crate::temp_state::TempState;
use crate::TestResult;

const GROUP_NAME: &str = "root";
const CLUSTER_ID: &str = "local-beryl-e2e";

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
    registration_state: Arc<beryl_worker::control::RegistrationSet>,
    block_report: Arc<MetadataBlockReportLoop>,
    background_block_report: Option<tokio::task::JoinHandle<()>>,
    heartbeat: MetadataHeartbeatLoop,
    block_store: Arc<StoreDirs>,
    metadata_server: MetadataServiceInstance,
    metadata_process: Option<MetadataProcessInstance>,
    worker_server: WorkerServiceInstance,
    additional_workers: Vec<StartedWorkerService>,
}

impl TestCluster {
    pub async fn start() -> TestResult<Self> {
        Self::start_with_cleanup_grace(None).await
    }

    /// Starts a cluster with short cleanup timing for lifecycle tests.
    pub async fn start_with_cleanup() -> TestResult<Self> {
        Self::start_with_cleanup_grace(Some(1)).await
    }

    async fn start_with_cleanup_grace(reclaim_grace_ms: Option<u64>) -> TestResult<Self> {
        let temp_state = TempState::new()?;
        let group_name = GroupName::parse(GROUP_NAME)?;
        let metadata_port = PortReservation::reserve_localhost().await?;
        let metadata_addr = metadata_port.addr();
        let worker_port = PortReservation::reserve_localhost().await?;
        let worker_addr = worker_port.addr();

        let mut metadata_config = metadata_config(temp_state.metadata_dir(), metadata_addr, group_name.clone())?;
        if let Some(reclaim_grace_ms) = reclaim_grace_ms {
            metadata_config.cleanup.scan_interval_ms = 20;
            metadata_config.cleanup.reclaim_grace_ms = reclaim_grace_ms;
            metadata_config.cleanup.retry_initial_backoff_ms = 20;
            metadata_config.cleanup.retry_max_backoff_ms = 100;
        }
        format_metadata_storage(&metadata_config).await?;
        let (metadata_server, worker_manager) =
            start_metadata_instance(&metadata_config, metadata_port.into_listener()).await?;

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

        let mut cluster = Self {
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
            background_block_report: None,
            heartbeat: worker.heartbeat,
            block_store: worker.block_store,
            metadata_server,
            metadata_process: None,
            worker_server: worker.worker_server,
            additional_workers: Vec::new(),
        };
        cluster.converge_block_reports().await?;
        cluster.start_background_block_reports();
        Ok(cluster)
    }

    pub fn client(&self) -> &FsClient {
        &self.client
    }

    /// Start a bounded E2E reporter so write RPCs exercise the asynchronous
    /// Worker-to-Metadata report path without manual convergence.
    pub fn start_background_block_reports(&mut self) {
        if self.background_block_report.is_some() {
            return;
        }
        let mut block_reports = vec![Arc::clone(&self.block_report)];
        block_reports.extend(
            self.additional_workers
                .iter()
                .map(|worker| Arc::clone(&worker.block_report)),
        );
        let group_name = self.group_name.clone();
        self.background_block_report = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                interval.tick().await;
                for block_report in &block_reports {
                    if block_report.has_delta_baseline(&group_name) {
                        let _ = block_report.send_delta_once().await;
                    } else {
                        let _ = block_report.send_full_once().await;
                    }
                }
            }
        }));
    }

    pub async fn stop_background_block_reports(&mut self) {
        if let Some(task) = self.background_block_report.take() {
            task.abort();
            let _ = task.await;
        }
    }

    pub fn metadata_endpoint(&self) -> String {
        format!("http://{}", self.metadata_addr)
    }

    pub fn ready_block_count(&self) -> TestResult<usize> {
        let primary = self.block_store.scan_group_blocks(&self.group_name)?.len();
        self.additional_workers.iter().try_fold(primary, |count, worker| {
            Ok(count + worker.block_store.scan_group_blocks(&self.group_name)?.len())
        })
    }

    /// Returns blocks whose local reclamation has fully updated store accounting.
    pub fn physical_block_count(&self) -> TestResult<usize> {
        let primary = store_report_block_count(&self.block_store)?;
        self.additional_workers.iter().try_fold(primary, |count, worker| {
            Ok(count + store_report_block_count(&worker.block_store)?)
        })
    }

    pub fn current_worker_run_id(&self) -> Option<WorkerRunId> {
        self.registration_state
            .registration_for_group(&self.group_name)
            .map(|registration| registration.worker_run_id)
    }

    pub fn current_worker_run_ids(&self) -> Vec<WorkerRunId> {
        let mut run_ids = self.current_worker_run_id().into_iter().collect::<Vec<_>>();
        run_ids.extend(self.additional_workers.iter().filter_map(|worker| {
            worker
                .registration_state
                .registration_for_group(&self.group_name)
                .map(|registration| registration.worker_run_id)
        }));
        run_ids
    }

    pub async fn start_additional_worker(&mut self) -> TestResult<WorkerId> {
        let worker_port = PortReservation::reserve_localhost().await?;
        let worker_addr = worker_port.addr();
        let worker_root = self
            ._temp_state
            .root()
            .join(format!("worker-extra-{}", self.additional_workers.len() + 1));
        let config = worker_config(worker_root, worker_addr, self.metadata_addr, self.group_name.clone())?;
        let worker = start_worker_instance(&config, worker_port.into_listener())?;
        if worker.worker_id == self.worker_id {
            return Err("additional worker reused the primary worker ID".into());
        }
        worker.registrar.register_once().await?;
        readiness::wait_for_worker_registration(
            &worker.registration_state,
            &self.worker_manager,
            &self.group_name,
            worker.worker_id,
        )
        .await?;
        readiness::send_heartbeat(&worker.heartbeat, &worker.block_store).await?;
        readiness::wait_for_worker_heartbeat(
            &worker.registration_state,
            &self.worker_manager,
            &self.group_name,
            worker.worker_id,
        )
        .await?;
        readiness::converge_block_reports(
            &worker.heartbeat,
            &worker.block_report,
            &worker.block_store,
            &worker.registration_state,
            &self.worker_manager,
            &self.group_name,
            worker.worker_id,
        )
        .await?;
        let worker_id = worker.worker_id;
        self.additional_workers.push(worker);
        if self.background_block_report.is_some() {
            self.stop_background_block_reports().await;
            self.start_background_block_reports();
        }
        Ok(worker_id)
    }

    pub async fn restart_worker(&mut self) -> TestResult<()> {
        let restart_background = self.background_block_report.is_some();
        self.restart_worker_until_heartbeat().await?;
        let result = self.converge_block_reports().await;
        if restart_background {
            self.start_background_block_reports();
        }
        result
    }

    pub async fn restart_worker_until_heartbeat(&mut self) -> TestResult<()> {
        self.stop_background_block_reports().await;
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
        let restart_background = self.background_block_report.is_some();
        self.stop_background_block_reports().await;
        self.metadata_server.shutdown().await?;
        let result = self.start_metadata_from_disk().await;
        if restart_background && result.is_ok() {
            self.start_background_block_reports();
        }
        result
    }

    pub async fn start_metadata_process(&mut self, executable: &std::path::Path) -> TestResult<()> {
        if self.metadata_process.is_some() {
            return Err("metadata child process is already running".into());
        }
        let restart_background = self.background_block_report.is_some();
        self.stop_background_block_reports().await;
        self.metadata_server.shutdown().await?;
        let result = self.start_metadata_child(executable).await;
        if restart_background && result.is_ok() {
            self.start_background_block_reports();
        }
        result
    }

    /// Restarts the full metadata process while preserving its durable storage.
    pub async fn restart_metadata_process(&mut self, executable: &std::path::Path) -> TestResult<()> {
        let restart_background = self.background_block_report.is_some();
        self.stop_background_block_reports().await;
        let process = self
            .metadata_process
            .take()
            .ok_or("metadata child process is not running")?;
        process.kill().await?;
        let result = self.start_metadata_child(executable).await;
        if restart_background && result.is_ok() {
            self.start_background_block_reports();
        }
        result
    }

    async fn start_metadata_child(&mut self, executable: &std::path::Path) -> TestResult<()> {
        let metrics_port = PortReservation::reserve_localhost().await?;
        let metrics_addr = metrics_port.addr();
        let config_path = self.write_metadata_process_config(metrics_addr)?;
        drop(metrics_port);
        self.metadata_process = Some(MetadataProcessInstance::start(executable, &config_path)?);
        if let Err(error) = readiness::wait_for_metadata_filesystem(&self.client).await {
            if let Some(mut process) = self.metadata_process.take() {
                process.abort();
            }
            return Err(error);
        }
        self.register_workers_with_external_metadata().await
    }

    pub async fn kill_metadata_process_and_restart(&mut self) -> TestResult<()> {
        let restart_background = self.background_block_report.is_some();
        self.stop_background_block_reports().await;
        let process = self
            .metadata_process
            .take()
            .ok_or("metadata child process is not running")?;
        process.kill().await?;
        let result = self.start_metadata_from_disk().await;
        if restart_background && result.is_ok() {
            self.start_background_block_reports();
        }
        result
    }

    async fn start_metadata_from_disk(&mut self) -> TestResult<()> {
        let listener = TcpListener::bind(self.metadata_addr).await?;
        let (metadata_server, worker_manager) = start_metadata_instance(&self.metadata_config, listener).await?;
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
        for worker in &self.additional_workers {
            worker.registration_state.mark_needs_register(&self.group_name);
            worker.registrar.register_once().await?;
            readiness::wait_for_worker_registration(
                &worker.registration_state,
                &self.worker_manager,
                &self.group_name,
                worker.worker_id,
            )
            .await?;
            readiness::send_heartbeat(&worker.heartbeat, &worker.block_store).await?;
            readiness::wait_for_worker_heartbeat(
                &worker.registration_state,
                &self.worker_manager,
                &self.group_name,
                worker.worker_id,
            )
            .await?;
        }
        self.converge_block_reports().await
    }

    pub async fn converge_block_reports(&mut self) -> TestResult<()> {
        let restart_background = self.background_block_report.is_some();
        self.stop_background_block_reports().await;
        let result = async {
            if self.metadata_process.is_some() {
                send_full_block_report_to_external_metadata(&self.heartbeat, &self.block_report, &self.block_store)
                    .await?;
                for worker in &self.additional_workers {
                    send_full_block_report_to_external_metadata(
                        &worker.heartbeat,
                        &worker.block_report,
                        &worker.block_store,
                    )
                    .await?;
                }
                return Ok(());
            }

            readiness::converge_block_reports(
                &self.heartbeat,
                &self.block_report,
                &self.block_store,
                &self.registration_state,
                &self.worker_manager,
                &self.group_name,
                self.worker_id,
            )
            .await?;
            for worker in &self.additional_workers {
                readiness::converge_block_reports(
                    &worker.heartbeat,
                    &worker.block_report,
                    &worker.block_store,
                    &worker.registration_state,
                    &self.worker_manager,
                    &self.group_name,
                    worker.worker_id,
                )
                .await?;
            }
            Ok(())
        }
        .await;
        if restart_background {
            self.start_background_block_reports();
        }
        result
    }

    /// Drives cleanup until physical deletion and metadata absence both converge.
    ///
    /// Heartbeats deliver cleanup commands and delta reports publish completion.
    /// In-process metadata is also checked for location removal; external
    /// metadata receives a final full report because its location state is not
    /// directly observable from this test harness.
    pub async fn converge_cleanup(&self, expected_physical_blocks: usize) -> TestResult<()> {
        let external_metadata = self.metadata_process.is_some();
        readiness::ReadinessCheck::startup("block cleanup convergence")
            .wait_for_async(|| async {
                if readiness::send_heartbeat(&self.heartbeat, &self.block_store)
                    .await
                    .is_err()
                {
                    return false;
                }
                let Ok(_round) = self.block_report.send_delta_once().await else {
                    return false;
                };
                for worker in &self.additional_workers {
                    if readiness::send_heartbeat(&worker.heartbeat, &worker.block_store)
                        .await
                        .is_err()
                    {
                        return false;
                    }
                    let Ok(_round) = worker.block_report.send_delta_once().await else {
                        return false;
                    };
                }
                if self.physical_block_count().ok() != Some(expected_physical_blocks) {
                    return false;
                }
                if !external_metadata {
                    return self.worker_manager.get_all_locations_count() == expected_physical_blocks;
                }

                if send_full_block_report_to_external_metadata(&self.heartbeat, &self.block_report, &self.block_store)
                    .await
                    .is_err()
                {
                    return false;
                }
                for worker in &self.additional_workers {
                    if send_full_block_report_to_external_metadata(
                        &worker.heartbeat,
                        &worker.block_report,
                        &worker.block_store,
                    )
                    .await
                    .is_err()
                    {
                        return false;
                    }
                }
                true
            })
            .await
    }

    pub async fn shutdown(&mut self) -> TestResult<()> {
        self.stop_background_block_reports().await;
        for worker in &mut self.additional_workers {
            worker.worker_server.shutdown().await?;
        }
        self.worker_server.shutdown().await?;
        if let Some(process) = self.metadata_process.take() {
            process.kill().await?;
        } else {
            self.metadata_server.shutdown().await?;
        }
        Ok(())
    }

    async fn register_workers_with_external_metadata(&self) -> TestResult<()> {
        register_worker_with_external_metadata(
            &self.registrar,
            &self.registration_state,
            &self.heartbeat,
            &self.block_report,
            &self.block_store,
            &self.group_name,
        )
        .await?;
        for worker in &self.additional_workers {
            register_worker_with_external_metadata(
                &worker.registrar,
                &worker.registration_state,
                &worker.heartbeat,
                &worker.block_report,
                &worker.block_store,
                &self.group_name,
            )
            .await?;
        }
        Ok(())
    }

    fn write_metadata_process_config(&self, metrics_addr: SocketAddr) -> TestResult<std::path::PathBuf> {
        let config_path = self._temp_state.root().join("metadata-process.yaml");
        let storage_dir = self.metadata_config.storage_dir.to_string_lossy();
        let config = format!(
            r#"cluster.id: {cluster_id:?}
metadata.group.name: {group_name:?}
metadata.storage.dir: {storage_dir:?}
metadata.rpc.addr: {rpc_addr:?}
metadata.rpc.port: {rpc_port}
metadata.raft.mode: "single"
metadata.raft.node_id: 1
metadata.cleanup.scan_interval_ms: {cleanup_scan_interval_ms}
metadata.cleanup.reclaim_grace_ms: {cleanup_reclaim_grace_ms}
metadata.cleanup.max_replicas_per_scan: {cleanup_max_replicas_per_scan}
metadata.cleanup.max_candidates: {cleanup_max_candidates}
metadata.cleanup.dispatch_enabled: {cleanup_dispatch_enabled}
metadata.cleanup.max_commands_per_heartbeat: {cleanup_max_commands_per_heartbeat}
metadata.cleanup.retry_initial_backoff_ms: {cleanup_retry_initial_backoff_ms}
metadata.cleanup.retry_max_backoff_ms: {cleanup_retry_max_backoff_ms}
metadata.bootstrap.ready.timeout_ms: 10000
metadata.bootstrap.ready.warn_after_ms: 1000
metadata.bootstrap.ready.fail_fast: false
observe.log.format: "compact"
observe.log.output: "stderr"
observe.log.level: "warn,openraft=warn"
observe.metrics.prometheus.bind: {metrics_addr:?}
observe.metrics.prometheus.path: "/metrics"
"#,
            cluster_id = self.metadata_config.cluster_id,
            group_name = self.group_name.as_str(),
            storage_dir = storage_dir,
            rpc_addr = self.metadata_addr.ip().to_string(),
            rpc_port = self.metadata_addr.port(),
            cleanup_scan_interval_ms = self.metadata_config.cleanup.scan_interval_ms,
            cleanup_reclaim_grace_ms = self.metadata_config.cleanup.reclaim_grace_ms,
            cleanup_max_replicas_per_scan = self.metadata_config.cleanup.max_replicas_per_scan,
            cleanup_max_candidates = self.metadata_config.cleanup.max_candidates,
            cleanup_dispatch_enabled = self.metadata_config.cleanup.dispatch_enabled,
            cleanup_max_commands_per_heartbeat = self.metadata_config.cleanup.max_commands_per_heartbeat,
            cleanup_retry_initial_backoff_ms = self.metadata_config.cleanup.retry_initial_backoff_ms,
            cleanup_retry_max_backoff_ms = self.metadata_config.cleanup.retry_max_backoff_ms,
            metrics_addr = metrics_addr.to_string(),
        );
        std::fs::write(&config_path, config)?;
        Ok(config_path)
    }
}

fn store_report_block_count(store: &StoreDirs) -> TestResult<usize> {
    store.report()?.dirs.iter().try_fold(0usize, |count, dir| {
        usize::try_from(dir.block_count)
            .ok()
            .and_then(|dir_count| count.checked_add(dir_count))
            .ok_or_else(|| "worker physical block count overflow".into())
    })
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        if let Some(task) = self.background_block_report.take() {
            task.abort();
        }
        for worker in &mut self.additional_workers {
            worker.worker_server.abort();
        }
        self.worker_server.abort();
        if let Some(process) = &mut self.metadata_process {
            process.abort();
        }
        self.metadata_server.abort();
    }
}

async fn register_worker_with_external_metadata(
    registrar: &MetadataRegistrar,
    registration_state: &beryl_worker::control::RegistrationSet,
    heartbeat: &MetadataHeartbeatLoop,
    block_report: &MetadataBlockReportLoop,
    block_store: &StoreDirs,
    group_name: &GroupName,
) -> TestResult<()> {
    registration_state.mark_needs_register(group_name);
    registrar.register_once().await?;
    readiness::send_heartbeat(heartbeat, block_store).await?;
    let report = block_report.send_full_once().await?;
    if report.accepted_peers == 0 || report.needs_register || report.worker_run_mismatch {
        return Err(format!("external metadata rejected full block report: {report:?}").into());
    }
    Ok(())
}

async fn send_full_block_report_to_external_metadata(
    heartbeat: &MetadataHeartbeatLoop,
    block_report: &MetadataBlockReportLoop,
    block_store: &StoreDirs,
) -> TestResult<()> {
    readiness::send_heartbeat(heartbeat, block_store).await?;
    let round = block_report.send_full_once().await?;
    if round.accepted_peers == 0 || round.needs_register || round.worker_run_mismatch {
        return Err(format!("external metadata rejected full block report: {round:?}").into());
    }
    Ok(())
}

async fn start_metadata_instance(
    metadata_config: &MetadataConfig,
    listener: TcpListener,
) -> TestResult<(MetadataServiceInstance, Arc<WorkerManager>)> {
    prepare_metadata_start(metadata_config)
        .await
        .map_err(|err| io::Error::other(err.to_string()))?;
    let authority = build_authority(metadata_config)
        .await
        .map_err(|err| io::Error::other(err.to_string()))?;
    let worker_manager = Arc::new(WorkerManager::new(60));
    worker_manager.reset_worker_soft_state();
    worker_manager.load_registered_workers(authority.registered_workers()?)?;
    let readiness_state = build_readiness(metadata_config, &authority).await;
    let filesystem = build_filesystem_service(
        metadata_config,
        &authority,
        Arc::clone(&worker_manager),
        &readiness_state,
    )
    .await
    .map_err(|err| io::Error::other(err.to_string()))?;
    let worker_control = authority.worker_service(Arc::clone(&worker_manager));
    let metadata_server = MetadataServiceInstance::start(listener, filesystem, worker_control, authority);
    Ok((metadata_server, worker_manager))
}

struct StartedWorkerService {
    worker_id: WorkerId,
    registrar: MetadataRegistrar,
    registration_state: Arc<beryl_worker::control::RegistrationSet>,
    block_report: Arc<MetadataBlockReportLoop>,
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
    let block_store = Arc::new(StoreDirs::open(
        worker_config.store.dirs.clone(),
        worker_config.store.reserve_space_bytes,
        worker_config.store.check_interval_ms,
    )?);
    let worker_core = Arc::new(WorkerCore::with_local_store(
        worker_config.default_frame_size,
        worker_config.max_frame_size,
        Duration::from_millis(worker_config.stream_idle_timeout_ms),
        Arc::clone(&block_store) as Arc<dyn beryl_worker::store::block::LocalBlockStore + Send + Sync>,
    ));
    let cleanup = BlockCleanupExecutor::start(
        Arc::clone(&worker_core),
        Arc::clone(&registration_state),
        BlockCleanupOptions::default(),
    )?;
    let heartbeat = MetadataHeartbeatLoop::new(
        worker_config.metadata.clone(),
        descriptor.clone(),
        Arc::clone(&registration_state),
        cleanup,
    )?;
    let block_report = Arc::new(MetadataBlockReportLoop::new(
        worker_config.metadata.clone(),
        descriptor,
        Arc::clone(&registration_state),
        Arc::clone(&block_store),
        Arc::clone(&worker_core),
    )?);
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
        raft: RaftConfig::default(),
        authority: MetadataAuthorityConfig { group_name },
        cleanup: CleanupConfig::default(),
        detached_root_reclamation: DetachedRootReclamationConfig::default(),
        worker: WorkerConfig::default(),
        bootstrap: BootstrapConfig {
            root_readiness: beryl_metadata::RootReadinessConfig {
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
    flat.set("client.retry.max_attempts", 3i64);
    flat.set("client.operation.timeout_ms", 2_000i64);
    Ok(FsClient::try_new(ClientConfig::from_flat(flat)?)?)
}

fn observability_config() -> Result<ObservabilityConfig, beryl_common::CommonError> {
    let mut flat = FlatConfig::new();
    flat.set("observe.log.format", "compact");
    flat.set("observe.log.output", "stderr");
    flat.set("observe.log.level", "warn");
    flat.set("observe.metrics.prometheus.bind", "127.0.0.1:0");
    flat.set("observe.metrics.prometheus.path", "/metrics");
    ObservabilityConfig::from_flat(&flat)
}
