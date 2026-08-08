// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker main entry point.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use beryl_common::observe::{init_observability, ServiceInfo};
use beryl_common::service_http::{spawn_service_http, ServiceHttpHandle};
use beryl_common::termination::{TerminationMonitor, TerminationSignal};
use beryl_worker::{
    config::WorkerConfig,
    control::{
        prepare_worker_start, BlockCleanupOptions, BlockCleanupRuntime, BlockReportOptions, MetadataBlockReportLoop,
        MetadataHeartbeatLoop, MetadataRegistrar, RegistrationSet,
    },
    net, observe,
    store::dirs::StoreDirs,
    WorkerCore,
};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    let command = WorkerCommand::parse(std::env::args().skip(1))?;
    let mut termination = TerminationSignal::install()
        .context("Failed to install Worker termination handlers")?
        .monitor();
    let config_path = command
        .config_path
        .clone()
        .unwrap_or_else(|| "conf/worker.yaml".to_string());

    let config = match WorkerConfig::load(&config_path).context("Failed to load worker configuration") {
        Ok(config) => config,
        Err(error) => {
            termination.shutdown().await.context("Worker termination task failed")?;
            return Err(error);
        }
    };

    let result = run_worker(config, &mut termination).await;
    termination.shutdown().await.context("Worker termination task failed")?;
    result
}

/// Owns every long-lived Worker service from startup through bounded shutdown.
///
/// Readiness is closed before RPC drain. Background control-plane and cleanup
/// tasks are then cancelled and awaited under the configured process timeout.
async fn run_worker(config: WorkerConfig, termination: &mut TerminationMonitor) -> Result<()> {
    let worker_id = prepare_worker_start(&config).context("Worker storage start validation failed")?;
    if termination.is_cancelled() {
        log_startup_signal(termination).await?;
        return Ok(());
    }

    let obs_config = config.observability.clone();
    let service_info = ServiceInfo {
        name: "beryl-worker".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        environment: "development".to_string(),
        instance_id: format!("worker-{}", std::process::id()),
        node_name: Some(format!("worker-node-{}", std::process::id())),
    };
    let obs_guard = init_observability(&obs_config, service_info)
        .map_err(|e| anyhow::anyhow!("Failed to initialize observability: {}", e))?;
    observe::record_worker_started("worker", env!("CARGO_PKG_VERSION"));
    observe::set_worker_registered(false);
    if termination.is_cancelled() {
        log_startup_signal(termination).await?;
        return Ok(());
    }

    info!(
        event = "worker_data_service_starting",
        rpc_bind = %config.rpc_bind,
        rpc_address = %config.rpc_address(),
        http_bind = %config.http_addr(),
        rpc_max_inflight = config.rpc_max_inflight,
        default_frame_size = config.default_frame_size,
        max_frame_size = config.max_frame_size,
        store_dirs = config.store.dirs.len(),
        store_reserve_space_bytes = config.store.reserve_space_bytes,
        store_check_interval_ms = config.store.check_interval_ms,
        net_listeners = config.net.listeners.len(),
        "starting worker data service"
    );
    for listener in &config.net.listeners {
        info!(
            event = "worker_net_listener_configured",
            protocol = %listener.protocol,
            bind = %listener.bind,
            max_inflight = listener.max_inflight,
            max_frame_size = listener.max_frame_size,
            "Configured worker net listener"
        );
    }

    let registration_state = Arc::new(RegistrationSet::new());
    let readiness_state = Arc::clone(&registration_state);
    let readiness_group = config.metadata.group_name.clone();
    let http = spawn_service_http(
        config.http_addr(),
        obs_guard.prometheus_handle(),
        Arc::new(move || readiness_state.is_ready(&readiness_group)),
    )
    .context("Failed to start Worker HTTP service")?;
    if termination.is_cancelled() {
        registration_state.begin_shutdown();
        let signal_result = log_startup_signal(termination).await;
        shutdown_worker_start(http, None, config.shutdown_timeout_ms).await?;
        signal_result?;
        return Ok(());
    }
    let descriptor = match MetadataRegistrar::descriptor_from_config(&config, worker_id) {
        Ok(descriptor) => descriptor,
        Err(error) => {
            registration_state.begin_shutdown();
            shutdown_worker_start(http, None, config.shutdown_timeout_ms).await?;
            return Err(error).context("Failed to build worker registration descriptor");
        }
    };
    let block_report_descriptor = descriptor.clone();
    let registrar = match MetadataRegistrar::new(config.metadata.clone(), descriptor, Arc::clone(&registration_state)) {
        Ok(registrar) => Arc::new(registrar),
        Err(error) => {
            registration_state.begin_shutdown();
            shutdown_worker_start(http, None, config.shutdown_timeout_ms).await?;
            return Err(error).context("Failed to create worker metadata registrar");
        }
    };

    let block_store = match StoreDirs::open(
        config.store.dirs.clone(),
        config.store.reserve_space_bytes,
        config.store.check_interval_ms,
    ) {
        Ok(block_store) => Arc::new(block_store),
        Err(error) => {
            registration_state.begin_shutdown();
            shutdown_worker_start(http, None, config.shutdown_timeout_ms).await?;
            return Err(error).context("Failed to initialize worker store dirs");
        }
    };
    if termination.is_cancelled() {
        registration_state.begin_shutdown();
        let signal_result = log_startup_signal(termination).await;
        shutdown_worker_start(http, None, config.shutdown_timeout_ms).await?;
        signal_result?;
        return Ok(());
    }
    let core = Arc::new(WorkerCore::with_local_store(
        config.default_frame_size,
        config.max_frame_size,
        Duration::from_millis(config.stream_idle_timeout_ms),
        block_store.clone(),
    ));
    let cleanup = match BlockCleanupRuntime::start(
        Arc::clone(&core),
        Arc::clone(&registration_state),
        BlockCleanupOptions {
            max_pending: config.block_cleanup.queue_capacity,
            max_concurrent: config.block_cleanup.concurrency,
            retry_initial_backoff: Duration::from_millis(config.block_cleanup.retry_initial_backoff_ms),
            retry_max_backoff: Duration::from_millis(config.block_cleanup.retry_max_backoff_ms),
        },
    ) {
        Ok(cleanup) => cleanup,
        Err(error) => {
            registration_state.begin_shutdown();
            shutdown_worker_start(http, None, config.shutdown_timeout_ms).await?;
            return Err(error).context("Failed to create worker block cleanup executor");
        }
    };
    let heartbeat = match MetadataHeartbeatLoop::with_interval(
        config.metadata.clone(),
        block_report_descriptor.clone(),
        Arc::clone(&registration_state),
        cleanup.executor(),
        Duration::from_millis(config.heartbeat_interval_ms),
    ) {
        Ok(heartbeat) => heartbeat,
        Err(error) => {
            registration_state.begin_shutdown();
            shutdown_worker_start(http, Some(cleanup), config.shutdown_timeout_ms).await?;
            return Err(error).context("Failed to create worker metadata heartbeat loop");
        }
    };
    let block_report = match MetadataBlockReportLoop::with_options_and_interval(
        config.metadata.clone(),
        block_report_descriptor,
        Arc::clone(&registration_state),
        Arc::clone(&block_store),
        Arc::clone(&core),
        BlockReportOptions {
            full_max_blocks_per_batch: config.block_report_batch_size,
            delta_max_entries_per_batch: config.block_report_batch_size,
        },
        Duration::from_millis(config.block_report_interval_ms),
    ) {
        Ok(block_report) => block_report,
        Err(error) => {
            registration_state.begin_shutdown();
            shutdown_worker_start(http, Some(cleanup), config.shutdown_timeout_ms).await?;
            return Err(error).context("Failed to create worker block report loop");
        }
    };

    if termination.is_cancelled() {
        registration_state.begin_shutdown();
        let signal_result = log_startup_signal(termination).await;
        shutdown_worker_start(http, Some(cleanup), config.shutdown_timeout_ms).await?;
        signal_result?;
        return Ok(());
    }

    let registration = tokio::select! {
        biased;
        signal = termination.recv() => {
            let signal_result = signal.context("Worker termination task failed");
            if let Ok(signal) = &signal_result {
                info!(?signal, "Shutdown signal received before Worker registration completed");
            }
            registration_state.begin_shutdown();
            shutdown_worker_start(http, Some(cleanup), config.shutdown_timeout_ms).await?;
            signal_result?;
            return Ok(());
        }
        result = registrar.register_with_retry(std::future::pending::<()>()) => {
            result.context("Worker metadata registration failed")?
        }
    };
    info!(
        group_name = %registration.group_name,
        worker_id = registration.worker_id.as_raw(),
        worker_run_id = %registration.worker_run_id,
        "Worker metadata registration completed"
    );

    let mut rpc = match net::server::spawn_worker_data_with_registration(
        &config.net,
        Arc::clone(&core),
        Arc::clone(&registration_state),
    ) {
        Ok(rpc) => rpc,
        Err(error) => {
            registration_state.begin_shutdown();
            shutdown_worker_start(http, Some(cleanup), config.shutdown_timeout_ms).await?;
            return Err(error).context("Failed to start Worker data service");
        }
    };

    let background_shutdown = CancellationToken::new();
    let heartbeat_handle = heartbeat.spawn_with_registrar_and_store_until_shutdown(
        Arc::clone(&registrar),
        Arc::clone(&block_store),
        background_shutdown.child_token(),
    );
    let block_report_handle = block_report.spawn_until_shutdown(background_shutdown.child_token());

    let mut stop_error = None;
    tokio::select! {
        biased;
        signal = termination.recv() => {
            match signal {
                Ok(signal) => info!(?signal, "Shutdown signal received"),
                Err(error) => stop_error = Some(error.into()),
            }
        }
        result = rpc.wait() => {
            stop_error = Some(match result {
                Ok(()) => anyhow::anyhow!("Worker RPC server stopped unexpectedly"),
                Err(error) => error.into(),
            });
        }
    }

    registration_state.begin_shutdown();
    background_shutdown.cancel();
    let deadline = Instant::now() + Duration::from_millis(config.shutdown_timeout_ms);
    let (rpc_result, heartbeat_result, block_report_result, cleanup_result, http_result) = tokio::join!(
        rpc.shutdown_until(deadline),
        stop_task_until(Some(heartbeat_handle), deadline),
        stop_task_until(Some(block_report_handle), deadline),
        cleanup.shutdown_until(deadline),
        http.shutdown_until(deadline),
    );

    let rpc_forced = rpc_result.as_ref().copied().unwrap_or(false);
    let heartbeat_forced = heartbeat_result.as_ref().is_ok_and(|result| result.1);
    let block_report_forced = block_report_result.as_ref().is_ok_and(|result| result.1);
    let cleanup_forced = cleanup_result.as_ref().copied().unwrap_or(false);
    let http_forced = http_result.as_ref().copied().unwrap_or(false);
    if rpc_forced || heartbeat_forced || block_report_forced || cleanup_forced || http_forced {
        tracing::warn!(
            rpc_forced,
            heartbeat_forced,
            block_report_forced,
            cleanup_forced,
            http_forced,
            timeout_ms = config.shutdown_timeout_ms,
            "Worker forced remaining work after the graceful drain deadline"
        );
    }

    rpc_result.context("Worker data service task failed")?;
    heartbeat_result.context("Worker heartbeat task failed")?;
    block_report_result.context("Worker block report task failed")?;
    cleanup_result.context("Worker cleanup shutdown failed")?;
    http_result.context("Worker HTTP shutdown failed")?;
    if let Some(error) = stop_error {
        error!(%error, "Worker server stopped unexpectedly");
        return Err(error);
    }
    Ok(())
}

async fn log_startup_signal(termination: &mut TerminationMonitor) -> Result<()> {
    let signal = termination.recv().await.context("Worker termination task failed")?;
    info!(?signal, "Shutdown signal received during Worker startup");
    Ok(())
}

/// Reclaims the subset of Worker services constructed before registration.
async fn shutdown_worker_start(
    http: ServiceHttpHandle,
    cleanup: Option<BlockCleanupRuntime>,
    timeout_ms: u64,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let cleanup_shutdown = async move {
        match cleanup {
            Some(cleanup) => cleanup.shutdown_until(deadline).await,
            None => Ok(false),
        }
    };
    let (cleanup_result, http_result) = tokio::join!(cleanup_shutdown, http.shutdown_until(deadline));
    let cleanup_forced = cleanup_result.context("Worker cleanup shutdown failed")?;
    let http_forced = http_result.context("Worker HTTP shutdown failed")?;
    if cleanup_forced || http_forced {
        tracing::warn!(
            cleanup_forced,
            http_forced,
            timeout_ms,
            "Worker forced startup work after the drain deadline"
        );
    }
    Ok(())
}

/// Gracefully drains an owned task, then explicitly aborts and awaits it.
async fn stop_task_until<T>(task: Option<JoinHandle<T>>, deadline: Instant) -> Result<(Option<T>, bool), JoinError> {
    let Some(mut task) = task else {
        return Ok((None, false));
    };
    match tokio::time::timeout_at(deadline, &mut task).await {
        Ok(result) => result.map(|output| (Some(output), false)),
        Err(_) => {
            task.abort();
            match task.await {
                Ok(output) => Ok((Some(output), true)),
                Err(error) if error.is_cancelled() => Ok((None, true)),
                Err(error) => Err(error),
            }
        }
    }
}

struct WorkerCommand {
    config_path: Option<String>,
}

impl WorkerCommand {
    fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut args = args.into_iter().peekable();
        if let Some(first) = args.peek().cloned() {
            match first.as_str() {
                "start" => {
                    args.next();
                }
                _ if first.starts_with('-') => {}
                _ if looks_like_path(&first) => {
                    anyhow::bail!("worker config path must be passed with --config: {first}");
                }
                _ => anyhow::bail!("unsupported worker command: {first}"),
            }
        }

        let mut config_path = None;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    let Some(path) = args.next() else {
                        anyhow::bail!("--config requires a path");
                    };
                    config_path = Some(path);
                }
                "--force" => anyhow::bail!("--force is not supported for worker start"),
                _ => anyhow::bail!("unknown worker argument: {arg}"),
            }
        }

        Ok(Self { config_path })
    }
}

fn looks_like_path(value: &str) -> bool {
    value.contains('/') || value.ends_with(".yaml") || value.ends_with(".yml") || value.ends_with(".toml")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn parse(args: &[&str]) -> Result<WorkerCommand> {
        WorkerCommand::parse(args.iter().map(|arg| arg.to_string()))
    }

    #[test]
    fn valid_worker_start_command_parses() {
        let start = parse(&["start", "--config", "conf/worker.yaml"]).unwrap();
        assert_eq!(start.config_path.as_deref(), Some("conf/worker.yaml"));

        let default_start = parse(&[]).unwrap();
        assert!(default_start.config_path.is_none());
    }

    #[test]
    fn worker_observe_cli_overrides_are_rejected() {
        for flag in [
            "--observe-profile",
            "--log-level",
            "--log-format",
            "--log-output",
            "--metrics-bind",
            "--metrics-path",
            "--trace-enabled",
        ] {
            let err = parse(&["start", flag, "value"])
                .err()
                .expect("observe CLI override must fail");
            assert!(err.to_string().contains("unknown worker argument"), "{flag}: {err}");
        }
    }

    #[test]
    fn worker_startup_load_uses_file_observe_values() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("worker.yaml");
        let store_path = temp_dir.path().join("hdd0");
        fs::write(
            &config_path,
            format!(
                r#"
beryl.worker.bind-host: 127.0.0.1
beryl.worker.rpc.port: 19090
beryl.worker.http.port: 19091
beryl.worker.storage.dirs:
  hdd0:
    path: "{}"
    tier: hdd
    capacity: 10GiB
beryl.worker.metadata.addresses:
  - 127.0.0.1:18080
beryl.logging.format: json
beryl.logging.output: stdout
beryl.logging.level: "warn"
"#,
                store_path.display()
            ),
        )
        .unwrap();

        let command = parse(&["start", "--config", config_path.to_str().unwrap()]).unwrap();
        let config_path = command.config_path.as_deref().expect("config path");
        let config = WorkerConfig::load(config_path).unwrap();

        assert_eq!(config.observability.log.format, "json");
        assert_eq!(config.observability.log.output, "stdout");
        assert_eq!(config.http_addr(), "127.0.0.1:19091".parse().unwrap());
    }

    #[test]
    fn worker_config_path_requires_explicit_config_flag() {
        let err = parse(&["conf/worker.yaml"])
            .err()
            .expect("positional worker config path must fail");
        assert!(err.to_string().contains("--config"));
    }
}
