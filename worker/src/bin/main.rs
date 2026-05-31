// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker main entry point.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use common::observe::{init_observability, ObservabilityConfig, ServiceInfo};
use tracing::{error, info};
use worker::{
    config::WorkerConfig,
    control::{
        prepare_worker_start, MetadataBlockReportLoop, MetadataHeartbeatLoop, MetadataRegistrar, RegistrationSet,
    },
    net,
    store::block::{FullBlockFileStore, FullBlockFileStoreConfig},
    WorkerCore,
};

#[tokio::main]
async fn main() -> Result<()> {
    let command = WorkerCommand::parse(std::env::args().skip(1))?;
    let config_path = command
        .config_path
        .clone()
        .unwrap_or_else(|| "conf/core-site.yaml".to_string());

    let config = WorkerConfig::load(&config_path).context("Failed to load worker configuration")?;

    let worker_id = prepare_worker_start(&config).context("Worker storage start validation failed")?;

    let mut obs_config = ObservabilityConfig::default();
    obs_config.metrics.prometheus.bind = config.http_bind.clone();
    let service_info = ServiceInfo {
        name: "vecton-worker".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        environment: "development".to_string(),
        instance_id: format!("worker-{}", std::process::id()),
        node_name: Some(format!("worker-node-{}", std::process::id())),
    };
    let _obs_guard = init_observability(&obs_config, service_info)
        .map_err(|e| anyhow::anyhow!("Failed to initialize observability: {}", e))?;

    info!(
        rpc_bind = %config.rpc_bind,
        rpc_advertised_endpoint = %config.rpc_advertised_endpoint,
        rpc_max_inflight = config.rpc_max_inflight,
        default_frame_size = config.default_frame_size,
        max_frame_size = config.max_frame_size,
        window_bytes = config.window_bytes,
        storage_root = ?config.storage_root,
        net_listeners = config.net.listeners.len(),
        "Starting worker data service skeleton"
    );
    for listener in &config.net.listeners {
        info!(
            protocol = %listener.protocol,
            bind = %listener.bind,
            max_inflight = listener.max_inflight,
            max_frame_size = listener.max_frame_size,
            roles = ?listener.role,
            "Configured worker net listener"
        );
    }

    let registration_state = Arc::new(RegistrationSet::new());
    let descriptor = MetadataRegistrar::descriptor_from_config(&config, worker_id)
        .context("Failed to build worker registration descriptor")?;
    let block_report_descriptor = descriptor.clone();
    let heartbeat = MetadataHeartbeatLoop::new(
        config.metadata.clone(),
        descriptor.clone(),
        Arc::clone(&registration_state),
    )
    .context("Failed to create worker metadata heartbeat loop")?;
    let registrar = Arc::new(
        MetadataRegistrar::new(config.metadata.clone(), descriptor, Arc::clone(&registration_state))
            .context("Failed to create worker metadata registrar")?,
    );

    let block_store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
        config.storage_root.clone(),
    )));
    let core = Arc::new(WorkerCore::with_local_store(
        config.default_frame_size,
        config.max_frame_size,
        config.window_bytes,
        Duration::from_millis(config.stream_idle_timeout_ms),
        block_store.clone(),
    ));

    registrar
        .register_with_retry(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("Worker metadata registration failed")?;
    let _heartbeat_handle = heartbeat.spawn_with_registrar(Arc::clone(&registrar));
    let block_report = MetadataBlockReportLoop::new(
        config.metadata.clone(),
        block_report_descriptor,
        Arc::clone(&registration_state),
        block_store,
    )
    .context("Failed to create worker block report loop")?;
    let _block_report_handle = block_report.spawn();

    if let Err(error) = net::server::serve_worker_data_with_registration(&config.net, core, registration_state)
        .await
        .context("Worker data service server failed")
    {
        error!(%error, "Worker RPC server failed");
        return Err(error);
    }

    Ok(())
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

    fn parse(args: &[&str]) -> Result<WorkerCommand> {
        WorkerCommand::parse(args.iter().map(|arg| arg.to_string()))
    }

    #[test]
    fn valid_worker_start_command_parses() {
        let start = parse(&["start", "--config", "conf/local/core-site.yaml"]).unwrap();
        assert_eq!(start.config_path.as_deref(), Some("conf/local/core-site.yaml"));

        let default_start = parse(&[]).unwrap();
        assert!(default_start.config_path.is_none());
    }

    #[test]
    fn removed_worker_command_words_fail() {
        for args in [
            &["format"][..],
            &["bootstrap"][..],
            &["auto-format"][..],
            &["format", "--config", "conf/core-site.yaml"][..],
        ] {
            let err = parse(args).err().expect("removed worker command must fail");
            assert!(err.to_string().contains("unsupported worker command"));
        }
    }

    #[test]
    fn worker_config_path_requires_explicit_config_flag() {
        let err = parse(&["conf/local/core-site.yaml"])
            .err()
            .expect("positional worker config path must fail");
        assert!(err.to_string().contains("--config"));
    }
}
