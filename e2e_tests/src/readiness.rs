// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::sync::Arc;
use std::time::Duration;

use client::FsClient;
use metadata::worker::WorkerManager;
use tokio::time::{sleep, timeout, Instant};
use types::{GroupName, WorkerId};
use worker::control::{HeartbeatSnapshot, MetadataBlockReportLoop, MetadataHeartbeatLoop, RegistrationSet};
use worker::store::dirs::StoreDirs;

use crate::TestResult;

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

pub struct ReadinessCheck {
    name: &'static str,
    timeout: Duration,
    poll_interval: Duration,
}

impl ReadinessCheck {
    pub fn startup(name: &'static str) -> Self {
        Self {
            name,
            timeout: STARTUP_TIMEOUT,
            poll_interval: POLL_INTERVAL,
        }
    }

    pub async fn wait_for(&self, mut condition: impl FnMut() -> bool) -> TestResult<()> {
        let started = Instant::now();
        timeout(self.timeout, async {
            loop {
                if condition() {
                    return;
                }
                sleep(self.poll_interval).await;
            }
        })
        .await
        .map_err(|_| format!("{} timed out after {:?}", self.name, started.elapsed()))?;
        Ok(())
    }

    pub async fn wait_for_async<F, Fut>(&self, mut condition: F) -> TestResult<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let started = Instant::now();
        timeout(self.timeout, async {
            loop {
                if condition().await {
                    return;
                }
                sleep(self.poll_interval).await;
            }
        })
        .await
        .map_err(|_| format!("{} timed out after {:?}", self.name, started.elapsed()))?;
        Ok(())
    }
}

pub async fn wait_for_metadata_filesystem(client: &FsClient) -> TestResult<()> {
    ReadinessCheck::startup("metadata filesystem readiness")
        .wait_for_async(|| async { client.stat("/").await.is_ok() })
        .await
}

pub async fn wait_for_worker_registration(
    registration_state: &RegistrationSet,
    worker_manager: &WorkerManager,
    group_name: &GroupName,
    worker_id: WorkerId,
) -> TestResult<()> {
    ReadinessCheck::startup("worker registration")
        .wait_for(|| {
            registration_state.is_registered(group_name)
                && worker_manager.get_registration(group_name, worker_id).is_some()
        })
        .await
}

pub async fn wait_for_worker_heartbeat(
    registration_state: &RegistrationSet,
    worker_manager: &WorkerManager,
    group_name: &GroupName,
    worker_id: WorkerId,
) -> TestResult<()> {
    ReadinessCheck::startup("worker heartbeat readiness")
        .wait_for(|| registration_state.is_ready(group_name) && worker_manager.is_worker_live(group_name, worker_id))
        .await
}

pub async fn send_heartbeat(heartbeat: &MetadataHeartbeatLoop, block_store: &StoreDirs) -> TestResult<()> {
    let round = heartbeat
        .send_once(HeartbeatSnapshot::from(block_store.report()?))
        .await?;
    if round.accepted_peers == 0 || round.needs_register || round.worker_run_mismatch {
        return Err(format!("heartbeat not accepted: {round:?}").into());
    }
    Ok(())
}

pub async fn converge_block_reports(
    heartbeat: &MetadataHeartbeatLoop,
    block_report: &MetadataBlockReportLoop,
    block_store: &StoreDirs,
    registration_state: &RegistrationSet,
    worker_manager: &WorkerManager,
    group_name: &GroupName,
    worker_id: WorkerId,
) -> TestResult<()> {
    send_heartbeat(heartbeat, block_store).await?;
    let round = block_report.send_full_once().await?;
    if round.accepted_peers == 0 || round.needs_register || round.worker_run_mismatch {
        return Err(format!("full block report did not converge: {round:?}").into());
    }
    let ready_block_count = block_store.scan_group_blocks(group_name)?.len();
    ReadinessCheck::startup("block report convergence")
        .wait_for(|| {
            registration_state.is_ready(group_name)
                && worker_manager.is_worker_live(group_name, worker_id)
                && !worker_manager.needs_full_block_report(group_name, worker_id)
                && worker_manager.get_all_locations_count() >= ready_block_count
        })
        .await
}

pub fn shared_registration_state() -> Arc<RegistrationSet> {
    Arc::new(RegistrationSet::new())
}
