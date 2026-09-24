// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::sync::Arc;
use std::time::Duration;

use beryl_client::FsClient;
use beryl_types::GroupName;
use beryl_worker::control::{
    BlockReportOutcome, HeartbeatOutcome, MetadataBlockReportLoop, MetadataHeartbeatLoop, RegistrationState,
};
use beryl_worker::store::dirs::StoreDirs;
use tokio::time::{sleep, timeout, Instant};

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
        .wait_for_async(|| async { client.get_status("/").await.is_ok() })
        .await
}

pub async fn wait_for_worker_registration(
    registration_state: &RegistrationState,
    group_name: &GroupName,
) -> TestResult<()> {
    ReadinessCheck::startup("worker registration")
        .wait_for(|| registration_state.is_registered(group_name))
        .await
}

pub async fn wait_for_worker_heartbeat(
    registration_state: &RegistrationState,
    group_name: &GroupName,
) -> TestResult<()> {
    ReadinessCheck::startup("worker heartbeat readiness")
        .wait_for(|| registration_state.is_ready(group_name))
        .await
}

pub async fn send_heartbeat(heartbeat: &MetadataHeartbeatLoop, block_store: &StoreDirs) -> TestResult<()> {
    let round = heartbeat.send_once(&block_store.report()).await?;
    if round != HeartbeatOutcome::Accepted {
        return Err(format!("heartbeat not accepted: {round:?}").into());
    }
    Ok(())
}

/// A successful EOF report is acknowledged only after Metadata publishes the baseline.
pub async fn converge_block_reports(
    heartbeat: &MetadataHeartbeatLoop,
    block_report: &MetadataBlockReportLoop,
    block_store: &StoreDirs,
) -> TestResult<()> {
    send_heartbeat(heartbeat, block_store).await?;
    let round = block_report.send_full_once().await?;
    if round != BlockReportOutcome::Accepted {
        return Err(format!("full block report did not converge: {round:?}").into());
    }
    Ok(())
}

pub fn shared_registration_state() -> Arc<RegistrationState> {
    Arc::new(RegistrationState::new())
}
