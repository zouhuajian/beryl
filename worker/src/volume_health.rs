// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Volume health monitoring: failure detection, isolation, and recovery.

use crate::volume_manager::{VolumeInfo, VolumeManager, VolumeState};
use anyhow::Result;
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Volume health configuration.
#[derive(Clone, Debug)]
pub struct VolumeHealthConfig {
    /// Error rate threshold (errors per second) to trigger failure.
    pub error_rate_threshold: f64,
    /// Consecutive failures to trigger failure state.
    pub consecutive_failures_threshold: u32,
    /// Recovery probe interval (seconds).
    pub recovery_probe_interval_secs: u64,
    /// Recovery probe timeout (seconds).
    pub recovery_probe_timeout_secs: u64,
}

impl Default for VolumeHealthConfig {
    fn default() -> Self {
        Self {
            error_rate_threshold: 10.0, // 10 errors/second
            consecutive_failures_threshold: 5,
            recovery_probe_interval_secs: 60, // 1 minute
            recovery_probe_timeout_secs: 5,
        }
    }
}

/// Volume health state.
#[derive(Clone, Debug)]
struct VolumeHealthState {
    /// Current error count (sliding window).
    error_count: u32,
    /// Last error time.
    last_error_time: Option<Instant>,
    /// Consecutive failures.
    consecutive_failures: u32,
    /// Last probe time.
    last_probe_time: Option<Instant>,
    /// Last successful operation time.
    last_success_time: Option<Instant>,
}

impl VolumeHealthState {
    fn new() -> Self {
        Self {
            error_count: 0,
            last_error_time: None,
            consecutive_failures: 0,
            last_probe_time: None,
            last_success_time: Some(Instant::now()),
        }
    }

    /// Record an error.
    fn record_error(&mut self) {
        self.error_count += 1;
        self.last_error_time = Some(Instant::now());
        self.consecutive_failures += 1;
    }

    /// Record a success.
    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.last_success_time = Some(Instant::now());
        // Reset error count after a window (simplified: reset on success)
        // In production, use a proper sliding window
        if self.last_success_time.is_some() {
            self.error_count = 0;
        }
    }

    /// Calculate error rate (errors per second).
    fn error_rate(&self) -> f64 {
        if let Some(last_error) = self.last_error_time {
            let elapsed = last_error.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                self.error_count as f64 / elapsed
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Check if volume should be marked as failed.
    fn should_fail(&self, config: &VolumeHealthConfig) -> bool {
        self.error_rate() >= config.error_rate_threshold
            || self.consecutive_failures >= config.consecutive_failures_threshold
    }
}

/// Volume health manager.
pub struct VolumeHealthManager {
    /// Volume manager.
    volume_manager: Arc<VolumeManager>,
    /// Configuration.
    config: VolumeHealthConfig,
    /// Health state per volume path.
    health_states: Arc<RwLock<std::collections::HashMap<PathBuf, VolumeHealthState>>>,
    /// Metrics.
    metrics: VolumeHealthMetrics,
}

/// Volume health metrics.
#[derive(Clone, Debug)]
pub struct VolumeHealthMetrics {
    /// Total volume failures.
    volume_failed_total: Arc<RwLock<u64>>,
    /// Total recovery attempts.
    volume_recover_attempts_total: Arc<RwLock<u64>>,
    /// Total successful recoveries.
    volume_recover_success_total: Arc<RwLock<u64>>,
}

impl VolumeHealthMetrics {
    fn new() -> Self {
        Self {
            volume_failed_total: Arc::new(RwLock::new(0)),
            volume_recover_attempts_total: Arc::new(RwLock::new(0)),
            volume_recover_success_total: Arc::new(RwLock::new(0)),
        }
    }

    pub fn inc_volume_failed(&self) {
        *self.volume_failed_total.write() += 1;
    }

    pub fn inc_recover_attempt(&self) {
        *self.volume_recover_attempts_total.write() += 1;
    }

    pub fn inc_recover_success(&self) {
        *self.volume_recover_success_total.write() += 1;
    }

    pub fn get_volume_failed_total(&self) -> u64 {
        *self.volume_failed_total.read()
    }

    pub fn get_recover_attempts_total(&self) -> u64 {
        *self.volume_recover_attempts_total.read()
    }

    pub fn get_recover_success_total(&self) -> u64 {
        *self.volume_recover_success_total.read()
    }
}

impl VolumeHealthManager {
    /// Create a new volume health manager.
    pub fn new(volume_manager: Arc<VolumeManager>, config: VolumeHealthConfig) -> Self {
        Self {
            volume_manager,
            config,
            health_states: Arc::new(RwLock::new(std::collections::HashMap::new())),
            metrics: VolumeHealthMetrics::new(),
        }
    }

    /// Record an I/O error for a volume.
    pub fn record_error(&self, volume_path: &Path) {
        let mut states = self.health_states.write();
        let state = states
            .entry(volume_path.to_path_buf())
            .or_insert_with(VolumeHealthState::new);
        state.record_error();

        // Check if volume should be marked as failed
        if state.should_fail(&self.config) {
            let current_state = self
                .volume_manager
                .volumes()
                .iter()
                .find(|v| v.path == volume_path)
                .map(|v| v.state);

            if current_state != Some(VolumeState::Failed) {
                self.volume_manager
                    .update_volume_state(volume_path, VolumeState::Failed);
                self.metrics.inc_volume_failed();
                error!(
                    path = %volume_path.display(),
                    error_rate = state.error_rate(),
                    consecutive_failures = state.consecutive_failures,
                    "Volume marked as failed"
                );
            }
        }
    }

    /// Record a successful I/O operation for a volume.
    pub fn record_success(&self, volume_path: &Path) {
        let mut states = self.health_states.write();
        let state = states
            .entry(volume_path.to_path_buf())
            .or_insert_with(VolumeHealthState::new);
        state.record_success();

        // If volume was in Failed/Recovering state, check if it should be recovered
        let current_state = self
            .volume_manager
            .volumes()
            .iter()
            .find(|v| v.path == volume_path)
            .map(|v| v.state);

        if current_state == Some(VolumeState::Failed) || current_state == Some(VolumeState::Recovering) {
            // Multiple successes indicate recovery
            if state.consecutive_failures == 0 && state.error_rate() < self.config.error_rate_threshold {
                self.volume_manager
                    .update_volume_state(volume_path, VolumeState::Healthy);
                self.metrics.inc_recover_success();
                info!(
                    path = %volume_path.display(),
                    "Volume recovered to Healthy"
                );
            }
        }
    }

    /// Probe a failed volume for recovery.
    async fn probe_volume(&self, volume_path: &Path) -> Result<bool> {
        self.metrics.inc_recover_attempt();

        debug!(path = %volume_path.display(), "Probing volume for recovery");

        // Try a simple I/O operation: create and delete a test file
        let test_file = volume_path.join(".volume_health_probe");

        // Write test
        match tokio::fs::write(&test_file, b"probe").await {
            Ok(_) => {
                // Read test
                match tokio::fs::read(&test_file).await {
                    Ok(data) if data == b"probe" => {
                        // Delete test
                        let _ = tokio::fs::remove_file(&test_file).await;

                        // Refresh volume capacity
                        if let Err(e) = self.volume_manager.refresh_volume(volume_path) {
                            warn!(error = %e, "Failed to refresh volume capacity");
                        }

                        self.record_success(volume_path);
                        Ok(true)
                    }
                    Ok(_) => {
                        let _ = tokio::fs::remove_file(&test_file).await;
                        self.record_error(volume_path);
                        Ok(false)
                    }
                    Err(e) => {
                        let _ = tokio::fs::remove_file(&test_file).await;
                        self.record_error(volume_path);
                        Err(anyhow::anyhow!("Probe read failed: {}", e))
                    }
                }
            }
            Err(e) => {
                self.record_error(volume_path);
                Err(anyhow::anyhow!("Probe write failed: {}", e))
            }
        }
    }

    /// Background recovery task (probes failed volumes periodically).
    pub async fn run_background_task(&self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(self.config.recovery_probe_interval_secs));

        loop {
            interval.tick().await;

            let volumes = self.volume_manager.volumes();
            for volume in volumes {
                if volume.state == VolumeState::Failed || volume.state == VolumeState::Recovering {
                    // Mark as Recovering if it was Failed
                    if volume.state == VolumeState::Failed {
                        self.volume_manager
                            .update_volume_state(&volume.path, VolumeState::Recovering);
                    }

                    // Probe volume
                    match tokio::time::timeout(
                        Duration::from_secs(self.config.recovery_probe_timeout_secs),
                        self.probe_volume(&volume.path),
                    )
                    .await
                    {
                        Ok(Ok(true)) => {
                            info!(path = %volume.path.display(), "Volume probe successful");
                        }
                        Ok(Ok(false)) => {
                            debug!(path = %volume.path.display(), "Volume probe failed");
                        }
                        Ok(Err(e)) => {
                            warn!(error = %e, path = %volume.path.display(), "Volume probe error");
                        }
                        Err(_) => {
                            warn!(path = %volume.path.display(), "Volume probe timeout");
                            self.record_error(&volume.path);
                        }
                    }
                }
            }
        }
    }

    /// Get metrics.
    pub fn metrics(&self) -> &VolumeHealthMetrics {
        &self.metrics
    }

    /// Check if a volume can accept writes.
    pub fn can_accept_writes(&self, volume_path: &Path) -> bool {
        let volumes = self.volume_manager.volumes();
        volumes
            .iter()
            .find(|v| v.path == volume_path)
            .map(|v| v.state == VolumeState::Healthy || v.state == VolumeState::Degraded)
            .unwrap_or(false)
    }

    /// Check if a volume can serve reads (default: disabled for failed volumes).
    pub fn can_serve_reads(&self, volume_path: &Path) -> bool {
        let volumes = self.volume_manager.volumes();
        volumes
            .iter()
            .find(|v| v.path == volume_path)
            .map(|v| {
                // Default: only Healthy/Degraded volumes can serve reads
                // Failed volumes are not allowed to serve reads (safety)
                v.state == VolumeState::Healthy || v.state == VolumeState::Degraded
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volume_health_config_default() {
        let config = VolumeHealthConfig::default();
        assert_eq!(config.error_rate_threshold, 10.0);
        assert_eq!(config.consecutive_failures_threshold, 5);
    }

    #[test]
    fn test_volume_health_state() {
        let mut state = VolumeHealthState::new();
        assert_eq!(state.error_rate(), 0.0);
        assert_eq!(state.consecutive_failures, 0);

        state.record_error();
        assert_eq!(state.consecutive_failures, 1);

        state.record_success();
        assert_eq!(state.consecutive_failures, 0);
    }
}
