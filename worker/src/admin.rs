// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Admin/health endpoints for worker diagnostics.

use crate::block_store::BlockStore;
use crate::eviction::EvictionManager;
use crate::lifecycle::{Lifecycle, WorkerState};
use crate::orphan::OrphanManager;
use crate::volume_health::VolumeHealthManager;
use crate::volume_manager::VolumeManager;
use common::audit::AuditLogger;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Health check response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Worker state.
    pub state: String,
    /// Whether worker is healthy.
    pub healthy: bool,
    /// Group session states.
    pub group_sessions: Vec<GroupSessionState>,
    /// Volume states.
    pub volumes: Vec<VolumeHealthInfo>,
    /// Queue depths (if applicable).
    pub queue_depths: QueueDepths,
    /// Timestamp.
    pub timestamp: String,
}

/// Group session state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GroupSessionState {
    /// Group ID.
    pub group_id: u64,
    /// Session state (e.g., "connected", "disconnected").
    pub state: String,
    /// Last heartbeat time (ISO 8601).
    pub last_heartbeat: Option<String>,
}

/// Volume health information.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeHealthInfo {
    /// Volume path.
    pub path: String,
    /// Volume state.
    pub state: String,
    /// Total capacity in bytes.
    pub total_bytes: u64,
    /// Used capacity in bytes.
    pub used_bytes: u64,
    /// Available capacity in bytes.
    pub available_bytes: u64,
    /// Usage percentage.
    pub usage_percent: f64,
}

/// Queue depths.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueDepths {
    /// Audit log queue depth.
    pub audit_queue: usize,
    /// Write queue depth (if applicable).
    pub write_queue: usize,
    /// Read queue depth (if applicable).
    pub read_queue: usize,
}

/// Admin service.
pub struct AdminService {
    /// Lifecycle manager.
    lifecycle: Arc<Lifecycle>,
    /// Volume manager.
    volume_manager: Arc<VolumeManager>,
    /// Block store.
    block_store: Arc<BlockStore>,
    /// Eviction manager (optional).
    eviction_manager: Option<Arc<EvictionManager>>,
    /// Orphan manager (optional).
    orphan_manager: Option<Arc<OrphanManager>>,
    /// Volume health manager (optional).
    volume_health_manager: Option<Arc<VolumeHealthManager>>,
    /// Audit logger.
    audit_logger: Arc<AuditLogger>,
}

impl AdminService {
    /// Create a new admin service.
    pub fn new(
        lifecycle: Arc<Lifecycle>,
        volume_manager: Arc<VolumeManager>,
        block_store: Arc<BlockStore>,
        audit_logger: Arc<AuditLogger>,
    ) -> Self {
        Self {
            lifecycle,
            volume_manager,
            block_store,
            eviction_manager: None,
            orphan_manager: None,
            volume_health_manager: None,
            audit_logger,
        }
    }

    /// Set eviction manager.
    pub fn with_eviction_manager(mut self, manager: Arc<EvictionManager>) -> Self {
        self.eviction_manager = Some(manager);
        self
    }

    /// Set orphan manager.
    pub fn with_orphan_manager(mut self, manager: Arc<OrphanManager>) -> Self {
        self.orphan_manager = Some(manager);
        self
    }

    /// Set volume health manager.
    pub fn with_volume_health_manager(mut self, manager: Arc<VolumeHealthManager>) -> Self {
        self.volume_health_manager = Some(manager);
        self
    }

    /// Get health status.
    pub fn health(&self) -> HealthResponse {
        let state = self.lifecycle.state();
        let healthy = state == WorkerState::Serving || state == WorkerState::Degraded;

        // Get group session states (default group only)
        let group_sessions = vec![GroupSessionState {
            group_id: 0,                    // Default group
            state: "connected".to_string(), // Always connected in current implementation
            last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
        }];

        // Get volume states
        let volumes: Vec<VolumeHealthInfo> = self
            .volume_manager
            .volumes()
            .iter()
            .map(|v| VolumeHealthInfo {
                path: v.path.display().to_string(),
                state: format!("{:?}", v.state),
                total_bytes: v.total_bytes,
                used_bytes: v.used_bytes,
                available_bytes: v.available_bytes,
                usage_percent: v.usage_percent(),
            })
            .collect();

        // Get queue depths
        let queue_depths = QueueDepths {
            audit_queue: self.audit_logger.queue_size(),
            write_queue: 0, // TODO: track write queue depth
            read_queue: 0,  // TODO: track read queue depth
        };

        HealthResponse {
            state: format!("{:?}", state),
            healthy,
            group_sessions,
            volumes,
            queue_depths,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Get detailed metrics (for Prometheus or similar).
    pub fn metrics(&self) -> String {
        let mut metrics = Vec::new();

        // Worker state
        let state = self.lifecycle.state();
        metrics.push(format!(
            "# HELP worker_state Worker state (0=Stopped, 1=Starting, 2=Running, 3=Stopping)"
        ));
        metrics.push(format!(
            "worker_state {}",
            match state {
                WorkerState::Stopped => 0,
                WorkerState::Bootstrapping => 1,
                WorkerState::Serving => 2,
                WorkerState::Draining => 3,
                _ => 4,
            }
        ));

        // Volume metrics
        let volumes = self.volume_manager.volumes();
        for (i, volume) in volumes.iter().enumerate() {
            let labels = format!(r#"volume="{}""#, volume.path.display());
            metrics.push(format!(
                "# HELP worker_volume_total_bytes Volume total capacity in bytes"
            ));
            metrics.push(format!(
                "worker_volume_total_bytes{{{}}} {}",
                labels, volume.total_bytes
            ));
            metrics.push(format!("# HELP worker_volume_used_bytes Volume used capacity in bytes"));
            metrics.push(format!("worker_volume_used_bytes{{{}}} {}", labels, volume.used_bytes));
            metrics.push(format!(
                "# HELP worker_volume_available_bytes Volume available capacity in bytes"
            ));
            metrics.push(format!(
                "worker_volume_available_bytes{{{}}} {}",
                labels, volume.available_bytes
            ));
        }

        // Eviction metrics
        if let Some(eviction) = &self.eviction_manager {
            let m = eviction.metrics();
            metrics.push(format!("# HELP worker_eviction_total Total number of evictions"));
            metrics.push(format!("worker_eviction_total {}", m.get_eviction_total()));
            metrics.push(format!("# HELP worker_eviction_bytes_total Total bytes evicted"));
            metrics.push(format!("worker_eviction_bytes_total {}", m.get_eviction_bytes()));
            metrics.push(format!(
                "# HELP worker_watermark_trigger_total Total watermark triggers"
            ));
            metrics.push(format!(
                "worker_watermark_trigger_total {}",
                m.get_watermark_trigger_total()
            ));
            metrics.push(format!("# HELP worker_reject_write_total Total write rejections"));
            metrics.push(format!("worker_reject_write_total {}", m.get_reject_write_total()));
        }

        // Orphan metrics
        if let Some(orphan) = &self.orphan_manager {
            let m = orphan.metrics();
            metrics.push(format!("# HELP worker_orphan_found_total Total orphans found"));
            metrics.push(format!("worker_orphan_found_total {}", m.get_orphan_found_total()));
            metrics.push(format!("# HELP worker_orphan_deleted_total Total orphans deleted"));
            metrics.push(format!("worker_orphan_deleted_total {}", m.get_orphan_deleted_total()));
            metrics.push(format!("# HELP worker_reconcile_runs_total Total reconcile runs"));
            metrics.push(format!("worker_reconcile_runs_total {}", m.get_reconcile_runs_total()));
            metrics.push(format!(
                "# HELP worker_reconcile_diff_total Total reconcile differences"
            ));
            metrics.push(format!("worker_reconcile_diff_total {}", m.get_reconcile_diff_total()));
        }

        // Volume health metrics
        if let Some(health) = &self.volume_health_manager {
            let m = health.metrics();
            metrics.push(format!("# HELP worker_volume_failed_total Total volume failures"));
            metrics.push(format!("worker_volume_failed_total {}", m.get_volume_failed_total()));
            metrics.push(format!(
                "# HELP worker_volume_recover_attempts_total Total recovery attempts"
            ));
            metrics.push(format!(
                "worker_volume_recover_attempts_total {}",
                m.get_recover_attempts_total()
            ));
            metrics.push(format!(
                "# HELP worker_volume_recover_success_total Total successful recoveries"
            ));
            metrics.push(format!(
                "worker_volume_recover_success_total {}",
                m.get_recover_success_total()
            ));
        }

        metrics.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_response_serialization() {
        let response = HealthResponse {
            state: "Running".to_string(),
            healthy: true,
            group_sessions: vec![],
            volumes: vec![],
            queue_depths: QueueDepths {
                audit_queue: 0,
                write_queue: 0,
                read_queue: 0,
            },
            timestamp: "2024-01-01T00:00:00Z".to_string(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("healthy"));
    }
}
