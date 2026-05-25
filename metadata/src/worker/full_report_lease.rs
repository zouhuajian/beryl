// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Full block report lease manager evolved from the slot manager.
//!
//! This module implements a lease-based mechanism for controlling full block report concurrency,
//! replacing the previous slot-based approach. Leases include epoch and TTL for
//! better consistency across leader changes and route updates.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use types::group_watermark::MountEpoch;
use types::ids::WorkerId;

/// Full block report lease.
#[derive(Clone, Debug)]
pub struct FullReportLease {
    /// Worker ID that holds this lease.
    pub worker_id: WorkerId,
    /// Target metadata epoch (must match current leader's epoch).
    pub target_metadata_epoch: u64,
    /// Mount epoch (optional, for route consistency).
    pub mount_epoch: Option<MountEpoch>,
    /// Expiration timestamp (milliseconds since epoch).
    pub expire_ms: u64,
}

impl FullReportLease {
    /// Check if lease is expired.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        now_ms >= self.expire_ms
    }

    /// Check if lease is valid for the given metadata epoch and mount epoch.
    pub fn is_valid(&self, metadata_epoch: u64, mount_epoch: Option<MountEpoch>, now_ms: u64) -> bool {
        if self.is_expired(now_ms) {
            return false;
        }
        if self.target_metadata_epoch != metadata_epoch {
            return false;
        }
        if let (Some(lease_mount_epoch), Some(current_mount_epoch)) = (self.mount_epoch, mount_epoch) {
            if lease_mount_epoch.as_u64() != current_mount_epoch.as_u64() {
                return false;
            }
        }
        true
    }
}

/// Full block report lease manager.
///
/// Manages leases for full block reports to prevent storm and ensure consistency.
/// Leases are leader-only (in-memory) and can be recovered on leader change.
pub struct FullReportLeaseManager {
    /// Active leases: token -> lease.
    leases: Arc<RwLock<HashMap<u64, FullReportLease>>>,
    /// Worker to token mapping: worker_id -> token (for quick lookup).
    worker_tokens: Arc<RwLock<HashMap<WorkerId, u64>>>,
}

impl FullReportLeaseManager {
    /// Create a new FullReportLeaseManager.
    pub fn new(_max_concurrent: usize, _lease_ttl_ms: u64) -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
            worker_tokens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Verify and release a lease.
    ///
    /// Returns true if lease is valid and released, false otherwise.
    pub async fn verify_and_release(
        &self,
        token: u64,
        worker_id: WorkerId,
        metadata_epoch: u64,
        mount_epoch: Option<MountEpoch>,
    ) -> bool {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let mut leases = self.leases.write().await;
        let mut worker_tokens = self.worker_tokens.write().await;

        if let Some(lease) = leases.get(&token) {
            // Verify lease ownership and validity
            if lease.worker_id != worker_id {
                warn!(
                    token,
                    lease_worker_id = lease.worker_id.as_raw(),
                    request_worker_id = worker_id.as_raw(),
                    "Lease token worker_id mismatch"
                );
                return false;
            }

            if !lease.is_valid(metadata_epoch, mount_epoch, now_ms) {
                warn!(
                    token,
                    worker_id = worker_id.as_raw(),
                    "Lease token invalid (expired or epoch mismatch)"
                );
                // Remove invalid lease
                leases.remove(&token);
                worker_tokens.remove(&worker_id);
                return false;
            }

            // Release lease
            leases.remove(&token);
            worker_tokens.remove(&worker_id);

            debug!(token, worker_id = worker_id.as_raw(), "Released full report lease");

            true
        } else {
            warn!(token, worker_id = worker_id.as_raw(), "Lease token not found");
            false
        }
    }

    /// Get current active lease count (for metrics).
    pub async fn active_lease_count(&self) -> usize {
        let leases = self.leases.read().await;
        leases.len()
    }
}
