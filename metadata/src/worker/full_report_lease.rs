// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Full block report lease manager evolved from the slot manager.
//!
//! This module implements a lease-based mechanism for controlling full block report concurrency,
//! replacing the previous slot-based approach. Leases include token, epoch, and TTL for
//! better consistency across leader changes and route updates.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use types::group_watermark::MountEpoch;
use types::ids::{ShardGroupId, WorkerId};

/// Full block report lease.
#[derive(Clone, Debug)]
pub struct FullReportLease {
    /// Lease token (unique identifier).
    pub token: u64,
    /// Worker ID that holds this lease.
    pub worker_id: WorkerId,
    /// Shard group ID (or global if None).
    pub shard_group_id: Option<ShardGroupId>,
    /// Target metadata epoch (must match current leader's epoch).
    pub target_metadata_epoch: u64,
    /// Mount epoch (optional, for route consistency).
    pub mount_epoch: Option<MountEpoch>,
    /// Expiration timestamp (milliseconds since epoch).
    pub expire_ms: u64,
    /// Created timestamp (milliseconds since epoch).
    pub created_at_ms: u64,
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
    /// Next token ID (monotonically increasing).
    next_token: Arc<std::sync::atomic::AtomicU64>,
    /// Maximum concurrent leases (per shard_group or global).
    max_concurrent: usize,
    /// Lease TTL in milliseconds.
    lease_ttl_ms: u64,
}

impl FullReportLeaseManager {
    /// Create a new FullReportLeaseManager.
    pub fn new(max_concurrent: usize, lease_ttl_ms: u64) -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
            worker_tokens: Arc::new(RwLock::new(HashMap::new())),
            next_token: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            max_concurrent,
            lease_ttl_ms,
        }
    }

    /// Try to allocate a lease for a worker.
    ///
    /// Returns Some(token) if lease is allocated, None if max_concurrent reached.
    pub async fn try_allocate(
        &self,
        worker_id: WorkerId,
        shard_group_id: Option<ShardGroupId>,
        target_metadata_epoch: u64,
        mount_epoch: Option<MountEpoch>,
    ) -> Option<u64> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Cleanup expired leases first
        self.cleanup_expired(now_ms).await;

        let mut leases = self.leases.write().await;
        let mut worker_tokens = self.worker_tokens.write().await;

        // Check if worker already has a lease
        if let Some(&existing_token) = worker_tokens.get(&worker_id) {
            if let Some(lease) = leases.get(&existing_token) {
                if lease.is_valid(target_metadata_epoch, mount_epoch, now_ms) {
                    // Worker already has valid lease, return existing token
                    debug!(
                        worker_id = worker_id.as_raw(),
                        token = existing_token,
                        "Worker already has valid lease"
                    );
                    return Some(existing_token);
                } else {
                    // Existing lease is invalid, remove it
                    leases.remove(&existing_token);
                    worker_tokens.remove(&worker_id);
                }
            }
        }

        // Check concurrent limit (per shard_group or global)
        let active_count = if let Some(group_id) = shard_group_id {
            // Count leases for this shard_group
            leases
                .values()
                .filter(|lease| lease.shard_group_id == Some(group_id))
                .count()
        } else {
            // Count all leases (global)
            leases.len()
        };

        if active_count >= self.max_concurrent {
            debug!(
                worker_id = worker_id.as_raw(),
                shard_group_id = ?shard_group_id,
                active_count,
                max_concurrent = self.max_concurrent,
                "Max concurrent leases reached"
            );
            return None;
        }

        // Allocate new lease
        let token = self.next_token.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let expire_ms = now_ms + self.lease_ttl_ms;

        let lease = FullReportLease {
            token,
            worker_id,
            shard_group_id,
            target_metadata_epoch,
            mount_epoch,
            expire_ms,
            created_at_ms: now_ms,
        };

        leases.insert(token, lease.clone());
        worker_tokens.insert(worker_id, token);

        info!(
            worker_id = worker_id.as_raw(),
            token,
            shard_group_id = ?shard_group_id,
            target_metadata_epoch,
            expire_ms,
            "Allocated full report lease"
        );

        Some(token)
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

    /// Cleanup expired leases.
    pub async fn cleanup_expired(&self, now_ms: u64) {
        let mut leases = self.leases.write().await;
        let mut worker_tokens = self.worker_tokens.write().await;

        let expired_tokens: Vec<u64> = leases
            .iter()
            .filter(|(_, lease)| lease.is_expired(now_ms))
            .map(|(token, _)| *token)
            .collect();

        for token in expired_tokens {
            if let Some(lease) = leases.remove(&token) {
                worker_tokens.remove(&lease.worker_id);
                debug!(token, worker_id = lease.worker_id.as_raw(), "Cleaned up expired lease");
            }
        }
    }

    /// Get current active lease count (for metrics).
    pub async fn active_lease_count(&self) -> usize {
        let leases = self.leases.read().await;
        leases.len()
    }

    /// Invalidate all leases (e.g., on leader change).
    pub async fn invalidate_all(&self) {
        let mut leases = self.leases.write().await;
        let mut worker_tokens = self.worker_tokens.write().await;

        let count = leases.len();
        leases.clear();
        worker_tokens.clear();

        info!(count, "Invalidated all full report leases (leader change)");
    }
}
