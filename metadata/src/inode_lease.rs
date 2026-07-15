// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Inode-level lease management for write path and truncate support.
//!
//! This module manages write leases at the inode level (not block level).
//! Leases provide mutual exclusion for writers and enable fencing.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{interval, Duration};
use tracing::{debug, warn};
use types::fs::InodeId;
use types::ids::{ClientId, LeaseId};

/// Write mode for lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Write mode (future: can write at any offset).
    Write,
    /// Append mode (writes must start from file_size).
    Append,
}

/// Active lease entry (runtime-only, not persisted to Raft).
#[derive(Clone, Debug)]
pub struct ActiveLease {
    /// Lease ID.
    pub lease_id: LeaseId,
    /// Lease epoch (monotonically increasing).
    pub lease_epoch: u64,
    /// Owner client ID.
    pub owner_client_id: ClientId,
    /// Optional owner call ID (for diagnostics).
    pub owner_call_id: Option<types::CallId>,
    /// Expiration time (milliseconds since epoch).
    pub expires_at_ms: u64,
    /// Write mode.
    pub mode: WriteMode,
}

/// Inode lease manager (runtime, leader-only).
///
/// Lease state:
/// - Runtime: ActiveLease entries in memory (for fast renewals)
/// - Persisted: lease_epoch in InodeData::File (for fencing after restart)
///
/// Note: After metadata restart, memory table is lost. New writers can
/// acquire leases (lease_epoch increments), and old lease holders will
/// fail on commit due to fencing (lease_epoch mismatch).
pub struct LeaseManager {
    /// Active leases: inode_id -> ActiveLease.
    leases: Arc<RwLock<HashMap<InodeId, ActiveLease>>>,
    /// Lease TTL in milliseconds (default: 60 seconds).
    lease_ttl_ms: u64,
    /// Renewal interval for cleanup (default: 10 seconds).
    cleanup_interval_ms: u64,
}

impl LeaseManager {
    /// Create a new LeaseManager.
    pub fn new(lease_ttl_ms: u64, cleanup_interval_ms: u64) -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
            lease_ttl_ms,
            cleanup_interval_ms,
        }
    }

    /// Try to acquire a lease for an inode.
    ///
    /// Returns:
    /// - Ok((lease_id, lease_epoch, expires_at_ms)) if acquired
    /// - Err(EBusy) if there's an active, non-expired lease
    pub fn try_acquire(
        &self,
        inode_id: InodeId,
        client_id: ClientId,
        call_id: Option<types::CallId>,
        mode: WriteMode,
        current_lease_epoch: Option<u64>, // From inode (persisted)
    ) -> Result<(LeaseId, u64, u64), FsErrorCode> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let mut leases = self.leases.write();

        // Check for existing active lease
        if let Some(existing) = leases.get(&inode_id) {
            if now_ms < existing.expires_at_ms {
                // Active lease exists and not expired
                debug!(
                    inode_id = %inode_id,
                    existing_lease_id = ?existing.lease_id,
                    existing_epoch = existing.lease_epoch,
                    expires_at = existing.expires_at_ms,
                    "Lease conflict: active lease exists"
                );
                return Err(FsErrorCode::EBusy);
            }
            // Lease expired, can be stolen
            debug!(
                inode_id = %inode_id,
                expired_lease_id = ?existing.lease_id,
                "Lease expired, allowing steal"
            );
        }

        // Generate new lease
        let base_epoch = current_lease_epoch.unwrap_or(0);
        let new_epoch = base_epoch + 1;
        let lease_id = LeaseId::new((inode_id.as_raw() as u128) << 64 | (new_epoch as u128));
        let expires_at_ms = now_ms + self.lease_ttl_ms;

        let active_lease = ActiveLease {
            lease_id,
            lease_epoch: new_epoch,
            owner_client_id: client_id,
            owner_call_id: call_id,
            expires_at_ms,
            mode,
        };

        leases.insert(inode_id, active_lease.clone());

        debug!(
            inode_id = %inode_id,
            lease_id = ?lease_id,
            lease_epoch = new_epoch,
            expires_at = expires_at_ms,
            mode = ?mode,
            "Lease acquired"
        );

        Ok((lease_id, new_epoch, expires_at_ms))
    }

    /// Renew a lease (runtime-only, does not write to Raft).
    ///
    /// Returns:
    /// - Ok(expires_at_ms) if renewed
    /// - Err(EPerm) if lease_id/lease_epoch mismatch or expired
    pub fn renew(&self, inode_id: InodeId, lease_id: LeaseId, lease_epoch: u64) -> Result<u64, FsErrorCode> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let mut leases = self.leases.write();

        let active_lease = leases.get_mut(&inode_id).ok_or(FsErrorCode::EPerm)?;

        // Validate lease_id and lease_epoch
        if active_lease.lease_id != lease_id || active_lease.lease_epoch != lease_epoch {
            warn!(
                inode_id = %inode_id,
                expected_lease_id = ?active_lease.lease_id,
                got_lease_id = ?lease_id,
                expected_epoch = active_lease.lease_epoch,
                got_epoch = lease_epoch,
                "Lease renewal failed: mismatch"
            );
            return Err(FsErrorCode::EPerm);
        }

        // Check if already expired
        if now_ms >= active_lease.expires_at_ms {
            warn!(
                inode_id = %inode_id,
                lease_id = ?lease_id,
                "Lease renewal failed: already expired"
            );
            leases.remove(&inode_id);
            return Err(FsErrorCode::EPerm);
        }

        // Extend expiration
        active_lease.expires_at_ms = now_ms + self.lease_ttl_ms;

        debug!(
            inode_id = %inode_id,
            lease_id = ?lease_id,
            new_expires_at = active_lease.expires_at_ms,
            "Lease renewed"
        );

        Ok(active_lease.expires_at_ms)
    }

    /// Release a lease (called on close/commit or error).
    pub fn release(&self, inode_id: InodeId, lease_id: LeaseId, lease_epoch: u64) {
        let mut leases = self.leases.write();
        if let Some(active) = leases.get(&inode_id) {
            if active.lease_id == lease_id && active.lease_epoch == lease_epoch {
                leases.remove(&inode_id);
                debug!(
                    inode_id = %inode_id,
                    lease_id = ?lease_id,
                    "Lease released"
                );
            }
        }
    }

    /// Validate lease for commit/truncate (fencing check).
    ///
    /// Returns:
    /// - Ok(()) if lease is valid
    /// - Err(EPerm) if lease is invalid (mismatch or expired)
    pub fn validate_lease(&self, inode_id: InodeId, lease_id: LeaseId, lease_epoch: u64) -> Result<(), FsErrorCode> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let leases = self.leases.read();
        let active_lease = leases.get(&inode_id).ok_or(FsErrorCode::EPerm)?;

        // Check lease_id and lease_epoch match
        if active_lease.lease_id != lease_id || active_lease.lease_epoch != lease_epoch {
            warn!(
                inode_id = %inode_id,
                expected_lease_id = ?active_lease.lease_id,
                got_lease_id = ?lease_id,
                expected_epoch = active_lease.lease_epoch,
                got_epoch = lease_epoch,
                "Lease validation failed: mismatch (fencing)"
            );
            return Err(FsErrorCode::EPerm);
        }

        // Check expiration
        if now_ms >= active_lease.expires_at_ms {
            warn!(
                inode_id = %inode_id,
                lease_id = ?lease_id,
                "Lease validation failed: expired"
            );
            return Err(FsErrorCode::EPerm);
        }

        Ok(())
    }

    /// Get active lease for an inode (if any).
    pub fn get_active_lease(&self, inode_id: InodeId) -> Option<ActiveLease> {
        self.leases.read().get(&inode_id).cloned()
    }

    /// Check if an inode has an active, non-expired lease.
    pub fn has_active_lease(&self, inode_id: InodeId) -> bool {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        self.leases
            .read()
            .get(&inode_id)
            .map(|lease| now_ms < lease.expires_at_ms)
            .unwrap_or(false)
    }

    pub(crate) fn is_active_lease(&self, inode_id: InodeId, lease_id: LeaseId, lease_epoch: u64) -> bool {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        self.leases
            .read()
            .get(&inode_id)
            .map(|lease| lease.lease_id == lease_id && lease.lease_epoch == lease_epoch && now_ms < lease.expires_at_ms)
            .unwrap_or(false)
    }

    /// Clean up expired leases (should be called periodically).
    pub fn cleanup_expired(&self) -> usize {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let mut leases = self.leases.write();
        let expired: Vec<InodeId> = leases
            .iter()
            .filter(|(_, lease)| now_ms >= lease.expires_at_ms)
            .map(|(inode_id, _)| *inode_id)
            .collect();

        for inode_id in &expired {
            leases.remove(inode_id);
        }

        if !expired.is_empty() {
            debug!(count = expired.len(), "Cleaned up expired leases");
        }

        expired.len()
    }

    /// Start background cleanup task (tokio spawn).
    pub fn start_cleanup_task(self: Arc<Self>) {
        let interval_ms = self.cleanup_interval_ms;
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(interval_ms));
            loop {
                interval.tick().await;
                self.cleanup_expired();
            }
        });
    }
}

impl Default for LeaseManager {
    fn default() -> Self {
        Self::new(60_000, 10_000) // 60s TTL, 10s cleanup interval
    }
}

use types::fs::FsErrorCode;
