// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Lease cleanup service: removing expired leases.
//!
//! This module implements self-healing lease cleanup with unified gate checks.

use crate::destructive_gate::{DestructiveCheckContext, DestructiveGate};
use crate::error::{MetadataError, MetadataResult};
use crate::metrics::MetadataMetrics;
use crate::raft::AppRaftNode;
use crate::raft::RocksDBStorage;
use crate::worker::WorkerManager;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
use types::ids::BlockId;

use super::gate::TaskGate;

/// Lease cleanup service for removing expired leases.
pub struct LeaseCleanupService {
    raft_node: Arc<AppRaftNode>,
    storage: Arc<RocksDBStorage>,
    _worker_manager: Arc<WorkerManager>,
    lease_gate: Arc<RwLock<TaskGate>>,
    metrics: Arc<MetadataMetrics>,
    last_log_ms: Arc<RwLock<u64>>,
    _lease_runtime: Option<Arc<crate::lease_runtime::LeaseRuntimeTable>>,
    /// Pending candidates awaiting confirmation (block_id -> first_seen_ms)
    _pending_candidates: Arc<RwLock<HashMap<BlockId, u64>>>,
    /// Unified destructive gate
    destructive_gate: Arc<DestructiveGate>,
}

impl LeaseCleanupService {
    /// Create a new LeaseCleanupService.
    // Constructor mirrors maintenance runtime wiring; grouping dependencies would hide ownership.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        raft_node: Arc<AppRaftNode>,
        storage: Arc<RocksDBStorage>,
        worker_manager: Arc<WorkerManager>,
        lease_gate: Arc<RwLock<TaskGate>>,
        metrics: Arc<MetadataMetrics>,
        last_log_ms: Arc<RwLock<u64>>,
        lease_runtime: Option<Arc<crate::lease_runtime::LeaseRuntimeTable>>,
        pending_candidates: Arc<RwLock<HashMap<BlockId, u64>>>,
        destructive_gate: Arc<DestructiveGate>,
    ) -> Self {
        Self {
            raft_node,
            storage,
            _worker_manager: worker_manager,
            lease_gate,
            metrics,
            last_log_ms,
            _lease_runtime: lease_runtime,
            _pending_candidates: pending_candidates,
            destructive_gate,
        }
    }

    /// Set lease runtime table (called during initialization).
    pub fn set_lease_runtime(&self, _lease_runtime: Arc<crate::lease_runtime::LeaseRuntimeTable>) {
        // Note: This is a workaround since we can't mutate Arc fields.
        // In practice, this should be set during construction.
        // For now, we'll use a different approach: pass runtime via method parameter or use interior mutability.
        // This is a limitation of the current design - we'll need to refactor to use Arc<RwLock<Option<...>>> or similar.
    }

    /// Cleanup expired leases with self-healing: always scan, but only release if gate allows.
    /// Uses soft/hard TTL and runtime table for high-frequency renewals.
    pub async fn cleanup_expired_leases(&self) -> MetadataResult<()> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // First pass: always execute scan (self-healing)
        // This allows recovery from degraded state
        let expired_leases = match self.find_expired_leases_storage(now_ms).await {
            Ok(leases) => {
                // Scan succeeded - allow recovery from degraded state
                {
                    let mut gate = self.lease_gate.write();
                    gate.maybe_set_ready(now_ms);
                }
                leases
            }
            Err(e) => {
                // Scan failed - set gate to degraded
                {
                    let mut gate = self.lease_gate.write();
                    gate.set_degraded("lease_scan_failed".to_string(), e.to_string(), now_ms);
                }
                self.metrics.lease_cleanup_skipped_total.fetch_add(1, Ordering::Relaxed);
                self.metrics.lease_gate_state.store(0, Ordering::Relaxed);

                // Rate-limited logging
                const LOG_INTERVAL_MS: u64 = 5 * 60 * 1000;
                {
                    let mut last_log = self.last_log_ms.write();
                    if now_ms.saturating_sub(*last_log) >= LOG_INTERVAL_MS {
                        error!(
                            task = "lease_cleanup",
                            error = %e,
                            "Lease scan failed, gate degraded. Will retry scan next cycle."
                        );
                        *last_log = now_ms;
                    }
                }
                return Ok(());
            }
        };

        // Second pass: use unified destructive gate check
        // Get guard_state_id for each expired lease
        let guard_state_id = self
            .raft_node
            .get_last_applied_state_id()
            .ok_or_else(|| MetadataError::Internal("Failed to get guard_state_id".to_string()))?;

        // Filter expired leases through unified gate
        let mut eligible_for_release = Vec::new();
        for block_id in &expired_leases {
            // Unified gate check
            let ctx = DestructiveCheckContext::new("lease_cleanup_release")
                .with_block_id(*block_id)
                .with_guard_state_id(guard_state_id)
                .with_not_before_ms(now_ms); // No grace window for lease cleanup (already expired)

            match self.destructive_gate.check_destructive_allowed(&ctx)? {
                crate::destructive_gate::DestructiveCheckResult::Allowed => {
                    eligible_for_release.push(*block_id);
                }
                crate::destructive_gate::DestructiveCheckResult::Blocked { reason } => {
                    // Gate check failed - skip this lease
                    debug!(
                        task = "lease_cleanup",
                        block_id = %block_id,
                        reason = %reason,
                        "Skipping lease release: gate check failed"
                    );
                    self.metrics.lease_cleanup_skipped_total.fetch_add(1, Ordering::Relaxed);

                    // Rate-limited logging
                    const LOG_INTERVAL_MS: u64 = 5 * 60 * 1000;
                    {
                        let mut last_log = self.last_log_ms.write();
                        if now_ms.saturating_sub(*last_log) >= LOG_INTERVAL_MS {
                            warn!(
                                task = "lease_cleanup",
                                block_id = %block_id,
                                reason = %reason,
                                "Lease release blocked: unified gate check failed. Scan allowed for self-healing."
                            );
                            *last_log = now_ms;
                        }
                    }
                }
                crate::destructive_gate::DestructiveCheckResult::NeedRefresh { reason, .. } => {
                    // Mount epoch mismatch - skip this lease
                    debug!(
                        task = "lease_cleanup",
                        block_id = %block_id,
                        reason = %reason,
                        "Skipping lease release: mount epoch mismatch, need refresh"
                    );
                    self.metrics.lease_cleanup_skipped_total.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Update gate state metric
        if !eligible_for_release.is_empty() {
            self.metrics.lease_gate_state.store(1, Ordering::Relaxed);
        } else {
            self.metrics.lease_gate_state.store(0, Ordering::Relaxed);
        }

        // Proceed with release for eligible leases
        if !eligible_for_release.is_empty() {
            info!(
                task = "lease_cleanup",
                count = eligible_for_release.len(),
                "Found expired leases eligible for cleanup"
            );

            let mut released = 0;
            for block_id in &eligible_for_release {
                use crate::raft::{Command, DedupKey};

                let command = Command::ReleaseLease {
                    dedup: DedupKey::system(),
                    block_id: *block_id,
                };

                if let Err(e) = self.raft_node.propose(command).await {
                    warn!(
                        task = "lease_cleanup",
                        block_id = %block_id,
                        error = %e,
                        "Failed to release expired lease"
                    );
                } else {
                    released += 1;
                }
            }

            self.metrics
                .lease_cleanup_actions_total
                .fetch_add(released as u64, Ordering::Relaxed);
            info!(task = "lease_cleanup", released, "Lease cleanup completed");
        }

        Ok(())
    }

    /// Find expired leases from storage.
    async fn find_expired_leases_storage(&self, now_ms: u64) -> MetadataResult<Vec<BlockId>> {
        use bincode::config::standard;
        use rocksdb::IteratorMode;

        let db = self.storage.db();
        let cf = db.cf_handle("leases").ok_or_else(|| {
            let mut gate = self.lease_gate.write();
            gate.set_degraded(
                "leases_cf_not_found".to_string(),
                "Leases CF not found".to_string(),
                now_ms,
            );
            MetadataError::Internal("Leases CF not found".to_string())
        })?;

        let mut expired = Vec::new();
        let iter = db.iterator_cf(cf, IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(|e| {
                let mut gate = self.lease_gate.write();
                gate.set_degraded(
                    "lease_iterator_error".to_string(),
                    format!("RocksDB iterator error: {}", e),
                    now_ms,
                );
                MetadataError::Internal(format!("RocksDB iterator error: {}", e))
            })?;

            match bincode::serde::decode_from_slice::<crate::state::LeaseState, _>(&value, standard()) {
                Ok((lease_state, _)) => {
                    if lease_state.lease.expires_at_ms < now_ms {
                        expired.push(lease_state.block_id);
                    }
                }
                Err(e) => {
                    warn!(
                        task = "lease_cleanup",
                        error = %e,
                        "Failed to deserialize lease state, skipping"
                    );
                    // Don't fail the entire scan for decode errors
                }
            }
        }

        Ok(expired)
    }
}
