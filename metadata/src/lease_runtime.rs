// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Lease runtime table: in-memory lease state for high-frequency renewals.
//!
//! This module implements a leader-only, in-memory table for tracking lease renewals.
//! RenewLease operations update this table without writing to Raft, allowing high QPS.
//!
//! Authoritative lease operations (Create/Release/Recover) still go through Raft.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};
use types::ids::{BlockId, ClientId};

/// Lease runtime entry (in-memory only).
#[derive(Clone, Debug)]
pub struct LeaseRuntimeEntry {
    /// Last renewal timestamp (milliseconds since epoch).
    pub last_renew_ms: u64,
    /// Current deadline (milliseconds since epoch).
    pub deadline_ms: u64,
    /// Last seen client ID (for tracking).
    pub last_seen_client_id: Option<ClientId>,
}

/// Lease runtime table (leader-only, in-memory).
pub struct LeaseRuntimeTable {
    /// Runtime entries: block_id -> LeaseRuntimeEntry
    entries: Arc<RwLock<HashMap<BlockId, LeaseRuntimeEntry>>>,
    /// Whether runtime is ready (warmup completed).
    is_ready: Arc<AtomicBool>,
    /// When warmup started (milliseconds since epoch).
    warmup_start_ms: Arc<AtomicU64>,
    /// Hard TTL for leases (milliseconds).
    hard_ttl_ms: u64,
    /// Soft TTL for leases (milliseconds, used for suspect marking).
    soft_ttl_ms: u64,
    /// Warmup window (milliseconds) - during this window, destructive actions are blocked.
    warmup_window_ms: u64,
}

impl LeaseRuntimeTable {
    /// Create a new lease runtime table.
    pub fn new(hard_ttl_ms: u64, soft_ttl_ms: u64, warmup_window_ms: u64) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            is_ready: Arc::new(AtomicBool::new(false)),
            warmup_start_ms: Arc::new(AtomicU64::new(0)),
            hard_ttl_ms,
            soft_ttl_ms,
            warmup_window_ms,
        }
    }

    /// Check if runtime is ready (warmup completed).
    pub fn is_runtime_ready(&self) -> bool {
        self.is_ready.load(Ordering::Acquire)
    }

    /// Start warmup: mark warmup start time and clear ready flag.
    pub fn start_warmup(&self) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        self.warmup_start_ms.store(now_ms, Ordering::Release);
        self.is_ready.store(false, Ordering::Release);
        info!(warmup_window_ms = self.warmup_window_ms, "Lease runtime warmup started");
    }

    /// Complete warmup: mark runtime as ready.
    pub fn complete_warmup(&self) {
        self.is_ready.store(true, Ordering::Release);
        let warmup_duration_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
            - self.warmup_start_ms.load(Ordering::Acquire);
        info!(
            warmup_duration_ms,
            entries_count = self.entries.read().len(),
            "Lease runtime warmup completed"
        );
    }

    /// Check if warmup window has passed (allows destructive actions).
    pub fn is_warmup_window_passed(&self) -> bool {
        if self.is_ready.load(Ordering::Acquire) {
            return true;
        }
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let warmup_start = self.warmup_start_ms.load(Ordering::Acquire);
        if warmup_start == 0 {
            // Warmup not started yet
            return false;
        }
        now_ms.saturating_sub(warmup_start) >= self.warmup_window_ms
    }

    /// Renew lease (update runtime entry).
    /// Returns new deadline_ms and renew_after_ms.
    pub fn renew_lease(&self, block_id: BlockId, client_id: ClientId, now_ms: u64) -> (u64, u64) {
        let mut entries = self.entries.write();
        let new_deadline_ms = now_ms + self.hard_ttl_ms;
        let renew_after_ms = self.hard_ttl_ms / 2; // Recommend renew at half TTL

        let entry = entries.entry(block_id).or_insert_with(|| LeaseRuntimeEntry {
            last_renew_ms: now_ms,
            deadline_ms: new_deadline_ms,
            last_seen_client_id: Some(client_id),
        });

        // Update entry
        entry.last_renew_ms = now_ms;
        entry.deadline_ms = new_deadline_ms;
        entry.last_seen_client_id = Some(client_id);

        (new_deadline_ms, renew_after_ms)
    }

    /// Get runtime entry for a block.
    pub fn get_entry(&self, block_id: BlockId) -> Option<LeaseRuntimeEntry> {
        let entries = self.entries.read();
        entries.get(&block_id).cloned()
    }

    /// Initialize runtime from authoritative lease storage (during warmup).
    pub fn warmup_from_storage(
        &self,
        leases: Vec<(BlockId, u64)>, // (block_id, expires_at_ms)
        now_ms: u64,
    ) {
        let mut entries = self.entries.write();
        entries.clear();

        for (block_id, expires_at_ms) in leases {
            // Only include leases that haven't expired yet
            if expires_at_ms > now_ms {
                entries.insert(
                    block_id,
                    LeaseRuntimeEntry {
                        last_renew_ms: now_ms, // Approximate
                        deadline_ms: expires_at_ms,
                        last_seen_client_id: None,
                    },
                );
            }
        }

        debug!(
            entries_loaded = entries.len(),
            "Lease runtime warmup: loaded entries from storage"
        );
    }

    /// Remove entry (when lease is released/recovered).
    pub fn remove_entry(&self, block_id: BlockId) {
        let mut entries = self.entries.write();
        entries.remove(&block_id);
    }

    /// Find soft-expired leases (expired in runtime but not yet hard-expired).
    /// Returns (block_id, entry) pairs.
    pub fn find_soft_expired(&self, now_ms: u64) -> Vec<(BlockId, LeaseRuntimeEntry)> {
        let entries = self.entries.read();
        let mut soft_expired = Vec::new();

        for (block_id, entry) in entries.iter() {
            // Soft-expired: runtime deadline passed, but within soft TTL window
            if entry.deadline_ms < now_ms && now_ms.saturating_sub(entry.deadline_ms) < self.soft_ttl_ms {
                soft_expired.push((*block_id, entry.clone()));
            }
        }

        soft_expired
    }

    /// Find hard-expired leases (expired beyond soft TTL).
    /// Returns (block_id, entry) pairs.
    pub fn find_hard_expired(&self, now_ms: u64) -> Vec<(BlockId, LeaseRuntimeEntry)> {
        let entries = self.entries.read();
        let mut hard_expired = Vec::new();

        for (block_id, entry) in entries.iter() {
            // Hard-expired: runtime deadline passed and beyond soft TTL window
            if entry.deadline_ms < now_ms && now_ms.saturating_sub(entry.deadline_ms) >= self.soft_ttl_ms {
                hard_expired.push((*block_id, entry.clone()));
            }
        }

        hard_expired
    }

    /// Get all entries (for metrics/debugging).
    pub fn get_entries_count(&self) -> usize {
        let entries = self.entries.read();
        entries.len()
    }

    /// Clear all entries (for testing or when leader relinquishes role).
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        entries.clear();
        self.is_ready.store(false, Ordering::Release);
        self.warmup_start_ms.store(0, Ordering::Release);
    }
}
