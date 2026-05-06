// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Inflight registry: per-block single-flight tracking across maintenance operations.
//!
//! This module provides a unified registry to prevent concurrent operations on the same block,
//! ensuring that repair and delete operations don't interfere with each other.

use crate::error::MetadataResult;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};
use types::ids::BlockId;

/// Operation kind for inflight tracking.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InflightKind {
    /// Repair operation (replication, move, etc.).
    Repair,
    /// Delete operation (evict, remove).
    Delete,
    /// Over-replicated replica eviction (delete excess replicas).
    OverRepEvict,
}

impl InflightKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            InflightKind::Repair => "repair",
            InflightKind::Delete => "delete",
            InflightKind::OverRepEvict => "overrep_evict",
        }
    }

    /// Get priority (higher number = higher priority).
    /// Data-loss repair has highest priority, delete has lowest.
    pub fn priority(&self) -> u8 {
        match self {
            InflightKind::Repair => 1,       // Higher priority: data-loss repair
            InflightKind::OverRepEvict => 0, // Same as Delete: replica eviction
            InflightKind::Delete => 0,       // Lower priority: deletion
        }
    }
}

/// Inflight entry.
#[derive(Clone, Debug)]
struct InflightEntry {
    kind: InflightKind,
    since_ms: u64,
    ttl_ms: u64,
}

/// Inflight registry for per-block single-flight tracking.
pub struct InflightRegistry {
    /// Map: block_id -> (kind, since_ms, ttl_ms)
    entries: Arc<RwLock<HashMap<BlockId, InflightEntry>>>,
    /// Next operation ID (for tracking).
    _next_op_id: Arc<AtomicU64>,
    /// Default TTL for operations (milliseconds).
    default_ttl_ms: u64,
}

impl InflightRegistry {
    /// Create a new inflight registry.
    pub fn new(default_ttl_ms: u64) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            _next_op_id: Arc::new(AtomicU64::new(1)),
            default_ttl_ms,
        }
    }

    /// Try to acquire a lock for a block operation.
    ///
    /// Returns:
    /// - `Ok(true)` if acquired successfully
    /// - `Ok(false)` if blocked by existing operation (with higher or equal priority)
    /// - `Err` if internal error
    pub fn try_acquire(&self, block_id: BlockId, kind: InflightKind, ttl_ms: Option<u64>) -> MetadataResult<bool> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let ttl = ttl_ms.unwrap_or(self.default_ttl_ms);
        let _expire_ms = now_ms + ttl;

        let mut entries = self.entries.write();

        // Reap expired entries first
        self.reap_expired_internal(&mut entries, now_ms);

        // Check if block is already in-flight
        if let Some(existing) = entries.get(&block_id) {
            // Check if existing operation has higher or equal priority
            if existing.kind.priority() >= kind.priority() {
                debug!(
                    block_id = %block_id,
                    existing_kind = existing.kind.as_str(),
                    requested_kind = kind.as_str(),
                    existing_priority = existing.kind.priority(),
                    requested_priority = kind.priority(),
                    "Block already in-flight with higher/equal priority operation"
                );
                return Ok(false);
            }

            // Lower priority operation exists - we can preempt it
            warn!(
                block_id = %block_id,
                existing_kind = existing.kind.as_str(),
                requested_kind = kind.as_str(),
                "Preempting lower priority operation"
            );
        }

        // Acquire lock
        entries.insert(
            block_id,
            InflightEntry {
                kind,
                since_ms: now_ms,
                ttl_ms: ttl,
            },
        );

        debug!(
            block_id = %block_id,
            kind = kind.as_str(),
            ttl_ms = ttl,
            "Acquired inflight lock"
        );

        Ok(true)
    }

    /// Release a lock for a block operation.
    pub fn release(&self, block_id: BlockId) {
        let mut entries = self.entries.write();
        if entries.remove(&block_id).is_some() {
            debug!(block_id = %block_id, "Released inflight lock");
        }
    }

    /// Check if a block is in-flight (any operation).
    #[cfg(test)]
    fn is_inflight(&self, block_id: BlockId) -> bool {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let entries = self.entries.read();
        if let Some(entry) = entries.get(&block_id) {
            // Check if expired
            if now_ms >= entry.since_ms + entry.ttl_ms {
                return false; // Expired, consider as not in-flight
            }
            return true;
        }
        false
    }

    /// Get the kind of operation currently in-flight for a block.
    #[cfg(test)]
    fn get_inflight_kind(&self, block_id: BlockId) -> Option<InflightKind> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let entries = self.entries.read();
        if let Some(entry) = entries.get(&block_id) {
            // Check if expired
            if now_ms >= entry.since_ms + entry.ttl_ms {
                return None; // Expired
            }
            return Some(entry.kind);
        }
        None
    }

    /// Reap expired entries (internal, requires write lock).
    fn reap_expired_internal(&self, entries: &mut HashMap<BlockId, InflightEntry>, now_ms: u64) {
        entries.retain(|block_id, entry| {
            let expired = now_ms >= entry.since_ms + entry.ttl_ms;
            if expired {
                debug!(
                    block_id = %block_id,
                    kind = entry.kind.as_str(),
                    "Reaping expired inflight entry"
                );
            }
            !expired
        });
    }

    /// Reap expired entries (public API).
    #[cfg(test)]
    fn reap_expired(&self) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let mut entries = self.entries.write();
        self.reap_expired_internal(&mut entries, now_ms);
    }

    /// Get count of in-flight operations.
    #[cfg(test)]
    fn get_inflight_count(&self) -> usize {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let entries = self.entries.read();
        entries
            .iter()
            .filter(|(_, entry)| now_ms < entry.since_ms + entry.ttl_ms)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::{InflightKind, InflightRegistry};
    use types::ids::{BlockId, BlockIndex, DataHandleId};

    fn block_id(raw: u64) -> BlockId {
        BlockId::new(DataHandleId::new(raw), BlockIndex::new(0))
    }

    #[test]
    fn higher_priority_operation_preempts_lower_priority_inflight_entry() {
        let registry = InflightRegistry::new(5 * 60 * 1000);
        let block_id = block_id(1);

        assert!(registry.try_acquire(block_id, InflightKind::Delete, None).unwrap());
        assert!(registry.try_acquire(block_id, InflightKind::Repair, None).unwrap());
        assert_eq!(registry.get_inflight_kind(block_id), Some(InflightKind::Repair));

        registry.release(block_id);
        assert!(registry.try_acquire(block_id, InflightKind::Delete, None).unwrap());
    }

    #[test]
    fn lower_priority_operation_is_blocked_by_higher_priority_inflight_entry() {
        let registry = InflightRegistry::new(5 * 60 * 1000);
        let block_id = block_id(2);

        assert!(registry.try_acquire(block_id, InflightKind::Repair, None).unwrap());
        assert!(!registry.try_acquire(block_id, InflightKind::Delete, None).unwrap());
        assert_eq!(registry.get_inflight_kind(block_id), Some(InflightKind::Repair));
    }

    #[test]
    fn expired_entries_do_not_count_as_inflight() {
        let registry = InflightRegistry::new(0);
        let block_id = block_id(3);

        assert!(registry.try_acquire(block_id, InflightKind::Repair, Some(0)).unwrap());
        registry.reap_expired();

        assert!(!registry.is_inflight(block_id));
        assert_eq!(registry.get_inflight_count(), 0);
    }
}
