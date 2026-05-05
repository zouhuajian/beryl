// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Orphan block queue: tracks blocks that exist on worker but not in metadata.

use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use types::ids::{BlockId, WorkerId};

/// Orphan entry with timestamp for min-age/grace period tracking.
#[derive(Clone, Debug)]
struct OrphanEntry {
    block_id: BlockId,
    worker_id: WorkerId,
    added_at_ms: u64,
}

/// Orphan block queue (blocks that exist on worker but not in metadata).
pub struct OrphanQueue {
    orphans: Arc<RwLock<VecDeque<OrphanEntry>>>,
    max_queue_size: usize,
    /// Minimum age in milliseconds before an orphan can be processed (grace period).
    min_age_ms: u64,
    /// Optional metrics collector for observability.
    metrics: Option<Arc<OrphanMetrics>>,
}

/// Metrics for orphan queue.
pub struct OrphanMetrics {
    pub queue_len: std::sync::atomic::AtomicUsize,
    pub orphan_added_total: std::sync::atomic::AtomicU64,
    pub orphan_dropped_total: std::sync::atomic::AtomicU64,
    pub orphan_processed_total: std::sync::atomic::AtomicU64,
}

impl OrphanMetrics {
    pub fn new() -> Self {
        Self {
            queue_len: std::sync::atomic::AtomicUsize::new(0),
            orphan_added_total: std::sync::atomic::AtomicU64::new(0),
            orphan_dropped_total: std::sync::atomic::AtomicU64::new(0),
            orphan_processed_total: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn update_queue_len(&self, len: usize) {
        self.queue_len.store(len, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn inc_orphan_added(&self) {
        self.orphan_added_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn inc_orphan_dropped(&self) {
        self.orphan_dropped_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn inc_orphan_processed(&self) {
        self.orphan_processed_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Default for OrphanMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl OrphanQueue {
    /// Create a new orphan queue with default min_age (1 minute).
    pub fn new(max_queue_size: usize) -> Self {
        Self::with_config(max_queue_size, 60_000) // Default: 1 minute grace period
    }

    /// Create a new orphan queue with custom min_age.
    pub fn with_config(max_queue_size: usize, min_age_ms: u64) -> Self {
        Self {
            orphans: Arc::new(RwLock::new(VecDeque::new())),
            max_queue_size,
            min_age_ms,
            metrics: None,
        }
    }

    /// Set metrics for observability.
    pub fn set_metrics(&mut self, metrics: Arc<OrphanMetrics>) {
        self.metrics = Some(metrics);
    }

    /// Add an orphan block.
    pub fn add(&self, block_id: BlockId, worker_id: WorkerId) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let mut orphans = self.orphans.write();

        if orphans.len() >= self.max_queue_size {
            tracing::warn!(
                block_id = %block_id,
                worker_id = worker_id.as_raw(),
                "Orphan queue is full, dropping orphan block"
            );
            if let Some(ref metrics) = self.metrics {
                metrics.inc_orphan_dropped();
            }
            return;
        }

        // Check for duplicate
        if orphans
            .iter()
            .any(|e| e.block_id == block_id && e.worker_id == worker_id)
        {
            return;
        }

        orphans.push_back(OrphanEntry {
            block_id,
            worker_id,
            added_at_ms: now_ms,
        });

        // Update metrics
        let len = orphans.len();
        drop(orphans);
        if let Some(ref metrics) = self.metrics {
            metrics.inc_orphan_added();
            metrics.update_queue_len(len);
        }
    }

    /// Get queue length.
    pub fn len(&self) -> usize {
        self.orphans.read().len()
    }

    /// Check if the queue has no entries.
    pub fn is_empty(&self) -> bool {
        self.orphans.read().is_empty()
    }

    /// Clear all orphans.
    pub fn clear(&self) {
        let mut orphans = self.orphans.write();
        orphans.clear();
        let len = orphans.len();
        drop(orphans);
        if let Some(ref metrics) = self.metrics {
            metrics.update_queue_len(len);
        }
    }

    /// Dequeue an orphan block that has passed the min-age grace period.
    /// Returns None if no eligible orphan is available.
    pub fn dequeue(&self) -> Option<(BlockId, WorkerId)> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let mut orphans = self.orphans.write();

        // Find first orphan that has passed min-age
        let eligible_idx = orphans.iter().position(|e| now_ms >= e.added_at_ms + self.min_age_ms);

        if let Some(idx) = eligible_idx {
            let entry = orphans.remove(idx).unwrap();
            let len = orphans.len();
            drop(orphans);
            if let Some(ref metrics) = self.metrics {
                metrics.inc_orphan_processed();
                metrics.update_queue_len(len);
            }
            Some((entry.block_id, entry.worker_id))
        } else {
            None
        }
    }

    /// Peek at the oldest orphan without removing it (for secondary confirmation).
    /// Returns None if no orphan is available or if the oldest hasn't passed min-age.
    pub fn peek_oldest(&self) -> Option<(BlockId, WorkerId)> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let orphans = self.orphans.read();
        orphans
            .front()
            .filter(|e| now_ms >= e.added_at_ms + self.min_age_ms)
            .map(|e| (e.block_id, e.worker_id))
    }

    /// Get the number of eligible orphans (passed min-age).
    pub fn len_eligible(&self) -> usize {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let orphans = self.orphans.read();
        orphans
            .iter()
            .filter(|e| now_ms >= e.added_at_ms + self.min_age_ms)
            .count()
    }
}
