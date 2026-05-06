// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metrics for worker management and block reporting.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Worker management metrics.
pub struct WorkerMetrics {
    pub worker_live: AtomicUsize,
    pub blockreport_blocks_total: AtomicU64,
    pub locations_size: AtomicUsize,
    pub orphan_queue_len: AtomicUsize,
    pub repair_queue_len: AtomicUsize,
}

impl WorkerMetrics {
    pub fn new() -> Self {
        Self {
            worker_live: AtomicUsize::new(0),
            blockreport_blocks_total: AtomicU64::new(0),
            locations_size: AtomicUsize::new(0),
            orphan_queue_len: AtomicUsize::new(0),
            repair_queue_len: AtomicUsize::new(0),
        }
    }

    pub fn update_worker_live(&self, count: usize) {
        self.worker_live.store(count, Ordering::Relaxed);
    }

    pub fn record_blockreport_blocks(&self, count: u64) {
        self.blockreport_blocks_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn update_locations_size(&self, size: usize) {
        self.locations_size.store(size, Ordering::Relaxed);
    }

    pub fn update_orphan_queue_len(&self, len: usize) {
        self.orphan_queue_len.store(len, Ordering::Relaxed);
    }

    pub fn update_repair_queue_len(&self, len: usize) {
        self.repair_queue_len.store(len, Ordering::Relaxed);
    }
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        Self::new()
    }
}
