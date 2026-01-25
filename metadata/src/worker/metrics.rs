// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metrics for worker management and block reporting.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Worker management metrics.
pub struct WorkerMetrics {
    pub worker_live: AtomicUsize,
    pub heartbeat_latency_ms: AtomicU64,
    pub blockreport_blocks_total: AtomicU64,
    pub locations_size: AtomicUsize,
    pub orphan_queue_len: AtomicUsize,
    pub repair_queue_len: AtomicUsize,
}

/// Repair queue and task metrics.
pub struct RepairMetrics {
    // Gauges
    pub queue_pending: AtomicUsize,
    pub queue_inflight: AtomicUsize,
    pub queue_total: AtomicUsize,

    // Counters
    pub task_enqueued_total: AtomicU64,
    pub task_acked_total: AtomicU64,
    pub task_timeout_total: AtomicU64,
    pub task_retry_total: AtomicU64,
    pub task_failed_total: AtomicU64,
    pub task_dedup_skipped_total: AtomicU64,
}

impl WorkerMetrics {
    pub fn new() -> Self {
        Self {
            worker_live: AtomicUsize::new(0),
            heartbeat_latency_ms: AtomicU64::new(0),
            blockreport_blocks_total: AtomicU64::new(0),
            locations_size: AtomicUsize::new(0),
            orphan_queue_len: AtomicUsize::new(0),
            repair_queue_len: AtomicUsize::new(0),
        }
    }

    pub fn update_worker_live(&self, count: usize) {
        self.worker_live.store(count, Ordering::Relaxed);
    }

    pub fn record_heartbeat_latency(&self, ms: u64) {
        self.heartbeat_latency_ms.store(ms, Ordering::Relaxed);
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

impl RepairMetrics {
    pub fn new() -> Self {
        Self {
            queue_pending: AtomicUsize::new(0),
            queue_inflight: AtomicUsize::new(0),
            queue_total: AtomicUsize::new(0),
            task_enqueued_total: AtomicU64::new(0),
            task_acked_total: AtomicU64::new(0),
            task_timeout_total: AtomicU64::new(0),
            task_retry_total: AtomicU64::new(0),
            task_failed_total: AtomicU64::new(0),
            task_dedup_skipped_total: AtomicU64::new(0),
        }
    }

    pub fn update_queue_pending(&self, count: usize) {
        self.queue_pending.store(count, Ordering::Relaxed);
    }

    pub fn update_queue_inflight(&self, count: usize) {
        self.queue_inflight.store(count, Ordering::Relaxed);
    }

    pub fn update_queue_total(&self, count: usize) {
        self.queue_total.store(count, Ordering::Relaxed);
    }

    pub fn inc_task_enqueued(&self) {
        self.task_enqueued_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_task_acked(&self) {
        self.task_acked_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_task_timeout(&self) {
        self.task_timeout_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_task_retry(&self) {
        self.task_retry_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_task_failed(&self) {
        self.task_failed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_task_dedup_skipped(&self) {
        self.task_dedup_skipped_total.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for RepairMetrics {
    fn default() -> Self {
        Self::new()
    }
}
