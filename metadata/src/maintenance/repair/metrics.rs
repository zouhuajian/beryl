// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metrics owned by the maintenance repair queue.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Repair queue and task metrics.
pub(crate) struct RepairMetrics {
    // Gauges
    pub(crate) queue_pending: AtomicUsize,
    pub(crate) queue_inflight: AtomicUsize,
    pub(crate) queue_total: AtomicUsize,

    // Counters
    pub(crate) task_enqueued_total: AtomicU64,
    pub(crate) task_acked_total: AtomicU64,
    pub(crate) task_timeout_total: AtomicU64,
    pub(crate) task_retry_total: AtomicU64,
    pub(crate) task_failed_total: AtomicU64,
    pub(crate) task_dedup_skipped_total: AtomicU64,
}

impl RepairMetrics {
    pub(crate) fn new() -> Self {
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

    pub(crate) fn update_queue_pending(&self, count: usize) {
        self.queue_pending.store(count, Ordering::Relaxed);
    }

    pub(crate) fn update_queue_inflight(&self, count: usize) {
        self.queue_inflight.store(count, Ordering::Relaxed);
    }

    pub(crate) fn update_queue_total(&self, count: usize) {
        self.queue_total.store(count, Ordering::Relaxed);
    }

    pub(crate) fn inc_task_enqueued(&self) {
        self.task_enqueued_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_task_acked(&self) {
        self.task_acked_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_task_timeout(&self) {
        self.task_timeout_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_task_retry(&self) {
        self.task_retry_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_task_failed(&self) {
        self.task_failed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_task_dedup_skipped(&self) {
        self.task_dedup_skipped_total.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for RepairMetrics {
    fn default() -> Self {
        Self::new()
    }
}
