// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Repair queue: state machine, deduplication, and retry for repair tasks.

use super::metrics::RepairMetrics;
use super::types::{
    RepairDedupKey, RepairTask, RepairTaskId, RepairTaskRecord, RepairTaskState, TaskAckStatus, TaskFailureClass,
};
use crate::error::{MetadataError, MetadataResult};
use crate::inflight_registry::{InflightKind, InflightRegistry};
use crate::observe;
use beryl_types::ids::WorkerId;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Internal state for RepairQueue (protected by single mutex to avoid deadlocks).
struct RepairQueueState {
    /// Queue of pending task IDs (FIFO order, rotated during polling).
    pending_ids: VecDeque<RepairTaskId>,
    /// All task records indexed by task ID (includes pending, in-flight, done, failed).
    records: HashMap<RepairTaskId, RepairTaskRecord>,
    /// Deduplication map: dedup_key → task_id (prevents duplicate tasks).
    dedup: HashMap<RepairDedupKey, RepairTaskId>,
    /// Worker inflight count: worker_id → number of in-flight tasks (for rate limiting).
    worker_inflight: HashMap<WorkerId, usize>,
}

impl RepairQueueState {
    fn new() -> Self {
        Self {
            pending_ids: VecDeque::new(),
            records: HashMap::new(),
            dedup: HashMap::new(),
            worker_inflight: HashMap::new(),
        }
    }

    /// Calculate pending count (inline, no lock needed).
    fn count_pending(&self) -> usize {
        self.records
            .values()
            .filter(|r| matches!(r.state, RepairTaskState::Pending { .. }))
            .count()
    }

    /// Calculate inflight count (inline, no lock needed).
    fn count_inflight(&self) -> usize {
        self.records
            .values()
            .filter(|r| matches!(r.state, RepairTaskState::InFlight { .. }))
            .count()
    }
}

/// Helper struct for metrics updates (calculated inline, applied after lock release).
#[derive(Default)]
struct MetricsUpdate {
    pending: usize,
    inflight: usize,
    total: usize,
    acked: bool,
    failed: bool,
    retry: bool,
}

/// Repair queue with state machine, deduplication, and retry.
///
/// Manages the lifecycle of repair tasks (Replicate, EvictReplica) with:
/// - State tracking: Pending → InFlight → Done/Failed
/// - Deduplication: Prevents duplicate tasks based on dedup keys
/// - Worker-level rate limiting: Limits concurrent tasks per worker
/// - Adaptive backoff: Retry with exponential backoff based on error class
/// - Timeout handling: Automatically requeues timed-out in-flight tasks
///
/// Uses a single mutex for all state to avoid ABBA deadlocks.
pub struct RepairQueue {
    /// Next task ID generator (atomic counter, auto-increments on enqueue).
    next_id: AtomicU64,
    /// All queue state protected by single mutex (eliminates deadlock risk).
    state: Arc<parking_lot::Mutex<RepairQueueState>>,
    /// Maximum queue size (enqueue fails if exceeded).
    max_queue_size: usize,
    /// Maximum retry attempts per task (exceeded → Failed state).
    max_attempts: u32,
    /// In-flight task timeout in milliseconds (exceeded → requeue with backoff).
    inflight_timeout_ms: u64,
    /// Initial backoff delay in milliseconds (for first retry).
    initial_backoff_ms: u64,
    /// Maximum backoff delay in milliseconds (caps exponential backoff).
    max_backoff_ms: u64,
    /// Maximum concurrent tasks per worker (poll returns empty if exceeded).
    worker_inflight_limit: usize,
    /// Optional metrics collector for observability (counters, gauges).
    metrics: Option<Arc<RepairMetrics>>,
    /// Optional inflight registry for cross-operation mutual exclusion.
    inflight_registry: Option<Arc<InflightRegistry>>,
}

impl RepairQueue {
    pub fn new(max_queue_size: usize) -> Self {
        Self::with_config(
            max_queue_size,
            3,       // max_attempts
            300_000, // inflight_timeout_ms (5 minutes)
            1_000,   // initial_backoff_ms (1 second)
            60_000,  // max_backoff_ms (1 minute)
            4,       // worker_inflight_limit (default 4)
        )
    }

    pub fn with_config(
        max_queue_size: usize,
        max_attempts: u32,
        inflight_timeout_ms: u64,
        initial_backoff_ms: u64,
        max_backoff_ms: u64,
        worker_inflight_limit: usize,
    ) -> Self {
        Self::with_config_and_metrics(
            max_queue_size,
            max_attempts,
            inflight_timeout_ms,
            initial_backoff_ms,
            max_backoff_ms,
            worker_inflight_limit,
            None,
        )
    }

    pub(crate) fn with_config_and_metrics(
        max_queue_size: usize,
        max_attempts: u32,
        inflight_timeout_ms: u64,
        initial_backoff_ms: u64,
        max_backoff_ms: u64,
        worker_inflight_limit: usize,
        metrics: Option<Arc<RepairMetrics>>,
    ) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            state: Arc::new(parking_lot::Mutex::new(RepairQueueState::new())),
            max_queue_size,
            max_attempts,
            inflight_timeout_ms,
            initial_backoff_ms,
            max_backoff_ms,
            worker_inflight_limit,
            metrics,
            inflight_registry: None,
        }
    }

    /// Set inflight registry for cross-operation mutual exclusion.
    pub fn set_inflight_registry(&mut self, inflight_registry: Arc<InflightRegistry>) {
        self.inflight_registry = Some(inflight_registry);
    }

    /// Enqueue a repair task.
    ///
    /// All tasks use single block + single worker semantics for consistency.
    /// If batch operations are needed, call enqueue() multiple times.
    ///
    /// # Leader-only
    /// This method should only be called by the leader node. Repair tasks are only
    /// processed and dispatched by the leader. Follower nodes should not enqueue tasks.
    pub fn enqueue(&self, task: RepairTask) -> MetadataResult<RepairTaskId> {
        // Note: Leader check is done at the caller level (service.rs, maintenance.rs)
        // This is a performance-critical path, so we avoid redundant checks here
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Generate dedup key (single key per task)
        let dedup_key = RepairDedupKey::from_task(&task);

        // Single lock for all state access
        let mut state = self.state.lock();

        // Check for existing task with same dedup key
        if let Some(existing_id) = state.dedup.get(&dedup_key) {
            if let Some(existing_record) = state.records.get(existing_id) {
                // Check if existing task is still active (not Done/Failed)
                match &existing_record.state {
                    RepairTaskState::Failed { .. } => {
                        // Old task is done, allow new one
                    }
                    RepairTaskState::Pending { .. } | RepairTaskState::InFlight { .. } => {
                        // Task is still active (pending or in-flight), skip and return existing ID
                        if let Some(metrics) = &self.metrics {
                            metrics.inc_task_dedup_skipped();
                        }
                        debug!(
                            task_id = existing_id.0,
                            state = ?existing_record.state,
                            "Duplicate repair task (active), skipping"
                        );
                        return Ok(*existing_id);
                    }
                }
            }
        }

        // Check queue size
        if state.records.len() >= self.max_queue_size {
            return Err(MetadataError::ServiceUnavailable("Repair queue is full".to_string()));
        }

        // Generate new task ID
        let task_id = RepairTaskId(self.next_id.fetch_add(1, Ordering::Relaxed));

        // Create record
        let record = RepairTaskRecord {
            id: task_id,
            task: task.clone(),
            state: RepairTaskState::Pending {
                next_visible_at_ms: now_ms,
            },
            attempt: 0,
            updated_at_ms: now_ms,
            dedup_key: dedup_key.clone(),
        };

        // Insert record
        state.records.insert(task_id, record);

        // Update dedup map
        state.dedup.insert(dedup_key, task_id);

        // Add to pending queue
        state.pending_ids.push_back(task_id);

        // Calculate metrics inline (no additional lock needed)
        let pending_count = state.count_pending();
        let total_count = state.records.len();

        // Update metrics (after releasing lock)
        drop(state);
        if let Some(metrics) = &self.metrics {
            metrics.inc_task_enqueued();
            metrics.update_queue_pending(pending_count);
            metrics.update_queue_total(total_count);
        }
        observe::set_repair_queue_depth(pending_count);

        info!(
            task_id = task_id.0,
            task_type = task.task_type(),
            "Enqueued repair task"
        );

        Ok(task_id)
    }

    /// Poll tasks for a specific worker (with worker-level rate limiting).
    pub fn poll_for_worker(&self, worker_id: WorkerId, max: usize) -> Vec<RepairTaskRecord> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Single lock for all state access
        let mut state = self.state.lock();

        // Check worker inflight limit
        let current_inflight = state.worker_inflight.get(&worker_id).copied().unwrap_or(0);
        if current_inflight >= self.worker_inflight_limit {
            // Worker is at limit, return empty
            return Vec::new();
        }

        let mut result = Vec::new();

        // Calculate how many tasks we can assign (respecting limit)
        let available_slots = self.worker_inflight_limit - current_inflight;
        let max_to_assign = max.min(available_slots);

        // Scan pending queue (limited iterations to avoid long lock)
        // Performance optimization: increase scan limit for larger queues while maintaining fairness
        // We scan up to 200 items or the queue size, whichever is smaller
        // This ensures we can find tasks even in large queues without holding the lock too long
        let mut scanned = 0;
        let max_scan = state.pending_ids.len().min(200);

        // Track how many tasks we've skipped due to backoff to avoid infinite loops
        let mut skipped_backoff = 0;
        let max_skipped_backoff = 50; // Stop if we skip too many backoff tasks

        while result.len() < max_to_assign && scanned < max_scan {
            if state.pending_ids.is_empty() {
                break;
            }

            // Rotate: take from front, check, put back if not matched
            if let Some(task_id) = state.pending_ids.pop_front() {
                scanned += 1;

                if let Some(record) = state.records.get_mut(&task_id) {
                    // Check if task is visible (backoff expired)
                    let is_visible = match &record.state {
                        RepairTaskState::Pending { next_visible_at_ms } => *next_visible_at_ms <= now_ms,
                        _ => false,
                    };

                    if !is_visible {
                        // Not visible yet (still in backoff), put back
                        state.pending_ids.push_back(task_id);
                        skipped_backoff += 1;
                        // If we've skipped too many backoff tasks, break to avoid spinning
                        if skipped_backoff >= max_skipped_backoff {
                            break;
                        }
                        continue;
                    }

                    // Check if task matches this worker
                    let matches = match &record.task {
                        RepairTask::Replicate { target_worker, .. }
                        | RepairTask::EvictReplica { target_worker, .. } => *target_worker == worker_id,
                    };

                    if matches {
                        // Check inflight registry against other maintenance actions.
                        let block_id = record.task.block_id();
                        let acquire_ok = if let Some(ref inflight_registry) = self.inflight_registry {
                            // Try to acquire inflight lock for Repair (highest priority)
                            match inflight_registry.try_acquire(
                                block_id,
                                InflightKind::Repair,
                                Some(self.inflight_timeout_ms),
                            ) {
                                Ok(true) => true,
                                Ok(false) => {
                                    // Block is already owned by another maintenance action.
                                    debug!(
                                        task_id = task_id.0,
                                        block_id = %block_id,
                                        "Repair task blocked by inflight maintenance action"
                                    );
                                    false
                                }
                                Err(e) => {
                                    warn!(
                                        task_id = task_id.0,
                                        block_id = %block_id,
                                        error = %e,
                                        "Failed to check inflight registry, skipping task"
                                    );
                                    false
                                }
                            }
                        } else {
                            // Tests and isolated queue users may run without cross-operation gating.
                            true
                        };

                        if acquire_ok {
                            // Mark as InFlight
                            let deadline_ms = now_ms + self.inflight_timeout_ms;
                            record.state = RepairTaskState::InFlight { worker_id, deadline_ms };
                            record.updated_at_ms = now_ms;

                            result.push(record.clone());

                            // Update worker inflight count (after cloning record)
                            *state.worker_inflight.entry(worker_id).or_insert(0) += 1;
                        } else {
                            // Inflight registry blocked - put back to pending queue
                            state.pending_ids.push_back(task_id);
                        }
                    } else {
                        // Doesn't match, put back
                        state.pending_ids.push_back(task_id);
                    }
                } else {
                    // Record not found (shouldn't happen), skip
                }
            } else {
                break;
            }
        }

        result
    }

    /// Acknowledge task completion (with error class for adaptive backoff).
    pub fn ack(
        &self,
        task_id: RepairTaskId,
        worker_id: WorkerId,
        status: TaskAckStatus,
        message: Option<String>,
        error_class: Option<TaskFailureClass>,
    ) -> MetadataResult<Option<RepairTask>> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Single lock for all state access
        let mut state = self.state.lock();

        // Get queue total early (before any removals) for metrics
        let _queue_total_before = state.records.len();

        // First verify state and owner (without removing)
        let mut record = state
            .records
            .get(&task_id)
            .ok_or_else(|| MetadataError::NotFound(format!("Task not found: {}", task_id.0)))?
            .clone();

        match &record.state {
            RepairTaskState::InFlight { worker_id: owner, .. } => {
                if *owner != worker_id {
                    warn!(
                        task_id = task_id.0,
                        expected_worker = owner.as_raw(),
                        actual_worker = worker_id.as_raw(),
                        "Task ack from wrong worker, ignoring"
                    );
                    return Ok(None); // Ignore but don't error
                }
            }
            _ => {
                warn!(
                    task_id = task_id.0,
                    state = ?record.state,
                    "Task ack for non-inflight task, ignoring"
                );
                return Ok(None); // Ignore but don't error
            }
        }

        // Remove record from state (will re-insert if retrying)
        state.records.remove(&task_id);

        // Decrease worker inflight count
        if let Some(count) = state.worker_inflight.get_mut(&worker_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.worker_inflight.remove(&worker_id);
            }
        }

        // Release inflight registry lock (before handling success/failure)
        let block_id = record.task.block_id();
        if let Some(ref inflight_registry) = self.inflight_registry {
            inflight_registry.release(block_id);
            debug!(
                task_id = task_id.0,
                block_id = %block_id,
                "Released inflight registry lock for repair task"
            );
        }

        // Handle success or failure
        let followup_task: Option<RepairTask> = None;
        let mut metrics_update = MetricsUpdate::default();

        match status {
            TaskAckStatus::Success => {
                // Remove from dedup map
                state.dedup.remove(&record.dedup_key);

                // Record is already removed from records, don't re-insert

                // Calculate metrics inline (no additional lock)
                metrics_update.pending = state.count_pending();
                metrics_update.inflight = state.count_inflight();
                metrics_update.total = state.records.len();
                metrics_update.acked = true;

                info!(task_id = task_id.0, "Task completed successfully");
            }
            TaskAckStatus::Failed | TaskAckStatus::RetryableFailed => {
                // Increment attempt for failed tasks
                record.attempt += 1;

                // Determine error class (default to Retryable if not provided)
                let error_class = error_class.unwrap_or(TaskFailureClass::Retryable);

                match error_class {
                    TaskFailureClass::Fatal => {
                        // Permanent error, mark as Failed and remove
                        let dedup_key = record.dedup_key.clone();

                        // Remove from dedup
                        state.dedup.remove(&dedup_key);

                        // Record is already removed from records, don't re-insert

                        // Calculate metrics inline
                        metrics_update.total = state.records.len();
                        metrics_update.failed = true;

                        warn!(
                            task_id = task_id.0,
                            attempts = record.attempt,
                            "Task failed permanently (fatal error)"
                        );
                    }
                    TaskFailureClass::Retryable | TaskFailureClass::NeedRefresh => {
                        // Retry with adaptive backoff
                        if record.attempt >= self.max_attempts {
                            // Exceeded max attempts
                            let dedup_key = record.dedup_key.clone();

                            // Remove from dedup
                            state.dedup.remove(&dedup_key);

                            // Record is already removed from records, don't re-insert

                            // Calculate metrics inline
                            metrics_update.total = state.records.len();
                            metrics_update.failed = true;

                            warn!(
                                task_id = task_id.0,
                                attempts = record.attempt,
                                "Task failed permanently (max retries)"
                            );
                        } else {
                            // Calculate adaptive backoff
                            let attempt = record.attempt;
                            let backoff_ms = self.calculate_adaptive_backoff(attempt, error_class, message.as_deref());
                            record.state = RepairTaskState::Pending {
                                next_visible_at_ms: now_ms + backoff_ms,
                            };
                            record.updated_at_ms = now_ms;

                            // Put back to pending queue
                            state.pending_ids.push_back(task_id);

                            // Re-insert record into records (for retry)
                            state.records.insert(task_id, record);

                            // Calculate metrics inline
                            metrics_update.pending = state.count_pending();
                            metrics_update.retry = true;

                            warn!(
                                task_id = task_id.0,
                                attempt = attempt,
                                error_class = ?error_class,
                                backoff_ms = backoff_ms,
                                "Task failed, will retry with backoff"
                            );
                        }
                    }
                    TaskFailureClass::Ok => {
                        // Should not happen for Failed status, but handle gracefully
                        // Remove from dedup
                        state.dedup.remove(&record.dedup_key);
                        // Record already removed, nothing to do
                    }
                }
            }
        }

        // Release lock before updating metrics (avoid callback deadlock)
        drop(state);

        // Update metrics (after releasing lock)
        if let Some(metrics) = &self.metrics {
            if metrics_update.acked {
                metrics.inc_task_acked();
            }
            if metrics_update.failed {
                metrics.inc_task_failed();
            }
            if metrics_update.retry {
                metrics.inc_task_retry();
            }
            metrics.update_queue_pending(metrics_update.pending);
            metrics.update_queue_inflight(metrics_update.inflight);
            metrics.update_queue_total(metrics_update.total);
        }
        observe::set_repair_queue_depth(metrics_update.pending);
        if metrics_update.acked {
            observe::record_repair_attempt("ok", "none");
        }
        if metrics_update.failed {
            observe::record_repair_attempt("error", "failed");
        }
        if metrics_update.retry {
            observe::record_repair_attempt("error", "retryable");
        }

        Ok(followup_task)
    }

    /// Calculate adaptive backoff based on error class and attempt count.
    fn calculate_adaptive_backoff(
        &self,
        attempt: u32,
        error_class: TaskFailureClass,
        _error_message: Option<&str>,
    ) -> u64 {
        match error_class {
            TaskFailureClass::NeedRefresh => {
                // Shorter backoff for refresh-needed errors
                let base = self.initial_backoff_ms / 2; // 500ms
                let backoff = base * (1u64 << attempt.min(5)); // Cap at 2^5
                backoff.min(self.max_backoff_ms / 2) // Cap at 30s
            }
            TaskFailureClass::Retryable => {
                // Standard exponential backoff
                self.calculate_backoff(attempt)
            }
            _ => {
                // Fallback to standard backoff
                self.calculate_backoff(attempt)
            }
        }
    }

    /// Requeue timed-out in-flight tasks.
    pub fn requeue_timeouts(&self, now_ms: u64) -> usize {
        // Single lock for all state access
        let mut state = self.state.lock();

        let mut timeout_count = 0;
        let mut to_requeue = Vec::new();
        let mut timed_out_workers = Vec::new();
        let mut dedup_keys_to_remove = Vec::new();

        // First pass: update records and collect changes
        for (task_id, record) in state.records.iter_mut() {
            if let RepairTaskState::InFlight { deadline_ms, worker_id } = &record.state {
                if *deadline_ms < now_ms {
                    // Timeout
                    record.attempt += 1;

                    // Track worker for inflight count update
                    timed_out_workers.push(*worker_id);

                    if record.attempt >= self.max_attempts {
                        // Exceeded max attempts
                        let dedup_key = record.dedup_key.clone();
                        record.state = RepairTaskState::Failed {
                            reason: "Timeout: exceeded max retries".to_string(),
                        };
                        record.updated_at_ms = now_ms;

                        // Collect dedup key for removal (after iteration)
                        dedup_keys_to_remove.push(dedup_key);
                    } else {
                        // Retry with backoff
                        let backoff_ms = self.calculate_backoff(record.attempt);
                        record.state = RepairTaskState::Pending {
                            next_visible_at_ms: now_ms + backoff_ms,
                        };
                        record.updated_at_ms = now_ms;

                        to_requeue.push(*task_id);
                    }

                    timeout_count += 1;
                }
            }
        }

        // Second pass: apply changes that require accessing other state fields
        for dedup_key in dedup_keys_to_remove {
            state.dedup.remove(&dedup_key);
        }

        // Update worker inflight counts
        for worker_id in timed_out_workers {
            if let Some(count) = state.worker_inflight.get_mut(&worker_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    state.worker_inflight.remove(&worker_id);
                }
            }
        }

        // Add timed-out tasks back to pending queue
        for task_id in to_requeue {
            state.pending_ids.push_back(task_id);
        }

        // Calculate metrics inline (no additional lock)
        let pending_count = state.count_pending();
        let inflight_count = state.count_inflight();
        let total_count = state.records.len();

        // Release lock before updating metrics
        drop(state);

        if timeout_count > 0 {
            // Update metrics (after releasing lock)
            if let Some(metrics) = &self.metrics {
                for _ in 0..timeout_count {
                    metrics.inc_task_timeout();
                }
                metrics.update_queue_pending(pending_count);
                metrics.update_queue_inflight(inflight_count);
                metrics.update_queue_total(total_count);
            }
            observe::set_repair_queue_depth(pending_count);
            for _ in 0..timeout_count {
                observe::record_repair_attempt("error", "timeout");
            }
            info!(timeout_count, "Requeued timed-out tasks");
        }

        timeout_count
    }

    /// Calculate exponential backoff delay.
    fn calculate_backoff(&self, attempt: u32) -> u64 {
        let backoff_ms = self.initial_backoff_ms * (1u64 << attempt.min(10)); // Cap at 2^10
        backoff_ms.min(self.max_backoff_ms)
    }

    /// Get pending queue length.
    pub fn len_pending(&self) -> usize {
        let state = self.state.lock();
        state.count_pending()
    }

    /// Get in-flight queue length.
    pub fn len_inflight(&self) -> usize {
        let state = self.state.lock();
        state.count_inflight()
    }

    /// Get in-flight count for a specific worker.
    pub fn worker_inflight_count(&self, worker_id: WorkerId) -> usize {
        let state = self.state.lock();
        state.worker_inflight.get(&worker_id).copied().unwrap_or(0)
    }

    /// Get total queue length.
    pub fn len_total(&self) -> usize {
        let state = self.state.lock();
        state.records.len()
    }

    /// Check if queue is empty.
    pub fn is_empty(&self) -> bool {
        let state = self.state.lock();
        state.records.is_empty()
    }

    /// Clear all tasks.
    pub fn clear(&self) {
        let mut state = self.state.lock();
        state.pending_ids.clear();
        state.records.clear();
        state.dedup.clear();
        state.worker_inflight.clear();
        observe::set_repair_queue_depth(0);
    }
}
