// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Repair task types: IDs, states, tasks, and related enums.

use beryl_types::ids::{BlockId, WorkerId};

/// Repair task ID (monotonically increasing).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RepairTaskId(pub u64);

/// Repair task state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepairTaskState {
    /// Task is pending and can be polled.
    Pending { next_visible_at_ms: u64 },
    /// Task is in-flight (assigned to a worker).
    InFlight { worker_id: WorkerId, deadline_ms: u64 },
    /// Task failed permanently (exceeded max retries).
    Failed { reason: String },
}

/// Repair task with execution context.
#[derive(Clone, Debug)]
pub enum RepairTask {
    /// Replicate block to a single target worker.
    Replicate {
        block_id: BlockId,
        src_workers: Vec<WorkerId>, // Current locations (for replication source)
        target_worker: WorkerId,    // Single target worker
        replication_factor: Option<u8>,
        reason: Option<String>,
    },
    /// Evict one replica from a worker after repair planning.
    EvictReplica {
        target_worker: WorkerId,
        block_id: BlockId,
        reason: String,
    },
}

impl RepairTask {
    /// Get primary block_id for this task.
    pub fn block_id(&self) -> BlockId {
        match self {
            RepairTask::Replicate { block_id, .. } | RepairTask::EvictReplica { block_id, .. } => *block_id,
        }
    }

    /// Get task type name for metrics.
    pub fn task_type(&self) -> &'static str {
        match self {
            RepairTask::Replicate { .. } => "replicate",
            RepairTask::EvictReplica { .. } => "evict_replica",
        }
    }
}

/// Deduplication key for repair tasks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RepairDedupKey {
    Replicate { block_id: BlockId, target_worker: WorkerId },
    EvictReplica { block_id: BlockId, target_worker: WorkerId },
}

impl RepairDedupKey {
    /// Generate dedup key for a task.
    pub(crate) fn from_task(task: &RepairTask) -> RepairDedupKey {
        match task {
            RepairTask::Replicate {
                block_id,
                target_worker,
                ..
            } => RepairDedupKey::Replicate {
                block_id: *block_id,
                target_worker: *target_worker,
            },
            RepairTask::EvictReplica {
                block_id,
                target_worker,
                ..
            } => RepairDedupKey::EvictReplica {
                block_id: *block_id,
                target_worker: *target_worker,
            },
        }
    }
}

/// Repair task record with state tracking.
#[derive(Clone, Debug)]
pub struct RepairTaskRecord {
    pub id: RepairTaskId,
    pub task: RepairTask,
    pub state: RepairTaskState,
    pub attempt: u32,
    pub updated_at_ms: u64,
    pub dedup_key: RepairDedupKey,
}

/// Task acknowledgment status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskAckStatus {
    Success,
    Failed,
    RetryableFailed,
}

/// Error class for adaptive backoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskFailureClass {
    Ok,
    Retryable,   // Transient error, should retry with backoff
    Fatal,       // Permanent error, should not retry
    NeedRefresh, // Need to refresh state
}
