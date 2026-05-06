// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Repair signal handling for worker block-report deltas.

use super::{OrphanQueue, RepairPlanner, RepairQueue};
use crate::error::MetadataResult;
use crate::raft::AppRaftNode;
use crate::worker::WorkerManager;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::warn;
use types::ids::{BlockId, WorkerId};

/// Soft-state block-report delta handed off from worker RPC ingress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockReportDelta {
    pub worker_id: WorkerId,
    pub added_blocks: Vec<BlockId>,
    pub removed_blocks: Vec<BlockId>,
}

/// Queue lengths observed after repair signal processing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepairSignalQueueLengths {
    pub orphan_queue_len: usize,
    pub repair_queue_len: usize,
}

/// Summary returned to worker ingress for logging and coarse metrics.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RepairSignalOutcome {
    pub blocks_checked: usize,
    pub orphan_blocks: usize,
    pub repair_tasks_enqueued: usize,
    pub enqueue_failures: usize,
    pub skipped_blocks: usize,
    pub removed_blocks_ignored: usize,
    pub queue_lengths: Option<RepairSignalQueueLengths>,
}

/// Boundary consumed by WorkerService for block-report repair signals.
#[async_trait]
pub trait RepairSignalSink: Send + Sync {
    async fn handle_block_report_delta(&self, delta: BlockReportDelta) -> MetadataResult<RepairSignalOutcome>;
}

/// Dependencies for the block-report repair signal handler.
pub struct RepairSignalHandlerDeps {
    pub raft_node: Arc<AppRaftNode>,
    pub worker_manager: Arc<WorkerManager>,
    pub repair_queue: Arc<RepairQueue>,
    pub orphan_queue: Arc<OrphanQueue>,
    pub repair_planner: Arc<RepairPlanner>,
}

/// Handles repair signals derived from worker block-report deltas.
pub struct RepairSignalHandler {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    repair_queue: Arc<RepairQueue>,
    orphan_queue: Arc<OrphanQueue>,
    repair_planner: Arc<RepairPlanner>,
}

impl RepairSignalHandler {
    pub fn new(deps: RepairSignalHandlerDeps) -> Self {
        Self {
            raft_node: deps.raft_node,
            worker_manager: deps.worker_manager,
            repair_queue: deps.repair_queue,
            orphan_queue: deps.orphan_queue,
            repair_planner: deps.repair_planner,
        }
    }

    fn queue_lengths(&self) -> RepairSignalQueueLengths {
        RepairSignalQueueLengths {
            orphan_queue_len: self.orphan_queue.len(),
            repair_queue_len: self.repair_queue.len_pending(),
        }
    }
}

#[async_trait]
impl RepairSignalSink for RepairSignalHandler {
    async fn handle_block_report_delta(&self, delta: BlockReportDelta) -> MetadataResult<RepairSignalOutcome> {
        let mut outcome = RepairSignalOutcome {
            removed_blocks_ignored: delta.removed_blocks.len(),
            ..RepairSignalOutcome::default()
        };

        if !self.raft_node.is_leader() {
            outcome.skipped_blocks = delta.added_blocks.len();
            outcome.queue_lengths = Some(self.queue_lengths());
            return Ok(outcome);
        }

        for block_id in delta.added_blocks {
            outcome.blocks_checked += 1;
            let block_exists = self.raft_node.read(false, |sm| sm.get_block(block_id)).await?;

            if block_exists.is_none() {
                self.orphan_queue.add(block_id, delta.worker_id);
                outcome.orphan_blocks += 1;
                warn!(
                    block_id = %block_id,
                    worker_id = delta.worker_id.as_raw(),
                    "Orphan block detected from block report signal"
                );
                continue;
            }

            let current_locations = self.worker_manager.get_block_locations(block_id);
            let live_workers = self.worker_manager.list_live_workers();
            let replication_factor = 3u8;
            let actions =
                self.repair_planner
                    .plan_replication(block_id, &current_locations, replication_factor, &live_workers);
            for action in actions {
                let task = action.into_task();
                if let Err(e) = self.repair_queue.enqueue(task) {
                    outcome.enqueue_failures += 1;
                    warn!(
                        block_id = %block_id,
                        error = %e,
                        "Failed to enqueue replication task from block report signal"
                    );
                } else {
                    outcome.repair_tasks_enqueued += 1;
                }
            }
        }

        outcome.queue_lengths = Some(self.queue_lengths());
        Ok(outcome)
    }
}
