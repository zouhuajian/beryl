// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Delete executor: executes DeleteIntent by sending DeleteBlocks commands to workers.
//!
//! This module implements:
//! - Periodic polling of pending DeleteIntents
//! - Safety gate checks (not_before_ms, blockreport_converged, guard_state_id)
//! - Rate limiting (per-worker concurrency, per-block single-flight)
//! - Batch command generation (group by worker)

use crate::destructive_gate::{DestructiveCheckContext, DestructiveGate};
use crate::error::MetadataResult;
use crate::inflight_registry::{InflightKind, InflightRegistry};
use crate::metrics::MetadataMetrics;
use crate::mount::MountTable;
use crate::raft::{AppRaftNode, Command, DedupKey, RocksDBStorage};
use crate::state::{DeleteIntent, DeleteIntentReason, DeleteIntentStatus};
use crate::worker::WorkerManager;
use parking_lot::RwLock;
use proto::metadata::*;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use types::block::BlockState;
use types::ids::{BlockId, WorkerId};

/// Delete executor background task handle.
pub struct DeleteExecutorHandle {
    _task: JoinHandle<()>,
}

/// Delete intent execution status.
#[derive(Clone, Debug, PartialEq, Eq)]
enum IntentExecutionStatus {
    /// Pending: not yet started.
    Pending,
    /// InFlight: command sent to a worker.
    InFlight {
        worker_id: WorkerId,
        task_id: u64,
        sent_at_ms: u64,
    },
    /// Completed: authoritative status persisted.
    Completed { completed_at_ms: u64 },
}

/// Per-intent execution state. Terminal authoritative status is persisted through Raft.
#[derive(Clone)]
struct IntentExecutionState {
    intent: DeleteIntent,
    status: IntentExecutionStatus,
    registry_held: bool,
    /// Track which workers we've sent commands to.
    sent_to_workers: HashSet<WorkerId>,
}

/// Per-worker in-flight tracking (for rate limiting).
struct WorkerInFlight {
    /// Number of in-flight requests for this worker.
    count: usize,
    /// Block IDs currently in-flight for this worker.
    blocks: HashSet<BlockId>,
}

#[derive(Clone, Copy, Debug)]
struct DeleteTaskRoute {
    intent_id: u64,
    worker_id: WorkerId,
    task_id: u64,
    block_id: BlockId,
}

/// Delete executor service.
pub struct DeleteExecutor {
    raft_node: Arc<AppRaftNode>,
    storage: Arc<RocksDBStorage>,
    worker_manager: Arc<WorkerManager>,
    metrics: Arc<MetadataMetrics>,
    /// Execution state: intent_id -> IntentExecutionState
    execution_state: Arc<RwLock<HashMap<u64, IntentExecutionState>>>,
    /// Per-worker in-flight tracking: worker_id -> WorkerInFlight
    worker_inflight: Arc<RwLock<HashMap<WorkerId, WorkerInFlight>>>,
    /// Per-block in-flight tracking: block_id -> intent_id (single-flight guarantee)
    block_inflight: Arc<RwLock<HashMap<BlockId, u64>>>,
    /// Worker task routing: task_id -> intent route.
    task_routes: Arc<RwLock<HashMap<u64, DeleteTaskRoute>>>,
    /// Next task ID (monotonically increasing).
    next_task_id: Arc<AtomicU64>,
    /// Configuration
    poll_interval_sec: u64,
    max_intents_per_poll: usize,
    max_concurrent_per_worker: usize,
    /// Unified destructive gate
    destructive_gate: Arc<DestructiveGate>,
    /// Inflight registry for cross-operation mutual exclusion
    inflight_registry: Arc<InflightRegistry>,
}

impl DeleteExecutor {
    /// Create a new delete executor.
    pub fn new(
        raft_node: Arc<AppRaftNode>,
        storage: Arc<RocksDBStorage>,
        worker_manager: Arc<WorkerManager>,
        metrics: Arc<MetadataMetrics>,
        mount_table: Arc<MountTable>,
        inflight_registry: Arc<InflightRegistry>,
    ) -> Self {
        // Create unified destructive gate
        let destructive_gate = Arc::new(DestructiveGate::new(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            mount_table,
        ));

        Self {
            raft_node,
            storage,
            worker_manager,
            metrics,
            execution_state: Arc::new(RwLock::new(HashMap::new())),
            worker_inflight: Arc::new(RwLock::new(HashMap::new())),
            block_inflight: Arc::new(RwLock::new(HashMap::new())),
            task_routes: Arc::new(RwLock::new(HashMap::new())),
            next_task_id: Arc::new(AtomicU64::new(1)),
            poll_interval_sec: 10, // 10 seconds
            max_intents_per_poll: 100,
            max_concurrent_per_worker: 8, // Default: 8 concurrent per worker
            destructive_gate,
            inflight_registry,
        }
    }

    /// Start the executor main loop.
    pub fn start(self: &Arc<Self>) -> DeleteExecutorHandle {
        let executor = Arc::clone(self);
        let poll_interval = self.poll_interval_sec;
        let task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(poll_interval));
            loop {
                interval.tick().await;
                if executor.raft_node.is_leader() {
                    if let Err(e) = executor.run_once().await {
                        error!(error = %e, "DeleteExecutor run_once failed");
                    }
                }
            }
        });
        info!("DeleteExecutor started");
        DeleteExecutorHandle { _task: task }
    }

    /// Run one execution cycle.
    pub(super) async fn run_once(&self) -> MetadataResult<()> {
        // Leader-only check
        if !self.raft_node.is_leader() {
            return Ok(()); // Only run on leader
        }

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Check safety gates (blockreport_converged)
        if !self.check_safety_gates(now_ms).await? {
            debug!("DeleteExecutor: safety gates not satisfied, skipping");
            return Ok(());
        }

        // Poll pending intents
        let pending_intents = self
            .storage
            .list_pending_delete_intents(self.max_intents_per_poll, now_ms)?;

        if pending_intents.is_empty() {
            return Ok(());
        }

        debug!(count = pending_intents.len(), "DeleteExecutor: found pending intents");

        // Process each intent
        for intent in &pending_intents {
            // Check if already in execution
            {
                let state = self.execution_state.read();
                if state.contains_key(&intent.intent_id) {
                    continue; // Already being processed
                }
            }

            // Check not_before_ms
            if now_ms < intent.not_before_ms {
                continue; // Not yet ready
            }

            // Unified gate check (enhanced with shard_group_id and guard_watermark)
            let mut ctx = DestructiveCheckContext::new("delete_executor_execute")
                .with_block_id(intent.block_id)
                .with_not_before_ms(intent.not_before_ms);

            // Prefer guard_watermark if available, otherwise fallback to guard_state_id
            if let Some(guard_watermark) = intent.guard_watermark {
                ctx = ctx
                    .with_group_id(guard_watermark.group_id)
                    .with_guard_watermark(guard_watermark);
                if let Some(mount_epoch) = intent.mount_epoch {
                    ctx = ctx.with_mount_epoch(mount_epoch);
                }
            } else {
                // Legacy: fallback to guard_state_id
                ctx = ctx.with_guard_state_id(intent.guard_state_id);
            }

            match self.destructive_gate.check_destructive_allowed(&ctx)? {
                crate::destructive_gate::DestructiveCheckResult::Allowed => {
                    // Proceed with execution
                }
                crate::destructive_gate::DestructiveCheckResult::Blocked { reason } => {
                    debug!(
                        intent_id = intent.intent_id,
                        block_id = %intent.block_id,
                        reason = %reason,
                        "Intent execution blocked: gate check failed"
                    );
                    continue;
                }
                crate::destructive_gate::DestructiveCheckResult::NeedRefresh { reason, .. } => {
                    warn!(
                        intent_id = intent.intent_id,
                        block_id = %intent.block_id,
                        reason = %reason,
                        "Intent execution blocked: mount epoch mismatch, need refresh"
                    );
                    continue;
                }
            }

            // State gate: verify block state is allowed for deletion
            // Only Sealed/Aborted blocks can be deleted (not Open/Writing)
            let block_state_allowed = self
                .raft_node
                .read(false, |sm| {
                    let block_meta = sm.storage().get_block(intent.block_id)?;
                    Ok(block_meta
                        .as_ref()
                        .map(|b| matches!(b.state, BlockState::Sealed | BlockState::Aborted))
                        .unwrap_or(false))
                })
                .await?;

            if !block_state_allowed {
                debug!(
                    intent_id = intent.intent_id,
                    block_id = %intent.block_id,
                    "Intent execution blocked: block state not allowed (must be Sealed/Aborted)"
                );
                continue;
            }

            // Check for active lease
            let has_active_lease = self
                .raft_node
                .read(false, |sm| {
                    let lease = sm.storage().get_lease(intent.block_id)?;
                    Ok(lease.is_some())
                })
                .await?;

            if has_active_lease {
                debug!(
                    intent_id = intent.intent_id,
                    block_id = %intent.block_id,
                    "Intent execution blocked: block has active lease"
                );
                continue;
            }

            // Check inflight registry against other maintenance actions.
            // Use appropriate InflightKind based on intent reason
            let inflight_kind = match intent.reason {
                crate::state::DeleteIntentReason::OverRep => InflightKind::OverRepEvict,
                _ => InflightKind::Delete,
            };
            if !self
                .inflight_registry
                .try_acquire(intent.block_id, inflight_kind, None)?
            {
                debug!(
                    intent_id = intent.intent_id,
                    block_id = %intent.block_id,
                    "Intent execution blocked: block is in-flight for another maintenance action"
                );
                continue;
            }

            // Initialize execution state
            {
                let mut state = self.execution_state.write();
                state.insert(
                    intent.intent_id,
                    IntentExecutionState {
                        intent: intent.clone(),
                        status: IntentExecutionStatus::Pending,
                        registry_held: true,
                        sent_to_workers: HashSet::new(),
                    },
                );
            }
        }

        // Generate and queue commands for pending intents
        self.generate_commands(now_ms).await?;

        // Cleanup completed/failed intents (older than 1 hour)
        self.cleanup_old_intents(now_ms);

        Ok(())
    }

    /// Check safety gates.
    async fn check_safety_gates(&self, now_ms: u64) -> MetadataResult<bool> {
        // Check block report convergence
        let epoch = self.worker_manager.get_metadata_epoch();
        let active_ttl_ms = self.worker_manager.heartbeat_timeout_sec() * 1000;
        let snapshot = self.worker_manager.blockreport_convergence_snapshot(
            now_ms,
            active_ttl_ms,
            epoch,
            0.80, // 80% threshold
        );

        if !snapshot.converged {
            debug!(
                active_workers = snapshot.active_workers,
                full_reported_workers = snapshot.full_reported_workers,
                ratio = snapshot.ratio,
                "DeleteExecutor: block report not converged"
            );
            return Ok(false);
        }

        Ok(true)
    }

    /// Generate commands for pending intents.
    async fn generate_commands(&self, now_ms: u64) -> MetadataResult<()> {
        // Group intents by worker (for batching)
        let mut worker_commands: HashMap<WorkerId, Vec<(u64, BlockId)>> = HashMap::new();

        // Collect pending intents that need commands
        let pending_intents: Vec<(u64, DeleteIntent)> = {
            let state = self.execution_state.read();
            state
                .iter()
                .filter_map(|(intent_id, exec_state)| {
                    if matches!(exec_state.status, IntentExecutionStatus::Pending) {
                        Some((*intent_id, exec_state.intent.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        };

        for (intent_id, intent) in pending_intents {
            // Check per-block single-flight (local tracking)
            {
                let block_inflight = self.block_inflight.read();
                if block_inflight.contains_key(&intent.block_id) {
                    continue; // Block already in-flight (local)
                }
            }

            if !self.ensure_registry_held(intent_id, intent.block_id, intent.reason)? {
                continue;
            }

            // Determine target workers
            let target_workers = if !intent.target_workers.is_empty() {
                // Use explicit target_workers (e.g., for Orphan)
                intent.target_workers.clone()
            } else if let Some(group_id) = intent.shard_group_id {
                self.worker_manager.get_block_locations(group_id, intent.block_id)
            } else {
                // Delete execution requires an authoritative shard_group_id for
                // implicit target resolution. Do not infer a group from
                // block-report soft state.
                debug!(
                    intent_id,
                    block_id = %intent.block_id,
                    "Skipping delete intent without authoritative shard_group_id"
                );
                continue;
            };

            if target_workers.is_empty() {
                // No known locations - mark as completed (block already gone)
                debug!(
                    intent_id,
                    block_id = %intent.block_id,
                    "Intent completed: no known locations"
                );
                self.mark_intent_completed(intent_id, now_ms, true).await;
                continue;
            }

            // Check per-worker concurrency limit
            for worker_id in &target_workers {
                let mut worker_inflight = self.worker_inflight.write();
                let in_flight = worker_inflight.entry(*worker_id).or_insert_with(|| WorkerInFlight {
                    count: 0,
                    blocks: HashSet::new(),
                });

                if in_flight.count >= self.max_concurrent_per_worker {
                    continue; // Worker at capacity
                }

                if in_flight.blocks.contains(&intent.block_id) {
                    continue; // Block already in-flight for this worker
                }

                // Add to batch
                worker_commands
                    .entry(*worker_id)
                    .or_default()
                    .push((intent_id, intent.block_id));
            }
        }

        // Generate commands and update state
        for (worker_id, blocks) in worker_commands {
            if blocks.is_empty() {
                continue;
            }

            // Update in-flight tracking
            {
                let mut worker_inflight = self.worker_inflight.write();
                let mut block_inflight = self.block_inflight.write();
                let mut task_routes = self.task_routes.write();
                let mut state = self.execution_state.write();
                let in_flight = worker_inflight.entry(worker_id).or_insert_with(|| WorkerInFlight {
                    count: 0,
                    blocks: HashSet::new(),
                });

                for (intent_id, block_id) in &blocks {
                    let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
                    in_flight.count += 1;
                    in_flight.blocks.insert(*block_id);
                    block_inflight.insert(*block_id, *intent_id);
                    task_routes.insert(
                        task_id,
                        DeleteTaskRoute {
                            intent_id: *intent_id,
                            worker_id,
                            task_id,
                            block_id: *block_id,
                        },
                    );

                    if let Some(exec_state) = state.get_mut(intent_id) {
                        exec_state.status = IntentExecutionStatus::InFlight {
                            worker_id,
                            task_id,
                            sent_at_ms: now_ms,
                        };
                        exec_state.sent_to_workers.insert(worker_id);
                    }
                }
            }

            // Update metrics
            self.metrics
                .delete_executor_requests_total
                .fetch_add(blocks.len() as u64, Ordering::Relaxed);

            debug!(
                worker_id = worker_id.as_raw(),
                block_count = blocks.len(),
                "Generated DeleteBlocksCommand for worker"
            );
        }

        Ok(())
    }

    fn ensure_registry_held(
        &self,
        intent_id: u64,
        block_id: BlockId,
        reason: DeleteIntentReason,
    ) -> MetadataResult<bool> {
        {
            let state = self.execution_state.read();
            if state
                .get(&intent_id)
                .map(|exec_state| exec_state.registry_held)
                .unwrap_or(false)
            {
                return Ok(true);
            }
        }

        let inflight_kind = match reason {
            DeleteIntentReason::OverRep => InflightKind::OverRepEvict,
            _ => InflightKind::Delete,
        };
        if !self.inflight_registry.try_acquire(block_id, inflight_kind, None)? {
            debug!(
                intent_id,
                block_id = %block_id,
                "Intent execution blocked: block is in-flight for another maintenance action"
            );
            return Ok(false);
        }

        let mut state = self.execution_state.write();
        let Some(exec_state) = state.get_mut(&intent_id) else {
            self.inflight_registry.release(block_id);
            return Ok(false);
        };
        if !matches!(exec_state.status, IntentExecutionStatus::Pending) {
            self.inflight_registry.release(block_id);
            return Ok(false);
        }
        exec_state.registry_held = true;
        Ok(true)
    }

    fn release_registry_held(&self, intent_id: u64, block_id: BlockId) {
        let should_release = {
            let mut state = self.execution_state.write();
            if let Some(exec_state) = state.get_mut(&intent_id) {
                let was_held = exec_state.registry_held;
                exec_state.registry_held = false;
                was_held
            } else {
                false
            }
        };
        if should_release {
            self.inflight_registry.release(block_id);
        }
    }

    fn release_inflight_route(&self, route: DeleteTaskRoute) {
        {
            let mut worker_inflight = self.worker_inflight.write();
            if let Some(in_flight) = worker_inflight.get_mut(&route.worker_id) {
                in_flight.count = in_flight.count.saturating_sub(1);
                in_flight.blocks.remove(&route.block_id);
            }
        }
        {
            let mut block_inflight = self.block_inflight.write();
            if block_inflight.get(&route.block_id).copied() == Some(route.intent_id) {
                block_inflight.remove(&route.block_id);
            }
        }
        self.task_routes.write().remove(&route.task_id);
        self.release_registry_held(route.intent_id, route.block_id);
    }

    fn release_inflight_routes_for_intent(&self, intent_id: u64) {
        let routes: Vec<DeleteTaskRoute> = {
            let task_routes = self.task_routes.read();
            task_routes
                .values()
                .filter(|route| route.intent_id == intent_id)
                .copied()
                .collect()
        };
        for route in routes {
            self.release_inflight_route(route);
        }
    }

    /// Get pending commands for a worker (called from Heartbeat handler).
    pub fn get_pending_commands(&self, worker_id: WorkerId, max: usize) -> Vec<WorkerCommandProto> {
        let mut commands = Vec::new();
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Collect blocks to evict for this worker
        let mut blocks_to_evict: Vec<(u64, BlockId)> = Vec::new();
        {
            let state = self.execution_state.read();
            for (intent_id, exec_state) in state.iter() {
                if let IntentExecutionStatus::InFlight {
                    worker_id: target_worker,
                    task_id: _,
                    ..
                } = &exec_state.status
                {
                    if *target_worker == worker_id && exec_state.sent_to_workers.contains(&worker_id) {
                        blocks_to_evict.push((*intent_id, exec_state.intent.block_id));
                    }
                }
            }
        }

        // Group by intent_id (for batching)
        let mut intent_blocks: HashMap<u64, Vec<BlockId>> = HashMap::new();
        for (intent_id, block_id) in blocks_to_evict.into_iter().take(max) {
            intent_blocks.entry(intent_id).or_default().push(block_id);
        }

        // Generate commands
        for (intent_id, block_ids) in intent_blocks {
            let intent = {
                let state = self.execution_state.read();
                state.get(&intent_id).map(|s| s.intent.clone()).unwrap_or_else(|| {
                    // Fallback: try to load from storage
                    match self.storage.get_delete_intent(intent_id) {
                        Ok(Some(intent)) => intent,
                        _ => {
                            // Last resort: create minimal intent (should not happen in normal flow)
                            warn!(
                                intent_id,
                                "DeleteIntent not found in execution_state or storage, creating fallback"
                            );
                            // Last resort: create minimal intent
                            DeleteIntent {
                                intent_id,
                                block_id: block_ids[0],
                                reason: crate::state::DeleteIntentReason::Gc,
                                created_at_ms: now_ms,
                                not_before_ms: now_ms,
                                shard_group_id: None,
                                guard_watermark: None,
                                mount_epoch: None,
                                guard_state_id: types::RaftLogId::default(),
                                target_workers: Vec::new(),
                                status: crate::state::DeleteIntentStatus::Pending,
                                finished_at_ms: None,
                                last_error_msg: None,
                            }
                        }
                    }
                })
            };

            let block_ids_proto: Vec<proto::common::BlockIdProto> = block_ids
                .iter()
                .copied()
                .map(proto::common::BlockIdProto::from)
                .collect();

            let task_id = {
                let state = self.execution_state.read();
                if let Some(exec_state) = state.get(&intent_id) {
                    if let IntentExecutionStatus::InFlight { task_id, .. } = exec_state.status {
                        task_id
                    } else {
                        self.next_task_id.fetch_add(1, Ordering::Relaxed)
                    }
                } else {
                    self.next_task_id.fetch_add(1, Ordering::Relaxed)
                }
            };

            // Determine op_kind based on intent reason
            let op_kind = match intent.reason {
                crate::state::DeleteIntentReason::OverRep => {
                    proto::metadata::DeleteOpKindProto::DeleteOpKindReplicaEvict as i32
                }
                _ => proto::metadata::DeleteOpKindProto::DeleteOpKindDelete as i32,
            };

            // Generate DeleteBlocksCommandProto with per-block status support
            let delete_blocks_command = DeleteBlocksCommandProto {
                intent_id,
                op_kind,
                blocks: block_ids_proto
                    .iter()
                    .map(|proto_bid| proto::metadata::DeleteBlockRequestProto {
                        block_id: Some(*proto_bid),
                        expected_state: String::new(), // TODO: Add expected state check
                    })
                    .collect(),
                not_before_ms: intent.not_before_ms,
                expected_epoch: 0, // TODO: Add epoch check
            };

            commands.push(WorkerCommandProto {
                task_id,
                command: Some(proto::metadata::worker_command_proto::Command::DeleteBlocks(
                    delete_blocks_command,
                )),
            });
        }

        commands
    }

    /// Cleanup old completed/failed intents.
    fn cleanup_old_intents(&self, now_ms: u64) {
        const CLEANUP_TTL_MS: u64 = 60 * 60 * 1000; // 1 hour

        let mut state = self.execution_state.write();
        state.retain(|_intent_id, exec_state| match &exec_state.status {
            IntentExecutionStatus::Pending | IntentExecutionStatus::InFlight { .. } => true,
            IntentExecutionStatus::Completed { completed_at_ms } => {
                now_ms.saturating_sub(*completed_at_ms) < CLEANUP_TTL_MS
            }
        });
    }

    async fn propose_delete_intent_status(
        &self,
        intent_id: u64,
        status: DeleteIntentStatus,
        finished_at_ms: Option<u64>,
        error_msg: Option<String>,
    ) -> MetadataResult<()> {
        let command = Command::UpdateDeleteIntentStatus {
            dedup: DedupKey::system(),
            intent_id,
            status,
            finished_at_ms,
            error_msg,
        };
        self.raft_node.propose(command).await.map(|_| ())
    }

    /// Mark intent as completed (ack + reconcile both satisfied).
    async fn mark_intent_completed(&self, intent_id: u64, now_ms: u64, reconciled: bool) {
        let block_id = {
            let state = self.execution_state.read();
            state.get(&intent_id).map(|exec_state| exec_state.intent.block_id)
        };

        if let Some(block_id) = block_id {
            if let Err(e) = self
                .propose_delete_intent_status(intent_id, DeleteIntentStatus::Completed, Some(now_ms), None)
                .await
            {
                warn!(
                    intent_id,
                    error = %e,
                    "Failed to persist DeleteIntent completed status"
                );
                return;
            }

            {
                let mut state = self.execution_state.write();
                if let Some(exec_state) = state.get_mut(&intent_id) {
                    exec_state.status = IntentExecutionStatus::Completed {
                        completed_at_ms: now_ms,
                    };
                }
            }

            // Update metrics
            self.metrics
                .delete_intents_completed_total
                .fetch_add(1, Ordering::Relaxed);
            if reconciled {
                self.metrics
                    .delete_intents_completed_by_reconcile_total
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.metrics
                    .delete_intents_completed_by_ack_only_total
                    .fetch_add(1, Ordering::Relaxed);
            }

            self.release_inflight_routes_for_intent(intent_id);
            self.release_registry_held(intent_id, block_id);

            info!(
                intent_id,
                block_id = %block_id,
                reconciled,
                "DeleteIntent completed and persisted"
            );
        }
    }
}

// Note: DeleteExecutor contains Arc fields, so we don't need explicit Clone
// If Clone is needed, it should be implemented to clone the Arc fields
// For now, we'll remove the Clone requirement from start() method
// // DeleteExecutor contains Arc fields, so Clone is not needed
// The start() method now takes &Arc<Self> instead
