// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! GC service: collecting unreferenced blocks.
//!
//! This module implements a mark-and-sweep GC: mark (collect candidates) and sweep (create DeleteIntents).

use crate::destructive_gate::{DestructiveCheckContext, DestructiveGate};
use crate::error::{MetadataError, MetadataResult};
use crate::inflight_registry::{InflightKind, InflightRegistry};
use crate::metrics::MetadataMetrics;
use crate::mount::MountTable;
use crate::raft::{AppRaftNode, RocksDBStorage};
use crate::state::DeleteIntentReason;
use crate::worker::WorkerManager;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
use types::block::BlockState;
use types::ids::{BlockId, BlockIndex, DataHandleId};

use super::delete::DeleteIntentBuilder;
use super::gate::{GateCheckResult, GateState, TaskGate};

/// Block report convergence threshold (default: 80% of active workers must have full report).
pub const BLOCKREPORT_CONVERGENCE_THRESHOLD: f64 = 0.80;

/// Reference count for blocks (data_handle_id -> block_id -> count).
type BlockRefCounts = HashMap<DataHandleId, HashMap<BlockId, u32>>;

/// GC candidate for mark-and-sweep collection.
#[derive(Debug, Clone)]
pub struct GcCandidate {
    first_seen_ms: u64,
    last_seen_ms: u64,
    seen_count: u32,
    last_reason: String,
}

impl GcCandidate {
    fn new(now_ms: u64, reason: String) -> Self {
        Self {
            first_seen_ms: now_ms,
            last_seen_ms: now_ms,
            seen_count: 1,
            last_reason: reason,
        }
    }

    fn update(&mut self, now_ms: u64, reason: String) {
        self.last_seen_ms = now_ms;
        self.seen_count += 1;
        self.last_reason = reason;
    }

    /// Check if candidate is eligible for sweep.
    /// Minimum conditions: seen_count >= 2 and age >= grace_period (10 minutes).
    fn is_eligible_for_sweep(&self, now_ms: u64) -> bool {
        const MIN_SEEN_COUNT: u32 = 2;
        const GRACE_PERIOD_MS: u64 = 10 * 60 * 1000; // 10 minutes

        self.seen_count >= MIN_SEEN_COUNT && (now_ms.saturating_sub(self.first_seen_ms) >= GRACE_PERIOD_MS)
    }
}

/// GC service for collecting unreferenced blocks.
///
/// Responsibilities:
/// - Mark pass: scan blocks, collect candidates (refcount=0)
/// - Sweep pass: create DeleteIntents for eligible candidates (via Raft)
/// - Delete execution: DeleteExecutor consumes pending DeleteIntents
pub struct GcService {
    raft_node: Arc<AppRaftNode>,
    storage: Arc<RocksDBStorage>,
    worker_manager: Arc<WorkerManager>,
    block_ref_counts: Arc<RwLock<BlockRefCounts>>,
    gc_gate: Arc<RwLock<TaskGate>>,
    metrics: Arc<MetadataMetrics>,
    candidates: Arc<RwLock<HashMap<BlockId, GcCandidate>>>,
    last_log_ms: Arc<RwLock<u64>>,
    // Shared from MaintenanceService
    destructive_gate: Arc<DestructiveGate>,
    inflight_registry: Arc<InflightRegistry>,
    /// Mount table for computing mount_epoch when convergence gating is enabled.
    mount_table: Arc<MountTable>,
}

impl GcService {
    /// Create a new GcService.
    // Constructor mirrors maintenance runtime wiring; grouping dependencies would hide ownership.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        raft_node: Arc<AppRaftNode>,
        storage: Arc<RocksDBStorage>,
        worker_manager: Arc<WorkerManager>,
        block_ref_counts: Arc<RwLock<BlockRefCounts>>,
        gc_gate: Arc<RwLock<TaskGate>>,
        metrics: Arc<MetadataMetrics>,
        candidates: Arc<RwLock<HashMap<BlockId, GcCandidate>>>,
        last_log_ms: Arc<RwLock<u64>>,
        destructive_gate: Arc<DestructiveGate>,
        inflight_registry: Arc<InflightRegistry>,
        mount_table: Arc<MountTable>,
    ) -> Self {
        Self {
            raft_node,
            storage,
            worker_manager,
            block_ref_counts,
            gc_gate,
            metrics,
            candidates,
            last_log_ms,
            destructive_gate,
            inflight_registry,
            mount_table,
        }
    }

    /// Run GC cycle.
    pub async fn run_gc(&self) -> MetadataResult<()> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Check gate
        let gate_check = {
            let gate = self.gc_gate.read();
            gate.check("gc", now_ms)
        };

        // Extract reason before match to avoid partial move
        let (is_ready, reason_opt) = match &gate_check {
            GateCheckResult::Ready => (true, None),
            GateCheckResult::Degraded { reason, .. } => (false, Some(reason.clone())),
            GateCheckResult::Blocked { reason } => (false, Some(reason.clone())),
        };

        if !is_ready {
            // Skip destructive actions, but allow statistics
            self.metrics.gc_skipped_total.fetch_add(1, Ordering::Relaxed);
            self.metrics.gc_gate_state.store(0, Ordering::Relaxed);

            // Rate-limited logging
            const LOG_INTERVAL_MS: u64 = 5 * 60 * 1000; // 5 minutes
            {
                let mut last_log = self.last_log_ms.write();
                if now_ms.saturating_sub(*last_log) >= LOG_INTERVAL_MS {
                    let gate_state = match &gate_check {
                        GateCheckResult::Ready => "ready",
                        GateCheckResult::Degraded { .. } => "degraded",
                        GateCheckResult::Blocked { .. } => "blocked",
                    };
                    error!(
                        task = "gc",
                        gate_state,
                        reason = %reason_opt.as_deref().unwrap_or("unknown"),
                        "GC skipped: gate not ready. Only statistics allowed."
                    );
                    *last_log = now_ms;
                }
            }
            return Ok(());
        }

        // Gate is ready - proceed with GC
        self.metrics.gc_gate_state.store(1, Ordering::Relaxed);
        self.metrics.gc_cycles_total.fetch_add(1, Ordering::Relaxed);

        debug!(task = "gc", "Starting GC cycle");

        // Mark pass: collect candidates
        let all_blocks = self.collect_all_blocks().await?;
        debug!(
            task = "gc",
            block_count = all_blocks.len(),
            "Collected blocks from storage for GC scan"
        );

        // Get ref_counts (only if gate is ready)
        let ref_counts: HashMap<DataHandleId, HashMap<BlockId, u32>> = {
            let ref_counts = self.block_ref_counts.read();
            ref_counts.clone()
        };

        let mut new_candidates = 0;
        let mut updated_candidates = 0;
        let mut blocks_to_check = Vec::new();

        // First pass: collect blocks that might be candidates
        for block_id in &all_blocks {
            // Check block state and lease in metadata (fail-closed: Err must not be swallowed)
            let (block_state, has_lease, ref_count) =
                match self
                    .raft_node
                    .read(false, |sm| {
                        // Fail-closed: return Err if storage read fails
                        let block_meta = sm.storage().get_block(*block_id).map_err(|e| {
                            MetadataError::Internal(format!("Failed to read block {}: {}", block_id, e))
                        })?;
                        let lease = sm.storage().get_lease(*block_id).map_err(|e| {
                            MetadataError::Internal(format!("Failed to read lease {}: {}", block_id, e))
                        })?;

                        let state = block_meta.as_ref().map(|b| b.state);
                        let has_lease = lease.is_some();
                        let ref_count = if block_meta.is_some() {
                            ref_counts
                                .get(&block_id.data_handle_id)
                                .and_then(|file_refs| file_refs.get(block_id))
                                .copied()
                                .unwrap_or(0)
                        } else {
                            0
                        };
                        Ok((state, has_lease, ref_count))
                    })
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        // Fail-closed: set gate to degraded and skip this block
                        {
                            let mut gate = self.gc_gate.write();
                            gate.set_degraded("block_read_failed".to_string(), e.to_string(), now_ms);
                        }
                        self.metrics.gc_skipped_total.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            task = "gc",
                            block_id = %block_id,
                            error = %e,
                            "Failed to read block/lease, gate degraded, skipping block"
                        );
                        continue;
                    }
                };

            // Candidate conditions:
            // 1. Block state is Sealed or Aborted
            // 2. No active lease
            // 3. ref_count == 0
            if let Some(state) = block_state {
                if matches!(state, BlockState::Sealed | BlockState::Aborted) && !has_lease && ref_count == 0 {
                    blocks_to_check.push(*block_id);
                }
            }
        }

        // Second pass: update candidates (lock held briefly, no await)
        {
            let mut candidates = self.candidates.write();
            for block_id in &blocks_to_check {
                let reason = "sealed_or_aborted_no_ref_no_lease".to_string();
                if let Some(candidate) = candidates.get_mut(block_id) {
                    candidate.update(now_ms, reason.clone());
                    updated_candidates += 1;
                } else {
                    candidates.insert(*block_id, GcCandidate::new(now_ms, reason));
                    new_candidates += 1;
                }
            }
        }

        self.metrics
            .gc_candidates_total
            .fetch_add((new_candidates + updated_candidates) as u64, Ordering::Relaxed);
        let total_candidates = self.candidates.read().len();
        self.metrics.gc_candidates.store(total_candidates, Ordering::Relaxed);

        if new_candidates > 0 || updated_candidates > 0 || total_candidates > 0 {
            info!(
                task = "gc",
                new_candidates, updated_candidates, total_candidates, "GC mark pass completed"
            );
        } else {
            debug!(
                task = "gc",
                new_candidates, updated_candidates, total_candidates, "GC mark pass completed"
            );
        }

        // Sweep pass: check block report convergence before destructive actions
        let epoch = self.worker_manager.get_metadata_epoch();
        let active_ttl_ms = self.worker_manager.heartbeat_timeout_sec() * 1000;
        let snapshot = self.worker_manager.blockreport_convergence_snapshot(
            now_ms,
            active_ttl_ms,
            epoch,
            BLOCKREPORT_CONVERGENCE_THRESHOLD,
        );

        // Update metrics
        self.metrics
            .maintenance_blockreport_active_workers
            .store(snapshot.active_workers, Ordering::Relaxed);
        self.metrics
            .maintenance_blockreport_full_reported_workers
            .store(snapshot.full_reported_workers, Ordering::Relaxed);
        self.metrics
            .maintenance_blockreport_ratio
            .store((snapshot.ratio * 1000.0) as usize, Ordering::Relaxed);
        self.metrics
            .maintenance_blockreport_converged
            .store(if snapshot.converged { 1 } else { 0 }, Ordering::Relaxed);

        // Hard gate: block report must be converged before sweep
        if !snapshot.converged {
            self.metrics.gc_skipped_total.fetch_add(1, Ordering::Relaxed);
            self.metrics.gc_gate_state.store(0, Ordering::Relaxed);

            // Rate-limited logging
            const LOG_INTERVAL_MS: u64 = 5 * 60 * 1000; // 5 minutes
            {
                let mut last_log = self.last_log_ms.write();
                if now_ms.saturating_sub(*last_log) >= LOG_INTERVAL_MS {
                    warn!(
                        task = "gc",
                        active_workers = snapshot.active_workers,
                        full_reported_workers = snapshot.full_reported_workers,
                        ratio = snapshot.ratio,
                        threshold = BLOCKREPORT_CONVERGENCE_THRESHOLD,
                        epoch = epoch,
                        "GC sweep blocked: block report not converged. Only scan/candidates allowed."
                    );
                    *last_log = now_ms;
                }
            }
            // Allow scan/candidates accumulation, but skip sweep
            return Ok(());
        }

        // Block report converged - proceed with sweep
        let guard_state_id = self
            .raft_node
            .get_last_applied_state_id()
            .ok_or_else(|| MetadataError::Internal("Failed to get guard_state_id".to_string()))?;

        let to_sweep: Vec<BlockId> = {
            let candidates = self.candidates.read();
            candidates
                .iter()
                .filter_map(|(block_id, candidate)| {
                    if candidate.is_eligible_for_sweep(now_ms) {
                        Some(*block_id)
                    } else {
                        None
                    }
                })
                .collect()
        };

        let mut eligible_blocks = Vec::new();
        let mut to_remove = Vec::new();
        let mut skipped_total = 0;

        for block_id in &to_sweep {
            // Final check: verify block is still eligible (fail-closed: Err must not be swallowed)
            let should_delete =
                match self
                    .raft_node
                    .read(false, |sm| {
                        // Fail-closed: return Err if storage read fails
                        let block_meta = sm.storage().get_block(*block_id).map_err(|e| {
                            MetadataError::Internal(format!("Failed to read block {}: {}", block_id, e))
                        })?;
                        let lease = sm.storage().get_lease(*block_id).map_err(|e| {
                            MetadataError::Internal(format!("Failed to read lease {}: {}", block_id, e))
                        })?;

                        let ref_count = {
                            let ref_counts = self.block_ref_counts.read();
                            ref_counts
                                .get(&block_id.data_handle_id)
                                .and_then(|file_refs| file_refs.get(block_id))
                                .copied()
                                .unwrap_or(0)
                        };

                        // State gate: only allow deletion of Sealed/Aborted blocks (not Open/Writing)
                        Ok(block_meta
                            .as_ref()
                            .map(|b| {
                                let state_allowed = matches!(b.state, BlockState::Sealed | BlockState::Aborted);
                                let no_lease = lease.is_none();
                                let no_refs = ref_count == 0;
                                state_allowed && no_lease && no_refs
                            })
                            .unwrap_or(false))
                    })
                    .await
                {
                    Ok(true) => true,
                    Ok(false) => {
                        // Block no longer eligible - remove from candidates
                        to_remove.push(*block_id);
                        false
                    }
                    Err(e) => {
                        // Fail-closed: set gate to degraded and skip this block
                        {
                            let mut gate = self.gc_gate.write();
                            gate.set_degraded("block_read_failed".to_string(), e.to_string(), now_ms);
                        }
                        self.metrics.gc_skipped_total.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            task = "gc",
                            block_id = %block_id,
                            error = %e,
                            "Failed to read block/lease during sweep, gate degraded, skipping block"
                        );
                        skipped_total += 1;
                        false
                    }
                };

            if should_delete {
                // Check inflight registry against other maintenance actions.
                if !self
                    .inflight_registry
                    .try_acquire(*block_id, InflightKind::Delete, None)?
                {
                    // Block is in-flight for another maintenance action - skip
                    debug!(
                        task = "gc",
                        block_id = %block_id,
                        "Skipping block: already in-flight for another maintenance action"
                    );
                    self.metrics.gc_skipped_total.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                eligible_blocks.push(*block_id);
            }
        }

        // Create DeleteIntents for eligible blocks (batch operation)
        let mut intents_created = 0;
        if !eligible_blocks.is_empty() {
            const GRACE_WINDOW_MS: u64 = 10 * 60 * 1000; // 10 minutes default grace window
            let not_before_ms = now_ms + GRACE_WINDOW_MS;

            // Create intent builder
            let intent_builder = DeleteIntentBuilder::new(Arc::clone(&self.mount_table), Arc::clone(&self.storage));

            // Generate intents
            let mut intents = Vec::new();
            let mut built_blocks = Vec::new();
            let mut build_skipped = 0;
            for block_id in &eligible_blocks {
                // Unified gate check before creating intent
                // The grace window is persisted on the intent and enforced by DeleteExecutor.
                let ctx = DestructiveCheckContext::new("gc_create_delete_intent")
                    .with_block_id(*block_id)
                    .with_guard_state_id(guard_state_id);

                match self.destructive_gate.check_destructive_allowed(&ctx)? {
                    crate::destructive_gate::DestructiveCheckResult::Allowed => {
                        // Proceed with intent creation
                    }
                    crate::destructive_gate::DestructiveCheckResult::Blocked { reason } => {
                        // Gate check failed - release inflight lock and skip
                        self.inflight_registry.release(*block_id);
                        debug!(
                            task = "gc",
                            block_id = %block_id,
                            reason = %reason,
                            "Skipping DeleteIntent creation: gate check failed"
                        );
                        self.metrics.gc_skipped_total.fetch_add(1, Ordering::Relaxed);
                        build_skipped += 1;
                        continue;
                    }
                    crate::destructive_gate::DestructiveCheckResult::NeedRefresh { reason, .. } => {
                        // Mount epoch mismatch - release inflight lock and skip
                        self.inflight_registry.release(*block_id);
                        warn!(
                            task = "gc",
                            block_id = %block_id,
                            reason = %reason,
                            "Skipping DeleteIntent creation: mount epoch mismatch, need refresh"
                        );
                        self.metrics.gc_skipped_total.fetch_add(1, Ordering::Relaxed);
                        build_skipped += 1;
                        continue;
                    }
                }
                match intent_builder.build(
                    0,
                    *block_id,
                    DeleteIntentReason::Gc,
                    now_ms,
                    not_before_ms,
                    guard_state_id,
                    Vec::new(), // Empty target_workers for GC
                ) {
                    Ok(intent) => {
                        intents.push(intent);
                        built_blocks.push(*block_id);
                    }
                    Err(e) => {
                        // Fail-closed: router missing or unable to resolve, skip creating DeleteIntent
                        warn!(
                            task = "gc",
                            block_id = %block_id,
                            error = %e,
                            "Failed to build DeleteIntent, skipping"
                        );
                        self.inflight_registry.release(*block_id);
                        self.metrics.gc_skipped_total.fetch_add(1, Ordering::Relaxed);
                        build_skipped += 1;
                        continue;
                    }
                }
            }
            skipped_total += build_skipped;

            if intents.is_empty() {
                debug!(
                    task = "gc",
                    eligible_count = eligible_blocks.len(),
                    skipped_count = build_skipped,
                    "No GC DeleteIntents built after gate and owner resolution checks"
                );
            } else {
                // Batch propose CreateDeleteIntents
                use crate::raft::{Command, DedupKey};
                let command = Command::AllocateDeleteIntents {
                    dedup: DedupKey::system(),
                    intents,
                };

                match self.raft_node.propose(command).await {
                    Ok(_) => {
                        intents_created = built_blocks.len();
                        info!(
                            task = "gc",
                            count = intents_created,
                            "Created delete intents for GC (batch operation)"
                        );
                        // Update metrics
                        self.metrics
                            .delete_intents_created_total
                            .fetch_add(intents_created as u64, Ordering::Relaxed);
                        self.metrics
                            .maintenance_gc_created_intents_total
                            .fetch_add(intents_created as u64, Ordering::Relaxed);
                        for block_id in &built_blocks {
                            self.inflight_registry.release(*block_id);
                        }
                        // Remove from candidates after successful creation.
                        to_remove.extend(built_blocks);
                    }
                    Err(e) => {
                        warn!(
                            task = "gc",
                            count = built_blocks.len(),
                            error = %e,
                            "Failed to propose CreateDeleteIntents command"
                        );
                        self.metrics
                            .delete_intents_create_failed_total
                            .fetch_add(built_blocks.len() as u64, Ordering::Relaxed);
                        for block_id in &built_blocks {
                            self.inflight_registry.release(*block_id);
                        }
                        skipped_total += built_blocks.len();
                    }
                }
            }
        }

        if skipped_total > 0 {
            debug!(
                task = "gc",
                skipped_total, "GC sweep: skipped blocks (not eligible or gate not satisfied)"
            );
        }

        // Update candidates
        {
            let mut candidates = self.candidates.write();
            for block_id in &to_remove {
                candidates.remove(block_id);
            }
            // Cleanup stale candidates (older than 24 hours)
            const CANDIDATE_TTL_MS: u64 = 24 * 60 * 60 * 1000;
            candidates.retain(|_, candidate| now_ms.saturating_sub(candidate.last_seen_ms) < CANDIDATE_TTL_MS);
        }

        self.metrics
            .gc_actions_total
            .fetch_add(intents_created as u64, Ordering::Relaxed);
        self.metrics
            .gc_candidates
            .store(self.candidates.read().len(), Ordering::Relaxed);

        if intents_created > 0 {
            info!(
                task = "gc",
                count = intents_created,
                "GC sweep pass completed: delete intents created"
            );
        } else {
            debug!(task = "gc", "GC sweep pass completed: no intents created");
        }

        Ok(())
    }

    /// Reload block reference counts (self-healing).
    pub async fn reload_refcounts(
        &self,
        storage: &Arc<RocksDBStorage>,
        block_ref_counts: &Arc<RwLock<BlockRefCounts>>,
        gc_gate: &Arc<RwLock<TaskGate>>,
        metrics: &Arc<MetadataMetrics>,
    ) -> MetadataResult<()> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Only reload if gate is not ready
        {
            let gate = gc_gate.read();
            if gate.state() == GateState::Ready {
                return Ok(()); // Already ready, no need to reload
            }
        }

        match storage.get_all_block_ref_counts() {
            Ok(ref_counts) => {
                let mut new_ref_counts = HashMap::new();
                // Convert from global block_id refcount to data_handle_id:block_id format.
                for (block_id, count) in ref_counts {
                    let data_handle_id = block_id.data_handle_id;
                    let file_refs = new_ref_counts.entry(data_handle_id).or_insert_with(HashMap::new);
                    file_refs.insert(block_id, count as u32);
                }

                // Update ref_counts
                {
                    let mut refs = block_ref_counts.write();
                    *refs = new_ref_counts;
                }

                // Update gate to Ready
                {
                    let mut gate = gc_gate.write();
                    gate.set_ready(now_ms);
                }

                metrics.gc_refcount_reload_success_total.fetch_add(1, Ordering::Relaxed);
                info!(
                    task = "gc",
                    count = block_ref_counts.read().len(),
                    "GC refcounts reloaded successfully, gate recovered to READY"
                );

                Ok(())
            }
            Err(e) => {
                // Update gate to Degraded
                {
                    let mut gate = gc_gate.write();
                    gate.set_degraded("refcount_reload_failed".to_string(), e.to_string(), now_ms);
                }
                metrics.gc_refcount_reload_fail_total.fetch_add(1, Ordering::Relaxed);
                Err(MetadataError::Internal(format!("Failed to reload refcounts: {}", e)))
            }
        }
    }

    /// Collect all blocks from storage.
    async fn collect_all_blocks(&self) -> MetadataResult<Vec<BlockId>> {
        use rocksdb::IteratorMode;

        let db = self.storage.db();
        let cf = db
            .cf_handle("blocks")
            .ok_or_else(|| MetadataError::Internal("Blocks CF not found".to_string()))?;

        let mut blocks = Vec::new();
        let iter = db.iterator_cf(cf, IteratorMode::Start);

        for item in iter {
            let (key, _) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;

            let key_str = String::from_utf8_lossy(&key);
            if let Some((file_str, index_str)) = key_str.split_once(':') {
                if let (Ok(data_handle_id_raw), Ok(index_raw)) = (file_str.parse::<u64>(), index_str.parse::<u32>()) {
                    let data_handle_id = DataHandleId::new(data_handle_id_raw);
                    let block_index = BlockIndex::new(index_raw);
                    let block_id = BlockId::new(data_handle_id, block_index);
                    blocks.push(block_id);
                }
            }
        }

        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RaftConfig;
    use crate::mount::{DataIoPolicy, MountKind};
    use crate::raft::AppRaftStateMachine;
    use crate::state::{BlockMetaState, DeleteIntentStatus};
    use crate::worker::WorkerManager;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;
    use tempfile::TempDir;
    use types::block::{BlockPlacement, BlockState};
    use types::fs::{FileAttrs, Inode, InodeId};
    use types::GroupName;
    use types::WorkerId;

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    struct GcTestEnv {
        _temp_dir: TempDir,
        storage: Arc<RocksDBStorage>,
        mount_table: Arc<MountTable>,
        metrics: Arc<MetadataMetrics>,
        candidates: Arc<RwLock<HashMap<BlockId, GcCandidate>>>,
        service: GcService,
    }

    async fn new_gc_test_env() -> MetadataResult<GcTestEnv> {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(temp_dir.path())?);
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(
                raft_config.node_id,
                Arc::clone(&storage),
                Arc::clone(&state_machine),
                &raft_config,
            )
            .await
            .unwrap(),
        );
        raft_node
            .initialize_single_node("127.0.0.1:0".to_string())
            .await
            .unwrap();
        wait_for_test_leader(&raft_node).await;

        let worker_manager = Arc::new(WorkerManager::new(60));
        let block_ref_counts = Arc::new(RwLock::new(HashMap::new()));
        let gc_gate = Arc::new(RwLock::new(TaskGate::new()));
        let metrics = Arc::new(MetadataMetrics::new());
        let candidates = Arc::new(RwLock::new(HashMap::new()));
        let last_log_ms = Arc::new(RwLock::new(0));
        let destructive_gate = Arc::new(DestructiveGate::new(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::clone(&mount_table),
        ));
        let inflight_registry = Arc::new(InflightRegistry::new(5 * 60 * 1000));
        let service = GcService::new(
            raft_node,
            storage.clone(),
            worker_manager,
            block_ref_counts,
            gc_gate,
            Arc::clone(&metrics),
            Arc::clone(&candidates),
            last_log_ms,
            destructive_gate,
            inflight_registry,
            Arc::clone(&mount_table),
        );

        Ok(GcTestEnv {
            _temp_dir: temp_dir,
            storage,
            mount_table,
            metrics,
            candidates,
            service,
        })
    }

    async fn wait_for_test_leader(raft_node: &AppRaftNode) {
        for _ in 0..100 {
            if raft_node.is_leader() && raft_node.get_last_applied_state_id().is_some() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
        assert!(raft_node.is_leader());
        assert!(raft_node.get_last_applied_state_id().is_some());
    }

    fn put_sealed_block(storage: &RocksDBStorage, block_id: BlockId, inode_id: InodeId) -> MetadataResult<()> {
        storage.put_block(&BlockMetaState {
            block_id,
            inode_id,
            data_handle_id: block_id.data_handle_id,
            state: BlockState::Sealed,
            placement: BlockPlacement {
                primary: WorkerId::new(1),
                replicas: Vec::new(),
            },
            committed_length: 4096,
        })
    }

    fn bind_block_to_mount_owner(
        storage: &RocksDBStorage,
        mount_table: &MountTable,
        block_id: BlockId,
        inode_id: InodeId,
        owner_group: GroupName,
    ) -> MetadataResult<()> {
        let mount_entry = mount_table.create_mount(
            format!("/gc-{}", block_id.data_handle_id.as_raw()),
            MountKind::External,
            Some("ufs://gc-test".to_string()),
            DataIoPolicy::Allow,
            owner_group,
            InodeId::new(10),
        )?;
        let inode = Inode::new_file(
            inode_id,
            FileAttrs::new(),
            mount_entry.mount_id,
            block_id.data_handle_id,
        );
        storage.put_inode(&inode)?;
        storage.put_data_handle_owner(block_id.data_handle_id, inode_id)?;
        Ok(())
    }

    fn seed_eligible_candidate(candidates: &RwLock<HashMap<BlockId, GcCandidate>>, block_id: BlockId) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        candidates.write().insert(
            block_id,
            GcCandidate {
                first_seen_ms: now_ms - 11 * 60 * 1000,
                last_seen_ms: now_ms - 11 * 60 * 1000,
                seen_count: 2,
                last_reason: "test".to_string(),
            },
        );
    }

    #[tokio::test]
    async fn gc_intent_build_failure_does_not_remove_unbuilt_candidate() -> MetadataResult<()> {
        let env = new_gc_test_env().await?;
        let failed_block = BlockId::new(DataHandleId::new(901), BlockIndex::new(0));
        let built_block = BlockId::new(DataHandleId::new(902), BlockIndex::new(0));
        put_sealed_block(&env.storage, failed_block, InodeId::new(901))?;
        put_sealed_block(&env.storage, built_block, InodeId::new(902))?;
        bind_block_to_mount_owner(
            &env.storage,
            &env.mount_table,
            built_block,
            InodeId::new(902),
            group_name("g7"),
        )?;
        seed_eligible_candidate(&env.candidates, failed_block);
        seed_eligible_candidate(&env.candidates, built_block);

        env.service.run_gc().await?;

        let candidates = env.candidates.read();
        assert!(candidates.contains_key(&failed_block));
        assert!(!candidates.contains_key(&built_block));
        drop(candidates);

        let intents = env.storage.list_pending_delete_intents(10, u64::MAX)?;
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].block_id, built_block);
        assert!(matches!(intents[0].status, DeleteIntentStatus::Pending));

        Ok(())
    }

    #[tokio::test]
    async fn gc_intent_build_failure_counts_only_built_intents() -> MetadataResult<()> {
        let env = new_gc_test_env().await?;
        let failed_block = BlockId::new(DataHandleId::new(911), BlockIndex::new(0));
        let built_block = BlockId::new(DataHandleId::new(912), BlockIndex::new(0));
        put_sealed_block(&env.storage, failed_block, InodeId::new(911))?;
        put_sealed_block(&env.storage, built_block, InodeId::new(912))?;
        bind_block_to_mount_owner(
            &env.storage,
            &env.mount_table,
            built_block,
            InodeId::new(912),
            group_name("g8"),
        )?;
        seed_eligible_candidate(&env.candidates, failed_block);
        seed_eligible_candidate(&env.candidates, built_block);

        env.service.run_gc().await?;

        assert_eq!(env.metrics.delete_intents_created_total.load(Ordering::Relaxed), 1);
        assert_eq!(
            env.metrics.maintenance_gc_created_intents_total.load(Ordering::Relaxed),
            1
        );
        assert_eq!(env.metrics.gc_actions_total.load(Ordering::Relaxed), 1);
        assert_eq!(
            env.metrics.delete_intents_create_failed_total.load(Ordering::Relaxed),
            0
        );

        Ok(())
    }
}
