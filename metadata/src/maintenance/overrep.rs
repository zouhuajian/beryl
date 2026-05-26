// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Over-replicated replica cleanup service: removes excess replicas when current > desired.
//!
//! This module implements:
//! - Scanning blocks to detect over-replication (current > desired)
//! - Selection algorithm: load_score > fault_domain > last_access > stable_random
//! - Conflict protection via InflightRegistry
//! - Intent creation via DeleteIntentBuilder

use crate::destructive_gate::{DestructiveCheckContext, DestructiveGate};
use crate::error::MetadataResult;
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
use types::ids::{BlockId, ShardGroupId, WorkerId};

use super::delete::DeleteIntentBuilder;
use super::repair::RepairPolicy;

/// Over-replication candidate tracked across scans.
#[derive(Debug, Clone)]
pub struct OverRepCandidate {
    first_seen_ms: u64,
    last_seen_ms: u64,
    seen_count: u32,
    current_replicas: u32,
    desired_replicas: u32,
}

impl OverRepCandidate {
    fn new(now_ms: u64, current_replicas: u32, desired_replicas: u32) -> Self {
        Self {
            first_seen_ms: now_ms,
            last_seen_ms: now_ms,
            seen_count: 1,
            current_replicas,
            desired_replicas,
        }
    }

    fn update(&mut self, now_ms: u64, current_replicas: u32, desired_replicas: u32) {
        self.last_seen_ms = now_ms;
        self.seen_count += 1;
        self.current_replicas = current_replicas;
        self.desired_replicas = desired_replicas;
    }

    /// Check if candidate is eligible for eviction.
    /// Minimum conditions: seen_count >= 2 and age >= grace_period (10 minutes).
    fn is_eligible_for_eviction(&self, now_ms: u64) -> bool {
        const MIN_SEEN_COUNT: u32 = 2;
        const GRACE_PERIOD_MS: u64 = 10 * 60 * 1000; // 10 minutes

        self.seen_count >= MIN_SEEN_COUNT
            && (now_ms.saturating_sub(self.first_seen_ms) >= GRACE_PERIOD_MS)
            && self.current_replicas > self.desired_replicas
    }
}

/// Replica information for selection algorithm.
#[derive(Debug, Clone)]
struct ReplicaInfo {
    worker_id: WorkerId,
    load_score: Option<f64>,      // Higher = more loaded (prefer to delete)
    fault_domain: Option<String>, // For diversity preservation
    last_access_ms: Option<u64>,  // Older = prefer to delete
}

/// Over-replicated replica cleanup service.
pub struct OverReplicaCleanupService {
    raft_node: Arc<AppRaftNode>,
    storage: Arc<RocksDBStorage>,
    worker_manager: Arc<WorkerManager>,
    candidates: Arc<RwLock<HashMap<BlockId, OverRepCandidate>>>,
    metrics: Arc<MetadataMetrics>,
    destructive_gate: Arc<DestructiveGate>,
    inflight_registry: Arc<InflightRegistry>,
    mount_table: Arc<MountTable>,
    intent_builder: DeleteIntentBuilder,
    repair_policy: RepairPolicy,
}

impl OverReplicaCleanupService {
    /// Create a new OverReplicaCleanupService.
    // Constructor mirrors maintenance runtime wiring; grouping dependencies would hide ownership.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        raft_node: Arc<AppRaftNode>,
        storage: Arc<RocksDBStorage>,
        worker_manager: Arc<WorkerManager>,
        metrics: Arc<MetadataMetrics>,
        candidates: Arc<RwLock<HashMap<BlockId, OverRepCandidate>>>,
        destructive_gate: Arc<DestructiveGate>,
        inflight_registry: Arc<InflightRegistry>,
        mount_table: Arc<MountTable>,
        repair_policy: RepairPolicy,
    ) -> Self {
        let intent_builder = DeleteIntentBuilder::new(Arc::clone(&mount_table), Arc::clone(&storage));

        Self {
            raft_node,
            storage,
            worker_manager,
            candidates,
            metrics,
            destructive_gate,
            inflight_registry,
            mount_table,
            intent_builder,
            repair_policy,
        }
    }

    /// Run one cleanup cycle (scan + evict).
    pub async fn run_once(&self) -> MetadataResult<()> {
        // Leader-only check
        if !self.raft_node.is_leader() {
            return Ok(());
        }

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Scan and collect candidates
        self.scan_candidates(now_ms).await?;

        // Create intents for eligible candidates
        self.create_intents(now_ms).await?;

        Ok(())
    }

    /// Scan blocks and collect over-replication candidates.
    async fn scan_candidates(&self, now_ms: u64) -> MetadataResult<()> {
        // Note: We scan blocks from worker_manager blockreport instead of state machine
        // This is more efficient and reflects actual block presence

        // Scan blocks from worker_manager blockreport
        // Note: This is a simplified scan - in production, we'd iterate through all blocks efficiently
        // For now, we'll scan blocks that are reported by workers
        let all_blocks = self.worker_manager.list_reported_blocks();
        let scanned_count = all_blocks.len();

        // Collect updates first (without holding lock across await)
        let mut updates: Vec<(BlockId, Option<(u32, u32)>)> = Vec::new();
        for reported in all_blocks {
            let block_id = reported.block_id;
            let group_id = reported.group_id;
            let owner_group_id =
                match crate::maintenance::owner_group_for_block(&self.storage, &self.mount_table, block_id) {
                    Ok(group_id) => group_id,
                    Err(error) => {
                        debug!(
                            block_id = %block_id,
                            error = %error,
                            "Skipping overrep scan: block owner group is not authoritative"
                        );
                        continue;
                    }
                };
            if owner_group_id != group_id {
                debug!(
                    block_id = %block_id,
                    reported_group_id = group_id.as_raw(),
                    owner_group_id = owner_group_id.as_raw(),
                    "Skipping overrep scan: reported group is not authoritative"
                );
                continue;
            }
            let current_locations = self.worker_manager.get_block_locations(group_id, block_id);
            if current_locations.is_empty() {
                debug!(
                    block_id = %block_id,
                    group_id = group_id.as_raw(),
                    "Skipping overrep scan: block has no live locations in owner group"
                );
                continue;
            }
            let current_replicas = current_locations.len() as u32;

            // Get desired replicas from file layout or the current repair policy placeholder.
            let desired_replicas = self
                .get_desired_replicas(block_id)
                .await
                .unwrap_or(self.repair_policy.default_replication_factor as u32);

            if current_replicas > desired_replicas {
                // Over-replicated: mark for update
                updates.push((block_id, Some((current_replicas, desired_replicas))));
            } else {
                // Not over-replicated: mark for removal
                updates.push((block_id, None));
            }
        }

        // Apply updates while holding lock (no await points)
        {
            let mut candidates = self.candidates.write();
            for (block_id, update) in updates {
                match update {
                    Some((current_replicas, desired_replicas)) => match candidates.get_mut(&block_id) {
                        Some(candidate) => {
                            candidate.update(now_ms, current_replicas, desired_replicas);
                        }
                        None => {
                            candidates.insert(
                                block_id,
                                OverRepCandidate::new(now_ms, current_replicas, desired_replicas),
                            );
                        }
                    },
                    None => {
                        candidates.remove(&block_id);
                    }
                }
            }
        }

        // Update metrics after releasing lock
        let candidates_count = {
            let candidates = self.candidates.read();
            let count = candidates.len();
            self.metrics
                .overrep_candidates_total
                .store(count as u64, Ordering::Relaxed);
            count
        };
        self.metrics
            .overrep_scanned_total
            .fetch_add(scanned_count as u64, Ordering::Relaxed);

        debug!(
            scanned = scanned_count,
            candidates = candidates_count,
            "OverRep scan completed"
        );

        Ok(())
    }

    /// Get desired replica count for a block (from file layout).
    ///
    /// NOTE: FileMeta has been removed. Layout information is now stored in inodes.
    /// For now, this uses RepairPolicy default until per-block policy exists.
    async fn get_desired_replicas(&self, _block_id: BlockId) -> Option<u32> {
        Some(self.repair_policy.default_replication_factor as u32)
    }

    /// Create intents for eligible candidates.
    async fn create_intents(&self, now_ms: u64) -> MetadataResult<()> {
        let candidates_to_process: Vec<(BlockId, OverRepCandidate)> = {
            let candidates = self.candidates.read();
            candidates
                .iter()
                .filter(|(_, candidate)| candidate.is_eligible_for_eviction(now_ms))
                .map(|(block_id, candidate)| (*block_id, candidate.clone()))
                .collect()
        };

        if candidates_to_process.is_empty() {
            return Ok(());
        }

        let mut intents_created = 0;
        let mut skipped_conflict = 0;
        let mut skipped_gate = 0;
        let mut skipped_state = 0;

        for (block_id, candidate) in candidates_to_process {
            // Check inflight registry against other maintenance actions.
            if !self
                .inflight_registry
                .try_acquire(block_id, InflightKind::OverRepEvict, None)?
            {
                skipped_conflict += 1;
                debug!(
                    block_id = %block_id,
                    "Skipping overrep cleanup: block in-flight for another maintenance action"
                );
                continue;
            }

            // Check block state: only process Sealed/Aborted blocks
            let block_state_allowed = self
                .raft_node
                .read(false, |sm| {
                    let block_meta = sm.storage().get_block(block_id)?;
                    Ok(block_meta
                        .as_ref()
                        .map(|b| matches!(b.state, BlockState::Sealed | BlockState::Aborted))
                        .unwrap_or(false))
                })
                .await?;

            if !block_state_allowed {
                skipped_state += 1;
                self.inflight_registry.release(block_id);
                debug!(
                    block_id = %block_id,
                    "Skipping overrep cleanup: block state not allowed"
                );
                continue;
            }

            // Check for active lease
            let has_active_lease = self
                .raft_node
                .read(false, |sm| {
                    let lease = sm.storage().get_lease(block_id)?;
                    Ok(lease.is_some())
                })
                .await?;

            if has_active_lease {
                skipped_state += 1;
                self.inflight_registry.release(block_id);
                debug!(
                    block_id = %block_id,
                    "Skipping overrep cleanup: block has active lease"
                );
                continue;
            }

            let group_id = match crate::maintenance::owner_group_for_block(&self.storage, &self.mount_table, block_id) {
                Ok(group_id) => group_id,
                Err(error) => {
                    self.inflight_registry.release(block_id);
                    debug!(
                        block_id = %block_id,
                        error = %error,
                        "Skipping overrep cleanup: block owner group is not authoritative"
                    );
                    continue;
                }
            };
            let current_locations = self.worker_manager.get_block_locations(group_id, block_id);
            if current_locations.is_empty() {
                self.inflight_registry.release(block_id);
                debug!(
                    block_id = %block_id,
                    group_id = group_id.as_raw(),
                    "Skipping overrep cleanup: block has no live locations in owner group"
                );
                continue;
            }
            if current_locations.len() as u32 <= candidate.desired_replicas {
                // No longer over-replicated
                self.inflight_registry.release(block_id);
                let mut candidates = self.candidates.write();
                candidates.remove(&block_id);
                continue;
            }

            // Select target workers to evict
            let target_workers = self.select_target_workers(
                group_id,
                &current_locations,
                candidate.current_replicas,
                candidate.desired_replicas,
            )?;

            if target_workers.is_empty() {
                self.inflight_registry.release(block_id);
                warn!(
                    block_id = %block_id,
                    "No target workers selected for eviction"
                );
                continue;
            }

            // Check gate before creating intent
            let guard_state_id = self.raft_node.get_last_applied_state_id().unwrap_or_default();
            let not_before_ms = now_ms + 60_000; // 1 minute grace window

            let mut ctx = DestructiveCheckContext::new("overrep_cleanup")
                .with_block_id(block_id)
                .with_not_before_ms(not_before_ms)
                .with_guard_state_id(guard_state_id);

            // Resolve group_id and guard_watermark using inode -> mount owner group.
            let group_id = crate::maintenance::owner_group_for_block(&self.storage, &self.mount_table, block_id)
                .inspect_err(|_e| {
                    self.inflight_registry.release(block_id);
                })?;
            ctx = ctx.with_group_id(group_id);
            let guard_watermark = types::group_watermark::GroupStateWatermark::new(group_id, guard_state_id);
            ctx = ctx.with_guard_watermark(guard_watermark);
            let mount_epoch = types::group_watermark::MountEpoch::new(self.mount_table.version());
            ctx = ctx.with_mount_epoch(mount_epoch);

            match self.destructive_gate.check_destructive_allowed(&ctx)? {
                crate::destructive_gate::DestructiveCheckResult::Allowed => {
                    // Proceed with intent creation
                }
                crate::destructive_gate::DestructiveCheckResult::Blocked { reason } => {
                    skipped_gate += 1;
                    self.inflight_registry.release(block_id);
                    debug!(
                        block_id = %block_id,
                        reason = %reason,
                        "Gate check blocked overrep cleanup"
                    );
                    continue;
                }
                crate::destructive_gate::DestructiveCheckResult::NeedRefresh { reason, .. } => {
                    skipped_gate += 1;
                    self.inflight_registry.release(block_id);
                    warn!(
                        block_id = %block_id,
                        reason = %reason,
                        "Gate check need refresh for overrep cleanup"
                    );
                    continue;
                }
            }

            let intent = match self.intent_builder.build(
                0,
                block_id,
                DeleteIntentReason::OverRep,
                now_ms,
                not_before_ms,
                guard_state_id,
                target_workers.clone(),
            ) {
                Ok(intent) => intent,
                Err(e) => {
                    self.inflight_registry.release(block_id);
                    return Err(e);
                }
            };

            // Propose intent via Raft
            use crate::raft::{Command, DedupKey};
            let command = Command::AllocateDeleteIntents {
                dedup: DedupKey::system(),
                intents: vec![intent.clone()],
            };

            match self.raft_node.propose(command).await {
                Ok(_) => {
                    intents_created += 1;
                    self.metrics
                        .overrep_intents_created_total
                        .fetch_add(1, Ordering::Relaxed);
                    info!(
                        block_id = %block_id,
                        target_workers = ?target_workers,
                        current_replicas = candidate.current_replicas,
                        desired_replicas = candidate.desired_replicas,
                        "Created OverRep eviction intent"
                    );
                    self.inflight_registry.release(block_id);
                }
                Err(e) => {
                    self.inflight_registry.release(block_id);
                    error!(
                        block_id = %block_id,
                        error = %e,
                        "Failed to propose OverRep eviction intent"
                    );
                }
            }
        }

        self.metrics
            .overrep_skipped_conflict_total
            .fetch_add(skipped_conflict, Ordering::Relaxed);
        self.metrics
            .overrep_skipped_gate_total
            .fetch_add(skipped_gate, Ordering::Relaxed);
        self.metrics
            .overrep_skipped_state_total
            .fetch_add(skipped_state, Ordering::Relaxed);

        debug!(
            intents_created,
            skipped_conflict, skipped_gate, skipped_state, "OverRep intent creation completed"
        );

        Ok(())
    }

    /// Select target workers to evict using selection algorithm.
    ///
    /// Algorithm priority:
    /// 1. Fault domain diversity (preserve diversity, prefer deleting from domains with >1 replica)
    /// 2. Load score (prefer deleting from highest load)
    /// 3. Last access (prefer deleting least recently accessed)
    /// 4. Stable random (hash-based sorting for determinism)
    fn select_target_workers(
        &self,
        group_id: ShardGroupId,
        current_locations: &[WorkerId],
        current_replicas: u32,
        desired_replicas: u32,
    ) -> MetadataResult<Vec<WorkerId>> {
        let k = (current_replicas - desired_replicas) as usize;
        if k == 0 || k > current_locations.len() {
            return Ok(Vec::new());
        }

        // Build replica info with available metadata
        let replicas: Vec<ReplicaInfo> = current_locations
            .iter()
            .map(|&worker_id| {
                // Get worker info (combined descriptor + runtime)
                let worker_info = self.worker_manager.get_worker(group_id, worker_id);

                let load_score = worker_info.as_ref().map(|w| {
                    // Calculate load score: capacity_used_ratio + active_ops_penalty
                    let capacity_ratio = if w.capacity_total > 0 {
                        w.capacity_used as f64 / w.capacity_total as f64
                    } else {
                        0.0
                    };
                    let ops_penalty = (w.active_reads + w.active_writes) as f64 * 0.1;
                    capacity_ratio + ops_penalty
                });

                let fault_domain = worker_info.and_then(|w| w.fault_domain.clone());
                // TODO: Get last_access from block metadata if available
                let last_access_ms = None;

                ReplicaInfo {
                    worker_id,
                    load_score,
                    fault_domain,
                    last_access_ms,
                }
            })
            .collect();

        // Group by fault domain
        let mut domain_groups: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, replica) in replicas.iter().enumerate() {
            if let Some(ref domain) = replica.fault_domain {
                domain_groups.entry(domain.clone()).or_default().push(idx);
            }
        }

        // Selection algorithm
        let mut selected_indices = Vec::new();
        let mut remaining_indices: Vec<usize> = (0..replicas.len()).collect();

        // First pass: fault domain diversity preservation
        // Prefer deleting from domains that have >1 replica (to preserve diversity)
        let domain_counts: HashMap<String, usize> = domain_groups
            .iter()
            .map(|(domain, indices)| (domain.clone(), indices.len()))
            .collect();

        // Sort domains by count (descending) - prefer deleting from domains with more replicas
        let mut domain_order: Vec<(String, usize)> = domain_counts.into_iter().collect();
        domain_order.sort_by_key(|entry| std::cmp::Reverse(entry.1));

        // Select from domains with >1 replica first
        for (domain, _count) in &domain_order {
            if domain_groups.get(domain).map(|v| v.len()).unwrap_or(0) > 1 {
                if let Some(indices) = domain_groups.get(domain) {
                    for &idx in indices {
                        if selected_indices.len() < k && remaining_indices.contains(&idx) {
                            selected_indices.push(idx);
                            remaining_indices.retain(|&i| i != idx);
                        }
                        if selected_indices.len() >= k {
                            break;
                        }
                    }
                }
                if selected_indices.len() >= k {
                    break;
                }
            }
        }

        // Second pass: if still need more, use load_score or last_access or stable random
        while selected_indices.len() < k && !remaining_indices.is_empty() {
            let best_idx = remaining_indices
                .iter()
                .max_by(|&&a, &&b| {
                    let replica_a = &replicas[a];
                    let replica_b = &replicas[b];

                    // Compare by load_score (higher = prefer to delete)
                    if let (Some(score_a), Some(score_b)) = (replica_a.load_score, replica_b.load_score) {
                        return score_a.partial_cmp(&score_b).unwrap_or(std::cmp::Ordering::Equal);
                    }

                    // Compare by last_access (older = prefer to delete)
                    if let (Some(access_a), Some(access_b)) = (replica_a.last_access_ms, replica_b.last_access_ms) {
                        return access_a.cmp(&access_b); // Older (smaller) first
                    }

                    // Stable random: hash-based sorting
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut hasher_a = DefaultHasher::new();
                    let mut hasher_b = DefaultHasher::new();
                    replica_a.worker_id.hash(&mut hasher_a);
                    replica_b.worker_id.hash(&mut hasher_b);
                    hasher_a.finish().cmp(&hasher_b.finish())
                })
                .copied();

            if let Some(idx) = best_idx {
                selected_indices.push(idx);
                remaining_indices.retain(|&i| i != idx);
            } else {
                break;
            }
        }

        // Convert indices to worker IDs
        let target_workers: Vec<WorkerId> = selected_indices
            .into_iter()
            .map(|idx| replicas[idx].worker_id)
            .collect();

        Ok(target_workers)
    }
}
