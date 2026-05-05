// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker manager: tracks worker liveness, capacity, and block locations.

use crate::error::{MetadataError, MetadataResult};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;
use types::block::BlockPlacement;
use types::ids::{BlockId, WorkerId};

/// Worker descriptor (low-frequency, authoritative, persisted in Raft).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerDescriptor {
    pub worker_id: WorkerId,
    pub address: String,
    /// Network transport kind (0=unspecified/grpc, 1=grpc, 2=quic, 3=rdma).
    pub net_transport_kind: i32,
    /// Worker epoch/boot_id (monotonically increasing or UUID-based).
    pub worker_epoch: u64,
    pub fault_domain: Option<String>,
}

/// Worker runtime (high-frequency, soft-state, memory-only with TTL).
#[derive(Clone, Debug)]
pub struct WorkerRuntime {
    pub last_seen_ms: u64, // Unix timestamp in milliseconds
    pub capacity_total: u64,
    pub capacity_used: u64,
    pub capacity_available: u64,
    pub active_reads: u32,
    pub active_writes: u32,
    pub health: HealthStatus,
}

/// Legacy WorkerInfo for backward compatibility (used in RocksDB storage).
/// This is only used when reading from RocksDB during migration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub worker_id: WorkerId,
    pub address: String,
    /// Network transport kind (0=unspecified/grpc, 1=grpc, 2=quic, 3=rdma).
    pub net_transport_kind: i32,
    /// Worker epoch/boot_id (monotonically increasing or UUID-based).
    pub worker_epoch: u64,
    pub capacity_total: u64,
    pub capacity_used: u64,
    pub capacity_available: u64,
    pub active_reads: u32,
    pub active_writes: u32,
    pub health: HealthStatus,
    pub last_heartbeat: u64, // Unix timestamp in seconds
    pub fault_domain: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

impl From<i32> for HealthStatus {
    fn from(v: i32) -> Self {
        match v {
            1 => HealthStatus::Healthy,
            2 => HealthStatus::Degraded,
            3 => HealthStatus::Unhealthy,
            _ => HealthStatus::Healthy,
        }
    }
}

/// Block locations: block_id -> [worker_ids...]
pub type BlockLocations = HashMap<BlockId, Vec<WorkerId>>;

/// Worker sync state: tracks whether a worker has completed full sync with this metadata node.
/// This is per-metadata-node state (memory-only, soft-state).
#[derive(Clone, Debug, Default)]
pub struct WorkerSyncState {
    /// Metadata epoch/instance_id (to detect metadata restarts).
    pub metadata_epoch: u64,
    /// Whether full block report has been received and processed.
    pub full_received: bool,
    /// Timestamp of last full report (Unix timestamp in milliseconds).
    pub last_full_ts: u64,
    /// Last sequence number (for INCREMENTAL dedup, optional).
    pub last_seq: u64,
}

/// Block report convergence snapshot for maintenance safety gate.
#[derive(Debug, Clone)]
pub struct BlockReportConvergenceSnapshot {
    pub active_workers: usize,
    pub full_reported_workers: usize,
    pub ratio: f64,
    pub converged: bool,
}

/// Worker manager.
pub struct WorkerManager {
    /// Worker descriptors (authoritative, from Raft state).
    descriptors: Arc<RwLock<HashMap<WorkerId, WorkerDescriptor>>>,
    /// Worker runtime (soft-state, memory-only, updated via fanout heartbeat).
    runtime: Arc<RwLock<HashMap<WorkerId, WorkerRuntime>>>,
    /// Block presence: block_id -> [worker_ids] (soft-state, memory-only).
    locations: Arc<RwLock<BlockLocations>>, // block_id -> [worker_ids]
    /// Worker blocks: worker_id -> [block_ids] (soft-state, memory-only).
    worker_blocks: Arc<RwLock<HashMap<WorkerId, Vec<BlockId>>>>, // worker_id -> [block_ids]
    /// Worker sync state: worker_id -> sync state (per-metadata-node, memory-only).
    worker_sync_state: Arc<RwLock<HashMap<WorkerId, WorkerSyncState>>>,
    /// Current metadata epoch (incremented on metadata restart).
    metadata_epoch: Arc<std::sync::atomic::AtomicU64>,
    /// Heartbeat timeout in seconds.
    heartbeat_timeout_sec: u64,
    /// Maximum concurrent full syncs (for storm control) - deprecated, use lease_manager.
    max_concurrent_full_syncs: usize,
    /// Current number of workers in full sync (for storm control) - deprecated, use lease_manager.
    concurrent_full_syncs: Arc<std::sync::atomic::AtomicUsize>,
    /// Full report lease manager (leader-only, memory-only).
    lease_manager: Arc<super::full_report_lease::FullReportLeaseManager>,
}

impl WorkerManager {
    pub fn new(heartbeat_timeout_sec: u64) -> Self {
        // Generate initial metadata epoch (based on current time in seconds)
        let initial_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Create lease manager (default: 10 concurrent leases, 5 minute TTL)
        const DEFAULT_MAX_CONCURRENT_LEASES: usize = 10;
        const DEFAULT_LEASE_TTL_MS: u64 = 5 * 60 * 1000; // 5 minutes
        let lease_manager = Arc::new(super::full_report_lease::FullReportLeaseManager::new(
            DEFAULT_MAX_CONCURRENT_LEASES,
            DEFAULT_LEASE_TTL_MS,
        ));

        Self {
            descriptors: Arc::new(RwLock::new(HashMap::new())),
            runtime: Arc::new(RwLock::new(HashMap::new())),
            locations: Arc::new(RwLock::new(HashMap::new())),
            worker_blocks: Arc::new(RwLock::new(HashMap::new())),
            worker_sync_state: Arc::new(RwLock::new(HashMap::new())),
            metadata_epoch: Arc::new(std::sync::atomic::AtomicU64::new(initial_epoch)),
            heartbeat_timeout_sec,
            max_concurrent_full_syncs: DEFAULT_MAX_CONCURRENT_LEASES, // Keep for backward compatibility
            concurrent_full_syncs: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            lease_manager,
        }
    }

    /// Get current metadata epoch (for detecting metadata restarts).
    pub fn get_metadata_epoch(&self) -> u64 {
        self.metadata_epoch.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get heartbeat timeout in seconds.
    pub fn heartbeat_timeout_sec(&self) -> u64 {
        self.heartbeat_timeout_sec
    }

    /// Increment metadata epoch (call on metadata restart).
    pub fn increment_metadata_epoch(&self) {
        let new_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.metadata_epoch
            .store(new_epoch, std::sync::atomic::Ordering::Relaxed);

        // Reset all worker sync states (metadata restart invalidates all full syncs)
        let mut sync_state = self.worker_sync_state.write();
        for state in sync_state.values_mut() {
            state.full_received = false;
            state.metadata_epoch = new_epoch;
        }
    }

    /// Get worker sync state (creates default if not exists).
    pub fn get_or_create_sync_state(&self, worker_id: WorkerId) -> WorkerSyncState {
        let mut sync_state = self.worker_sync_state.write();
        let current_epoch = self.get_metadata_epoch();

        sync_state
            .entry(worker_id)
            .and_modify(|state| {
                // If metadata epoch changed, reset full_received
                if state.metadata_epoch != current_epoch {
                    state.full_received = false;
                    state.metadata_epoch = current_epoch;
                }
            })
            .or_insert_with(|| WorkerSyncState {
                metadata_epoch: current_epoch,
                full_received: false,
                last_full_ts: 0,
                last_seq: 0,
            })
            .clone()
    }

    /// Check if worker needs full sync (metadata epoch mismatch or never synced).
    pub fn needs_full_sync(&self, worker_id: WorkerId) -> bool {
        let sync_state = self.worker_sync_state.read();
        let current_epoch = self.get_metadata_epoch();

        sync_state
            .get(&worker_id)
            .map(|state| !state.full_received || state.metadata_epoch != current_epoch)
            .unwrap_or(true) // If no state exists, needs full sync
    }

    /// Try to start full sync (returns true if allowed, false if rate-limited).
    /// DEPRECATED: Use lease_manager.try_allocate() instead.
    /// This method is kept for backward compatibility but delegates to lease_manager.
    pub fn try_start_full_sync(&self, worker_id: WorkerId) -> bool {
        // Check if already in full sync
        let sync_state = self.worker_sync_state.read();
        if let Some(state) = sync_state.get(&worker_id) {
            if state.full_received {
                // Already synced, no need to start
                return true;
            }
        }
        drop(sync_state);

        // Use lease manager (but don't allocate lease here, that's done in heartbeat)
        // This method is only used for legacy code paths
        // Note: This is a synchronous check, but lease_manager is async
        // For legacy compatibility, we just check the counter
        let current = self.concurrent_full_syncs.load(std::sync::atomic::Ordering::Relaxed);
        if current >= self.max_concurrent_full_syncs {
            return false; // Rate limited
        }

        // For legacy compatibility, still update counter
        self.concurrent_full_syncs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    }

    /// Get lease manager.
    pub fn lease_manager(&self) -> Arc<super::full_report_lease::FullReportLeaseManager> {
        Arc::clone(&self.lease_manager)
    }

    /// Mark full sync as completed.
    pub fn mark_full_sync_complete(&self, worker_id: WorkerId) {
        let mut sync_state = self.worker_sync_state.write();
        let current_epoch = self.get_metadata_epoch();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        sync_state
            .entry(worker_id)
            .and_modify(|state| {
                state.full_received = true;
                state.last_full_ts = now_ms;
                state.metadata_epoch = current_epoch;
            })
            .or_insert_with(|| WorkerSyncState {
                metadata_epoch: current_epoch,
                full_received: true,
                last_full_ts: now_ms,
                last_seq: 0,
            });

        // Decrement concurrent counter
        self.concurrent_full_syncs
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Apply full block report (replaces all blocks for this worker).
    /// Returns (added_blocks, removed_blocks) for repair planning.
    ///
    /// NOTE: Lease verification and release should be done before calling this method.
    /// This method only updates locations and marks sync complete.
    pub fn apply_full_report(
        &self,
        worker_id: WorkerId,
        reported_blocks: Vec<BlockId>,
    ) -> MetadataResult<(Vec<BlockId>, Vec<BlockId>)> {
        // Update locations (full replacement)
        let result = self.update_locations(worker_id, reported_blocks)?;

        // Mark full sync as complete
        self.mark_full_sync_complete(worker_id);

        // Note: Lease is already released in verify_and_release() before calling this method.

        Ok(result)
    }

    /// Apply incremental block report (delta operations).
    /// Returns (added_blocks, removed_blocks) for repair planning.
    pub fn apply_delta_report(
        &self,
        worker_id: WorkerId,
        added_blocks: Vec<BlockId>,
        removed_blocks: Vec<BlockId>,
    ) -> MetadataResult<(Vec<BlockId>, Vec<BlockId>)> {
        // Check if full sync is required first
        if self.needs_full_sync(worker_id) {
            return Err(MetadataError::InvalidArgument(
                "Full sync required before incremental reports".to_string(),
            ));
        }

        // Get current blocks
        let worker_blocks = self.worker_blocks.read();
        let mut current_blocks: std::collections::HashSet<BlockId> = worker_blocks
            .get(&worker_id)
            .map(|blocks| blocks.iter().copied().collect())
            .unwrap_or_default();
        drop(worker_blocks);

        // Apply delta operations
        for block_id in &added_blocks {
            current_blocks.insert(*block_id);
        }
        for block_id in &removed_blocks {
            current_blocks.remove(block_id);
        }

        // Update locations with new block set
        let new_blocks: Vec<BlockId> = current_blocks.into_iter().collect();
        self.update_locations(worker_id, new_blocks)
    }

    /// Upsert worker descriptor (called from Raft apply).
    pub fn upsert_descriptor(&self, descriptor: WorkerDescriptor) -> MetadataResult<()> {
        let mut descriptors = self.descriptors.write();
        descriptors.insert(descriptor.worker_id, descriptor);
        Ok(())
    }

    /// Get worker descriptor.
    pub fn get_descriptor(&self, worker_id: WorkerId) -> Option<WorkerDescriptor> {
        let descriptors = self.descriptors.read();
        descriptors.get(&worker_id).cloned()
    }

    /// Register or update a worker descriptor in runtime soft state after Raft apply succeeds.
    pub fn register_worker(
        &self,
        worker_id: WorkerId,
        address: String,
        net_transport_kind: i32,
        worker_epoch: u64,
        fault_domain: Option<String>,
    ) -> MetadataResult<()> {
        let descriptor = WorkerDescriptor {
            worker_id,
            address,
            net_transport_kind,
            worker_epoch,
            fault_domain,
        };
        self.upsert_descriptor(descriptor)
    }

    /// Update worker runtime (fanout heartbeat, memory-only, no Raft).
    /// Returns true if descriptor fields changed (requires re-register).
    // Heartbeat fields mirror the worker report wire payload; grouping would obscure drift checks.
    #[allow(clippy::too_many_arguments)]
    pub fn update_runtime(
        &self,
        worker_id: WorkerId,
        net_transport_kind: i32,
        worker_epoch: u64,
        capacity_total: u64,
        capacity_used: u64,
        capacity_available: u64,
        active_reads: u32,
        active_writes: u32,
        health: HealthStatus,
    ) -> MetadataResult<bool> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let mut runtime = self.runtime.write();
        let descriptors = self.descriptors.read();

        // Check if descriptor exists
        if !descriptors.contains_key(&worker_id) {
            return Err(MetadataError::NotFound(format!(
                "Worker descriptor not found: {:?}",
                worker_id
            )));
        }

        // Check if descriptor fields changed
        let descriptor_changed = if let Some(desc) = descriptors.get(&worker_id) {
            desc.net_transport_kind != net_transport_kind || desc.worker_epoch != worker_epoch
        } else {
            false
        };

        // Update runtime (always, even if descriptor changed)
        let worker_runtime = WorkerRuntime {
            last_seen_ms: now_ms,
            capacity_total,
            capacity_used,
            capacity_available,
            active_reads,
            active_writes,
            health,
        };
        runtime.insert(worker_id, worker_runtime);

        Ok(descriptor_changed)
    }

    /// Get worker info (combined descriptor + runtime, for backward compatibility).
    pub fn get_worker(&self, worker_id: WorkerId) -> Option<WorkerInfo> {
        let descriptors = self.descriptors.read();
        let runtime = self.runtime.read();

        let descriptor = descriptors.get(&worker_id)?;
        let runtime_data = runtime.get(&worker_id)?;

        Some(WorkerInfo {
            worker_id: descriptor.worker_id,
            address: descriptor.address.clone(),
            net_transport_kind: descriptor.net_transport_kind,
            worker_epoch: descriptor.worker_epoch,
            capacity_total: runtime_data.capacity_total,
            capacity_used: runtime_data.capacity_used,
            capacity_available: runtime_data.capacity_available,
            active_reads: runtime_data.active_reads,
            active_writes: runtime_data.active_writes,
            health: runtime_data.health,
            last_heartbeat: runtime_data.last_seen_ms / 1000, // Convert ms to seconds
            fault_domain: descriptor.fault_domain.clone(),
        })
    }

    /// List all live workers (based on runtime last_seen_ms).
    pub fn list_live_workers(&self) -> Vec<WorkerId> {
        let runtime = self.runtime.read();
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let timeout_ms = self.heartbeat_timeout_sec * 1000;

        runtime
            .iter()
            .filter(|(_, r)| now_ms.saturating_sub(r.last_seen_ms) < timeout_ms)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Check if worker is live (based on runtime last_seen_ms).
    pub fn is_worker_live(&self, worker_id: WorkerId) -> bool {
        let runtime = self.runtime.read();
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let timeout_ms = self.heartbeat_timeout_sec * 1000;

        runtime
            .get(&worker_id)
            .map(|r| now_ms.saturating_sub(r.last_seen_ms) < timeout_ms)
            .unwrap_or(false)
    }

    /// List all workers (for background cleanup).
    pub fn list_all_workers(&self) -> Vec<WorkerId> {
        let descriptors = self.descriptors.read();
        descriptors.keys().copied().collect()
    }

    /// Get total number of block locations (for metrics).
    pub fn get_all_locations_count(&self) -> usize {
        let locations = self.locations.read();
        locations.len()
    }

    /// Get all reported blocks (for scanning).
    pub fn get_all_reported_blocks(&self) -> Vec<BlockId> {
        let locations = self.locations.read();
        locations.keys().copied().collect()
    }

    /// Update block locations from block report (full replacement + diff).
    /// This must be called ONCE per block_report with the complete reported_blocks set.
    /// Returns (added_blocks, removed_blocks) for repair planning.
    pub fn update_locations(
        &self,
        worker_id: WorkerId,
        reported_blocks: Vec<BlockId>,
    ) -> MetadataResult<(Vec<BlockId>, Vec<BlockId>)> {
        use std::collections::HashSet;

        // Get current blocks for this worker (before update)
        let mut worker_blocks = self.worker_blocks.write();
        let old_blocks = worker_blocks.get(&worker_id).cloned().unwrap_or_default();

        // Convert to HashSet for O(1) lookup (performance optimization)
        let old_blocks_set: HashSet<BlockId> = old_blocks.iter().copied().collect();
        let reported_blocks_set: HashSet<BlockId> = reported_blocks.iter().copied().collect();

        // Full replacement: update worker -> blocks mapping
        worker_blocks.insert(worker_id, reported_blocks.clone());

        // Update block -> workers mapping
        let mut locations = self.locations.write();

        // Remove worker from blocks that are no longer reported (O(n) with HashSet)
        let removed_blocks: Vec<BlockId> = old_blocks
            .iter()
            .filter(|b| !reported_blocks_set.contains(b))
            .copied()
            .collect();

        for block_id in &removed_blocks {
            if let Some(workers) = locations.get_mut(block_id) {
                workers.retain(|&w| w != worker_id);
                if workers.is_empty() {
                    locations.remove(block_id);
                }
            }
        }

        // Add worker to newly reported blocks (O(n) with HashSet)
        let added_blocks: Vec<BlockId> = reported_blocks
            .iter()
            .filter(|b| !old_blocks_set.contains(b))
            .copied()
            .collect();

        for block_id in &reported_blocks {
            let workers = locations.entry(*block_id).or_default();
            if !workers.contains(&worker_id) {
                workers.push(worker_id);
            }
        }

        Ok((added_blocks, removed_blocks))
    }

    /// Get block locations (only live workers).
    pub fn get_block_locations(&self, block_id: BlockId) -> Vec<WorkerId> {
        let locations = self.locations.read();
        let live_workers = self.list_live_workers();
        let live_set: std::collections::HashSet<WorkerId> = live_workers.into_iter().collect();

        locations
            .get(&block_id)
            .map(|workers| workers.iter().filter(|w| live_set.contains(w)).copied().collect())
            .unwrap_or_default()
    }

    /// Remove dead worker and clean up locations.
    /// Note: descriptor is kept (from Raft state), only runtime and presence are cleaned.
    pub fn remove_dead_worker(&self, worker_id: WorkerId) -> Vec<BlockId> {
        // Remove runtime (soft-state)
        let mut runtime = self.runtime.write();
        runtime.remove(&worker_id);

        // Remove worker blocks and locations
        let mut worker_blocks = self.worker_blocks.write();
        let blocks = worker_blocks.remove(&worker_id).unwrap_or_default();

        // Remove worker from locations
        let mut locations = self.locations.write();
        for block_id in &blocks {
            if let Some(workers) = locations.get_mut(block_id) {
                workers.retain(|&w| w != worker_id);
                if workers.is_empty() {
                    locations.remove(block_id);
                }
            }
        }

        blocks
    }

    /// Get all blocks for a worker.
    pub fn get_worker_blocks(&self, worker_id: WorkerId) -> Vec<BlockId> {
        let worker_blocks = self.worker_blocks.read();
        worker_blocks.get(&worker_id).cloned().unwrap_or_default()
    }

    /// Select workers for block placement.
    ///
    /// Returns primary and replicas based on:
    /// - Available capacity
    /// - Load (active reads/writes)
    /// - Fault domain distribution (if available)
    /// - Health status
    pub fn select_workers_for_placement(
        &self,
        replication_factor: u8,
        preferred_fault_domain: Option<String>,
    ) -> MetadataResult<BlockPlacement> {
        // Get live workers
        let live_workers = self.list_live_workers();

        if live_workers.is_empty() {
            return Err(MetadataError::ServiceUnavailable(
                "No live workers available".to_string(),
            ));
        }

        // Collect worker info with comprehensive scoring
        let mut candidates: Vec<(WorkerId, WorkerInfo, PlacementScore)> = live_workers
            .iter()
            .filter_map(|&id| {
                self.get_worker(id).map(|w| {
                    let score = self.calculate_placement_score(&w, &preferred_fault_domain);
                    (id, w, score)
                })
            })
            .collect();

        // Sort by score (descending)
        candidates.sort_by(|a, b| b.2.cmp(&a.2));

        let needed = replication_factor as usize;
        let available_count = candidates.len();
        if available_count < needed {
            warn!(
                available = available_count,
                needed = needed,
                "Not enough workers for replication factor"
            );
        }

        // Select workers with fault domain distribution
        let selected = self.select_with_fault_domain_distribution(&candidates, needed.min(available_count));

        if selected.is_empty() {
            return Err(MetadataError::ServiceUnavailable(
                "No suitable workers found".to_string(),
            ));
        }

        let primary = selected[0];
        let replicas = selected[1..].to_vec();

        Ok(BlockPlacement { primary, replicas })
    }

    /// Calculate placement score for a worker.
    fn calculate_placement_score(
        &self,
        worker: &WorkerInfo,
        preferred_fault_domain: &Option<String>,
    ) -> PlacementScore {
        // Base score from available capacity (normalized to 0-1000)
        let capacity_score = if worker.capacity_total > 0 {
            (worker.capacity_available * 1000 / worker.capacity_total.max(1)) as i64
        } else {
            0
        };

        // Load penalty: subtract points for high load
        let load_penalty = {
            let total_load = worker.active_reads + worker.active_writes;
            // Penalty: -10 points per active operation (capped at -500)
            (-(total_load as i64 * 10)).max(-500)
        };

        // Health bonus/penalty
        let health_score = match worker.health {
            HealthStatus::Healthy => 100,
            HealthStatus::Degraded => 50,
            HealthStatus::Unhealthy => -500,
        };

        // Fault domain bonus: prefer workers in preferred domain, but also distribute
        let fault_domain_bonus =
            if let (Some(ref preferred), Some(ref worker_domain)) = (preferred_fault_domain, &worker.fault_domain) {
                if preferred == worker_domain {
                    50 // Small bonus for preferred domain
                } else {
                    0
                }
            } else {
                0
            };

        let total_score = capacity_score + load_penalty + health_score + fault_domain_bonus;

        PlacementScore {
            total: total_score,
            _capacity: capacity_score,
            _load_penalty: load_penalty,
            _health: health_score,
            _fault_domain_bonus: fault_domain_bonus,
            fault_domain: worker.fault_domain.clone(),
        }
    }

    /// Select workers with fault domain distribution.
    fn select_with_fault_domain_distribution(
        &self,
        candidates: &[(WorkerId, WorkerInfo, PlacementScore)],
        count: usize,
    ) -> Vec<WorkerId> {
        use std::collections::HashSet;

        let mut selected = Vec::new();
        let mut used_fault_domains = HashSet::new();

        // First pass: try to select one worker from each fault domain
        for (worker_id, _, score) in candidates.iter() {
            if selected.len() >= count {
                break;
            }

            // Get fault domain for this worker
            let fault_domain = score.fault_domain.as_deref().unwrap_or("default");

            // If we haven't used this fault domain yet, or we need more workers
            if !used_fault_domains.contains(fault_domain) || selected.len() < count {
                selected.push(*worker_id);
                used_fault_domains.insert(fault_domain.to_string());
            }
        }

        // Second pass: fill remaining slots with best available workers
        if selected.len() < count {
            for (worker_id, _, _) in candidates.iter() {
                if selected.len() >= count {
                    break;
                }
                if !selected.contains(worker_id) {
                    selected.push(*worker_id);
                }
            }
        }

        selected
    }

    /// Get statistics.
    pub fn stats(&self) -> WorkerManagerStats {
        let descriptors = self.descriptors.read();
        let runtime = self.runtime.read();
        let locations = self.locations.read();

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let timeout_ms = self.heartbeat_timeout_sec * 1000;

        let live_count = runtime
            .values()
            .filter(|r| now_ms.saturating_sub(r.last_seen_ms) < timeout_ms)
            .count();

        WorkerManagerStats {
            total_workers: descriptors.len(),
            live_workers: live_count,
            total_blocks: locations.len(),
            total_locations: locations.values().map(|v| v.len()).sum(),
        }
    }

    /// Clean up stale runtime entries (TTL-based cleanup).
    pub fn cleanup_stale_runtime(&self) {
        let mut runtime = self.runtime.write();
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let timeout_ms = self.heartbeat_timeout_sec * 1000 * 2; // 2x timeout for cleanup

        runtime.retain(|_, r| now_ms.saturating_sub(r.last_seen_ms) < timeout_ms);
    }

    /// Get block report convergence snapshot for maintenance safety gate.
    ///
    /// Returns a snapshot of block report convergence status:
    /// - active_workers: number of workers that have sent heartbeat within active_ttl_ms
    /// - full_reported_workers: number of active workers that have completed full sync for required_epoch
    /// - ratio: full_reported_workers / active_workers (1.0 if active_workers == 0)
    /// - converged: true if ratio >= threshold
    pub fn blockreport_convergence_snapshot(
        &self,
        now_ms: u64,
        active_ttl_ms: u64,
        required_epoch: u64,
        threshold: f64,
    ) -> BlockReportConvergenceSnapshot {
        let runtime = self.runtime.read();
        let sync_state = self.worker_sync_state.read();

        // Count active workers (last_seen_ms within active_ttl_ms)
        let active_workers: Vec<WorkerId> = runtime
            .iter()
            .filter(|(_, r)| now_ms.saturating_sub(r.last_seen_ms) < active_ttl_ms)
            .map(|(id, _)| *id)
            .collect();

        let active_count = active_workers.len();

        // Count full reported workers (active + full_received + epoch match)
        let full_reported_count = active_workers
            .iter()
            .filter(|worker_id| {
                sync_state
                    .get(worker_id)
                    .map(|state| state.full_received && state.metadata_epoch == required_epoch)
                    .unwrap_or(false)
            })
            .count();

        // Calculate ratio (1.0 if no active workers to avoid division by zero)
        let ratio = if active_count == 0 {
            1.0
        } else {
            full_reported_count as f64 / active_count as f64
        };

        let converged = ratio >= threshold;

        BlockReportConvergenceSnapshot {
            active_workers: active_count,
            full_reported_workers: full_reported_count,
            ratio,
            converged,
        }
    }

    /// Check if block report is converged (convenience method with default parameters).
    pub fn is_blockreport_converged(&self, now_ms: u64) -> BlockReportConvergenceSnapshot {
        const DEFAULT_THRESHOLD: f64 = 0.80;

        let active_ttl_ms = self.heartbeat_timeout_sec * 1000;
        let required_epoch = self.get_metadata_epoch();

        self.blockreport_convergence_snapshot(now_ms, active_ttl_ms, required_epoch, DEFAULT_THRESHOLD)
    }
}

#[cfg(test)]
impl WorkerManager {
    /// Test-only helper to override last_seen_ms for deterministic timeout checks.
    pub(crate) fn set_last_seen_ms_for_test(&self, worker_id: WorkerId, last_seen_ms: u64) {
        let mut runtime = self.runtime.write();
        if let Some(worker) = runtime.get_mut(&worker_id) {
            worker.last_seen_ms = last_seen_ms;
        }
    }
}

/// Placement score for worker selection.
#[derive(Clone, Debug)]
struct PlacementScore {
    total: i64,
    _capacity: i64,
    _load_penalty: i64,
    _health: i64,
    _fault_domain_bonus: i64,
    fault_domain: Option<String>,
}

impl PartialEq for PlacementScore {
    fn eq(&self, other: &Self) -> bool {
        self.total == other.total
    }
}

impl Eq for PlacementScore {}

impl PartialOrd for PlacementScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PlacementScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.total.cmp(&other.total)
    }
}

#[derive(Debug)]
pub struct WorkerManagerStats {
    pub total_workers: usize,
    pub live_workers: usize,
    pub total_blocks: usize,
    pub total_locations: usize,
}
