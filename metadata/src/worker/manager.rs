// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker manager: tracks worker registration, heartbeat liveness, and block report locations.

use crate::error::{MetadataError, MetadataResult};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;
use types::block::BlockPlacement;
use types::ids::{BlockId, ShardGroupId, WorkerId};
use types::WorkerRunId;

/// Worker descriptor (low-frequency, authoritative, persisted in Raft).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerDescriptor {
    pub group_id: ShardGroupId,
    pub worker_id: WorkerId,
    pub address: String,
    /// Worker network protocol (0=unspecified/grpc, 1=grpc, 2=quic, 3=rdma).
    pub worker_net_protocol: i32,
    /// Existing data-plane freshness field, separate from startup registration.
    pub worker_epoch: u64,
    pub fault_domain: Option<String>,
}

/// Worker runtime (high-frequency, soft-state, memory-only with TTL).
#[derive(Clone, Debug)]
pub struct WorkerRuntime {
    pub worker_run_id: WorkerRunId,
    pub heartbeat_seq: u64,
    pub last_seen_at: Instant,
    pub last_seen_ms: u64, // Unix timestamp in milliseconds
    pub capacity_total: u64,
    pub capacity_used: u64,
    pub capacity_available: u64,
    pub active_reads: u32,
    pub active_writes: u32,
    pub health: HealthStatus,
}

/// Worker information persisted by RocksDB storage.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub group_id: ShardGroupId,
    pub worker_id: WorkerId,
    pub address: String,
    /// Worker network protocol (0=unspecified/grpc, 1=grpc, 2=quic, 3=rdma).
    pub worker_net_protocol: i32,
    /// Existing data-plane freshness field, separate from startup registration.
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

/// Block locations keyed by metadata group and block identity.
pub type BlockLocations = HashMap<BlockLocationKey, Vec<WorkerRegistrationKey>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BlockLocationKey {
    pub group_id: ShardGroupId,
    pub block_id: BlockId,
}

impl BlockLocationKey {
    pub const fn new(group_id: ShardGroupId, block_id: BlockId) -> Self {
        Self { group_id, block_id }
    }
}

/// Group-scoped key for worker registration and liveness state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WorkerRegistrationKey {
    pub group_id: ShardGroupId,
    pub worker_id: WorkerId,
}

impl WorkerRegistrationKey {
    pub const fn new(group_id: ShardGroupId, worker_id: WorkerId) -> Self {
        Self { group_id, worker_id }
    }
}

fn ready_block_ids<'a>(blocks: impl Iterator<Item = &'a BlockReportBlock>) -> HashSet<BlockId> {
    blocks
        .filter(|block| block.block_state == BlockReportBlockState::Ready)
        .map(|block| block.block_id)
        .collect()
}

/// Live startup registration state for the current metadata process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRegistrationState {
    pub group_id: ShardGroupId,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub address: String,
    pub worker_net_protocol: i32,
    pub fault_domain: Option<String>,
    pub registered_at_ms: u64,
    pub lease_deadline: Instant,
}

/// Worker liveness view updated only by group-scoped heartbeat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerLiveState {
    pub group_id: ShardGroupId,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub heartbeat_seq: u64,
    pub last_seen_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockReportBlockState {
    Ready,
    Partial,
    Corrupt,
    Deleting,
}

/// Worker-reported block-location entry.
///
/// The entry is block-level only. Chunk presence and range routing are not part
/// of this report view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockReportBlock {
    pub block_id: BlockId,
    pub data_handle_id: u64,
    pub block_index: u32,
    pub block_stamp: u64,
    pub effective_len: u64,
    pub committed_length: u64,
    pub block_state: BlockReportBlockState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockReportDeltaOp {
    AddUpdate,
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockReportDeltaEntry {
    pub op: BlockReportDeltaOp,
    pub block: BlockReportBlock,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockReportApplyResult {
    pub added_blocks: Vec<BlockId>,
    pub removed_blocks: Vec<BlockId>,
    pub next_delta_seq: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum BlockReportState {
    #[default]
    Empty,
    Receiving,
    Ready,
}

#[derive(Clone, Debug, Default)]
struct WorkerBlockReportRuntime {
    /// WorkerRunId is live-only. A new worker run must publish a new full
    /// baseline before delta reports are accepted.
    worker_run_id: Option<WorkerRunId>,
    state: BlockReportState,
    /// Monotonic within one worker run and one group.
    report_seq: u64,
    next_batch_seq: u64,
    staging_blocks: HashMap<BlockId, BlockReportBlock>,
    published_blocks: HashMap<BlockId, BlockReportBlock>,
    /// Next delta sequence expected for the current published full baseline.
    delta_seq: u64,
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
    descriptors: Arc<RwLock<HashMap<WorkerRegistrationKey, WorkerDescriptor>>>,
    /// Accepted worker process runs for this metadata process, learned through Raft apply.
    registrations: Arc<RwLock<HashMap<WorkerRegistrationKey, WorkerRegistrationState>>>,
    /// Worker runtime (soft-state, memory-only, updated via fanout heartbeat).
    runtime: Arc<RwLock<HashMap<WorkerRegistrationKey, WorkerRuntime>>>,
    /// Block presence keyed by (group_id, block_id), memory-only.
    locations: Arc<RwLock<BlockLocations>>,
    /// Worker blocks: (group_id, worker_id) -> [block_ids] (soft-state, memory-only).
    worker_blocks: Arc<RwLock<HashMap<WorkerRegistrationKey, Vec<BlockId>>>>,
    /// Full/delta block report runtime keyed by (group_id, worker_id).
    block_reports: Arc<RwLock<HashMap<WorkerRegistrationKey, WorkerBlockReportRuntime>>>,
    /// Current metadata epoch (incremented on metadata restart).
    metadata_epoch: Arc<std::sync::atomic::AtomicU64>,
    /// Heartbeat timeout in seconds.
    heartbeat_timeout_sec: u64,
}

impl WorkerManager {
    pub fn new(heartbeat_timeout_sec: u64) -> Self {
        // Generate initial metadata epoch (based on current time in seconds)
        let initial_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            descriptors: Arc::new(RwLock::new(HashMap::new())),
            registrations: Arc::new(RwLock::new(HashMap::new())),
            runtime: Arc::new(RwLock::new(HashMap::new())),
            locations: Arc::new(RwLock::new(HashMap::new())),
            worker_blocks: Arc::new(RwLock::new(HashMap::new())),
            block_reports: Arc::new(RwLock::new(HashMap::new())),
            metadata_epoch: Arc::new(std::sync::atomic::AtomicU64::new(initial_epoch)),
            heartbeat_timeout_sec,
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

    fn heartbeat_timeout(&self) -> Duration {
        Duration::from_secs(self.heartbeat_timeout_sec)
    }

    /// Increment metadata epoch (call on metadata restart).
    pub fn increment_metadata_epoch(&self) {
        let new_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.metadata_epoch
            .store(new_epoch, std::sync::atomic::Ordering::Relaxed);

        // Metadata restart drops live registration and reconstructable report state.
        self.registrations.write().clear();
        self.runtime.write().clear();
        self.clear_all_block_reports();
    }

    /// Upsert worker descriptor (called from Raft apply).
    pub fn upsert_descriptor(&self, descriptor: WorkerDescriptor) -> MetadataResult<()> {
        let mut descriptors = self.descriptors.write();
        descriptors.insert(
            WorkerRegistrationKey::new(descriptor.group_id, descriptor.worker_id),
            descriptor,
        );
        Ok(())
    }

    /// Load persisted descriptors from replicated storage.
    ///
    /// WorkerRunId is intentionally not reconstructed here. Startup
    /// registration state is live-only, so reload/snapshot recovery fails closed
    /// until the worker registers again through Raft apply.
    pub fn load_registered_workers(&self, workers: Vec<WorkerInfo>) -> MetadataResult<()> {
        let mut descriptors = self.descriptors.write();
        let mut registrations = self.registrations.write();
        let mut runtime = self.runtime.write();
        let mut locations = self.locations.write();
        let mut worker_blocks = self.worker_blocks.write();
        let mut block_reports = self.block_reports.write();
        descriptors.clear();
        registrations.clear();
        runtime.clear();
        locations.clear();
        worker_blocks.clear();
        block_reports.clear();
        for worker in workers {
            let descriptor = WorkerDescriptor {
                group_id: worker.group_id,
                worker_id: worker.worker_id,
                address: worker.address,
                worker_net_protocol: worker.worker_net_protocol,
                worker_epoch: worker.worker_epoch,
                fault_domain: worker.fault_domain,
            };
            descriptors.insert(
                WorkerRegistrationKey::new(descriptor.group_id, descriptor.worker_id),
                descriptor,
            );
        }
        Ok(())
    }

    /// Get a worker descriptor scoped to one metadata group.
    pub fn get_descriptor(&self, group_id: ShardGroupId, worker_id: WorkerId) -> Option<WorkerDescriptor> {
        let descriptors = self.descriptors.read();
        descriptors
            .get(&WorkerRegistrationKey::new(group_id, worker_id))
            .cloned()
    }

    /// Get live startup registration state scoped to one metadata group.
    pub fn get_registration(&self, group_id: ShardGroupId, worker_id: WorkerId) -> Option<WorkerRegistrationState> {
        let registrations = self.registrations.read();
        registrations
            .get(&WorkerRegistrationKey::new(group_id, worker_id))
            .cloned()
    }

    /// Validate same-run idempotence and same-live-worker replacement conflicts.
    pub fn validate_worker_run_registration(
        &self,
        group_id: ShardGroupId,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
    ) -> MetadataResult<()> {
        self.expire_liveness();
        let registrations = self.registrations.read();
        let key = WorkerRegistrationKey::new(group_id, worker_id);
        if let Some(existing) = registrations.get(&key) {
            if existing.worker_run_id != worker_run_id {
                return Err(MetadataError::AlreadyExists(format!(
                    "worker_id {} in group_id {} is already registered with worker_run_id {}",
                    worker_id.as_raw(),
                    group_id.as_raw(),
                    existing.worker_run_id
                )));
            }
        }
        Ok(())
    }

    /// Register or update a worker descriptor in runtime soft state after Raft apply succeeds.
    pub fn register_worker(
        &self,
        group_id: ShardGroupId,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        worker_epoch: u64,
        fault_domain: Option<String>,
    ) -> MetadataResult<()> {
        let descriptor = WorkerDescriptor {
            group_id,
            worker_id,
            address,
            worker_net_protocol,
            worker_epoch,
            fault_domain,
        };
        self.upsert_descriptor(descriptor)
    }

    /// Register or update live startup-registration state after Raft apply succeeds.
    pub fn register_worker_run(
        &self,
        group_id: ShardGroupId,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        worker_run_id: WorkerRunId,
        fault_domain: Option<String>,
    ) -> MetadataResult<()> {
        self.validate_worker_run_registration(group_id, worker_id, worker_run_id)?;
        let registered_at_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let lease_deadline = Instant::now() + self.heartbeat_timeout();
        let descriptor_address = address.clone();
        let descriptor_fault_domain = fault_domain.clone();
        let descriptor = WorkerDescriptor {
            group_id,
            worker_id,
            address: descriptor_address,
            worker_net_protocol,
            worker_epoch: 0,
            fault_domain: descriptor_fault_domain,
        };
        self.upsert_descriptor(descriptor)?;

        let mut registrations = self.registrations.write();
        registrations.insert(
            WorkerRegistrationKey::new(group_id, worker_id),
            WorkerRegistrationState {
                group_id,
                worker_id,
                worker_run_id,
                address,
                worker_net_protocol,
                fault_domain,
                registered_at_ms,
                lease_deadline,
            },
        );
        drop(registrations);
        self.clear_block_report_for_worker(WorkerRegistrationKey::new(group_id, worker_id));
        Ok(())
    }

    /// Receive one full-report batch.
    ///
    /// `batch_seq == 0` starts a staged report for `report_seq`. Staged blocks
    /// are not visible until `final_batch` publishes the full baseline.
    #[allow(clippy::too_many_arguments)]
    pub fn receive_full_block_report(
        &self,
        group_id: ShardGroupId,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        report_seq: u64,
        batch_seq: u64,
        final_batch: bool,
        blocks: Vec<BlockReportBlock>,
    ) -> MetadataResult<BlockReportApplyResult> {
        self.validate_report_source(group_id, worker_id, worker_run_id)?;
        let key = WorkerRegistrationKey::new(group_id, worker_id);

        let mut reports = self.block_reports.write();
        let report = reports.entry(key).or_default();
        if batch_seq == 0 {
            if report.worker_run_id == Some(worker_run_id) && report.report_seq > report_seq {
                return Err(MetadataError::FullReportRequired(format!(
                    "full report required: stale report_seq {} for group_id={}, worker_id={}, current {}",
                    report_seq,
                    group_id.as_raw(),
                    worker_id.as_raw(),
                    report.report_seq
                )));
            }
            report.worker_run_id = Some(worker_run_id);
            report.state = BlockReportState::Receiving;
            report.report_seq = report_seq;
            report.next_batch_seq = 0;
            report.staging_blocks.clear();
        }

        if report.state != BlockReportState::Receiving
            || report.worker_run_id != Some(worker_run_id)
            || report.report_seq != report_seq
            || report.next_batch_seq != batch_seq
        {
            return Err(MetadataError::FullReportRequired(format!(
                "full report required: expected batch_seq {} for group_id={}, worker_id={}",
                report.next_batch_seq,
                group_id.as_raw(),
                worker_id.as_raw()
            )));
        }

        for block in blocks {
            report.staging_blocks.insert(block.block_id, block);
        }
        report.next_batch_seq = batch_seq.saturating_add(1);

        if !final_batch {
            return Ok(BlockReportApplyResult {
                next_delta_seq: report.delta_seq,
                ..BlockReportApplyResult::default()
            });
        }

        let old_ready = ready_block_ids(report.published_blocks.values());
        let published_blocks = std::mem::take(&mut report.staging_blocks);
        let new_ready = ready_block_ids(published_blocks.values());
        report.published_blocks = published_blocks;
        report.state = BlockReportState::Ready;
        report.delta_seq = 0;
        let next_delta_seq = report.delta_seq;
        let published_for_index = report.published_blocks.clone();
        drop(reports);

        self.rebuild_location_index_for_worker(key, &published_for_index);
        Ok(BlockReportApplyResult {
            added_blocks: new_ready.difference(&old_ready).copied().collect(),
            removed_blocks: old_ready.difference(&new_ready).copied().collect(),
            next_delta_seq,
        })
    }

    /// Apply one ordered delta-report batch to the current published baseline.
    pub fn apply_delta_block_report(
        &self,
        group_id: ShardGroupId,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        report_seq: u64,
        delta_seq: u64,
        deltas: Vec<BlockReportDeltaEntry>,
    ) -> MetadataResult<BlockReportApplyResult> {
        self.validate_report_source(group_id, worker_id, worker_run_id)?;
        let key = WorkerRegistrationKey::new(group_id, worker_id);

        let mut reports = self.block_reports.write();
        let report = reports.get_mut(&key).ok_or_else(|| {
            MetadataError::FullReportRequired(format!(
                "full report required before delta for group_id={}, worker_id={}",
                group_id.as_raw(),
                worker_id.as_raw()
            ))
        })?;
        if report.state != BlockReportState::Ready
            || report.worker_run_id != Some(worker_run_id)
            || report.report_seq != report_seq
        {
            return Err(MetadataError::FullReportRequired(format!(
                "full report required for current baseline: group_id={}, worker_id={}",
                group_id.as_raw(),
                worker_id.as_raw()
            )));
        }

        let delta_count = u64::try_from(deltas.len()).unwrap_or(u64::MAX);
        if delta_seq < report.delta_seq {
            let old_delta_end = delta_seq.saturating_add(delta_count);
            if old_delta_end <= report.delta_seq {
                return Ok(BlockReportApplyResult {
                    next_delta_seq: report.delta_seq,
                    ..BlockReportApplyResult::default()
                });
            }
            return Err(MetadataError::FullReportRequired(format!(
                "full report required after overlapping old delta: expected delta_seq {}, got {}",
                report.delta_seq, delta_seq
            )));
        }
        if delta_seq > report.delta_seq {
            return Err(MetadataError::FullReportRequired(format!(
                "full report required after delta gap: expected delta_seq {}, got {}",
                report.delta_seq, delta_seq
            )));
        }

        let old_ready = ready_block_ids(report.published_blocks.values());
        for delta in deltas {
            match delta.op {
                BlockReportDeltaOp::AddUpdate => {
                    report.published_blocks.insert(delta.block.block_id, delta.block);
                }
                BlockReportDeltaOp::Remove => {
                    report.published_blocks.remove(&delta.block.block_id);
                }
            }
        }
        report.delta_seq = report.delta_seq.saturating_add(delta_count);
        let new_ready = ready_block_ids(report.published_blocks.values());
        let next_delta_seq = report.delta_seq;
        let published_for_index = report.published_blocks.clone();
        drop(reports);

        self.rebuild_location_index_for_worker(key, &published_for_index);
        Ok(BlockReportApplyResult {
            added_blocks: new_ready.difference(&old_ready).copied().collect(),
            removed_blocks: old_ready.difference(&new_ready).copied().collect(),
            next_delta_seq,
        })
    }

    /// True when the worker has no published full-report baseline in memory.
    pub fn needs_full_block_report(&self, group_id: ShardGroupId, worker_id: WorkerId) -> bool {
        self.block_reports
            .read()
            .get(&WorkerRegistrationKey::new(group_id, worker_id))
            .map(|report| report.state != BlockReportState::Ready)
            .unwrap_or(true)
    }

    fn validate_report_source(
        &self,
        group_id: ShardGroupId,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
    ) -> MetadataResult<()> {
        self.expire_liveness();
        let registration = self.get_registration(group_id, worker_id).ok_or_else(|| {
            MetadataError::NotFound(format!(
                "worker not registered for group_id={}, worker_id={}",
                group_id.as_raw(),
                worker_id.as_raw()
            ))
        })?;
        if registration.worker_run_id != worker_run_id {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_id={}, worker_id={}",
                group_id.as_raw(),
                worker_id.as_raw()
            )));
        }
        if !self.is_worker_live(group_id, worker_id) {
            return Err(MetadataError::NotFound(format!(
                "worker heartbeat readiness lease not found for group_id={}, worker_id={}",
                group_id.as_raw(),
                worker_id.as_raw()
            )));
        }
        Ok(())
    }

    fn rebuild_location_index_for_worker(
        &self,
        key: WorkerRegistrationKey,
        published_blocks: &HashMap<BlockId, BlockReportBlock>,
    ) {
        {
            let mut worker_blocks = self.worker_blocks.write();
            worker_blocks.insert(
                key,
                published_blocks
                    .values()
                    .filter(|block| block.block_state == BlockReportBlockState::Ready)
                    .map(|block| block.block_id)
                    .collect(),
            );
        }

        let mut locations = self.locations.write();
        for workers in locations.values_mut() {
            workers.retain(|worker_key| *worker_key != key);
        }
        locations.retain(|_, workers| !workers.is_empty());
        for block in published_blocks
            .values()
            .filter(|block| block.block_state == BlockReportBlockState::Ready)
        {
            let workers = locations
                .entry(BlockLocationKey::new(key.group_id, block.block_id))
                .or_default();
            if !workers.contains(&key) {
                workers.push(key);
            }
        }
    }

    fn clear_block_report_for_worker(&self, key: WorkerRegistrationKey) {
        self.block_reports.write().remove(&key);
        self.worker_blocks.write().remove(&key);
        let mut locations = self.locations.write();
        for workers in locations.values_mut() {
            workers.retain(|worker_key| *worker_key != key);
        }
        locations.retain(|_, workers| !workers.is_empty());
    }

    fn clear_all_block_reports(&self) {
        self.block_reports.write().clear();
        self.worker_blocks.write().clear();
        self.locations.write().clear();
    }

    /// Record a validated group-scoped heartbeat in volatile live state.
    ///
    /// Stale sequence numbers renew the local liveness lease but do not replace
    /// the last accepted resource snapshot.
    #[allow(clippy::too_many_arguments)]
    pub fn record_heartbeat(
        &self,
        group_id: ShardGroupId,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        heartbeat_seq: u64,
        advertised_endpoint: &str,
        worker_net_protocol: i32,
        capacity_total: u64,
        capacity_used: u64,
        capacity_available: u64,
        active_reads: u32,
        active_writes: u32,
        health: HealthStatus,
    ) -> MetadataResult<WorkerLiveState> {
        self.expire_liveness();
        let key = WorkerRegistrationKey::new(group_id, worker_id);
        let descriptor = {
            let descriptors = self.descriptors.read();
            descriptors.get(&key).cloned().ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "worker descriptor not found for group_id={}, worker_id={}",
                    group_id.as_raw(),
                    worker_id.as_raw()
                ))
            })?
        };
        let registration = {
            let registrations = self.registrations.read();
            registrations.get(&key).cloned().ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "live worker registration not found for group_id={}, worker_id={}",
                    group_id.as_raw(),
                    worker_id.as_raw()
                ))
            })?
        };

        if registration.worker_run_id != worker_run_id {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_id={}, worker_id={}",
                group_id.as_raw(),
                worker_id.as_raw()
            )));
        }
        if descriptor.address != advertised_endpoint || descriptor.worker_net_protocol != worker_net_protocol {
            return Err(MetadataError::InvalidArgument(format!(
                "worker descriptor mismatch for group_id={}, worker_id={}",
                group_id.as_raw(),
                worker_id.as_raw()
            )));
        }

        let now = Instant::now();
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let mut runtime = self.runtime.write();
        let live_state = match runtime.get_mut(&key) {
            Some(existing) if heartbeat_seq <= existing.heartbeat_seq => {
                existing.last_seen_at = now;
                existing.last_seen_ms = now_ms;
                existing.worker_run_id = worker_run_id;
                WorkerLiveState {
                    group_id,
                    worker_id,
                    worker_run_id,
                    heartbeat_seq: existing.heartbeat_seq,
                    last_seen_ms: existing.last_seen_ms,
                }
            }
            existing => {
                let worker_runtime = WorkerRuntime {
                    worker_run_id,
                    heartbeat_seq,
                    last_seen_at: now,
                    last_seen_ms: now_ms,
                    capacity_total,
                    capacity_used,
                    capacity_available,
                    active_reads,
                    active_writes,
                    health,
                };
                match existing {
                    Some(slot) => *slot = worker_runtime,
                    None => {
                        runtime.insert(key, worker_runtime);
                    }
                }
                WorkerLiveState {
                    group_id,
                    worker_id,
                    worker_run_id,
                    heartbeat_seq,
                    last_seen_ms: now_ms,
                }
            }
        };
        drop(runtime);

        if let Some(registration) = self.registrations.write().get_mut(&key) {
            registration.lease_deadline = now + self.heartbeat_timeout();
        }

        Ok(live_state)
    }

    /// Get worker info by combining persisted descriptor and current runtime state.
    pub fn get_worker(&self, group_id: ShardGroupId, worker_id: WorkerId) -> Option<WorkerInfo> {
        let descriptors = self.descriptors.read();
        let runtime = self.runtime.read();
        let key = WorkerRegistrationKey::new(group_id, worker_id);

        let descriptor = descriptors.get(&key)?;
        let runtime_data = runtime.get(&key)?;

        Some(WorkerInfo {
            group_id: descriptor.group_id,
            worker_id: descriptor.worker_id,
            address: descriptor.address.clone(),
            worker_net_protocol: descriptor.worker_net_protocol,
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

    /// List all live workers (based on runtime last_seen_ms), preserving group identity.
    pub fn list_live_workers(&self) -> Vec<WorkerRegistrationKey> {
        let runtime = self.runtime.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();

        runtime
            .iter()
            .filter(|(_, r)| now.duration_since(r.last_seen_at) < timeout)
            .map(|(key, _)| *key)
            .collect()
    }

    /// List live workers scoped to one metadata group.
    pub fn list_live_workers_in_group(&self, group_id: ShardGroupId) -> Vec<WorkerId> {
        let runtime = self.runtime.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();

        runtime
            .iter()
            .filter(|(key, r)| key.group_id == group_id && now.duration_since(r.last_seen_at) < timeout)
            .map(|(key, _)| key.worker_id)
            .collect()
    }

    /// Check if worker is live (based on runtime last_seen_ms).
    pub fn is_worker_live(&self, group_id: ShardGroupId, worker_id: WorkerId) -> bool {
        let runtime = self.runtime.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();
        let key = WorkerRegistrationKey::new(group_id, worker_id);

        runtime
            .get(&key)
            .map(|r| now.duration_since(r.last_seen_at) < timeout)
            .unwrap_or(false)
    }

    /// List all workers for background scans, preserving group identity.
    pub fn list_all_workers(&self) -> Vec<WorkerRegistrationKey> {
        let descriptors = self.descriptors.read();
        descriptors.keys().copied().collect()
    }

    /// Get total number of block locations (for metrics).
    pub fn get_all_locations_count(&self) -> usize {
        let locations = self.locations.read();
        locations.len()
    }

    /// List group-qualified reported blocks for background scans.
    pub fn list_reported_blocks(&self) -> Vec<BlockLocationKey> {
        let locations = self.locations.read();
        locations.keys().copied().collect()
    }

    /// Get block locations for one metadata group (only live workers in that group).
    pub fn get_block_locations(&self, group_id: ShardGroupId, block_id: BlockId) -> Vec<WorkerId> {
        let locations = self.locations.read();
        let live_workers = self.list_live_workers_in_group(group_id);
        let live_set: std::collections::HashSet<WorkerId> = live_workers.into_iter().collect();

        locations
            .get(&BlockLocationKey::new(group_id, block_id))
            .map(|workers| {
                workers
                    .iter()
                    .filter(|key| key.group_id == group_id && live_set.contains(&key.worker_id))
                    .map(|key| key.worker_id)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Remove dead worker and clean up locations.
    /// Note: descriptor is kept (from Raft state), only runtime and presence are cleaned.
    pub fn remove_dead_worker(&self, group_id: ShardGroupId, worker_id: WorkerId) -> Vec<BlockId> {
        let key = WorkerRegistrationKey::new(group_id, worker_id);

        // Remove runtime (soft-state)
        let mut runtime = self.runtime.write();
        runtime.remove(&key);

        // Remove worker blocks and locations
        let mut worker_blocks = self.worker_blocks.write();
        let blocks = worker_blocks.remove(&key).unwrap_or_default();

        // Remove worker from locations
        let mut locations = self.locations.write();
        for block_id in &blocks {
            let location_key = BlockLocationKey::new(group_id, *block_id);
            if let Some(workers) = locations.get_mut(&location_key) {
                workers.retain(|&w| w != key);
                if workers.is_empty() {
                    locations.remove(&location_key);
                }
            }
        }

        blocks
    }

    /// Get all blocks for a worker.
    pub fn get_worker_blocks(&self, group_id: ShardGroupId, worker_id: WorkerId) -> Vec<BlockId> {
        let worker_blocks = self.worker_blocks.read();
        worker_blocks
            .get(&WorkerRegistrationKey::new(group_id, worker_id))
            .cloned()
            .unwrap_or_default()
    }

    /// Select workers for block placement within one metadata group.
    pub fn select_workers_for_placement_in_group(
        &self,
        group_id: ShardGroupId,
        replication_factor: u8,
        preferred_fault_domain: Option<String>,
    ) -> MetadataResult<BlockPlacement> {
        let live_workers = self.list_live_workers_in_group(group_id);

        if live_workers.is_empty() {
            return Err(MetadataError::ServiceUnavailable(format!(
                "No live workers available for group_id={}",
                group_id.as_raw()
            )));
        }

        let mut candidates: Vec<(WorkerId, WorkerInfo, PlacementScore)> = live_workers
            .iter()
            .filter_map(|&id| {
                self.get_worker(group_id, id).map(|w| {
                    let score = self.calculate_placement_score(&w, &preferred_fault_domain);
                    (id, w, score)
                })
            })
            .collect();

        candidates.sort_by(|a, b| b.2.cmp(&a.2));

        let needed = replication_factor as usize;
        let available_count = candidates.len();
        if available_count < needed {
            warn!(
                group_id = group_id.as_raw(),
                available = available_count,
                needed,
                "Not enough workers for replication factor"
            );
        }

        let selected = self.select_with_fault_domain_distribution(&candidates, needed.min(available_count));

        if selected.is_empty() {
            return Err(MetadataError::ServiceUnavailable(format!(
                "No suitable workers found for group_id={}",
                group_id.as_raw()
            )));
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

        let now = Instant::now();
        let timeout = self.heartbeat_timeout();

        let live_count = runtime
            .values()
            .filter(|r| now.duration_since(r.last_seen_at) < timeout)
            .count();

        WorkerManagerStats {
            total_workers: descriptors.len(),
            live_workers: live_count,
            total_blocks: locations.len(),
            total_locations: locations.values().map(|v| v.len()).sum(),
        }
    }

    /// Expire heartbeat liveness and live process-run registrations.
    pub fn expire_liveness(&self) -> Vec<(ShardGroupId, WorkerId)> {
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();
        let mut expired = Vec::new();

        {
            let mut runtime = self.runtime.write();
            runtime.retain(|key, runtime| {
                let is_live = now.duration_since(runtime.last_seen_at) < timeout;
                if !is_live {
                    expired.push((key.group_id, key.worker_id));
                }
                is_live
            });
        }

        {
            let mut registrations = self.registrations.write();
            registrations.retain(|key, registration| {
                let is_live = registration.lease_deadline > now;
                if !is_live {
                    let entry = (key.group_id, key.worker_id);
                    if !expired.contains(&entry) {
                        expired.push(entry);
                    }
                }
                is_live
            });
        }

        expired
    }

    /// Get block report convergence snapshot for maintenance safety gate.
    ///
    /// Returns a snapshot of block report convergence status:
    /// - active_workers: number of workers that have sent heartbeat within active_ttl_ms
    /// - full_reported_workers: number of active workers with a published report baseline
    /// - ratio: full_reported_workers / active_workers (1.0 if active_workers == 0)
    /// - converged: true if ratio >= threshold
    pub fn blockreport_convergence_snapshot(
        &self,
        now_ms: u64,
        active_ttl_ms: u64,
        _required_epoch: u64,
        threshold: f64,
    ) -> BlockReportConvergenceSnapshot {
        let runtime = self.runtime.read();
        let reports = self.block_reports.read();

        // Count active workers (last_seen_ms within active_ttl_ms)
        let active_workers: Vec<WorkerRegistrationKey> = runtime
            .iter()
            .filter(|(_, r)| now_ms.saturating_sub(r.last_seen_ms) < active_ttl_ms)
            .map(|(key, _)| *key)
            .collect();

        let active_count = active_workers.len();

        // Count full reported workers against the in-memory report baseline.
        let full_reported_count = active_workers
            .iter()
            .filter(|key| {
                reports
                    .get(key)
                    .map(|report| {
                        report.state == BlockReportState::Ready
                            && report.worker_run_id == Some(runtime.get(key).expect("active runtime").worker_run_id)
                    })
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
