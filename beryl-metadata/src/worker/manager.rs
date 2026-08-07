// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker manager: tracks worker registration, heartbeat liveness, and block report locations.

use crate::error::{MetadataError, MetadataResult};
use crate::placement::{ReportedBlockLocation, WorkerPlacementView};
use beryl_types::ids::{BlockId, WorkerId};
use beryl_types::layout::BlockFormatId;
use beryl_types::{GroupName, TierFree, WorkerNetProtocol, WorkerRunId, WriteTarget};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

pub(super) const WORKER_NET_PROTOCOL_GRPC: i32 = 1;

pub(super) fn worker_net_protocol_label(worker_net_protocol: i32) -> &'static str {
    if worker_net_protocol == WORKER_NET_PROTOCOL_GRPC {
        "grpc"
    } else {
        "unknown"
    }
}

/// Worker descriptor (low-frequency, authoritative, persisted in Raft).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerDescriptor {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub address: String,
    /// Worker network protocol wire value. Current runtime accepts gRPC only.
    pub worker_net_protocol: i32,
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
    pub tier_free: Vec<TierFree>,
    pub active_reads: u32,
    pub active_writes: u32,
    pub health: HealthStatus,
}

/// Worker information persisted by RocksDB storage.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub address: String,
    /// Worker network protocol wire value. Current runtime accepts gRPC only.
    pub worker_net_protocol: i32,
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlockLocationKey {
    pub group_name: GroupName,
    pub block_id: BlockId,
}

impl BlockLocationKey {
    pub fn new(group_name: &GroupName, block_id: BlockId) -> Self {
        Self {
            group_name: group_name.clone(),
            block_id,
        }
    }
}

/// Group-scoped key for worker registration and liveness state.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerRegistrationKey {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
}

impl WorkerRegistrationKey {
    pub fn new(group_name: &GroupName, worker_id: WorkerId) -> Self {
        Self {
            group_name: group_name.clone(),
            worker_id,
        }
    }
}

/// Exact identity of one ready physical replica reported by a worker run.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReplicaKey {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub block_id: BlockId,
    pub block_stamp: u64,
}

/// Stable exclusive position after one worker or one Ready block.
///
/// `block_id = None` means this worker is fully consumed and the next page
/// starts from its successor. `Some(block_id)` resumes strictly after that
/// block and preserves the inclusive block high watermark captured when this
/// worker was first entered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReadyReplicaCursor {
    /// Last worker visited in group-scoped worker-key order.
    pub worker_id: WorkerId,
    /// Last Ready block visited for that worker, if one was emitted.
    pub block_id: Option<BlockId>,
    /// Inclusive block upper bound captured on first entry into this worker.
    pub worker_end_block_id: Option<BlockId>,
}

/// One bounded page from the current published Ready reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadyReplicaPage {
    /// Exact current-run Ready replicas encountered in this page.
    pub replicas: Vec<ReplicaKey>,
    /// Exclusive continuation position, or `None` when the traversal reached EOF.
    pub next_cursor: Option<ReadyReplicaCursor>,
}

/// Live startup registration state for the current metadata process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRegistrationState {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub address: String,
    pub worker_net_protocol: i32,
    pub fault_domain: Option<String>,
}

/// Worker liveness view updated only by group-scoped heartbeat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerLiveState {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub heartbeat_seq: u64,
    pub last_seen_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeartbeatRejectionReason {
    NeedRegister,
    WorkerRunMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HeartbeatRejectionState {
    worker_run_id: WorkerRunId,
    reason: HeartbeatRejectionReason,
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
    pub block_stamp: u64,
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
    pub baseline_established: bool,
    pub baseline_replaced: bool,
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
    full_report_had_baseline: bool,
    staging_blocks: HashMap<BlockId, BlockReportBlock>,
    published_blocks: HashMap<BlockId, BlockReportBlock>,
    /// Ordered Ready block identities used for bounded cleanup pagination.
    ready_blocks: BTreeSet<BlockId>,
    /// Next delta sequence expected for the current published full baseline.
    delta_seq: u64,
}

/// Result of checking whether one publication batch has readable worker evidence.
///
/// `Pending` is reserved for observations that may still converge without
/// replacing the active write session. Deterministic identity or local block
/// state conflicts are returned separately so publication can fail closed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublishReadyStatus {
    Ready,
    Pending { block_id: BlockId },
    Conflict(PublishReadyConflict),
}

/// Deterministic worker evidence that cannot authorize file publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublishReadyConflict {
    MissingWriteEndpoint {
        block_id: BlockId,
    },
    WorkerRunMismatch {
        block_id: BlockId,
        worker_id: WorkerId,
        expected: WorkerRunId,
        current: Option<WorkerRunId>,
    },
    EndpointMismatch {
        block_id: BlockId,
        worker_id: WorkerId,
    },
    BlockStampMismatch {
        block_id: BlockId,
        worker_id: WorkerId,
        expected: u64,
        reported: u64,
    },
    UnreadableBlock {
        block_id: BlockId,
        worker_id: WorkerId,
        state: BlockReportBlockState,
    },
}

/// Block report convergence snapshot for maintenance safety gate.
#[derive(Debug, Clone)]
pub struct BlockReportConvergenceSnapshot {
    pub active_workers: usize,
    pub full_reported_workers: usize,
    pub ratio: f64,
    pub converged: bool,
}

#[derive(Debug)]
pub struct WorkerManagerStats {
    pub total_workers: usize,
    pub live_workers: usize,
    pub total_blocks: usize,
    pub total_locations: usize,
}

fn ready_block_ids<'a>(blocks: impl Iterator<Item = &'a BlockReportBlock>) -> HashSet<BlockId> {
    blocks
        .filter(|block| block.block_state == BlockReportBlockState::Ready)
        .map(|block| block.block_id)
        .collect()
}

fn validate_same_run_descriptor(
    group_name: &GroupName,
    worker_id: WorkerId,
    existing: &WorkerRegistrationState,
    address: &str,
    worker_net_protocol: i32,
) -> MetadataResult<()> {
    if existing.address == address && existing.worker_net_protocol == worker_net_protocol {
        return Ok(());
    }
    Err(MetadataError::InvalidArgument(format!(
        "worker descriptor mismatch for group_name={}, worker_id={}, worker_run_id={}: registered endpoint {} protocol {}, requested endpoint {} protocol {}",
        group_name,
        worker_id.as_raw(),
        existing.worker_run_id,
        existing.address,
        worker_net_protocol_label(existing.worker_net_protocol),
        address,
        worker_net_protocol_label(worker_net_protocol)
    )))
}

fn endpoint_host(endpoint: &str) -> Option<String> {
    let without_scheme = endpoint.rsplit_once("://").map(|(_, rest)| rest).unwrap_or(endpoint);
    let host = without_scheme
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(without_scheme)
        .trim_matches(['[', ']'])
        .trim();
    (!host.is_empty()).then(|| host.to_string())
}

/// Worker manager.
pub struct WorkerManager {
    /// Worker descriptors (authoritative, from Raft state).
    descriptors: Arc<RwLock<HashMap<WorkerRegistrationKey, WorkerDescriptor>>>,
    /// Accepted worker process runs for this metadata process, learned through Raft apply.
    registrations: Arc<RwLock<HashMap<WorkerRegistrationKey, WorkerRegistrationState>>>,
    /// Worker runtime (soft-state, memory-only, updated via fanout heartbeat).
    runtime: Arc<RwLock<HashMap<WorkerRegistrationKey, WorkerRuntime>>>,
    /// Last heartbeat rejection state per worker, used only to suppress repeated unchanged warn logs.
    heartbeat_rejections: Arc<RwLock<HashMap<WorkerRegistrationKey, HeartbeatRejectionState>>>,
    /// Block presence keyed by (group_name, block_id), memory-only.
    locations: Arc<RwLock<BlockLocations>>,
    /// Full/delta report runtime in stable group/worker pagination order.
    block_reports: Arc<RwLock<BTreeMap<WorkerRegistrationKey, WorkerBlockReportRuntime>>>,
    /// Coalesced revision for publication-relevant worker observations.
    ///
    /// Ready evidence is leader-local and reconstructable. The revision only
    /// wakes waiters so they can rebuild and revalidate a complete snapshot.
    publication_observation: watch::Sender<u64>,
    /// Heartbeat timeout shared by RPC responses and all soft-state checks.
    heartbeat_timeout_ms: u32,
}

impl WorkerManager {
    pub fn new(heartbeat_timeout_ms: u32) -> Self {
        let (publication_observation, _) = watch::channel(0);
        Self {
            descriptors: Arc::new(RwLock::new(HashMap::new())),
            registrations: Arc::new(RwLock::new(HashMap::new())),
            runtime: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_rejections: Arc::new(RwLock::new(HashMap::new())),
            locations: Arc::new(RwLock::new(HashMap::new())),
            block_reports: Arc::new(RwLock::new(BTreeMap::new())),
            publication_observation,
            heartbeat_timeout_ms,
        }
    }

    fn notify_publication_observation_changed(&self) {
        self.publication_observation
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    /// Returns the exact timeout carried by heartbeat responses.
    pub fn heartbeat_timeout_ms(&self) -> u32 {
        self.heartbeat_timeout_ms
    }

    fn heartbeat_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.heartbeat_timeout_ms))
    }

    /// Drops live registration and reconstructable report state on metadata restart.
    pub fn reset_worker_soft_state(&self) {
        let mut registrations = self.registrations.write();
        let mut block_reports = self.block_reports.write();
        registrations.clear();
        block_reports.clear();
        drop(block_reports);
        drop(registrations);
        self.runtime.write().clear();
        self.heartbeat_rejections.write().clear();
        self.locations.write().clear();
        self.notify_publication_observation_changed();
    }

    /// Upsert worker descriptor (called from Raft apply).
    pub fn upsert_descriptor(&self, descriptor: WorkerDescriptor) -> MetadataResult<()> {
        let mut descriptors = self.descriptors.write();
        descriptors.insert(
            WorkerRegistrationKey::new(&descriptor.group_name, descriptor.worker_id),
            descriptor,
        );
        drop(descriptors);
        self.notify_publication_observation_changed();
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
        let mut heartbeat_rejections = self.heartbeat_rejections.write();
        let mut locations = self.locations.write();
        // Keep report state last to match readers that hold runtime or location
        // guards while reading reports, preventing lock-order inversion.
        let mut block_reports = self.block_reports.write();
        descriptors.clear();
        registrations.clear();
        block_reports.clear();
        runtime.clear();
        heartbeat_rejections.clear();
        locations.clear();
        for worker in workers {
            let descriptor = WorkerDescriptor {
                group_name: worker.group_name,
                worker_id: worker.worker_id,
                address: worker.address,
                worker_net_protocol: worker.worker_net_protocol,
                fault_domain: worker.fault_domain,
            };
            descriptors.insert(
                WorkerRegistrationKey::new(&descriptor.group_name, descriptor.worker_id),
                descriptor,
            );
        }
        drop(block_reports);
        drop(locations);
        drop(heartbeat_rejections);
        drop(runtime);
        drop(registrations);
        drop(descriptors);
        self.notify_publication_observation_changed();
        Ok(())
    }

    /// Get a worker descriptor scoped to one metadata group.
    pub fn get_descriptor(&self, group_name: &GroupName, worker_id: WorkerId) -> Option<WorkerDescriptor> {
        let descriptors = self.descriptors.read();
        descriptors
            .get(&WorkerRegistrationKey::new(group_name, worker_id))
            .cloned()
    }

    /// Get live startup registration state scoped to one metadata group.
    pub fn get_registration(&self, group_name: &GroupName, worker_id: WorkerId) -> Option<WorkerRegistrationState> {
        let registrations = self.registrations.read();
        registrations
            .get(&WorkerRegistrationKey::new(group_name, worker_id))
            .cloned()
    }

    /// Runtime preflight rejects a live different-run endpoint conflict before Raft proposal.
    pub fn validate_worker_registration_preflight(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        address: &str,
        worker_net_protocol: i32,
    ) -> MetadataResult<()> {
        self.expire_liveness();
        let key = WorkerRegistrationKey::new(group_name, worker_id);
        let existing = {
            let registrations = self.registrations.read();
            registrations.get(&key).cloned()
        };
        if let Some(existing) = existing {
            let same_run = existing.worker_run_id.matches(worker_run_id);
            let endpoint_changed = existing.address != address || existing.worker_net_protocol != worker_net_protocol;
            if same_run {
                validate_same_run_descriptor(group_name, worker_id, &existing, address, worker_net_protocol)?;
            }
            if !same_run && endpoint_changed && self.is_worker_live(group_name, worker_id) {
                return Err(MetadataError::ActiveWorkerConflict(format!(
                    "worker_id {} in group_name {} is live at {} protocol {} with worker_run_id {}, rejected registration from {} protocol {} with worker_run_id {}",
                    worker_id.as_raw(),
                    group_name,
                    existing.address,
                    worker_net_protocol_label(existing.worker_net_protocol),
                    existing.worker_run_id,
                    address,
                    worker_net_protocol_label(worker_net_protocol),
                    worker_run_id
                )));
            }
        }
        Ok(())
    }

    /// Deterministic apply validation for a registration command already in the Raft log.
    pub fn validate_worker_registration_for_apply(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        address: &str,
        worker_net_protocol: i32,
    ) -> MetadataResult<()> {
        if worker_id.as_raw() == 0 {
            return Err(MetadataError::InvalidArgument(
                "worker_id must be non-zero for registration".to_string(),
            ));
        }
        let key = WorkerRegistrationKey::new(group_name, worker_id);
        if let Some(existing) = self.registrations.read().get(&key) {
            if &existing.group_name != group_name || existing.worker_id != worker_id {
                return Err(MetadataError::Internal(format!(
                    "worker registration key mismatch for group_name={}, worker_id={}",
                    group_name,
                    worker_id.as_raw()
                )));
            }
            if existing.worker_run_id.matches(worker_run_id) {
                validate_same_run_descriptor(group_name, worker_id, existing, address, worker_net_protocol)?;
                return Ok(());
            }
        }
        Ok(())
    }

    /// Register or update a worker descriptor in runtime soft state after Raft apply succeeds.
    pub fn register_worker(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        fault_domain: Option<String>,
    ) -> MetadataResult<()> {
        let descriptor = WorkerDescriptor {
            group_name: group_name.clone(),
            worker_id,
            address,
            worker_net_protocol,
            fault_domain,
        };
        self.upsert_descriptor(descriptor)
    }

    /// Register or update live startup-registration state after Raft apply succeeds.
    pub fn register_worker_run(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        worker_run_id: WorkerRunId,
        fault_domain: Option<String>,
    ) -> MetadataResult<()> {
        self.validate_worker_registration_for_apply(
            group_name,
            worker_id,
            worker_run_id,
            &address,
            worker_net_protocol,
        )?;
        let key = WorkerRegistrationKey::new(group_name, worker_id);
        let descriptor_address = address.clone();
        let descriptor_fault_domain = fault_domain.clone();
        let descriptor = WorkerDescriptor {
            group_name: group_name.clone(),
            worker_id,
            address: descriptor_address,
            worker_net_protocol,
            fault_domain: descriptor_fault_domain,
        };
        self.upsert_descriptor(descriptor)?;

        let mut registrations = self.registrations.write();
        let mut block_reports = self.block_reports.write();
        let same_registered_run = registrations
            .get(&key)
            .map(|registration| registration.worker_run_id.matches(worker_run_id))
            .unwrap_or(false);
        registrations.insert(
            key.clone(),
            WorkerRegistrationState {
                group_name: group_name.clone(),
                worker_id,
                worker_run_id,
                address,
                worker_net_protocol,
                fault_domain,
            },
        );
        if !same_registered_run {
            block_reports.remove(&key);
        }
        drop(block_reports);
        drop(registrations);
        self.heartbeat_rejections.write().remove(&key);
        if !same_registered_run {
            self.runtime.write().remove(&key);
            self.remove_location_index_for_worker(&key);
        }
        self.notify_publication_observation_changed();
        Ok(())
    }

    /// Receive one full-report batch.
    ///
    /// `batch_seq == 0` starts a staged report for `report_seq`. Staged blocks
    /// are not visible until `final_batch` publishes the full baseline.
    #[allow(clippy::too_many_arguments)]
    pub fn receive_full_block_report(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        report_seq: u64,
        batch_seq: u64,
        final_batch: bool,
        blocks: Vec<BlockReportBlock>,
    ) -> MetadataResult<BlockReportApplyResult> {
        self.validate_report_source(group_name, worker_id, worker_run_id)?;
        let key = WorkerRegistrationKey::new(group_name, worker_id);

        let registrations = self.registrations.read();
        if !registrations
            .get(&key)
            .is_some_and(|registration| registration.worker_run_id.matches(worker_run_id))
        {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }
        let mut reports = self.block_reports.write();
        let report = reports.entry(key.clone()).or_default();
        if batch_seq == 0 {
            let full_report_had_baseline = report.state == BlockReportState::Ready;
            if report
                .worker_run_id
                .is_some_and(|report_worker_run_id| report_worker_run_id.matches(worker_run_id))
                && report.report_seq > report_seq
            {
                return Err(MetadataError::FullReportRequired(format!(
                    "full report required: stale report_seq {} for group_name={}, worker_id={}, current {}",
                    report_seq,
                    group_name,
                    worker_id.as_raw(),
                    report.report_seq
                )));
            }
            report.worker_run_id = Some(worker_run_id);
            report.state = BlockReportState::Receiving;
            report.report_seq = report_seq;
            report.next_batch_seq = 0;
            report.full_report_had_baseline = full_report_had_baseline;
            report.staging_blocks.clear();
        }

        if report.state != BlockReportState::Receiving
            || !report
                .worker_run_id
                .is_some_and(|report_worker_run_id| report_worker_run_id.matches(worker_run_id))
            || report.report_seq != report_seq
            || report.next_batch_seq != batch_seq
        {
            return Err(MetadataError::FullReportRequired(format!(
                "full report required: expected batch_seq {} for group_name={}, worker_id={}",
                report.next_batch_seq,
                group_name,
                worker_id.as_raw()
            )));
        }

        for block in blocks {
            report.staging_blocks.insert(block.block_id, block);
        }
        report.next_batch_seq = batch_seq.saturating_add(1);

        if !final_batch {
            let next_delta_seq = report.delta_seq;
            return Ok(BlockReportApplyResult {
                next_delta_seq,
                ..BlockReportApplyResult::default()
            });
        }

        let old_published_blocks = report.published_blocks.clone();
        let old_ready = ready_block_ids(old_published_blocks.values());
        let published_blocks = std::mem::take(&mut report.staging_blocks);
        let baseline_established = !report.full_report_had_baseline;
        let baseline_replaced = report.full_report_had_baseline && old_published_blocks != published_blocks;
        let new_ready = ready_block_ids(published_blocks.values());
        report.published_blocks = published_blocks;
        report.ready_blocks = report
            .published_blocks
            .values()
            .filter(|block| block.block_state == BlockReportBlockState::Ready)
            .map(|block| block.block_id)
            .collect();
        report.state = BlockReportState::Ready;
        report.delta_seq = 0;
        let next_delta_seq = report.delta_seq;
        let published_for_index = report.published_blocks.clone();
        drop(reports);
        drop(registrations);

        self.rebuild_location_index_for_worker(key, &published_for_index);
        self.notify_publication_observation_changed();
        tracing::debug!(
            group_name = %group_name,
            worker_id = worker_id.as_raw(),
            worker_run_id = %worker_run_id,
            report_seq,
            "Worker full block report converged"
        );
        Ok(BlockReportApplyResult {
            added_blocks: new_ready.difference(&old_ready).copied().collect(),
            removed_blocks: old_ready.difference(&new_ready).copied().collect(),
            next_delta_seq,
            baseline_established,
            baseline_replaced,
        })
    }

    /// Apply one ordered delta-report batch to the current published baseline.
    pub fn apply_delta_block_report(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        report_seq: u64,
        delta_seq: u64,
        deltas: Vec<BlockReportDeltaEntry>,
    ) -> MetadataResult<BlockReportApplyResult> {
        self.validate_report_source(group_name, worker_id, worker_run_id)?;
        let key = WorkerRegistrationKey::new(group_name, worker_id);

        let registrations = self.registrations.read();
        if !registrations
            .get(&key)
            .is_some_and(|registration| registration.worker_run_id.matches(worker_run_id))
        {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }
        let mut reports = self.block_reports.write();
        let report = reports.get_mut(&key).ok_or_else(|| {
            MetadataError::FullReportRequired(format!(
                "full report required before delta for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            ))
        })?;
        if report.state != BlockReportState::Ready
            || !report
                .worker_run_id
                .is_some_and(|report_worker_run_id| report_worker_run_id.matches(worker_run_id))
            || report.report_seq != report_seq
        {
            return Err(MetadataError::FullReportRequired(format!(
                "full report required for current baseline: group_name={}, worker_id={}",
                group_name,
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
            let block_id = delta.block.block_id;
            match delta.op {
                BlockReportDeltaOp::AddUpdate => {
                    if delta.block.block_state == BlockReportBlockState::Ready {
                        report.ready_blocks.insert(block_id);
                    } else {
                        report.ready_blocks.remove(&block_id);
                    }
                    report.published_blocks.insert(block_id, delta.block);
                }
                BlockReportDeltaOp::Remove => {
                    report.ready_blocks.remove(&block_id);
                    report.published_blocks.remove(&block_id);
                }
            }
        }
        report.delta_seq = report.delta_seq.saturating_add(delta_count);
        let new_ready = ready_block_ids(report.published_blocks.values());
        let next_delta_seq = report.delta_seq;
        let published_for_index = report.published_blocks.clone();
        drop(reports);
        drop(registrations);

        self.rebuild_location_index_for_worker(key, &published_for_index);
        self.notify_publication_observation_changed();
        Ok(BlockReportApplyResult {
            added_blocks: new_ready.difference(&old_ready).copied().collect(),
            removed_blocks: old_ready.difference(&new_ready).copied().collect(),
            next_delta_seq,
            baseline_established: false,
            baseline_replaced: false,
        })
    }

    /// True when the worker has no published full-report baseline in memory.
    pub fn needs_full_block_report(&self, group_name: &GroupName, worker_id: WorkerId) -> bool {
        self.block_reports
            .read()
            .get(&WorkerRegistrationKey::new(group_name, worker_id))
            .map(|report| report.state != BlockReportState::Ready)
            .unwrap_or(true)
    }

    fn validate_report_source(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
    ) -> MetadataResult<()> {
        self.expire_liveness();
        let registration = self.get_registration(group_name, worker_id).ok_or_else(|| {
            MetadataError::NotFound(format!(
                "worker not registered for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            ))
        })?;
        if !registration.worker_run_id.matches(worker_run_id) {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }
        if !self.is_worker_live(group_name, worker_id) {
            return Err(MetadataError::NotFound(format!(
                "worker heartbeat readiness lease not found for group_name={}, worker_id={}",
                group_name,
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
        let mut locations = self.locations.write();
        for workers in locations.values_mut() {
            workers.retain(|worker_key| worker_key != &key);
        }
        locations.retain(|_, workers| !workers.is_empty());
        for block in published_blocks
            .values()
            .filter(|block| block.block_state == BlockReportBlockState::Ready)
        {
            let workers = locations
                .entry(BlockLocationKey::new(&key.group_name, block.block_id))
                .or_default();
            if !workers.contains(&key) {
                workers.push(key.clone());
            }
        }
    }

    fn remove_location_index_for_worker(&self, key: &WorkerRegistrationKey) {
        let mut locations = self.locations.write();
        for workers in locations.values_mut() {
            workers.retain(|worker_key| worker_key != key);
        }
        locations.retain(|_, workers| !workers.is_empty());
    }

    pub fn mark_heartbeat_need_register_if_changed(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
    ) -> bool {
        self.mark_heartbeat_rejection_if_changed(
            group_name,
            worker_id,
            worker_run_id,
            HeartbeatRejectionReason::NeedRegister,
        )
    }

    pub fn mark_heartbeat_run_mismatch_if_changed(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
    ) -> bool {
        self.mark_heartbeat_rejection_if_changed(
            group_name,
            worker_id,
            worker_run_id,
            HeartbeatRejectionReason::WorkerRunMismatch,
        )
    }

    fn mark_heartbeat_rejection_if_changed(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        reason: HeartbeatRejectionReason,
    ) -> bool {
        let current = HeartbeatRejectionState { worker_run_id, reason };
        let previous = self
            .heartbeat_rejections
            .write()
            .insert(WorkerRegistrationKey::new(group_name, worker_id), current);
        previous != Some(current)
    }

    fn clear_heartbeat_rejection(&self, key: &WorkerRegistrationKey) {
        self.heartbeat_rejections.write().remove(key);
    }

    /// Record a validated group-scoped heartbeat in volatile live state.
    ///
    /// Stale sequence numbers renew the local liveness lease but do not replace
    /// the last accepted resource snapshot.
    #[allow(clippy::too_many_arguments)]
    pub fn record_heartbeat(
        &self,
        group_name: &GroupName,
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
        self.record_heartbeat_with_tier_free(
            group_name,
            worker_id,
            worker_run_id,
            heartbeat_seq,
            advertised_endpoint,
            worker_net_protocol,
            capacity_total,
            capacity_used,
            capacity_available,
            vec![TierFree {
                tier: beryl_types::Tier::Hdd,
                free_bytes: capacity_available,
            }],
            active_reads,
            active_writes,
            health,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_heartbeat_with_tier_free(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        heartbeat_seq: u64,
        advertised_endpoint: &str,
        worker_net_protocol: i32,
        capacity_total: u64,
        capacity_used: u64,
        capacity_available: u64,
        tier_free: Vec<TierFree>,
        active_reads: u32,
        active_writes: u32,
        health: HealthStatus,
    ) -> MetadataResult<WorkerLiveState> {
        self.expire_liveness();
        let key = WorkerRegistrationKey::new(group_name, worker_id);
        let descriptor = {
            let descriptors = self.descriptors.read();
            descriptors.get(&key).cloned().ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "worker descriptor not found for group_name={}, worker_id={}",
                    group_name,
                    worker_id.as_raw()
                ))
            })?
        };
        let registration = {
            let registrations = self.registrations.read();
            registrations.get(&key).cloned().ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "live worker registration not found for group_name={}, worker_id={}",
                    group_name,
                    worker_id.as_raw()
                ))
            })?
        };

        if !registration.worker_run_id.matches(worker_run_id) {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }
        if descriptor.address != advertised_endpoint || descriptor.worker_net_protocol != worker_net_protocol {
            return Err(MetadataError::InvalidArgument(format!(
                "worker descriptor mismatch for group_name={}, worker_id={}",
                group_name,
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
                    group_name: group_name.clone(),
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
                    tier_free,
                    active_reads,
                    active_writes,
                    health,
                };
                match existing {
                    Some(slot) => *slot = worker_runtime,
                    None => {
                        runtime.insert(key.clone(), worker_runtime);
                    }
                }
                WorkerLiveState {
                    group_name: group_name.clone(),
                    worker_id,
                    worker_run_id,
                    heartbeat_seq,
                    last_seen_ms: now_ms,
                }
            }
        };
        drop(runtime);
        self.clear_heartbeat_rejection(&key);
        self.notify_publication_observation_changed();

        Ok(live_state)
    }

    /// Expire heartbeat liveness.
    pub fn expire_liveness(&self) -> Vec<(GroupName, WorkerId)> {
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();
        let mut expired = Vec::new();

        {
            let mut runtime = self.runtime.write();
            runtime.retain(|key, runtime| {
                let is_live = now.duration_since(runtime.last_seen_at) < timeout;
                if !is_live {
                    expired.push((key.group_name.clone(), key.worker_id));
                }
                is_live
            });
        }

        if !expired.is_empty() {
            self.notify_publication_observation_changed();
        }
        expired
    }

    /// Remove dead-worker runtime state and keep the persisted descriptor.
    pub fn remove_dead_worker(&self, group_name: &GroupName, worker_id: WorkerId) -> (bool, Vec<BlockId>) {
        let key = WorkerRegistrationKey::new(group_name, worker_id);
        let mut removed = false;
        let mut affected_blocks = HashSet::new();

        let mut registrations = self.registrations.write();
        let mut block_reports = self.block_reports.write();
        let registration_removed = registrations.remove(&key).is_some();
        let removed_report = block_reports.remove(&key);
        if registration_removed || removed_report.is_some() {
            removed = true;
        }
        drop(block_reports);
        drop(registrations);
        if self.runtime.write().remove(&key).is_some() {
            removed = true;
        }

        if let Some(report) = removed_report {
            affected_blocks.extend(ready_block_ids(report.published_blocks.values()));
        }

        {
            let mut locations = self.locations.write();
            for (location_key, workers) in locations.iter_mut() {
                let before = workers.len();
                workers.retain(|worker_key| worker_key != &key);
                if workers.len() != before {
                    removed = true;
                    affected_blocks.insert(location_key.block_id);
                }
            }
            locations.retain(|_, workers| !workers.is_empty());
        }

        let mut affected_blocks: Vec<_> = affected_blocks.into_iter().collect();
        affected_blocks.sort_by_key(|block_id| (block_id.inode_id.as_raw(), block_id.index.as_raw()));
        if removed {
            self.notify_publication_observation_changed();
        }
        (removed, affected_blocks)
    }

    /// Get worker info by combining persisted descriptor and current runtime state.
    pub fn get_worker(&self, group_name: &GroupName, worker_id: WorkerId) -> Option<WorkerInfo> {
        let descriptors = self.descriptors.read();
        let runtime = self.runtime.read();
        let key = WorkerRegistrationKey::new(group_name, worker_id);

        let descriptor = descriptors.get(&key)?;
        let runtime_data = runtime.get(&key)?;

        Some(WorkerInfo {
            group_name: descriptor.group_name.clone(),
            worker_id: descriptor.worker_id,
            address: descriptor.address.clone(),
            worker_net_protocol: descriptor.worker_net_protocol,
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
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// List current in-memory worker run registrations for runtime scans.
    pub fn list_registered_workers(&self) -> Vec<WorkerRegistrationKey> {
        let registrations = self.registrations.read();
        registrations.keys().cloned().collect()
    }

    /// List live workers scoped to one metadata group.
    pub fn list_live_workers_in_group(&self, group_name: &GroupName) -> Vec<WorkerId> {
        let runtime = self.runtime.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();

        runtime
            .iter()
            .filter(|(key, r)| &key.group_name == group_name && now.duration_since(r.last_seen_at) < timeout)
            .map(|(key, _)| key.worker_id)
            .collect()
    }

    /// Check if worker is live (based on runtime last_seen_ms).
    pub fn is_worker_live(&self, group_name: &GroupName, worker_id: WorkerId) -> bool {
        let runtime = self.runtime.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();
        let key = WorkerRegistrationKey::new(group_name, worker_id);

        runtime
            .get(&key)
            .map(|r| now.duration_since(r.last_seen_at) < timeout)
            .unwrap_or(false)
    }

    /// List persisted worker descriptors. Descriptors are not active runtime state.
    pub fn list_worker_descriptors(&self) -> Vec<WorkerRegistrationKey> {
        let descriptors = self.descriptors.read();
        descriptors.keys().cloned().collect()
    }

    /// Build the placement worker view from group-scoped registration and heartbeat state.
    pub fn collect_worker_placement_views(&self, group_name: &GroupName) -> Vec<WorkerPlacementView> {
        let descriptors = self.descriptors.read();
        let registrations = self.registrations.read();
        let runtime = self.runtime.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();

        let mut views = Vec::new();
        for (key, descriptor) in descriptors.iter().filter(|(key, _)| &key.group_name == group_name) {
            let registration = registrations.get(key);
            let live = runtime.get(key);
            let registered = registration.is_some();
            let lease_valid = registered
                && live
                    .map(|runtime| now.duration_since(runtime.last_seen_at) < timeout)
                    .unwrap_or(false);
            views.push(WorkerPlacementView {
                group_name: key.group_name.clone(),
                worker_id: key.worker_id,
                worker_run_id: registration.map(|registration| registration.worker_run_id),
                endpoint: descriptor.address.clone(),
                worker_net_protocol: descriptor.worker_net_protocol,
                registered,
                lease_valid,
                ip: endpoint_host(&descriptor.address),
                host: endpoint_host(&descriptor.address),
                az: None,
                rack: descriptor.fault_domain.clone(),
                region: None,
                free_bytes: live.map(|runtime| runtime.capacity_available),
                tier_free: live.map(|runtime| runtime.tier_free.clone()).unwrap_or_default(),
                supported_block_formats: vec![BlockFormatId::CURRENT_FOR_NEW_FILE],
            });
        }
        views.sort_by_key(|view| view.worker_id.as_raw());
        views
    }

    /// Get total number of block locations (for metrics).
    pub fn get_all_locations_count(&self) -> usize {
        let locations = self.locations.read();
        locations.len()
    }

    /// List group-qualified reported blocks for background scans.
    pub fn list_reported_blocks(&self) -> Vec<BlockLocationKey> {
        let locations = self.locations.read();
        locations.keys().cloned().collect()
    }

    /// Get block locations for one metadata group (only live workers in that group).
    pub fn get_block_locations(&self, group_name: &GroupName, block_id: BlockId) -> Vec<WorkerId> {
        let locations = self.locations.read();
        let live_workers = self.list_live_workers_in_group(group_name);
        let live_set: std::collections::HashSet<WorkerId> = live_workers.into_iter().collect();

        locations
            .get(&BlockLocationKey::new(group_name, block_id))
            .map(|workers| {
                workers
                    .iter()
                    .filter(|key| &key.group_name == group_name && live_set.contains(&key.worker_id))
                    .map(|key| key.worker_id)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Return ready block-report locations with the report's worker run id.
    pub fn reported_block_locations(&self, group_name: &GroupName, block_id: BlockId) -> Vec<ReportedBlockLocation> {
        let locations = self.locations.read();
        let reports = self.block_reports.read();
        let Some(worker_keys) = locations.get(&BlockLocationKey::new(group_name, block_id)) else {
            return Vec::new();
        };

        let mut reported = Vec::with_capacity(worker_keys.len());
        for key in worker_keys {
            if &key.group_name != group_name {
                continue;
            }
            let Some(report) = reports.get(key) else {
                continue;
            };
            if report.state != BlockReportState::Ready {
                continue;
            }
            let Some(worker_run_id) = report.worker_run_id else {
                continue;
            };
            let Some(block) = report.published_blocks.get(&block_id) else {
                continue;
            };
            if block.block_state != BlockReportBlockState::Ready {
                continue;
            }
            reported.push(ReportedBlockLocation {
                group_name: group_name.clone(),
                block_id,
                block_stamp: block.block_stamp,
                worker_id: key.worker_id,
                worker_run_id,
            });
        }
        reported.sort_by_key(|location| location.worker_id.as_raw());
        reported
    }

    /// Get all blocks for a worker.
    pub fn get_worker_blocks(&self, group_name: &GroupName, worker_id: WorkerId) -> Vec<BlockId> {
        self.block_reports
            .read()
            .get(&WorkerRegistrationKey::new(group_name, worker_id))
            .map(|report| report.ready_blocks.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Captures the inclusive Worker end of the group's current report keyspace.
    ///
    /// Later workers are deferred to the next scan cycle. The paginator also
    /// captures an inclusive block end when it first enters each worker, so
    /// appends cannot indefinitely delay progress to the next worker.
    pub(crate) fn ready_replica_scan_end(&self, group_name: &GroupName) -> Option<WorkerId> {
        let reports = self.block_reports.read();
        let end_key = WorkerRegistrationKey::new(group_name, WorkerId::new(u64::MAX));
        let (worker_key, _) = reports.range(..=end_key).next_back()?;
        if &worker_key.group_name != group_name {
            return None;
        }
        Some(worker_key.worker_id)
    }

    /// Copies one bounded, stably ordered page from published Ready reports.
    ///
    /// Report changes may defer keys inserted at or before the cursor until the
    /// next complete cycle. Registration and report guards are held together so
    /// one page never pairs different Worker runs. The work budget counts each
    /// emitted block and each visited worker that cannot emit a block, keeping
    /// scans bounded even when reports are not Ready.
    pub(crate) fn list_ready_replica_page(
        &self,
        group_name: &GroupName,
        cursor: Option<ReadyReplicaCursor>,
        scan_end_worker_id: WorkerId,
        limit: usize,
    ) -> MetadataResult<ReadyReplicaPage> {
        if limit == 0 {
            return Err(MetadataError::InvalidArgument(
                "ready replica page limit must be greater than zero".to_string(),
            ));
        }

        if cursor.is_some_and(|cursor| {
            cursor.worker_id > scan_end_worker_id
                || (cursor.worker_id == scan_end_worker_id && cursor.block_id.is_none())
        }) {
            return Ok(ReadyReplicaPage {
                replicas: Vec::new(),
                next_cursor: None,
            });
        }
        if cursor.is_some_and(|cursor| cursor.block_id.is_some() && cursor.worker_end_block_id.is_none()) {
            return Err(MetadataError::Internal(
                "ready replica cursor is missing its worker block end".to_string(),
            ));
        }

        let registrations = self.registrations.read();
        let reports = self.block_reports.read();
        let start_worker_id = cursor
            .map(|cursor| cursor.worker_id)
            .unwrap_or_else(|| WorkerId::new(0));
        let start_key = WorkerRegistrationKey::new(group_name, start_worker_id);
        let mut replicas = Vec::new();
        let mut visited = 0;

        for (worker_key, report) in reports.range(start_key..) {
            if &worker_key.group_name != group_name {
                break;
            }
            if worker_key.worker_id > scan_end_worker_id {
                break;
            }
            if cursor.is_some_and(|cursor| cursor.worker_id == worker_key.worker_id && cursor.block_id.is_none()) {
                continue;
            }
            let is_end_worker = worker_key.worker_id == scan_end_worker_id;

            let report_run_id = report.worker_run_id;
            let current_run = registrations
                .get(worker_key)
                .map(|registration| registration.worker_run_id);
            if report.state != BlockReportState::Ready
                || report_run_id.is_none()
                || !current_run
                    .is_some_and(|run_id| report_run_id.is_some_and(|report_run_id| run_id.matches(report_run_id)))
                || report.ready_blocks.is_empty()
            {
                visited += 1;
                if is_end_worker {
                    return Ok(ReadyReplicaPage {
                        replicas,
                        next_cursor: None,
                    });
                }
                let next_cursor = ReadyReplicaCursor {
                    worker_id: worker_key.worker_id,
                    block_id: None,
                    worker_end_block_id: None,
                };
                if visited == limit {
                    return Ok(ReadyReplicaPage {
                        replicas,
                        next_cursor: Some(next_cursor),
                    });
                }
                continue;
            }
            let report_run_id = report_run_id.expect("Ready report run checked above");

            let worker_cursor = cursor.filter(|cursor| cursor.worker_id == worker_key.worker_id);
            let after_block = worker_cursor.and_then(|cursor| cursor.block_id);
            let worker_end_block_id = worker_cursor
                .and_then(|cursor| cursor.worker_end_block_id)
                .or_else(|| report.ready_blocks.last().copied())
                .expect("non-empty Ready report has a last block");
            let lower_bound = after_block.map(Excluded).unwrap_or(Unbounded);
            let replicas_before_worker = replicas.len();
            for block_id in report.ready_blocks.range((lower_bound, Included(worker_end_block_id))) {
                let Some(block) = report.published_blocks.get(block_id) else {
                    return Err(MetadataError::Internal(format!(
                        "Ready block index is missing report state for group_name={}, worker_id={}, block_id={}",
                        group_name,
                        worker_key.worker_id.as_raw(),
                        block_id
                    )));
                };
                if block.block_state != BlockReportBlockState::Ready {
                    return Err(MetadataError::Internal(format!(
                        "Ready block index contains non-Ready report state for group_name={}, worker_id={}, block_id={}",
                        group_name,
                        worker_key.worker_id.as_raw(),
                        block_id
                    )));
                }
                replicas.push(ReplicaKey {
                    group_name: group_name.clone(),
                    worker_id: worker_key.worker_id,
                    worker_run_id: report_run_id,
                    block_id: *block_id,
                    block_stamp: block.block_stamp,
                });
                visited += 1;
                if *block_id == worker_end_block_id {
                    if is_end_worker {
                        return Ok(ReadyReplicaPage {
                            replicas,
                            next_cursor: None,
                        });
                    }
                    if visited == limit {
                        return Ok(ReadyReplicaPage {
                            replicas,
                            next_cursor: Some(ReadyReplicaCursor {
                                worker_id: worker_key.worker_id,
                                block_id: None,
                                worker_end_block_id: None,
                            }),
                        });
                    }
                    break;
                }
                if visited == limit {
                    return Ok(ReadyReplicaPage {
                        replicas,
                        next_cursor: Some(ReadyReplicaCursor {
                            worker_id: worker_key.worker_id,
                            block_id: Some(*block_id),
                            worker_end_block_id: Some(worker_end_block_id),
                        }),
                    });
                }
            }
            if replicas.len() == replicas_before_worker {
                visited += 1;
                if visited == limit && !is_end_worker {
                    return Ok(ReadyReplicaPage {
                        replicas,
                        next_cursor: Some(ReadyReplicaCursor {
                            worker_id: worker_key.worker_id,
                            block_id: None,
                            worker_end_block_id: None,
                        }),
                    });
                }
            }
            if is_end_worker {
                return Ok(ReadyReplicaPage {
                    replicas,
                    next_cursor: None,
                });
            }
        }

        Ok(ReadyReplicaPage {
            replicas,
            next_cursor: None,
        })
    }

    /// Returns whether an exact replica is still Ready in the current worker run.
    ///
    /// Registration and report guards are held together so a replacement run
    /// cannot be paired with the previous run's report.
    pub(crate) fn is_current_ready_replica(&self, replica: &ReplicaKey) -> bool {
        let worker_key = WorkerRegistrationKey::new(&replica.group_name, replica.worker_id);
        let registrations = self.registrations.read();
        let reports = self.block_reports.read();

        let Some(registration) = registrations.get(&worker_key) else {
            return false;
        };
        if !registration.worker_run_id.matches(replica.worker_run_id) {
            return false;
        }

        let Some(report) = reports.get(&worker_key) else {
            return false;
        };
        if report.state != BlockReportState::Ready
            || !report
                .worker_run_id
                .is_some_and(|report_run_id| report_run_id.matches(replica.worker_run_id))
        {
            return false;
        }

        report.published_blocks.get(&replica.block_id).is_some_and(|block| {
            block.block_state == BlockReportBlockState::Ready && block.block_stamp == replica.block_stamp
        })
    }

    /// Subscribe before checking Ready evidence so a concurrent report cannot
    /// be lost between the snapshot check and the asynchronous wait.
    pub(crate) fn subscribe_publication_observations(&self) -> watch::Receiver<u64> {
        self.publication_observation.subscribe()
    }

    /// Check all newly visible write targets against one current worker view.
    ///
    /// This observation never becomes durable authority. Registration,
    /// heartbeat, descriptor, and full-report guards remain held together while
    /// every target is checked, and callers must recheck after every wakeup and
    /// immediately before proposing the visibility-changing Raft command.
    pub(crate) fn check_publish_ready(&self, group_name: &GroupName, targets: &[WriteTarget]) -> PublishReadyStatus {
        let descriptors = self.descriptors.read();
        let registrations = self.registrations.read();
        let runtime = self.runtime.read();
        let reports = self.block_reports.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();

        for target in targets {
            if target.worker_endpoints.is_empty() {
                return PublishReadyStatus::Conflict(PublishReadyConflict::MissingWriteEndpoint {
                    block_id: target.block_id,
                });
            }

            let mut conflict = None;
            let mut ready = false;
            for endpoint in &target.worker_endpoints {
                let key = WorkerRegistrationKey::new(group_name, endpoint.worker_id);
                let Some(registration) = registrations.get(&key) else {
                    conflict = Some(PublishReadyConflict::WorkerRunMismatch {
                        block_id: target.block_id,
                        worker_id: endpoint.worker_id,
                        expected: endpoint.worker_run_id,
                        current: None,
                    });
                    continue;
                };
                if !registration.worker_run_id.matches(endpoint.worker_run_id) {
                    conflict = Some(PublishReadyConflict::WorkerRunMismatch {
                        block_id: target.block_id,
                        worker_id: endpoint.worker_id,
                        expected: endpoint.worker_run_id,
                        current: Some(registration.worker_run_id),
                    });
                    continue;
                }

                let endpoint_matches = descriptors.get(&key).is_some_and(|descriptor| {
                    descriptor.address == endpoint.endpoint
                        && descriptor.worker_net_protocol == WORKER_NET_PROTOCOL_GRPC
                        && endpoint.worker_net_protocol == WorkerNetProtocol::Grpc
                }) && registration.address == endpoint.endpoint
                    && registration.worker_net_protocol == WORKER_NET_PROTOCOL_GRPC;
                if !endpoint_matches {
                    conflict = Some(PublishReadyConflict::EndpointMismatch {
                        block_id: target.block_id,
                        worker_id: endpoint.worker_id,
                    });
                    continue;
                }

                let Some(worker_runtime) = runtime.get(&key) else {
                    continue;
                };
                if !worker_runtime.worker_run_id.matches(endpoint.worker_run_id)
                    || now.duration_since(worker_runtime.last_seen_at) >= timeout
                {
                    continue;
                }

                let Some(report) = reports.get(&key) else {
                    continue;
                };
                if report.state != BlockReportState::Ready
                    || !report
                        .worker_run_id
                        .is_some_and(|report_run_id| report_run_id.matches(endpoint.worker_run_id))
                {
                    continue;
                }
                let Some(block) = report.published_blocks.get(&target.block_id) else {
                    continue;
                };
                if block.block_stamp != target.block_stamp {
                    conflict = Some(PublishReadyConflict::BlockStampMismatch {
                        block_id: target.block_id,
                        worker_id: endpoint.worker_id,
                        expected: target.block_stamp,
                        reported: block.block_stamp,
                    });
                    continue;
                }
                match block.block_state {
                    BlockReportBlockState::Ready => {
                        ready = true;
                        break;
                    }
                    BlockReportBlockState::Partial => {}
                    BlockReportBlockState::Corrupt | BlockReportBlockState::Deleting => {
                        conflict = Some(PublishReadyConflict::UnreadableBlock {
                            block_id: target.block_id,
                            worker_id: endpoint.worker_id,
                            state: block.block_state,
                        });
                    }
                }
            }

            if !ready {
                return conflict.map_or(
                    PublishReadyStatus::Pending {
                        block_id: target.block_id,
                    },
                    PublishReadyStatus::Conflict,
                );
            }
        }

        PublishReadyStatus::Ready
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
        threshold: f64,
    ) -> BlockReportConvergenceSnapshot {
        let runtime = self.runtime.read();
        let reports = self.block_reports.read();

        // Count active workers (last_seen_ms within active_ttl_ms)
        let active_workers: Vec<WorkerRegistrationKey> = runtime
            .iter()
            .filter(|(_, r)| now_ms.saturating_sub(r.last_seen_ms) < active_ttl_ms)
            .map(|(key, _)| key.clone())
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
                            && report.worker_run_id.is_some_and(|report_worker_run_id| {
                                report_worker_run_id.matches(runtime.get(key).expect("active runtime").worker_run_id)
                            })
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

        let active_ttl_ms = u64::from(self.heartbeat_timeout_ms);
        self.blockreport_convergence_snapshot(now_ms, active_ttl_ms, DEFAULT_THRESHOLD)
    }
}

#[cfg(test)]
mod tests {
    //! Tests for worker manager and registration.

    use super::{
        BlockLocationKey, BlockReportBlock, BlockReportBlockState, BlockReportDeltaEntry, BlockReportDeltaOp,
        HealthStatus, PublishReadyConflict, PublishReadyStatus, ReadyReplicaCursor, ReplicaKey, WorkerInfo,
        WorkerManager, WorkerRegistrationKey,
    };
    use crate::error::MetadataError;
    use beryl_types::ids::{BlockId, BlockIndex, InodeId, WorkerId};
    use beryl_types::lease::FencingToken;
    use beryl_types::{
        BlockFormatId, ClientId, GroupName, Tier, WorkerEndpointInfo, WorkerNetProtocol, WorkerRunId, WriteTarget,
    };
    use std::time::{Duration, Instant};

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    fn report_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440100".parse().unwrap()
    }

    fn report_block(index: u32) -> BlockReportBlock {
        let block_id = BlockId::new(InodeId::new(9), BlockIndex::new(index));
        report_block_with_id(block_id)
    }

    fn report_block_with_id(block_id: BlockId) -> BlockReportBlock {
        BlockReportBlock {
            block_id,
            block_stamp: u64::from(block_id.index.as_raw()) + 100,
            block_state: BlockReportBlockState::Ready,
        }
    }

    fn register_live_report_worker(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        run_id: WorkerRunId,
    ) {
        manager
            .register_worker_run(group_name, worker_id, "127.0.0.1:9090".to_string(), 1, run_id, None)
            .unwrap();
        manager
            .record_heartbeat(
                group_name,
                worker_id,
                run_id,
                1,
                "127.0.0.1:9090",
                1,
                1_000,
                100,
                900,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();
    }

    fn ready_scan_end(manager: &WorkerManager, group_name: &GroupName) -> WorkerId {
        manager
            .ready_replica_scan_end(group_name)
            .expect("group should have a report scan end")
    }

    fn publication_target(
        worker_id: WorkerId,
        run_id: WorkerRunId,
        block_id: BlockId,
        block_stamp: u64,
    ) -> WriteTarget {
        WriteTarget {
            block_id,
            file_offset: 0,
            block_size: 64,
            effective_len: 64,
            worker_endpoints: vec![WorkerEndpointInfo {
                worker_id,
                endpoint: "127.0.0.1:9090".to_string(),
                worker_net_protocol: WorkerNetProtocol::Grpc,
                worker_run_id: run_id,
            }],
            fencing_token: FencingToken {
                block_id,
                owner: ClientId::new(7),
                epoch: 1,
            },
            block_stamp,
            chunk_size: 64,
            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            tier: Tier::Hdd,
        }
    }

    #[test]
    fn publication_ready_check_requires_current_live_exact_worker_evidence() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g-publish");
        let worker_id = WorkerId::new(5);
        let run_id = report_run_id();
        let block_id = BlockId::new(InodeId::new(91), BlockIndex::new(0));
        let target = publication_target(worker_id, run_id, block_id, 7);
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

        assert_eq!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Pending { block_id }
        );

        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                1,
                0,
                true,
                vec![BlockReportBlock {
                    block_id,
                    block_stamp: 7,
                    block_state: BlockReportBlockState::Ready,
                }],
            )
            .unwrap();
        assert_eq!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Ready
        );

        manager
            .runtime
            .write()
            .get_mut(&WorkerRegistrationKey::new(&group_name_value, worker_id))
            .unwrap()
            .last_seen_at = Instant::now() - Duration::from_secs(61);
        assert_eq!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Pending { block_id }
        );
    }

    #[test]
    fn publication_ready_check_rejects_run_stamp_endpoint_and_unreadable_conflicts() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g-conflict");
        let worker_id = WorkerId::new(5);
        let run_id = report_run_id();
        let block_id = BlockId::new(InodeId::new(92), BlockIndex::new(0));
        let target = publication_target(worker_id, run_id, block_id, 7);
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                1,
                0,
                true,
                vec![BlockReportBlock {
                    block_id,
                    block_stamp: 8,
                    block_state: BlockReportBlockState::Ready,
                }],
            )
            .unwrap();
        assert!(matches!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Conflict(PublishReadyConflict::BlockStampMismatch { .. })
        ));

        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                2,
                0,
                true,
                vec![BlockReportBlock {
                    block_id,
                    block_stamp: 7,
                    block_state: BlockReportBlockState::Corrupt,
                }],
            )
            .unwrap();
        assert!(matches!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Conflict(PublishReadyConflict::UnreadableBlock { .. })
        ));

        let mut wrong_endpoint = target.clone();
        wrong_endpoint.worker_endpoints[0].endpoint = "127.0.0.1:9191".to_string();
        assert!(matches!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&wrong_endpoint)),
            PublishReadyStatus::Conflict(PublishReadyConflict::EndpointMismatch { .. })
        ));

        let replacement_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440101".parse().unwrap();
        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                replacement_run,
                None,
            )
            .unwrap();
        assert!(matches!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Conflict(PublishReadyConflict::WorkerRunMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn publication_observation_does_not_lose_report_before_wait() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g-watch");
        let worker_id = WorkerId::new(5);
        let run_id = report_run_id();
        let block_id = BlockId::new(InodeId::new(93), BlockIndex::new(0));
        let target = publication_target(worker_id, run_id, block_id, 7);
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);
        let mut observations = manager.subscribe_publication_observations();

        assert_eq!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Pending { block_id }
        );
        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                1,
                0,
                true,
                vec![BlockReportBlock {
                    block_id,
                    block_stamp: 7,
                    block_state: BlockReportBlockState::Ready,
                }],
            )
            .unwrap();

        tokio::time::timeout(Duration::from_millis(100), observations.changed())
            .await
            .expect("observation should wake")
            .expect("sender remains open");
        assert_eq!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Ready
        );
    }

    #[test]
    fn full_report_batches_publish_only_after_final_batch() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g21");
        let worker_id = WorkerId::new(5);
        let run_id = report_run_id();
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, false, vec![report_block(0)])
            .unwrap();

        assert!(manager
            .get_block_locations(&group_name_value, report_block(0).block_id)
            .is_empty());
        assert!(manager.get_worker_blocks(&group_name_value, worker_id).is_empty());

        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 1, true, vec![report_block(1)])
            .unwrap();

        assert_eq!(manager.get_worker_blocks(&group_name_value, worker_id).len(), 2);
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(0).block_id),
            vec![worker_id]
        );
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(1).block_id),
            vec![worker_id]
        );
    }

    #[test]
    fn ready_replica_pages_require_current_run_and_reach_eof_in_stable_order() {
        let manager = WorkerManager::new(60_000);
        let local_group = group_name("g-ready");
        let other_group = group_name("g-other");
        let worker_id = WorkerId::new(15);
        let other_worker_id = WorkerId::new(16);
        let run_id = report_run_id();
        let other_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440116".parse().unwrap();
        register_live_report_worker(&manager, &local_group, worker_id, run_id);
        register_live_report_worker(&manager, &other_group, other_worker_id, other_run_id);
        assert!(matches!(
            manager.list_ready_replica_page(&local_group, None, worker_id, 0,),
            Err(MetadataError::InvalidArgument(_))
        ));

        manager
            .receive_full_block_report(&local_group, worker_id, run_id, 1, 0, false, vec![report_block(0)])
            .unwrap();
        let receiving = manager
            .list_ready_replica_page(&local_group, None, ready_scan_end(&manager, &local_group), 10)
            .unwrap();
        assert!(receiving.replicas.is_empty());
        assert!(receiving.next_cursor.is_none());

        let mut partial = report_block(2);
        partial.block_state = BlockReportBlockState::Partial;
        manager
            .receive_full_block_report(
                &local_group,
                worker_id,
                run_id,
                1,
                1,
                true,
                vec![report_block(1), partial],
            )
            .unwrap();
        manager
            .receive_full_block_report(
                &other_group,
                other_worker_id,
                other_run_id,
                1,
                0,
                true,
                vec![report_block(3)],
            )
            .unwrap();

        let mut cursor = None;
        let mut replicas = Vec::new();
        let scan_end = ready_scan_end(&manager, &local_group);
        loop {
            let page = manager
                .list_ready_replica_page(&local_group, cursor, scan_end, 1)
                .unwrap();
            replicas.extend(page.replicas);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(
            replicas,
            vec![
                ReplicaKey {
                    group_name: local_group.clone(),
                    worker_id,
                    worker_run_id: run_id,
                    block_id: report_block(0).block_id,
                    block_stamp: report_block(0).block_stamp,
                },
                ReplicaKey {
                    group_name: local_group.clone(),
                    worker_id,
                    worker_run_id: run_id,
                    block_id: report_block(1).block_id,
                    block_stamp: report_block(1).block_stamp,
                },
            ]
        );

        let replacement_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440117".parse().unwrap();
        manager
            .register_worker_run(
                &local_group,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                replacement_run,
                None,
            )
            .unwrap();
        assert!(manager.ready_replica_scan_end(&local_group).is_none());
    }

    #[test]
    fn ready_replica_pages_follow_full_and_delta_report_state() {
        let manager = WorkerManager::new(60_000);
        let local_group = group_name("g-ready-report-state");
        let worker_id = WorkerId::new(16);
        let run_id = report_run_id();
        register_live_report_worker(&manager, &local_group, worker_id, run_id);

        manager
            .receive_full_block_report(&local_group, worker_id, run_id, 1, 0, true, vec![report_block(0)])
            .unwrap();
        manager
            .apply_delta_block_report(
                &local_group,
                worker_id,
                run_id,
                1,
                0,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::AddUpdate,
                    block: report_block(1),
                }],
            )
            .unwrap();
        assert_eq!(
            manager
                .list_ready_replica_page(&local_group, None, ready_scan_end(&manager, &local_group), 10)
                .unwrap()
                .replicas
                .len(),
            2
        );

        manager
            .receive_full_block_report(&local_group, worker_id, run_id, 2, 0, false, vec![report_block(2)])
            .unwrap();
        assert_eq!(manager.get_worker_blocks(&local_group, worker_id).len(), 2);
        assert!(manager
            .list_ready_replica_page(&local_group, None, ready_scan_end(&manager, &local_group), 10)
            .unwrap()
            .replicas
            .is_empty());

        manager
            .receive_full_block_report(&local_group, worker_id, run_id, 2, 1, true, vec![report_block(3)])
            .unwrap();
        assert_eq!(
            manager
                .list_ready_replica_page(&local_group, None, ready_scan_end(&manager, &local_group), 10)
                .unwrap()
                .replicas
                .len(),
            2
        );
    }

    #[test]
    fn ready_replica_cursor_advances_past_a_nonready_worker() {
        let manager = WorkerManager::new(60_000);
        let local_group = group_name("g-ready-workers");
        let receiving_worker = WorkerId::new(18);
        let ready_worker = WorkerId::new(19);
        let receiving_run = report_run_id();
        let ready_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440119".parse().unwrap();
        register_live_report_worker(&manager, &local_group, receiving_worker, receiving_run);
        register_live_report_worker(&manager, &local_group, ready_worker, ready_run);
        manager
            .receive_full_block_report(
                &local_group,
                receiving_worker,
                receiving_run,
                1,
                0,
                false,
                vec![report_block(0)],
            )
            .unwrap();
        manager
            .receive_full_block_report(&local_group, ready_worker, ready_run, 1, 0, true, vec![report_block(1)])
            .unwrap();

        let scan_end = ready_scan_end(&manager, &local_group);
        let first = manager
            .list_ready_replica_page(&local_group, None, scan_end, 1)
            .unwrap();
        assert!(first.replicas.is_empty());
        assert_eq!(
            first.next_cursor,
            Some(ReadyReplicaCursor {
                worker_id: receiving_worker,
                block_id: None,
                worker_end_block_id: None,
            })
        );
        let second = manager
            .list_ready_replica_page(&local_group, first.next_cursor, scan_end, 1)
            .unwrap();
        assert_eq!(second.replicas.len(), 1);
        assert_eq!(second.replicas[0].worker_id, ready_worker);
    }

    #[test]
    fn ready_replica_pages_traverse_more_than_ten_thousand_blocks_during_other_worker_full_reports() {
        const BLOCK_COUNT: usize = 10_001;
        const PAGE_SIZE: usize = 1_000;
        let manager = WorkerManager::new(60_000);
        let local_group = group_name("g-ready-large");
        let worker_id = WorkerId::new(17);
        let run_id = report_run_id();
        let churn_worker_id = WorkerId::new(18);
        let churn_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440120".parse().unwrap();
        let churn_block = report_block(20_000);
        register_live_report_worker(&manager, &local_group, worker_id, run_id);
        register_live_report_worker(&manager, &local_group, churn_worker_id, churn_run_id);
        manager
            .receive_full_block_report(
                &local_group,
                worker_id,
                run_id,
                1,
                0,
                true,
                (0..BLOCK_COUNT as u32).map(report_block).collect(),
            )
            .unwrap();
        manager
            .receive_full_block_report(
                &local_group,
                churn_worker_id,
                churn_run_id,
                1,
                0,
                true,
                vec![churn_block.clone()],
            )
            .unwrap();

        let mut cursor = None;
        let mut replicas = Vec::new();
        let mut churn_report_seq = 1;
        let scan_end = ready_scan_end(&manager, &local_group);
        loop {
            let page = manager
                .list_ready_replica_page(&local_group, cursor, scan_end, PAGE_SIZE)
                .unwrap();
            replicas.extend(page.replicas);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
            churn_report_seq += 1;
            manager
                .receive_full_block_report(
                    &local_group,
                    churn_worker_id,
                    churn_run_id,
                    churn_report_seq,
                    0,
                    true,
                    vec![churn_block.clone()],
                )
                .unwrap();
        }

        assert_eq!(replicas.len(), BLOCK_COUNT + 1);
        assert_eq!(replicas.first().unwrap().block_id, report_block(0).block_id);
        assert_eq!(replicas.last().unwrap().block_id, churn_block.block_id);
        assert_eq!(replicas.last().unwrap().worker_id, churn_worker_id);
    }

    #[test]
    fn exact_ready_replica_check_tracks_report_and_run_changes() {
        let manager = WorkerManager::new(60_000);
        let local_group = group_name("g-cleanup-ready");
        let worker_id = WorkerId::new(17);
        let run_id = report_run_id();
        let block = report_block(0);
        let replica = ReplicaKey {
            group_name: local_group.clone(),
            worker_id,
            worker_run_id: run_id,
            block_id: block.block_id,
            block_stamp: block.block_stamp,
        };
        register_live_report_worker(&manager, &local_group, worker_id, run_id);
        manager
            .receive_full_block_report(&local_group, worker_id, run_id, 1, 0, true, vec![block.clone()])
            .unwrap();
        assert!(manager.is_current_ready_replica(&replica));
        let mut wrong_stamp = replica.clone();
        wrong_stamp.block_stamp += 1;
        assert!(!manager.is_current_ready_replica(&wrong_stamp));

        let mut deleting = block.clone();
        deleting.block_state = BlockReportBlockState::Deleting;
        manager
            .apply_delta_block_report(
                &local_group,
                worker_id,
                run_id,
                1,
                0,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::AddUpdate,
                    block: deleting,
                }],
            )
            .unwrap();
        assert!(!manager.is_current_ready_replica(&replica));

        manager
            .apply_delta_block_report(
                &local_group,
                worker_id,
                run_id,
                1,
                1,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::AddUpdate,
                    block: block.clone(),
                }],
            )
            .unwrap();
        assert!(manager.is_current_ready_replica(&replica));

        manager
            .apply_delta_block_report(
                &local_group,
                worker_id,
                run_id,
                1,
                2,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::Remove,
                    block: block.clone(),
                }],
            )
            .unwrap();
        assert!(!manager.is_current_ready_replica(&replica));

        manager
            .receive_full_block_report(&local_group, worker_id, run_id, 2, 0, true, vec![block.clone()])
            .unwrap();
        assert!(manager.is_current_ready_replica(&replica));
        manager
            .receive_full_block_report(&local_group, worker_id, run_id, 3, 0, true, Vec::new())
            .unwrap();
        assert!(!manager.is_current_ready_replica(&replica));

        let replacement_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440118".parse().unwrap();
        manager
            .register_worker_run(
                &local_group,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                replacement_run,
                None,
            )
            .unwrap();
        assert!(!manager.is_current_ready_replica(&replica));
    }

    #[test]
    fn final_full_report_marks_active_worker_converged() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g22");
        let worker_id = WorkerId::new(6);
        let run_id = report_run_id();
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let before = manager.blockreport_convergence_snapshot(now_ms, 60_000, 0.80);
        assert_eq!(before.active_workers, 1);
        assert_eq!(before.full_reported_workers, 0);
        assert!(!before.converged);

        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
            .unwrap();

        let after = manager.blockreport_convergence_snapshot(now_ms, 60_000, 0.80);
        assert_eq!(after.active_workers, 1);
        assert_eq!(after.full_reported_workers, 1);
        assert!(after.converged);
    }

    #[test]
    fn stale_full_report_seq_cannot_roll_back_published_view() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g25");
        let worker_id = WorkerId::new(9);
        let run_id = report_run_id();
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 7, 0, true, vec![report_block(0)])
            .unwrap();
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(0).block_id),
            vec![worker_id]
        );

        let stale = manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 6, 0, true, vec![report_block(1)])
            .expect_err("stale report_seq must not reset the published baseline");
        assert!(stale.to_string().contains("full report required"));
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(0).block_id),
            vec![worker_id]
        );
        assert!(manager
            .get_block_locations(&group_name_value, report_block(1).block_id)
            .is_empty());
    }

    #[test]
    fn full_report_rejects_sequence_run_and_registration_errors() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g22");
        let worker_id = WorkerId::new(6);
        let run_id = report_run_id();

        let missing = manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
            .expect_err("missing registration must fail");
        assert!(missing.to_string().contains("not registered"));

        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);
        let stale_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440101".parse().unwrap();
        let stale = manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                stale_run,
                1,
                0,
                true,
                vec![report_block(0)],
            )
            .expect_err("stale worker_run_id must fail");
        assert!(stale.to_string().contains("worker_run_id mismatch"));

        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 2, 0, false, vec![report_block(0)])
            .unwrap();
        let mismatch = manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 2, 2, true, vec![report_block(1)])
            .expect_err("batch_seq gap must fail");
        assert!(mismatch.to_string().contains("full report required"));
        assert!(manager.get_worker_blocks(&group_name_value, worker_id).is_empty());
    }

    #[test]
    fn delta_report_requires_ready_baseline_and_ordered_sequence() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g23");
        let worker_id = WorkerId::new(7);
        let run_id = report_run_id();
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

        let before_full = manager
            .apply_delta_block_report(
                &group_name_value,
                worker_id,
                run_id,
                1,
                0,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::AddUpdate,
                    block: report_block(0),
                }],
            )
            .expect_err("delta before full report must fail");
        assert!(before_full.to_string().contains("full report required"));

        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 7, 0, true, vec![report_block(0)])
            .unwrap();

        manager
            .apply_delta_block_report(
                &group_name_value,
                worker_id,
                run_id,
                7,
                0,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::AddUpdate,
                    block: report_block(1),
                }],
            )
            .unwrap();
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(1).block_id),
            vec![worker_id]
        );

        manager
            .apply_delta_block_report(
                &group_name_value,
                worker_id,
                run_id,
                7,
                0,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::AddUpdate,
                    block: report_block(1),
                }],
            )
            .unwrap();

        let gap = manager
            .apply_delta_block_report(
                &group_name_value,
                worker_id,
                run_id,
                7,
                3,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::Remove,
                    block: report_block(1),
                }],
            )
            .expect_err("delta gap must require full report");
        assert!(gap.to_string().contains("full report required"));

        let epoch_mismatch = manager
            .apply_delta_block_report(
                &group_name_value,
                worker_id,
                run_id,
                8,
                1,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::Remove,
                    block: report_block(1),
                }],
            )
            .expect_err("report_seq mismatch must require full report");
        assert!(epoch_mismatch.to_string().contains("full report required"));
    }

    #[test]
    fn recreated_report_runtime_requires_full_report_again() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g24");
        let worker_id = WorkerId::new(8);
        let run_id = report_run_id();
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);
        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
            .unwrap();
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(0).block_id),
            vec![worker_id]
        );
        manager
            .load_registered_workers(vec![WorkerInfo {
                group_name: group_name_value.clone(),
                worker_id,
                address: "127.0.0.1:9090".to_string(),
                worker_net_protocol: 1,
                capacity_total: 0,
                capacity_used: 0,
                capacity_available: 0,
                active_reads: 0,
                active_writes: 0,
                health: HealthStatus::Healthy,
                last_heartbeat: 0,
                fault_domain: None,
            }])
            .unwrap();
        assert!(manager
            .get_block_locations(&group_name_value, report_block(0).block_id)
            .is_empty());
        let delta = manager
            .apply_delta_block_report(
                &group_name_value,
                worker_id,
                run_id,
                1,
                0,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::AddUpdate,
                    block: report_block(1),
                }],
            )
            .expect_err("metadata restart must require a new full report");
        assert!(delta.to_string().contains("not registered"));
    }

    #[test]
    fn soft_state_reset_clears_ready_report_authority() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g-reset-report-state");
        let worker_id = WorkerId::new(9);
        let run_id = report_run_id();
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);
        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
            .unwrap();
        let replica = ReplicaKey {
            group_name: group_name_value.clone(),
            worker_id,
            worker_run_id: run_id,
            block_id: report_block(0).block_id,
            block_stamp: report_block(0).block_stamp,
        };
        assert!(manager.is_current_ready_replica(&replica));

        manager.reset_worker_soft_state();

        assert!(manager.ready_replica_scan_end(&group_name_value).is_none());
        assert!(!manager.is_current_ready_replica(&replica));
    }

    #[test]
    fn reported_block_locations_are_group_qualified() {
        let manager = WorkerManager::new(60_000);
        let first_group = group_name("g31");
        let second_group = group_name("g32");
        let first_worker = WorkerId::new(10);
        let second_worker = WorkerId::new(11);
        let first_run = report_run_id();
        let second_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440102".parse().unwrap();
        let block_id = BlockId::new(InodeId::new(41), BlockIndex::new(0));

        register_live_report_worker(&manager, &first_group, first_worker, first_run);
        register_live_report_worker(&manager, &second_group, second_worker, second_run);
        manager
            .receive_full_block_report(
                &first_group,
                first_worker,
                first_run,
                1,
                0,
                true,
                vec![report_block_with_id(block_id)],
            )
            .unwrap();
        manager
            .receive_full_block_report(
                &second_group,
                second_worker,
                second_run,
                1,
                0,
                true,
                vec![report_block_with_id(block_id)],
            )
            .unwrap();

        assert_eq!(manager.get_block_locations(&first_group, block_id), vec![first_worker]);
        assert_eq!(
            manager.get_block_locations(&second_group, block_id),
            vec![second_worker]
        );

        let mut reported = manager.list_reported_blocks();
        reported.sort_by_key(|key| (key.group_name.to_string(), key.block_id.to_string()));
        assert_eq!(
            reported,
            vec![
                BlockLocationKey::new(&first_group, block_id),
                BlockLocationKey::new(&second_group, block_id),
            ]
        );

        manager
            .receive_full_block_report(&first_group, first_worker, first_run, 2, 0, true, Vec::new())
            .unwrap();

        assert!(manager.get_block_locations(&first_group, block_id).is_empty());
        assert_eq!(
            manager.get_block_locations(&second_group, block_id),
            vec![second_worker]
        );
        assert_eq!(
            manager.list_reported_blocks(),
            vec![BlockLocationKey::new(&second_group, block_id)]
        );
    }

    #[test]
    fn worker_descriptor_runtime_and_liveness_are_group_scoped() {
        let manager = WorkerManager::new(60_000);
        let worker_id = WorkerId::new(7);
        let first_group = group_name("g11");
        let second_group = group_name("g12");
        let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440050".parse().unwrap();
        let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440051".parse().unwrap();

        manager
            .register_worker_run(
                &first_group,
                worker_id,
                "127.0.0.1:9107".to_string(),
                1,
                first_run_id,
                Some("rack-a".to_string()),
            )
            .unwrap();
        manager
            .register_worker_run(
                &second_group,
                worker_id,
                "127.0.0.1:9207".to_string(),
                1,
                second_run_id,
                Some("rack-b".to_string()),
            )
            .unwrap();
        manager
            .record_heartbeat(
                &first_group,
                worker_id,
                first_run_id,
                1,
                "127.0.0.1:9107",
                1,
                1_000,
                100,
                900,
                1,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();
        manager
            .record_heartbeat(
                &second_group,
                worker_id,
                second_run_id,
                1,
                "127.0.0.1:9207",
                1,
                2_000,
                300,
                1_700,
                3,
                1,
                HealthStatus::Degraded,
            )
            .unwrap();

        let first_descriptor = manager.get_descriptor(&first_group, worker_id).unwrap();
        let second_descriptor = manager.get_descriptor(&second_group, worker_id).unwrap();
        let first_registration = manager.get_registration(&first_group, worker_id).unwrap();
        let second_registration = manager.get_registration(&second_group, worker_id).unwrap();
        let first_runtime = manager.get_worker(&first_group, worker_id).unwrap();
        let second_runtime = manager.get_worker(&second_group, worker_id).unwrap();

        assert_eq!(first_descriptor.address, "127.0.0.1:9107");
        assert_eq!(second_descriptor.address, "127.0.0.1:9207");
        assert_eq!(first_registration.worker_run_id, first_run_id);
        assert_eq!(second_registration.worker_run_id, second_run_id);
        assert_eq!(first_runtime.capacity_total, 1_000);
        assert_eq!(second_runtime.capacity_total, 2_000);
        assert!(manager.is_worker_live(&first_group, worker_id));
        assert!(manager.is_worker_live(&second_group, worker_id));
        let mut live_workers = manager.list_live_workers();
        live_workers.sort_by_key(|key| (key.group_name.to_string(), key.worker_id.as_raw()));
        assert_eq!(
            live_workers,
            vec![
                WorkerRegistrationKey::new(&first_group, worker_id),
                WorkerRegistrationKey::new(&second_group, worker_id),
            ]
        );
    }

    #[test]
    fn worker_run_registration_same_run_same_descriptor_is_idempotent() {
        let manager = WorkerManager::new(60_000);
        let worker_id = WorkerId::new(1);
        let group_name_value = group_name("g1");
        let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440020".parse().unwrap();

        register_live_report_worker(&manager, &group_name_value, worker_id, first_run_id);
        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                first_run_id,
                1,
                0,
                true,
                vec![report_block(0)],
            )
            .unwrap();

        manager
            .validate_worker_registration_preflight(&group_name_value, worker_id, first_run_id, "127.0.0.1:9090", 1)
            .unwrap();
        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                first_run_id,
                None,
            )
            .unwrap();

        let descriptor = manager.get_descriptor(&group_name_value, worker_id).unwrap();
        assert_eq!(descriptor.address, "127.0.0.1:9090");
        assert_eq!(descriptor.worker_net_protocol, 1);
        assert_eq!(
            manager
                .get_registration(&group_name_value, worker_id)
                .unwrap()
                .worker_run_id,
            first_run_id
        );
        assert!(manager.is_worker_live(&group_name_value, worker_id));
        assert!(!manager.needs_full_block_report(&group_name_value, worker_id));
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(0).block_id),
            vec![worker_id]
        );
    }

    #[test]
    fn worker_run_registration_rejects_same_run_descriptor_mismatches_without_clearing_state() {
        for (worker_id, endpoint, protocol, run_id) in [
            (
                WorkerId::new(2),
                "127.0.0.1:9091",
                1,
                "550e8400-e29b-41d4-a716-446655440021".parse().unwrap(),
            ),
            (
                WorkerId::new(3),
                "127.0.0.1:9090",
                2,
                "550e8400-e29b-41d4-a716-446655440022".parse().unwrap(),
            ),
        ] {
            let manager = WorkerManager::new(60_000);
            let group_name_value = group_name("g1");
            register_live_report_worker(&manager, &group_name_value, worker_id, run_id);
            manager
                .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
                .unwrap();

            let error = manager
                .validate_worker_registration_preflight(&group_name_value, worker_id, run_id, endpoint, protocol)
                .expect_err("same worker_run_id must not change its descriptor");
            assert!(matches!(error, MetadataError::InvalidArgument(_)));
            assert!(error.to_string().contains("worker descriptor mismatch"));

            let apply_error = manager
                .register_worker_run(
                    &group_name_value,
                    worker_id,
                    endpoint.to_string(),
                    protocol,
                    run_id,
                    None,
                )
                .expect_err("same worker_run_id descriptor mismatch must fail at apply");
            assert!(matches!(apply_error, MetadataError::InvalidArgument(_)));
            assert!(apply_error.to_string().contains("worker descriptor mismatch"));

            let descriptor = manager.get_descriptor(&group_name_value, worker_id).unwrap();
            assert_eq!(descriptor.address, "127.0.0.1:9090");
            assert_eq!(descriptor.worker_net_protocol, 1);
            assert_eq!(
                manager
                    .get_registration(&group_name_value, worker_id)
                    .unwrap()
                    .worker_run_id,
                run_id
            );
            assert!(manager.is_worker_live(&group_name_value, worker_id));
            assert!(!manager.needs_full_block_report(&group_name_value, worker_id));
            assert_eq!(
                manager.get_block_locations(&group_name_value, report_block(0).block_id),
                vec![worker_id]
            );
        }
    }

    #[test]
    fn worker_run_registration_replaces_restart_and_resets_run_state() {
        let manager = WorkerManager::new(60_000);
        let worker_id = WorkerId::new(4);
        let group_name_value = group_name("g1");
        let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440023".parse().unwrap();
        let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440024".parse().unwrap();

        register_live_report_worker(&manager, &group_name_value, worker_id, first_run_id);
        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                first_run_id,
                1,
                0,
                true,
                vec![report_block(0)],
            )
            .unwrap();
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(0).block_id),
            vec![worker_id]
        );

        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                second_run_id,
                None,
            )
            .unwrap();

        assert_eq!(
            manager
                .get_registration(&group_name_value, worker_id)
                .unwrap()
                .worker_run_id,
            second_run_id
        );
        assert!(!manager.is_worker_live(&group_name_value, worker_id));
        assert!(manager.needs_full_block_report(&group_name_value, worker_id));
        assert!(manager.get_worker_blocks(&group_name_value, worker_id).is_empty());
        assert!(manager
            .get_block_locations(&group_name_value, report_block(0).block_id)
            .is_empty());
        let after_restart_snapshot = manager.blockreport_convergence_snapshot(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            60_000,
            0.80,
        );
        assert_eq!(after_restart_snapshot.active_workers, 0);
        assert_eq!(after_restart_snapshot.full_reported_workers, 0);

        let old_heartbeat = manager
            .record_heartbeat(
                &group_name_value,
                worker_id,
                first_run_id,
                2,
                "127.0.0.1:9090",
                1,
                1_000,
                100,
                900,
                0,
                0,
                HealthStatus::Healthy,
            )
            .expect_err("old worker_run_id must be fenced after replacement");
        assert!(matches!(old_heartbeat, MetadataError::StaleState(_)));
        assert!(old_heartbeat.to_string().contains("worker_run_id mismatch"));

        let old_report = manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                first_run_id,
                2,
                0,
                true,
                vec![report_block(1)],
            )
            .expect_err("old worker_run_id block report must be fenced after replacement");
        assert!(matches!(old_report, MetadataError::StaleState(_)));
        assert!(old_report.to_string().contains("worker_run_id mismatch"));

        manager
            .record_heartbeat(
                &group_name_value,
                worker_id,
                second_run_id,
                1,
                "127.0.0.1:9090",
                1,
                1_000,
                100,
                900,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();
        let before_new_full_report = manager.blockreport_convergence_snapshot(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            60_000,
            0.80,
        );
        assert_eq!(before_new_full_report.active_workers, 1);
        assert_eq!(before_new_full_report.full_reported_workers, 0);
        assert!(!before_new_full_report.converged);

        let delta = manager
            .apply_delta_block_report(
                &group_name_value,
                worker_id,
                second_run_id,
                1,
                0,
                vec![BlockReportDeltaEntry {
                    op: BlockReportDeltaOp::AddUpdate,
                    block: report_block(1),
                }],
            )
            .expect_err("replacement must require a new full report baseline");
        assert!(matches!(delta, MetadataError::FullReportRequired(_)));
    }

    #[test]
    fn worker_run_registration_updates_endpoint_when_previous_run_is_not_live() {
        let manager = WorkerManager::new(60_000);
        let worker_id = WorkerId::new(5);
        let group_name_value = group_name("g1");
        let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440025".parse().unwrap();
        let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440026".parse().unwrap();

        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                first_run_id,
                None,
            )
            .unwrap();
        manager
            .validate_worker_registration_preflight(&group_name_value, worker_id, second_run_id, "127.0.0.1:9091", 2)
            .unwrap();
        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9091".to_string(),
                2,
                second_run_id,
                None,
            )
            .unwrap();

        let descriptor = manager.get_descriptor(&group_name_value, worker_id).unwrap();
        let registration = manager.get_registration(&group_name_value, worker_id).unwrap();
        assert_eq!(descriptor.address, "127.0.0.1:9091");
        assert_eq!(descriptor.worker_net_protocol, 2);
        assert_eq!(registration.worker_run_id, second_run_id);
    }

    #[test]
    fn worker_run_registration_rejects_live_endpoint_conflict() {
        let manager = WorkerManager::new(60_000);
        let worker_id = WorkerId::new(6);
        let group_name_value = group_name("g1");
        let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440027".parse().unwrap();
        let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440028".parse().unwrap();

        register_live_report_worker(&manager, &group_name_value, worker_id, first_run_id);
        let error = manager
            .validate_worker_registration_preflight(&group_name_value, worker_id, second_run_id, "127.0.0.1:9091", 2)
            .expect_err("different endpoint for a live WorkerId must conflict");
        assert!(matches!(error, MetadataError::ActiveWorkerConflict(_)));
        assert!(error.to_string().contains("active worker conflict"));
        assert_eq!(
            manager
                .get_registration(&group_name_value, worker_id)
                .unwrap()
                .worker_run_id,
            first_run_id
        );
    }

    #[test]
    fn loading_persisted_workers_drops_live_run_registration() {
        let manager = WorkerManager::new(60_000);
        let worker_id = WorkerId::new(1);
        let group_name_value = group_name("g1");
        let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440030".parse().unwrap();

        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                run_id,
                Some("rack-a".to_string()),
            )
            .unwrap();
        manager
            .record_heartbeat(
                &group_name_value,
                worker_id,
                run_id,
                1,
                "127.0.0.1:9090",
                1,
                1000,
                10,
                990,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();
        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
            .unwrap();

        manager
            .load_registered_workers(vec![WorkerInfo {
                group_name: group_name_value.clone(),
                worker_id,
                address: "127.0.0.1:9090".to_string(),
                worker_net_protocol: 1,
                capacity_total: 0,
                capacity_used: 0,
                capacity_available: 0,
                active_reads: 0,
                active_writes: 0,
                health: HealthStatus::Healthy,
                last_heartbeat: 0,
                fault_domain: Some("rack-a".to_string()),
            }])
            .unwrap();

        assert!(manager.get_registration(&group_name_value, worker_id).is_none());
        assert!(manager.get_descriptor(&group_name_value, worker_id).is_some());
        assert!(manager.get_worker(&group_name_value, worker_id).is_none());
        assert!(manager.needs_full_block_report(&group_name_value, worker_id));
        assert_eq!(
            manager.list_worker_descriptors(),
            vec![WorkerRegistrationKey::new(&group_name_value, worker_id)]
        );
        assert!(manager.list_registered_workers().is_empty());
    }

    #[test]
    fn worker_heartbeat_updates_live_state_without_moving_stale_seq_backward() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g1");
        let worker_id = WorkerId::new(1);
        let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440040".parse().unwrap();

        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                run_id,
                Some("rack-a".to_string()),
            )
            .unwrap();

        let first = manager
            .record_heartbeat(
                &group_name_value,
                worker_id,
                run_id,
                10,
                "127.0.0.1:9090",
                1,
                1_000,
                100,
                900,
                2,
                1,
                HealthStatus::Healthy,
            )
            .unwrap();
        assert_eq!(first.heartbeat_seq, 10);
        assert_eq!(
            manager.get_worker(&group_name_value, worker_id).unwrap().capacity_total,
            1_000
        );

        let stale = manager
            .record_heartbeat(
                &group_name_value,
                worker_id,
                run_id,
                9,
                "127.0.0.1:9090",
                1,
                2_000,
                1_000,
                1_000,
                9,
                9,
                HealthStatus::Unhealthy,
            )
            .unwrap();
        assert_eq!(stale.heartbeat_seq, 10);

        let worker = manager.get_worker(&group_name_value, worker_id).unwrap();
        assert_eq!(worker.capacity_total, 1_000);
        assert_eq!(worker.active_reads, 2);
        assert_eq!(worker.health, HealthStatus::Healthy);
    }

    #[test]
    fn heartbeat_liveness_expiry_removes_runtime_but_keeps_registration() {
        let manager = WorkerManager::new(1_000);
        let group_name_value = group_name("g1");
        let worker_id = WorkerId::new(1);
        let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440041".parse().unwrap();

        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                run_id,
                None,
            )
            .unwrap();
        manager
            .record_heartbeat(
                &group_name_value,
                worker_id,
                run_id,
                1,
                "127.0.0.1:9090",
                1,
                1_000,
                100,
                900,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();

        std::thread::sleep(Duration::from_millis(1100));
        let expired = manager.expire_liveness();

        assert_eq!(expired, vec![(group_name_value.clone(), worker_id)]);
        assert!(!manager.is_worker_live(&group_name_value, worker_id));
        assert_eq!(
            manager
                .get_registration(&group_name_value, worker_id)
                .expect("current run registration")
                .worker_run_id,
            run_id
        );
        assert!(manager.get_descriptor(&group_name_value, worker_id).is_some());
    }

    #[test]
    fn remove_dead_worker_clears_runtime_state_but_keeps_descriptor() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g1");
        let worker_id = WorkerId::new(1);
        let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440042".parse().unwrap();
        let block = report_block(0).block_id;

        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);
        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                1,
                0,
                true,
                vec![report_block_with_id(block)],
            )
            .unwrap();
        assert_eq!(manager.get_worker_blocks(&group_name_value, worker_id), vec![block]);
        assert_eq!(
            manager.list_registered_workers(),
            vec![WorkerRegistrationKey::new(&group_name_value, worker_id)]
        );
        assert_eq!(
            manager.list_reported_blocks(),
            vec![BlockLocationKey::new(&group_name_value, block)]
        );
        assert!(!manager.needs_full_block_report(&group_name_value, worker_id));

        let (removed, affected_blocks) = manager.remove_dead_worker(&group_name_value, worker_id);

        assert!(removed);
        assert_eq!(affected_blocks, vec![block]);
        assert!(manager.get_registration(&group_name_value, worker_id).is_none());
        assert!(manager.get_worker(&group_name_value, worker_id).is_none());
        assert!(manager.get_worker_blocks(&group_name_value, worker_id).is_empty());
        assert!(manager.list_reported_blocks().is_empty());
        assert!(manager.needs_full_block_report(&group_name_value, worker_id));
        assert!(manager.get_descriptor(&group_name_value, worker_id).is_some());
        assert!(manager.list_registered_workers().is_empty());
        assert_eq!(
            manager.list_worker_descriptors(),
            vec![WorkerRegistrationKey::new(&group_name_value, worker_id)]
        );

        let (removed_again, affected_again) = manager.remove_dead_worker(&group_name_value, worker_id);
        assert!(!removed_again);
        assert!(affected_again.is_empty());
    }
}
