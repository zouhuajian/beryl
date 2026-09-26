// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-to-metadata full and incremental block reporting.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use beryl_common::error::rpc::{ErrorKind, RecoveryAction, RpcErrorDetail, WorkerErrorKind};
use beryl_proto::convert::rpc_error_from_proto;
use beryl_proto::metadata::metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient;
use beryl_proto::metadata::{
    block_report_request_proto, delta_block_report_entry_proto, BlockReportKindProto, BlockReportRequestProto,
    BlockReportResponseProto, DeltaBlockReportBatchProto, DeltaBlockReportEntryProto, FullBlockReportBatchProto,
    ReportedBlockProto, ReportedBlockStateProto,
};
use beryl_types::{BlockId, GroupName, MAX_REPORT_ENTRIES};
use thiserror::Error;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use tonic::Code;
use tracing::{debug, warn};

use crate::config::WorkerRegistrationConfig;
use crate::control::{ControlIdentity, ControlOp, Registration, RegistrationState};
use crate::error::WorkerError;
use crate::observe;
use crate::report::DirtyBlock;
use crate::store::block::{BlockMetaPayload, BlockState};
use crate::store::dirs::StoreDirs;
use crate::WorkerRuntime;

/// Configuration, retryable transport, and fatal protocol failures from reporting.
#[derive(Debug, Error)]
pub enum BlockReportError {
    #[error("invalid worker block report config: {0}")]
    InvalidConfig(String),
    #[error("retryable metadata block report error: {0}")]
    Retryable(String),
    #[error("fatal metadata block report error: {0}")]
    Fatal(String),
}

/// Result of one report submission to the configured Metadata leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockReportOutcome {
    Skipped,
    Accepted,
    FullReportRequired,
    NeedRegister,
    WorkerRunMismatch,
}

/// Immutable Full snapshot retained across every result-unknown retry.
#[derive(Debug)]
struct FullReportInFlight {
    registration_epoch: u64,
    baseline_seq: u64,
    store_snapshot_revision: u64,
    runtime_snapshot_revision: u64,
    blocks: Vec<ReportedBlockProto>,
    batch_ops: Vec<ControlOp>,
}

/// Dirty revisions covered by one immutable Delta entry.
#[derive(Clone, Copy, Debug)]
struct TrackedBlockChange {
    block_id: BlockId,
    store_revision: Option<u64>,
    runtime_revision: Option<u64>,
}

/// Immutable Delta request retained until Metadata acknowledges its sequence.
#[derive(Debug)]
struct DeltaReportInFlight {
    registration_epoch: u64,
    baseline_seq: u64,
    batch_seq: u64,
    op: ControlOp,
    entries: Vec<DeltaBlockReportEntryProto>,
    tracked: Vec<TrackedBlockChange>,
}

/// Worker-side synchronization state for one configured metadata group.
///
/// The state stores only report identity and one in-flight request. The local
/// stores remain physical authority, so a long-lived copy of the entire block
/// inventory is neither required nor allowed on the Delta path.
#[derive(Debug, Default)]
struct ReportRuntime {
    registration_epoch: u64,
    next_baseline_seq: u64,
    active_baseline_seq: Option<u64>,
    next_delta_batch_seq: u64,
    full_inflight: Option<Arc<FullReportInFlight>>,
    delta_inflight: Option<Arc<DeltaReportInFlight>>,
}

/// Sends Full reports only to establish or recover a baseline and uses retained
/// dirty identities for the steady-state Delta path.
///
/// The interval is a bounded flush and retry cadence. It never schedules a
/// periodic Full report while the current baseline remains continuous.
pub struct MetadataBlockReportLoop {
    config: WorkerRegistrationConfig,
    state: Arc<RegistrationState>,
    endpoint: Endpoint,
    store: Arc<StoreDirs>,
    worker_runtime: Arc<WorkerRuntime>,
    batch_size: usize,
    delta_flush_interval: Duration,
    control_identity: ControlIdentity,
    report: Mutex<ReportRuntime>,
}

impl MetadataBlockReportLoop {
    /// Builds a reporter with an explicit retry and Delta flush cadence.
    pub fn new(
        config: WorkerRegistrationConfig,
        state: Arc<RegistrationState>,
        store: Arc<StoreDirs>,
        worker_runtime: Arc<WorkerRuntime>,
        batch_size: usize,
        delta_flush_interval: Duration,
    ) -> Result<Self, BlockReportError> {
        let endpoint = config
            .validate()
            .map_err(|err| BlockReportError::InvalidConfig(err.message))?;
        if delta_flush_interval.is_zero() {
            return Err(BlockReportError::InvalidConfig(
                "block report Delta flush interval must be greater than zero".to_string(),
            ));
        }
        validate_batch_limit(batch_size)?;
        if store.block_report_changes().group_name != config.group_name
            || worker_runtime.block_report_changes().group_name != config.group_name
        {
            return Err(BlockReportError::InvalidConfig(
                "block report sources must track the configured group".into(),
            ));
        }

        Ok(Self {
            config,
            state,
            endpoint,
            store,
            worker_runtime,
            batch_size,
            delta_flush_interval,
            control_identity: ControlIdentity::new_local(),
            report: Mutex::new(ReportRuntime::default()),
        })
    }

    /// Starts block reporting under the process shutdown token.
    pub fn spawn_until_shutdown(self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(shutdown).await })
    }

    /// Returns whether the current registration owns an accepted Full baseline.
    pub fn has_delta_baseline(&self) -> bool {
        let Some((_, registration_epoch)) = self.state.ready_registration(&self.config.group_name) else {
            return false;
        };
        let report = self.report.lock().expect("block report state poisoned");
        report.registration_epoch == registration_epoch && report.active_baseline_seq.is_some()
    }

    /// Sends or exactly retries one Full snapshot for the current registration.
    pub async fn send_full_once(&self) -> Result<BlockReportOutcome, BlockReportError> {
        let Some((registration, registration_epoch)) = self.ready_registration() else {
            return Ok(BlockReportOutcome::Skipped);
        };
        let full = self.prepare_full_report(registration_epoch)?;
        let started = Instant::now();
        match self.send_full(&registration, &full).await {
            Ok(BlockReportReply::FullAccepted { .. }) => {
                let duration = started.elapsed().as_secs_f64();
                observe::record_metadata_rpc("block_report", "ok", "none", duration);
                observe::record_block_report_sent("full", "ok", "none", duration);
                self.accept_full_report(registration_epoch, full.baseline_seq);
                Ok(BlockReportOutcome::Accepted)
            }
            Ok(outcome) => Ok(self.record_structured_outcome(&registration.group_name, outcome, "full", started)),
            Err(error) => {
                observe::record_metadata_rpc(
                    "block_report",
                    "error",
                    block_report_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                debug!(%error, "Worker full block report endpoint attempt failed");
                Err(error)
            }
        }
    }

    /// Sends or exactly retries one bounded Delta batch for the current baseline.
    pub async fn send_delta_once(&self) -> Result<BlockReportOutcome, BlockReportError> {
        let Some((registration, registration_epoch)) = self.ready_registration() else {
            return Ok(BlockReportOutcome::Skipped);
        };
        let delta = match self.prepare_delta_report(&registration.group_name, registration_epoch)? {
            DeltaPreparation::NoChanges => return Ok(BlockReportOutcome::Skipped),
            DeltaPreparation::FullRequired => {
                return Ok(BlockReportOutcome::FullReportRequired);
            }
            DeltaPreparation::Ready(delta) => delta,
        };

        let started = Instant::now();
        match self.send_delta(&registration, &delta).await {
            Ok(BlockReportReply::DeltaAccepted { next_batch_seq }) => {
                let duration = started.elapsed().as_secs_f64();
                observe::record_metadata_rpc("block_report", "ok", "none", duration);
                observe::record_block_report_sent("delta", "ok", "none", duration);
                self.accept_delta_report(registration_epoch, delta.batch_seq, next_batch_seq)?;
                Ok(BlockReportOutcome::Accepted)
            }
            Ok(outcome) => Ok(self.record_structured_outcome(&registration.group_name, outcome, "delta", started)),
            Err(error) => {
                observe::record_metadata_rpc(
                    "block_report",
                    "error",
                    block_report_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                debug!(%error, "Worker delta block report endpoint attempt failed");
                Err(error)
            }
        }
    }

    fn ready_registration(&self) -> Option<(Registration, u64)> {
        self.state.ready_registration(&self.config.group_name)
    }

    /// Builds a Full snapshot once and retains it unchanged until acknowledgement.
    fn prepare_full_report(&self, registration_epoch: u64) -> Result<Arc<FullReportInFlight>, BlockReportError> {
        let mut report = self.report.lock().expect("block report state poisoned");
        bind_registration(&mut report, registration_epoch);
        if let Some(full) = &report.full_inflight {
            return Ok(Arc::clone(full));
        }

        let store_snapshot_revision = self.store.block_report_changes().begin_full_snapshot();
        let runtime_snapshot_revision = self.worker_runtime.block_report_changes().begin_full_snapshot();
        let blocks = self.scan_report_blocks()?;
        report.next_baseline_seq = report
            .next_baseline_seq
            .checked_add(1)
            .ok_or_else(|| BlockReportError::Fatal("block report baseline sequence overflow".to_string()))?;
        let batch_count = blocks.len().max(1).div_ceil(self.batch_size);
        let full = Arc::new(FullReportInFlight {
            registration_epoch,
            baseline_seq: report.next_baseline_seq,
            store_snapshot_revision,
            runtime_snapshot_revision,
            blocks,
            batch_ops: (0..batch_count).map(|_| self.control_identity.new_op()).collect(),
        });
        report.active_baseline_seq = None;
        report.next_delta_batch_seq = 0;
        report.delta_inflight = None;
        report.full_inflight = Some(Arc::clone(&full));
        Ok(full)
    }

    /// Builds one Delta from retained dirty identities without scanning inventory.
    fn prepare_delta_report(
        &self,
        group_name: &GroupName,
        registration_epoch: u64,
    ) -> Result<DeltaPreparation, BlockReportError> {
        let mut report = self.report.lock().expect("block report state poisoned");
        bind_registration(&mut report, registration_epoch);
        if let Some(delta) = &report.delta_inflight {
            return Ok(DeltaPreparation::Ready(Arc::clone(delta)));
        }
        let Some(baseline_seq) = report.active_baseline_seq else {
            return Ok(DeltaPreparation::FullRequired);
        };

        let store_dirty = match self.store.block_report_changes().snapshot() {
            Ok(dirty) => dirty,
            Err(()) => {
                reset_baseline(&mut report);
                return Ok(DeltaPreparation::FullRequired);
            }
        };
        let runtime_dirty = match self.worker_runtime.block_report_changes().snapshot() {
            Ok(dirty) => dirty,
            Err(()) => {
                reset_baseline(&mut report);
                return Ok(DeltaPreparation::FullRequired);
            }
        };
        let tracked = merge_dirty_changes(store_dirty, runtime_dirty, self.batch_size);
        if tracked.is_empty() {
            return Ok(DeltaPreparation::NoChanges);
        }

        let mut entries = Vec::with_capacity(tracked.len());
        for entry in &tracked {
            entries.push(self.resolve_delta_entry(group_name, entry.block_id)?);
        }
        let delta = Arc::new(DeltaReportInFlight {
            registration_epoch,
            baseline_seq,
            batch_seq: report.next_delta_batch_seq,
            op: self.control_identity.new_op(),
            entries,
            tracked,
        });
        report.delta_inflight = Some(Arc::clone(&delta));
        Ok(DeltaPreparation::Ready(delta))
    }

    /// Resolves one dirty identity against the current store and reclaim fence.
    fn resolve_delta_entry(
        &self,
        group_name: &GroupName,
        block_id: BlockId,
    ) -> Result<DeltaBlockReportEntryProto, BlockReportError> {
        if self.worker_runtime.is_reclaiming(group_name, block_id) {
            return Ok(present_entry(ReportedBlockProto {
                block_id: Some(block_id.into()),
                lease_epoch: 0,
                tier: 0,
                state: ReportedBlockStateProto::ReportedBlockStateDeleting as i32,
                effective_len: 0,
            }));
        }
        match self.store.load_report_meta(group_name, block_id) {
            Ok(meta) => Ok(present_entry(meta_to_report_block(meta))),
            Err(WorkerError::NotFound(_)) => Ok(DeltaBlockReportEntryProto {
                block: Some(delta_block_report_entry_proto::Block::Absent(block_id.into())),
            }),
            Err(error) => Err(BlockReportError::Retryable(format!(
                "load changed local block for report failed: {error}"
            ))),
        }
    }

    /// Builds the authoritative local view used only by Full recovery.
    fn scan_report_blocks(&self) -> Result<Vec<ReportedBlockProto>, BlockReportError> {
        let metas = self
            .store
            .scan_group_blocks(&self.config.group_name)
            .map_err(|err| BlockReportError::Retryable(format!("scan local block report group failed: {err}")))?;
        let mut blocks = BTreeMap::new();
        for meta in metas {
            blocks.insert(meta.block_id, meta_to_report_block(meta));
        }
        for block_id in self.worker_runtime.reclaiming_blocks(&self.config.group_name) {
            blocks.insert(
                block_id,
                ReportedBlockProto {
                    block_id: Some(block_id.into()),
                    lease_epoch: 0,
                    tier: 0,
                    state: ReportedBlockStateProto::ReportedBlockStateDeleting as i32,
                    effective_len: 0,
                },
            );
        }
        Ok(blocks.into_values().collect())
    }

    /// Commits a Full acknowledgement only if it still names the in-flight snapshot.
    fn accept_full_report(&self, registration_epoch: u64, baseline_seq: u64) {
        let mut report = self.report.lock().expect("block report state poisoned");
        let Some(full) = report.full_inflight.as_ref() else {
            return;
        };
        if full.registration_epoch != registration_epoch || full.baseline_seq != baseline_seq {
            return;
        }
        let store_continuous = self
            .store
            .block_report_changes()
            .acknowledge_full(full.store_snapshot_revision);
        let runtime_continuous = self
            .worker_runtime
            .block_report_changes()
            .acknowledge_full(full.runtime_snapshot_revision);
        report.full_inflight = None;
        report.delta_inflight = None;
        if store_continuous && runtime_continuous {
            report.active_baseline_seq = Some(baseline_seq);
            report.next_delta_batch_seq = 0;
        } else {
            reset_baseline(&mut report);
        }
    }

    /// Advances Delta state only after the exact in-flight batch is acknowledged.
    fn accept_delta_report(
        &self,
        registration_epoch: u64,
        batch_seq: u64,
        next_batch_seq: u64,
    ) -> Result<(), BlockReportError> {
        let expected_next = batch_seq
            .checked_add(1)
            .ok_or_else(|| BlockReportError::Fatal("delta batch sequence overflow".to_string()))?;
        if next_batch_seq != expected_next {
            return Err(BlockReportError::Fatal(format!(
                "metadata acknowledged next delta batch {next_batch_seq}, expected {expected_next}"
            )));
        }
        let mut report = self.report.lock().expect("block report state poisoned");
        let Some(delta) = report.delta_inflight.as_ref() else {
            return Ok(());
        };
        if delta.registration_epoch != registration_epoch || delta.batch_seq != batch_seq {
            return Ok(());
        }

        let store_ack = delta
            .tracked
            .iter()
            .filter_map(|entry| {
                entry.store_revision.map(|revision| DirtyBlock {
                    block_id: entry.block_id,
                    revision,
                })
            })
            .collect::<Vec<_>>();
        let runtime_ack = delta
            .tracked
            .iter()
            .filter_map(|entry| {
                entry.runtime_revision.map(|revision| DirtyBlock {
                    block_id: entry.block_id,
                    revision,
                })
            })
            .collect::<Vec<_>>();
        self.store.block_report_changes().acknowledge(&store_ack);
        self.worker_runtime.block_report_changes().acknowledge(&runtime_ack);
        report.delta_inflight = None;
        report.next_delta_batch_seq = next_batch_seq;
        Ok(())
    }

    fn reset_baseline(&self) {
        let mut report = self.report.lock().expect("block report state poisoned");
        reset_baseline(&mut report);
    }

    fn record_structured_outcome(
        &self,
        group_name: &GroupName,
        outcome: BlockReportReply,
        report_kind: &'static str,
        started: Instant,
    ) -> BlockReportOutcome {
        let (result, error_kind) = match outcome {
            BlockReportReply::FullReportRequired => {
                self.reset_baseline();
                (BlockReportOutcome::FullReportRequired, "full_report_required")
            }
            BlockReportReply::NeedRegister => {
                self.state.mark_needs_register(group_name);
                self.reset_baseline();
                (BlockReportOutcome::NeedRegister, "need_register")
            }
            BlockReportReply::WorkerRunMismatch => {
                self.state.mark_needs_register(group_name);
                self.reset_baseline();
                (BlockReportOutcome::WorkerRunMismatch, "worker_run_mismatch")
            }
            BlockReportReply::FullAccepted { .. } | BlockReportReply::DeltaAccepted { .. } => {
                unreachable!("accepted outcomes are handled by the caller")
            }
        };
        observe::record_metadata_rpc("block_report", "error", error_kind, started.elapsed().as_secs_f64());
        observe::record_block_report_sent(report_kind, "error", error_kind, started.elapsed().as_secs_f64());
        result
    }

    async fn send_full(
        &self,
        registration: &Registration,
        full: &FullReportInFlight,
    ) -> Result<BlockReportReply, BlockReportError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let channel = time::timeout(timeout, self.endpoint.connect())
            .await
            .map_err(|_| BlockReportError::Retryable("metadata block report connect timed out".to_string()))?
            .map_err(|err| BlockReportError::Retryable(format!("metadata block report endpoint unavailable: {err}")))?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);

        let batch_count = full.batch_ops.len();
        let mut batch_seq = 0usize;
        loop {
            let start = batch_seq * self.batch_size;
            let end = (start + self.batch_size).min(full.blocks.len());
            let blocks = &full.blocks[start..end];
            let final_batch = batch_seq + 1 == batch_count;
            let outcome = self
                .send_full_batch(&mut client, registration, full, batch_seq, blocks, final_batch)
                .await?;
            match outcome {
                BlockReportReply::FullAccepted {
                    baseline_published: true,
                    ..
                } => return Ok(outcome),
                BlockReportReply::FullAccepted {
                    next_batch_seq,
                    baseline_published: false,
                } => {
                    let next_batch_seq = usize::try_from(next_batch_seq).map_err(|_| {
                        BlockReportError::Fatal(
                            "metadata full report acknowledgement exceeds local batch range".to_string(),
                        )
                    })?;
                    if next_batch_seq >= batch_count {
                        return Err(BlockReportError::Fatal(format!(
                            "metadata full report acknowledgement selected invalid next_batch_seq {next_batch_seq} after batch {batch_seq} of {batch_count}"
                        )));
                    }
                    batch_seq = next_batch_seq;
                }
                _ => return Ok(outcome),
            }
        }
    }

    async fn send_full_batch(
        &self,
        client: &mut MetadataWorkerServiceProtoClient<tonic::transport::Channel>,
        registration: &Registration,
        full: &FullReportInFlight,
        batch_seq: usize,
        blocks: &[ReportedBlockProto],
        final_batch: bool,
    ) -> Result<BlockReportReply, BlockReportError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let op = &full.batch_ops[batch_seq];
        let request = BlockReportRequestProto {
            header: Some(op.request_header(&registration.group_name)),
            worker_id: registration.worker_id.as_raw(),
            worker_run_id: registration.worker_run_id.to_string(),
            baseline_seq: full.baseline_seq,
            batch: Some(block_report_request_proto::Batch::FullReport(
                FullBlockReportBatchProto {
                    batch_seq: batch_seq as u64,
                    final_batch,
                    blocks: blocks.to_vec(),
                },
            )),
        };
        let tonic_request = tonic::Request::new(request);
        let response = time::timeout(timeout, client.block_report(tonic_request))
            .await
            .map_err(|_| BlockReportError::Retryable("metadata full block report timed out".to_string()))?
            .map_err(classify_status)?
            .into_inner();
        classify_block_report_response(
            &registration.group_name,
            full.baseline_seq,
            Some((batch_seq as u64, final_batch)),
            response,
        )
    }

    async fn send_delta(
        &self,
        registration: &Registration,
        delta: &DeltaReportInFlight,
    ) -> Result<BlockReportReply, BlockReportError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let channel = time::timeout(timeout, self.endpoint.connect())
            .await
            .map_err(|_| BlockReportError::Retryable("metadata delta report connect timed out".to_string()))?
            .map_err(|err| BlockReportError::Retryable(format!("metadata delta report endpoint unavailable: {err}")))?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);
        let request = BlockReportRequestProto {
            header: Some(delta.op.request_header(&registration.group_name)),
            worker_id: registration.worker_id.as_raw(),
            worker_run_id: registration.worker_run_id.to_string(),
            baseline_seq: delta.baseline_seq,
            batch: Some(block_report_request_proto::Batch::DeltaReport(
                DeltaBlockReportBatchProto {
                    batch_seq: delta.batch_seq,
                    entries: delta.entries.clone(),
                },
            )),
        };
        let tonic_request = tonic::Request::new(request);
        let response = time::timeout(timeout, client.block_report(tonic_request))
            .await
            .map_err(|_| BlockReportError::Retryable("metadata delta block report timed out".to_string()))?
            .map_err(classify_status)?
            .into_inner();
        classify_block_report_response(&registration.group_name, delta.baseline_seq, None, response)
    }

    /// Flushes retained changes and retries in-flight requests until shutdown.
    async fn run(self, shutdown: CancellationToken) {
        let mut interval = time::interval(self.delta_flush_interval);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = interval.tick() => {}
                _ = self.store.wait_for_block_report_change() => {}
                _ = self.worker_runtime.wait_for_block_report_change() => {}
            }
            let report = async {
                if self.has_delta_baseline() {
                    match self.send_delta_once().await {
                        Ok(BlockReportOutcome::FullReportRequired) => {
                            if let Err(error) = self.send_full_once().await {
                                warn!(%error, "Worker full block report recovery failed");
                            }
                        }
                        Ok(_) => {}
                        Err(error) => warn!(%error, "Worker delta block report round failed"),
                    }
                } else if let Err(error) = self.send_full_once().await {
                    warn!(%error, "Worker full block report round failed");
                }
            };
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = report => {}
            }
        }
    }
}

enum DeltaPreparation {
    NoChanges,
    FullRequired,
    Ready(Arc<DeltaReportInFlight>),
}

enum BlockReportReply {
    FullAccepted {
        next_batch_seq: u64,
        baseline_published: bool,
    },
    DeltaAccepted {
        next_batch_seq: u64,
    },
    FullReportRequired,
    NeedRegister,
    WorkerRunMismatch,
}

fn validate_batch_limit(value: usize) -> Result<(), BlockReportError> {
    if value == 0 {
        return Err(BlockReportError::InvalidConfig(
            "block report batch_size must be greater than zero".to_string(),
        ));
    }
    if value > MAX_REPORT_ENTRIES {
        return Err(BlockReportError::InvalidConfig(format!(
            "block report batch_size {value} exceeds maximum {MAX_REPORT_ENTRIES}"
        )));
    }
    Ok(())
}

/// Fences report identity and in-flight work to the current registration epoch.
fn bind_registration(report: &mut ReportRuntime, registration_epoch: u64) {
    if report.registration_epoch == registration_epoch {
        return;
    }
    report.registration_epoch = registration_epoch;
    reset_baseline(report);
}

/// Drops only synchronization state; retained dirty identities remain pending.
fn reset_baseline(report: &mut ReportRuntime) {
    report.active_baseline_seq = None;
    report.next_delta_batch_seq = 0;
    report.full_inflight = None;
    report.delta_inflight = None;
}

/// Coalesces store and reclaim changes by identity while preserving both revisions.
fn merge_dirty_changes(store: Vec<DirtyBlock>, runtime: Vec<DirtyBlock>, limit: usize) -> Vec<TrackedBlockChange> {
    let mut merged = BTreeMap::<BlockId, TrackedBlockChange>::new();
    for entry in store {
        merged
            .entry(entry.block_id)
            .or_insert(TrackedBlockChange {
                block_id: entry.block_id,
                store_revision: None,
                runtime_revision: None,
            })
            .store_revision = Some(entry.revision);
    }
    for entry in runtime {
        merged
            .entry(entry.block_id)
            .or_insert(TrackedBlockChange {
                block_id: entry.block_id,
                store_revision: None,
                runtime_revision: None,
            })
            .runtime_revision = Some(entry.revision);
    }
    merged.into_values().take(limit).collect()
}

fn present_entry(block: ReportedBlockProto) -> DeltaBlockReportEntryProto {
    DeltaBlockReportEntryProto {
        block: Some(delta_block_report_entry_proto::Block::Present(block)),
    }
}

fn meta_to_report_block(meta: BlockMetaPayload) -> ReportedBlockProto {
    let block_state = match meta.block_state {
        BlockState::Ready => ReportedBlockStateProto::ReportedBlockStateReady,
        BlockState::Deleting => ReportedBlockStateProto::ReportedBlockStateDeleting,
    };
    let block_id = meta.block_id;
    ReportedBlockProto {
        block_id: Some(block_id.into()),
        lease_epoch: meta.fencing_token.epoch.as_raw(),
        tier: beryl_proto::common::TierProto::from(meta.tier) as i32,
        state: block_state as i32,
        effective_len: meta.durable_len,
    }
}

fn block_report_error_kind(error: &BlockReportError) -> &'static str {
    match error {
        BlockReportError::InvalidConfig(_) => "invalid_config",
        BlockReportError::Retryable(_) => "retryable",
        BlockReportError::Fatal(_) => "fatal",
    }
}

/// Validates that Metadata confirmed the exact report kind and baseline progress.
fn classify_block_report_response(
    request_group_name: &GroupName,
    baseline_seq: u64,
    full_batch: Option<(u64, bool)>,
    response: BlockReportResponseProto,
) -> Result<BlockReportReply, BlockReportError> {
    let header = response
        .header
        .as_ref()
        .ok_or_else(|| BlockReportError::Fatal("metadata block report response missing ResponseHeader".to_string()))?;
    let response_group_name = header.group_name.as_str();
    if response_group_name != request_group_name.as_str() {
        return Err(BlockReportError::Fatal(format!(
            "metadata block report response confirmed group_name {response_group_name}, expected {request_group_name}"
        )));
    }
    if let Some(error) = header.error.as_ref() {
        return classify_rpc_error(rpc_error_from_proto(error));
    }
    let report_kind = BlockReportKindProto::try_from(response.report_kind).map_err(|_| {
        BlockReportError::Fatal(format!(
            "metadata block report response returned unknown report_kind {}",
            response.report_kind
        ))
    })?;
    if response.baseline_seq != baseline_seq {
        return Err(BlockReportError::Fatal(format!(
            "metadata block report response confirmed baseline_seq {}, expected {}",
            response.baseline_seq, baseline_seq
        )));
    }
    match (full_batch, report_kind) {
        (Some((batch_seq, final_batch)), BlockReportKindProto::BlockReportKindFull) => {
            let expected_next = batch_seq
                .checked_add(1)
                .ok_or_else(|| BlockReportError::Fatal("full report batch sequence overflow".to_string()))?;
            if response.baseline_published || (!final_batch && response.next_batch_seq >= expected_next) {
                Ok(BlockReportReply::FullAccepted {
                    next_batch_seq: response.next_batch_seq,
                    baseline_published: response.baseline_published,
                })
            } else {
                Err(BlockReportError::Fatal(format!(
                    "metadata acknowledged full batch with next_batch_seq={} and baseline_published={}, expected next_batch_seq>={}{}",
                    response.next_batch_seq,
                    response.baseline_published,
                    expected_next,
                    if final_batch { " and a published baseline" } else { "" }
                )))
            }
        }
        (None, BlockReportKindProto::BlockReportKindDelta) if response.baseline_published => {
            Ok(BlockReportReply::DeltaAccepted {
                next_batch_seq: response.next_batch_seq,
            })
        }
        _ => Err(BlockReportError::Fatal(
            "metadata block report response did not confirm the requested report kind or a published Delta baseline"
                .to_string(),
        )),
    }
}

fn classify_rpc_error(error: RpcErrorDetail) -> Result<BlockReportReply, BlockReportError> {
    match error.recovery {
        RecoveryAction::SendFullBlockReport => Ok(BlockReportReply::FullReportRequired),
        RecoveryAction::RegisterWorker if error.kind == ErrorKind::Worker(WorkerErrorKind::RunMismatch) => {
            Ok(BlockReportReply::WorkerRunMismatch)
        }
        RecoveryAction::RegisterWorker => Ok(BlockReportReply::NeedRegister),
        RecoveryAction::Retry { .. } | RecoveryAction::RefreshMetadata { .. } => {
            Err(BlockReportError::Retryable(error.message))
        }
        RecoveryAction::Fail | RecoveryAction::ReopenWriteSession { .. } => Err(BlockReportError::Fatal(format!(
            "fatal metadata block report error: {}",
            error.message
        ))),
    }
}

fn classify_status(status: tonic::Status) -> BlockReportError {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted => {
            BlockReportError::Retryable(status.to_string())
        }
        _ => BlockReportError::Fatal(format!("metadata block report RPC failed: {status}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_proto::common::ResponseHeaderProto;

    #[test]
    fn acknowledgements_must_confirm_requested_report_and_progress() {
        let group_name = GroupName::parse("root").unwrap();
        let full = BlockReportKindProto::BlockReportKindFull as i32;
        let delta = BlockReportKindProto::BlockReportKindDelta as i32;
        for (full_batch, kind, baseline, next, published, accepted) in [
            (Some((0, false)), i32::MAX, 1, 1, false, false),
            (Some((0, false)), delta, 1, 1, true, false),
            (Some((0, false)), full, 2, 1, false, false),
            (Some((0, false)), full, 1, 0, false, false),
            (Some((0, true)), full, 1, 1, false, false),
            (None, delta, 1, 1, false, false),
            (Some((0, false)), full, 1, 1, false, true),
            (Some((0, false)), full, 1, 2, false, true),
            (Some((0, true)), full, 1, 0, true, true),
            (None, delta, 1, 1, true, true),
        ] {
            let response = BlockReportResponseProto {
                header: Some(ResponseHeaderProto {
                    group_name: group_name.to_string(),
                    ..Default::default()
                }),
                report_kind: kind,
                baseline_seq: baseline,
                next_batch_seq: next,
                baseline_published: published,
            };
            let result = classify_block_report_response(&group_name, 1, full_batch, response);
            if accepted {
                assert!(result.is_ok(), "{:?}", result.err());
            } else {
                assert!(matches!(result, Err(BlockReportError::Fatal(_))), "{:?}", result.err());
            }
        }
    }
}
