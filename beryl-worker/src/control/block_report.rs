// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-to-metadata block report fanout.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use beryl_common::error::rpc::{ErrorKind, RecoveryAction, RpcErrorDetail, WorkerErrorKind};
use beryl_common::header::RequestHeader;
use beryl_proto::common::RequestHeaderProto;
use beryl_proto::convert::rpc_error_from_proto;
use beryl_proto::metadata::metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient;
use beryl_proto::metadata::{
    block_report_request_proto, BlockReportBlockProto, BlockReportBlockStateProto, BlockReportDeltaOpProto,
    BlockReportDeltaProto, BlockReportRequestProto, BlockReportResponseProto, DeltaBlockReportProto,
    FullBlockReportBatchProto,
};
use beryl_types::{BlockId, GroupName};
use thiserror::Error;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use tonic::Code;
use tracing::{debug, warn};

use beryl_types::MAX_REPORT_ENTRIES;

use crate::config::WorkerRegistrationConfig;
use crate::control::{
    metadata_tonic_request, ControlIdentity, ControlOp, Registration, RegistrationDescriptor, RegistrationSet,
};
use crate::observe;
use crate::store::block::{BlockMetaPayload, BlockState};
use crate::store::dirs::StoreDirs;
use crate::WorkerCore;

/// Worker-side batching policy constrained by the shared report protocol cap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockReportOptions {
    /// Maximum block entries sent in one full-report batch.
    pub full_max_blocks_per_batch: usize,
    /// Maximum delta entries sent in one delta-report request.
    pub delta_max_entries_per_batch: usize,
}

impl Default for BlockReportOptions {
    fn default() -> Self {
        Self {
            full_max_blocks_per_batch: MAX_REPORT_ENTRIES,
            delta_max_entries_per_batch: MAX_REPORT_ENTRIES,
        }
    }
}

#[derive(Debug, Error)]
pub enum BlockReportError {
    #[error("invalid worker block report config: {0}")]
    InvalidConfig(String),
    #[error("retryable metadata block report error: {0}")]
    Retryable(String),
    #[error("fatal metadata block report error: {0}")]
    Fatal(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockReportRound {
    pub attempted_peers: usize,
    pub accepted_peers: usize,
    pub full_report_required: bool,
    pub needs_register: bool,
    pub worker_run_mismatch: bool,
}

/// Last Metadata-accepted block view used to derive ordered delta reports.
///
/// A baseline is usable only by the registration epoch that established it.
#[derive(Clone, Debug, Default)]
struct ReportBaseline {
    report_seq: u64,
    next_delta_seq: u64,
    registration_epoch: u64,
    blocks: HashMap<BlockId, BlockReportBlockProto>,
    ready: bool,
}

/// Sends full and delta block reports for one registered metadata group.
///
/// Local block changes wake the loop promptly, while the periodic tick remains
/// the bounded recovery path for coalesced notifications and failed RPCs.
pub struct MetadataBlockReportLoop {
    config: WorkerRegistrationConfig,
    _descriptor: RegistrationDescriptor,
    state: Arc<RegistrationSet>,
    endpoints: Vec<Endpoint>,
    store: Arc<StoreDirs>,
    core: Arc<WorkerCore>,
    options: BlockReportOptions,
    interval: Duration,
    control_identity: ControlIdentity,
    baselines: Mutex<HashMap<GroupName, ReportBaseline>>,
}

impl MetadataBlockReportLoop {
    pub fn new(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
        store: Arc<StoreDirs>,
        core: Arc<WorkerCore>,
    ) -> Result<Self, BlockReportError> {
        Self::with_options(config, descriptor, state, store, core, BlockReportOptions::default())
    }

    pub fn with_options(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
        store: Arc<StoreDirs>,
        core: Arc<WorkerCore>,
        options: BlockReportOptions,
    ) -> Result<Self, BlockReportError> {
        Self::with_options_and_interval(config, descriptor, state, store, core, options, Duration::from_secs(1))
    }

    pub fn with_options_and_interval(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
        store: Arc<StoreDirs>,
        core: Arc<WorkerCore>,
        options: BlockReportOptions,
        interval: Duration,
    ) -> Result<Self, BlockReportError> {
        config
            .validate()
            .map_err(|err| BlockReportError::InvalidConfig(err.message))?;
        if interval.is_zero() {
            return Err(BlockReportError::InvalidConfig(
                "block report interval must be greater than zero".to_string(),
            ));
        }
        if options.full_max_blocks_per_batch == 0 {
            return Err(BlockReportError::InvalidConfig(
                "full_max_blocks_per_batch must be greater than zero".to_string(),
            ));
        }
        if options.full_max_blocks_per_batch > MAX_REPORT_ENTRIES {
            return Err(BlockReportError::InvalidConfig(format!(
                "full_max_blocks_per_batch {} exceeds maximum {}",
                options.full_max_blocks_per_batch, MAX_REPORT_ENTRIES
            )));
        }
        if options.delta_max_entries_per_batch == 0 {
            return Err(BlockReportError::InvalidConfig(
                "delta_max_entries_per_batch must be greater than zero".to_string(),
            ));
        }
        if options.delta_max_entries_per_batch > MAX_REPORT_ENTRIES {
            return Err(BlockReportError::InvalidConfig(format!(
                "delta_max_entries_per_batch {} exceeds maximum {}",
                options.delta_max_entries_per_batch, MAX_REPORT_ENTRIES
            )));
        }

        let mut endpoints = Vec::with_capacity(config.endpoints.len());
        for endpoint in &config.endpoints {
            endpoints.push(
                Endpoint::from_shared(endpoint.clone()).map_err(|err| {
                    BlockReportError::InvalidConfig(format!("beryl.worker.metadata.addresses: {err}"))
                })?,
            );
        }

        Ok(Self {
            config,
            _descriptor: descriptor,
            state,
            endpoints,
            store,
            core,
            options,
            interval,
            control_identity: ControlIdentity::new_local(),
            baselines: Mutex::new(HashMap::new()),
        })
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        self.spawn_until_shutdown(CancellationToken::new())
    }

    /// Starts block reporting under the process shutdown token.
    pub fn spawn_until_shutdown(self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(shutdown).await })
    }

    /// Returns whether the current live registration has an accepted
    /// full-report baseline.
    pub fn has_delta_baseline(&self, group_name: &GroupName) -> bool {
        let Some((_, registration_epoch)) = self.state.ready_registration(group_name) else {
            return false;
        };
        self.baselines
            .lock()
            .expect("block report baseline state poisoned")
            .get(group_name)
            .map(|baseline| baseline.ready && baseline.registration_epoch == registration_epoch)
            .unwrap_or(false)
    }

    /// Sends one full-report round and binds any accepted baseline to the
    /// captured registration epoch.
    pub async fn send_full_once(&self) -> Result<BlockReportRound, BlockReportError> {
        let Some((registration, registration_epoch)) = self.ready_registration() else {
            return Ok(BlockReportRound::default());
        };
        let blocks = self.scan_report_blocks()?;
        let report_seq = self.next_report_seq(&registration.group_name);
        let mut round = BlockReportRound {
            attempted_peers: self.endpoints.len(),
            ..BlockReportRound::default()
        };
        let mut last_error = None;
        let mut accepted_next_delta_seq = 0;

        for endpoint in &self.endpoints {
            let started = Instant::now();
            match self
                .send_full_to_peer(endpoint.clone(), &registration, report_seq, &blocks)
                .await
            {
                Ok(BlockReportPeerOutcome::Accepted { next_delta_seq }) => {
                    let duration = started.elapsed().as_secs_f64();
                    observe::record_metadata_rpc("block_report", "ok", "none", duration);
                    observe::record_block_report_sent("full", "ok", "none", duration);
                    round.accepted_peers += 1;
                    accepted_next_delta_seq = next_delta_seq;
                }
                Ok(BlockReportPeerOutcome::FullReportRequired) => {
                    observe::record_metadata_rpc(
                        "block_report",
                        "error",
                        "full_report_required",
                        started.elapsed().as_secs_f64(),
                    );
                    round.full_report_required = true;
                }
                Ok(BlockReportPeerOutcome::NeedRegister) => {
                    observe::record_metadata_rpc(
                        "block_report",
                        "error",
                        "need_register",
                        started.elapsed().as_secs_f64(),
                    );
                    round.needs_register = true;
                    self.state.mark_needs_register(&registration.group_name);
                    self.reset_baseline(&registration.group_name);
                    break;
                }
                Ok(BlockReportPeerOutcome::WorkerRunMismatch) => {
                    observe::record_metadata_rpc(
                        "block_report",
                        "error",
                        "worker_run_mismatch",
                        started.elapsed().as_secs_f64(),
                    );
                    round.worker_run_mismatch = true;
                    self.state.mark_needs_register(&registration.group_name);
                    self.reset_baseline(&registration.group_name);
                    break;
                }
                Err(error) => {
                    observe::record_metadata_rpc(
                        "block_report",
                        "error",
                        block_report_error_kind(&error),
                        started.elapsed().as_secs_f64(),
                    );
                    debug!(%error, "Worker full block report peer attempt failed");
                    last_error = Some(error);
                }
            }
        }

        if round.accepted_peers > 0 && !round.needs_register && !round.worker_run_mismatch {
            self.publish_baseline(
                &registration.group_name,
                registration_epoch,
                report_seq,
                accepted_next_delta_seq,
                blocks,
            );
        } else if round.attempted_peers > 0
            && !round.full_report_required
            && !round.needs_register
            && !round.worker_run_mismatch
        {
            return Err(
                last_error.unwrap_or_else(|| BlockReportError::Retryable("no block report peer accepted".into()))
            );
        }

        Ok(round)
    }

    /// Sends one delta round only when the current registration owns the
    /// baseline.
    pub async fn send_delta_once(&self) -> Result<BlockReportRound, BlockReportError> {
        let Some((registration, registration_epoch)) = self.ready_registration() else {
            return Ok(BlockReportRound::default());
        };
        let Some((report_seq, delta_seq, deltas)) =
            self.build_delta_batch(&registration.group_name, registration_epoch)?
        else {
            return Ok(BlockReportRound::default());
        };

        let mut round = BlockReportRound {
            attempted_peers: self.endpoints.len(),
            ..BlockReportRound::default()
        };
        let mut last_error = None;
        let mut accepted_next_delta_seq = delta_seq;

        for endpoint in &self.endpoints {
            let started = Instant::now();
            match self
                .send_delta_to_peer(endpoint.clone(), &registration, report_seq, delta_seq, &deltas)
                .await
            {
                Ok(BlockReportPeerOutcome::Accepted { next_delta_seq }) => {
                    let duration = started.elapsed().as_secs_f64();
                    observe::record_metadata_rpc("block_report", "ok", "none", duration);
                    observe::record_block_report_sent("delta", "ok", "none", duration);
                    round.accepted_peers += 1;
                    accepted_next_delta_seq = next_delta_seq;
                }
                Ok(BlockReportPeerOutcome::FullReportRequired) => {
                    observe::record_metadata_rpc(
                        "block_report",
                        "error",
                        "full_report_required",
                        started.elapsed().as_secs_f64(),
                    );
                    round.full_report_required = true;
                    self.reset_baseline(&registration.group_name);
                }
                Ok(BlockReportPeerOutcome::NeedRegister) => {
                    observe::record_metadata_rpc(
                        "block_report",
                        "error",
                        "need_register",
                        started.elapsed().as_secs_f64(),
                    );
                    round.needs_register = true;
                    self.state.mark_needs_register(&registration.group_name);
                    self.reset_baseline(&registration.group_name);
                    break;
                }
                Ok(BlockReportPeerOutcome::WorkerRunMismatch) => {
                    observe::record_metadata_rpc(
                        "block_report",
                        "error",
                        "worker_run_mismatch",
                        started.elapsed().as_secs_f64(),
                    );
                    round.worker_run_mismatch = true;
                    self.state.mark_needs_register(&registration.group_name);
                    self.reset_baseline(&registration.group_name);
                    break;
                }
                Err(error) => {
                    observe::record_metadata_rpc(
                        "block_report",
                        "error",
                        block_report_error_kind(&error),
                        started.elapsed().as_secs_f64(),
                    );
                    debug!(%error, "Worker delta block report peer attempt failed");
                    last_error = Some(error);
                }
            }
        }

        if round.accepted_peers > 0
            && !round.full_report_required
            && !round.needs_register
            && !round.worker_run_mismatch
        {
            self.apply_delta_baseline(&registration.group_name, accepted_next_delta_seq, deltas);
        } else if round.attempted_peers > 0
            && !round.full_report_required
            && !round.needs_register
            && !round.worker_run_mismatch
        {
            return Err(
                last_error.unwrap_or_else(|| BlockReportError::Retryable("no delta report peer accepted".into()))
            );
        }

        Ok(round)
    }

    fn ready_registration(&self) -> Option<(Registration, u64)> {
        self.state.ready_registration(&self.config.group_name)
    }

    /// Builds the local block view used by both full and delta reports.
    ///
    /// Runtime `Reclaiming` entries override the filesystem scan as `Deleting`.
    /// This keeps a block observable after its Ready metadata is removed but
    /// before crash-safe reclamation and lifecycle cleanup have completed.
    fn scan_report_blocks(&self) -> Result<Vec<BlockReportBlockProto>, BlockReportError> {
        let metas = self
            .store
            .scan_group_blocks(&self.config.group_name)
            .map_err(|err| BlockReportError::Retryable(format!("scan local block report group failed: {err}")))?;
        let mut blocks = HashMap::with_capacity(metas.len());
        for meta in metas {
            let block = meta_to_report_block(meta)?;
            let id = block_id(&block).expect("local block report entry has an id");
            blocks.insert(id, block);
        }
        for reclaiming in self.core.reclaiming_blocks(&self.config.group_name) {
            blocks.insert(
                reclaiming.block_id,
                BlockReportBlockProto {
                    block_id: Some(reclaiming.block_id.into()),
                    block_stamp: reclaiming.block_stamp,
                    block_state: BlockReportBlockStateProto::BlockReportBlockStateDeleting as i32,
                    effective_len: 0,
                },
            );
        }
        let mut blocks = blocks.into_values().collect::<Vec<_>>();
        blocks.sort_by_key(|block| {
            let id = block_id(block).expect("local block report entry has an id");
            (id.inode_id.as_raw(), id.index.as_raw())
        });
        Ok(blocks)
    }

    fn next_report_seq(&self, group_name: &GroupName) -> u64 {
        let mut baselines = self.baselines.lock().expect("block report baseline state poisoned");
        let baseline = baselines.entry(group_name.clone()).or_default();
        baseline.report_seq = baseline.report_seq.saturating_add(1).max(1);
        baseline.ready = false;
        baseline.report_seq
    }

    /// Replaces the delta baseline with a Metadata-accepted full-report view.
    fn publish_baseline(
        &self,
        group_name: &GroupName,
        registration_epoch: u64,
        report_seq: u64,
        next_delta_seq: u64,
        blocks: Vec<BlockReportBlockProto>,
    ) {
        let mut baselines = self.baselines.lock().expect("block report baseline state poisoned");
        baselines.insert(
            group_name.clone(),
            ReportBaseline {
                report_seq,
                next_delta_seq,
                registration_epoch,
                blocks: blocks
                    .into_iter()
                    .filter_map(|block| block_id(&block).map(|id| (id, block)))
                    .collect(),
                ready: true,
            },
        );
    }

    /// Diffs the current local view against a baseline from the same
    /// registration lifecycle.
    ///
    /// Returning `None` makes the caller rebuild state with a full report.
    fn build_delta_batch(
        &self,
        group_name: &GroupName,
        registration_epoch: u64,
    ) -> Result<Option<(u64, u64, Vec<BlockReportDeltaProto>)>, BlockReportError> {
        let current = self.scan_report_blocks()?;
        let current: HashMap<BlockId, BlockReportBlockProto> = current
            .into_iter()
            .filter_map(|block| block_id(&block).map(|id| (id, block)))
            .collect();
        let baselines = self.baselines.lock().expect("block report baseline state poisoned");
        let Some(baseline) = baselines
            .get(group_name)
            .filter(|baseline| baseline.ready && baseline.registration_epoch == registration_epoch)
        else {
            return Ok(None);
        };

        let mut deltas = Vec::new();
        for (id, block) in &current {
            if baseline.blocks.get(id) != Some(block) {
                deltas.push(BlockReportDeltaProto {
                    op: BlockReportDeltaOpProto::BlockReportDeltaOpAddUpdate as i32,
                    block: Some(*block),
                });
            }
        }
        for (id, block) in &baseline.blocks {
            if !current.contains_key(id) {
                deltas.push(BlockReportDeltaProto {
                    op: BlockReportDeltaOpProto::BlockReportDeltaOpRemove as i32,
                    block: Some(*block),
                });
            }
        }
        deltas.truncate(self.options.delta_max_entries_per_batch);
        if deltas.is_empty() {
            return Ok(None);
        }
        Ok(Some((baseline.report_seq, baseline.next_delta_seq, deltas)))
    }

    fn apply_delta_baseline(&self, group_name: &GroupName, next_delta_seq: u64, deltas: Vec<BlockReportDeltaProto>) {
        let mut baselines = self.baselines.lock().expect("block report baseline state poisoned");
        let Some(baseline) = baselines.get_mut(group_name) else {
            return;
        };
        for delta in deltas {
            let Some(block) = delta.block else {
                continue;
            };
            let Some(id) = block_id(&block) else {
                continue;
            };
            match delta.op() {
                BlockReportDeltaOpProto::BlockReportDeltaOpAddUpdate => {
                    baseline.blocks.insert(id, block);
                }
                BlockReportDeltaOpProto::BlockReportDeltaOpRemove => {
                    baseline.blocks.remove(&id);
                }
                BlockReportDeltaOpProto::BlockReportDeltaOpUnspecified => {}
            }
        }
        baseline.next_delta_seq = next_delta_seq;
    }

    fn reset_baseline(&self, group_name: &GroupName) {
        if let Some(baseline) = self
            .baselines
            .lock()
            .expect("block report baseline state poisoned")
            .get_mut(group_name)
        {
            baseline.ready = false;
        }
    }

    async fn send_full_to_peer(
        &self,
        endpoint: Endpoint,
        registration: &Registration,
        report_seq: u64,
        blocks: &[BlockReportBlockProto],
    ) -> Result<BlockReportPeerOutcome, BlockReportError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let channel = time::timeout(timeout, endpoint.connect())
            .await
            .map_err(|_| BlockReportError::Retryable("metadata block report connect timed out".to_string()))?
            .map_err(|err| BlockReportError::Retryable(format!("metadata block report endpoint unavailable: {err}")))?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);
        let batch_size = self.options.full_max_blocks_per_batch;
        let total_batches = blocks.len().max(1).div_ceil(batch_size);
        let mut outcome = BlockReportPeerOutcome::Accepted { next_delta_seq: 0 };

        for batch_idx in 0..total_batches {
            let start = batch_idx * batch_size;
            let end = (start + batch_size).min(blocks.len());
            let batch_blocks = if start < end {
                blocks[start..end].to_vec()
            } else {
                Vec::new()
            };
            // Each batch is submitted once here. Any future retry must preserve this op.
            let op = self.control_identity.new_op();
            let request = BlockReportRequestProto {
                header: Some(block_report_request_header(&registration.group_name, &op)),
                worker_id: registration.worker_id.as_raw(),
                worker_run_id: registration.worker_run_id.to_string(),
                report_seq,
                report: Some(block_report_request_proto::Report::Full(FullBlockReportBatchProto {
                    batch_seq: batch_idx as u64,
                    final_batch: batch_idx + 1 == total_batches,
                    blocks: batch_blocks,
                })),
            };
            let tonic_request = metadata_tonic_request(request.clone(), request.header.as_ref());
            let response = time::timeout(timeout, client.block_report(tonic_request))
                .await
                .map_err(|_| BlockReportError::Retryable("metadata full block report timed out".to_string()))?
                .map_err(classify_status)?
                .into_inner();
            outcome = classify_block_report_response(&request, response)?;
            if !matches!(outcome, BlockReportPeerOutcome::Accepted { .. }) {
                return Ok(outcome);
            }
        }

        Ok(outcome)
    }

    async fn send_delta_to_peer(
        &self,
        endpoint: Endpoint,
        registration: &Registration,
        report_seq: u64,
        delta_seq: u64,
        deltas: &[BlockReportDeltaProto],
    ) -> Result<BlockReportPeerOutcome, BlockReportError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let channel = time::timeout(timeout, endpoint.connect())
            .await
            .map_err(|_| BlockReportError::Retryable("metadata delta report connect timed out".to_string()))?
            .map_err(|err| BlockReportError::Retryable(format!("metadata delta report endpoint unavailable: {err}")))?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);
        // The delta RPC is submitted once here. If retry is added, reuse this op across attempts.
        let op = self.control_identity.new_op();
        let request = BlockReportRequestProto {
            header: Some(block_report_request_header(&registration.group_name, &op)),
            worker_id: registration.worker_id.as_raw(),
            worker_run_id: registration.worker_run_id.to_string(),
            report_seq,
            report: Some(block_report_request_proto::Report::Delta(DeltaBlockReportProto {
                delta_seq,
                deltas: deltas.to_vec(),
            })),
        };
        let tonic_request = metadata_tonic_request(request.clone(), request.header.as_ref());
        let response = time::timeout(timeout, client.block_report(tonic_request))
            .await
            .map_err(|_| BlockReportError::Retryable("metadata delta block report timed out".to_string()))?
            .map_err(classify_status)?
            .into_inner();
        classify_block_report_response(&request, response)
    }

    /// Runs the event-driven reporter with periodic retry and full-report recovery.
    ///
    /// Every wake-up re-evaluates baseline validity; an invalid or missing
    /// baseline always selects a full report instead of silently skipping work.
    async fn run(self, shutdown: CancellationToken) {
        let mut interval = time::interval(self.interval);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = interval.tick() => {}
                _ = self.store.wait_for_block_report_change() => {}
                _ = self.core.wait_for_block_report_change() => {}
            }
            let report = async {
                if self.has_delta_baseline(&self.config.group_name) {
                    match self.send_delta_once().await {
                        Ok(round) if round.full_report_required => {
                            if let Err(error) = self.send_full_once().await {
                                warn!(%error, "Worker full block report recovery failed");
                            }
                        }
                        Ok(_) => {}
                        Err(error) => warn!(%error, "Worker delta block report round failed"),
                    }
                } else {
                    match self.send_full_once().await {
                        Ok(_) => {}
                        Err(error) => warn!(%error, "Worker full block report round failed"),
                    }
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

enum BlockReportPeerOutcome {
    Accepted { next_delta_seq: u64 },
    FullReportRequired,
    NeedRegister,
    WorkerRunMismatch,
}

fn meta_to_report_block(meta: BlockMetaPayload) -> Result<BlockReportBlockProto, BlockReportError> {
    let block_state = match meta.visibility.block_state {
        BlockState::Ready => BlockReportBlockStateProto::BlockReportBlockStateReady,
        BlockState::Corrupt => BlockReportBlockStateProto::BlockReportBlockStateCorrupt,
        BlockState::Loading => {
            return Err(BlockReportError::Fatal(
                "loading block metadata is not valid for block report".to_string(),
            ));
        }
    };
    let block_id = meta.identity.block_id;
    Ok(BlockReportBlockProto {
        block_id: Some(block_id.into()),
        block_stamp: meta.visibility.block_stamp,
        block_state: block_state as i32,
        effective_len: meta.source.effective_len,
    })
}

fn block_id(block: &BlockReportBlockProto) -> Option<BlockId> {
    block.block_id.map(|block_id| {
        BlockId::try_from(block_id).unwrap_or_else(|error| panic!("stored BlockId must be valid: {error}"))
    })
}

fn block_report_error_kind(error: &BlockReportError) -> &'static str {
    match error {
        BlockReportError::InvalidConfig(_) => "invalid_config",
        BlockReportError::Retryable(_) => "retryable",
        BlockReportError::Fatal(_) => "fatal",
    }
}

fn classify_block_report_response(
    request: &BlockReportRequestProto,
    response: BlockReportResponseProto,
) -> Result<BlockReportPeerOutcome, BlockReportError> {
    let response_group_name = response
        .header
        .as_ref()
        .map(|header| header.group_name.as_str())
        .ok_or_else(|| BlockReportError::Fatal("metadata block report response missing ResponseHeader".to_string()))?;
    let request_group_name = request
        .header
        .as_ref()
        .map(|header| header.group_name.as_str())
        .ok_or_else(|| BlockReportError::Fatal("metadata block report request missing RequestHeader".to_string()))?;
    if response_group_name != request_group_name {
        return Err(BlockReportError::Fatal(format!(
            "metadata block report response confirmed group_name {response_group_name}, expected {request_group_name}"
        )));
    }
    if let Some(outcome) = classify_header(response.header.as_ref())? {
        return Ok(outcome);
    }
    if response.report_seq != request.report_seq {
        return Err(BlockReportError::Fatal(format!(
            "metadata block report response confirmed report_seq {}, expected {}",
            response.report_seq, request.report_seq
        )));
    }
    Ok(BlockReportPeerOutcome::Accepted {
        next_delta_seq: response.next_delta_seq,
    })
}

fn classify_header(
    header: Option<&beryl_proto::common::ResponseHeaderProto>,
) -> Result<Option<BlockReportPeerOutcome>, BlockReportError> {
    let header = header
        .ok_or_else(|| BlockReportError::Fatal("metadata block report response missing ResponseHeader".to_string()))?;
    let Some(error) = header.error.as_ref() else {
        return Ok(None);
    };
    classify_rpc_error(rpc_error_from_proto(error)).map(Some)
}

fn classify_rpc_error(error: RpcErrorDetail) -> Result<BlockReportPeerOutcome, BlockReportError> {
    match error.recovery {
        RecoveryAction::SendFullBlockReport => Ok(BlockReportPeerOutcome::FullReportRequired),
        RecoveryAction::RegisterWorker if error.kind == ErrorKind::Worker(WorkerErrorKind::RunMismatch) => {
            Ok(BlockReportPeerOutcome::WorkerRunMismatch)
        }
        RecoveryAction::RegisterWorker => Ok(BlockReportPeerOutcome::NeedRegister),
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

fn block_report_request_header(group_name: &GroupName, op: &ControlOp) -> RequestHeaderProto {
    let mut header = RequestHeader::new(op.client_id).with_group_name(group_name.clone());
    header.client.call_id = op.call_id;
    (&header).into()
}
