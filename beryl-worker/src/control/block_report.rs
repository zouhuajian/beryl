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
use tonic::transport::Endpoint;
use tonic::Code;
use tracing::{debug, warn};

use crate::config::WorkerRegistrationConfig;
use crate::control::{
    metadata_tonic_request, ControlIdentity, ControlOp, Registration, RegistrationDescriptor, RegistrationSet,
};
use crate::observe;
use crate::store::block::{BlockMetaPayload, BlockState};
use crate::store::dirs::StoreDirs;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockReportOptions {
    pub full_max_blocks_per_batch: usize,
    pub delta_max_entries_per_batch: usize,
}

impl Default for BlockReportOptions {
    fn default() -> Self {
        Self {
            full_max_blocks_per_batch: 1_000,
            delta_max_entries_per_batch: 1_000,
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

#[derive(Clone, Debug, Default)]
struct ReportBaseline {
    report_seq: u64,
    next_delta_seq: u64,
    blocks: HashMap<BlockId, BlockReportBlockProto>,
    ready: bool,
}

/// Sends full and delta block reports for one registered metadata group.
pub struct MetadataBlockReportLoop {
    config: WorkerRegistrationConfig,
    _descriptor: RegistrationDescriptor,
    state: Arc<RegistrationSet>,
    endpoints: Vec<Endpoint>,
    store: Arc<StoreDirs>,
    options: BlockReportOptions,
    control_identity: ControlIdentity,
    baselines: Mutex<HashMap<GroupName, ReportBaseline>>,
}

impl MetadataBlockReportLoop {
    pub fn new(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
        store: Arc<StoreDirs>,
    ) -> Result<Self, BlockReportError> {
        Self::with_options(config, descriptor, state, store, BlockReportOptions::default())
    }

    pub fn with_options(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
        store: Arc<StoreDirs>,
        options: BlockReportOptions,
    ) -> Result<Self, BlockReportError> {
        config
            .validate()
            .map_err(|err| BlockReportError::InvalidConfig(err.message))?;
        if options.full_max_blocks_per_batch == 0 {
            return Err(BlockReportError::InvalidConfig(
                "full_max_blocks_per_batch must be greater than zero".to_string(),
            ));
        }
        if options.delta_max_entries_per_batch == 0 {
            return Err(BlockReportError::InvalidConfig(
                "delta_max_entries_per_batch must be greater than zero".to_string(),
            ));
        }

        let mut endpoints = Vec::with_capacity(config.endpoints.len());
        for endpoint in &config.endpoints {
            endpoints.push(
                Endpoint::from_shared(endpoint.clone())
                    .map_err(|err| BlockReportError::InvalidConfig(format!("worker.metadata.endpoints: {err}")))?,
            );
        }

        Ok(Self {
            config,
            _descriptor: descriptor,
            state,
            endpoints,
            store,
            options,
            control_identity: ControlIdentity::new_local(),
            baselines: Mutex::new(HashMap::new()),
        })
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    pub fn has_delta_baseline(&self, group_name: &GroupName) -> bool {
        self.baselines
            .lock()
            .expect("block report baseline state poisoned")
            .get(group_name)
            .map(|baseline| baseline.ready)
            .unwrap_or(false)
    }

    pub async fn send_full_once(&self) -> Result<BlockReportRound, BlockReportError> {
        let Some(registration) = self.ready_registration() else {
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
            self.publish_baseline(&registration.group_name, report_seq, accepted_next_delta_seq, blocks);
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

    pub async fn send_delta_once(&self) -> Result<BlockReportRound, BlockReportError> {
        let Some(registration) = self.ready_registration() else {
            return Ok(BlockReportRound::default());
        };
        let Some((report_seq, delta_seq, deltas)) = self.build_delta_batch(&registration.group_name)? else {
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

    fn ready_registration(&self) -> Option<Registration> {
        let registration = self.state.registration(&self.config.group_name)?;
        self.state.is_ready(&registration.group_name).then_some(registration)
    }

    fn scan_report_blocks(&self) -> Result<Vec<BlockReportBlockProto>, BlockReportError> {
        let metas = self
            .store
            .scan_group_blocks(&self.config.group_name)
            .map_err(|err| BlockReportError::Retryable(format!("scan local block report group failed: {err}")))?;
        let mut blocks = Vec::with_capacity(metas.len());
        for meta in metas {
            blocks.push(meta_to_report_block(meta)?);
        }
        Ok(blocks)
    }

    fn next_report_seq(&self, group_name: &GroupName) -> u64 {
        let mut baselines = self.baselines.lock().expect("block report baseline state poisoned");
        let baseline = baselines.entry(group_name.clone()).or_default();
        baseline.report_seq = baseline.report_seq.saturating_add(1).max(1);
        baseline.ready = false;
        baseline.report_seq
    }

    fn publish_baseline(
        &self,
        group_name: &GroupName,
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
                blocks: blocks
                    .into_iter()
                    .filter_map(|block| block_id(&block).map(|id| (id, block)))
                    .collect(),
                ready: true,
            },
        );
    }

    fn build_delta_batch(
        &self,
        group_name: &GroupName,
    ) -> Result<Option<(u64, u64, Vec<BlockReportDeltaProto>)>, BlockReportError> {
        let current = self.scan_report_blocks()?;
        let current: HashMap<BlockId, BlockReportBlockProto> = current
            .into_iter()
            .filter_map(|block| block_id(&block).map(|id| (id, block)))
            .collect();
        let baselines = self.baselines.lock().expect("block report baseline state poisoned");
        let Some(baseline) = baselines.get(group_name).filter(|baseline| baseline.ready) else {
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
        let timeout = Duration::from_millis(self.config.register_timeout_ms);
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
        let timeout = Duration::from_millis(self.config.register_timeout_ms);
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

    async fn run(self) {
        let mut interval = time::interval(Duration::from_millis(1_000));
        loop {
            interval.tick().await;
            match self.send_full_once().await {
                Ok(round) if round.accepted_peers > 0 => break,
                Ok(_) => {}
                Err(error) => warn!(%error, "Worker full block report round failed"),
            }
        }

        loop {
            interval.tick().await;
            match self.send_delta_once().await {
                Ok(round) if round.full_report_required => {
                    if let Err(error) = self.send_full_once().await {
                        warn!(%error, "Worker full block report recovery failed");
                    }
                }
                Ok(_) => {}
                Err(error) => warn!(%error, "Worker delta block report round failed"),
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
    })
}

fn block_id(block: &BlockReportBlockProto) -> Option<BlockId> {
    block.block_id.map(|block_id| {
        BlockId::try_from(block_id).unwrap_or_else(|()| unreachable!("BlockIdProto conversion is infallible"))
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
