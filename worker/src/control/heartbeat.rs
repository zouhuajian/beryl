// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker-to-metadata heartbeat fanout.

use std::collections::HashMap;
use std::future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::error::rpc::{ErrorKind, RecoveryAction, RpcErrorDetail, WorkerErrorKind};
use common::header::RequestHeader;
use proto::common::{EndpointProto, RequestHeaderProto};
use proto::convert::{require_worker_run_id, rpc_error_from_proto};
use proto::metadata::metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient;
use proto::metadata::{
    CapacityInfoProto, HealthStatusProto, HeartbeatRequestProto, HeartbeatResponseProto, LoadInfoProto,
    MetadataServerRoleProto, TierFreeProto,
};
use thiserror::Error;
use tokio::time;
use tonic::transport::Endpoint;
use tonic::Code;
use tracing::{debug, info, warn};
use types::{GroupName, TierFree, WorkerRunId};

use crate::config::WorkerRegistrationConfig;
use crate::control::{
    metadata_tonic_request, ControlIdentity, ControlOp, MetadataRegistrar, Registration, RegistrationDescriptor,
    RegistrationSet,
};
use crate::net::protocol::WorkerNetProtocol;
use crate::observe;
use crate::store::dirs::{StoreDirs, StoreReport};

/// Lightweight local resource snapshot sent on heartbeat.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeartbeatSnapshot {
    pub capacity_total_bytes: u64,
    pub capacity_used_bytes: u64,
    pub capacity_available_bytes: u64,
    pub tier_free: Vec<TierFree>,
    pub active_reads: u32,
    pub active_writes: u32,
    pub cpu_usage_percent: u32,
    pub memory_used_bytes: u64,
}

#[derive(Debug, Error)]
pub enum HeartbeatError {
    #[error("invalid worker metadata heartbeat config: {0}")]
    InvalidConfig(String),
    #[error("retryable metadata heartbeat error: {0}")]
    Retryable(String),
    #[error("fatal metadata heartbeat error: {0}")]
    Fatal(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HeartbeatRound {
    pub attempted_peers: usize,
    pub accepted_peers: usize,
    pub needs_register: bool,
    pub worker_run_mismatch: bool,
}

/// Heartbeat sender for one registered metadata group.
pub struct MetadataHeartbeatLoop {
    config: WorkerRegistrationConfig,
    descriptor: RegistrationDescriptor,
    state: Arc<RegistrationSet>,
    endpoints: Vec<Endpoint>,
    control_identity: ControlIdentity,
    heartbeat_seq: Mutex<HashMap<(GroupName, WorkerRunId), u64>>,
}

impl MetadataHeartbeatLoop {
    pub fn new(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
    ) -> Result<Self, HeartbeatError> {
        config
            .validate()
            .map_err(|err| HeartbeatError::InvalidConfig(err.message))?;
        let mut endpoints = Vec::with_capacity(config.endpoints.len());
        for endpoint in &config.endpoints {
            endpoints.push(
                Endpoint::from_shared(endpoint.clone())
                    .map_err(|err| HeartbeatError::InvalidConfig(format!("worker.metadata.endpoints: {err}")))?,
            );
        }
        Ok(Self {
            config,
            descriptor,
            state,
            endpoints,
            control_identity: ControlIdentity::new_local(),
            heartbeat_seq: Mutex::new(HashMap::new()),
        })
    }

    pub fn spawn_with_registrar(self, registrar: Arc<MetadataRegistrar>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(registrar, None).await })
    }

    pub fn spawn_with_registrar_and_store(
        self,
        registrar: Arc<MetadataRegistrar>,
        store: Arc<StoreDirs>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(registrar, Some(store)).await })
    }

    pub async fn send_once(&self, snapshot: HeartbeatSnapshot) -> Result<HeartbeatRound, HeartbeatError> {
        let Some(registration) = self.state.registration(&self.config.group_name) else {
            return Ok(HeartbeatRound::default());
        };
        let seq = self.next_heartbeat_seq(&registration);
        let op = self.control_identity.new_op();
        let request = self.build_request(&registration, &op, seq, &snapshot);
        let mut round = HeartbeatRound {
            attempted_peers: self.endpoints.len(),
            ..HeartbeatRound::default()
        };
        let mut last_error = None;

        for endpoint in &self.endpoints {
            let started = Instant::now();
            match self.send_to_peer(endpoint.clone(), request.clone()).await {
                Ok(HeartbeatPeerOutcome::Accepted { liveness_timeout }) => {
                    let duration = started.elapsed().as_secs_f64();
                    observe::record_metadata_rpc("heartbeat", "ok", "none", duration);
                    observe::record_heartbeat_sent("ok", "none");
                    round.accepted_peers += 1;
                    self.state
                        .record_heartbeat_success(&registration.group_name, liveness_timeout);
                }
                Ok(HeartbeatPeerOutcome::NeedRegister) => {
                    observe::record_metadata_rpc(
                        "heartbeat",
                        "error",
                        "need_register",
                        started.elapsed().as_secs_f64(),
                    );
                    round.needs_register = true;
                    self.state.mark_needs_register(&registration.group_name);
                }
                Ok(HeartbeatPeerOutcome::WorkerRunMismatch) => {
                    observe::record_metadata_rpc(
                        "heartbeat",
                        "error",
                        "worker_run_mismatch",
                        started.elapsed().as_secs_f64(),
                    );
                    round.worker_run_mismatch = true;
                    self.state.mark_needs_register(&registration.group_name);
                }
                Err(error) => {
                    observe::record_metadata_rpc(
                        "heartbeat",
                        "error",
                        heartbeat_error_kind(&error),
                        started.elapsed().as_secs_f64(),
                    );
                    debug!(%error, "Worker heartbeat peer attempt failed");
                    last_error = Some(error);
                }
            }
        }

        if round.accepted_peers == 0 && !round.needs_register && !round.worker_run_mismatch && round.attempted_peers > 0
        {
            return Err(last_error.unwrap_or_else(|| HeartbeatError::Retryable("no heartbeat peer accepted".into())));
        }

        Ok(round)
    }

    fn build_request(
        &self,
        registration: &Registration,
        op: &ControlOp,
        heartbeat_seq: u64,
        snapshot: &HeartbeatSnapshot,
    ) -> HeartbeatRequestProto {
        HeartbeatRequestProto {
            header: Some(heartbeat_request_header(&registration.group_name, op)),
            worker_id: registration.worker_id.as_raw(),
            worker_run_id: registration.worker_run_id.to_string(),
            heartbeat_seq,
            advertised_endpoint: Some(EndpointProto {
                host: self.descriptor.endpoint_host.clone(),
                port: self.descriptor.endpoint_port,
                protocol: self.descriptor.worker_net_protocol.to_string(),
            }),
            capacity: Some(CapacityInfoProto {
                total_bytes: snapshot.capacity_total_bytes,
                used_bytes: snapshot.capacity_used_bytes,
                available_bytes: snapshot.capacity_available_bytes,
                tier_free: snapshot
                    .tier_free
                    .iter()
                    .map(|entry| TierFreeProto {
                        tier: proto::common::TierProto::from(entry.tier) as i32,
                        free_bytes: entry.free_bytes,
                    })
                    .collect(),
            }),
            load: Some(LoadInfoProto {
                active_reads: snapshot.active_reads,
                active_writes: snapshot.active_writes,
                cpu_usage_percent: snapshot.cpu_usage_percent,
                memory_used_bytes: snapshot.memory_used_bytes,
            }),
            health: HealthStatusProto::HealthStatusHealthy as i32,
            worker_net_protocol: worker_protocol_to_proto(self.descriptor.worker_net_protocol) as i32,
            acks: Vec::new(),
            group_name: registration.group_name.to_string(),
        }
    }

    fn next_heartbeat_seq(&self, registration: &Registration) -> u64 {
        let mut seqs = self.heartbeat_seq.lock().expect("heartbeat seq state poisoned");
        let entry = seqs
            .entry((registration.group_name.clone(), registration.worker_run_id))
            .or_insert(0);
        *entry = entry.saturating_add(1);
        *entry
    }

    async fn send_to_peer(
        &self,
        endpoint: Endpoint,
        request: HeartbeatRequestProto,
    ) -> Result<HeartbeatPeerOutcome, HeartbeatError> {
        let timeout = Duration::from_millis(self.config.register_timeout_ms);
        let channel = time::timeout(timeout, endpoint.connect())
            .await
            .map_err(|_| HeartbeatError::Retryable("metadata heartbeat connect timed out".to_string()))?
            .map_err(|err| HeartbeatError::Retryable(format!("metadata heartbeat endpoint unavailable: {err}")))?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);
        let tonic_request = metadata_tonic_request(request.clone(), request.header.as_ref());
        let response = time::timeout(timeout, client.heartbeat(tonic_request))
            .await
            .map_err(|_| HeartbeatError::Retryable("metadata heartbeat request timed out".to_string()))?
            .map_err(classify_status)?
            .into_inner();
        classify_heartbeat_response(&request, response)
    }

    async fn run(self, registrar: Arc<MetadataRegistrar>, store: Option<Arc<StoreDirs>>) {
        let mut interval = time::interval(Duration::from_millis(1_000));
        loop {
            interval.tick().await;
            if self.state.registration(&self.config.group_name).is_none() {
                match registrar.register_with_retry(future::pending::<()>()).await {
                    Ok(registration) => {
                        info!(
                            group_name = %registration.group_name,
                            worker_id = registration.worker_id.as_raw(),
                            worker_run_id = %registration.worker_run_id,
                            "Worker re-registered after heartbeat requested registration"
                        );
                    }
                    Err(error) => {
                        warn!(%error, "Worker metadata re-registration failed in heartbeat loop");
                        continue;
                    }
                }
            }

            let snapshot = match store.as_ref() {
                Some(store) => match store.report() {
                    Ok(report) => {
                        observe::record_store_report(&report);
                        HeartbeatSnapshot::from(report)
                    }
                    Err(error) => {
                        warn!(%error, "Worker store report failed before heartbeat");
                        continue;
                    }
                },
                None => HeartbeatSnapshot::default(),
            };

            match self.send_once(snapshot).await {
                Ok(round) if round.needs_register => {
                    warn!("Metadata heartbeat requested worker registration");
                }
                Ok(round) if round.worker_run_mismatch => {
                    warn!("Metadata heartbeat reported worker_run_id mismatch");
                }
                Ok(_) => {}
                Err(error) => warn!(%error, "Worker heartbeat round failed"),
            }
        }
    }
}

impl From<StoreReport> for HeartbeatSnapshot {
    fn from(report: StoreReport) -> Self {
        Self {
            capacity_total_bytes: report.total_bytes,
            capacity_used_bytes: report.used_bytes,
            capacity_available_bytes: report.free_bytes,
            tier_free: report.tier_free,
            ..Self::default()
        }
    }
}

enum HeartbeatPeerOutcome {
    Accepted { liveness_timeout: Duration },
    NeedRegister,
    WorkerRunMismatch,
}

fn heartbeat_error_kind(error: &HeartbeatError) -> &'static str {
    match error {
        HeartbeatError::InvalidConfig(_) => "invalid_config",
        HeartbeatError::Retryable(_) => "retryable",
        HeartbeatError::Fatal(_) => "fatal",
    }
}

fn classify_heartbeat_response(
    request: &HeartbeatRequestProto,
    response: HeartbeatResponseProto,
) -> Result<HeartbeatPeerOutcome, HeartbeatError> {
    if let Some(outcome) = classify_header(response.header.as_ref())? {
        return Ok(outcome);
    }
    if response.group_name != request.group_name {
        return Err(HeartbeatError::Fatal(format!(
            "metadata heartbeat response confirmed group_name {}, expected {}",
            response.group_name, request.group_name
        )));
    }
    if response.worker_id != request.worker_id {
        return Err(HeartbeatError::Fatal(
            "metadata heartbeat response did not confirm worker_id".to_string(),
        ));
    }
    let accepted_worker_run_id = require_worker_run_id(
        &response.accepted_worker_run_id,
        "HeartbeatResponse.accepted_worker_run_id",
    )
    .map_err(HeartbeatError::Fatal)?;
    let expected_worker_run_id = require_worker_run_id(&request.worker_run_id, "HeartbeatRequest.worker_run_id")
        .map_err(HeartbeatError::Fatal)?;
    if !accepted_worker_run_id.matches(expected_worker_run_id) {
        return Err(HeartbeatError::Fatal(
            "metadata heartbeat response did not confirm worker_run_id".to_string(),
        ));
    }
    if response.server_role() == MetadataServerRoleProto::MetadataServerRoleFollower && !response.commands.is_empty() {
        warn!("Ignoring worker commands returned by follower heartbeat response");
    }
    let liveness_timeout = Duration::from_millis(u64::from(response.liveness_timeout_ms.max(1)));
    Ok(HeartbeatPeerOutcome::Accepted { liveness_timeout })
}

fn classify_header(
    header: Option<&proto::common::ResponseHeaderProto>,
) -> Result<Option<HeartbeatPeerOutcome>, HeartbeatError> {
    let header = header
        .ok_or_else(|| HeartbeatError::Fatal("metadata heartbeat response missing ResponseHeader".to_string()))?;
    let Some(error) = header.error.as_ref() else {
        return Ok(None);
    };
    classify_rpc_error(rpc_error_from_proto(error)).map(Some)
}

fn classify_rpc_error(error: RpcErrorDetail) -> Result<HeartbeatPeerOutcome, HeartbeatError> {
    match error.recovery {
        RecoveryAction::RegisterWorker if error.kind == ErrorKind::Worker(WorkerErrorKind::RunMismatch) => {
            Ok(HeartbeatPeerOutcome::WorkerRunMismatch)
        }
        RecoveryAction::RegisterWorker => Ok(HeartbeatPeerOutcome::NeedRegister),
        RecoveryAction::Retry { .. } | RecoveryAction::RefreshMetadata { .. } | RecoveryAction::SendFullBlockReport => {
            Err(HeartbeatError::Retryable(error.message))
        }
        RecoveryAction::Fail | RecoveryAction::ReopenWriteSession { .. } => Err(HeartbeatError::Fatal(format!(
            "fatal metadata heartbeat error: {}",
            error.message
        ))),
    }
}

fn classify_status(status: tonic::Status) -> HeartbeatError {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted => {
            HeartbeatError::Retryable(status.to_string())
        }
        _ => HeartbeatError::Fatal(format!("metadata heartbeat RPC failed: {status}")),
    }
}

fn heartbeat_request_header(group_name: &GroupName, op: &ControlOp) -> RequestHeaderProto {
    let mut header = RequestHeader::new(op.client_id).with_group_name(group_name.clone());
    header.client.call_id = op.call_id;
    (&header).into()
}

fn worker_protocol_to_proto(protocol: WorkerNetProtocol) -> proto::common::WorkerNetProtocolProto {
    match protocol {
        WorkerNetProtocol::Grpc => proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc,
    }
}
