// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker-to-metadata heartbeat fanout.

use std::collections::HashMap;
use std::future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode, RefreshReason};
use common::header::RequestHeader;
use common::header::RpcErrorCode;
use proto::common::{EndpointProto, RequestHeaderProto};
use proto::convert::error_detail_to_canonical;
use proto::metadata::metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient;
use proto::metadata::{
    CapacityInfoProto, HealthStatusProto, HeartbeatRequestProto, HeartbeatResponseProto, LoadInfoProto,
    MetadataServerRoleProto,
};
use thiserror::Error;
use tokio::time;
use tonic::transport::Endpoint;
use tonic::Code;
use tracing::{debug, info, warn};
use types::{ClientId, GroupName, WorkerRunId};

use crate::config::WorkerRegistrationConfig;
use crate::control::{MetadataRegistrar, Registration, RegistrationDescriptor, RegistrationSet};
use crate::net::protocol::WorkerNetProtocol;

/// Lightweight local resource snapshot sent on heartbeat.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeartbeatSnapshot {
    pub capacity_total_bytes: u64,
    pub capacity_used_bytes: u64,
    pub capacity_available_bytes: u64,
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
            heartbeat_seq: Mutex::new(HashMap::new()),
        })
    }

    pub fn spawn_with_registrar(self, registrar: Arc<MetadataRegistrar>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(registrar).await })
    }

    pub async fn send_once(&self, snapshot: HeartbeatSnapshot) -> Result<HeartbeatRound, HeartbeatError> {
        let Some(registration) = self.state.registration(&self.config.group_name) else {
            return Ok(HeartbeatRound::default());
        };
        let seq = self.next_heartbeat_seq(&registration);
        let request = self.build_request(&registration, seq, &snapshot);
        let mut round = HeartbeatRound {
            attempted_peers: self.endpoints.len(),
            ..HeartbeatRound::default()
        };
        let mut last_error = None;

        for endpoint in &self.endpoints {
            match self.send_to_peer(endpoint.clone(), request.clone()).await {
                Ok(HeartbeatPeerOutcome::Accepted { liveness_timeout }) => {
                    round.accepted_peers += 1;
                    self.state
                        .record_heartbeat_success(&registration.group_name, liveness_timeout);
                }
                Ok(HeartbeatPeerOutcome::NeedRegister) => {
                    round.needs_register = true;
                    self.state.mark_needs_register(&registration.group_name);
                }
                Ok(HeartbeatPeerOutcome::WorkerRunMismatch) => {
                    round.worker_run_mismatch = true;
                    self.state.mark_needs_register(&registration.group_name);
                }
                Err(error) => {
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
        heartbeat_seq: u64,
        snapshot: &HeartbeatSnapshot,
    ) -> HeartbeatRequestProto {
        HeartbeatRequestProto {
            header: Some(heartbeat_request_header()),
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
        let response = time::timeout(timeout, client.heartbeat(tonic::Request::new(request.clone())))
            .await
            .map_err(|_| HeartbeatError::Retryable("metadata heartbeat request timed out".to_string()))?
            .map_err(classify_status)?
            .into_inner();
        classify_heartbeat_response(&request, response)
    }

    async fn run(self, registrar: Arc<MetadataRegistrar>) {
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

            match self.send_once(HeartbeatSnapshot::default()).await {
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

enum HeartbeatPeerOutcome {
    Accepted { liveness_timeout: Duration },
    NeedRegister,
    WorkerRunMismatch,
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
    if response.accepted_worker_run_id != request.worker_run_id {
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
    classify_canonical_error(error_detail_to_canonical(error)).map(Some)
}

fn classify_canonical_error(error: CanonicalError) -> Result<HeartbeatPeerOutcome, HeartbeatError> {
    match error.reason {
        Some(RefreshReason::NeedRegister) => return Ok(HeartbeatPeerOutcome::NeedRegister),
        Some(RefreshReason::WorkerRunMismatch) => return Ok(HeartbeatPeerOutcome::WorkerRunMismatch),
        _ => {}
    }

    match error.class {
        ErrorClass::Ok => Err(HeartbeatError::Fatal(
            "metadata heartbeat response contained malformed OK error detail".to_string(),
        )),
        ErrorClass::NeedRefresh
            if matches!(
                error.code,
                Some(ErrorCode::RpcCode(
                    RpcErrorCode::WorkerNotRegistered | RpcErrorCode::WorkerDescriptorMismatch
                ))
            ) =>
        {
            Ok(HeartbeatPeerOutcome::NeedRegister)
        }
        ErrorClass::NeedRefresh if matches!(error.code, Some(ErrorCode::RpcCode(RpcErrorCode::WorkerRunMismatch))) => {
            Ok(HeartbeatPeerOutcome::WorkerRunMismatch)
        }
        ErrorClass::Retryable | ErrorClass::NeedRefresh => Err(HeartbeatError::Retryable(error.message)),
        ErrorClass::Fatal => Err(HeartbeatError::Fatal(format!(
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

fn heartbeat_request_header() -> RequestHeaderProto {
    let client_id = u64::from(std::process::id()).max(1);
    let header = RequestHeader::new(ClientId::new(client_id));
    (&header).into()
}

fn worker_protocol_to_proto(protocol: WorkerNetProtocol) -> proto::common::WorkerNetProtocolProto {
    match protocol {
        WorkerNetProtocol::Grpc => proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc,
        WorkerNetProtocol::Quic => proto::common::WorkerNetProtocolProto::WorkerNetProtocolQuic,
        WorkerNetProtocol::Rdma => proto::common::WorkerNetProtocolProto::WorkerNetProtocolRdma,
    }
}
