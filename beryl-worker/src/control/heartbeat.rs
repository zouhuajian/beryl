// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-to-metadata heartbeat reporting.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use beryl_common::error::rpc::{ErrorKind, RecoveryAction, RpcErrorDetail, WorkerErrorKind};
use beryl_proto::common::EndpointProto;
use beryl_proto::convert::{require_worker_run_id, rpc_error_from_proto};
use beryl_proto::metadata::metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient;
use beryl_proto::metadata::{CapacityInfoProto, HeartbeatRequestProto, HeartbeatResponseProto, TierFreeProto};
use beryl_types::BlockId;
use thiserror::Error;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use tonic::Code;
use tracing::{debug, info, warn};

use crate::config::WorkerRegistrationConfig;
use crate::control::{
    BlockCleanupExecutor, ControlIdentity, ControlOp, MetadataRegistrar, Registration, RegistrationDescriptor,
    RegistrationState,
};
use crate::observe;
use crate::store::dirs::{StoreDirs, StoreReport};

#[derive(Debug, Error)]
pub enum HeartbeatError {
    #[error("invalid worker metadata heartbeat config: {0}")]
    InvalidConfig(String),
    #[error("retryable metadata heartbeat error: {0}")]
    Retryable(String),
    #[error("fatal metadata heartbeat error: {0}")]
    Fatal(String),
}

/// Result of one heartbeat submission to the configured Metadata leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeartbeatOutcome {
    Skipped,
    Accepted,
    NeedRegister,
    WorkerRunMismatch,
}

/// Heartbeat sender for one registered metadata group.
pub struct MetadataHeartbeatLoop {
    config: WorkerRegistrationConfig,
    advertised_endpoint: EndpointProto,
    state: Arc<RegistrationState>,
    endpoint: Endpoint,
    control_identity: ControlIdentity,
    heartbeat_seq: Mutex<u64>,
    cleanup: BlockCleanupExecutor,
    interval: Duration,
}

impl MetadataHeartbeatLoop {
    /// Builds a heartbeat loop whose accepted cleanup commands use `cleanup`.
    ///
    /// Endpoint and registration configuration is validated before any
    /// background task or RPC is started.
    pub fn new(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationState>,
        cleanup: BlockCleanupExecutor,
        interval: Duration,
    ) -> Result<Self, HeartbeatError> {
        if interval.is_zero() {
            return Err(HeartbeatError::InvalidConfig(
                "heartbeat interval must be greater than zero".to_string(),
            ));
        }
        let endpoint = config
            .validate()
            .map_err(|err| HeartbeatError::InvalidConfig(err.message))?;
        Ok(Self {
            config,
            advertised_endpoint: EndpointProto {
                host: descriptor.endpoint_host,
                port: descriptor.endpoint_port,
            },
            state,
            endpoint,
            control_identity: ControlIdentity::new_local(),
            heartbeat_seq: Mutex::new(0),
            cleanup,
            interval,
        })
    }

    /// Starts store-backed heartbeat reporting under the process shutdown token.
    pub fn spawn_with_registrar_and_store_until_shutdown(
        self,
        registrar: Arc<MetadataRegistrar>,
        store: Arc<StoreDirs>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(registrar, store, shutdown).await })
    }

    /// Sends one heartbeat round and enqueues commands from accepted responses.
    ///
    /// A response must confirm the requested group, worker, and worker run.
    /// Cleanup commands are parsed as one batch, so a malformed command rejects
    /// the response without partially enqueueing destructive work.
    pub async fn send_once(&self, report: &StoreReport) -> Result<HeartbeatOutcome, HeartbeatError> {
        let Some(registration) = self.state.registration(&self.config.group_name) else {
            return Ok(HeartbeatOutcome::Skipped);
        };
        let seq = self.next_heartbeat_seq();
        let op = self.control_identity.new_op();
        let request = self.build_request(&registration, &op, seq, report);
        let started = Instant::now();
        match self.send(&registration, request).await {
            Ok(HeartbeatReply::Accepted {
                liveness_timeout,
                cleanup_commands,
            }) => {
                let duration = started.elapsed().as_secs_f64();
                observe::record_metadata_rpc("heartbeat", "ok", "none", duration);
                observe::record_heartbeat_sent("ok", "none");
                self.state
                    .record_heartbeat_success(&registration.group_name, liveness_timeout);
                self.cleanup.enqueue(&registration, cleanup_commands);
                Ok(HeartbeatOutcome::Accepted)
            }
            Ok(HeartbeatReply::NeedRegister) => {
                observe::record_metadata_rpc("heartbeat", "error", "need_register", started.elapsed().as_secs_f64());
                self.state.mark_needs_register(&registration.group_name);
                Ok(HeartbeatOutcome::NeedRegister)
            }
            Ok(HeartbeatReply::WorkerRunMismatch) => {
                observe::record_metadata_rpc(
                    "heartbeat",
                    "error",
                    "worker_run_mismatch",
                    started.elapsed().as_secs_f64(),
                );
                self.state.mark_needs_register(&registration.group_name);
                Ok(HeartbeatOutcome::WorkerRunMismatch)
            }
            Err(error) => {
                observe::record_metadata_rpc(
                    "heartbeat",
                    "error",
                    heartbeat_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                debug!(%error, "Worker heartbeat endpoint attempt failed");
                Err(error)
            }
        }
    }

    fn build_request(
        &self,
        registration: &Registration,
        op: &ControlOp,
        heartbeat_seq: u64,
        report: &StoreReport,
    ) -> HeartbeatRequestProto {
        HeartbeatRequestProto {
            header: Some(op.request_header(&registration.group_name)),
            worker_id: registration.worker_id.as_raw(),
            worker_run_id: registration.worker_run_id.to_string(),
            heartbeat_seq,
            advertised_endpoint: Some(self.advertised_endpoint.clone()),
            capacity: Some(CapacityInfoProto {
                tier_free: report
                    .tier_free
                    .iter()
                    .map(|entry| TierFreeProto {
                        tier: beryl_proto::common::TierProto::from(entry.tier) as i32,
                        free_bytes: entry.free_bytes,
                    })
                    .collect(),
            }),
        }
    }

    fn next_heartbeat_seq(&self) -> u64 {
        let mut seq = self.heartbeat_seq.lock().expect("heartbeat seq state poisoned");
        *seq = seq.saturating_add(1);
        *seq
    }

    async fn send(
        &self,
        registration: &Registration,
        request: HeartbeatRequestProto,
    ) -> Result<HeartbeatReply, HeartbeatError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let channel = time::timeout(timeout, self.endpoint.connect())
            .await
            .map_err(|_| HeartbeatError::Retryable("metadata heartbeat connect timed out".to_string()))?
            .map_err(|err| HeartbeatError::Retryable(format!("metadata heartbeat endpoint unavailable: {err}")))?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);
        let tonic_request = tonic::Request::new(request);
        let response = time::timeout(timeout, client.heartbeat(tonic_request))
            .await
            .map_err(|_| HeartbeatError::Retryable("metadata heartbeat request timed out".to_string()))?
            .map_err(classify_status)?
            .into_inner();
        classify_heartbeat_response(registration, response)
    }

    async fn run(self, registrar: Arc<MetadataRegistrar>, store: Arc<StoreDirs>, shutdown: CancellationToken) {
        let mut interval = time::interval(self.interval);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = interval.tick() => {}
            }
            if self.state.registration(&self.config.group_name).is_none() {
                match registrar.register_with_retry(shutdown.clone().cancelled_owned()).await {
                    Ok(registration) => {
                        info!(
                            group_name = %registration.group_name,
                            worker_id = registration.worker_id.as_raw(),
                            worker_run_id = %registration.worker_run_id,
                            "Worker re-registered after heartbeat requested registration"
                        );
                    }
                    Err(error) => {
                        if shutdown.is_cancelled() {
                            return;
                        }
                        warn!(%error, "Worker metadata re-registration failed in heartbeat loop");
                        continue;
                    }
                }
            }

            let report = store.report();
            observe::record_store_report(&report);

            let heartbeat = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                result = self.send_once(&report) => result,
            };
            match heartbeat {
                Ok(HeartbeatOutcome::NeedRegister) => {
                    warn!("Metadata heartbeat requested worker registration");
                }
                Ok(HeartbeatOutcome::WorkerRunMismatch) => {
                    warn!("Metadata heartbeat reported worker_run_id mismatch");
                }
                Ok(_) => {}
                Err(error) => warn!(%error, "Worker heartbeat round failed"),
            }
        }
    }
}

enum HeartbeatReply {
    Accepted {
        liveness_timeout: Duration,
        cleanup_commands: Vec<BlockId>,
    },
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

/// Authenticates heartbeat response identity and decodes its command batch.
///
/// All commands are validated before an accepted outcome is returned. Callers
/// therefore never execute a valid prefix from an otherwise malformed batch.
fn classify_heartbeat_response(
    registration: &Registration,
    response: HeartbeatResponseProto,
) -> Result<HeartbeatReply, HeartbeatError> {
    let header = response
        .header
        .as_ref()
        .ok_or_else(|| HeartbeatError::Fatal("metadata heartbeat response missing ResponseHeader".to_string()))?;
    let response_group_name = header.group_name.as_str();
    let request_group_name = registration.group_name.as_str();
    if response_group_name != request_group_name {
        return Err(HeartbeatError::Fatal(format!(
            "metadata heartbeat response confirmed group_name {response_group_name}, expected {request_group_name}"
        )));
    }
    if let Some(error) = header.error.as_ref() {
        return classify_rpc_error(rpc_error_from_proto(error));
    }
    if response.worker_id != registration.worker_id.as_raw() {
        return Err(HeartbeatError::Fatal(
            "metadata heartbeat response did not confirm worker_id".to_string(),
        ));
    }
    let accepted_worker_run_id = require_worker_run_id(
        &response.accepted_worker_run_id,
        "HeartbeatResponse.accepted_worker_run_id",
    )
    .map_err(HeartbeatError::Fatal)?;
    if accepted_worker_run_id != registration.worker_run_id {
        return Err(HeartbeatError::Fatal(
            "metadata heartbeat response did not confirm worker_run_id".to_string(),
        ));
    }
    let cleanup_commands = response
        .cleanup_commands
        .into_iter()
        .map(|block_id| BlockId::try_from(block_id).map_err(HeartbeatError::Fatal))
        .collect::<Result<Vec<_>, _>>()?;
    let liveness_timeout = Duration::from_millis(u64::from(response.liveness_timeout_ms.max(1)));
    Ok(HeartbeatReply::Accepted {
        liveness_timeout,
        cleanup_commands,
    })
}

fn classify_rpc_error(error: RpcErrorDetail) -> Result<HeartbeatReply, HeartbeatError> {
    match error.recovery {
        RecoveryAction::RegisterWorker if error.kind == ErrorKind::Worker(WorkerErrorKind::RunMismatch) => {
            Ok(HeartbeatReply::WorkerRunMismatch)
        }
        RecoveryAction::RegisterWorker => Ok(HeartbeatReply::NeedRegister),
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
