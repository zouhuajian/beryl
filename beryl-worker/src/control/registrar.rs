// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! MetadataWorkerService registrar used during worker startup.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use beryl_common::error::rpc::{RecoveryAction, RpcErrorDetail};
use beryl_proto::common::EndpointProto;
use beryl_proto::convert::{require_worker_run_id, rpc_error_from_proto};
use beryl_proto::metadata::metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient;
use beryl_proto::metadata::{RegisterWorkerRequestProto, RegisterWorkerResponseProto};
use beryl_types::{GroupName, WorkerId, WorkerRunId};
use thiserror::Error;
use tokio::time;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;
use tracing::{info, warn};

use crate::config::{WorkerConfig, WorkerRegistrationConfig};
use crate::control::{ControlIdentity, ControlOp, Registration, RegistrationState};

/// Worker descriptor sent to metadata during startup registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistrationDescriptor {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub endpoint_host: String,
    pub endpoint_port: u32,
}

#[derive(Debug, Error)]
pub enum RegistrationError {
    #[error("invalid worker metadata registration config: {0}")]
    InvalidConfig(String),
    #[error("retryable metadata registration error: {0}")]
    Retryable(String),
    #[error("fatal metadata registration error: {0}")]
    Fatal(String),
    #[error("metadata registration was cancelled before worker became ready")]
    Cancelled,
}

/// Startup registrar for MetadataWorkerService.RegisterWorker.
pub struct MetadataRegistrar {
    config: WorkerRegistrationConfig,
    descriptor: RegistrationDescriptor,
    state: Arc<RegistrationState>,
    endpoint: Endpoint,
    control_identity: ControlIdentity,
}

impl MetadataRegistrar {
    pub fn new(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationState>,
    ) -> Result<Self, RegistrationError> {
        let endpoint = config
            .validate()
            .map_err(|err| RegistrationError::InvalidConfig(err.message))?;
        Ok(Self {
            config,
            descriptor,
            state,
            endpoint,
            control_identity: ControlIdentity::new_local(),
        })
    }

    pub fn descriptor_from_config(config: &WorkerConfig, worker_id: WorkerId) -> RegistrationDescriptor {
        RegistrationDescriptor {
            group_name: config.metadata.group_name.clone(),
            worker_id,
            worker_run_id: WorkerRunId::new(),
            endpoint_host: config.host.clone(),
            endpoint_port: u32::from(config.rpc_port),
        }
    }

    pub async fn register_once(&self) -> Result<Registration, RegistrationError> {
        let op = self.control_identity.new_op();
        self.register_once_with_op(&op).await
    }

    async fn register_once_with_op(&self, op: &ControlOp) -> Result<Registration, RegistrationError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let channel = self.connect(timeout).await?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);
        let request = self.build_request(op);
        let tonic_request = tonic::Request::new(request);
        let response = time::timeout(timeout, client.register_worker(tonic_request))
            .await
            .map_err(|_| RegistrationError::Retryable("metadata register request timed out".to_string()))?
            .map_err(classify_status)?
            .into_inner();

        let registration = self.registration_from_response(response)?;
        self.state.record_registered(registration.clone());
        Ok(registration)
    }

    pub async fn register_with_retry<S>(&self, shutdown: S) -> Result<Registration, RegistrationError>
    where
        S: Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        let mut backoff = Duration::from_millis(self.config.retry_initial_backoff_ms);
        let max_backoff = Duration::from_millis(self.config.retry_max_backoff_ms);
        let op = self.control_identity.new_op();
        loop {
            match self.register_once_with_op(&op).await {
                Ok(registration) => {
                    info!(
                        group_name = %registration.group_name,
                        worker_id = registration.worker_id.as_raw(),
                        worker_run_id = %registration.worker_run_id,
                        "Worker registered with metadata"
                    );
                    return Ok(registration);
                }
                Err(RegistrationError::Retryable(message)) => {
                    warn!(
                        error = %message,
                        backoff_ms = backoff.as_millis() as u64,
                        "Worker metadata registration failed; retrying"
                    );
                    tokio::select! {
                        _ = time::sleep(backoff) => {}
                        _ = &mut shutdown => return Err(RegistrationError::Cancelled),
                    }
                    backoff = (backoff * 2).min(max_backoff);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn connect(&self, timeout: Duration) -> Result<Channel, RegistrationError> {
        time::timeout(timeout, self.endpoint.clone().connect())
            .await
            .map_err(|_| RegistrationError::Retryable("metadata endpoint connect timed out".to_string()))?
            .map_err(|err| RegistrationError::Retryable(format!("metadata endpoint unavailable: {err}")))
    }

    fn build_request(&self, op: &ControlOp) -> RegisterWorkerRequestProto {
        RegisterWorkerRequestProto {
            header: Some(op.request_header(&self.descriptor.group_name)),
            worker_id: self.descriptor.worker_id.as_raw(),
            worker_run_id: self.descriptor.worker_run_id.to_string(),
            advertised_endpoint: Some(EndpointProto {
                host: self.descriptor.endpoint_host.clone(),
                port: self.descriptor.endpoint_port,
            }),
        }
    }

    fn registration_from_response(
        &self,
        response: RegisterWorkerResponseProto,
    ) -> Result<Registration, RegistrationError> {
        let header = response
            .header
            .as_ref()
            .ok_or_else(|| RegistrationError::Fatal("metadata register response missing ResponseHeader".to_string()))?;
        if header.group_name != self.descriptor.group_name.as_str() {
            return Err(RegistrationError::Fatal(format!(
                "metadata register response confirmed group_name {}, expected {}",
                header.group_name, self.descriptor.group_name
            )));
        }
        if let Some(error) = header.error.as_ref() {
            return Err(classify_rpc_error(rpc_error_from_proto(error)));
        }
        if response.worker_id != self.descriptor.worker_id.as_raw() {
            return Err(RegistrationError::Fatal(
                "metadata register response did not confirm worker_id".to_string(),
            ));
        }
        let accepted_worker_run_id = require_worker_run_id(
            &response.accepted_worker_run_id,
            "RegisterWorkerResponse.accepted_worker_run_id",
        )
        .map_err(RegistrationError::Fatal)?;
        if accepted_worker_run_id != self.descriptor.worker_run_id {
            return Err(RegistrationError::Fatal(
                "metadata register response did not confirm worker_run_id".to_string(),
            ));
        };

        Ok(Registration {
            group_name: self.descriptor.group_name.clone(),
            worker_id: self.descriptor.worker_id,
            worker_run_id: accepted_worker_run_id,
        })
    }
}

fn classify_rpc_error(error: RpcErrorDetail) -> RegistrationError {
    match error.recovery {
        RecoveryAction::Retry { .. } | RecoveryAction::RefreshMetadata { .. } | RecoveryAction::RegisterWorker => {
            RegistrationError::Retryable(error.message)
        }
        RecoveryAction::Fail | RecoveryAction::ReopenWriteSession { .. } | RecoveryAction::SendFullBlockReport => {
            RegistrationError::Fatal(format!("fatal metadata registration error: {}", error.message))
        }
    }
}

fn classify_status(status: tonic::Status) -> RegistrationError {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted => {
            RegistrationError::Retryable(status.to_string())
        }
        _ => RegistrationError::Fatal(format!("metadata register RPC failed: {status}")),
    }
}
