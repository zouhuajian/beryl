// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataWorkerService registrar used during worker startup.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use common::error::canonical::{CanonicalError, ErrorClass};
use common::header::RequestHeader;
use proto::common::{EndpointProto, RequestHeaderProto, WorkerNetProtocolProto};
use proto::convert::error_detail_to_canonical;
use proto::metadata::metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient;
use proto::metadata::{RegisterWorkerRequestProto, RegisterWorkerResponseProto};
use thiserror::Error;
use tokio::time;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;
use tracing::{info, warn};
use types::{ClientId, GroupName, WorkerId, WorkerRunId};

use crate::config::{WorkerConfig, WorkerRegistrationConfig};
use crate::control::{Registration, RegistrationSet};
use crate::net::protocol::WorkerNetProtocol;

/// Worker descriptor sent to metadata during startup registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistrationDescriptor {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub endpoint_host: String,
    pub endpoint_port: u32,
    pub advertised_endpoint: String,
    pub worker_net_protocol: WorkerNetProtocol,
    pub version: String,
    pub capabilities: u64,
    pub labels: BTreeMap<String, String>,
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
    state: Arc<RegistrationSet>,
    endpoint: Endpoint,
}

impl MetadataRegistrar {
    pub fn new(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
    ) -> Result<Self, RegistrationError> {
        config
            .validate()
            .map_err(|err| RegistrationError::InvalidConfig(err.message))?;
        let registration_endpoint = config
            .endpoints
            .first()
            .ok_or_else(|| RegistrationError::InvalidConfig("worker.metadata.endpoints must not be empty".into()))?
            .clone();
        let endpoint = Endpoint::from_shared(registration_endpoint)
            .map_err(|err| RegistrationError::InvalidConfig(format!("worker.metadata.endpoints: {err}")))?;
        Ok(Self {
            config,
            descriptor,
            state,
            endpoint,
        })
    }

    pub fn descriptor_from_config(
        config: &WorkerConfig,
        worker_id: WorkerId,
    ) -> Result<RegistrationDescriptor, RegistrationError> {
        let listener = config
            .net
            .listeners
            .iter()
            .find(|listener| listener.protocol == WorkerNetProtocol::Grpc)
            .ok_or_else(|| {
                RegistrationError::InvalidConfig("worker registration requires a gRPC data listener".into())
            })?;
        let (endpoint_host, endpoint_port) = config
            .rpc_advertised_endpoint_parts()
            .map_err(|err| RegistrationError::InvalidConfig(err.message))?;

        Ok(RegistrationDescriptor {
            group_name: config.metadata.group_name.clone(),
            worker_id,
            worker_run_id: WorkerRunId::new(),
            endpoint_host,
            endpoint_port,
            advertised_endpoint: config.rpc_advertised_endpoint.clone(),
            worker_net_protocol: listener.protocol,
            version: env!("CARGO_PKG_VERSION").to_string(),
            capabilities: 0,
            labels: BTreeMap::new(),
        })
    }

    pub async fn register_once(&self) -> Result<Registration, RegistrationError> {
        let timeout = Duration::from_millis(self.config.register_timeout_ms);
        let channel = self.connect(timeout).await?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);
        let request = self.build_request();
        let response = time::timeout(timeout, client.register_worker(tonic::Request::new(request)))
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
        let mut backoff = Duration::from_millis(self.config.register_retry_initial_backoff_ms);
        let max_backoff = Duration::from_millis(self.config.register_retry_max_backoff_ms);

        loop {
            match self.register_once().await {
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

    fn build_request(&self) -> RegisterWorkerRequestProto {
        RegisterWorkerRequestProto {
            header: Some(registration_request_header(&self.descriptor.group_name)),
            worker_id: self.descriptor.worker_id.as_raw(),
            worker_run_id: self.descriptor.worker_run_id.to_string(),
            advertised_endpoint: Some(EndpointProto {
                host: self.descriptor.endpoint_host.clone(),
                port: self.descriptor.endpoint_port,
                protocol: self.descriptor.worker_net_protocol.to_string(),
            }),
            capabilities: self.descriptor.capabilities,
            version: self.descriptor.version.clone(),
            labels: self.descriptor.labels.clone().into_iter().collect::<HashMap<_, _>>(),
            worker_net_protocol: worker_protocol_to_proto(self.descriptor.worker_net_protocol) as i32,
            group_name: self.descriptor.group_name.to_string(),
        }
    }

    fn registration_from_response(
        &self,
        response: RegisterWorkerResponseProto,
    ) -> Result<Registration, RegistrationError> {
        classify_header(response.header)?;
        if response.group_name != self.descriptor.group_name.as_str() {
            return Err(RegistrationError::Fatal(format!(
                "metadata register response confirmed group_name {}, expected {}",
                response.group_name, self.descriptor.group_name
            )));
        }
        if response.worker_id != self.descriptor.worker_id.as_raw() {
            return Err(RegistrationError::Fatal(
                "metadata register response did not confirm worker_id".to_string(),
            ));
        }
        let accepted_worker_run_id = response.accepted_worker_run_id.parse::<WorkerRunId>().map_err(|err| {
            RegistrationError::Fatal(format!(
                "metadata register response accepted_worker_run_id is malformed: {err}"
            ))
        })?;
        if accepted_worker_run_id != self.descriptor.worker_run_id {
            return Err(RegistrationError::Fatal(
                "metadata register response did not confirm worker_run_id".to_string(),
            ));
        };

        Ok(Registration {
            group_name: self.descriptor.group_name.clone(),
            worker_id: self.descriptor.worker_id,
            worker_run_id: accepted_worker_run_id,
            advertised_endpoint: self.descriptor.advertised_endpoint.clone(),
        })
    }
}

fn registration_request_header(group_name: &GroupName) -> RequestHeaderProto {
    let client_id = u64::from(std::process::id()).max(1);
    let header = RequestHeader::new(ClientId::new(client_id)).with_group_name(group_name.clone());
    (&header).into()
}

fn worker_protocol_to_proto(protocol: WorkerNetProtocol) -> WorkerNetProtocolProto {
    match protocol {
        WorkerNetProtocol::Grpc => WorkerNetProtocolProto::WorkerNetProtocolGrpc,
        WorkerNetProtocol::Quic => WorkerNetProtocolProto::WorkerNetProtocolQuic,
        WorkerNetProtocol::Rdma => WorkerNetProtocolProto::WorkerNetProtocolRdma,
    }
}

fn classify_header(header: Option<proto::common::ResponseHeaderProto>) -> Result<(), RegistrationError> {
    let header = header
        .ok_or_else(|| RegistrationError::Fatal("metadata register response missing ResponseHeader".to_string()))?;
    let Some(error) = header.error.as_ref() else {
        return Ok(());
    };
    classify_canonical_error(error_detail_to_canonical(error))
}

fn classify_canonical_error(error: CanonicalError) -> Result<(), RegistrationError> {
    match error.class {
        ErrorClass::Ok => Err(RegistrationError::Fatal(
            "metadata register response contained malformed OK error detail".to_string(),
        )),
        ErrorClass::Retryable | ErrorClass::NeedRefresh => Err(RegistrationError::Retryable(error.message)),
        ErrorClass::Fatal => Err(RegistrationError::Fatal(format!(
            "fatal metadata registration error: {}",
            error.message
        ))),
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
