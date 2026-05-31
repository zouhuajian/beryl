// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataWorkerService implementation.

use super::manager::{
    BlockReportBlock, BlockReportBlockState, BlockReportDeltaEntry, BlockReportDeltaOp, WorkerManager,
};
use super::metrics::WorkerMetrics;
use crate::error::{to_canonical_rpc, MetadataError, MetadataResult};
use crate::raft::Command;
use crate::raft::{AppDataResponse, AppRaftNode, WorkerCommandResult};
use crate::service::extract_and_inject_context;
use ::common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use ::common::header::{ResponseHeader, RpcErrorCode, RpcStatus};
use proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProto;
use proto::metadata::*;
use std::net::IpAddr;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};
use types::{BlockId, GroupName, WorkerId, WorkerRunId};

/// Worker service background task handles.
pub struct WorkerBackgroundHandle {}

impl WorkerBackgroundHandle {
    pub fn task_count(&self) -> usize {
        0
    }
}

trait WorkerServiceResponse {
    fn set_header(&mut self, header: proto::common::ResponseHeaderProto);
}

macro_rules! impl_worker_service_response {
    ($($resp_ty:ty),+ $(,)?) => {
        $(
            impl WorkerServiceResponse for $resp_ty {
                fn set_header(&mut self, header: proto::common::ResponseHeaderProto) {
                    self.header = Some(header);
                }
            }
        )+
    };
}

impl_worker_service_response!(
    RegisterWorkerResponseProto,
    HeartbeatResponseProto,
    BlockReportResponseProto,
);

/// MetadataWorkerService implementation.
pub struct MetadataWorkerServiceImpl {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    metrics: Arc<WorkerMetrics>,
    slot_metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
    /// Mount table used to compute mount_epoch for lease gating.
    _mount_table: Arc<crate::mount::MountTable>,
    served_group_name: GroupName,
}

impl MetadataWorkerServiceImpl {
    pub fn new(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        mount_table: Arc<crate::mount::MountTable>,
        served_group_name: GroupName,
    ) -> Self {
        let metrics = Arc::new(WorkerMetrics::new());

        Self {
            raft_node,
            worker_manager,
            metrics,
            slot_metrics: None, // Will be set via set_slot_metrics
            _mount_table: mount_table,
            served_group_name,
        }
    }

    /// Set slot metrics (called after metrics are available).
    pub(crate) fn set_slot_metrics(&mut self, metrics: Arc<crate::metrics::MetadataMetrics>) {
        self.slot_metrics = Some(metrics);
    }

    /// Helper: create a response header from request header with group name.
    fn create_response_header_from_request(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        group_name: Option<&GroupName>,
    ) -> ::common::header::ResponseHeader {
        let client = req_header
            .as_ref()
            .and_then(|h| h.client.as_ref())
            .and_then(|c| ::common::header::ClientInfo::try_from(c.clone()).ok())
            .unwrap_or_else(|| ::common::header::ClientInfo::new(types::ClientId::new(0)));
        let mut header = ResponseHeader::ok(client);
        if let Some(group_name) = group_name {
            header = header.with_group_name(group_name.clone());
        }
        if self.raft_node.is_leader() {
            if let (Some(group_name), Some(sid)) = (group_name, self.raft_node.get_last_applied_state_id()) {
                header = header.with_state(vec![types::GroupStateWatermark::new(group_name.clone(), sid)]);
            }
        }
        header
    }

    fn group_name_from_request_header(req_header: &Option<proto::common::RequestHeaderProto>) -> Option<GroupName> {
        req_header
            .as_ref()
            .and_then(|header| (!header.group_name.is_empty()).then_some(header.group_name.as_str()))
            .and_then(|group_name| GroupName::parse(group_name).ok())
    }

    fn error_response_header_from_request(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        error: CanonicalError,
    ) -> proto::common::ResponseHeaderProto {
        debug_assert!(
            error.class != ErrorClass::Ok,
            "metadata worker error response must carry a non-OK canonical error"
        );
        let mut header = self
            .create_response_header_from_request(req_header, Self::group_name_from_request_header(req_header).as_ref());
        header.status = match error.class {
            ErrorClass::Ok => RpcStatus::Ok,
            ErrorClass::NeedRefresh | ErrorClass::Retryable => RpcStatus::Error,
            ErrorClass::Fatal => RpcStatus::Fatal,
        };
        header.canonical_error = Some(error);
        (&header).into()
    }

    fn response_with_error<T>(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        error: CanonicalError,
    ) -> Result<Response<T>, Status>
    where
        T: Default + WorkerServiceResponse,
    {
        let mut response = T::default();
        response.set_header(self.error_response_header_from_request(req_header, error));
        Ok(Response::new(response))
    }

    fn invalid_request_response<T>(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status>
    where
        T: Default + WorkerServiceResponse,
    {
        self.response_with_error(
            req_header,
            to_canonical_rpc(MetadataError::InvalidArgument(message.into())),
        )
    }

    fn metadata_error_response<T>(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        error: MetadataError,
    ) -> Result<Response<T>, Status>
    where
        T: Default + WorkerServiceResponse,
    {
        self.response_with_error(req_header, to_canonical_rpc(error))
    }

    fn group_mismatch_response<T>(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status>
    where
        T: Default + WorkerServiceResponse,
    {
        self.response_with_error(
            req_header,
            CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::InvalidArgument)),
                reason: Some(RefreshReason::GroupMismatch),
                retry_after_ms: None,
                message: message.into(),
                refresh_hint: None,
            },
        )
    }

    fn need_register_response<T>(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status>
    where
        T: Default + WorkerServiceResponse,
    {
        self.response_with_error(
            req_header,
            CanonicalError::need_refresh(RpcErrorCode::WorkerNotRegistered, RefreshReason::NeedRegister, message),
        )
    }

    fn worker_run_mismatch_response<T>(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status>
    where
        T: Default + WorkerServiceResponse,
    {
        self.response_with_error(
            req_header,
            CanonicalError::need_refresh(
                RpcErrorCode::WorkerRunMismatch,
                RefreshReason::WorkerRunMismatch,
                message,
            ),
        )
    }

    fn worker_descriptor_mismatch_response<T>(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status>
    where
        T: Default + WorkerServiceResponse,
    {
        self.response_with_error(
            req_header,
            CanonicalError::need_refresh(
                RpcErrorCode::WorkerDescriptorMismatch,
                RefreshReason::NeedRegister,
                message,
            ),
        )
    }

    /// Start worker-local background tasks.
    pub(crate) fn start_background_tasks(&self) -> WorkerBackgroundHandle {
        WorkerBackgroundHandle {}
    }

    fn heartbeat_interval_ms(&self) -> u32 {
        1_000
    }

    fn liveness_timeout_ms(&self) -> u32 {
        self.worker_manager
            .heartbeat_timeout_sec()
            .saturating_mul(1000)
            .try_into()
            .unwrap_or(u32::MAX)
    }

    fn server_role(&self) -> MetadataServerRoleProto {
        if self.raft_node.is_leader() {
            MetadataServerRoleProto::MetadataServerRoleLeader
        } else {
            MetadataServerRoleProto::MetadataServerRoleFollower
        }
    }

    fn leader_hint(&self) -> Option<proto::common::EndpointProto> {
        let leader_id = self.raft_node.get_leader_id()?;
        let membership = self.raft_node.get_membership()?;
        let (_, node) = membership.nodes().find(|(node_id, _)| **node_id == leader_id)?;
        parse_metadata_endpoint(&node.address)
    }

    fn full_report_required_response<T>(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status>
    where
        T: Default + WorkerServiceResponse,
    {
        self.response_with_error(
            req_header,
            CanonicalError::need_refresh(
                RpcErrorCode::FullReportRequired,
                RefreshReason::FullReportRequired,
                message,
            ),
        )
    }

    fn proto_to_report_block(block: BlockReportBlockProto) -> MetadataResult<BlockReportBlock> {
        let block_id_proto = block
            .block_id
            .ok_or_else(|| MetadataError::InvalidArgument("block report entry missing block_id".to_string()))?;
        let block_id = BlockId::try_from(block_id_proto)
            .unwrap_or_else(|()| unreachable!("BlockIdProto conversion is infallible"));
        let block_state = match block.block_state() {
            BlockReportBlockStateProto::BlockReportBlockStateReady => BlockReportBlockState::Ready,
            BlockReportBlockStateProto::BlockReportBlockStatePartial => BlockReportBlockState::Partial,
            BlockReportBlockStateProto::BlockReportBlockStateCorrupt => BlockReportBlockState::Corrupt,
            BlockReportBlockStateProto::BlockReportBlockStateDeleting => BlockReportBlockState::Deleting,
            BlockReportBlockStateProto::BlockReportBlockStateUnspecified => {
                return Err(MetadataError::InvalidArgument(
                    "block report entry block_state must be specified".to_string(),
                ));
            }
        };
        Ok(BlockReportBlock {
            block_id,
            data_handle_id: block.data_handle_id,
            block_index: block.block_index,
            block_stamp: block.block_stamp,
            effective_len: block.effective_len,
            committed_length: block.committed_length,
            block_state,
        })
    }

    fn proto_to_delta(delta: BlockReportDeltaProto) -> MetadataResult<BlockReportDeltaEntry> {
        let block = delta
            .block
            .ok_or_else(|| MetadataError::InvalidArgument("block report delta missing block".to_string()))?;
        let op = match delta.op() {
            BlockReportDeltaOpProto::BlockReportDeltaOpAddUpdate => BlockReportDeltaOp::AddUpdate,
            BlockReportDeltaOpProto::BlockReportDeltaOpRemove => BlockReportDeltaOp::Remove,
            BlockReportDeltaOpProto::BlockReportDeltaOpUnspecified => {
                return Err(MetadataError::InvalidArgument(
                    "block report delta op must be specified".to_string(),
                ));
            }
        };
        Ok(BlockReportDeltaEntry {
            op,
            block: Self::proto_to_report_block(block)?,
        })
    }

    fn map_report_error(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        error: MetadataError,
    ) -> Result<Response<BlockReportResponseProto>, Status> {
        match error {
            MetadataError::NotFound(message) => {
                self.need_register_response::<BlockReportResponseProto>(req_header, message)
            }
            MetadataError::StaleState(message) => {
                self.worker_run_mismatch_response::<BlockReportResponseProto>(req_header, message)
            }
            MetadataError::FullReportRequired(message) => {
                self.full_report_required_response::<BlockReportResponseProto>(req_header, message)
            }
            other => self.metadata_error_response::<BlockReportResponseProto>(req_header, other),
        }
    }
}

fn validate_advertised_endpoint(endpoint: proto::common::EndpointProto) -> Result<String, String> {
    if endpoint.protocol != "grpc" {
        return Err("advertised_endpoint protocol must be grpc".to_string());
    }
    if endpoint.host.trim().is_empty() {
        return Err("advertised_endpoint host must not be empty".to_string());
    }
    if endpoint.port == 0 || endpoint.port > u32::from(u16::MAX) {
        return Err("advertised_endpoint port must be between 1 and 65535".to_string());
    }
    if endpoint
        .host
        .parse::<IpAddr>()
        .is_ok_and(|address| address.is_unspecified())
    {
        return Err("advertised_endpoint must not use a wildcard host".to_string());
    }
    Ok(format!("{}:{}", endpoint.host, endpoint.port))
}

fn parse_metadata_endpoint(address: &str) -> Option<proto::common::EndpointProto> {
    let without_scheme = address
        .strip_prefix("http://")
        .or_else(|| address.strip_prefix("https://"))
        .unwrap_or(address);
    let (host, port) = without_scheme.rsplit_once(':')?;
    let port = port.parse::<u32>().ok()?;
    Some(proto::common::EndpointProto {
        host: host.trim_matches(['[', ']']).to_string(),
        port,
        protocol: "grpc".to_string(),
    })
}

async fn persist_worker_descriptor(
    persist_descriptor: impl std::future::Future<Output = MetadataResult<AppDataResponse>>,
) -> MetadataResult<WorkerId> {
    match persist_descriptor.await? {
        AppDataResponse::Worker(WorkerCommandResult::Upserted(worker_id)) => Ok(worker_id),
        other => Err(MetadataError::Internal(format!(
            "RegisterWorker returned unexpected Raft response: {:?}",
            other
        ))),
    }
}

#[tonic::async_trait]
impl MetadataWorkerServiceProto for MetadataWorkerServiceImpl {
    #[instrument(skip(self), fields(call_id, client_id))]
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequestProto>,
    ) -> Result<Response<RegisterWorkerResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        if !self.raft_node.is_leader() {
            return self.metadata_error_response::<RegisterWorkerResponseProto>(
                &req.header,
                MetadataError::LeaderChanged("worker registration must be sent to the metadata group leader".into()),
            );
        }

        let group_name = match GroupName::parse(&req.group_name) {
            Ok(group_name) => group_name,
            Err(error) => {
                return self.invalid_request_response::<RegisterWorkerResponseProto>(
                    &req.header,
                    format!("group_name is invalid: {error}"),
                )
            }
        };
        if group_name != self.served_group_name {
            return self.invalid_request_response::<RegisterWorkerResponseProto>(
                &req.header,
                format!(
                    "register group_name {} does not match served metadata group {}",
                    group_name, self.served_group_name
                ),
            );
        }
        let worker_id = WorkerId::new(req.worker_id);
        if worker_id.as_raw() == 0 {
            return self
                .invalid_request_response::<RegisterWorkerResponseProto>(&req.header, "worker_id must be non-zero");
        }
        let worker_run_id = match req.worker_run_id.parse::<WorkerRunId>() {
            Ok(worker_run_id) => worker_run_id,
            Err(error) => {
                return self.invalid_request_response::<RegisterWorkerResponseProto>(
                    &req.header,
                    format!("worker_run_id must be a UUID: {error}"),
                )
            }
        };
        let worker_net_protocol = req.worker_net_protocol() as i32;
        if req.worker_net_protocol() != proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc {
            return self.invalid_request_response::<RegisterWorkerResponseProto>(
                &req.header,
                "worker_net_protocol must be gRPC for startup registration",
            );
        }
        let _labels = req.labels;
        let endpoint = match req.advertised_endpoint {
            Some(endpoint) => endpoint,
            None => {
                return self.invalid_request_response::<RegisterWorkerResponseProto>(
                    &req.header,
                    "Missing advertised_endpoint",
                );
            }
        };
        let address = match validate_advertised_endpoint(endpoint) {
            Ok(address) => address,
            Err(message) => return self.invalid_request_response::<RegisterWorkerResponseProto>(&req.header, message),
        };
        if let Err(error) = self
            .worker_manager
            .validate_worker_run_registration(&group_name, worker_id, worker_run_id)
        {
            return self.metadata_error_response::<RegisterWorkerResponseProto>(&req.header, error);
        }

        let command = Command::RegisterWorker {
            dedup: crate::raft::DedupKey::new(_caller_ctx.client.client_id, _caller_ctx.client.call_id),
            group_name: group_name.clone(),
            worker_id,
            worker_run_id,
            address: address.clone(),
            worker_net_protocol,
            fault_domain: None, // TODO: Extract fault_domain from labels
        };

        let accepted_worker_id = match persist_worker_descriptor(self.raft_node.propose(command)).await {
            Ok(worker_id) => worker_id,
            Err(error) => return self.metadata_error_response::<RegisterWorkerResponseProto>(&req.header, error),
        };
        if accepted_worker_id != worker_id {
            return self.metadata_error_response::<RegisterWorkerResponseProto>(
                &req.header,
                MetadataError::Internal(format!(
                    "RegisterWorker returned worker_id {}, expected {}",
                    accepted_worker_id.as_raw(),
                    worker_id.as_raw()
                )),
            );
        }

        info!(
            group_name = %group_name,
            worker_id = accepted_worker_id.as_raw(),
            worker_run_id = %worker_run_id,
            "Worker registered"
        );

        Ok(Response::new(RegisterWorkerResponseProto {
            header: Some((&self.create_response_header_from_request(&req.header, Some(&group_name))).into()),
            worker_id: accepted_worker_id.as_raw(),
            accepted_worker_run_id: worker_run_id.to_string(),
            group_name: group_name.to_string(),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequestProto>,
    ) -> Result<Response<HeartbeatResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        let group_name = match GroupName::parse(&req.group_name) {
            Ok(group_name) => group_name,
            Err(error) => {
                return self.invalid_request_response::<HeartbeatResponseProto>(
                    &req.header,
                    format!("group_name is invalid: {error}"),
                )
            }
        };
        if group_name != self.served_group_name {
            return self.group_mismatch_response::<HeartbeatResponseProto>(
                &req.header,
                format!(
                    "heartbeat group_name {} does not match served metadata group {}",
                    group_name, self.served_group_name
                ),
            );
        }
        let worker_id = WorkerId::new(req.worker_id);
        if worker_id.as_raw() == 0 {
            return self.invalid_request_response::<HeartbeatResponseProto>(&req.header, "worker_id must be non-zero");
        }
        let worker_run_id = match req.worker_run_id.parse::<WorkerRunId>() {
            Ok(worker_run_id) => worker_run_id,
            Err(error) => {
                return self.invalid_request_response::<HeartbeatResponseProto>(
                    &req.header,
                    format!("worker_run_id must be a UUID: {error}"),
                )
            }
        };

        let capacity = match req.capacity {
            Some(capacity) => capacity,
            None => return self.invalid_request_response::<HeartbeatResponseProto>(&req.header, "Missing capacity"),
        };

        let load = match req.load {
            Some(load) => load,
            None => return self.invalid_request_response::<HeartbeatResponseProto>(&req.header, "Missing load"),
        };

        let health_proto = req.health();
        let worker_net_protocol = req.worker_net_protocol() as i32; // Convert enum to i32
        if req.worker_net_protocol() != proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc {
            return self.invalid_request_response::<HeartbeatResponseProto>(
                &req.header,
                "worker_net_protocol must be gRPC for heartbeat",
            );
        }
        let endpoint = match req.advertised_endpoint {
            Some(endpoint) => endpoint,
            None => {
                return self
                    .invalid_request_response::<HeartbeatResponseProto>(&req.header, "Missing advertised_endpoint");
            }
        };
        let advertised_endpoint = match validate_advertised_endpoint(endpoint) {
            Ok(address) => address,
            Err(message) => return self.invalid_request_response::<HeartbeatResponseProto>(&req.header, message),
        };

        self.worker_manager.expire_liveness();

        let descriptor = match self.worker_manager.get_descriptor(&group_name, worker_id) {
            Some(descriptor) => descriptor,
            None => {
                return self.need_register_response::<HeartbeatResponseProto>(
                    &req.header,
                    format!(
                        "worker descriptor not found for group_name={}, worker_id={}",
                        group_name,
                        worker_id.as_raw()
                    ),
                );
            }
        };
        let registration = match self.worker_manager.get_registration(&group_name, worker_id) {
            Some(registration) => registration,
            None => {
                return self.need_register_response::<HeartbeatResponseProto>(
                    &req.header,
                    format!(
                        "live worker registration not found for group_name={}, worker_id={}",
                        group_name,
                        worker_id.as_raw()
                    ),
                );
            }
        };
        if registration.worker_run_id != worker_run_id {
            return self.worker_run_mismatch_response::<HeartbeatResponseProto>(
                &req.header,
                format!(
                    "worker_run_id mismatch for group_name={}, worker_id={}",
                    group_name,
                    worker_id.as_raw()
                ),
            );
        }
        if descriptor.address != advertised_endpoint || descriptor.worker_net_protocol != worker_net_protocol {
            return self.worker_descriptor_mismatch_response::<HeartbeatResponseProto>(
                &req.header,
                format!(
                    "advertised endpoint or protocol does not match registration for group_name={}, worker_id={}",
                    group_name,
                    worker_id.as_raw()
                ),
            );
        }

        use super::manager::HealthStatus;
        let health_status = HealthStatus::from(health_proto as i32);

        let live_state = match self.worker_manager.record_heartbeat(
            &group_name,
            worker_id,
            worker_run_id,
            req.heartbeat_seq,
            &advertised_endpoint,
            worker_net_protocol,
            capacity.total_bytes,
            capacity.used_bytes,
            capacity.available_bytes,
            load.active_reads,
            load.active_writes,
            health_status,
        ) {
            Ok(live_state) => live_state,
            Err(MetadataError::NotFound(message)) => {
                return self.need_register_response::<HeartbeatResponseProto>(&req.header, message);
            }
            Err(MetadataError::StaleState(message)) => {
                return self.worker_run_mismatch_response::<HeartbeatResponseProto>(&req.header, message);
            }
            Err(MetadataError::InvalidArgument(message)) => {
                return self.worker_descriptor_mismatch_response::<HeartbeatResponseProto>(&req.header, message);
            }
            Err(error) => return self.metadata_error_response::<HeartbeatResponseProto>(&req.header, error),
        };

        // Update metrics (all nodes)
        let live_count = self.worker_manager.list_live_workers().len();
        self.metrics.update_worker_live(live_count);

        if !req.acks.is_empty() {
            warn!(
                worker_id = worker_id.as_raw(),
                ack_count = req.acks.len(),
                "Ignoring worker command acks because command ack handling is not enabled"
            );
        }

        Ok(Response::new(HeartbeatResponseProto {
            header: Some((&self.create_response_header_from_request(&req.header, Some(&group_name))).into()),
            commands: Vec::new(),
            worker_id: live_state.worker_id.as_raw(),
            accepted_worker_run_id: live_state.worker_run_id.to_string(),
            heartbeat_interval_ms: self.heartbeat_interval_ms(),
            liveness_timeout_ms: self.liveness_timeout_ms(),
            server_role: self.server_role() as i32,
            leader_hint: self.leader_hint(),
            group_name: group_name.to_string(),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn block_report(
        &self,
        request: Request<BlockReportRequestProto>,
    ) -> Result<Response<BlockReportResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        let group_name = match GroupName::parse(&req.group_name) {
            Ok(group_name) => group_name,
            Err(error) => {
                return self.invalid_request_response::<BlockReportResponseProto>(
                    &req.header,
                    format!("group_name is invalid: {error}"),
                )
            }
        };
        if group_name != self.served_group_name {
            return self.group_mismatch_response::<BlockReportResponseProto>(
                &req.header,
                format!(
                    "block report group_name {} does not match served metadata group {}",
                    group_name, self.served_group_name
                ),
            );
        }
        let worker_id = WorkerId::new(req.worker_id);
        if worker_id.as_raw() == 0 {
            return self
                .invalid_request_response::<BlockReportResponseProto>(&req.header, "worker_id must be non-zero");
        }
        let worker_run_id = match req.worker_run_id.parse::<WorkerRunId>() {
            Ok(worker_run_id) => worker_run_id,
            Err(error) => {
                return self.invalid_request_response::<BlockReportResponseProto>(
                    &req.header,
                    format!("worker_run_id must be a UUID: {error}"),
                )
            }
        };
        let report_seq = req.report_seq;
        let Some(report) = req.report else {
            return self
                .invalid_request_response::<BlockReportResponseProto>(&req.header, "block report body is required");
        };

        let result = match report {
            block_report_request_proto::Report::Full(full) => {
                let mut blocks = Vec::with_capacity(full.blocks.len());
                for block in full.blocks {
                    match Self::proto_to_report_block(block) {
                        Ok(block) => blocks.push(block),
                        Err(error) => {
                            return self.metadata_error_response::<BlockReportResponseProto>(&req.header, error);
                        }
                    }
                }
                match self.worker_manager.receive_full_block_report(
                    &group_name,
                    worker_id,
                    worker_run_id,
                    report_seq,
                    full.batch_seq,
                    full.final_batch,
                    blocks,
                ) {
                    Ok(result) => result,
                    Err(error) => return self.map_report_error(&req.header, error),
                }
            }
            block_report_request_proto::Report::Delta(delta) => {
                let mut deltas = Vec::with_capacity(delta.deltas.len());
                for delta in delta.deltas {
                    match Self::proto_to_delta(delta) {
                        Ok(delta) => deltas.push(delta),
                        Err(error) => {
                            return self.metadata_error_response::<BlockReportResponseProto>(&req.header, error);
                        }
                    }
                }
                match self.worker_manager.apply_delta_block_report(
                    &group_name,
                    worker_id,
                    worker_run_id,
                    report_seq,
                    delta.delta_seq,
                    deltas,
                ) {
                    Ok(result) => result,
                    Err(error) => return self.map_report_error(&req.header, error),
                }
            }
        };

        // Update metrics
        let total_blocks = result.added_blocks.len() + result.removed_blocks.len();
        self.metrics.record_blockreport_blocks(total_blocks as u64);
        let locations_size = self.worker_manager.get_all_locations_count();
        self.metrics.update_locations_size(locations_size);

        info!(
            group_name = %group_name,
            worker_id = worker_id.as_raw(),
            report_seq,
            next_delta_seq = result.next_delta_seq,
            added_blocks = result.added_blocks.len(),
            removed_blocks = result.removed_blocks.len(),
            "Block report processed"
        );

        Ok(Response::new(BlockReportResponseProto {
            header: Some((&self.create_response_header_from_request(&req.header, Some(&group_name))).into()),
            report_seq,
            next_delta_seq: result.next_delta_seq,
            retry_after_ms: 0,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MetadataError;
    use crate::raft::{AppRaftStateMachine, RocksDBStorage};
    use crate::worker::HealthStatus;
    use crate::MountTable;
    use proto::common::{error_detail_proto, ErrorClassProto, RefreshReasonProto, RpcErrorCodeProto};
    use tempfile::TempDir;

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    async fn leader_raft(dir: &TempDir) -> Arc<AppRaftNode> {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), mount_table));
        let raft_config = crate::config::RaftConfig::default();
        let raft_node = Arc::new(AppRaftNode::new(1, storage, state_machine, &raft_config).await.unwrap());
        raft_node
            .initialize_single_node("127.0.0.1:0".to_string())
            .await
            .unwrap();
        for _ in 0..100 {
            if raft_node.is_leader() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
        assert!(raft_node.is_leader());
        raft_node
    }

    async fn nonleader_raft(dir: &TempDir) -> Arc<AppRaftNode> {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), mount_table));
        let raft_config = crate::config::RaftConfig::default();
        let raft_node = Arc::new(AppRaftNode::new(1, storage, state_machine, &raft_config).await.unwrap());
        assert!(!raft_node.is_leader());
        raft_node
    }

    fn block_proto(block_id: BlockId) -> proto::common::BlockIdProto {
        block_id.into()
    }

    fn report_block_proto(block_id: BlockId) -> BlockReportBlockProto {
        BlockReportBlockProto {
            block_id: Some(block_proto(block_id)),
            data_handle_id: block_id.data_handle_id.as_raw(),
            block_index: block_id.index.as_raw(),
            block_stamp: 100 + u64::from(block_id.index.as_raw()),
            effective_len: 4096,
            committed_length: 4096,
            block_state: BlockReportBlockStateProto::BlockReportBlockStateReady as i32,
        }
    }

    fn full_report_request(
        group_name: GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        report_seq: u64,
        batch_seq: u64,
        final_batch: bool,
        blocks: Vec<BlockId>,
    ) -> BlockReportRequestProto {
        let group_name = group_name.to_string();
        BlockReportRequestProto {
            header: Some(proto::common::RequestHeaderProto {
                group_name: group_name.clone(),
                ..Default::default()
            }),
            worker_id: worker_id.as_raw(),
            worker_run_id: worker_run_id.to_string(),
            report_seq,
            report: Some(block_report_request_proto::Report::Full(FullBlockReportBatchProto {
                batch_seq,
                final_batch,
                blocks: blocks.into_iter().map(report_block_proto).collect(),
            })),
            group_name,
        }
    }

    fn delta_report_request(
        group_name: GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        report_seq: u64,
        delta_seq: u64,
        deltas: Vec<(BlockReportDeltaOpProto, BlockId)>,
    ) -> BlockReportRequestProto {
        let group_name = group_name.to_string();
        BlockReportRequestProto {
            header: Some(proto::common::RequestHeaderProto {
                group_name: group_name.clone(),
                ..Default::default()
            }),
            worker_id: worker_id.as_raw(),
            worker_run_id: worker_run_id.to_string(),
            report_seq,
            report: Some(block_report_request_proto::Report::Delta(DeltaBlockReportProto {
                delta_seq,
                deltas: deltas
                    .into_iter()
                    .map(|(op, block_id)| BlockReportDeltaProto {
                        op: op as i32,
                        block: Some(report_block_proto(block_id)),
                    })
                    .collect(),
            })),
            group_name,
        }
    }

    fn test_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
    }

    fn second_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440001".parse().unwrap()
    }

    fn worker_run_id_for(group_name: &GroupName, worker_id: WorkerId) -> WorkerRunId {
        let group_component = group_name
            .as_str()
            .bytes()
            .fold(0u64, |acc, byte| acc.saturating_add(u64::from(byte)));
        let suffix = group_component
            .saturating_mul(1_000_000)
            .saturating_add(worker_id.as_raw());
        format!("550e8400-e29b-41d4-a716-{suffix:012x}")
            .parse()
            .expect("valid test WorkerRunId")
    }

    #[allow(clippy::too_many_arguments)]
    fn record_heartbeat(
        worker_manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        capacity_total: u64,
        capacity_used: u64,
        capacity_available: u64,
        active_reads: u32,
        active_writes: u32,
        health: HealthStatus,
    ) -> WorkerRunId {
        let descriptor = worker_manager
            .get_descriptor(group_name, worker_id)
            .expect("worker descriptor should be registered");
        let worker_run_id = worker_manager
            .get_registration(group_name, worker_id)
            .map(|registration| registration.worker_run_id)
            .unwrap_or_else(|| {
                let worker_run_id = worker_run_id_for(group_name, worker_id);
                worker_manager
                    .register_worker_run(
                        group_name,
                        worker_id,
                        descriptor.address.clone(),
                        descriptor.worker_net_protocol,
                        worker_run_id,
                        descriptor.fault_domain.clone(),
                    )
                    .expect("worker run should register");
                worker_run_id
            });
        worker_manager
            .record_heartbeat(
                group_name,
                worker_id,
                worker_run_id,
                1,
                &descriptor.address,
                descriptor.worker_net_protocol,
                capacity_total,
                capacity_used,
                capacity_available,
                active_reads,
                active_writes,
                health,
            )
            .expect("heartbeat should be accepted");
        worker_manager
            .upsert_descriptor(descriptor)
            .expect("descriptor should be restored");
        worker_run_id
    }

    fn heartbeat_request(
        group_name: GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        heartbeat_seq: u64,
        endpoint_port: u32,
    ) -> HeartbeatRequestProto {
        let group_name = group_name.to_string();
        HeartbeatRequestProto {
            header: Some(proto::common::RequestHeaderProto {
                group_name: group_name.clone(),
                ..Default::default()
            }),
            worker_id: worker_id.as_raw(),
            worker_run_id: worker_run_id.to_string(),
            heartbeat_seq,
            advertised_endpoint: Some(proto::common::EndpointProto {
                host: "127.0.0.1".to_string(),
                port: endpoint_port,
                protocol: "grpc".to_string(),
            }),
            capacity: Some(CapacityInfoProto {
                total_bytes: 1_000,
                used_bytes: 100,
                available_bytes: 900,
            }),
            load: Some(LoadInfoProto {
                active_reads: 0,
                active_writes: 0,
                cpu_usage_percent: 0,
                memory_used_bytes: 0,
            }),
            health: HealthStatusProto::HealthStatusHealthy as i32,
            worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
            acks: Vec::new(),
            group_name,
        }
    }

    #[tokio::test]
    async fn register_worker_persist_helper_propagates_propose_failure() {
        let result =
            persist_worker_descriptor(async { Err(MetadataError::Internal("propose failed".to_string())) }).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("propose failed"));
    }

    #[tokio::test]
    async fn register_worker_persist_helper_returns_accepted_worker_id() {
        let worker_id = WorkerId::new(7);

        let returned_worker_id =
            persist_worker_descriptor(async { Ok(AppDataResponse::Worker(WorkerCommandResult::Upserted(worker_id))) })
                .await
                .unwrap();
        assert_eq!(returned_worker_id, worker_id);
    }

    #[tokio::test]
    async fn register_worker_persist_helper_rejects_unexpected_response() {
        let result = persist_worker_descriptor(async { Ok(AppDataResponse::None) }).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unexpected Raft response"));
    }

    #[tokio::test]
    async fn block_report_applies_soft_state_without_maintenance_signal() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let before_state_id = raft_node.get_last_applied_state_id();
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(7);
        let block_id = BlockId::from_u64_u32(70, 0);
        worker_manager
            .register_worker(&group_name("root"), worker_id, "127.0.0.1:9090".to_string(), 1, None)
            .unwrap();
        let worker_run_id = record_heartbeat(
            &worker_manager,
            &group_name("root"),
            worker_id,
            1_000,
            500,
            500,
            0,
            0,
            HealthStatus::Healthy,
        );
        let service = MetadataWorkerServiceImpl::new(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
            &service,
            Request::new(full_report_request(
                group_name("root"),
                worker_id,
                worker_run_id,
                1,
                0,
                true,
                vec![block_id],
            )),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(response.report_seq, 1);
        assert_eq!(response.next_delta_seq, 0);
        assert_eq!(
            worker_manager.get_block_locations(&group_name("root"), block_id),
            vec![worker_id]
        );
        assert_eq!(raft_node.get_last_applied_state_id(), before_state_id);
    }

    #[tokio::test]
    async fn follower_block_report_updates_local_view_without_commands() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(8);
        let block_id = BlockId::from_u64_u32(80, 0);
        worker_manager
            .register_worker(&group_name("root"), worker_id, "127.0.0.1:9091".to_string(), 1, None)
            .unwrap();
        let worker_run_id = record_heartbeat(
            &worker_manager,
            &group_name("root"),
            worker_id,
            1_000,
            500,
            500,
            0,
            0,
            HealthStatus::Healthy,
        );
        worker_manager
            .receive_full_block_report(
                &group_name("root"),
                worker_id,
                worker_run_id,
                3,
                0,
                true,
                vec![BlockReportBlock {
                    block_id,
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                    block_stamp: 100,
                    effective_len: 4096,
                    committed_length: 4096,
                    block_state: BlockReportBlockState::Ready,
                }],
            )
            .unwrap();
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
            &service,
            Request::new(delta_report_request(
                group_name("root"),
                worker_id,
                worker_run_id,
                3,
                0,
                vec![(BlockReportDeltaOpProto::BlockReportDeltaOpRemove, block_id)],
            )),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(response.next_delta_seq, 1);
        assert!(worker_manager
            .get_block_locations(&group_name("root"), block_id)
            .is_empty());
    }

    #[tokio::test]
    async fn register_worker_invalid_request_returns_header_error() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: None,
                worker_id: 9,
                worker_run_id: test_worker_run_id().to_string(),
                advertised_endpoint: None,
                capabilities: 0,
                version: String::new(),
                labels: Default::default(),
                worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                group_name: "root".to_string(),
            }),
        )
        .await
        .expect("business validation must return gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
        assert!(error.message.contains("Missing advertised_endpoint"));
    }

    #[tokio::test]
    async fn follower_register_worker_returns_not_leader_header_error() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: None,
                worker_id: 123,
                worker_run_id: test_worker_run_id().to_string(),
                advertised_endpoint: Some(proto::common::EndpointProto {
                    host: "127.0.0.1".to_string(),
                    port: 9090,
                    protocol: "grpc".to_string(),
                }),
                capabilities: 0,
                version: "0.1.0".to_string(),
                labels: Default::default(),
                worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                group_name: "root".to_string(),
            }),
        )
        .await
        .expect("follower business redirect returns gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert_eq!(error.refresh_reason, RefreshReasonProto::RefreshReasonNotLeader as i32);
    }

    #[tokio::test]
    async fn register_worker_response_confirms_worker_id_and_run_id() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        raft_node.set_worker_manager(Arc::clone(&worker_manager)).unwrap();
        let worker_run_id = test_worker_run_id();
        let service = MetadataWorkerServiceImpl::new(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: None,
                worker_id: 123,
                worker_run_id: worker_run_id.to_string(),
                advertised_endpoint: Some(proto::common::EndpointProto {
                    host: "127.0.0.1".to_string(),
                    port: 9090,
                    protocol: "grpc".to_string(),
                }),
                capabilities: 0,
                version: "0.1.0".to_string(),
                labels: Default::default(),
                worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                group_name: "root".to_string(),
            }),
        )
        .await
        .expect("register worker response")
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(response.group_name, "root");
        assert_eq!(response.worker_id, 123);
        assert_eq!(response.accepted_worker_run_id, worker_run_id.to_string());
        let descriptor = worker_manager
            .get_descriptor(&group_name("root"), WorkerId::new(123))
            .unwrap();
        assert_eq!(descriptor.address, "127.0.0.1:9090");
        assert_eq!(
            worker_manager
                .get_registration(&group_name("root"), WorkerId::new(123))
                .expect("live registration")
                .worker_run_id,
            worker_run_id
        );
    }

    #[tokio::test]
    async fn register_worker_service_does_not_mutate_live_manager_without_apply_observer() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_run_id = test_worker_run_id();
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: None,
                worker_id: 124,
                worker_run_id: worker_run_id.to_string(),
                advertised_endpoint: Some(proto::common::EndpointProto {
                    host: "127.0.0.1".to_string(),
                    port: 9091,
                    protocol: "grpc".to_string(),
                }),
                capabilities: 0,
                version: "0.1.0".to_string(),
                labels: Default::default(),
                worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                group_name: "root".to_string(),
            }),
        )
        .await
        .expect("register worker response")
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert!(worker_manager
            .get_descriptor(&group_name("root"), WorkerId::new(124))
            .is_none());
        assert!(worker_manager
            .get_registration(&group_name("root"), WorkerId::new(124))
            .is_none());
    }

    #[tokio::test]
    async fn register_worker_rejects_different_live_worker_run_id() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        raft_node.set_worker_manager(Arc::clone(&worker_manager)).unwrap();
        let service = MetadataWorkerServiceImpl::new(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );
        let request = |worker_run_id: WorkerRunId| RegisterWorkerRequestProto {
            header: None,
            worker_id: 123,
            worker_run_id: worker_run_id.to_string(),
            advertised_endpoint: Some(proto::common::EndpointProto {
                host: "127.0.0.1".to_string(),
                port: 9090,
                protocol: "grpc".to_string(),
            }),
            capabilities: 0,
            version: "0.1.0".to_string(),
            labels: Default::default(),
            worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
            group_name: "root".to_string(),
        };

        let first = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(request(test_worker_run_id())),
        )
        .await
        .expect("first register")
        .into_inner();
        assert!(first.header.expect("header").error.is_none());

        let second = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(request(second_worker_run_id())),
        )
        .await
        .expect("conflicting register returns header error")
        .into_inner();
        let error = second.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
        assert!(error.message.contains("already registered"));
        assert_eq!(
            worker_manager
                .get_registration(&group_name("root"), WorkerId::new(123))
                .expect("registration")
                .worker_run_id,
            test_worker_run_id()
        );

        let mut next_request = request(second_worker_run_id());
        next_request.worker_id = 124;
        next_request.advertised_endpoint = Some(proto::common::EndpointProto {
            host: "127.0.0.1".to_string(),
            port: 9091,
            protocol: "grpc".to_string(),
        });
        let next = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(next_request),
        )
        .await
        .expect("raft core remains available after conflicting register")
        .into_inner();
        assert!(next.header.expect("header").error.is_none());
    }

    #[tokio::test]
    async fn register_worker_rejects_non_served_group_without_mutating_worker_manager() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: Some(proto::common::RequestHeaderProto {
                    group_name: "other".to_string(),
                    ..Default::default()
                }),
                worker_id: 123,
                worker_run_id: test_worker_run_id().to_string(),
                advertised_endpoint: Some(proto::common::EndpointProto {
                    host: "127.0.0.1".to_string(),
                    port: 9090,
                    protocol: "grpc".to_string(),
                }),
                capabilities: 0,
                version: "0.1.0".to_string(),
                labels: Default::default(),
                worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                group_name: "other".to_string(),
            }),
        )
        .await
        .expect("wrong-group register returns gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
        assert!(error.message.contains("served metadata group"));
        assert!(worker_manager
            .get_descriptor(&group_name("g2"), WorkerId::new(123))
            .is_none());
    }

    #[tokio::test]
    async fn heartbeat_unknown_worker_returns_header_error() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name("root"),
                WorkerId::new(99),
                test_worker_run_id(),
                1,
                9090,
            )),
        )
        .await
        .expect("business validation must return gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert!(matches!(
            error.code,
            Some(error_detail_proto::Code::RpcCode(code))
                if code == RpcErrorCodeProto::RpcErrCodeWorkerNotRegistered as i32
        ));
        assert_eq!(
            error.refresh_reason,
            RefreshReasonProto::RefreshReasonNeedRegister as i32
        );
    }

    #[tokio::test]
    async fn heartbeat_missing_live_registration_returns_need_register() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(10);
        worker_manager
            .register_worker(&group_name("root"), worker_id, "127.0.0.1:9090".to_string(), 1, None)
            .unwrap();
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name("root"),
                worker_id,
                test_worker_run_id(),
                1,
                9090,
            )),
        )
        .await
        .expect("heartbeat business error uses gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert!(matches!(
            error.code,
            Some(error_detail_proto::Code::RpcCode(code))
                if code == RpcErrorCodeProto::RpcErrCodeWorkerNotRegistered as i32
        ));
        assert_eq!(
            error.refresh_reason,
            RefreshReasonProto::RefreshReasonNeedRegister as i32
        );
        assert!(response.commands.is_empty());
    }

    #[tokio::test]
    async fn heartbeat_stale_worker_run_returns_run_mismatch() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(11);
        worker_manager
            .register_worker_run(
                &group_name("root"),
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                test_worker_run_id(),
                None,
            )
            .unwrap();
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name("root"),
                worker_id,
                second_worker_run_id(),
                1,
                9090,
            )),
        )
        .await
        .expect("heartbeat business error uses gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert!(matches!(
            error.code,
            Some(error_detail_proto::Code::RpcCode(code))
                if code == RpcErrorCodeProto::RpcErrCodeWorkerRunMismatch as i32
        ));
        assert_eq!(
            error.refresh_reason,
            RefreshReasonProto::RefreshReasonWorkerRunMismatch as i32
        );
    }

    #[tokio::test]
    async fn heartbeat_expired_live_registration_returns_need_register_not_run_mismatch() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(1));
        let worker_id = WorkerId::new(110);
        worker_manager
            .register_worker_run(
                &group_name("root"),
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                test_worker_run_id(),
                None,
            )
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name("root"),
                worker_id,
                second_worker_run_id(),
                1,
                9090,
            )),
        )
        .await
        .expect("expired registration heartbeat returns gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert!(matches!(
            error.code,
            Some(error_detail_proto::Code::RpcCode(code))
                if code == RpcErrorCodeProto::RpcErrCodeWorkerNotRegistered as i32
        ));
        assert_eq!(
            error.refresh_reason,
            RefreshReasonProto::RefreshReasonNeedRegister as i32
        );
    }

    #[tokio::test]
    async fn follower_heartbeat_accepts_liveness_but_returns_empty_commands() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(12);
        worker_manager
            .register_worker_run(
                &group_name("root"),
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                test_worker_run_id(),
                None,
            )
            .unwrap();
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name("root"),
                worker_id,
                test_worker_run_id(),
                7,
                9090,
            )),
        )
        .await
        .expect("follower heartbeat succeeds")
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(response.group_name, "root");
        assert_eq!(response.worker_id, worker_id.as_raw());
        assert_eq!(response.accepted_worker_run_id, test_worker_run_id().to_string());
        assert_eq!(
            response.server_role(),
            MetadataServerRoleProto::MetadataServerRoleFollower
        );
        assert!(response.commands.is_empty());
        assert!(worker_manager.is_worker_live(&group_name("root"), worker_id));
    }

    #[tokio::test]
    async fn leader_heartbeat_accepts_liveness_without_raft_propose() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let before_state_id = raft_node.get_last_applied_state_id();
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(13);
        worker_manager
            .register_worker_run(
                &group_name("root"),
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                test_worker_run_id(),
                None,
            )
            .unwrap();
        let service = MetadataWorkerServiceImpl::new(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name("root"),
                worker_id,
                test_worker_run_id(),
                1,
                9090,
            )),
        )
        .await
        .expect("leader heartbeat succeeds")
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(
            response.server_role(),
            MetadataServerRoleProto::MetadataServerRoleLeader
        );
        assert!(response.commands.is_empty());
        assert_eq!(raft_node.get_last_applied_state_id(), before_state_id);
        assert!(worker_manager.is_worker_live(&group_name("root"), worker_id));
    }

    #[tokio::test]
    async fn heartbeat_wrong_group_returns_group_mismatch() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let mut request = heartbeat_request(group_name("root"), WorkerId::new(14), test_worker_run_id(), 1, 9090);
        request.group_name = "other".to_string();

        let response =
            <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(&service, Request::new(request))
                .await
                .expect("wrong group returns gRPC OK")
                .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
        assert!(matches!(
            error.code,
            Some(error_detail_proto::Code::RpcCode(code))
                if code == RpcErrorCodeProto::RpcErrCodeInvalidArgument as i32
        ));
        assert_eq!(
            error.refresh_reason,
            RefreshReasonProto::RefreshReasonGroupMismatch as i32
        );
        assert!(response.commands.is_empty());
    }

    #[tokio::test]
    async fn heartbeat_descriptor_mismatch_returns_need_register_header_error() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(9);
        worker_manager
            .register_worker_run(
                &group_name("root"),
                worker_id,
                "127.0.0.1:9099".to_string(),
                1,
                test_worker_run_id(),
                None,
            )
            .unwrap();
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name("root"),
                worker_id,
                test_worker_run_id(),
                1,
                9098,
            )),
        )
        .await
        .expect("business validation must return gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert!(matches!(
            error.code,
            Some(error_detail_proto::Code::RpcCode(code))
                if code == RpcErrorCodeProto::RpcErrCodeWorkerDescriptorMismatch as i32
        ));
        assert_eq!(
            error.refresh_reason,
            RefreshReasonProto::RefreshReasonNeedRegister as i32
        );
        assert!(error.message.contains("registration"));
    }

    #[tokio::test]
    async fn block_report_invalid_entry_returns_header_error() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(99);
        worker_manager
            .register_worker(&group_name("root"), worker_id, "127.0.0.1:9099".to_string(), 1, None)
            .unwrap();
        let worker_run_id = record_heartbeat(
            &worker_manager,
            &group_name("root"),
            worker_id,
            1_000,
            500,
            500,
            0,
            0,
            HealthStatus::Healthy,
        );
        worker_manager
            .receive_full_block_report(&group_name("root"), worker_id, worker_run_id, 1, 0, true, Vec::new())
            .unwrap();
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
            &service,
            Request::new(BlockReportRequestProto {
                header: Some(proto::common::RequestHeaderProto {
                    group_name: "root".to_string(),
                    ..Default::default()
                }),
                worker_id: worker_id.as_raw(),
                worker_run_id: worker_run_id.to_string(),
                report_seq: 1,
                report: Some(block_report_request_proto::Report::Delta(DeltaBlockReportProto {
                    delta_seq: 0,
                    deltas: vec![BlockReportDeltaProto {
                        op: BlockReportDeltaOpProto::BlockReportDeltaOpAddUpdate as i32,
                        block: None,
                    }],
                })),
                group_name: "root".to_string(),
            }),
        )
        .await
        .expect("business validation must return gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
        assert!(error.message.contains("missing block"));
    }
}
