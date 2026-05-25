// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataWorkerService implementation.

use super::command_router::WorkerCommandRouter;
use super::manager::WorkerManager;
use super::metrics::WorkerMetrics;
use crate::error::{to_canonical_rpc, MetadataError, MetadataResult};
use crate::maintenance::repair::{BlockReportDelta, RepairSignalSink};
use crate::raft::Command;
use crate::raft::{AppDataResponse, AppRaftNode, WorkerCommandResult};
use crate::service::extract_and_inject_context;
use ::common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use ::common::header::{ResponseHeader, RpcErrorCode, RpcStatus};
use proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProto;
use proto::metadata::*;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};
use types::ids::{BlockId, ShardGroupId, WorkerId};
use types::WorkerRunId;

/// Worker service background task handles.
pub struct WorkerBackgroundHandle {
    _lease_metrics_task: Option<JoinHandle<()>>,
}

impl WorkerBackgroundHandle {
    pub fn task_count(&self) -> usize {
        usize::from(self._lease_metrics_task.is_some())
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
    repair_signal_handler: Arc<dyn RepairSignalSink>,
    command_router: Option<Arc<WorkerCommandRouter>>,
    metrics: Arc<WorkerMetrics>,
    slot_metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
    /// Mount table used to compute mount_epoch for lease gating.
    mount_table: Arc<crate::mount::MountTable>,
    served_group_id: ShardGroupId,
}

impl MetadataWorkerServiceImpl {
    pub(crate) fn new(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        repair_signal_handler: Arc<dyn RepairSignalSink>,
        mount_table: Arc<crate::mount::MountTable>,
        served_group_id: ShardGroupId,
    ) -> Self {
        let metrics = Arc::new(WorkerMetrics::new());

        Self {
            raft_node,
            worker_manager,
            repair_signal_handler,
            command_router: None,
            metrics,
            slot_metrics: None, // Will be set via set_slot_metrics
            mount_table,
            served_group_id,
        }
    }

    /// Set command router after maintenance-owned command sources are available.
    pub(crate) fn set_command_router(&mut self, command_router: Arc<WorkerCommandRouter>) {
        self.command_router = Some(command_router);
    }

    /// Set slot metrics (called after metrics are available).
    pub(crate) fn set_slot_metrics(&mut self, metrics: Arc<crate::metrics::MetadataMetrics>) {
        self.slot_metrics = Some(metrics);
    }

    /// Helper: create a response header from request header with group_id.
    fn create_response_header_from_request(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        group_id: Option<u64>,
    ) -> ::common::header::ResponseHeader {
        let client = req_header
            .as_ref()
            .and_then(|h| h.client.as_ref())
            .and_then(|c| ::common::header::ClientInfo::try_from(c.clone()).ok())
            .unwrap_or_else(|| ::common::header::ClientInfo::new(types::ClientId::new(0)));
        let mut header = ResponseHeader::ok(client);
        if let Some(gid) = group_id {
            header = header.with_group_id(gid);
        }
        if self.raft_node.is_leader() {
            if let (Some(gid), Some(sid)) = (group_id, self.raft_node.get_last_applied_state_id()) {
                header = header.with_state(vec![types::GroupStateWatermark::new(
                    types::ShardGroupId::new(gid),
                    sid,
                )]);
            }
        }
        header
    }

    fn group_id_from_request_header(req_header: &Option<proto::common::RequestHeaderProto>) -> Option<u64> {
        req_header.as_ref().and_then(|header| {
            if header.group_id != 0 {
                Some(header.group_id)
            } else {
                None
            }
        })
    }

    fn ok_response_header_from_request(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
    ) -> proto::common::ResponseHeaderProto {
        (&self.create_response_header_from_request(req_header, Self::group_id_from_request_header(req_header))).into()
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
        let mut header =
            self.create_response_header_from_request(req_header, Self::group_id_from_request_header(req_header));
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
        // Start lease metrics update task
        let lease_metrics_task = if let Some(ref slot_metrics) = self.slot_metrics {
            let lease_manager = self.worker_manager.lease_manager();
            let worker_manager = Arc::clone(&self.worker_manager);
            let slot_metrics = Arc::clone(slot_metrics);
            let raft_node = Arc::clone(&self.raft_node);
            let served_group_id = self.served_group_id;
            Some(tokio::spawn(async move {
                use tokio::time::{interval, Duration};
                let mut interval = interval(Duration::from_secs(10)); // Update every 10 seconds
                loop {
                    interval.tick().await;

                    // Only update on leader
                    if !raft_node.is_leader() {
                        continue;
                    }

                    let active_leases = lease_manager.active_lease_count().await;
                    const DEFAULT_MAX_CONCURRENT_LEASES: usize = 10;
                    let available_leases = DEFAULT_MAX_CONCURRENT_LEASES.saturating_sub(active_leases);

                    slot_metrics
                        .full_report_leases_inflight
                        .store(active_leases, std::sync::atomic::Ordering::Relaxed);
                    slot_metrics
                        .full_report_leases_available
                        .store(available_leases, std::sync::atomic::Ordering::Relaxed);

                    // Estimate waiting count: count workers that need_full_sync but don't have lease
                    let waiting_count = {
                        let live_workers = worker_manager.list_live_workers();
                        let mut waiting: usize = 0;
                        for worker in live_workers.iter().filter(|worker| worker.group_id == served_group_id) {
                            if worker_manager.needs_full_sync(served_group_id, worker.worker_id) {
                                // Approximate: if needs_full_sync, count as waiting
                                // This is approximate because we don't track which workers have leases
                                waiting += 1;
                            }
                        }
                        // Subtract in-flight leases (those workers are not waiting)
                        waiting.saturating_sub(active_leases)
                    };
                    slot_metrics
                        .full_report_leases_waiting
                        .store(waiting_count, std::sync::atomic::Ordering::Relaxed);
                }
            }))
        } else {
            None
        };

        WorkerBackgroundHandle {
            _lease_metrics_task: lease_metrics_task,
        }
    }

    /// Convert proto BlockId to types BlockId.
    fn proto_to_block_id(proto: &proto::common::BlockIdProto) -> MetadataResult<BlockId> {
        Ok(BlockId::try_from(*proto).unwrap_or_else(|()| unreachable!("BlockIdProto conversion is infallible")))
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

        let header_group_id = Self::group_id_from_request_header(&req.header);
        let group_id = if req.group_id != 0 {
            req.group_id
        } else {
            header_group_id.unwrap_or(0)
        };
        if group_id == 0 {
            return self
                .invalid_request_response::<RegisterWorkerResponseProto>(&req.header, "group_id must be non-zero");
        }
        if let Some(header_group_id) = header_group_id {
            if header_group_id != group_id {
                return self.invalid_request_response::<RegisterWorkerResponseProto>(
                    &req.header,
                    "request header group_id must match register group_id",
                );
            }
        }
        if ShardGroupId::new(group_id) != self.served_group_id {
            return self.invalid_request_response::<RegisterWorkerResponseProto>(
                &req.header,
                format!(
                    "register group_id {} does not match served metadata group {}",
                    group_id,
                    self.served_group_id.as_raw()
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

        let command = Command::RegisterWorker {
            dedup: crate::raft::DedupKey::new(_caller_ctx.client.client_id, _caller_ctx.client.call_id),
            group_id: ShardGroupId::new(group_id),
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
            group_id,
            worker_id = accepted_worker_id.as_raw(),
            worker_run_id = %worker_run_id,
            "Worker registered"
        );

        Ok(Response::new(RegisterWorkerResponseProto {
            header: Some(self.ok_response_header_from_request(&req.header)),
            group_id,
            worker_id: accepted_worker_id.as_raw(),
            accepted_worker_run_id: worker_run_id.to_string(),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequestProto>,
    ) -> Result<Response<HeartbeatResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        let header_group_id = Self::group_id_from_request_header(&req.header);
        let group_id = if req.group_id != 0 {
            req.group_id
        } else {
            header_group_id.unwrap_or(0)
        };
        if group_id == 0 {
            return self.invalid_request_response::<HeartbeatResponseProto>(&req.header, "group_id must be non-zero");
        }
        if let Some(header_group_id) = header_group_id {
            if header_group_id != group_id {
                return self.group_mismatch_response::<HeartbeatResponseProto>(
                    &req.header,
                    "request header group_id must match heartbeat group_id",
                );
            }
        }
        let group_id = ShardGroupId::new(group_id);
        if group_id != self.served_group_id {
            return self.group_mismatch_response::<HeartbeatResponseProto>(
                &req.header,
                format!(
                    "heartbeat group_id {} does not match served metadata group {}",
                    group_id.as_raw(),
                    self.served_group_id.as_raw()
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

        let descriptor = match self.worker_manager.get_descriptor(group_id, worker_id) {
            Some(descriptor) => descriptor,
            None => {
                return self.need_register_response::<HeartbeatResponseProto>(
                    &req.header,
                    format!(
                        "worker descriptor not found for group_id={}, worker_id={}",
                        group_id.as_raw(),
                        worker_id.as_raw()
                    ),
                );
            }
        };
        let registration = match self.worker_manager.get_registration(group_id, worker_id) {
            Some(registration) => registration,
            None => {
                return self.need_register_response::<HeartbeatResponseProto>(
                    &req.header,
                    format!(
                        "live worker registration not found for group_id={}, worker_id={}",
                        group_id.as_raw(),
                        worker_id.as_raw()
                    ),
                );
            }
        };
        if registration.worker_run_id != worker_run_id {
            return self.worker_run_mismatch_response::<HeartbeatResponseProto>(
                &req.header,
                format!(
                    "worker_run_id mismatch for group_id={}, worker_id={}",
                    group_id.as_raw(),
                    worker_id.as_raw()
                ),
            );
        }
        if descriptor.address != advertised_endpoint || descriptor.worker_net_protocol != worker_net_protocol {
            return self.worker_descriptor_mismatch_response::<HeartbeatResponseProto>(
                &req.header,
                format!(
                    "advertised endpoint or protocol does not match registration for group_id={}, worker_id={}",
                    group_id.as_raw(),
                    worker_id.as_raw()
                ),
            );
        }

        use super::manager::HealthStatus;
        let health_status = HealthStatus::from(health_proto as i32);

        let live_state = match self.worker_manager.record_heartbeat(
            group_id,
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
            header: Some((&self.create_response_header_from_request(&req.header, Some(group_id.as_raw()))).into()),
            commands: Vec::new(),
            full_report_lease_token: 0,
            can_full_report: false,
            backoff_ms: 0,
            group_id: group_id.as_raw(),
            worker_id: live_state.worker_id.as_raw(),
            accepted_worker_run_id: live_state.worker_run_id.to_string(),
            heartbeat_interval_ms: self.heartbeat_interval_ms(),
            liveness_timeout_ms: self.liveness_timeout_ms(),
            server_role: self.server_role() as i32,
            leader_hint: self.leader_hint(),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn block_report(
        &self,
        request: Request<BlockReportRequestProto>,
    ) -> Result<Response<BlockReportResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        let worker_id = WorkerId::new(req.worker_id);
        let report_type = req.report_type();
        let lease_token = req.full_report_lease_token;

        info!(
            worker_id = worker_id.as_raw(),
            report_type = ?report_type,
            full_entries_count = req.full_entries.len(),
            delta_entries_count = req.delta_entries.len(),
            last_report_seq = req.last_report_seq,
            "Processing block report"
        );

        let (added_blocks, removed_blocks) = match report_type {
            proto::metadata::BlockReportTypeProto::BlockReportTypeFull => {
                // Leader-only: verify lease token for full reports when needs_full_sync
                let is_leader = self.raft_node.is_leader();
                if is_leader {
                    let needs_full_sync = self.worker_manager.needs_full_sync(self.served_group_id, worker_id);
                    if needs_full_sync {
                        // Worker needs full sync, must have valid lease token
                        if lease_token == 0 {
                            return self.invalid_request_response::<BlockReportResponseProto>(
                                &req.header,
                                "Full report requires lease token when needs_full_sync is true",
                            );
                        }

                        // Verify and release lease
                        let metadata_epoch = self.worker_manager.get_metadata_epoch();
                        let lease_manager = self.worker_manager.lease_manager();
                        let mount_epoch = Some(types::group_watermark::MountEpoch::new(self.mount_table.version()));

                        if !lease_manager
                            .verify_and_release(lease_token, worker_id, metadata_epoch, mount_epoch)
                            .await
                        {
                            return self.invalid_request_response::<BlockReportResponseProto>(
                                &req.header,
                                "Invalid or expired lease token for full report",
                            );
                        }
                    }
                }

                // FULL report: convert full_entries to BlockIds
                let mut reported_blocks = Vec::new();
                for entry in &req.full_entries {
                    let block_id = match entry.block_id.as_ref() {
                        Some(block_id) => block_id,
                        None => {
                            return self.invalid_request_response::<BlockReportResponseProto>(
                                &req.header,
                                "Missing block_id in full entry",
                            );
                        }
                    };
                    let block_id = match Self::proto_to_block_id(block_id) {
                        Ok(block_id) => block_id,
                        Err(error) => {
                            return self.metadata_error_response::<BlockReportResponseProto>(&req.header, error);
                        }
                    };
                    reported_blocks.push(block_id);
                }

                // Apply full report (lease already released in verify_and_release above)
                match self
                    .worker_manager
                    .apply_full_report(self.served_group_id, worker_id, reported_blocks.clone())
                {
                    Ok(result) => {
                        // Update metrics after successful full report
                        // Note: Lease metrics can be added later
                        result
                    }
                    Err(_e) => {
                        // Rate limited: return retry_after_ms
                        let retry_after_ms = {
                            use rand::Rng;
                            let base_ms = 5000;
                            let jitter_ms = rand::thread_rng().gen_range(0..25000);
                            (base_ms + jitter_ms) as u32
                        };
                        return Ok(Response::new(BlockReportResponseProto {
                            header: Some(self.ok_response_header_from_request(&req.header)),
                            report_seq: req.last_report_seq,
                            commands: vec![],
                            retry_after_ms,
                        }));
                    }
                }
            }
            proto::metadata::BlockReportTypeProto::BlockReportTypeIncremental => {
                // INCREMENTAL report: convert delta_entries to ADD/REMOVE operations
                // INCREMENTAL reports don't require slot token (only FULL reports do)

                let mut added_blocks = Vec::new();
                let mut removed_blocks = Vec::new();

                for entry in &req.delta_entries {
                    let block_id = match entry.block_id.as_ref() {
                        Some(block_id) => block_id,
                        None => {
                            return self.invalid_request_response::<BlockReportResponseProto>(
                                &req.header,
                                "Missing block_id in delta entry",
                            );
                        }
                    };
                    let block_id = match Self::proto_to_block_id(block_id) {
                        Ok(block_id) => block_id,
                        Err(error) => {
                            return self.metadata_error_response::<BlockReportResponseProto>(&req.header, error);
                        }
                    };

                    match entry.op() {
                        proto::metadata::BlockReportDeltaOpProto::BlockReportDeltaOpAdd => {
                            added_blocks.push(block_id);
                        }
                        proto::metadata::BlockReportDeltaOpProto::BlockReportDeltaOpRemove => {
                            removed_blocks.push(block_id);
                        }
                        _ => {
                            return self.invalid_request_response::<BlockReportResponseProto>(
                                &req.header,
                                "Invalid delta operation",
                            );
                        }
                    }
                }

                // Apply delta report
                match self.worker_manager.apply_delta_report(
                    self.served_group_id,
                    worker_id,
                    added_blocks.clone(),
                    removed_blocks.clone(),
                ) {
                    Ok(result) => result,
                    Err(_e) => {
                        // Full sync required: return RequestFullBlockReport command
                        let metadata_epoch = self.worker_manager.get_metadata_epoch();
                        return Ok(Response::new(BlockReportResponseProto {
                            header: Some(self.ok_response_header_from_request(&req.header)),
                            report_seq: req.last_report_seq,
                            commands: vec![WorkerCommandProto {
                                task_id: 0,
                                command: Some(proto::metadata::worker_command_proto::Command::RequestFullBlockReport(
                                    proto::metadata::RequestFullBlockReportCommandProto {
                                        target_metadata_epoch: metadata_epoch,
                                        reason: "FULL_SYNC_REQUIRED".to_string(),
                                    },
                                )),
                            }],
                            retry_after_ms: 0,
                        }));
                    }
                }
            }
            _ => {
                return self.invalid_request_response::<BlockReportResponseProto>(&req.header, "Invalid report_type");
            }
        };

        // Fanout: all nodes update presence (memory-only, no Raft)
        // No Raft propose for UpdateBlockLocations

        // Hand block-report repair signals to maintenance. The handler owns the leader gate.
        let is_leader = self.raft_node.is_leader();
        let outcome = match self
            .repair_signal_handler
            .handle_block_report_delta(BlockReportDelta {
                group_id: self.served_group_id,
                worker_id,
                added_blocks: added_blocks.clone(),
                removed_blocks: removed_blocks.clone(),
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => return self.metadata_error_response::<BlockReportResponseProto>(&req.header, error),
        };
        if let Some(queue_lengths) = outcome.queue_lengths {
            self.metrics.update_orphan_queue_len(queue_lengths.orphan_queue_len);
            self.metrics.update_repair_queue_len(queue_lengths.repair_queue_len);
        }
        if outcome.enqueue_failures > 0 {
            warn!(
                worker_id = worker_id.as_raw(),
                enqueue_failures = outcome.enqueue_failures,
                "Repair signal handler could not enqueue some planned tasks"
            );
        }

        // Generate new report sequence (monotonically increasing)
        // Note: last_report_seq is now memory-only (no persistence), allows restart duplicates
        let report_seq = if req.last_report_seq > 0 {
            req.last_report_seq + 1
        } else {
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
        };

        // Update metrics
        let total_blocks = added_blocks.len() + removed_blocks.len();
        self.metrics.record_blockreport_blocks(total_blocks as u64);
        let locations_size = self.worker_manager.get_all_locations_count();
        self.metrics.update_locations_size(locations_size);

        info!(
            worker_id = worker_id.as_raw(),
            report_type = ?report_type,
            added_blocks = added_blocks.len(),
            removed_blocks = removed_blocks.len(),
            report_seq = report_seq,
            "Block report processed"
        );

        // Leader-only: get pending commands (follower returns empty)
        let commands = if is_leader {
            self.command_router
                .as_ref()
                .map(|command_router| command_router.poll_commands(worker_id, 1))
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        Ok(Response::new(BlockReportResponseProto {
            header: Some(self.ok_response_header_from_request(&req.header)),
            report_seq,
            commands,
            retry_after_ms: 0,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MetadataError;
    use crate::maintenance::repair::signal::{RepairSignalOutcome, RepairSignalQueueLengths};
    use crate::maintenance::repair::{BlockReportDelta, RepairSignalSink};
    use crate::raft::{AppRaftStateMachine, RocksDBStorage};
    use crate::worker::HealthStatus;
    use crate::MountTable;
    use parking_lot::Mutex;
    use proto::common::{error_detail_proto, ErrorClassProto, RefreshReasonProto, RpcErrorCodeProto};
    use tempfile::TempDir;

    #[derive(Default)]
    struct RecordingRepairSignalSink {
        deltas: Mutex<Vec<BlockReportDelta>>,
    }

    #[async_trait::async_trait]
    impl RepairSignalSink for RecordingRepairSignalSink {
        async fn handle_block_report_delta(&self, delta: BlockReportDelta) -> MetadataResult<RepairSignalOutcome> {
            self.deltas.lock().push(delta);
            Ok(RepairSignalOutcome {
                queue_lengths: Some(RepairSignalQueueLengths {
                    orphan_queue_len: 0,
                    repair_queue_len: 0,
                }),
                ..RepairSignalOutcome::default()
            })
        }
    }

    async fn leader_raft(dir: &TempDir) -> Arc<AppRaftNode> {
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), mount_table));
        let raft_config = crate::config::RaftConfig {
            node_id: 1,
            peers: vec!["127.0.0.1:0".to_string()],
        };
        let raft_node = Arc::new(AppRaftNode::new(1, storage, state_machine, &raft_config).await.unwrap());
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
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), mount_table));
        let raft_config = crate::config::RaftConfig {
            node_id: 1,
            peers: Vec::new(),
        };
        let raft_node = Arc::new(AppRaftNode::new(1, storage, state_machine, &raft_config).await.unwrap());
        assert!(!raft_node.is_leader());
        raft_node
    }

    fn block_proto(block_id: BlockId) -> proto::common::BlockIdProto {
        block_id.into()
    }

    fn test_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
    }

    fn second_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440001".parse().unwrap()
    }

    fn heartbeat_request(
        group_id: u64,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        heartbeat_seq: u64,
        endpoint_port: u32,
    ) -> HeartbeatRequestProto {
        HeartbeatRequestProto {
            header: Some(proto::common::RequestHeaderProto {
                group_id,
                ..Default::default()
            }),
            group_id,
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
            worker_epoch: 0,
            acks: Vec::new(),
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
    async fn block_report_applies_soft_state_and_delegates_repair_signal() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(7);
        let block_id = BlockId::from_u64_u32(70, 0);
        worker_manager
            .register_worker(
                ShardGroupId::new(1),
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                100,
                None,
            )
            .unwrap();
        worker_manager
            .record_test_heartbeat(
                ShardGroupId::new(1),
                worker_id,
                1_000,
                500,
                500,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();
        worker_manager.mark_full_sync_complete(ShardGroupId::new(1), worker_id);
        let signal_sink = Arc::new(RecordingRepairSignalSink::default());
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            Arc::clone(&signal_sink) as Arc<dyn RepairSignalSink>,
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
            &service,
            Request::new(BlockReportRequestProto {
                header: None,
                worker_id: worker_id.as_raw(),
                report_type: BlockReportTypeProto::BlockReportTypeIncremental as i32,
                full_entries: Vec::new(),
                delta_entries: vec![BlockReportEntryDeltaProto {
                    block_id: Some(block_proto(block_id)),
                    op: BlockReportDeltaOpProto::BlockReportDeltaOpAdd as i32,
                    chunk_bitmap: None,
                }],
                last_report_seq: 41,
                full_report_lease_token: 0,
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(response.report_seq, 42);
        assert_eq!(
            worker_manager.get_block_locations(ShardGroupId::new(1), block_id),
            vec![worker_id]
        );
        let deltas = signal_sink.deltas.lock();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].worker_id, worker_id);
        assert_eq!(deltas[0].added_blocks, vec![block_id]);
        assert!(deltas[0].removed_blocks.is_empty());
    }

    #[tokio::test]
    async fn follower_block_report_delegates_repair_signal_to_handler_noop_gate() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(8);
        let block_id = BlockId::from_u64_u32(80, 0);
        worker_manager
            .register_worker(
                ShardGroupId::new(1),
                worker_id,
                "127.0.0.1:9091".to_string(),
                1,
                100,
                None,
            )
            .unwrap();
        worker_manager
            .record_test_heartbeat(
                ShardGroupId::new(1),
                worker_id,
                1_000,
                500,
                500,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();
        worker_manager.mark_full_sync_complete(ShardGroupId::new(1), worker_id);
        worker_manager
            .update_locations(ShardGroupId::new(1), worker_id, vec![block_id])
            .unwrap();
        let signal_sink = Arc::new(RecordingRepairSignalSink::default());
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            Arc::clone(&signal_sink) as Arc<dyn RepairSignalSink>,
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
            &service,
            Request::new(BlockReportRequestProto {
                header: None,
                worker_id: worker_id.as_raw(),
                report_type: BlockReportTypeProto::BlockReportTypeIncremental as i32,
                full_entries: Vec::new(),
                delta_entries: vec![BlockReportEntryDeltaProto {
                    block_id: Some(block_proto(block_id)),
                    op: BlockReportDeltaOpProto::BlockReportDeltaOpRemove as i32,
                    chunk_bitmap: None,
                }],
                last_report_seq: 52,
                full_report_lease_token: 0,
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(response.report_seq, 53);
        let deltas = signal_sink.deltas.lock();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].worker_id, worker_id);
        assert!(deltas[0].added_blocks.is_empty());
        assert_eq!(deltas[0].removed_blocks, vec![block_id]);
    }

    #[tokio::test]
    async fn register_worker_invalid_request_returns_header_error() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: None,
                group_id: 1,
                worker_id: 9,
                worker_run_id: test_worker_run_id().to_string(),
                advertised_endpoint: None,
                capabilities: 0,
                version: String::new(),
                labels: Default::default(),
                worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
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
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: None,
                group_id: 1,
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
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: None,
                group_id: 1,
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
            }),
        )
        .await
        .expect("register worker response")
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(response.group_id, 1);
        assert_eq!(response.worker_id, 123);
        assert_eq!(response.accepted_worker_run_id, worker_run_id.to_string());
        let descriptor = worker_manager
            .get_descriptor(ShardGroupId::new(1), WorkerId::new(123))
            .unwrap();
        assert_eq!(descriptor.address, "127.0.0.1:9090");
        assert_eq!(
            worker_manager
                .get_registration(ShardGroupId::new(1), WorkerId::new(123))
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
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: None,
                group_id: 1,
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
            }),
        )
        .await
        .expect("register worker response")
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert!(worker_manager
            .get_descriptor(ShardGroupId::new(1), WorkerId::new(124))
            .is_none());
        assert!(worker_manager
            .get_registration(ShardGroupId::new(1), WorkerId::new(124))
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
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );
        let request = |worker_run_id: WorkerRunId| RegisterWorkerRequestProto {
            header: None,
            group_id: 1,
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
                .get_registration(ShardGroupId::new(1), WorkerId::new(123))
                .expect("registration")
                .worker_run_id,
            test_worker_run_id()
        );
    }

    #[tokio::test]
    async fn register_worker_rejects_non_served_group_without_mutating_worker_manager() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: Some(proto::common::RequestHeaderProto {
                    group_id: 2,
                    ..Default::default()
                }),
                group_id: 2,
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
            }),
        )
        .await
        .expect("wrong-group register returns gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
        assert!(error.message.contains("served metadata group"));
        assert!(worker_manager
            .get_descriptor(ShardGroupId::new(2), WorkerId::new(123))
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
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(1, WorkerId::new(99), test_worker_run_id(), 1, 9090)),
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
            .register_worker(
                ShardGroupId::new(1),
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                0,
                None,
            )
            .unwrap();
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(1, worker_id, test_worker_run_id(), 1, 9090)),
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
                ShardGroupId::new(1),
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
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(1, worker_id, second_worker_run_id(), 1, 9090)),
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
                ShardGroupId::new(1),
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                test_worker_run_id(),
                None,
            )
            .unwrap();
        worker_manager.set_last_seen_ms_for_test(ShardGroupId::new(1), worker_id, 0);
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(1, worker_id, second_worker_run_id(), 1, 9090)),
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
                ShardGroupId::new(1),
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
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(1, worker_id, test_worker_run_id(), 7, 9090)),
        )
        .await
        .expect("follower heartbeat succeeds")
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(response.group_id, 1);
        assert_eq!(response.worker_id, worker_id.as_raw());
        assert_eq!(response.accepted_worker_run_id, test_worker_run_id().to_string());
        assert_eq!(
            response.server_role(),
            MetadataServerRoleProto::MetadataServerRoleFollower
        );
        assert!(response.commands.is_empty());
        assert!(worker_manager.is_worker_live(ShardGroupId::new(1), worker_id));
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
                ShardGroupId::new(1),
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
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(1, worker_id, test_worker_run_id(), 1, 9090)),
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
        assert!(worker_manager.is_worker_live(ShardGroupId::new(1), worker_id));
    }

    #[tokio::test]
    async fn heartbeat_wrong_group_returns_group_mismatch() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(2, WorkerId::new(14), test_worker_run_id(), 1, 9090)),
        )
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
                ShardGroupId::new(1),
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
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(1, worker_id, test_worker_run_id(), 1, 9098)),
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
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(RecordingRepairSignalSink::default()),
            Arc::new(MountTable::new()),
            ShardGroupId::new(1),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
            &service,
            Request::new(BlockReportRequestProto {
                header: None,
                worker_id: 99,
                report_type: BlockReportTypeProto::BlockReportTypeIncremental as i32,
                full_entries: Vec::new(),
                delta_entries: vec![BlockReportEntryDeltaProto {
                    block_id: None,
                    op: BlockReportDeltaOpProto::BlockReportDeltaOpAdd as i32,
                    chunk_bitmap: None,
                }],
                last_report_seq: 0,
                full_report_lease_token: 0,
            }),
        )
        .await
        .expect("business validation must return gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
        assert!(error.message.contains("Missing block_id"));
    }
}
