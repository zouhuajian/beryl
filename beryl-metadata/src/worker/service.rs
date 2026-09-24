// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! MetadataWorkerService implementation.

use super::manager::BlockReportChange;
use super::{BlockReportBlock, BlockReportBlockState, WorkerManager};
use crate::error::{to_rpc_error, MetadataError, MetadataResult};
use crate::maintenance::BlockCleanupCoordinator;
use crate::observe;
use crate::raft::{AppRaftNode, ApplySuccess, Command};
use crate::service::extract_and_inject_context;
use crate::service::ok_header_from_request;
use ::beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RpcErrorDetail, WorkerErrorKind};
use ::beryl_common::observe::propagation::extract_trace_context;
use beryl_common::header::TraceContext;
use beryl_proto::common::{
    EndpointProto, ErrorDetailProto, RequestHeaderProto, ResponseHeaderProto, TraceContextProto,
};
use beryl_proto::convert::require_worker_run_id;
use beryl_proto::metadata::block_report_request_proto::Batch;
use beryl_proto::metadata::delta_block_report_entry_proto::Block;
use beryl_proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProto;
use beryl_proto::metadata::{
    BlockReportKindProto, BlockReportRequestProto, BlockReportResponseProto, DeltaBlockReportEntryProto,
    HeartbeatRequestProto, HeartbeatResponseProto, RegisterWorkerRequestProto, RegisterWorkerResponseProto,
    ReportedBlockProto, ReportedBlockStateProto, TierFreeProto,
};
use beryl_types::{BlockId, GroupName, GroupStateWatermark, TierFree, WorkerId, MAX_REPORT_ENTRIES};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};

fn register_worker_response_with_header(header: ResponseHeaderProto) -> RegisterWorkerResponseProto {
    RegisterWorkerResponseProto {
        header: Some(header),
        ..Default::default()
    }
}

fn heartbeat_response_with_header(header: ResponseHeaderProto) -> HeartbeatResponseProto {
    HeartbeatResponseProto {
        header: Some(header),
        ..Default::default()
    }
}

fn block_report_response_with_header(header: ResponseHeaderProto) -> BlockReportResponseProto {
    BlockReportResponseProto {
        header: Some(header),
        ..Default::default()
    }
}

fn register_worker_response_header(response: &RegisterWorkerResponseProto) -> Option<&ResponseHeaderProto> {
    response.header.as_ref()
}

fn heartbeat_response_header(response: &HeartbeatResponseProto) -> Option<&ResponseHeaderProto> {
    response.header.as_ref()
}

fn block_report_response_header(response: &BlockReportResponseProto) -> Option<&ResponseHeaderProto> {
    response.header.as_ref()
}

#[derive(Clone, Copy)]
enum MetadataWorkerMetric {
    Registration,
    Heartbeat,
    BlockReport(&'static str),
}

/// MetadataWorkerService implementation.
pub struct MetadataWorkerServiceImpl {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    served_group_name: GroupName,
    cleanup: Arc<BlockCleanupCoordinator>,
    registration_serial: Mutex<()>,
}

impl MetadataWorkerServiceImpl {
    pub(crate) fn new(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        served_group_name: GroupName,
        cleanup: Arc<BlockCleanupCoordinator>,
    ) -> Self {
        Self {
            raft_node,
            worker_manager,
            served_group_name,
            cleanup,
            registration_serial: Mutex::new(()),
        }
    }

    /// Helper: create a response header from request header with group name.
    fn create_response_header_from_request(
        &self,
        req_header: &Option<RequestHeaderProto>,
        group_name: Option<&GroupName>,
    ) -> ResponseHeaderProto {
        let mut header = ok_header_from_request(req_header, group_name.cloned());
        if self.raft_node.is_leader() {
            if let (Some(group_name), Some(sid)) = (group_name, self.raft_node.get_last_applied_state_id()) {
                header.state = Some((&GroupStateWatermark::new(group_name.clone(), sid)).into());
            }
        }
        header
    }

    fn group_name_from_request_header(req_header: &Option<RequestHeaderProto>) -> Option<GroupName> {
        req_header
            .as_ref()
            .and_then(|header| GroupName::parse_optional(&header.group_name).ok().flatten())
    }

    fn error_response_header_from_request(
        &self,
        req_header: &Option<RequestHeaderProto>,
        error: RpcErrorDetail,
    ) -> ResponseHeaderProto {
        let mut header = self
            .create_response_header_from_request(req_header, Self::group_name_from_request_header(req_header).as_ref());
        header.error = Some(beryl_proto::convert::rpc_error_to_proto(&error));
        header
    }

    fn response_with_error<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        error: RpcErrorDetail,
        make_response: fn(ResponseHeaderProto) -> T,
    ) -> Response<T> {
        Response::new(make_response(
            self.error_response_header_from_request(req_header, error),
        ))
    }

    fn invalid_request_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Response<T> {
        self.response_with_error(
            req_header,
            to_rpc_error(MetadataError::InvalidArgument(message.into())),
            make_response,
        )
    }

    fn metadata_error_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        error: MetadataError,
    ) -> Response<T> {
        self.response_with_error(req_header, to_rpc_error(error), make_response)
    }

    fn group_mismatch_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Response<T> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::fail(ErrorKind::Metadata(MetadataErrorKind::GroupMismatch), message),
            make_response,
        )
    }

    fn need_register_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Response<T> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::NotRegistered), message),
            make_response,
        )
    }

    fn worker_run_mismatch_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Response<T> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::RunMismatch), message),
            make_response,
        )
    }

    fn worker_descriptor_mismatch_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Response<T> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::DescriptorMismatch), message),
            make_response,
        )
    }

    fn liveness_timeout_ms(&self) -> u32 {
        self.worker_manager.heartbeat_timeout_ms()
    }

    fn full_report_required_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Response<T> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::send_full_block_report(ErrorKind::Worker(WorkerErrorKind::FullReportRequired), message),
            make_response,
        )
    }

    fn proto_to_report_block(block: ReportedBlockProto) -> MetadataResult<BlockReportBlock> {
        let block_id_proto = block
            .block_id
            .ok_or_else(|| MetadataError::InvalidArgument("block report entry missing block_id".to_string()))?;
        let block_id = BlockId::try_from(block_id_proto).map_err(MetadataError::InvalidArgument)?;
        let block_state = match block.state() {
            ReportedBlockStateProto::ReportedBlockStateReady => BlockReportBlockState::Ready,
            ReportedBlockStateProto::ReportedBlockStateCorrupt => BlockReportBlockState::Corrupt,
            ReportedBlockStateProto::ReportedBlockStateDeleting => BlockReportBlockState::Deleting,
            ReportedBlockStateProto::ReportedBlockStateUnspecified => {
                return Err(MetadataError::InvalidArgument(
                    "block report entry state must be specified".to_string(),
                ));
            }
        };
        if block_state == BlockReportBlockState::Ready && block.lease_epoch == 0 {
            return Err(MetadataError::InvalidArgument(
                "Ready block report entry lease_epoch must be non-zero".to_string(),
            ));
        }
        let tier = if block_state == BlockReportBlockState::Ready {
            Some(beryl_proto::convert::parse_known_tier(block.tier).map_err(MetadataError::InvalidArgument)?)
        } else {
            None
        };
        Ok(BlockReportBlock {
            tier,
            block_id,
            lease_epoch: block.lease_epoch,
            block_state,
            effective_len: block.effective_len,
        })
    }

    fn proto_to_delta_entry(entry: DeltaBlockReportEntryProto) -> MetadataResult<BlockReportChange> {
        match entry.block {
            Some(Block::Present(block)) => Self::proto_to_report_block(block).map(BlockReportChange::Upsert),
            Some(Block::Absent(block_id)) => BlockId::try_from(block_id)
                .map(BlockReportChange::Remove)
                .map_err(|error| MetadataError::InvalidArgument(format!("invalid absent block_id: {error}"))),
            None => Err(MetadataError::InvalidArgument(
                "delta block report entry block must be specified".to_string(),
            )),
        }
    }

    fn record_worker_rpc_outcome<T>(
        method: &'static str,
        metric: MetadataWorkerMetric,
        started: Instant,
        outcome: &Response<T>,
        response_header: fn(&T) -> Option<&ResponseHeaderProto>,
    ) {
        let duration = started.elapsed().as_secs_f64();
        let (status, error_kind) = metadata_worker_outcome_labels(outcome, response_header);

        observe::record_rpc_request("metadata_worker", method, status, error_kind, duration);
        match metric {
            MetadataWorkerMetric::Registration => observe::record_worker_registration(status, error_kind, duration),
            MetadataWorkerMetric::Heartbeat => observe::record_worker_heartbeat(status, error_kind, duration),
            MetadataWorkerMetric::BlockReport(kind) => {
                observe::record_worker_block_report(kind, status, error_kind, duration)
            }
        }
    }
}

fn metadata_worker_outcome_labels<T>(
    outcome: &Response<T>,
    response_header: fn(&T) -> Option<&ResponseHeaderProto>,
) -> (&'static str, &'static str) {
    match response_header(outcome.get_ref()).and_then(|header| header.error.as_ref()) {
        Some(error) => ("error", metadata_worker_error_detail_kind(error)),
        None => ("ok", "none"),
    }
}

fn metadata_worker_error_detail_kind(error: &ErrorDetailProto) -> &'static str {
    let rpc_error = beryl_proto::convert::rpc_error_from_proto(error);
    observe::rpc_error_kind(&rpc_error)
}

fn block_report_kind(req: &BlockReportRequestProto) -> &'static str {
    match &req.batch {
        Some(Batch::FullReport(_)) => "full",
        Some(Batch::DeltaReport(_)) => "delta",
        None => "unknown",
    }
}

fn merge_request_header_transport_context(header: &mut Option<RequestHeaderProto>, context: &TraceContext) {
    let Some(header) = header else {
        return;
    };
    if header.trace_context.as_ref().is_some_and(trace_context_proto_is_empty) {
        header.trace_context = None;
    }
    if context.is_empty() {
        return;
    }
    let trace_context = header.trace_context.get_or_insert_with(Default::default);
    if trace_context.traceparent.is_none() {
        trace_context.traceparent = context.traceparent.clone();
    }
}

fn trace_context_proto_is_empty(context: &TraceContextProto) -> bool {
    context.traceparent.is_none()
}

fn validate_advertised_endpoint(endpoint: EndpointProto) -> Result<String, String> {
    if endpoint.host.trim().is_empty() {
        return Err("advertised_endpoint host must not be empty".to_string());
    }
    if endpoint.port == 0 || endpoint.port > u32::from(u16::MAX) {
        return Err("advertised_endpoint port must be between 1 and 65535".to_string());
    }
    match endpoint.host.parse::<IpAddr>() {
        Ok(address) if address.is_unspecified() => Err("advertised_endpoint must not use a wildcard host".to_string()),
        Ok(IpAddr::V6(_)) => Ok(format!("[{}]:{}", endpoint.host, endpoint.port)),
        _ => Ok(format!("{}:{}", endpoint.host, endpoint.port)),
    }
}

fn parse_tier_free(entries: &[TierFreeProto]) -> Result<Vec<TierFree>, String> {
    entries
        .iter()
        .map(|entry| {
            let tier = beryl_proto::convert::parse_known_tier(entry.tier)
                .map_err(|err| format!("capacity.tier_free tier invalid: {err}"))?;
            Ok(TierFree {
                tier,
                free_bytes: entry.free_bytes,
            })
        })
        .collect()
}

#[tonic::async_trait]
impl MetadataWorkerServiceProto for MetadataWorkerServiceImpl {
    #[instrument(skip_all)]
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequestProto>,
    ) -> Result<Response<RegisterWorkerResponseProto>, Status> {
        let started = Instant::now();
        let transport_context = extract_trace_context(request.metadata());
        let outcome = async {
            let mut req = request.into_inner();
            merge_request_header_transport_context(&mut req.header, &transport_context);
            let caller_ctx = match extract_and_inject_context(&req.header) {
                Ok(ctx) => ctx,
                Err(error) => {
                    return self.response_with_error(&req.header, error, register_worker_response_with_header)
                }
            };

            if !self.raft_node.is_leader() {
                return self.metadata_error_response(
                    &req.header,
                    register_worker_response_with_header,
                    MetadataError::LeaderChanged(
                        "worker registration must be sent to the metadata group leader".into(),
                    ),
                );
            }

            let group_name = match caller_ctx.group_name {
                Some(group_name) => group_name,
                None => {
                    return self.invalid_request_response(
                        &req.header,
                        register_worker_response_with_header,
                        "header group_name is invalid: must not be empty",
                    )
                }
            };
            if group_name != self.served_group_name {
                return self.invalid_request_response(
                    &req.header,
                    register_worker_response_with_header,
                    format!(
                        "register group_name {} does not match served metadata group {}",
                        group_name, self.served_group_name
                    ),
                );
            }
            let worker_id = WorkerId::new(req.worker_id);
            if worker_id.as_raw() == 0 {
                return self.invalid_request_response(
                    &req.header,
                    register_worker_response_with_header,
                    "worker_id must be non-zero",
                );
            }
            let worker_run_id = match require_worker_run_id(&req.worker_run_id, "RegisterWorkerRequest.worker_run_id") {
                Ok(worker_run_id) => worker_run_id,
                Err(error) => {
                    return self.invalid_request_response(&req.header, register_worker_response_with_header, error)
                }
            };
            let endpoint = match req.advertised_endpoint {
                Some(endpoint) => endpoint,
                None => {
                    return self.invalid_request_response(
                        &req.header,
                        register_worker_response_with_header,
                        "Missing advertised_endpoint",
                    );
                }
            };
            let address = match validate_advertised_endpoint(endpoint) {
                Ok(address) => address,
                Err(message) => {
                    return self.invalid_request_response(&req.header, register_worker_response_with_header, message)
                }
            };
            let _registration_guard = self.registration_serial.lock().await;
            if let Err(error) = self.worker_manager.validate_worker_registration_preflight(
                &group_name,
                worker_id,
                worker_run_id,
                &address,
            ) {
                return self.metadata_error_response(&req.header, register_worker_response_with_header, error);
            }

            let command = Command::RegisterWorkerDescriptor {
                group_name: group_name.clone(),
                worker_id,
                address: address.clone(),
            };

            match self.raft_node.propose(command).await {
                Ok(ApplySuccess::WorkerUpserted) => {}
                Ok(other) => {
                    return self.metadata_error_response(
                        &req.header,
                        register_worker_response_with_header,
                        MetadataError::Internal(format!("RegisterWorker returned unexpected Raft response: {other:?}")),
                    );
                }
                Err(error) => {
                    return self.metadata_error_response(&req.header, register_worker_response_with_header, error)
                }
            };
            self.worker_manager
                .register_worker_run(&group_name, worker_id, address.clone(), worker_run_id);

            info!(
                target: "metadata.worker",
                op = "RegisterWorker",
                result = "accepted",
                error_code = "none",
                event = "worker_registered",
                group_name = %group_name,
                worker_id = worker_id.as_raw(),
                worker_run_id = %worker_run_id,
                endpoint = %address,
                protocol = "grpc",
                "Worker registered"
            );

            Response::new(RegisterWorkerResponseProto {
                header: Some(self.create_response_header_from_request(&req.header, Some(&group_name))),
                worker_id: worker_id.as_raw(),
                accepted_worker_run_id: worker_run_id.to_string(),
            })
        }
        .await;
        Self::record_worker_rpc_outcome(
            "register_worker",
            MetadataWorkerMetric::Registration,
            started,
            &outcome,
            register_worker_response_header,
        );
        Ok(outcome)
    }

    #[instrument(skip_all)]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequestProto>,
    ) -> Result<Response<HeartbeatResponseProto>, Status> {
        let started = Instant::now();
        let transport_context = extract_trace_context(request.metadata());
        let outcome = async {
            let mut req = request.into_inner();
            merge_request_header_transport_context(&mut req.header, &transport_context);
            let caller_ctx = match extract_and_inject_context(&req.header) {
                Ok(ctx) => ctx,
                Err(error) => return self.response_with_error(&req.header, error, heartbeat_response_with_header),
            };

            let group_name = match caller_ctx.group_name {
                Some(group_name) => group_name,
                None => {
                    return self.invalid_request_response(
                        &req.header,
                        heartbeat_response_with_header,
                        "header group_name is invalid: must not be empty",
                    )
                }
            };
            if group_name != self.served_group_name {
                return self.group_mismatch_response(
                    &req.header,
                    heartbeat_response_with_header,
                    format!(
                        "heartbeat group_name {} does not match served metadata group {}",
                        group_name, self.served_group_name
                    ),
                );
            }
            let worker_id = WorkerId::new(req.worker_id);
            if worker_id.as_raw() == 0 {
                return self.invalid_request_response(
                    &req.header,
                    heartbeat_response_with_header,
                    "worker_id must be non-zero",
                );
            }
            let worker_run_id = match require_worker_run_id(&req.worker_run_id, "HeartbeatRequest.worker_run_id") {
                Ok(worker_run_id) => worker_run_id,
                Err(error) => return self.invalid_request_response(&req.header, heartbeat_response_with_header, error),
            };

            let capacity = match req.capacity.as_ref() {
                Some(capacity) => capacity,
                None => {
                    return self.invalid_request_response(
                        &req.header,
                        heartbeat_response_with_header,
                        "Missing capacity",
                    )
                }
            };
            let tier_free = match parse_tier_free(&capacity.tier_free) {
                Ok(tier_free) => tier_free,
                Err(message) => {
                    return self.invalid_request_response(&req.header, heartbeat_response_with_header, message)
                }
            };

            let endpoint = match req.advertised_endpoint {
                Some(endpoint) => endpoint,
                None => {
                    return self.invalid_request_response(
                        &req.header,
                        heartbeat_response_with_header,
                        "Missing advertised_endpoint",
                    );
                }
            };
            let advertised_endpoint = match validate_advertised_endpoint(endpoint) {
                Ok(address) => address,
                Err(message) => {
                    return self.invalid_request_response(&req.header, heartbeat_response_with_header, message)
                }
            };
            let received_at_ms = match self.worker_manager.record_heartbeat_with_tier_free(
                &group_name,
                worker_id,
                worker_run_id,
                req.heartbeat_seq,
                &advertised_endpoint,
                tier_free,
            ) {
                Ok(live_state) => live_state,
                Err(MetadataError::NotFound(message)) => {
                    if self.worker_manager.mark_heartbeat_need_register_if_changed(
                        &group_name,
                        worker_id,
                        worker_run_id,
                    ) {
                        warn!(
                            target: "metadata.worker",
                            op = "Heartbeat",
                            result = "rejected",
                            error_code = "need_register",
                            group_name = %group_name,
                            worker_id = worker_id.as_raw(),
                            worker_run_id = %worker_run_id,
                            "Heartbeat rejected"
                        );
                    }
                    return self.need_register_response(&req.header, heartbeat_response_with_header, message);
                }
                Err(MetadataError::StaleState(message)) => {
                    if self
                        .worker_manager
                        .mark_heartbeat_run_mismatch_if_changed(&group_name, worker_id, worker_run_id)
                    {
                        warn!(
                            target: "metadata.worker",
                            op = "Heartbeat",
                            result = "rejected",
                            error_code = "worker_run_mismatch",
                            group_name = %group_name,
                            worker_id = worker_id.as_raw(),
                            worker_run_id = %worker_run_id,
                            "Heartbeat rejected"
                        );
                    }
                    return self.worker_run_mismatch_response(&req.header, heartbeat_response_with_header, message);
                }
                Err(MetadataError::InvalidArgument(message)) => {
                    return self.worker_descriptor_mismatch_response(
                        &req.header,
                        heartbeat_response_with_header,
                        message,
                    );
                }
                Err(error) => return self.metadata_error_response(&req.header, heartbeat_response_with_header, error),
            };

            let live_count = self.worker_manager.list_live_workers().len();
            observe::set_worker_live(live_count);
            let now_ms = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(received_at_ms);
            observe::record_worker_heartbeat_lag(now_ms.saturating_sub(received_at_ms) as f64 / 1000.0);

            let cleanup_commands = self
                .cleanup
                .commands_for_heartbeat(&group_name, worker_id, worker_run_id, Instant::now())
                .into_iter()
                .map(Into::into)
                .collect();

            Response::new(HeartbeatResponseProto {
                header: Some(self.create_response_header_from_request(&req.header, Some(&group_name))),
                worker_id: worker_id.as_raw(),
                accepted_worker_run_id: worker_run_id.to_string(),
                liveness_timeout_ms: self.liveness_timeout_ms(),
                cleanup_commands,
            })
        }
        .await;
        Self::record_worker_rpc_outcome(
            "heartbeat",
            MetadataWorkerMetric::Heartbeat,
            started,
            &outcome,
            heartbeat_response_header,
        );
        Ok(outcome)
    }

    #[instrument(skip_all)]
    async fn block_report(
        &self,
        request: Request<BlockReportRequestProto>,
    ) -> Result<Response<BlockReportResponseProto>, Status> {
        let started = Instant::now();
        let metric_kind = block_report_kind(request.get_ref());
        let transport_context = extract_trace_context(request.metadata());
        let outcome = async {
            let mut req = request.into_inner();
            merge_request_header_transport_context(&mut req.header, &transport_context);
            let caller_ctx = match extract_and_inject_context(&req.header) {
                Ok(ctx) => ctx,
                Err(error) => return self.response_with_error(&req.header, error, block_report_response_with_header),
            };

            let group_name = match caller_ctx.group_name {
                Some(group_name) => group_name,
                None => {
                    return self.invalid_request_response(
                        &req.header,
                        block_report_response_with_header,
                        "header group_name is invalid: must not be empty",
                    )
                }
            };
            if group_name != self.served_group_name {
                return self.group_mismatch_response(
                    &req.header,
                    block_report_response_with_header,
                    format!(
                        "block report group_name {} does not match served metadata group {}",
                        group_name, self.served_group_name
                    ),
                );
            }
            let worker_id = WorkerId::new(req.worker_id);
            if worker_id.as_raw() == 0 {
                return self.invalid_request_response(
                    &req.header,
                    block_report_response_with_header,
                    "worker_id must be non-zero",
                );
            }
            let worker_run_id = match require_worker_run_id(&req.worker_run_id, "BlockReportRequest.worker_run_id") {
                Ok(worker_run_id) => worker_run_id,
                Err(error) => {
                    return self.invalid_request_response(&req.header, block_report_response_with_header, error)
                }
            };
            let baseline_seq = req.baseline_seq;
            if baseline_seq == 0 {
                return self.invalid_request_response(
                    &req.header,
                    block_report_response_with_header,
                    "block report baseline_seq must be non-zero",
                );
            }
            let Some(batch) = req.batch else {
                return self.invalid_request_response(
                    &req.header,
                    block_report_response_with_header,
                    "block report batch is required",
                );
            };

            let (report_kind, report_kind_proto, batch_seq, final_batch, apply_result) = match batch {
                Batch::FullReport(full) => {
                    let batch_seq = full.batch_seq;
                    let final_batch = full.final_batch;
                    if full.blocks.len() > MAX_REPORT_ENTRIES {
                        return self.metadata_error_response(
                            &req.header,
                            block_report_response_with_header,
                            MetadataError::ResourceExhausted(format!(
                                "full block report entry count {} exceeds maximum {}",
                                full.blocks.len(),
                                MAX_REPORT_ENTRIES
                            )),
                        );
                    }
                    let mut blocks = Vec::with_capacity(full.blocks.len());
                    for block in full.blocks {
                        match Self::proto_to_report_block(block) {
                            Ok(block) => blocks.push(block),
                            Err(error) => {
                                return self.metadata_error_response(
                                    &req.header,
                                    block_report_response_with_header,
                                    error,
                                );
                            }
                        }
                    }
                    let result = self.worker_manager.receive_full_block_report(
                        &group_name,
                        worker_id,
                        worker_run_id,
                        baseline_seq,
                        full.batch_seq,
                        full.final_batch,
                        blocks,
                    );
                    (
                        "full",
                        BlockReportKindProto::BlockReportKindFull,
                        batch_seq,
                        Some(final_batch),
                        result,
                    )
                }
                Batch::DeltaReport(delta) => {
                    let batch_seq = delta.batch_seq;
                    if delta.entries.is_empty() {
                        return self.invalid_request_response(
                            &req.header,
                            block_report_response_with_header,
                            "delta block report entries must be non-empty",
                        );
                    }
                    if delta.entries.len() > MAX_REPORT_ENTRIES {
                        return self.metadata_error_response(
                            &req.header,
                            block_report_response_with_header,
                            MetadataError::ResourceExhausted(format!(
                                "delta block report entry count {} exceeds maximum {}",
                                delta.entries.len(),
                                MAX_REPORT_ENTRIES
                            )),
                        );
                    }
                    let mut changes = Vec::with_capacity(delta.entries.len());
                    for entry in delta.entries {
                        match Self::proto_to_delta_entry(entry) {
                            Ok(change) => changes.push(change),
                            Err(error) => {
                                return self.metadata_error_response(
                                    &req.header,
                                    block_report_response_with_header,
                                    error,
                                );
                            }
                        }
                    }
                    let result = self.worker_manager.apply_delta_block_report(
                        &group_name,
                        worker_id,
                        worker_run_id,
                        baseline_seq,
                        batch_seq,
                        changes,
                    );
                    (
                        "delta",
                        BlockReportKindProto::BlockReportKindDelta,
                        batch_seq,
                        None,
                        result,
                    )
                }
            };

            let result = match apply_result {
                Ok(result) => result,
                Err(MetadataError::NotFound(message)) => {
                    warn!(
                        target: "metadata.worker",
                        op = "BlockReport",
                        result = "rejected",
                        error_code = "need_register",
                        report_kind,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        baseline_seq,
                        batch_seq,
                        "Block report rejected"
                    );
                    return self.need_register_response(&req.header, block_report_response_with_header, message);
                }
                Err(MetadataError::StaleState(message)) => {
                    warn!(
                        target: "metadata.worker",
                        op = "BlockReport",
                        result = "rejected",
                        error_code = "worker_run_mismatch",
                        report_kind,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        baseline_seq,
                        batch_seq,
                        "Block report rejected"
                    );
                    return self.worker_run_mismatch_response(&req.header, block_report_response_with_header, message);
                }
                Err(MetadataError::FullReportRequired(message)) => {
                    warn!(
                        target: "metadata.worker",
                        op = "BlockReport",
                        result = "rejected",
                        error_code = "full_report_required",
                        report_kind,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        baseline_seq,
                        batch_seq,
                        "Block report rejected"
                    );
                    return self.full_report_required_response(&req.header, block_report_response_with_header, message);
                }
                Err(error) => {
                    return self.metadata_error_response(&req.header, block_report_response_with_header, error)
                }
            };

            observe::record_worker_block_report_blocks("added", result.added_count);
            observe::record_worker_block_report_blocks("removed", result.removed_count);

            let changed_block_count = result.added_count + result.removed_count;
            if changed_block_count > 0 || result.baseline_published {
                if let Some(final_batch) = final_batch {
                    info!(
                        target: "metadata.block",
                        op = "FullBlockReport",
                        result = "processed",
                        error_code = "none",
                        report_kind,
                        client_id = %caller_ctx.client.client_id.as_raw(),
                        call_id = %caller_ctx.client.call_id,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        baseline_seq,
                        batch_seq,
                        final_batch,
                        next_batch_seq = result.next_batch_seq,
                        added_blocks = result.added_count,
                        removed_blocks = result.removed_count,
                        changed_block_count,
                        "Full block report processed"
                    );
                } else {
                    info!(
                        target: "metadata.block",
                        op = "DeltaBlockReport",
                        result = "processed",
                        error_code = "none",
                        report_kind,
                        client_id = %caller_ctx.client.client_id.as_raw(),
                        call_id = %caller_ctx.client.call_id,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        baseline_seq,
                        batch_seq,
                        next_batch_seq = result.next_batch_seq,
                        added_blocks = result.added_count,
                        removed_blocks = result.removed_count,
                        changed_block_count,
                        "Delta block report processed"
                    );
                }
            }

            let baseline_published = match report_kind_proto {
                BlockReportKindProto::BlockReportKindFull => result.baseline_published,
                BlockReportKindProto::BlockReportKindDelta => true,
                BlockReportKindProto::BlockReportKindUnspecified => {
                    unreachable!("validated block report batch always has a concrete kind")
                }
            };
            Response::new(BlockReportResponseProto {
                header: Some(self.create_response_header_from_request(&req.header, Some(&group_name))),
                report_kind: report_kind_proto as i32,
                baseline_seq,
                next_batch_seq: result.next_batch_seq,
                baseline_published,
            })
        }
        .await;
        Self::record_worker_rpc_outcome(
            "block_report",
            MetadataWorkerMetric::BlockReport(metric_kind),
            started,
            &outcome,
            block_report_response_header,
        );
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BlockCleanupConfig;
    use crate::raft::{AppRaftStateMachine, RocksDBStorage};
    use crate::session_registry::SessionRegistry;
    use crate::MountTable;
    use ::beryl_common::error::rpc::{ProtocolErrorKind, RecoveryAction};
    use beryl_common::header::RequestHeader;
    use beryl_proto::common::{BlockIdProto, TierProto};
    use beryl_proto::convert::rpc_error_from_proto;
    use beryl_proto::metadata::{CapacityInfoProto, DeltaBlockReportBatchProto};
    use beryl_types::{BlockIndex, ClientId, InodeId, Tier, WorkerRunId};
    use std::time::Duration;
    use tempfile::TempDir;

    fn assert_error_kind(error: &ErrorDetailProto, expected_kind: ErrorKind) -> RpcErrorDetail {
        let rpc_error = rpc_error_from_proto(error);
        assert_eq!(rpc_error.kind, expected_kind, "{rpc_error:?}");
        rpc_error
    }

    fn assert_error_register_worker(error: &ErrorDetailProto, expected_kind: ErrorKind) -> RpcErrorDetail {
        let rpc_error = assert_error_kind(error, expected_kind);
        assert!(
            matches!(rpc_error.recovery, RecoveryAction::RegisterWorker),
            "{rpc_error:?}"
        );
        rpc_error
    }

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    async fn leader_raft_with_storage(dir: &TempDir) -> (Arc<AppRaftNode>, Arc<RocksDBStorage>) {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::default());
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        let raft_node = Arc::new(
            AppRaftNode::new(1, Arc::clone(&storage), state_machine, mount_table)
                .await
                .unwrap(),
        );
        raft_node
            .initialize_single_node("127.0.0.1:0".to_string())
            .await
            .unwrap();
        for _ in 0..100 {
            if raft_node.is_leader() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(raft_node.is_leader());
        (raft_node, storage)
    }

    async fn nonleader_raft(dir: &TempDir) -> (Arc<AppRaftNode>, Arc<RocksDBStorage>) {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::default());
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        let raft_node = Arc::new(
            AppRaftNode::new(1, Arc::clone(&storage), state_machine, mount_table)
                .await
                .unwrap(),
        );
        assert!(!raft_node.is_leader());
        (raft_node, storage)
    }

    fn worker_service(
        storage: Arc<RocksDBStorage>,
        raft_node: Arc<AppRaftNode>,
        manager: Arc<WorkerManager>,
        group: GroupName,
    ) -> MetadataWorkerServiceImpl {
        let cleanup = Arc::new(BlockCleanupCoordinator::new(
            Arc::clone(&raft_node),
            storage,
            Arc::clone(&manager),
            Arc::new(SessionRegistry::default()),
            group.clone(),
            &BlockCleanupConfig::default(),
        ));
        MetadataWorkerServiceImpl::new(raft_node, manager, group, cleanup)
    }

    fn block_proto(block_id: BlockId) -> BlockIdProto {
        block_id.into()
    }

    fn absent_delta_request(
        group_name: GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        baseline_seq: u64,
        batch_seq: u64,
        block_id: BlockId,
    ) -> BlockReportRequestProto {
        BlockReportRequestProto {
            header: Some(valid_request_header(&group_name, ClientId::new(72))),
            worker_id: worker_id.as_raw(),
            worker_run_id: worker_run_id.to_string(),
            baseline_seq,
            batch: Some(Batch::DeltaReport(DeltaBlockReportBatchProto {
                batch_seq,
                entries: vec![DeltaBlockReportEntryProto {
                    block: Some(Block::Absent(block_proto(block_id))),
                }],
            })),
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

    fn record_heartbeat(worker_manager: &WorkerManager, group_name: &GroupName, worker_id: WorkerId) -> WorkerRunId {
        let descriptor = worker_manager
            .collect_worker_placement_views(group_name)
            .into_iter()
            .find(|view| view.worker_id == worker_id)
            .expect("worker descriptor should be registered");
        let worker_run_id = worker_manager
            .get_registered_run(group_name, worker_id)
            .unwrap_or_else(|| {
                let worker_run_id = worker_run_id_for(group_name, worker_id);
                worker_manager.register_worker_run(group_name, worker_id, descriptor.endpoint.clone(), worker_run_id);
                worker_run_id
            });
        worker_manager
            .record_heartbeat_with_tier_free(
                group_name,
                worker_id,
                worker_run_id,
                1,
                &descriptor.endpoint,
                vec![TierFree {
                    tier: Tier::Hdd,
                    free_bytes: 500,
                }],
            )
            .expect("heartbeat should be accepted");
        worker_run_id
    }

    fn heartbeat_request(
        group_name: GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        heartbeat_seq: u64,
        endpoint_port: u32,
    ) -> HeartbeatRequestProto {
        HeartbeatRequestProto {
            header: Some(valid_request_header(&group_name, ClientId::new(73))),
            worker_id: worker_id.as_raw(),
            worker_run_id: worker_run_id.to_string(),
            heartbeat_seq,
            advertised_endpoint: Some(EndpointProto {
                host: "127.0.0.1".to_string(),
                port: endpoint_port,
            }),
            capacity: Some(CapacityInfoProto {
                tier_free: vec![TierFreeProto {
                    tier: TierProto::TierHdd as i32,
                    free_bytes: 900,
                }],
            }),
        }
    }

    fn valid_request_header(group_name: &GroupName, client_id: ClientId) -> RequestHeaderProto {
        (&RequestHeader::new(client_id).with_group_name(group_name.clone())).into()
    }

    #[tokio::test]
    async fn worker_group_errors_preserve_header_and_leadership_precedence() {
        for leader in [false, true] {
            let dir = TempDir::new().unwrap();
            let (raft_node, storage) = if leader {
                leader_raft_with_storage(&dir).await
            } else {
                nonleader_raft(&dir).await
            };
            let service = worker_service(
                storage,
                Arc::clone(&raft_node),
                Arc::new(WorkerManager::new(60_000)),
                group_name("root"),
            );
            for raw_group in ["", "invalid group"] {
                let mut header = valid_request_header(&group_name("root"), ClientId::new(84));
                header.group_name = raw_group.to_string();
                let registration = service
                    .register_worker(Request::new(RegisterWorkerRequestProto {
                        header: Some(header.clone()),
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let heartbeat = service
                    .heartbeat(Request::new(HeartbeatRequestProto {
                        header: Some(header.clone()),
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let report = service
                    .block_report(Request::new(BlockReportRequestProto {
                        header: Some(header),
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let expected = if raw_group.is_empty() {
                    ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument)
                } else {
                    ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader)
                };
                assert_error_kind(
                    &registration.header.unwrap().error.unwrap(),
                    if raw_group.is_empty() && !leader {
                        ErrorKind::Metadata(MetadataErrorKind::NotLeader)
                    } else {
                        expected
                    },
                );
                assert_error_kind(&heartbeat.header.unwrap().error.unwrap(), expected);
                assert_error_kind(&report.header.unwrap().error.unwrap(), expected);
            }
            raft_node.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn block_reports_reject_zero_inode_id_without_panicking() {
        let dir = TempDir::new().unwrap();
        let (raft_node, storage) = nonleader_raft(&dir).await;
        let service = worker_service(
            storage,
            Arc::clone(&raft_node),
            Arc::new(WorkerManager::new(60_000)),
            group_name("root"),
        );
        let invalid_block = ReportedBlockProto {
            block_id: Some(BlockIdProto {
                inode_id: 0,
                block_index: 0,
            }),
            tier: TierProto::TierHdd as i32,
            state: ReportedBlockStateProto::ReportedBlockStateReady as i32,
            lease_epoch: 1,
            effective_len: 64,
        };
        for batch in [
            Batch::FullReport(beryl_proto::metadata::FullBlockReportBatchProto {
                blocks: vec![invalid_block],
                final_batch: true,
                batch_seq: 0,
            }),
            Batch::DeltaReport(DeltaBlockReportBatchProto {
                entries: vec![DeltaBlockReportEntryProto {
                    block: Some(Block::Present(invalid_block)),
                }],
                batch_seq: 0,
            }),
        ] {
            let response = service
                .block_report(Request::new(BlockReportRequestProto {
                    header: Some(valid_request_header(&group_name("root"), ClientId::new(85))),
                    worker_id: 8,
                    worker_run_id: WorkerRunId::new().to_string(),
                    baseline_seq: 1,
                    batch: Some(batch),
                }))
                .await
                .unwrap()
                .into_inner();
            assert_error_kind(
                &response.header.unwrap().error.unwrap(),
                ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument),
            );
        }
        raft_node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn follower_block_report_updates_local_view() {
        let dir = TempDir::new().unwrap();
        let (raft_node, storage) = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let worker_id = WorkerId::new(8);
        let block_id = BlockId::new(InodeId::new(80), BlockIndex::new(0));
        worker_manager.register_worker_run(
            &group_name("root"),
            worker_id,
            "127.0.0.1:9091".to_string(),
            worker_run_id_for(&group_name("root"), worker_id),
        );
        let worker_run_id = record_heartbeat(&worker_manager, &group_name("root"), worker_id);
        worker_manager
            .receive_full_block_report(
                &group_name("root"),
                worker_id,
                worker_run_id,
                3,
                0,
                true,
                vec![BlockReportBlock {
                    tier: Some(beryl_types::Tier::Hdd),
                    block_id,
                    lease_epoch: 100,
                    block_state: BlockReportBlockState::Ready,
                    effective_len: 64,
                }],
            )
            .unwrap();
        let service = worker_service(storage, raft_node, Arc::clone(&worker_manager), group_name("root"));

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
            &service,
            Request::new(absent_delta_request(
                group_name("root"),
                worker_id,
                worker_run_id,
                3,
                0,
                block_id,
            )),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(response.report_kind(), BlockReportKindProto::BlockReportKindDelta);
        assert_eq!(response.baseline_seq, 3);
        assert_eq!(response.next_batch_seq, 1);
        assert!(response.baseline_published);
        assert!(worker_manager
            .get_block_locations(&group_name("root"), block_id)
            .is_empty());
    }

    #[tokio::test]
    async fn register_worker_publishes_live_run_only_after_raft_success() {
        let dir = TempDir::new().unwrap();
        let (raft_node, storage) = leader_raft_with_storage(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let worker_run_id = test_worker_run_id();
        let service = worker_service(storage, raft_node, Arc::clone(&worker_manager), group_name("root"));

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: Some(valid_request_header(&group_name("root"), ClientId::new(84))),
                worker_id: 124,
                worker_run_id: worker_run_id.to_string(),
                advertised_endpoint: Some(EndpointProto {
                    host: "127.0.0.1".to_string(),
                    port: 9091,
                }),
            }),
        )
        .await
        .expect("register worker response")
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(
            worker_manager
                .collect_worker_placement_views(&group_name("root"))
                .into_iter()
                .find(|view| view.worker_id == WorkerId::new(124))
                .expect("published descriptor")
                .endpoint,
            "127.0.0.1:9091"
        );
        assert_eq!(
            worker_manager
                .get_registered_run(&group_name("root"), WorkerId::new(124))
                .expect("published live run"),
            worker_run_id
        );
    }

    #[tokio::test]
    async fn heartbeat_maps_registration_mismatches_to_recovery_headers() {
        let dir = TempDir::new().unwrap();
        let (raft_node, storage) = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let group_name = group_name("root");
        worker_manager.register_worker_run(
            &group_name,
            WorkerId::new(11),
            "127.0.0.1:9090".to_string(),
            test_worker_run_id(),
        );
        worker_manager.register_worker_run(
            &group_name,
            WorkerId::new(9),
            "127.0.0.1:9099".to_string(),
            test_worker_run_id(),
        );
        let service = worker_service(storage, raft_node, worker_manager, group_name.clone());

        let run_mismatch = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name.clone(),
                WorkerId::new(11),
                second_worker_run_id(),
                1,
                9090,
            )),
        )
        .await
        .expect("run mismatch returns gRPC OK")
        .into_inner();
        let error = run_mismatch.header.expect("header").error.expect("header error");
        assert_error_register_worker(&error, ErrorKind::Worker(WorkerErrorKind::RunMismatch));

        let descriptor_mismatch = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name,
                WorkerId::new(9),
                test_worker_run_id(),
                1,
                9098,
            )),
        )
        .await
        .expect("descriptor mismatch returns gRPC OK")
        .into_inner();
        let error = descriptor_mismatch.header.expect("header").error.expect("header error");
        assert_error_register_worker(&error, ErrorKind::Worker(WorkerErrorKind::DescriptorMismatch));
    }

    #[tokio::test]
    async fn heartbeat_accepts_liveness_without_raft_propose_for_leader_and_follower() {
        for (worker_id, leader, heartbeat_seq) in [(WorkerId::new(12), false, 7), (WorkerId::new(13), true, 1)] {
            let dir = TempDir::new().unwrap();
            let (raft_node, storage) = if leader {
                leader_raft_with_storage(&dir).await
            } else {
                nonleader_raft(&dir).await
            };
            let before_state_id = raft_node.get_last_applied_state_id();
            let worker_manager = Arc::new(WorkerManager::new(1_500));
            worker_manager.register_worker_run(
                &group_name("root"),
                worker_id,
                "127.0.0.1:9090".to_string(),
                test_worker_run_id(),
            );
            let service = worker_service(
                storage,
                Arc::clone(&raft_node),
                Arc::clone(&worker_manager),
                group_name("root"),
            );

            let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
                &service,
                Request::new(heartbeat_request(
                    group_name("root"),
                    worker_id,
                    test_worker_run_id(),
                    heartbeat_seq,
                    9090,
                )),
            )
            .await
            .expect("heartbeat succeeds")
            .into_inner();

            assert!(response.header.as_ref().expect("header").error.is_none());
            assert_eq!(response.header.as_ref().expect("header").group_name, "root");
            assert_eq!(response.worker_id, worker_id.as_raw());
            assert_eq!(response.accepted_worker_run_id, test_worker_run_id().to_string());
            assert_eq!(response.liveness_timeout_ms, 1_500);
            assert!(worker_manager.is_worker_live(&group_name("root"), worker_id));
            if leader {
                assert_eq!(raft_node.get_last_applied_state_id(), before_state_id);
            }
        }
    }

    #[tokio::test]
    async fn heartbeat_returns_due_cleanup_command_for_the_accepted_ready_replica() {
        let dir = TempDir::new().unwrap();
        let (raft_node, storage) = leader_raft_with_storage(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let group = group_name("root");
        let worker_id = WorkerId::new(21);
        let worker_run_id = test_worker_run_id();
        worker_manager.register_worker_run(&group, worker_id, "127.0.0.1:9090".to_string(), worker_run_id);

        let cleanup_config = BlockCleanupConfig {
            enabled: true,
            reclaim_grace_ms: 1,
            ..BlockCleanupConfig::default()
        };
        let cleanup = Arc::new(BlockCleanupCoordinator::new(
            Arc::clone(&raft_node),
            storage,
            Arc::clone(&worker_manager),
            Arc::new(SessionRegistry::default()),
            group.clone(),
            &cleanup_config,
        ));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            group.clone(),
            Arc::clone(&cleanup),
        );

        <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(group.clone(), worker_id, worker_run_id, 1, 9090)),
        )
        .await
        .expect("initial heartbeat succeeds");

        let block_id = BlockId::new(InodeId::new(700), BlockIndex::new(0));
        let lease_epoch = 991;
        worker_manager
            .receive_full_block_report(
                &group,
                worker_id,
                worker_run_id,
                1,
                0,
                true,
                vec![BlockReportBlock {
                    tier: Some(beryl_types::Tier::Hdd),
                    block_id,
                    lease_epoch,
                    block_state: BlockReportBlockState::Ready,
                    effective_len: 64,
                }],
            )
            .unwrap();
        cleanup.scan_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        cleanup.scan_once().await.unwrap();

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(group, worker_id, worker_run_id, 2, 9090)),
        )
        .await
        .expect("heartbeat succeeds")
        .into_inner();

        assert_eq!(response.cleanup_commands.len(), 1);
        assert_eq!(response.cleanup_commands[0], block_id.into());
    }
}
