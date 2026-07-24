// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! MetadataWorkerService implementation.

use super::manager::{
    worker_net_protocol_label, BlockReportBlock, BlockReportBlockState, BlockReportDeltaEntry, BlockReportDeltaOp,
    WorkerManager, WORKER_NET_PROTOCOL_GRPC,
};
use super::metrics::WorkerMetrics;
use crate::error::{to_rpc_error, MetadataError, MetadataResult};
use crate::observe;
use crate::raft::Command;
use crate::raft::{AppRaftNode, CommandResult};
use crate::service::extract_and_inject_context;
use ::beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RpcErrorDetail, WorkerErrorKind};
use ::beryl_common::header::ResponseHeader;
use ::beryl_common::observe::propagation::{extract_trace_context, ExtractedContext};
use beryl_proto::convert::require_worker_run_id;
use beryl_proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProto;
use beryl_proto::metadata::*;
use beryl_types::{BlockId, GroupName, TierFree, WorkerId};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};

/// Worker service background task handles.
pub struct WorkerBackgroundHandle {}

impl WorkerBackgroundHandle {
    pub fn task_count(&self) -> usize {
        0
    }
}

fn register_worker_response_with_header(
    header: beryl_proto::common::ResponseHeaderProto,
) -> RegisterWorkerResponseProto {
    RegisterWorkerResponseProto {
        header: Some(header),
        ..Default::default()
    }
}

fn heartbeat_response_with_header(header: beryl_proto::common::ResponseHeaderProto) -> HeartbeatResponseProto {
    HeartbeatResponseProto {
        header: Some(header),
        ..Default::default()
    }
}

fn block_report_response_with_header(header: beryl_proto::common::ResponseHeaderProto) -> BlockReportResponseProto {
    BlockReportResponseProto {
        header: Some(header),
        ..Default::default()
    }
}

fn register_worker_response_header(
    response: &RegisterWorkerResponseProto,
) -> Option<&beryl_proto::common::ResponseHeaderProto> {
    response.header.as_ref()
}

fn heartbeat_response_header(response: &HeartbeatResponseProto) -> Option<&beryl_proto::common::ResponseHeaderProto> {
    response.header.as_ref()
}

fn block_report_response_header(
    response: &BlockReportResponseProto,
) -> Option<&beryl_proto::common::ResponseHeaderProto> {
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
    metrics: Arc<WorkerMetrics>,
    slot_metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
    /// Mount table used to compute mount_epoch for lease gating.
    _mount_table: Arc<crate::mount::MountTable>,
    served_group_name: GroupName,
    registration_serial: tokio::sync::Mutex<()>,
}

impl MetadataWorkerServiceImpl {
    pub(crate) fn new(
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
            registration_serial: tokio::sync::Mutex::new(()),
        }
    }

    /// Set slot metrics (called after metrics are available).
    pub(crate) fn set_slot_metrics(&mut self, metrics: Arc<crate::metrics::MetadataMetrics>) {
        self.slot_metrics = Some(metrics);
    }

    /// Helper: create a response header from request header with group name.
    fn create_response_header_from_request(
        &self,
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        group_name: Option<&GroupName>,
    ) -> beryl_proto::common::ResponseHeaderProto {
        let mut header: beryl_proto::common::ResponseHeaderProto = req_header
            .as_ref()
            .and_then(|h| h.client.as_ref())
            .and_then(|c| ::beryl_common::header::ClientInfo::try_from(c.clone()).ok())
            .map(|client| (&ResponseHeader::ok(client)).into())
            .unwrap_or_default();
        if let Some(group_name) = group_name {
            header.group_name = group_name.to_string();
        }
        if self.raft_node.is_leader() {
            if let (Some(group_name), Some(sid)) = (group_name, self.raft_node.get_last_applied_state_id()) {
                header.state = vec![(&beryl_types::GroupStateWatermark::new(group_name.clone(), sid)).into()];
            }
        }
        header
    }

    fn group_name_from_request_header(
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
    ) -> Option<GroupName> {
        req_header
            .as_ref()
            .and_then(|header| GroupName::parse_optional(&header.group_name).ok().flatten())
    }

    fn error_response_header_from_request(
        &self,
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        error: RpcErrorDetail,
    ) -> beryl_proto::common::ResponseHeaderProto {
        let mut header = self
            .create_response_header_from_request(req_header, Self::group_name_from_request_header(req_header).as_ref());
        header.error = Some(beryl_proto::convert::rpc_error_to_proto(&error));
        header
    }

    fn response_with_error<T>(
        &self,
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        error: RpcErrorDetail,
        make_response: fn(beryl_proto::common::ResponseHeaderProto) -> T,
    ) -> Result<Response<T>, Status> {
        Ok(Response::new(make_response(
            self.error_response_header_from_request(req_header, error),
        )))
    }

    fn invalid_request_response<T>(
        &self,
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        make_response: fn(beryl_proto::common::ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            to_rpc_error(MetadataError::InvalidArgument(message.into())),
            make_response,
        )
    }

    fn metadata_error_response<T>(
        &self,
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        make_response: fn(beryl_proto::common::ResponseHeaderProto) -> T,
        error: MetadataError,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(req_header, to_rpc_error(error), make_response)
    }

    fn group_mismatch_response<T>(
        &self,
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        make_response: fn(beryl_proto::common::ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::fail(ErrorKind::Metadata(MetadataErrorKind::GroupMismatch), message),
            make_response,
        )
    }

    fn need_register_response<T>(
        &self,
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        make_response: fn(beryl_proto::common::ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::NotRegistered), message),
            make_response,
        )
    }

    fn worker_run_mismatch_response<T>(
        &self,
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        make_response: fn(beryl_proto::common::ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::RunMismatch), message),
            make_response,
        )
    }

    fn worker_descriptor_mismatch_response<T>(
        &self,
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        make_response: fn(beryl_proto::common::ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::DescriptorMismatch), message),
            make_response,
        )
    }

    /// Start worker-local background tasks.
    pub(crate) fn start_background_tasks(&self) -> WorkerBackgroundHandle {
        WorkerBackgroundHandle {}
    }

    fn liveness_timeout_ms(&self) -> u32 {
        self.worker_manager
            .heartbeat_timeout_sec()
            .saturating_mul(1000)
            .try_into()
            .unwrap_or(u32::MAX)
    }

    fn full_report_required_response<T>(
        &self,
        req_header: &Option<beryl_proto::common::RequestHeaderProto>,
        make_response: fn(beryl_proto::common::ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::send_full_block_report(ErrorKind::Worker(WorkerErrorKind::FullReportRequired), message),
            make_response,
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
            block_stamp: block.block_stamp,
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

    fn record_worker_rpc_outcome<T>(
        method: &'static str,
        metric: MetadataWorkerMetric,
        started: Instant,
        outcome: &Result<Response<T>, Status>,
        response_header: fn(&T) -> Option<&beryl_proto::common::ResponseHeaderProto>,
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
    outcome: &Result<Response<T>, Status>,
    response_header: fn(&T) -> Option<&beryl_proto::common::ResponseHeaderProto>,
) -> (&'static str, &'static str) {
    match outcome {
        Ok(response) => match response_header(response.get_ref()).and_then(|header| header.error.as_ref()) {
            Some(error) => ("error", metadata_worker_error_detail_kind(error)),
            None => ("ok", "none"),
        },
        Err(status) => ("error", tonic_status_error_kind(status)),
    }
}

fn metadata_worker_error_detail_kind(error: &beryl_proto::common::ErrorDetailProto) -> &'static str {
    let rpc_error = beryl_proto::convert::rpc_error_from_proto(error);
    observe::rpc_error_kind(&rpc_error)
}

fn tonic_status_error_kind(status: &Status) -> &'static str {
    match status.code() {
        tonic::Code::Ok => "none",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::NotFound => "not_found",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DeadlineExceeded => "timeout",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Cancelled => "cancelled",
        tonic::Code::Internal => "internal",
        _ => "rpc_status",
    }
}

fn block_report_kind(req: &BlockReportRequestProto) -> &'static str {
    match &req.report {
        Some(block_report_request_proto::Report::Full(_)) => "full",
        Some(block_report_request_proto::Report::Delta(_)) => "delta",
        None => "unknown",
    }
}

fn merge_request_header_transport_context(
    header: &mut Option<beryl_proto::common::RequestHeaderProto>,
    context: &ExtractedContext,
) {
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
    if trace_context.tracestate.is_none() {
        trace_context.tracestate = context.tracestate.clone();
    }
    if trace_context.baggage.is_none() {
        trace_context.baggage = context.baggage.clone();
    }
}

fn trace_context_proto_is_empty(context: &beryl_proto::common::TraceContextProto) -> bool {
    context.traceparent.is_none() && context.tracestate.is_none() && context.baggage.is_none()
}

fn validate_advertised_endpoint(endpoint: beryl_proto::common::EndpointProto) -> Result<String, String> {
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

fn parse_worker_request_group_name(
    req_header: &Option<beryl_proto::common::RequestHeaderProto>,
) -> Result<GroupName, String> {
    let header = req_header
        .as_ref()
        .ok_or_else(|| "request header is required".to_string())?;
    GroupName::parse(&header.group_name).map_err(|error| format!("header group_name is invalid: {error}"))
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
            let _caller_ctx = match extract_and_inject_context(&req.header) {
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

            let group_name = match parse_worker_request_group_name(&req.header) {
                Ok(group_name) => group_name,
                Err(error) => {
                    return self.invalid_request_response(&req.header, register_worker_response_with_header, error)
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
            let worker_net_protocol = WORKER_NET_PROTOCOL_GRPC;
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
                worker_net_protocol,
            ) {
                return self.metadata_error_response(&req.header, register_worker_response_with_header, error);
            }

            let command = Command::RegisterWorkerDescriptor {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                group_name: group_name.clone(),
                worker_id,
                address: address.clone(),
                worker_net_protocol,
                fault_domain: None,
            };

            let accepted_worker_id = match self.raft_node.propose(command).await {
                Ok(CommandResult::WorkerUpserted(worker_id)) => worker_id,
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
            if accepted_worker_id != worker_id {
                return self.metadata_error_response(
                    &req.header,
                    register_worker_response_with_header,
                    MetadataError::Internal(format!(
                        "RegisterWorker returned worker_id {}, expected {}",
                        accepted_worker_id.as_raw(),
                        worker_id.as_raw()
                    )),
                );
            }
            if let Err(error) = self.worker_manager.register_worker_run(
                &group_name,
                accepted_worker_id,
                address.clone(),
                worker_net_protocol,
                worker_run_id,
                None,
            ) {
                return self.metadata_error_response(&req.header, register_worker_response_with_header, error);
            }

            info!(
                target: "metadata.worker",
                op = "RegisterWorker",
                result = "accepted",
                error_code = "none",
                event = "worker_registered",
                group_name = %group_name,
                worker_id = accepted_worker_id.as_raw(),
                worker_run_id = %worker_run_id,
                endpoint = %address,
                protocol = worker_net_protocol_label(worker_net_protocol),
                "Worker registered"
            );

            Ok(Response::new(RegisterWorkerResponseProto {
                header: Some(self.create_response_header_from_request(&req.header, Some(&group_name))),
                worker_id: accepted_worker_id.as_raw(),
                accepted_worker_run_id: worker_run_id.to_string(),
            }))
        }
        .await;
        Self::record_worker_rpc_outcome(
            "register_worker",
            MetadataWorkerMetric::Registration,
            started,
            &outcome,
            register_worker_response_header,
        );
        outcome
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
            let _caller_ctx = match extract_and_inject_context(&req.header) {
                Ok(ctx) => ctx,
                Err(error) => return self.response_with_error(&req.header, error, heartbeat_response_with_header),
            };

            let group_name = match parse_worker_request_group_name(&req.header) {
                Ok(group_name) => group_name,
                Err(error) => return self.invalid_request_response(&req.header, heartbeat_response_with_header, error),
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

            let load = match req.load {
                Some(load) => load,
                None => {
                    return self.invalid_request_response(&req.header, heartbeat_response_with_header, "Missing load")
                }
            };

            let health_proto = req.health();
            let worker_net_protocol = WORKER_NET_PROTOCOL_GRPC;
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
            self.worker_manager.expire_liveness();

            let descriptor = match self.worker_manager.get_descriptor(&group_name, worker_id) {
                Some(descriptor) => descriptor,
                None => {
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
                    return self.need_register_response(
                        &req.header,
                        heartbeat_response_with_header,
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
                    return self.need_register_response(
                        &req.header,
                        heartbeat_response_with_header,
                        format!(
                            "live worker registration not found for group_name={}, worker_id={}",
                            group_name,
                            worker_id.as_raw()
                        ),
                    );
                }
            };
            if !registration.worker_run_id.matches(worker_run_id) {
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
                        expected_worker_run_id = %registration.worker_run_id,
                        "Heartbeat rejected"
                    );
                }
                return self.worker_run_mismatch_response(
                    &req.header,
                    heartbeat_response_with_header,
                    format!(
                        "worker_run_id mismatch for group_name={}, worker_id={}",
                        group_name,
                        worker_id.as_raw()
                    ),
                );
            }
            if descriptor.address != advertised_endpoint || descriptor.worker_net_protocol != worker_net_protocol {
                return self.worker_descriptor_mismatch_response(
                    &req.header,
                    heartbeat_response_with_header,
                    format!(
                        "advertised endpoint or protocol does not match registration for group_name={}, worker_id={}",
                        group_name,
                        worker_id.as_raw()
                    ),
                );
            }

            use super::manager::HealthStatus;
            let health_status = HealthStatus::from(health_proto as i32);

            let live_state = match self.worker_manager.record_heartbeat_with_tier_free(
                &group_name,
                worker_id,
                worker_run_id,
                req.heartbeat_seq,
                &advertised_endpoint,
                worker_net_protocol,
                capacity.total_bytes,
                capacity.used_bytes,
                capacity.available_bytes,
                tier_free,
                load.active_reads,
                load.active_writes,
                health_status,
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

            // Update metrics (all nodes)
            let live_count = self.worker_manager.list_live_workers().len();
            self.metrics.update_worker_live(live_count);
            observe::set_worker_live(live_count);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(live_state.last_seen_ms);
            observe::record_worker_heartbeat_lag(now_ms.saturating_sub(live_state.last_seen_ms) as f64 / 1000.0);

            Ok(Response::new(HeartbeatResponseProto {
                header: Some(self.create_response_header_from_request(&req.header, Some(&group_name))),
                worker_id: live_state.worker_id.as_raw(),
                accepted_worker_run_id: live_state.worker_run_id.to_string(),
                liveness_timeout_ms: self.liveness_timeout_ms(),
            }))
        }
        .await;
        Self::record_worker_rpc_outcome(
            "heartbeat",
            MetadataWorkerMetric::Heartbeat,
            started,
            &outcome,
            heartbeat_response_header,
        );
        outcome
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
            let _caller_ctx = match extract_and_inject_context(&req.header) {
                Ok(ctx) => ctx,
                Err(error) => return self.response_with_error(&req.header, error, block_report_response_with_header),
            };

            let group_name = match parse_worker_request_group_name(&req.header) {
                Ok(group_name) => group_name,
                Err(error) => {
                    return self.invalid_request_response(&req.header, block_report_response_with_header, error)
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
            let report_seq = req.report_seq;
            let Some(report) = req.report else {
                return self.invalid_request_response(
                    &req.header,
                    block_report_response_with_header,
                    "block report body is required",
                );
            };

            let (result, report_kind, batch_seq, delta_seq, final_batch) = match report {
                block_report_request_proto::Report::Full(full) => {
                    let batch_seq = full.batch_seq;
                    let final_batch = full.final_batch;
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
                    let result = match self.worker_manager.receive_full_block_report(
                        &group_name,
                        worker_id,
                        worker_run_id,
                        report_seq,
                        full.batch_seq,
                        full.final_batch,
                        blocks,
                    ) {
                        Ok(result) => result,
                        Err(MetadataError::NotFound(message)) => {
                            warn!(
                                target: "metadata.worker",
                                op = "BlockReport",
                                result = "rejected",
                                error_code = "need_register",
                                report_kind = "full",
                                group_name = %group_name,
                                worker_id = worker_id.as_raw(),
                                worker_run_id = %worker_run_id,
                                report_seq,
                                batch_seq,
                                final_batch,
                                "Block report rejected"
                            );
                            return self.need_register_response(
                                &req.header,
                                block_report_response_with_header,
                                message,
                            );
                        }
                        Err(MetadataError::StaleState(message)) => {
                            warn!(
                                target: "metadata.worker",
                                op = "BlockReport",
                                result = "rejected",
                                error_code = "worker_run_mismatch",
                                report_kind = "full",
                                group_name = %group_name,
                                worker_id = worker_id.as_raw(),
                                worker_run_id = %worker_run_id,
                                report_seq,
                                batch_seq,
                                final_batch,
                                "Block report rejected"
                            );
                            return self.worker_run_mismatch_response(
                                &req.header,
                                block_report_response_with_header,
                                message,
                            );
                        }
                        Err(MetadataError::FullReportRequired(message)) => {
                            warn!(
                                target: "metadata.worker",
                                op = "BlockReport",
                                result = "rejected",
                                error_code = "full_report_required",
                                report_kind = "full",
                                group_name = %group_name,
                                worker_id = worker_id.as_raw(),
                                worker_run_id = %worker_run_id,
                                report_seq,
                                batch_seq,
                                final_batch,
                                "Block report rejected"
                            );
                            return self.full_report_required_response(
                                &req.header,
                                block_report_response_with_header,
                                message,
                            );
                        }
                        Err(error) => {
                            return self.metadata_error_response(&req.header, block_report_response_with_header, error)
                        }
                    };
                    (result, "full", Some(batch_seq), None, Some(final_batch))
                }
                block_report_request_proto::Report::Delta(delta) => {
                    let delta_seq = delta.delta_seq;
                    let mut deltas = Vec::with_capacity(delta.deltas.len());
                    for delta in delta.deltas {
                        match Self::proto_to_delta(delta) {
                            Ok(delta) => deltas.push(delta),
                            Err(error) => {
                                return self.metadata_error_response(
                                    &req.header,
                                    block_report_response_with_header,
                                    error,
                                );
                            }
                        }
                    }
                    let result = match self.worker_manager.apply_delta_block_report(
                        &group_name,
                        worker_id,
                        worker_run_id,
                        report_seq,
                        delta_seq,
                        deltas,
                    ) {
                        Ok(result) => result,
                        Err(MetadataError::NotFound(message)) => {
                            warn!(
                                target: "metadata.worker",
                                op = "BlockReport",
                                result = "rejected",
                                error_code = "need_register",
                                report_kind = "delta",
                                group_name = %group_name,
                                worker_id = worker_id.as_raw(),
                                worker_run_id = %worker_run_id,
                                report_seq,
                                delta_seq,
                                "Block report rejected"
                            );
                            return self.need_register_response(
                                &req.header,
                                block_report_response_with_header,
                                message,
                            );
                        }
                        Err(MetadataError::StaleState(message)) => {
                            warn!(
                                target: "metadata.worker",
                                op = "BlockReport",
                                result = "rejected",
                                error_code = "worker_run_mismatch",
                                report_kind = "delta",
                                group_name = %group_name,
                                worker_id = worker_id.as_raw(),
                                worker_run_id = %worker_run_id,
                                report_seq,
                                delta_seq,
                                "Block report rejected"
                            );
                            return self.worker_run_mismatch_response(
                                &req.header,
                                block_report_response_with_header,
                                message,
                            );
                        }
                        Err(MetadataError::FullReportRequired(message)) => {
                            warn!(
                                target: "metadata.worker",
                                op = "BlockReport",
                                result = "rejected",
                                error_code = "full_report_required",
                                report_kind = "delta",
                                group_name = %group_name,
                                worker_id = worker_id.as_raw(),
                                worker_run_id = %worker_run_id,
                                report_seq,
                                delta_seq,
                                "Block report rejected"
                            );
                            return self.full_report_required_response(
                                &req.header,
                                block_report_response_with_header,
                                message,
                            );
                        }
                        Err(error) => {
                            return self.metadata_error_response(&req.header, block_report_response_with_header, error)
                        }
                    };
                    (result, "delta", None, Some(delta_seq), None)
                }
            };

            // In-memory worker state counters.
            let total_blocks = result.added_blocks.len() + result.removed_blocks.len();
            self.metrics.record_blockreport_blocks(total_blocks as u64);
            let locations_size = self.worker_manager.get_all_locations_count();
            self.metrics.update_locations_size(locations_size);

            observe::record_worker_block_report_blocks("added", result.added_blocks.len());
            observe::record_worker_block_report_blocks("removed", result.removed_blocks.len());

            let changed_block_count = result.added_blocks.len() + result.removed_blocks.len();
            let full_baseline_changed = matches!((batch_seq, final_batch), (Some(_), Some(true)))
                && (result.baseline_established || result.baseline_replaced);
            if changed_block_count > 0 || full_baseline_changed {
                if let (Some(batch_seq), Some(final_batch)) = (batch_seq, final_batch) {
                    info!(
                        target: "metadata.block",
                        op = "FullBlockReport",
                        result = "processed",
                        error_code = "none",
                        report_kind,
                        client_id = %_caller_ctx.client.client_id.as_raw(),
                        call_id = %_caller_ctx.client.call_id,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        report_seq,
                        batch_seq,
                        final_batch,
                        next_delta_seq = result.next_delta_seq,
                        added_blocks = result.added_blocks.len(),
                        removed_blocks = result.removed_blocks.len(),
                        changed_block_count,
                        "Full block report processed"
                    );
                } else if let Some(delta_seq) = delta_seq {
                    info!(
                        target: "metadata.block",
                        op = "DeltaBlockReport",
                        result = "processed",
                        error_code = "none",
                        report_kind,
                        client_id = %_caller_ctx.client.client_id.as_raw(),
                        call_id = %_caller_ctx.client.call_id,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        report_seq,
                        delta_seq,
                        next_delta_seq = result.next_delta_seq,
                        added_blocks = result.added_blocks.len(),
                        removed_blocks = result.removed_blocks.len(),
                        changed_block_count,
                        "Delta block report processed"
                    );
                }
            }

            Ok(Response::new(BlockReportResponseProto {
                header: Some(self.create_response_header_from_request(&req.header, Some(&group_name))),
                report_seq,
                next_delta_seq: result.next_delta_seq,
            }))
        }
        .await;
        Self::record_worker_rpc_outcome(
            "block_report",
            MetadataWorkerMetric::BlockReport(metric_kind),
            started,
            &outcome,
            block_report_response_header,
        );
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{AppRaftStateMachine, RocksDBStorage};
    use crate::worker::HealthStatus;
    use crate::MountTable;
    use ::beryl_common::error::rpc::{InternalErrorKind, ProtocolErrorKind, RecoveryAction};
    use beryl_proto::convert::rpc_error_from_proto;
    use beryl_types::ClientId;
    use beryl_types::WorkerRunId;
    use metrics::{
        Counter, CounterFn, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };
    use std::io;
    use std::sync::{Mutex, OnceLock};
    use tempfile::TempDir;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::{filter::LevelFilter, fmt, layer::SubscriberExt, Layer, Registry};

    #[derive(Clone)]
    struct LogCaptureWriter {
        output: Arc<Mutex<Vec<u8>>>,
    }

    impl LogCaptureWriter {
        fn new(output: Arc<Mutex<Vec<u8>>>) -> Self {
            Self { output }
        }
    }

    impl io::Write for LogCaptureWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.output
                .lock()
                .expect("log output must not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn captured_logs(output: &Arc<Mutex<Vec<u8>>>) -> Vec<serde_json::Value> {
        let bytes = output.lock().expect("log output must not be poisoned").clone();
        let text = String::from_utf8(bytes).expect("logs must be utf8");
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).unwrap_or_else(|err| panic!("invalid json log {line:?}: {err}")))
            .collect()
    }

    fn captured_text(output: &Arc<Mutex<Vec<u8>>>) -> String {
        let bytes = output.lock().expect("log output must not be poisoned").clone();
        String::from_utf8(bytes).expect("logs must be utf8")
    }

    fn log_test_mutex() -> &'static tokio::sync::Mutex<()> {
        static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    fn captured_json_subscriber(output: &Arc<Mutex<Vec<u8>>>) -> tracing::Dispatch {
        let writer = LogCaptureWriter::new(Arc::clone(output));
        let subscriber = Registry::default().with(
            fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_ansi(false)
                .with_target(true)
                .with_file(false)
                .with_line_number(false)
                .with_writer(move || writer.clone())
                .with_filter(LevelFilter::INFO),
        );
        tracing::Dispatch::new(subscriber)
    }

    fn captured_text_subscriber(output: &Arc<Mutex<Vec<u8>>>) -> tracing::Dispatch {
        let writer = LogCaptureWriter::new(Arc::clone(output));
        let subscriber = Registry::default().with(
            fmt::layer()
                .compact()
                .with_ansi(false)
                .with_target(true)
                .with_file(false)
                .with_line_number(false)
                .with_writer(move || writer.clone())
                .with_filter(LevelFilter::INFO),
        );
        tracing::Dispatch::new(subscriber)
    }

    fn assert_error_kind(error: &beryl_proto::common::ErrorDetailProto, expected_kind: ErrorKind) -> RpcErrorDetail {
        let rpc_error = rpc_error_from_proto(error);
        assert_eq!(rpc_error.kind, expected_kind, "{rpc_error:?}");
        rpc_error
    }

    fn assert_error_fail(error: &beryl_proto::common::ErrorDetailProto, expected_kind: ErrorKind) -> RpcErrorDetail {
        let rpc_error = assert_error_kind(error, expected_kind);
        assert!(matches!(rpc_error.recovery, RecoveryAction::Fail), "{rpc_error:?}");
        rpc_error
    }

    fn assert_error_refresh_metadata(
        error: &beryl_proto::common::ErrorDetailProto,
        expected_kind: ErrorKind,
    ) -> RpcErrorDetail {
        let rpc_error = assert_error_kind(error, expected_kind);
        assert!(
            matches!(rpc_error.recovery, RecoveryAction::RefreshMetadata { .. }),
            "{rpc_error:?}"
        );
        rpc_error
    }

    fn assert_error_register_worker(
        error: &beryl_proto::common::ErrorDetailProto,
        expected_kind: ErrorKind,
    ) -> RpcErrorDetail {
        let rpc_error = assert_error_kind(error, expected_kind);
        assert!(
            matches!(rpc_error.recovery, RecoveryAction::RegisterWorker),
            "{rpc_error:?}"
        );
        rpc_error
    }

    fn header_client_identity(header: &beryl_proto::common::RequestHeaderProto) -> (String, String) {
        let client = ::beryl_common::header::ClientInfo::try_from(header.client.clone().expect("client info"))
            .expect("valid client info");
        (client.client_id.as_raw().to_string(), client.call_id.to_string())
    }

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    async fn leader_raft(dir: &TempDir) -> Arc<AppRaftNode> {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = crate::config::RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(1, storage, state_machine, mount_table, &raft_config)
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
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
        assert!(raft_node.is_leader());
        raft_node
    }

    async fn nonleader_raft(dir: &TempDir) -> Arc<AppRaftNode> {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = crate::config::RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(1, storage, state_machine, mount_table, &raft_config)
                .await
                .unwrap(),
        );
        assert!(!raft_node.is_leader());
        raft_node
    }

    fn block_proto(block_id: BlockId) -> beryl_proto::common::BlockIdProto {
        block_id.into()
    }

    fn report_block_proto(block_id: BlockId) -> BlockReportBlockProto {
        BlockReportBlockProto {
            block_id: Some(block_proto(block_id)),
            block_stamp: 100 + u64::from(block_id.index.as_raw()),
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
        BlockReportRequestProto {
            header: Some(valid_request_header(&group_name, ClientId::new(71))),
            worker_id: worker_id.as_raw(),
            worker_run_id: worker_run_id.to_string(),
            report_seq,
            report: Some(block_report_request_proto::Report::Full(FullBlockReportBatchProto {
                batch_seq,
                final_batch,
                blocks: blocks.into_iter().map(report_block_proto).collect(),
            })),
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
        BlockReportRequestProto {
            header: Some(valid_request_header(&group_name, ClientId::new(72))),
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
        HeartbeatRequestProto {
            header: Some(valid_request_header(&group_name, ClientId::new(73))),
            worker_id: worker_id.as_raw(),
            worker_run_id: worker_run_id.to_string(),
            heartbeat_seq,
            advertised_endpoint: Some(beryl_proto::common::EndpointProto {
                host: "127.0.0.1".to_string(),
                port: endpoint_port,
            }),
            capacity: Some(CapacityInfoProto {
                total_bytes: 1_000,
                used_bytes: 100,
                available_bytes: 900,
                tier_free: vec![TierFreeProto {
                    tier: beryl_proto::common::TierProto::TierHdd as i32,
                    free_bytes: 900,
                }],
            }),
            load: Some(LoadInfoProto {
                active_reads: 0,
                active_writes: 0,
            }),
            health: HealthStatusProto::HealthStatusHealthy as i32,
        }
    }

    fn valid_request_header(group_name: &GroupName, client_id: ClientId) -> beryl_proto::common::RequestHeaderProto {
        (&::beryl_common::header::RequestHeader::new(client_id).with_group_name(group_name.clone())).into()
    }

    #[test]
    fn merge_transport_context_preserves_call_id_and_trace_context_boundary() {
        let mut header = Some(valid_request_header(&group_name("root"), ClientId::new(90)));
        let original_call_id = header
            .as_ref()
            .and_then(|header| header.client.as_ref())
            .expect("client info")
            .call_id
            .clone();
        let context = ExtractedContext {
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string()),
            tracestate: Some("vendor=state".to_string()),
            baggage: Some("tenant=local".to_string()),
        };

        merge_request_header_transport_context(&mut header, &context);

        let header = header.expect("merged header");
        let trace_context = header.trace_context.expect("trace context");
        assert_eq!(
            trace_context.traceparent.as_deref(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
        assert_eq!(trace_context.tracestate.as_deref(), Some("vendor=state"));
        assert_eq!(trace_context.baggage.as_deref(), Some("tenant=local"));
        assert_eq!(header.client.expect("client info").call_id, original_call_id);
    }

    fn register_request_with_header(
        header: Option<beryl_proto::common::RequestHeaderProto>,
        worker_id: WorkerId,
    ) -> RegisterWorkerRequestProto {
        RegisterWorkerRequestProto {
            header,
            worker_id: worker_id.as_raw(),
            worker_run_id: test_worker_run_id().to_string(),
            advertised_endpoint: Some(beryl_proto::common::EndpointProto {
                host: "127.0.0.1".to_string(),
                port: 9090 + worker_id.as_raw() as u32,
            }),
        }
    }

    fn assert_invalid_header(error: &beryl_proto::common::ErrorDetailProto, expected_message: &str) {
        let error = assert_error_fail(error, ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader));
        assert!(
            error.message.contains(expected_message),
            "message {:?} did not contain {:?}",
            error.message,
            expected_message
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn register_worker_accepted_emits_metadata_worker_log() {
        let _log_guard = log_test_mutex().lock().await;
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );
        let output = Arc::new(Mutex::new(Vec::new()));
        let dispatch = captured_json_subscriber(&output);

        async {
            let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
                &service,
                Request::new(register_request_with_header(
                    Some(valid_request_header(&group_name("root"), ClientId::new(181))),
                    WorkerId::new(181),
                )),
            )
            .await
            .expect("register worker response")
            .into_inner();
            assert!(response.header.expect("header").error.is_none());
        }
        .with_subscriber(dispatch)
        .await;

        let logs = captured_logs(&output);
        assert!(
            logs.iter().any(|log| {
                log["target"] == "metadata.worker"
                    && log["level"] == "INFO"
                    && log["op"] == "RegisterWorker"
                    && log["result"] == "accepted"
                    && log["error_code"] == "none"
                    && log["group_name"] == "root"
                    && log.get("group_id").is_none()
                    && log["worker_id"] == 181
                    && log["worker_run_id"] == test_worker_run_id().to_string()
            }),
            "{logs:?}"
        );
    }

    #[tokio::test]
    async fn repeated_heartbeat_need_register_emits_one_metadata_worker_warn_log() {
        let _log_guard = log_test_mutex().lock().await;
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(182);
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );
        let output = Arc::new(Mutex::new(Vec::new()));
        let dispatch = captured_json_subscriber(&output);

        async {
            for heartbeat_seq in [1, 2] {
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
                .expect("heartbeat business error uses gRPC OK")
                .into_inner();
                let error = response.header.expect("header").error.expect("header error");
                assert_error_register_worker(&error, ErrorKind::Worker(WorkerErrorKind::NotRegistered));
            }
        }
        .with_subscriber(dispatch)
        .await;

        let logs = captured_logs(&output);
        let warn_logs: Vec<_> = logs
            .iter()
            .filter(|log| {
                log["target"] == "metadata.worker"
                    && log["level"] == "WARN"
                    && log["op"] == "Heartbeat"
                    && log["result"] == "rejected"
                    && log["error_code"] == "need_register"
            })
            .collect();
        assert_eq!(warn_logs.len(), 1, "{logs:?}");
    }

    #[tokio::test]
    async fn full_block_report_processed_emits_metadata_block_summary_log() {
        let _log_guard = log_test_mutex().lock().await;
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(184);
        let block_id = BlockId::from_u64_u32(1840, 0);
        worker_manager
            .register_worker(&group_name("root"), worker_id, "127.0.0.1:9090".to_string(), 1, None)
            .unwrap();
        let worker_run_id = record_heartbeat(
            &worker_manager,
            &group_name("root"),
            worker_id,
            1_000,
            100,
            900,
            0,
            0,
            HealthStatus::Healthy,
        );
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );
        let output = Arc::new(Mutex::new(Vec::new()));
        let dispatch = captured_json_subscriber(&output);
        let request = full_report_request(group_name("root"), worker_id, worker_run_id, 1, 0, true, vec![block_id]);
        let (expected_client_id, expected_call_id) = header_client_identity(request.header.as_ref().expect("header"));

        async {
            let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
                &service,
                Request::new(request),
            )
            .await
            .expect("full block report response")
            .into_inner();
            assert!(response.header.expect("header").error.is_none());
        }
        .with_subscriber(dispatch)
        .await;

        let logs = captured_logs(&output);
        assert!(
            logs.iter().all(|log| {
                log["level"] != "INFO" || matches!(log["target"].as_str(), Some("metadata.worker" | "metadata.block"))
            }),
            "{logs:?}"
        );
        assert!(
            logs.iter().any(|log| {
                log["target"] == "metadata.block"
                    && log["level"] == "INFO"
                    && log["op"] == "FullBlockReport"
                    && log["result"] == "processed"
                    && log["error_code"] == "none"
                    && log["changed_block_count"] == 1
                    && log["client_id"] == expected_client_id
                    && log["call_id"] == expected_call_id
            }),
            "{logs:?}"
        );
    }

    #[tokio::test]
    async fn changed_delta_block_report_text_log_does_not_dump_request_or_duplicate_client_call_identity() {
        let _log_guard = log_test_mutex().lock().await;
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let group_name = group_name("root");
        let worker_id = WorkerId::new(189);
        let baseline_block = BlockId::from_u64_u32(1890, 0);
        let delta_block = BlockId::from_u64_u32(1891, 0);
        worker_manager
            .register_worker(&group_name, worker_id, "127.0.0.1:9090".to_string(), 1, None)
            .unwrap();
        let worker_run_id = record_heartbeat(
            &worker_manager,
            &group_name,
            worker_id,
            1_000,
            100,
            900,
            0,
            0,
            HealthStatus::Healthy,
        );
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name.clone(),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
            &service,
            Request::new(full_report_request(
                group_name.clone(),
                worker_id,
                worker_run_id,
                1,
                0,
                true,
                vec![baseline_block],
            )),
        )
        .await
        .expect("full block report response")
        .into_inner();
        assert!(response.header.expect("header").error.is_none());

        let output = Arc::new(Mutex::new(Vec::new()));
        let dispatch = captured_text_subscriber(&output);
        async {
            let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
                &service,
                Request::new(delta_report_request(
                    group_name,
                    worker_id,
                    worker_run_id,
                    1,
                    0,
                    vec![(BlockReportDeltaOpProto::BlockReportDeltaOpAddUpdate, delta_block)],
                )),
            )
            .await
            .expect("delta block report response")
            .into_inner();
            assert!(response.header.expect("header").error.is_none());
        }
        .with_subscriber(dispatch)
        .await;

        let text = captured_text(&output);
        assert!(!text.contains("request=Request"), "{text}");
        assert_eq!(text.matches("client_id=").count(), 1, "{text}");
        assert_eq!(text.matches("call_id=").count(), 1, "{text}");
        assert!(text.contains("op=\"DeltaBlockReport\""), "{text}");
    }

    #[tokio::test]
    async fn rejected_block_report_emits_metadata_worker_warn_log() {
        let _log_guard = log_test_mutex().lock().await;
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(185);
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );
        let output = Arc::new(Mutex::new(Vec::new()));
        let dispatch = captured_json_subscriber(&output);

        async {
            let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
                &service,
                Request::new(full_report_request(
                    group_name("root"),
                    worker_id,
                    test_worker_run_id(),
                    1,
                    0,
                    true,
                    Vec::new(),
                )),
            )
            .await
            .expect("rejected block report response")
            .into_inner();
            let error = response.header.expect("header").error.expect("header error");
            assert_error_register_worker(&error, ErrorKind::Worker(WorkerErrorKind::NotRegistered));
        }
        .with_subscriber(dispatch)
        .await;

        let logs = captured_logs(&output);
        assert!(
            logs.iter().any(|log| {
                log["target"] == "metadata.worker"
                    && log["level"] == "WARN"
                    && log["op"] == "BlockReport"
                    && log["result"] == "rejected"
                    && log["error_code"] == "need_register"
                    && log["report_kind"] == "full"
            }),
            "{logs:?}"
        );
    }

    #[tokio::test]
    async fn register_worker_rejects_invalid_request_header_before_raft_mutation() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let before_state_id = raft_node.get_last_applied_state_id();
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            Arc::clone(&raft_node),
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );
        let mut zero_client_header = valid_request_header(&group_name("root"), ClientId::new(7));
        zero_client_header.client.as_mut().expect("client").client_id =
            Some(beryl_proto::common::ClientIdProto { high: 0, low: 0 });
        let mut missing_client_id_header = valid_request_header(&group_name("root"), ClientId::new(8));
        missing_client_id_header.client.as_mut().expect("client").client_id = None;
        let mut invalid_call_id_header = valid_request_header(&group_name("root"), ClientId::new(9));
        invalid_call_id_header.client.as_mut().expect("client").call_id = "not-a-uuid".to_string();
        let cases = [
            (None, WorkerId::new(201), "RequestHeader"),
            (
                Some(beryl_proto::common::RequestHeaderProto {
                    group_name: "root".to_string(),
                    ..Default::default()
                }),
                WorkerId::new(202),
                "client",
            ),
            (Some(missing_client_id_header), WorkerId::new(203), "client_id"),
            (Some(zero_client_header), WorkerId::new(204), "client_id"),
            (Some(invalid_call_id_header), WorkerId::new(205), "call_id"),
        ];

        for (header, worker_id, expected_message) in cases {
            let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
                &service,
                Request::new(register_request_with_header(header, worker_id)),
            )
            .await
            .expect("malformed header returns gRPC OK")
            .into_inner();

            let error = response.header.expect("header").error.expect("header error");
            assert_invalid_header(&error, expected_message);
            assert!(worker_manager.get_descriptor(&group_name("root"), worker_id).is_none());
            assert!(worker_manager
                .get_registration(&group_name("root"), worker_id)
                .is_none());
            assert_eq!(raft_node.get_last_applied_state_id(), before_state_id);
        }
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
    async fn follower_block_report_updates_local_view() {
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
                    block_stamp: 100,
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

    #[test]
    fn metadata_worker_error_label_uses_kind_not_message_text() {
        for message in [
            "not found: block",
            "permission denied: worker",
            "active worker conflict: run",
            "arbitrary application failure",
        ] {
            let rpc_error = RpcErrorDetail::fail(ErrorKind::Internal(InternalErrorKind::Internal), message);
            let detail = beryl_proto::convert::rpc_error_to_proto(&rpc_error);
            assert_eq!(metadata_worker_error_detail_kind(&detail), "internal");
        }
    }

    #[tokio::test]
    async fn heartbeat_error_records_worker_metrics() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );
        let recorder = MetadataWorkerMetricsRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
                    &service,
                    Request::new(heartbeat_request(
                        group_name("root"),
                        WorkerId::new(18),
                        test_worker_run_id(),
                        1,
                        9090,
                    )),
                )
                .await
                .expect("heartbeat header error returns gRPC OK")
                .into_inner();

                assert!(response.header.expect("header").error.is_some());
            });
        });

        assert!(recorder.has_counter(
            observe::METADATA_RPC_REQUESTS_TOTAL,
            &[
                ("service", "metadata_worker"),
                ("method", "heartbeat"),
                ("status", "error"),
                ("error_kind", "worker_not_registered"),
            ],
        ));
        assert!(recorder.has_counter(
            observe::METADATA_WORKER_HEARTBEAT_TOTAL,
            &[("status", "error"), ("error_kind", "worker_not_registered")],
        ));
        assert!(recorder.has_histogram(
            observe::METADATA_RPC_REQUEST_DURATION_SECONDS,
            &[
                ("service", "metadata_worker"),
                ("method", "heartbeat"),
                ("status", "error"),
                ("error_kind", "worker_not_registered"),
            ],
        ));
        assert!(recorder.has_histogram(
            observe::METADATA_WORKER_HEARTBEAT_DURATION_SECONDS,
            &[("status", "error"), ("error_kind", "worker_not_registered")],
        ));
    }

    #[tokio::test]
    async fn block_report_success_records_worker_metrics_and_accepted_blocks() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(19);
        worker_manager
            .register_worker(
                &group_name("root"),
                worker_id,
                "127.0.0.1:9090".to_string(),
                WORKER_NET_PROTOCOL_GRPC,
                None,
            )
            .unwrap();
        let worker_run_id = record_heartbeat(
            &worker_manager,
            &group_name("root"),
            worker_id,
            1_000,
            100,
            900,
            0,
            0,
            HealthStatus::Healthy,
        );
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            worker_manager,
            Arc::new(MountTable::new()),
            group_name("root"),
        );
        let recorder = MetadataWorkerMetricsRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
                    &service,
                    Request::new(full_report_request(
                        group_name("root"),
                        worker_id,
                        worker_run_id,
                        1,
                        0,
                        true,
                        vec![BlockId::from_u64_u32(1_900, 0)],
                    )),
                )
                .await
                .expect("block report succeeds")
                .into_inner();

                assert!(response.header.expect("header").error.is_none());
            });
        });

        assert!(recorder.has_counter(
            observe::METADATA_RPC_REQUESTS_TOTAL,
            &[
                ("service", "metadata_worker"),
                ("method", "block_report"),
                ("status", "ok"),
                ("error_kind", "none"),
            ],
        ));
        assert!(recorder.has_counter(
            observe::METADATA_WORKER_BLOCK_REPORT_TOTAL,
            &[("kind", "full"), ("status", "ok"), ("error_kind", "none")],
        ));
        assert!(recorder.has_counter(
            observe::METADATA_WORKER_BLOCK_REPORT_BLOCKS_TOTAL,
            &[("change", "added")],
        ));
        assert!(recorder.has_histogram(
            observe::METADATA_RPC_REQUEST_DURATION_SECONDS,
            &[
                ("service", "metadata_worker"),
                ("method", "block_report"),
                ("status", "ok"),
                ("error_kind", "none"),
            ],
        ));
        assert!(recorder.has_histogram(
            observe::METADATA_WORKER_BLOCK_REPORT_DURATION_SECONDS,
            &[("kind", "full"), ("status", "ok"), ("error_kind", "none")],
        ));
    }

    #[tokio::test]
    async fn worker_service_rejects_non_served_header_group() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let register_response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: Some(valid_request_header(&group_name("other"), ClientId::new(81))),
                worker_id: 9,
                worker_run_id: test_worker_run_id().to_string(),
                advertised_endpoint: Some(beryl_proto::common::EndpointProto {
                    host: "127.0.0.1".to_string(),
                    port: 9090,
                }),
            }),
        )
        .await
        .expect("group-name mismatch returns gRPC OK")
        .into_inner();

        let error = register_response.header.expect("header").error.expect("header error");
        let error = assert_error_fail(&error, ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument));
        assert!(error.message.contains("served metadata group"));
        assert!(worker_manager
            .get_descriptor(&group_name("root"), WorkerId::new(9))
            .is_none());

        let mut heartbeat = heartbeat_request(group_name("root"), WorkerId::new(10), test_worker_run_id(), 1, 9090);
        heartbeat.header.as_mut().expect("header").group_name = "other".to_string();
        let heartbeat_response =
            <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(&service, Request::new(heartbeat))
                .await
                .expect("group-name mismatch returns gRPC OK")
                .into_inner();

        let error = heartbeat_response.header.expect("header").error.expect("header error");
        let error = assert_error_fail(&error, ErrorKind::Metadata(MetadataErrorKind::GroupMismatch));
        assert!(error.message.contains("served metadata group"));

        let block_id = BlockId::from_u64_u32(1000, 0);
        let mut block_report = full_report_request(
            group_name("root"),
            WorkerId::new(100),
            test_worker_run_id(),
            1,
            0,
            true,
            vec![block_id],
        );
        block_report.header.as_mut().expect("header").group_name = "other".to_string();
        let block_report_response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
            &service,
            Request::new(block_report),
        )
        .await
        .expect("group-name mismatch returns gRPC OK")
        .into_inner();

        let error = block_report_response
            .header
            .expect("header")
            .error
            .expect("header error");
        let error = assert_error_fail(&error, ErrorKind::Metadata(MetadataErrorKind::GroupMismatch));
        assert!(error.message.contains("served metadata group"));
        assert!(worker_manager
            .get_block_locations(&group_name("root"), block_id)
            .is_empty());
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
                header: Some(valid_request_header(&group_name("root"), ClientId::new(82))),
                worker_id: 123,
                worker_run_id: test_worker_run_id().to_string(),
                advertised_endpoint: Some(beryl_proto::common::EndpointProto {
                    host: "127.0.0.1".to_string(),
                    port: 9090,
                }),
            }),
        )
        .await
        .expect("follower business redirect returns gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        assert_error_refresh_metadata(&error, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
    }

    #[tokio::test]
    async fn register_worker_publishes_live_run_only_after_raft_success() {
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
                header: Some(valid_request_header(&group_name("root"), ClientId::new(84))),
                worker_id: 124,
                worker_run_id: worker_run_id.to_string(),
                advertised_endpoint: Some(beryl_proto::common::EndpointProto {
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
                .get_descriptor(&group_name("root"), WorkerId::new(124))
                .expect("published descriptor")
                .address,
            "127.0.0.1:9091"
        );
        assert_eq!(
            worker_manager
                .get_registration(&group_name("root"), WorkerId::new(124))
                .expect("published live run")
                .worker_run_id,
            worker_run_id
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
            Arc::new(MountTable::new()),
            group_name("root"),
        );

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: Some(valid_request_header(&group_name("other"), ClientId::new(88))),
                worker_id: 123,
                worker_run_id: test_worker_run_id().to_string(),
                advertised_endpoint: Some(beryl_proto::common::EndpointProto {
                    host: "127.0.0.1".to_string(),
                    port: 9090,
                }),
            }),
        )
        .await
        .expect("wrong-group register returns gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        let error = assert_error_fail(&error, ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument));
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
        assert_error_register_worker(&error, ErrorKind::Worker(WorkerErrorKind::NotRegistered));
    }

    #[tokio::test]
    async fn heartbeat_maps_registration_mismatches_to_recovery_headers() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let group_name = group_name("root");
        worker_manager
            .register_worker_run(
                &group_name,
                WorkerId::new(11),
                "127.0.0.1:9090".to_string(),
                1,
                test_worker_run_id(),
                None,
            )
            .unwrap();
        worker_manager
            .register_worker_run(
                &group_name,
                WorkerId::new(9),
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
            group_name.clone(),
        );

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
        for (worker_id, leader, report_seq) in [(WorkerId::new(12), false, 7), (WorkerId::new(13), true, 1)] {
            let dir = TempDir::new().unwrap();
            let raft_node = if leader {
                leader_raft(&dir).await
            } else {
                nonleader_raft(&dir).await
            };
            let before_state_id = raft_node.get_last_applied_state_id();
            let worker_manager = Arc::new(WorkerManager::new(60));
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
                    report_seq,
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
            assert!(worker_manager.is_worker_live(&group_name("root"), worker_id));
            if leader {
                assert_eq!(raft_node.get_last_applied_state_id(), before_state_id);
            }
        }
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
                header: Some(valid_request_header(&group_name("root"), ClientId::new(89))),
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
            }),
        )
        .await
        .expect("business validation must return gRPC OK")
        .into_inner();

        let error = response.header.expect("header").error.expect("header error");
        let error = assert_error_fail(&error, ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument));
        assert!(error.message.contains("missing block"));
    }

    #[derive(Default)]
    struct MetadataWorkerMetricsRecorder {
        counters: Arc<Mutex<Vec<RecordedMetric>>>,
        histograms: Arc<Mutex<Vec<RecordedMetric>>>,
    }

    impl MetadataWorkerMetricsRecorder {
        fn has_counter(&self, name: &str, labels: &[(&str, &str)]) -> bool {
            self.counters
                .lock()
                .expect("counter metrics poisoned")
                .iter()
                .any(|metric| metric.matches(name, labels))
        }

        fn has_histogram(&self, name: &str, labels: &[(&str, &str)]) -> bool {
            self.histograms
                .lock()
                .expect("histogram metrics poisoned")
                .iter()
                .any(|metric| metric.matches(name, labels))
        }
    }

    impl Recorder for MetadataWorkerMetricsRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::from_arc(Arc::new(MetricCounter {
                metric: RecordedMetric::from_key(key),
                recorder: Arc::clone(&self.counters),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::from_arc(Arc::new(MetricHistogram {
                metric: RecordedMetric::from_key(key),
                recorder: Arc::clone(&self.histograms),
            }))
        }
    }

    #[derive(Clone)]
    struct RecordedMetric {
        name: String,
        labels: Vec<(String, String)>,
    }

    impl RecordedMetric {
        fn from_key(key: &Key) -> Self {
            Self {
                name: key.name().to_string(),
                labels: key
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect(),
            }
        }

        fn matches(&self, name: &str, labels: &[(&str, &str)]) -> bool {
            self.name == name
                && labels.iter().all(|(key, value)| {
                    self.labels
                        .iter()
                        .any(|(actual_key, actual_value)| actual_key == key && actual_value == value)
                })
        }
    }

    struct MetricCounter {
        metric: RecordedMetric,
        recorder: Arc<Mutex<Vec<RecordedMetric>>>,
    }

    impl CounterFn for MetricCounter {
        fn increment(&self, _value: u64) {
            self.recorder
                .lock()
                .expect("counter metrics poisoned")
                .push(self.metric.clone());
        }

        fn absolute(&self, value: u64) {
            self.increment(value);
        }
    }

    struct MetricHistogram {
        metric: RecordedMetric,
        recorder: Arc<Mutex<Vec<RecordedMetric>>>,
    }

    impl HistogramFn for MetricHistogram {
        fn record(&self, _value: f64) {
            self.recorder
                .lock()
                .expect("histogram metrics poisoned")
                .push(self.metric.clone());
        }
    }
}
