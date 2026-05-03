// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataWorkerService implementation.

use super::command_sender::CommandSender;
use super::delete_executor::DeleteExecutor;
use super::manager::WorkerManager;
use super::metrics::WorkerMetrics;
use super::repair::{
    ErrorClass, OrphanQueue, RepairPlanner, RepairQueue, RepairTask, RepairTaskId, RepairTaskRecord, TaskAckStatus,
};
use crate::error::MetadataResult;
use crate::raft::Command;
use crate::raft::{AppDataResponse, AppRaftNode, WorkerCommandResult};
use crate::service::extract_and_inject_context;
use ::common::header::ResponseHeader;
use proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProto;
use proto::metadata::*;
use std::future::Future;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};
use types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};

/// Worker service background task handles.
pub struct WorkerBackgroundHandle {
    _lease_metrics_task: Option<JoinHandle<()>>,
    _dead_worker_cleanup_task: JoinHandle<()>,
}

impl WorkerBackgroundHandle {
    pub fn task_count(&self) -> usize {
        usize::from(self._lease_metrics_task.is_some()) + 1
    }
}

/// MetadataWorkerService implementation.
pub struct MetadataWorkerServiceImpl {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    repair_queue: Arc<RepairQueue>,
    orphan_queue: Arc<OrphanQueue>,
    repair_planner: Arc<RepairPlanner>,
    _command_sender: Arc<CommandSender>,
    delete_executor: Option<Arc<DeleteExecutor>>,
    metrics: Arc<WorkerMetrics>,
    slot_metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
    /// Mount table used to compute mount_epoch for lease gating (TODO-2).
    mount_table: Arc<crate::mount::MountTable>,
}

impl MetadataWorkerServiceImpl {
    pub fn new(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        repair_queue: Arc<RepairQueue>,
        orphan_queue: Arc<OrphanQueue>,
        mount_table: Arc<crate::mount::MountTable>,
    ) -> Self {
        let repair_planner = Arc::new(RepairPlanner::new(Arc::clone(&repair_queue), Arc::clone(&orphan_queue)));

        let command_sender = Arc::new(CommandSender::new(3, 100)); // 3 retries, 100ms base backoff
        let metrics = Arc::new(WorkerMetrics::new());

        Self {
            raft_node,
            worker_manager,
            repair_queue,
            orphan_queue,
            repair_planner,
            _command_sender: command_sender,
            delete_executor: None, // Will be set via set_delete_executor
            metrics,
            slot_metrics: None, // Will be set via set_slot_metrics
            mount_table,
        }
    }

    /// Set delete executor (called after storage is available).
    pub fn set_delete_executor(&mut self, delete_executor: Arc<DeleteExecutor>) {
        self.delete_executor = Some(delete_executor);
    }

    /// Set slot metrics (called after metrics are available).
    pub fn set_slot_metrics(&mut self, metrics: Arc<crate::metrics::MetadataMetrics>) {
        self.slot_metrics = Some(metrics);
    }

    /// Get repair planner (for external use).
    pub fn repair_planner(&self) -> Arc<RepairPlanner> {
        Arc::clone(&self.repair_planner)
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

    /// Start background task for worker dead cleanup, replication check, and lease metrics update.
    pub fn start_background_tasks(&self) -> WorkerBackgroundHandle {
        // Start lease metrics update task
        let lease_metrics_task = if let Some(ref slot_metrics) = self.slot_metrics {
            let lease_manager = self.worker_manager.lease_manager();
            let worker_manager = Arc::clone(&self.worker_manager);
            let slot_metrics = Arc::clone(slot_metrics);
            let raft_node = Arc::clone(&self.raft_node);
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
                        for worker_id in &live_workers {
                            if worker_manager.needs_full_sync(*worker_id) {
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

        let worker_manager = Arc::clone(&self.worker_manager);
        let repair_planner = Arc::clone(&self.repair_planner);
        let repair_queue = Arc::clone(&self.repair_queue);
        let raft_node = Arc::clone(&self.raft_node);

        let dead_worker_cleanup_task = tokio::spawn(async move {
            use tokio::time::{interval, Duration};
            // TODO: Check interval secs needs from core-site.yaml
            let mut interval = interval(Duration::from_secs(30)); // Check every 30 seconds

            loop {
                interval.tick().await;

                // Get live workers
                let live_workers = worker_manager.list_live_workers();
                let all_workers = worker_manager.list_all_workers();

                // Find dead workers
                let dead_workers: Vec<WorkerId> =
                    all_workers.into_iter().filter(|w| !live_workers.contains(w)).collect();

                // Remove dead workers and trigger replication check (leader-only)
                // Note: This background task runs on all nodes, but only leader should process
                // We check leader status here to avoid unnecessary work on followers
                if raft_node.is_leader() {
                    for dead_worker in dead_workers {
                        info!(worker_id = dead_worker.as_raw(), "Removing dead worker");
                        let affected_blocks = worker_manager.remove_dead_worker(dead_worker);

                        // Trigger replication check for affected blocks
                        let live_workers_after = worker_manager.list_live_workers();
                        for block_id in affected_blocks {
                            let current_locations = worker_manager.get_block_locations(block_id);
                            // TODO: Get from BlockMeta if available
                            let replication_factor = 3u8;
                            let actions = repair_planner.plan_replication(
                                block_id,
                                &current_locations,
                                replication_factor,
                                &live_workers_after,
                            );
                            for action in actions {
                                let task = action.to_task();
                                if let Err(e) = repair_queue.enqueue(task) {
                                    warn!(
                                        block_id = %block_id,
                                        error = %e,
                                        "Failed to enqueue replication task after worker removal"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        });

        WorkerBackgroundHandle {
            _lease_metrics_task: lease_metrics_task,
            _dead_worker_cleanup_task: dead_worker_cleanup_task,
        }
    }

    /// Get pending commands for a worker from repair queue.
    fn get_pending_commands(&self, worker_id: WorkerId, max: usize) -> Vec<WorkerCommandProto> {
        // Poll tasks for this worker
        let records = self.repair_queue.poll_for_worker(worker_id, max);

        // Convert records to commands
        records
            .into_iter()
            .filter_map(|record| self.record_to_command(record))
            .collect()
    }

    /// Convert repair task record to worker command.
    fn record_to_command(&self, record: RepairTaskRecord) -> Option<WorkerCommandProto> {
        let task_id = record.id.0;
        let command = match record.task {
            RepairTask::Replicate {
                block_id,
                src_workers: _,
                target_worker,
                ..
            } => {
                let proto_block_id = proto::common::BlockIdProto {
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                };

                Some(proto::metadata::worker_command_proto::Command::Replicate(
                    ReplicateCommandProto {
                        block_id: Some(proto_block_id),
                        target_worker_ids: vec![target_worker.as_raw()],
                    },
                ))
            }
            RepairTask::Evict {
                block_id,
                reason,
                target_worker: _,
            } => {
                let proto_block_id = proto::common::BlockIdProto {
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                };

                Some(proto::metadata::worker_command_proto::Command::Evict(
                    EvictCommandProto {
                        block_ids: vec![proto_block_id],
                        reason,
                        intent_id: 0, // Legacy EvictCommandProto - no intent_id
                        op_kind: proto::metadata::DeleteOpKindProto::DeleteOpKindDelete as i32,
                        not_before_ms: 0,
                        expected_epoch: 0,
                    },
                ))
            }
            RepairTask::MoveCopy {
                block_id,
                from_worker,
                to_worker,
            } => {
                // MoveCopy command: to_worker pulls from from_worker
                let proto_block_id = proto::common::BlockIdProto {
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                };

                Some(proto::metadata::worker_command_proto::Command::MoveCopy(
                    proto::metadata::MoveCopyCommandProto {
                        block_id: Some(proto_block_id),
                        from_worker_id: from_worker.as_raw(),
                        to_worker_id: to_worker.as_raw(),
                    },
                ))
            }
        };

        command.map(|cmd| WorkerCommandProto {
            task_id,
            command: Some(cmd),
        })
    }

    /// Process task acknowledgments from worker.
    async fn process_task_acks(&self, worker_id: WorkerId, acks: &[proto::metadata::TaskAckProto]) {
        // Process acks for delete executor (if available)
        if let Some(ref delete_executor) = self.delete_executor {
            delete_executor.process_task_acks(worker_id, acks).await;
        }

        // Process acks for repair queue (existing logic)
        for ack in acks {
            let task_id = RepairTaskId(ack.task_id);
            let status = match ack.status() {
                proto::metadata::TaskAckStatusProto::TaskAckStatusSuccess => TaskAckStatus::Success,
                proto::metadata::TaskAckStatusProto::TaskAckStatusFailed => TaskAckStatus::Failed,
                proto::metadata::TaskAckStatusProto::TaskAckStatusRetryableFailed => TaskAckStatus::RetryableFailed,
                _ => {
                    warn!(task_id = task_id.0, "Unknown ack status, treating as failed");
                    TaskAckStatus::Failed
                }
            };

            let message = if ack.error_message.is_empty() {
                None
            } else {
                Some(ack.error_message.clone())
            };

            // Parse error class from proto
            let error_class = match ack.error_class() {
                proto::metadata::ErrorClassProto::ErrorClassOk => Some(ErrorClass::Ok),
                proto::metadata::ErrorClassProto::ErrorClassRetryable => Some(ErrorClass::Retryable),
                proto::metadata::ErrorClassProto::ErrorClassFatal => Some(ErrorClass::Fatal),
                proto::metadata::ErrorClassProto::ErrorClassNeedRefresh => Some(ErrorClass::NeedRefresh),
                _ => None, // Will use default based on status
            };

            match self.repair_queue.ack(task_id, worker_id, status, message, error_class) {
                Ok(Some(followup_task)) => {
                    // MoveCopy succeeded, enqueue Evict task
                    if let Err(e) = self.repair_queue.enqueue(followup_task) {
                        warn!(
                            task_id = task_id.0,
                            error = %e,
                            "Failed to enqueue followup Evict task after MoveCopy"
                        );
                    } else {
                        info!(task_id = task_id.0, "MoveCopy completed, enqueued followup Evict task");
                    }
                }
                Ok(None) => {
                    // Normal completion, no followup
                }
                Err(e) => {
                    warn!(
                        task_id = task_id.0,
                        worker_id = worker_id.as_raw(),
                        error = %e,
                        "Failed to process task ack"
                    );
                }
            }
        }
    }

    /// Convert proto BlockId to types BlockId.
    fn proto_to_block_id(proto: &proto::common::BlockIdProto) -> MetadataResult<BlockId> {
        Ok(BlockId::new(
            DataHandleId::new(proto.data_handle_id),
            BlockIndex::new(proto.block_index),
        ))
    }
}

async fn persist_worker_descriptor_then_register<F>(
    worker_manager: &WorkerManager,
    address: String,
    net_transport_kind: i32,
    worker_epoch: u64,
    fault_domain: Option<String>,
    persist_descriptor: F,
) -> Result<WorkerId, Status>
where
    F: Future<Output = MetadataResult<AppDataResponse>>,
{
    let worker_id = match persist_descriptor
        .await
        .map_err(|e| Status::internal(format!("Failed to propose command: {}", e)))?
    {
        AppDataResponse::Worker(WorkerCommandResult::Upserted(worker_id)) => worker_id,
        other => {
            return Err(Status::internal(format!(
                "RegisterWorker returned unexpected Raft response: {:?}",
                other
            )))
        }
    };

    worker_manager
        .register_worker(worker_id, address, net_transport_kind, worker_epoch, fault_domain)
        .map_err(|e| Status::internal(format!("Failed to register worker: {}", e)))?;

    Ok(worker_id)
}

#[tonic::async_trait]
#[tonic::async_trait]
impl MetadataWorkerServiceProto for MetadataWorkerServiceImpl {
    #[instrument(skip(self), fields(call_id, client_id))]
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequestProto>,
    ) -> Result<Response<RegisterWorkerResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Extract all needed fields before any moves
        let net_transport_kind = req.net_transport_kind() as i32; // Convert enum to i32
        let worker_epoch = req.worker_epoch;
        if req.suggested_worker_id != 0 {
            return Err(Status::invalid_argument(
                "suggested_worker_id is no longer authoritative; worker id is allocated by Raft",
            ));
        }
        let labels = req.labels;
        let endpoint = req
            .endpoint
            .ok_or_else(|| Status::invalid_argument("Missing endpoint"))?;

        let address = format!("{}:{}", endpoint.host, endpoint.port);

        // Generate worker identity from address + labels for mapping (stable hash)
        let identity = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(address.as_bytes());
            // Sort labels by key for stable hash
            let mut sorted_labels: Vec<_> = labels.iter().collect();
            sorted_labels.sort_by_key(|(k, _)| *k);
            for (k, v) in sorted_labels {
                hasher.update(k.as_bytes());
                hasher.update(b":");
                hasher.update(v.as_bytes());
                hasher.update(b";");
            }
            format!("{:x}", hasher.finalize())
        };

        let command = Command::RegisterWorker {
            dedup: crate::raft::DedupKey::new(_caller_ctx.client.client_id, _caller_ctx.client.call_id),
            identity,
            address: address.clone(),
            net_transport_kind,
            worker_epoch,
            fault_domain: None, // TODO: Extract fault_domain from labels
        };

        let worker_id = persist_worker_descriptor_then_register(
            self.worker_manager.as_ref(),
            address.clone(),
            net_transport_kind,
            worker_epoch,
            None, // TODO: Extract fault_domain from labels
            self.raft_node.propose(command),
        )
        .await?;

        info!(worker_id = worker_id.as_raw(), "Worker registered");

        Ok(Response::new(RegisterWorkerResponseProto {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    req.header
                        .as_ref()
                        .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None }),
                ))
                    .into(),
            ),
            worker_id: worker_id.as_raw(),
            config: Some(WorkerConfigProto {
                heartbeat_interval_sec: 30,
                block_report_interval_sec: 60,
                params: std::collections::HashMap::new(),
            }),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequestProto>,
    ) -> Result<Response<HeartbeatResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        let worker_id = WorkerId::new(req.worker_id);

        let capacity = req
            .capacity
            .ok_or_else(|| Status::invalid_argument("Missing capacity"))?;

        let load = req.load.ok_or_else(|| Status::invalid_argument("Missing load"))?;

        let health_proto = req.health();

        // Extract net_transport_kind and worker_epoch from request
        let net_transport_kind = req.net_transport_kind() as i32; // Convert enum to i32
        let worker_epoch = req.worker_epoch;

        // Fanout heartbeat: update runtime in memory (all nodes, no Raft)
        use super::manager::HealthStatus;
        let health_status = HealthStatus::from(health_proto as i32);

        let descriptor_changed = self
            .worker_manager
            .update_runtime(
                worker_id,
                net_transport_kind,
                worker_epoch,
                capacity.total_bytes,
                capacity.used_bytes,
                capacity.available_bytes,
                load.active_reads,
                load.active_writes,
                health_status,
            )
            .map_err(|e| Status::internal(format!("Failed to update worker runtime: {}", e)))?;

        // If descriptor changed, require worker to re-register (leader-only check)
        let is_leader = self.raft_node.is_leader();
        if descriptor_changed && is_leader {
            // Leader detects descriptor change, return error to trigger re-register
            return Err(Status::failed_precondition(
                "Worker descriptor changed, please re-register",
            ));
        }

        // Update metrics (all nodes)
        let live_count = self.worker_manager.list_live_workers().len();
        self.metrics.update_worker_live(live_count);

        // All nodes: check if worker needs full sync (metadata restart detection)
        let mut commands = Vec::new();
        let needs_full_sync = self.worker_manager.needs_full_sync(worker_id);

        // Leader-only: lease allocation for full reports
        let (can_full_report, full_report_lease_token, backoff_ms) = if is_leader {
            if needs_full_sync {
                // Try to allocate lease
                let metadata_epoch = self.worker_manager.get_metadata_epoch();
                let lease_manager = self.worker_manager.lease_manager();

                // TODO: Get shard_group_id from router when available
                let shard_group_id = None; // Will be resolved from block_id/router in future

                // TODO-2: Get mount_epoch from mount_table
                let mount_epoch = Some(types::group_watermark::MountEpoch::new(self.mount_table.version()));

                if let Some(token) = lease_manager
                    .try_allocate(worker_id, shard_group_id, metadata_epoch, mount_epoch)
                    .await
                {
                    // Lease allocated
                    // Update metrics (keep slot_metrics for backward compatibility)
                    if let Some(ref slot_metrics) = self.slot_metrics {
                        slot_metrics
                            .full_report_granted_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        // Note: lease metrics can be added later
                    }

                    // Request full block report
                    commands.push(WorkerCommandProto {
                        task_id: 0, // No task ID for control commands
                        command: Some(proto::metadata::worker_command_proto::Command::RequestFullBlockReport(
                            proto::metadata::RequestFullBlockReportCommandProto {
                                target_metadata_epoch: metadata_epoch,
                                reason: "METADATA_RESTART".to_string(),
                            },
                        )),
                    });
                    (true, token, 0)
                } else {
                    // Lease allocation failed (rate-limited)
                    // Update metrics
                    if let Some(ref slot_metrics) = self.slot_metrics {
                        slot_metrics
                            .full_report_throttled_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }

                    // Calculate backoff with jitter (5-30 seconds)
                    use rand::Rng;
                    let base_ms = 5000; // 5 seconds base
                    let jitter_ms = rand::thread_rng().gen_range(0..25000); // 0-25 seconds jitter
                    let backoff_ms = (base_ms + jitter_ms) as u32;

                    commands.push(WorkerCommandProto {
                        task_id: 0, // No task ID for control commands
                        command: Some(proto::metadata::worker_command_proto::Command::BlockReportBackoff(
                            proto::metadata::BlockReportBackoffCommandProto {
                                retry_after_ms: backoff_ms,
                            },
                        )),
                    });
                    (false, 0, backoff_ms)
                }
            } else {
                // Worker doesn't need full sync
                (false, 0, 0)
            }
        } else {
            // Follower: no lease allocation
            (false, 0, 0)
        };

        // Leader-only: process task acknowledgments and get pending commands
        if is_leader {
            // Process task acknowledgments (if any)
            if !req.acks.is_empty() {
                self.process_task_acks(worker_id, &req.acks).await;
            }

            // Get pending commands from delete executor (if available)
            if let Some(ref delete_executor) = self.delete_executor {
                const MAX_DELETE_COMMANDS_PER_HEARTBEAT: usize = 4;
                let mut delete_commands =
                    delete_executor.get_pending_commands(worker_id, MAX_DELETE_COMMANDS_PER_HEARTBEAT);
                commands.append(&mut delete_commands);
            }

            // Get pending commands from repair queue
            const MAX_COMMANDS_PER_HEARTBEAT: usize = 8;
            let mut repair_commands = self.get_pending_commands(worker_id, MAX_COMMANDS_PER_HEARTBEAT);
            commands.append(&mut repair_commands);
        }

        Ok(Response::new(HeartbeatResponseProto {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    req.header
                        .as_ref()
                        .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None }),
                ))
                    .into(),
            ),
            commands,
            full_report_lease_token,
            can_full_report,
            backoff_ms,
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
                    let needs_full_sync = self.worker_manager.needs_full_sync(worker_id);
                    if needs_full_sync {
                        // Worker needs full sync, must have valid lease token
                        if lease_token == 0 {
                            return Err(Status::invalid_argument(
                                "Full report requires lease token when needs_full_sync is true",
                            ));
                        }

                        // Verify and release lease
                        let metadata_epoch = self.worker_manager.get_metadata_epoch();
                        let lease_manager = self.worker_manager.lease_manager();
                        // TODO-2: Get mount_epoch from mount_table
                        let mount_epoch = Some(types::group_watermark::MountEpoch::new(self.mount_table.version()));

                        if !lease_manager
                            .verify_and_release(lease_token, worker_id, metadata_epoch, mount_epoch)
                            .await
                        {
                            return Err(Status::invalid_argument(
                                "Invalid or expired lease token for full report",
                            ));
                        }
                    }
                }

                // FULL report: convert full_entries to BlockIds
                let mut reported_blocks = Vec::new();
                for entry in &req.full_entries {
                    let block_id = entry
                        .block_id
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("Missing block_id in full entry"))?;
                    let block_id =
                        Self::proto_to_block_id(block_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
                    reported_blocks.push(block_id);
                }

                // Apply full report (lease already released in verify_and_release above)
                match self
                    .worker_manager
                    .apply_full_report(worker_id, reported_blocks.clone())
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
                            header: Some(
                                (&self.create_response_header_from_request(
                                    &req.header,
                                    req.header.as_ref().and_then(
                                        |h| {
                                            if h.group_id != 0 {
                                                Some(h.group_id)
                                            } else {
                                                None
                                            }
                                        },
                                    ),
                                ))
                                    .into(),
                            ),
                            report_seq: req.last_report_seq,
                            commands: vec![],
                            retry_after_ms: retry_after_ms,
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
                    let block_id = entry
                        .block_id
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("Missing block_id in delta entry"))?;
                    let block_id =
                        Self::proto_to_block_id(block_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

                    match entry.op() {
                        proto::metadata::BlockReportDeltaOpProto::BlockReportDeltaOpAdd => {
                            added_blocks.push(block_id);
                        }
                        proto::metadata::BlockReportDeltaOpProto::BlockReportDeltaOpRemove => {
                            removed_blocks.push(block_id);
                        }
                        _ => {
                            return Err(Status::invalid_argument("Invalid delta operation"));
                        }
                    }
                }

                // Apply delta report
                match self
                    .worker_manager
                    .apply_delta_report(worker_id, added_blocks.clone(), removed_blocks.clone())
                {
                    Ok(result) => result,
                    Err(_e) => {
                        // Full sync required: return RequestFullBlockReport command
                        let metadata_epoch = self.worker_manager.get_metadata_epoch();
                        return Ok(Response::new(BlockReportResponseProto {
                            header: Some(
                                (&self.create_response_header_from_request(
                                    &req.header,
                                    req.header.as_ref().and_then(
                                        |h| {
                                            if h.group_id != 0 {
                                                Some(h.group_id)
                                            } else {
                                                None
                                            }
                                        },
                                    ),
                                ))
                                    .into(),
                            ),
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
                return Err(Status::invalid_argument("Invalid report_type"));
            }
        };

        // Fanout: all nodes update presence (memory-only, no Raft)
        // No Raft propose for UpdateBlockLocations

        // Leader-only: trigger repair/orphan processing
        // Performance optimization: only check added_blocks (not all reported_blocks)
        // This avoids O(n) synchronous raft_node.read calls for unchanged blocks
        let is_leader = self.raft_node.is_leader();
        if is_leader {
            // Only check newly added blocks for orphan detection and replication
            // This reduces lock contention from O(n) to O(added_blocks.len())
            for block_id in &added_blocks {
                let block_exists = self
                    .raft_node
                    .read(false, |sm| sm.get_block(*block_id))
                    .await
                    .map_err(|e| Status::internal(format!("Failed to read block: {}", e)))?;

                if block_exists.is_none() {
                    self.orphan_queue.add(*block_id, worker_id);
                    warn!(
                        block_id = %block_id,
                        worker_id = worker_id.as_raw(),
                        "Orphan block detected"
                    );
                } else {
                    // Trigger replication check for this block
                    let current_locations = self.worker_manager.get_block_locations(*block_id);
                    let live_workers = self.worker_manager.list_live_workers();
                    // Get replication factor from block metadata (default to 3 if not available)
                    let replication_factor = 3u8; // TODO: Get from BlockMeta if available
                    let actions = self.repair_planner.plan_replication(
                        *block_id,
                        &current_locations,
                        replication_factor,
                        &live_workers,
                    );
                    for action in actions {
                        let task = action.to_task();
                        if let Err(e) = self.repair_queue.enqueue(task) {
                            warn!(block_id = %block_id, error = %e, "Failed to enqueue replication task");
                        }
                    }
                }
            }
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
        self.metrics.update_orphan_queue_len(self.orphan_queue.len());
        self.metrics.update_repair_queue_len(self.repair_queue.len_pending());

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
            self.get_pending_commands(worker_id, 1)
        } else {
            Vec::new()
        };

        Ok(Response::new(BlockReportResponseProto {
            header: Some(
                (&self.create_response_header_from_request(
                    &req.header,
                    req.header
                        .as_ref()
                        .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None }),
                ))
                    .into(),
            ),
            report_seq,
            commands,
            retry_after_ms: 0,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    /// DEPRECATED: This method is deprecated. Use BlockReport instead for strongly-consistent
    /// block presence reporting. This method is kept for backward compatibility but
    /// does not perform any actual work.
    async fn report_presence(
        &self,
        request: Request<WorkerReportPresenceRequestProto>,
    ) -> Result<Response<WorkerReportPresenceResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Legacy presence reporting - just acknowledge (no-op)
        // DEPRECATED: For strongly-consistent reporting, use BlockReport instead
        let group_id = if let Some(ref header) = req.header {
            if header.group_id != 0 {
                Some(header.group_id)
            } else {
                None
            }
        } else {
            None
        };
        let response_header = self.create_response_header_from_request(&req.header, group_id);
        Ok(Response::new(WorkerReportPresenceResponseProto {
            header: Some((&response_header).into()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MetadataError;

    #[tokio::test]
    async fn register_worker_does_not_store_descriptor_when_propose_fails() {
        let manager = WorkerManager::new(60);
        let worker_id = WorkerId::new(7);

        let result =
            persist_worker_descriptor_then_register(&manager, "127.0.0.1:9090".to_string(), 1, 100, None, async {
                Err(MetadataError::Internal("propose failed".to_string()))
            })
            .await;

        assert!(result.is_err());
        assert!(manager.get_descriptor(worker_id).is_none());
    }

    #[tokio::test]
    async fn register_worker_stores_descriptor_after_propose_succeeds() {
        let manager = WorkerManager::new(60);
        let worker_id = WorkerId::new(7);

        let returned_worker_id =
            persist_worker_descriptor_then_register(&manager, "127.0.0.1:9090".to_string(), 1, 100, None, async {
                Ok(AppDataResponse::Worker(WorkerCommandResult::Upserted(worker_id)))
            })
            .await
            .unwrap();
        assert_eq!(returned_worker_id, worker_id);

        let descriptor = manager.get_descriptor(worker_id).unwrap();
        assert_eq!(descriptor.worker_id, worker_id);
        assert_eq!(descriptor.address, "127.0.0.1:9090");
        assert_eq!(descriptor.net_transport_kind, 1);
        assert_eq!(descriptor.worker_epoch, 100);
    }

    #[tokio::test]
    async fn repeated_register_remains_idempotent_after_successful_propose() {
        let manager = WorkerManager::new(60);
        let worker_id = WorkerId::new(7);

        for _ in 0..2 {
            persist_worker_descriptor_then_register(&manager, "127.0.0.1:9090".to_string(), 1, 100, None, async {
                Ok(AppDataResponse::Worker(WorkerCommandResult::Upserted(worker_id)))
            })
            .await
            .unwrap();
        }

        let descriptor = manager.get_descriptor(worker_id).unwrap();
        assert_eq!(descriptor.worker_id, worker_id);
        assert_eq!(descriptor.address, "127.0.0.1:9090");
    }
}
