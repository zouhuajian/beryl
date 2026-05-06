// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataWorkerService implementation.

use super::command_router::WorkerCommandRouter;
use super::manager::WorkerManager;
use super::metrics::WorkerMetrics;
use crate::error::MetadataResult;
use crate::maintenance::repair::{BlockReportDelta, RepairSignalSink};
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
}

impl WorkerBackgroundHandle {
    pub fn task_count(&self) -> usize {
        usize::from(self._lease_metrics_task.is_some())
    }
}

/// MetadataWorkerService implementation.
pub struct MetadataWorkerServiceImpl {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    repair_signal_handler: Arc<dyn RepairSignalSink>,
    command_router: Option<Arc<WorkerCommandRouter>>,
    metrics: Arc<WorkerMetrics>,
    slot_metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
    /// Mount table used to compute mount_epoch for lease gating (TODO-2).
    mount_table: Arc<crate::mount::MountTable>,
}

impl MetadataWorkerServiceImpl {
    pub(crate) fn new(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        repair_signal_handler: Arc<dyn RepairSignalSink>,
        mount_table: Arc<crate::mount::MountTable>,
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

    /// Start worker-local background tasks.
    pub(crate) fn start_background_tasks(&self) -> WorkerBackgroundHandle {
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

        WorkerBackgroundHandle {
            _lease_metrics_task: lease_metrics_task,
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
            if let Some(ref command_router) = self.command_router {
                if !req.acks.is_empty() {
                    command_router.handle_acks(worker_id, &req.acks).await;
                }

                const MAX_COMMANDS_PER_HEARTBEAT: usize = 12;
                commands.extend(command_router.poll_commands(worker_id, MAX_COMMANDS_PER_HEARTBEAT));
            } else if !req.acks.is_empty() {
                warn!(
                    worker_id = worker_id.as_raw(),
                    ack_count = req.acks.len(),
                    "Ignoring worker command acks because command router is not configured"
                );
            }
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

        // Hand block-report repair signals to maintenance. The handler owns the leader gate.
        let is_leader = self.raft_node.is_leader();
        let outcome = self
            .repair_signal_handler
            .handle_block_report_delta(BlockReportDelta {
                worker_id,
                added_blocks: added_blocks.clone(),
                removed_blocks: removed_blocks.clone(),
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to handle repair signal: {}", e)))?;
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
        proto::common::BlockIdProto {
            data_handle_id: block_id.data_handle_id.as_raw(),
            block_index: block_id.index.as_raw(),
        }
    }

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

    #[tokio::test]
    async fn block_report_applies_soft_state_and_delegates_repair_signal() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60));
        let worker_id = WorkerId::new(7);
        let block_id = BlockId::new(DataHandleId::new(70), BlockIndex::new(0));
        worker_manager
            .register_worker(worker_id, "127.0.0.1:9090".to_string(), 1, 100, None)
            .unwrap();
        worker_manager
            .update_runtime(worker_id, 1, 100, 1_000, 500, 500, 0, 0, HealthStatus::Healthy)
            .unwrap();
        worker_manager.mark_full_sync_complete(worker_id);
        let signal_sink = Arc::new(RecordingRepairSignalSink::default());
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            Arc::clone(&signal_sink) as Arc<dyn RepairSignalSink>,
            Arc::new(MountTable::new()),
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

        assert_eq!(response.report_seq, 42);
        assert_eq!(worker_manager.get_block_locations(block_id), vec![worker_id]);
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
        let block_id = BlockId::new(DataHandleId::new(80), BlockIndex::new(0));
        worker_manager
            .register_worker(worker_id, "127.0.0.1:9091".to_string(), 1, 100, None)
            .unwrap();
        worker_manager
            .update_runtime(worker_id, 1, 100, 1_000, 500, 500, 0, 0, HealthStatus::Healthy)
            .unwrap();
        worker_manager.mark_full_sync_complete(worker_id);
        worker_manager.update_locations(worker_id, vec![block_id]).unwrap();
        let signal_sink = Arc::new(RecordingRepairSignalSink::default());
        let service = MetadataWorkerServiceImpl::new(
            raft_node,
            Arc::clone(&worker_manager),
            Arc::clone(&signal_sink) as Arc<dyn RepairSignalSink>,
            Arc::new(MountTable::new()),
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

        assert_eq!(response.report_seq, 53);
        let deltas = signal_sink.deltas.lock();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].worker_id, worker_id);
        assert!(deltas[0].added_blocks.is_empty());
        assert_eq!(deltas[0].removed_blocks, vec![block_id]);
    }
}
