// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata client: Worker ↔ Metadata control plane (multi-raft group).
//!
//! This module implements:
//! - Register: Worker registration with metadata service
//! - Heartbeat: Periodic heartbeat with capacity/load/health info
//! - BlockReport: Block/chunk presence reporting (FULL or DELTA)
//! - Command execution: Handle commands from metadata (DeleteBlocks, Reconcile, Throttle, etc.)
//!
//! All operations are scoped by group_id (multi-raft group support).

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, sleep};
use tracing::{debug, error, info};

use common::header::RequestHeader;
use proto::common::{BlockIdProto as ProtoBlockId, EndpointProto as ProtoEndpoint, RequestHeaderProto};
use proto::metadata::{
    metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient, BlockReportEntryFullProto,
    BlockReportRequestProto, BlockReportTypeProto, CapacityInfoProto, HealthStatusProto, HeartbeatRequestProto,
    LoadInfoProto, RegisterWorkerRequestProto, RegisterWorkerResponseProto, WorkerCommandProto,
};
use tonic::transport::Channel;
use tonic::Request;
use types::ids::{ShardGroupId, WorkerId};

use crate::block_store::BlockStore;
use crate::command_executor::CommandExecutor;

/// Session state for a metadata group.
#[derive(Clone, Debug, PartialEq)]
pub enum SessionState {
    /// Not connected.
    Disconnected,
    /// Connecting to metadata.
    Connecting,
    /// Registered and active.
    Registered,
    /// Active (heartbeat working).
    Active,
    /// Leader changed, need to reconnect.
    LeaderChanged,
    /// In backoff, waiting before retry.
    Backoff,
}

/// Per-metadata sync state: tracks whether worker has completed full sync with this metadata node.
#[derive(Clone, Debug)]
struct MetadataSyncState {
    /// Metadata epoch/instance_id (to detect metadata restarts).
    metadata_epoch: u64,
    /// Whether full block report has been sent and acknowledged.
    full_synced: bool,
    /// Timestamp of last full report (Unix timestamp in milliseconds).
    last_full_report_ts: u64,
    /// Pending full report flag (set when RequestFullBlockReport is received).
    pending_full: bool,
    /// Last sequence number (for INCREMENTAL dedup, optional).
    last_seq: u64,
}

impl Default for MetadataSyncState {
    fn default() -> Self {
        Self {
            metadata_epoch: 0,
            full_synced: false,
            last_full_report_ts: 0,
            pending_full: false,
            last_seq: 0,
        }
    }
}

/// Metadata group session.
pub struct MetadataSession {
    /// Group ID.
    group_id: ShardGroupId,
    /// Current session state.
    state: Arc<RwLock<SessionState>>,
    /// Metadata endpoint.
    endpoint: String,
    /// gRPC client (recreated on leader change).
    client: Arc<RwLock<Option<MetadataWorkerServiceProtoClient<Channel>>>>,
    /// Worker ID (assigned by metadata).
    worker_id: Arc<RwLock<Option<WorkerId>>>,
    /// Last heartbeat RTT in milliseconds.
    last_heartbeat_rtt_ms: Arc<RwLock<Option<u64>>>,
    /// Last successful block report sequence.
    last_report_seq: Arc<RwLock<u64>>,
    /// Per-metadata sync state (tracks full sync status with this metadata node).
    sync_state: Arc<RwLock<MetadataSyncState>>,
    /// Backoff until timestamp (Unix timestamp in milliseconds, 0 = no backoff).
    backoff_until_ms: Arc<RwLock<u64>>,
}

impl MetadataSession {
    pub fn new(group_id: ShardGroupId, endpoint: String) -> Self {
        Self {
            group_id,
            state: Arc::new(RwLock::new(SessionState::Disconnected)),
            endpoint: endpoint.clone(),
            client: Arc::new(RwLock::new(None)),
            worker_id: Arc::new(RwLock::new(None)),
            last_heartbeat_rtt_ms: Arc::new(RwLock::new(None)),
            last_report_seq: Arc::new(RwLock::new(0)),
            sync_state: Arc::new(RwLock::new(MetadataSyncState::default())),
            backoff_until_ms: Arc::new(RwLock::new(0)),
        }
    }

    pub fn group_id(&self) -> ShardGroupId {
        self.group_id
    }

    pub async fn state(&self) -> SessionState {
        self.state.read().await.clone()
    }

    pub async fn set_state(&self, new_state: SessionState) {
        *self.state.write().await = new_state;
    }

    pub async fn worker_id(&self) -> Option<WorkerId> {
        *self.worker_id.read().await
    }

    pub async fn set_worker_id(&self, worker_id: WorkerId) {
        *self.worker_id.write().await = Some(worker_id);
    }

    pub async fn last_heartbeat_rtt_ms(&self) -> Option<u64> {
        *self.last_heartbeat_rtt_ms.read().await
    }

    pub async fn set_heartbeat_rtt(&self, rtt_ms: u64) {
        *self.last_heartbeat_rtt_ms.write().await = Some(rtt_ms);
    }

    pub async fn last_report_seq(&self) -> u64 {
        *self.last_report_seq.read().await
    }

    pub async fn set_report_seq(&self, seq: u64) {
        *self.last_report_seq.write().await = seq;
    }
}

/// Metadata client for worker ↔ metadata communication.
pub struct MetadataClient {
    /// Worker ID (local).
    worker_id: WorkerId,
    /// Worker endpoint (for registration).
    worker_endpoint: String,
    /// Network transport kind (0=unspecified/grpc, 1=grpc, 2=quic, 3=rdma).
    net_transport_kind: i32,
    /// Worker epoch/boot_id (monotonically increasing, generated at startup).
    worker_epoch: u64,
    /// Block store (for block reports).
    block_store: Arc<BlockStore>,
    /// Command executor for executing commands from metadata.
    command_executor: Arc<CommandExecutor>,
    /// Sessions per group_id.
    sessions: Arc<RwLock<HashMap<ShardGroupId, Arc<MetadataSession>>>>,
    /// Command receiver channel.
    command_receiver: mpsc::UnboundedReceiver<(ShardGroupId, WorkerCommandProto)>,
    /// Command sender (for internal use).
    _command_sender: mpsc::UnboundedSender<(ShardGroupId, WorkerCommandProto)>,
    /// Heartbeat interval.
    heartbeat_interval: Duration,
    /// Block report interval.
    block_report_interval: Duration,
    /// Backoff duration on failure.
    backoff_duration: Duration,
}

impl MetadataClient {
    /// Create a new metadata client.
    pub fn new(
        worker_id: WorkerId,
        worker_endpoint: String,
        net_transport_kind: i32,
        worker_epoch: u64,
        block_store: Arc<BlockStore>,
        command_executor: Arc<CommandExecutor>,
        heartbeat_interval: Duration,
        block_report_interval: Duration,
        backoff_duration: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            worker_id,
            worker_endpoint,
            net_transport_kind,
            worker_epoch,
            block_store,
            command_executor,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            command_receiver: rx,
            _command_sender: tx,
            heartbeat_interval,
            block_report_interval,
            backoff_duration,
        }
    }

    /// Add a metadata group session.
    pub async fn add_group(&self, group_id: ShardGroupId, endpoint: String) {
        let endpoint_display = endpoint.clone();
        let session = Arc::new(MetadataSession::new(group_id, endpoint));
        let mut sessions = self.sessions.write().await;
        sessions.insert(group_id, session);
        info!(group_id = group_id.as_raw(), endpoint = %endpoint_display, "Added metadata group");
    }

    /// Register worker with all metadata groups.
    pub async fn register_all(&self) -> Result<()> {
        let sessions = self.sessions.read().await;
        let mut tasks = Vec::new();

        for (group_id, session) in sessions.iter() {
            let session_clone = Arc::clone(session);
            let worker_id = self.worker_id;
            let worker_endpoint = self.worker_endpoint.clone();
            let net_transport_kind = self.net_transport_kind;
            let worker_epoch = self.worker_epoch;
            let group_id_val = *group_id;

            tasks.push(tokio::spawn(async move {
                Self::register_group(
                    session_clone,
                    worker_id,
                    worker_endpoint,
                    net_transport_kind,
                    worker_epoch,
                    group_id_val,
                )
                .await
            }));
        }

        // Wait for all registrations
        let mut errors = Vec::new();
        for task in tasks {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => errors.push(format!("Registration error: {}", e)),
                Err(e) => errors.push(format!("Task join error: {}", e)),
            }
        }

        if !errors.is_empty() {
            return Err(anyhow::anyhow!("Some registrations failed: {:?}", errors));
        }

        Ok(())
    }

    /// Register worker with a specific metadata group.
    async fn register_group(
        session: Arc<MetadataSession>,
        worker_id: WorkerId,
        worker_endpoint: String,
        net_transport_kind: i32,
        worker_epoch: u64,
        group_id: ShardGroupId,
    ) -> Result<()> {
        session.set_state(SessionState::Connecting).await;

        // Create gRPC channel
        let endpoint = session.endpoint.clone();
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| anyhow::anyhow!("Invalid endpoint {}: {}", endpoint, e))?
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", endpoint, e))?;

        let mut client = MetadataWorkerServiceProtoClient::new(channel);

        // Build register request
        let ctx = RequestHeader::new(types::ClientId::new(0));
        let req_header: RequestHeaderProto = (&ctx).into();

        let endpoint_parts: Vec<&str> = worker_endpoint.split(':').collect();
        let endpoint_proto = if endpoint_parts.len() == 2 {
            Some(ProtoEndpoint {
                host: endpoint_parts[0].to_string(),
                port: endpoint_parts[1].parse().unwrap_or(9090),
                protocol: "grpc".to_string(), // Default to grpc
            })
        } else {
            None
        };

        // Get net_transport_kind and worker_epoch from MetadataClient
        // Note: These are passed through the closure, we need to get them from the struct
        // For now, we'll pass them as parameters to register_group
        let request = RegisterWorkerRequestProto {
            header: Some(req_header),
            endpoint: endpoint_proto,
            capabilities: 0,
            version: env!("CARGO_PKG_VERSION").to_string(),
            labels: std::collections::HashMap::new(),
            suggested_worker_id: worker_id.as_raw(),
            net_transport_kind: net_transport_kind,
            worker_epoch: worker_epoch,
        };

        // Call RegisterWorker
        let response: RegisterWorkerResponseProto = client
            .register_worker(Request::new(request))
            .await
            .map_err(|e| anyhow::anyhow!("RegisterWorker RPC failed: {}", e))?
            .into_inner();

        // Store client and worker_id
        {
            let mut client_guard = session.client.write().await;
            *client_guard = Some(client);
        }
        session.set_worker_id(WorkerId::new(response.worker_id)).await;
        session.set_state(SessionState::Registered).await;

        info!(
            group_id = group_id.as_raw(),
            worker_id = response.worker_id,
            "Successfully registered with metadata group"
        );

        Ok(())
    }

    /// Start heartbeat loop for all groups.
    pub async fn start_heartbeat_loop(&self) {
        let mut interval = interval(self.heartbeat_interval);
        loop {
            interval.tick().await;
            self.heartbeat_all().await;
        }
    }

    /// Send heartbeat to all groups.
    async fn heartbeat_all(&self) {
        let sessions_clone = Arc::clone(&self.sessions);
        let worker_id = self.worker_id;
        let command_executor = Arc::clone(&self.command_executor);

        let sessions = sessions_clone.read().await;
        let mut tasks = Vec::new();

        for (group_id, session) in sessions.iter() {
            let session_clone = Arc::clone(session);
            let command_executor_clone = Arc::clone(&command_executor);
            let net_transport_kind = self.net_transport_kind;
            let worker_epoch = self.worker_epoch;
            let group_id_val = *group_id;

            tasks.push(tokio::spawn(async move {
                if let Err(e) = Self::heartbeat_group(
                    session_clone,
                    worker_id,
                    net_transport_kind,
                    worker_epoch,
                    group_id_val,
                    command_executor_clone,
                )
                .await
                {
                    error!(group_id = group_id_val.as_raw(), error = %e, "Heartbeat failed");
                }
            }));
        }
        drop(sessions);

        // Wait for all tasks (or spawn and forget)
        for task in tasks {
            let _ = task.await;
        }
    }

    /// Send heartbeat to a specific group.
    async fn heartbeat_group(
        session: Arc<MetadataSession>,
        worker_id: WorkerId,
        net_transport_kind: i32,
        worker_epoch: u64,
        group_id: ShardGroupId,
        command_executor: Arc<CommandExecutor>,
    ) -> Result<()> {
        let state = session.state().await;
        if state != SessionState::Registered && state != SessionState::Active {
            return Ok(()); // Skip if not registered
        }

        let start = Instant::now();

        // Build heartbeat request
        let ctx = RequestHeader::new(types::ClientId::new(0));
        let req_header: RequestHeaderProto = (&ctx).into();

        // Collect task acknowledgments from command_executor
        let pending_acks = command_executor.pending_acks();
        let acks = pending_acks.take_all().await;

        let request = HeartbeatRequestProto {
            header: Some(req_header),
            worker_id: worker_id.as_raw(),
            capacity: Some(CapacityInfoProto {
                total_bytes: 1_000_000_000_000,   // 1TB (placeholder)
                used_bytes: 100_000_000_000,      // 100GB (placeholder)
                available_bytes: 900_000_000_000, // 900GB (placeholder)
            }),
            load: Some(LoadInfoProto {
                active_reads: 0,
                active_writes: 0,
                cpu_usage_percent: 0,
                memory_used_bytes: 0,
            }),
            health: HealthStatusProto::HealthStatusHealthy as i32,
            net_transport_kind: net_transport_kind,
            worker_epoch: worker_epoch,
            acks, // Task acknowledgments (collected from command_executor)
        };

        // Get client
        let client_opt = {
            let client_guard = session.client.read().await;
            client_guard.clone()
        };

        let response = if let Some(mut client) = client_opt {
            // Call gRPC Heartbeat
            let result = client.heartbeat(Request::new(request)).await;
            match result {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    // Leader changed or connection error
                    let session_clone = Arc::clone(&session);
                    let error_code = e.code();
                    tokio::spawn(async move {
                        if error_code == tonic::Code::Unavailable || error_code == tonic::Code::NotFound {
                            session_clone.set_state(SessionState::LeaderChanged).await;
                        }
                    });
                    return Err(anyhow::anyhow!("Heartbeat RPC failed: {}", e));
                }
            }
        } else {
            // No client, skip
            return Ok(());
        };

        let rtt_ms = start.elapsed().as_millis() as u64;
        session.set_heartbeat_rtt(rtt_ms).await;
        session.set_state(SessionState::Active).await;

        // Handle commands from response (support both single command and batch commands)
        let commands_to_execute = if !response.commands.is_empty() {
            // Use batch commands (new API)
            response.commands
        } else {
            vec![]
        };

        for command in commands_to_execute {
            debug!(
                group_id = group_id.as_raw(),
                task_id = command.task_id,
                "Received command in heartbeat response"
            );
            // Execute command via CommandExecutor
            let command_executor_clone = Arc::clone(&command_executor);
            let group_id_val = group_id;
            tokio::spawn(async move {
                match command_executor_clone.execute(group_id_val, command).await {
                    Ok(Some(_ack)) => {
                        // Ack stored in pending_acks, will be sent in next heartbeat
                    }
                    Ok(None) => {
                        // No ack needed
                    }
                    Err(e) => {
                        error!(group_id = group_id_val.as_raw(), error = %e, "Failed to execute command from heartbeat");
                    }
                }
            });
        }

        Ok(())
    }

    /// Start block report loop for all groups.
    pub async fn start_block_report_loop(&self) {
        // Initial full report to all metadata nodes (delayed)
        sleep(Duration::from_secs(5)).await;
        self.block_report_all(true).await;

        // Then periodic incremental reports
        let mut interval = interval(self.block_report_interval);
        loop {
            interval.tick().await;
            self.block_report_all(false).await;
        }
    }

    /// Send block report to all groups.
    async fn block_report_all(&self, force_full: bool) {
        let sessions_clone = Arc::clone(&self.sessions);
        let block_store = Arc::clone(&self.block_store);
        let command_executor = Arc::clone(&self.command_executor);
        let worker_id = self.worker_id;

        let sessions = sessions_clone.read().await;
        let mut tasks = Vec::new();

        for (group_id, session) in sessions.iter() {
            let session_clone = Arc::clone(session);
            let block_store_clone = Arc::clone(&block_store);
            let command_executor_clone = Arc::clone(&command_executor);
            let group_id_val = *group_id;

            // Check if we need full report (force_full or pending_full or not synced)
            let needs_full = {
                let sync_state = session_clone.sync_state.read().await;
                force_full || !sync_state.full_synced || sync_state.pending_full
            };

            // Check backoff
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let backoff_until = *session_clone.backoff_until_ms.read().await;
            if needs_full && backoff_until > now_ms {
                // Still in backoff, skip this round
                continue;
            }

            tasks.push(tokio::spawn(async move {
                if let Err(e) = Self::block_report_group(
                    session_clone,
                    block_store_clone,
                    command_executor_clone,
                    worker_id,
                    group_id_val,
                    needs_full,
                )
                .await
                {
                    error!(group_id = group_id_val.as_raw(), error = %e, "Block report failed");
                }
            }));
        }
        drop(sessions);

        // Wait for all tasks (or spawn and forget)
        for task in tasks {
            let _ = task.await;
        }
    }

    /// Send block report to a specific group.
    async fn block_report_group(
        session: Arc<MetadataSession>,
        block_store: Arc<BlockStore>,
        command_executor: Arc<CommandExecutor>,
        worker_id: WorkerId,
        group_id: ShardGroupId,
        full: bool,
    ) -> Result<()> {
        let state = session.state().await;
        if state != SessionState::Active {
            return Ok(()); // Skip if not active
        }

        // Check if we should send FULL or INCREMENTAL
        let sync_state = session.sync_state.read().await;
        let should_send_full = full || !sync_state.full_synced || sync_state.pending_full;
        let last_seq = sync_state.last_seq;
        drop(sync_state);

        // Get blocks for this group
        let blocks = block_store.list_blocks(group_id);

        let ctx = RequestHeader::new(types::ClientId::new(0));
        let req_header: RequestHeaderProto = (&ctx).into();

        let request = if should_send_full {
            // FULL report: send complete snapshot
            let mut full_entries = Vec::new();
            for block_meta in blocks {
                use proto::common::ChunkBitmapProto as ProtoChunkBitmap;
                let chunk_bitmap = Some(ProtoChunkBitmap {
                    bits: block_meta.chunk_bitmap.bits.clone(),
                });

                full_entries.push(BlockReportEntryFullProto {
                    block_id: Some(ProtoBlockId {
                        data_handle_id: block_meta.block_id.data_handle_id.as_raw(),
                        block_index: block_meta.block_id.index.as_raw(),
                    }),
                    chunk_bitmap,
                });
            }

            BlockReportRequestProto {
                header: Some(req_header),
                worker_id: worker_id.as_raw(),
                report_type: BlockReportTypeProto::BlockReportTypeFull as i32,
                full_entries,
                delta_entries: vec![],
                last_report_seq: 0,         // FULL reports don't use sequence
                full_report_lease_token: 0, // TODO: Get from heartbeat response
            }
        } else {
            // INCREMENTAL report: send delta operations
            // TODO: Track block changes (ADD/REMOVE) since last report
            // For now, we'll send empty delta (simplified implementation)
            // In production, this would track block additions/removals
            BlockReportRequestProto {
                header: Some(req_header),
                worker_id: worker_id.as_raw(),
                report_type: BlockReportTypeProto::BlockReportTypeIncremental as i32,
                full_entries: vec![],
                delta_entries: vec![], // TODO: Implement delta tracking
                last_report_seq: last_seq,
                full_report_lease_token: 0, // Not used for incremental reports
            }
        };

        // Get client
        let client_opt = {
            let client_guard = session.client.read().await;
            client_guard.clone()
        };

        let response = if let Some(mut client) = client_opt {
            // Call gRPC BlockReport
            let result = client.block_report(Request::new(request)).await;
            match result {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    // Leader changed or connection error
                    let session_clone = Arc::clone(&session);
                    let error_code = e.code();
                    tokio::spawn(async move {
                        if error_code == tonic::Code::Unavailable || error_code == tonic::Code::NotFound {
                            session_clone.set_state(SessionState::LeaderChanged).await;
                        }
                    });
                    return Err(anyhow::anyhow!("BlockReport RPC failed: {}", e));
                }
            }
        } else {
            // No client, skip
            return Ok(());
        };

        // Update sync state based on response
        {
            let mut sync_state = session.sync_state.write().await;
            if should_send_full {
                // FULL report succeeded
                sync_state.full_synced = true;
                sync_state.pending_full = false;
                sync_state.last_full_report_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
            }
            sync_state.last_seq = response.report_seq;
        }

        // Update sequence number
        session.set_report_seq(response.report_seq).await;

        // Handle retry_after_ms (storm control)
        if response.retry_after_ms > 0 {
            let retry_after_ms = response.retry_after_ms;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let backoff_until = now_ms + retry_after_ms as u64;
            *session.backoff_until_ms.write().await = backoff_until;
            info!(
                group_id = group_id.as_raw(),
                retry_after_ms = retry_after_ms,
                "Received backoff request, will retry after {}ms",
                retry_after_ms
            );
        }

        // Handle commands from response
        for command in response.commands {
            // Handle control commands (RequestFullBlockReport, BlockReportBackoff)
            if command.task_id == 0 {
                match command.command {
                    Some(proto::metadata::worker_command_proto::Command::RequestFullBlockReport(cmd)) => {
                        info!(
                            group_id = group_id.as_raw(),
                            metadata_epoch = cmd.target_metadata_epoch,
                            reason = %cmd.reason,
                            "Received RequestFullBlockReport command"
                        );
                        // Mark pending_full and update metadata_epoch
                        let mut sync_state = session.sync_state.write().await;
                        sync_state.pending_full = true;
                        sync_state.full_synced = false;
                        sync_state.metadata_epoch = cmd.target_metadata_epoch;
                        // Apply jitter: wait random delay before sending FULL
                        use rand::Rng;
                        let jitter_ms = rand::thread_rng().gen_range(0..5000); // 0-5 seconds jitter
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;
                        *session.backoff_until_ms.write().await = now_ms + jitter_ms;
                    }
                    Some(proto::metadata::worker_command_proto::Command::BlockReportBackoff(cmd)) => {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;
                        let backoff_until = now_ms + cmd.retry_after_ms as u64;
                        *session.backoff_until_ms.write().await = backoff_until;
                        info!(
                            group_id = group_id.as_raw(),
                            retry_after_ms = cmd.retry_after_ms,
                            "Received BlockReportBackoff command"
                        );
                    }
                    _ => {
                        // Regular command (repair task, etc.)
                        debug!(
                            group_id = group_id.as_raw(),
                            task_id = command.task_id,
                            "Received command in block report response"
                        );
                        // Execute command via CommandExecutor
                        let command_executor_clone = Arc::clone(&command_executor);
                        let group_id_val = group_id;
                        tokio::spawn(async move {
                            match command_executor_clone.execute(group_id_val, command).await {
                                Ok(Some(_ack)) => {
                                    // Ack stored in pending_acks, will be sent in next heartbeat
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    error!(group_id = group_id_val.as_raw(), error = %e, "Failed to execute command from block report");
                                }
                            }
                        });
                    }
                }
            } else {
                // Regular command (repair task, etc.)
                debug!(
                    group_id = group_id.as_raw(),
                    task_id = command.task_id,
                    "Received command in block report response"
                );
                // Execute command via CommandExecutor
                let command_executor_clone = Arc::clone(&command_executor);
                let group_id_val = group_id;
                tokio::spawn(async move {
                    match command_executor_clone.execute(group_id_val, command).await {
                        Ok(Some(_ack)) => {
                            // Ack stored in pending_acks, will be sent in next heartbeat
                        }
                        Ok(None) => {}
                        Err(e) => {
                            error!(group_id = group_id_val.as_raw(), error = %e, "Failed to execute command from block report");
                        }
                    }
                });
            }
        }

        Ok(())
    }

    /// Handle command from metadata.
    pub async fn handle_command(&self, group_id: ShardGroupId, command: WorkerCommandProto) -> Result<()> {
        info!(group_id = group_id.as_raw(), "Received command from metadata");
        // Execute command via CommandExecutor
        match self.command_executor.execute(group_id, command).await {
            Ok(Some(_ack)) => {
                debug!("Command execution returned TaskAck (stored for next heartbeat)");
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
