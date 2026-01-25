// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Command executor: Execute commands from metadata service.
//!
//! Supports:
//! - DeleteBlocks: Delete specified blocks from local storage
//! - Reconcile: Reconcile block state with metadata
//! - Throttle: Adjust concurrency limits
//! - ConfigRefresh: Refresh configuration

use anyhow::Result;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

use proto::metadata::{
    ConfigRefreshCommandProto, EvictCommandProto, MoveCopyCommandProto, ReplicateCommandProto, ThrottleCommandProto,
    WorkerCommandProto,
};
use types::ids::{BlockId, ShardGroupId, WorkerId};
use types::layout::FileLayout;

use crate::block_manager::{BlockManager, ReplicationClient};
use crate::block_store::BlockStore;
use crate::delete_op_log::{DeleteOpLog, DeleteOpResultStatus, DeleteOpState};
use crate::pending_acks::PendingAcks;

/// Command executor for metadata commands.
pub struct CommandExecutor {
    block_store: Arc<BlockStore>,
    block_manager: Arc<BlockManager>,
    replication_client: Option<Arc<dyn ReplicationClient + Send + Sync>>,
    /// Delete operation log for idempotency and crash recovery.
    delete_op_log: Arc<DeleteOpLog>,
    /// Pending TaskAcks to send back to metadata.
    pending_acks: Arc<PendingAcks>,
}

impl CommandExecutor {
    pub fn new(block_store: Arc<BlockStore>) -> Self {
        Self::with_delete_op_log(block_store, Arc::new(DeleteOpLog::new()))
    }

    pub fn with_delete_op_log(block_store: Arc<BlockStore>, delete_op_log: Arc<DeleteOpLog>) -> Self {
        // Get block_size and chunk_size from BlockStore's internal fields
        // For now, use default values (will be improved when BlockStore exposes these)
        let block_size = 33_554_432; // 32MB default
        let chunk_size = 1_048_576; // 1MB default
        let layout = FileLayout::new(block_size, chunk_size, 3); // replication factor
        let block_manager = Arc::new(BlockManager::new(Arc::clone(&block_store), layout));
        Self {
            block_store,
            block_manager,
            replication_client: None,
            delete_op_log,
            pending_acks: Arc::new(PendingAcks::new()),
        }
    }

    /// Create with replication client.
    pub fn with_replication(
        block_store: Arc<BlockStore>,
        replication_client: Arc<dyn ReplicationClient + Send + Sync>,
    ) -> Self {
        // Get block_size and chunk_size from BlockStore's internal fields
        let block_size = 33_554_432; // 32MB default
        let chunk_size = 1_048_576; // 1MB default
        let layout = FileLayout::new(block_size, chunk_size, 3); // replication factor
        let block_manager = Arc::new(BlockManager::new(Arc::clone(&block_store), layout));
        Self {
            block_store,
            block_manager,
            replication_client: Some(replication_client),
            delete_op_log: Arc::new(DeleteOpLog::new()),
            pending_acks: Arc::new(PendingAcks::new()),
        }
    }

    /// Get reference to pending acks (for metadata_client to collect).
    pub fn pending_acks(&self) -> Arc<PendingAcks> {
        Arc::clone(&self.pending_acks)
    }

    /// Execute a command from metadata.
    /// Returns TaskAck for the command (for DeleteBlocksCommand, includes per-block results).
    pub async fn execute(
        &self,
        group_id: ShardGroupId,
        command: WorkerCommandProto,
    ) -> Result<Option<proto::metadata::TaskAckProto>> {
        match command.command {
            Some(proto::metadata::worker_command_proto::Command::Evict(evict)) => {
                self.execute_evict(group_id, evict).await
            }
            Some(proto::metadata::worker_command_proto::Command::DeleteBlocks(delete_blocks)) => {
                self.execute_delete_blocks(group_id, delete_blocks, command.task_id)
                    .await
            }
            Some(proto::metadata::worker_command_proto::Command::Replicate(replicate)) => {
                self.execute_replicate(group_id, replicate).await?;
                Ok(None) // Replicate doesn't return ack
            }
            Some(proto::metadata::worker_command_proto::Command::MoveCopy(move_copy)) => {
                self.execute_move_copy(group_id, move_copy, command.task_id).await?;
                Ok(None)
            }
            Some(proto::metadata::worker_command_proto::Command::Throttle(throttle)) => {
                self.execute_throttle(group_id, throttle).await?;
                Ok(None)
            }
            Some(proto::metadata::worker_command_proto::Command::ConfigRefresh(config)) => {
                self.execute_config_refresh(group_id, config).await?;
                Ok(None)
            }
            _ => {
                warn!(group_id = group_id.as_raw(), "Unknown command type");
                Ok(None)
            }
        }
    }

    /// Execute Replicate command: Replicate block to target workers.
    async fn execute_replicate(
        &self,
        group_id: ShardGroupId,
        replicate: ReplicateCommandProto,
    ) -> Result<Option<proto::metadata::TaskAckProto>> {
        let replication_client = match &self.replication_client {
            Some(client) => client,
            None => {
                warn!(
                    group_id = group_id.as_raw(),
                    "Replicate command received but replication client not configured"
                );
                return Ok(None);
            }
        };

        // Extract block ID
        let proto_block_id = replicate
            .block_id
            .ok_or_else(|| anyhow::anyhow!("Missing block_id in ReplicateCommand"))?;
        let block_id = BlockId::new(
            types::ids::DataHandleId::new(proto_block_id.data_handle_id),
            types::ids::BlockIndex::new(proto_block_id.block_index),
        );

        // Convert target worker IDs
        let target_workers: Vec<WorkerId> = replicate.target_worker_ids.into_iter().map(WorkerId::new).collect();

        if target_workers.is_empty() {
            warn!(
                group_id = group_id.as_raw(),
                block_id = %block_id,
                "Replicate command received with no target workers"
            );
            return Ok(None);
        }

        info!(
            group_id = group_id.as_raw(),
            block_id = %block_id,
            target_workers = target_workers.len(),
            "Executing Replicate command"
        );

        // Execute replication asynchronously
        let block_manager = Arc::clone(&self.block_manager);
        let replication_client_clone = Arc::clone(replication_client);
        let group_id_val = group_id;
        let block_id_val = block_id;
        let target_workers_val = target_workers;

        tokio::spawn(async move {
            match block_manager
                .replicate_block(group_id_val, block_id_val, target_workers_val, replication_client_clone)
                .await
            {
                Ok(success_count) => {
                    info!(
                        group_id = group_id_val.as_raw(),
                        block_id = %block_id_val,
                        success_count = success_count,
                        "Block replication completed"
                    );
                }
                Err(e) => {
                    error!(
                        group_id = group_id_val.as_raw(),
                        block_id = %block_id_val,
                        error = %e,
                        "Block replication failed"
                    );
                }
            }
        });

        Ok(None) // Replicate doesn't return ack
    }

    /// Execute Evict command: Delete specified blocks.
    /// Note: Legacy command, kept for backward compatibility.
    /// New code should use DeleteBlocks command for idempotency and per-block status.
    async fn execute_evict(
        &self,
        group_id: ShardGroupId,
        evict: EvictCommandProto,
    ) -> Result<Option<proto::metadata::TaskAckProto>> {
        info!(
            group_id = group_id.as_raw(),
            blocks = evict.block_ids.len(),
            reason = %evict.reason,
            "Executing Evict command (legacy)"
        );

        // Delete each block (no idempotency for legacy command)
        for proto_block_id in evict.block_ids {
            let block_id = BlockId::new(
                types::ids::DataHandleId::new(proto_block_id.data_handle_id),
                types::ids::BlockIndex::new(proto_block_id.block_index),
            );

            if let Err(e) = self.block_store.delete_block(group_id, block_id).await {
                warn!(
                    group_id = group_id.as_raw(),
                    block = %block_id,
                    error = %e,
                    "Failed to delete block during eviction"
                );
            } else {
                info!(
                    group_id = group_id.as_raw(),
                    block = %block_id,
                    "Block evicted successfully"
                );
            }
        }

        Ok(None) // Legacy command doesn't return ack
    }

    /// Execute DeleteBlocks command: Delete blocks with idempotency and per-block status.
    /// Returns TaskAck with per-block results.
    async fn execute_delete_blocks(
        &self,
        group_id: ShardGroupId,
        delete_blocks: proto::metadata::DeleteBlocksCommandProto,
        task_id: u64,
    ) -> Result<Option<proto::metadata::TaskAckProto>> {
        let intent_id = delete_blocks.intent_id;
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Determine operation kind (before moving delete_blocks.blocks)
        let op_kind = delete_blocks.op_kind();
        let is_replica_evict = matches!(op_kind, proto::metadata::DeleteOpKindProto::DeleteOpKindReplicaEvict);

        info!(
            group_id = group_id.as_raw(),
            intent_id,
            task_id,
            blocks = delete_blocks.blocks.len(),
            op_kind = ?op_kind,
            "Executing DeleteBlocks command"
        );

        // Collect per-block results
        let mut block_results = Vec::new();
        let mut has_error = false;

        // Process each block with idempotency
        for block_req in delete_blocks.blocks {
            let Some(proto_block_id) = block_req.block_id else {
                warn!(intent_id, "DeleteBlocks: missing block_id in request");
                continue;
            };
            let block_id = BlockId::new(
                types::ids::DataHandleId::new(proto_block_id.data_handle_id),
                types::ids::BlockIndex::new(proto_block_id.block_index),
            );

            // Check DeleteOpLog for idempotency
            if let Some(entry) = self.delete_op_log.get_entry(intent_id, block_id).await? {
                match entry.state {
                    DeleteOpState::Done => {
                        // Already done - return existing result (idempotent)
                        debug!(
                            intent_id,
                            block_id = %block_id,
                            result = ?entry.result_status,
                            "Delete operation already done (idempotent)"
                        );
                        // Add per-block result for idempotent success
                        let status = match entry.result_status {
                            Some(DeleteOpResultStatus::Deleted) => {
                                proto::metadata::DeleteBlockStatusProto::DeleteBlockStatusDeleted as i32
                            }
                            Some(DeleteOpResultStatus::Tombstoned) => {
                                proto::metadata::DeleteBlockStatusProto::DeleteBlockStatusTombstoned as i32
                            }
                            Some(DeleteOpResultStatus::NotFound) => {
                                proto::metadata::DeleteBlockStatusProto::DeleteBlockStatusNotFound as i32
                            }
                            Some(DeleteOpResultStatus::Failed) => {
                                proto::metadata::DeleteBlockStatusProto::DeleteBlockStatusFailedFatal as i32
                            }
                            None => proto::metadata::DeleteBlockStatusProto::DeleteBlockStatusDeleted as i32, // Default
                        };
                        block_results.push(proto::metadata::DeleteBlockResultProto {
                            block_id: Some(proto_block_id.clone()),
                            status,
                            error_class: proto::metadata::ErrorClassProto::ErrorClassOk as i32,
                            retry_after_ms: 0,
                            message: String::new(),
                        });
                        continue; // Skip execution
                    }
                    DeleteOpState::InFlight => {
                        // Already in-flight - return IN_PROGRESS status
                        debug!(
                            intent_id,
                            block_id = %block_id,
                            "Delete operation already in-flight"
                        );
                        block_results.push(proto::metadata::DeleteBlockResultProto {
                            block_id: Some(proto_block_id.clone()),
                            status: proto::metadata::DeleteBlockStatusProto::DeleteBlockStatusInProgress as i32,
                            error_class: proto::metadata::ErrorClassProto::ErrorClassRetryable as i32,
                            retry_after_ms: 1000, // Retry after 1 second
                            message: "Delete operation already in-flight".to_string(),
                        });
                        continue; // Skip execution
                    }
                    DeleteOpState::Accepted => {
                        // Accepted but not started - can proceed
                    }
                }
            }

            // Try to acquire operation (CAS)
            let acquired = self
                .delete_op_log
                .try_acquire(intent_id, block_id, group_id, now_ms)
                .await?;

            if !acquired {
                // Not acquired (already in-flight or done)
                continue;
            }

            // Mark as in-flight
            self.delete_op_log.mark_inflight(intent_id, block_id, now_ms).await?;

            // Execute delete (or replica evict)
            let result = if is_replica_evict {
                // For REPLICA_EVICT: only delete local copy, don't affect other replicas
                self.execute_replica_evict_internal(group_id, block_id).await
            } else {
                // For DELETE: full block deletion
                self.execute_delete_block_internal(group_id, block_id).await
            };

            // Mark as done with result and add per-block result
            match result {
                Ok(()) => {
                    self.delete_op_log
                        .mark_done(intent_id, block_id, DeleteOpResultStatus::Deleted, None, now_ms)
                        .await?;
                    info!(
                        intent_id,
                        block_id = %block_id,
                        "Block deleted successfully"
                    );
                    block_results.push(proto::metadata::DeleteBlockResultProto {
                        block_id: Some(proto_block_id.clone()),
                        status: proto::metadata::DeleteBlockStatusProto::DeleteBlockStatusDeleted as i32,
                        error_class: proto::metadata::ErrorClassProto::ErrorClassOk as i32,
                        retry_after_ms: 0,
                        message: String::new(),
                    });
                }
                Err(e) => {
                    let error_str = e.to_string();
                    // Check if block not found (idempotent success)
                    if error_str.contains("not found") || error_str.contains("NotFound") {
                        self.delete_op_log
                            .mark_done(intent_id, block_id, DeleteOpResultStatus::NotFound, None, now_ms)
                            .await?;
                        info!(
                            intent_id,
                            block_id = %block_id,
                            "Block not found (idempotent success)"
                        );
                        block_results.push(proto::metadata::DeleteBlockResultProto {
                            block_id: Some(proto_block_id.clone()),
                            status: proto::metadata::DeleteBlockStatusProto::DeleteBlockStatusNotFound as i32,
                            error_class: proto::metadata::ErrorClassProto::ErrorClassOk as i32,
                            retry_after_ms: 0,
                            message: String::new(),
                        });
                    } else if error_str.contains("BUSY") || error_str.contains("Writing") {
                        // Block is busy (write/repair in-flight) - return BUSY status
                        warn!(
                            intent_id,
                            block_id = %block_id,
                            error = %e,
                            "Block is busy (write/repair in-flight), will retry"
                        );
                        // Don't mark as done - allow retry
                        // Add BUSY result
                        block_results.push(proto::metadata::DeleteBlockResultProto {
                            block_id: Some(proto_block_id.clone()),
                            status: proto::metadata::DeleteBlockStatusProto::DeleteBlockStatusBusy as i32,
                            error_class: proto::metadata::ErrorClassProto::ErrorClassRetryable as i32,
                            retry_after_ms: 5000, // Retry after 5 seconds
                            message: error_str.clone(),
                        });
                        has_error = true;
                    } else {
                        // Permanent failure
                        self.delete_op_log
                            .mark_done(
                                intent_id,
                                block_id,
                                DeleteOpResultStatus::Failed,
                                Some(error_str.clone()),
                                now_ms,
                            )
                            .await?;
                        warn!(
                            intent_id,
                            block_id = %block_id,
                            error = %e,
                            "Failed to delete block"
                        );
                        block_results.push(proto::metadata::DeleteBlockResultProto {
                            block_id: Some(proto_block_id.clone()),
                            status: proto::metadata::DeleteBlockStatusProto::DeleteBlockStatusFailedFatal as i32,
                            error_class: proto::metadata::ErrorClassProto::ErrorClassFatal as i32,
                            retry_after_ms: 0,
                            message: error_str.clone(),
                        });
                        has_error = true;
                    }
                }
            }
        }

        // Generate TaskAckProto with per-block results
        let ack_status = if has_error {
            proto::metadata::TaskAckStatusProto::TaskAckStatusRetryableFailed as i32
        } else {
            proto::metadata::TaskAckStatusProto::TaskAckStatusSuccess as i32
        };

        let ack = proto::metadata::TaskAckProto {
            task_id,
            status: ack_status,
            error_message: if has_error {
                format!("Some blocks failed to delete (see block_results)")
            } else {
                String::new()
            },
            error_class: if has_error {
                proto::metadata::ErrorClassProto::ErrorClassRetryable as i32
            } else {
                proto::metadata::ErrorClassProto::ErrorClassOk as i32
            },
            error_code: String::new(),
            verify_ok: true,
            block_results,
            intent_id,
        };

        // Store ack for sending in next heartbeat
        let ack_clone = ack.clone();
        self.pending_acks.add(ack_clone).await;

        Ok(Some(ack))
    }

    /// Internal delete block implementation.
    async fn execute_delete_block_internal(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<()> {
        // Check if block is in use (write/repair in-flight)
        // Check block state: only delete Committed/Clean/Evictable blocks
        // Note: block_meta returns Result<Option<LocalBlockMeta>>
        if let Ok(Some(block_meta)) = self.block_store.block_meta(group_id, block_id) {
            use types::block::LocalBlockState;
            match block_meta.state {
                LocalBlockState::Writing => {
                    // Block is being written - return BUSY (retryable)
                    return Err(anyhow::anyhow!("Block is in Writing state, cannot delete (BUSY)"));
                }
                LocalBlockState::Committed | LocalBlockState::Clean | LocalBlockState::Evictable => {
                    // Allowed states - proceed with deletion
                }
                _ => {
                    // Other states (e.g., Deleting) - check if already deleted
                    // For now, proceed (idempotent)
                }
            }
        } else {
            // Block not found - this is idempotent (NOT_FOUND)
            // We'll handle this in the caller
        }

        // Delete block from block_store
        self.block_store.delete_block(group_id, block_id).await?;

        // Update block meta state to Deleted/Tombstone
        // Note: block_store.delete_block already removes from block_index
        // If we need to keep tombstone, we could mark state as Deleted here

        Ok(())
    }

    /// Internal replica evict implementation (only delete local copy).
    async fn execute_replica_evict_internal(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<()> {
        // Check if block is in use (write/repair in-flight)
        if let Ok(Some(block_meta)) = self.block_store.block_meta(group_id, block_id) {
            use types::block::LocalBlockState;
            match block_meta.state {
                LocalBlockState::Writing => {
                    // Block is being written - return BUSY (retryable)
                    return Err(anyhow::anyhow!(
                        "Block is in Writing state, cannot evict replica (BUSY)"
                    ));
                }
                LocalBlockState::Committed | LocalBlockState::Clean | LocalBlockState::Evictable => {
                    // Allowed states - proceed with eviction
                }
                _ => {
                    // Other states - proceed (idempotent)
                }
            }
        } else {
            // Block not found - this is idempotent (NOT_FOUND)
            // Return success (replica already evicted)
            return Ok(());
        }

        // Delete block from block_store (only local copy)
        self.block_store.delete_block(group_id, block_id).await?;

        Ok(())
    }

    /// Execute MoveCopy command: Copy block from source worker to this worker (copy then delete).
    async fn execute_move_copy(
        &self,
        group_id: ShardGroupId,
        move_copy: MoveCopyCommandProto,
        task_id: u64,
    ) -> Result<()> {
        let block_id = if let Some(proto_block_id) = move_copy.block_id {
            BlockId::new(
                types::ids::DataHandleId::new(proto_block_id.data_handle_id),
                types::ids::BlockIndex::new(proto_block_id.block_index),
            )
        } else {
            return Err(anyhow::anyhow!("MoveCopy command missing block_id"));
        };

        let from_worker_id = WorkerId::new(move_copy.from_worker_id);
        let to_worker_id = WorkerId::new(move_copy.to_worker_id);

        info!(
            group_id = group_id.as_raw(),
            task_id = task_id,
            block_id = %block_id,
            from_worker = from_worker_id.as_raw(),
            to_worker = to_worker_id.as_raw(),
            "Executing MoveCopy command"
        );

        // TODO: Implement MoveCopy logic:
        // 1. Pull block from from_worker (using replication client)
        // 2. Write to local block_store
        // 3. Verify block (layout/bitmap/checksum)
        // 4. Return ack with verify_ok=true if successful
        // For now, just log and return success
        warn!(
            group_id = group_id.as_raw(),
            task_id = task_id,
            "MoveCopy command not fully implemented yet"
        );

        Ok(())
    }

    /// Execute Throttle command: Adjust concurrency limits.
    async fn execute_throttle(&self, group_id: ShardGroupId, throttle: ThrottleCommandProto) -> Result<()> {
        info!(
            group_id = group_id.as_raw(),
            max_reads = throttle.max_reads,
            max_writes = throttle.max_writes,
            "Executing Throttle command"
        );

        // TODO: Implement throttle adjustment
        // This would update semaphores/limiters in the service layer
        warn!(group_id = group_id.as_raw(), "Throttle command not yet implemented");

        Ok(())
    }

    /// Execute ConfigRefresh command: Refresh configuration.
    async fn execute_config_refresh(&self, group_id: ShardGroupId, config: ConfigRefreshCommandProto) -> Result<()> {
        info!(group_id = group_id.as_raw(), "Executing ConfigRefresh command");

        // TODO: Implement config refresh
        // This would update worker configuration dynamically
        if let Some(worker_config) = config.config {
            warn!(
                group_id = group_id.as_raw(),
                heartbeat_interval = worker_config.heartbeat_interval_sec,
                block_report_interval = worker_config.block_report_interval_sec,
                "ConfigRefresh command not yet implemented"
            );
        }

        Ok(())
    }
}
