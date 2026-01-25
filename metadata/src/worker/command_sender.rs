// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Command sender: sends repair commands to workers.
//!
//! This module implements the client-side logic for sending commands from
//! metadata to workers via gRPC.

use crate::error::MetadataResult;
// Removed unused import: MetadataWorkerServiceClient
use parking_lot::RwLock;
use proto::metadata::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info, warn};
use types::ids::WorkerId;

/// Command sender for sending commands to workers.
pub struct CommandSender {
    /// Worker address cache: worker_id -> address
    worker_addresses: Arc<RwLock<HashMap<WorkerId, String>>>,
    /// Command retry configuration
    max_retries: u32,
    retry_backoff_ms: u64,
}

impl CommandSender {
    pub fn new(max_retries: u32, retry_backoff_ms: u64) -> Self {
        Self {
            worker_addresses: Arc::new(RwLock::new(HashMap::new())),
            max_retries,
            retry_backoff_ms,
        }
    }

    /// Update worker address.
    pub fn update_worker_address(&self, worker_id: WorkerId, address: String) {
        let mut addresses = self.worker_addresses.write();
        addresses.insert(worker_id, address);
    }

    /// Remove worker address.
    pub fn remove_worker_address(&self, worker_id: WorkerId) {
        let mut addresses = self.worker_addresses.write();
        addresses.remove(&worker_id);
    }

    /// Send a command to a worker.
    ///
    /// NOTE: System uses heartbeat pull mode - commands are returned via HeartbeatResponse.command.
    /// This method is kept for compatibility but is a NOOP. Commands are sent through
    /// the heartbeat pull mechanism (worker polls via Heartbeat RPC, metadata returns command in response).
    pub async fn send_command(&self, worker_id: WorkerId, command: WorkerCommandProto) -> MetadataResult<()> {
        // NOOP: Commands are sent via HeartbeatResponse.command (pull mode)
        // No need to create gRPC client or push commands actively
        // Worker will receive commands when it sends Heartbeat requests
        let _addresses = self.worker_addresses.read();

        info!(
            worker_id = worker_id.as_raw(),
            "Command queued (will be sent via heartbeat pull mode)"
        );

        // Log command for debugging
        match command.command {
            Some(proto::metadata::worker_command_proto::Command::Replicate(replicate)) => {
                info!(
                    worker_id = worker_id.as_raw(),
                    block_id = ?replicate.block_id,
                    target_workers = ?replicate.target_worker_ids,
                    "Replicate command"
                );
            }
            Some(proto::metadata::worker_command_proto::Command::Evict(evict)) => {
                info!(
                    worker_id = worker_id.as_raw(),
                    block_ids = ?evict.block_ids,
                    reason = %evict.reason,
                    "Evict command"
                );
            }
            _ => {
                warn!(worker_id = worker_id.as_raw(), "Unknown command type");
            }
        }

        Ok(())
    }

    /// Send command with retry.
    pub async fn send_command_with_retry(
        &self,
        worker_id: WorkerId,
        command: WorkerCommandProto,
    ) -> MetadataResult<()> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            match self.send_command(worker_id, command.clone()).await {
                Ok(()) => {
                    if attempt > 0 {
                        info!(
                            worker_id = worker_id.as_raw(),
                            attempt = attempt,
                            "Command sent successfully after retry"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);

                    // Check if error is retryable
                    if attempt < self.max_retries {
                        let backoff_ms = self.retry_backoff_ms * (1 << attempt); // Exponential backoff
                        warn!(
                            worker_id = worker_id.as_raw(),
                            attempt = attempt + 1,
                            backoff_ms = backoff_ms,
                            error = %last_error.as_ref().unwrap(),
                            "Command send failed, retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
        }

        error!(
            worker_id = worker_id.as_raw(),
            attempts = self.max_retries + 1,
            error = %last_error.as_ref().unwrap(),
            "Command send failed after all retries"
        );

        Err(last_error.unwrap())
    }
}

impl Clone for CommandSender {
    fn clone(&self) -> Self {
        Self {
            worker_addresses: Arc::clone(&self.worker_addresses),
            max_retries: self.max_retries,
            retry_backoff_ms: self.retry_backoff_ms,
        }
    }
}
