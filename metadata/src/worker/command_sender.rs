// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Legacy worker push-command hook.
//!
//! Current worker command delivery is heartbeat pull: workers poll metadata via
//! heartbeat and the leader returns commands in the heartbeat response. This
//! type is retained as a narrow legacy hook for existing wiring, but it does not
//! create a worker gRPC client and does not push commands.

use crate::error::MetadataResult;
use parking_lot::RwLock;
use proto::metadata::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info, warn};
use types::ids::WorkerId;

/// Legacy command-sender handle.
///
/// Worker commands are not delivered through this type in the current runtime.
/// Actual delivery is heartbeat pull from `MetadataWorkerServiceImpl`.
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

    /// Retain the legacy push-command API without sending a command.
    ///
    /// Current command delivery is heartbeat pull: metadata returns commands in
    /// `HeartbeatResponse.command` when a worker polls. This method is a no-op
    /// legacy hook and must not be treated as a queued push path.
    pub async fn send_command(&self, worker_id: WorkerId, command: WorkerCommandProto) -> MetadataResult<()> {
        // No-op: workers receive commands only through heartbeat pull.
        let _addresses = self.worker_addresses.read();

        info!(
            worker_id = worker_id.as_raw(),
            "Push command hook ignored; worker commands are delivered by heartbeat pull"
        );

        // Keep command shape visible in logs while making delivery semantics explicit.
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
