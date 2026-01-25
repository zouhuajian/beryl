// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Pending TaskAck storage for sending back to metadata in next heartbeat.

use proto::metadata::TaskAckProto;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Pending TaskAckProto storage.
/// TaskAcks are collected here after command execution and sent back to metadata in next heartbeat.
pub struct PendingAcks {
    /// Queue of pending acks (FIFO).
    acks: Arc<RwLock<VecDeque<TaskAckProto>>>,
    /// Maximum number of acks to keep (to prevent memory leak).
    max_size: usize,
}

impl PendingAcks {
    /// Create a new PendingAcks with default max size (1000).
    pub fn new() -> Self {
        Self::with_max_size(1000)
    }

    /// Create a new PendingAcks with custom max size.
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            acks: Arc::new(RwLock::new(VecDeque::new())),
            max_size,
        }
    }

    /// Add a TaskAckProto to the pending queue.
    pub async fn add(&self, ack: TaskAckProto) {
        let task_id = ack.task_id;
        let mut acks = self.acks.write().await;
        if acks.len() >= self.max_size {
            // Remove oldest ack to make room
            acks.pop_front();
            debug!("PendingAcks queue full, dropping oldest ack");
        }
        acks.push_back(ack);
        debug!(task_id, "Added TaskAck to pending queue");
    }

    /// Take all pending acks (drains the queue).
    pub async fn take_all(&self) -> Vec<TaskAckProto> {
        let mut acks = self.acks.write().await;
        let result: Vec<TaskAckProto> = acks.drain(..).collect();
        debug!(count = result.len(), "Took all pending TaskAcks");
        result
    }

    /// Get current count of pending acks.
    pub async fn len(&self) -> usize {
        self.acks.read().await.len()
    }
}

impl Default for PendingAcks {
    fn default() -> Self {
        Self::new()
    }
}
