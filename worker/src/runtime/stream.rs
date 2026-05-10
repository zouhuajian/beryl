// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Stream runtime state management.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use types::ids::StreamId;

use crate::data::core::StreamContext;

/// Mutable state for an active stream.
#[derive(Clone, Debug)]
pub struct StreamState {
    /// Open-time context. Stable block and transport facts live here.
    pub context: StreamContext,
    /// Next block-local byte offset expected by the runtime state machine.
    pub cursor: u64,
    /// Last acknowledged frame sequence for write streams.
    pub last_acked_seq: u64,
    /// Highest block-local byte offset known to be persisted by the worker.
    pub persisted_through: u64,
    /// Runtime activity timestamp used only for idle cleanup.
    pub last_activity: Instant,
}

impl StreamState {
    pub fn new(context: StreamContext) -> Self {
        Self {
            cursor: context.byte_range.map_or(0, |range| range.offset),
            last_acked_seq: 0,
            persisted_through: context.committed_length,
            last_activity: Instant::now(),
            context,
        }
    }

    fn is_idle(&self, timeout: Duration, now: Instant) -> bool {
        now.duration_since(self.last_activity) > timeout
    }
}

/// Registry for active stream runtime state.
pub struct StreamManager {
    streams: RwLock<HashMap<StreamId, StreamState>>,
    idle_timeout: Duration,
}

impl StreamManager {
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
            idle_timeout,
        }
    }

    pub fn with_default_timeout() -> Self {
        Self::new(Duration::from_secs(60))
    }

    pub async fn register(&self, state: StreamState) -> Option<StreamState> {
        self.streams.write().await.insert(state.context.stream_id, state)
    }

    pub async fn get(&self, stream_id: StreamId) -> Option<StreamState> {
        self.streams.read().await.get(&stream_id).cloned()
    }

    pub async fn touch(&self, stream_id: StreamId) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(state) = streams.get_mut(&stream_id) {
            state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn update_cursor(&self, stream_id: StreamId, cursor: u64) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(state) = streams.get_mut(&stream_id) {
            state.cursor = cursor;
            state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn ack(&self, stream_id: StreamId, seq: u64) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(state) = streams.get_mut(&stream_id) {
            state.last_acked_seq = state.last_acked_seq.max(seq);
            state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn mark_persisted(&self, stream_id: StreamId, persisted_through: u64) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(state) = streams.get_mut(&stream_id) {
            state.persisted_through = state.persisted_through.max(persisted_through);
            state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn remove(&self, stream_id: StreamId) -> Option<StreamState> {
        self.streams.write().await.remove(&stream_id)
    }

    pub async fn active_count(&self) -> usize {
        self.streams.read().await.len()
    }

    pub async fn cleanup_idle_streams(&self) -> usize {
        let now = Instant::now();
        let mut streams = self.streams.write().await;
        let before = streams.len();
        streams.retain(|_, state| !state.is_idle(self.idle_timeout, now));
        before - streams.len()
    }
}
