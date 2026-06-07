// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Stream runtime state management.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use types::ids::StreamId;

use crate::data::core::StreamContext;
use crate::data::core::StreamMode;
use crate::observe;

/// Mutable state for an active stream.
#[derive(Clone, Debug)]
pub struct StreamState {
    /// Open-time context. Stable block and transport facts live here.
    pub context: StreamContext,
    /// Next block-local byte offset expected by the runtime state machine.
    pub cursor: u64,
    /// Last acknowledged frame sequence for write streams.
    pub last_acked_seq: u64,
    /// Contiguous byte prefix written into the staging block.
    /// This is not readable until final metadata is published.
    pub written_through: u64,
    /// Runtime activity timestamp used only for idle cleanup.
    pub last_activity: Instant,
}

impl StreamState {
    pub fn new(context: StreamContext) -> Self {
        Self {
            cursor: context.start_offset,
            last_acked_seq: 0,
            written_through: context.committed_length,
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
    streams: RwLock<HashMap<StreamId, ActiveStream>>,
    idle_timeout: Duration,
}

struct ActiveStream {
    state: StreamState,
    _inflight: StreamInflightGuard,
}

impl ActiveStream {
    fn new(state: StreamState) -> Self {
        let mode = state.context.mode;
        Self {
            state,
            _inflight: StreamInflightGuard::new(mode),
        }
    }
}

struct StreamInflightGuard {
    mode: &'static str,
    active: bool,
}

impl StreamInflightGuard {
    fn new(mode: StreamMode) -> Self {
        let mode = stream_mode_label(mode);
        observe::increment_stream_inflight(mode);
        Self { mode, active: true }
    }
}

impl Drop for StreamInflightGuard {
    fn drop(&mut self) {
        if self.active {
            observe::decrement_stream_inflight(self.mode);
            self.active = false;
        }
    }
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
        self.streams
            .write()
            .await
            .insert(state.context.stream_id, ActiveStream::new(state))
            .map(|active| active.state)
    }

    pub async fn get(&self, stream_id: StreamId) -> Option<StreamState> {
        self.streams
            .read()
            .await
            .get(&stream_id)
            .map(|active| active.state.clone())
    }

    pub async fn touch(&self, stream_id: StreamId) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(active) = streams.get_mut(&stream_id) {
            active.state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn update_cursor(&self, stream_id: StreamId, cursor: u64) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(active) = streams.get_mut(&stream_id) {
            active.state.cursor = cursor;
            active.state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn ack(&self, stream_id: StreamId, seq: u64) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(active) = streams.get_mut(&stream_id) {
            active.state.last_acked_seq = active.state.last_acked_seq.max(seq);
            active.state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn mark_written(&self, stream_id: StreamId, written_through: u64) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(active) = streams.get_mut(&stream_id) {
            active.state.written_through = active.state.written_through.max(written_through);
            active.state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn advance_write_progress(&self, stream_id: StreamId, seq: u64, written_through: u64) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(active) = streams.get_mut(&stream_id) {
            active.state.cursor = written_through;
            active.state.last_acked_seq = seq;
            active.state.written_through = written_through;
            active.state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn remove(&self, stream_id: StreamId) -> Option<StreamState> {
        self.streams.write().await.remove(&stream_id).map(|active| active.state)
    }

    pub async fn active_count(&self) -> usize {
        self.streams.read().await.len()
    }

    pub async fn cleanup_idle_streams(&self) -> usize {
        let now = Instant::now();
        let mut streams = self.streams.write().await;
        let before = streams.len();
        streams.retain(|_, active| !active.state.is_idle(self.idle_timeout, now));
        before - streams.len()
    }
}

fn stream_mode_label(mode: StreamMode) -> &'static str {
    match mode {
        StreamMode::Read => "read",
        StreamMode::Write => "write",
    }
}
