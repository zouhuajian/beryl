// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Stream runtime state management.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use beryl_types::ids::StreamId;
use parking_lot::RwLock;

use crate::data::core::StreamContext;
use crate::data::core::StreamMode;
use crate::observe;
use crate::runtime::block::ReadPin;

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
    _read_pin: Option<ReadPin>,
    _inflight: StreamInflightGuard,
}

impl ActiveStream {
    fn new(state: StreamState, read_pin: Option<ReadPin>) -> Self {
        let mode = state.context.mode;
        Self {
            state,
            _read_pin: read_pin,
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

    pub(crate) async fn register_write(&self, state: StreamState) -> Option<StreamState> {
        assert_eq!(
            state.context.mode,
            StreamMode::Write,
            "write stream registration requires write mode"
        );
        self.streams
            .write()
            .insert(state.context.stream_id, ActiveStream::new(state, None))
            .map(|active| active.state)
    }

    /// Registers a read stream while retaining its block pin until stream removal.
    pub(crate) async fn register_read(&self, state: StreamState, read_pin: ReadPin) -> Option<StreamState> {
        assert_eq!(
            state.context.mode,
            StreamMode::Read,
            "read stream registration requires read mode"
        );
        self.streams
            .write()
            .insert(state.context.stream_id, ActiveStream::new(state, Some(read_pin)))
            .map(|active| active.state)
    }

    pub async fn get(&self, stream_id: StreamId) -> Option<StreamState> {
        self.streams.read().get(&stream_id).map(|active| active.state.clone())
    }

    /// Snapshots stream state while retaining a read pin for the current call.
    pub(crate) async fn get_for_read(&self, stream_id: StreamId) -> Option<(StreamState, Option<ReadPin>)> {
        self.streams
            .read()
            .get(&stream_id)
            .map(|active| (active.state.clone(), active._read_pin.clone()))
    }

    pub async fn touch(&self, stream_id: StreamId) -> bool {
        let mut streams = self.streams.write();
        if let Some(active) = streams.get_mut(&stream_id) {
            active.state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn update_cursor(&self, stream_id: StreamId, cursor: u64) -> bool {
        let mut streams = self.streams.write();
        if let Some(active) = streams.get_mut(&stream_id) {
            active.state.cursor = cursor;
            active.state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn ack(&self, stream_id: StreamId, seq: u64) -> bool {
        let mut streams = self.streams.write();
        if let Some(active) = streams.get_mut(&stream_id) {
            active.state.last_acked_seq = active.state.last_acked_seq.max(seq);
            active.state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn mark_written(&self, stream_id: StreamId, written_through: u64) -> bool {
        let mut streams = self.streams.write();
        if let Some(active) = streams.get_mut(&stream_id) {
            active.state.written_through = active.state.written_through.max(written_through);
            active.state.last_activity = Instant::now();
            true
        } else {
            false
        }
    }

    pub async fn advance_write_progress(&self, stream_id: StreamId, seq: u64, written_through: u64) -> bool {
        let mut streams = self.streams.write();
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
        self.remove_now(stream_id)
    }

    /// Removes one stream synchronously so cancellation cleanup never detaches.
    pub(crate) fn remove_now(&self, stream_id: StreamId) -> Option<StreamState> {
        self.streams.write().remove(&stream_id).map(|active| active.state)
    }

    pub async fn active_count(&self) -> usize {
        self.streams.read().len()
    }

    pub(crate) const fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// Drops only abandoned read streams so their block pins cannot stall
    /// reclamation forever. In-progress reads retain a cloned pin until return.
    pub(crate) async fn cleanup_idle_read_streams(&self) -> usize {
        let now = Instant::now();
        let mut streams = self.streams.write();
        let before = streams.len();
        streams.retain(|_, active| {
            active.state.context.mode != StreamMode::Read || !active.state.is_idle(self.idle_timeout, now)
        });
        before - streams.len()
    }

    pub async fn cleanup_idle_streams(&self) -> usize {
        let now = Instant::now();
        let mut streams = self.streams.write();
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
