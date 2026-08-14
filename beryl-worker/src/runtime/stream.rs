// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Stream runtime state management.

use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use beryl_types::ids::StreamId;
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::data::core::WriteStreamContext;
use crate::observe;

const STREAM_PHASE_OPEN: u8 = 0;
const STREAM_PHASE_RETIRING: u8 = 1;

/// Mutable progress for one write stream.
#[derive(Clone, Debug)]
pub struct StreamState {
    /// Metadata-authorized facts fixed when the write stream opens.
    pub context: WriteStreamContext,
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
    pub fn new(context: WriteStreamContext) -> Self {
        Self {
            cursor: 0,
            last_acked_seq: 0,
            written_through: 0,
            last_activity: Instant::now(),
            context,
        }
    }

    fn is_idle(&self, timeout: Duration, now: Instant) -> bool {
        now.duration_since(self.last_activity) > timeout
    }
}

/// Result of looking up a stream for a new operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamAccessError {
    Missing,
    Retiring,
}

/// Exclusive access to one stream operation.
///
/// The guard spans request validation, local IO, and progress mutation. This
/// serializes cursor-sensitive operations and prevents retirement from
/// reclaiming staging files before the operation returns.
pub(crate) struct StreamOperation {
    entry: Arc<StreamEntry>,
    state: OwnedMutexGuard<StreamState>,
}

impl StreamOperation {
    /// Prevents every later operation from entering this stream.
    pub(crate) fn mark_retiring(&self) {
        self.entry.mark_retiring();
    }
}

impl Deref for StreamOperation {
    type Target = StreamState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for StreamOperation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl Drop for StreamOperation {
    fn drop(&mut self) {
        self.state.last_activity = Instant::now();
    }
}

/// One registered stream and the resources retained until exact removal.
struct StreamEntry {
    phase: AtomicU8,
    state: Arc<AsyncMutex<StreamState>>,
    _inflight: StreamInflightGuard,
}

impl StreamEntry {
    fn new(state: StreamState) -> Self {
        Self {
            phase: AtomicU8::new(STREAM_PHASE_OPEN),
            state: Arc::new(AsyncMutex::new(state)),
            _inflight: StreamInflightGuard::new(),
        }
    }

    fn is_open(&self) -> bool {
        self.phase.load(Ordering::Acquire) == STREAM_PHASE_OPEN
    }

    fn mark_retiring(&self) {
        self.phase.store(STREAM_PHASE_RETIRING, Ordering::Release);
    }
}

/// Registry contents kept under one short-lived process-local lock.
struct StreamRegistryState {
    streams: HashMap<StreamId, Arc<StreamEntry>>,
    cleanup_order: VecDeque<StreamId>,
}

/// Registry for worker-local write stream state and bounded cleanup traversal.
///
/// The registry lock never spans stream IO. Per-stream operation guards own
/// cursor mutation and provide the drain boundary for terminal transitions.
pub struct StreamManager {
    inner: Mutex<StreamRegistryState>,
    idle_timeout: Duration,
}

/// Balances the stream inflight gauge with entry lifetime.
struct StreamInflightGuard {
    active: bool,
}

impl StreamInflightGuard {
    fn new() -> Self {
        observe::increment_stream_inflight("write");
        Self { active: true }
    }
}

impl Drop for StreamInflightGuard {
    fn drop(&mut self) {
        if self.active {
            observe::decrement_stream_inflight("write");
            self.active = false;
        }
    }
}

impl StreamManager {
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            inner: Mutex::new(StreamRegistryState {
                streams: HashMap::new(),
                cleanup_order: VecDeque::new(),
            }),
            idle_timeout,
        }
    }

    pub fn with_default_timeout() -> Self {
        Self::new(Duration::from_secs(60))
    }

    /// Registers a write stream without replacing an existing identity.
    pub(crate) fn register_write(&self, state: StreamState) -> bool {
        let stream_id = state.context.stream_id;
        let mut inner = self.inner.lock();
        if inner.streams.contains_key(&stream_id) {
            return false;
        }
        let entry = Arc::new(StreamEntry::new(state));
        inner.streams.insert(stream_id, entry);
        inner.cleanup_order.push_back(stream_id);
        true
    }

    pub async fn get(&self, stream_id: StreamId) -> Option<StreamState> {
        let entry = self.entry(stream_id)?;
        let state = entry.state.lock().await.clone();
        Some(state)
    }

    /// Returns whether the write stream identity is registered.
    pub(crate) fn contains(&self, stream_id: StreamId) -> bool {
        self.entry(stream_id).is_some()
    }

    /// Acquires the exclusive operation guard only while the stream is Open.
    pub(crate) async fn begin_operation(&self, stream_id: StreamId) -> Result<StreamOperation, StreamAccessError> {
        let entry = self.entry(stream_id).ok_or(StreamAccessError::Missing)?;
        if !entry.is_open() {
            return Err(StreamAccessError::Retiring);
        }
        let state = Arc::clone(&entry.state).lock_owned().await;
        if !entry.is_open() {
            return Err(StreamAccessError::Retiring);
        }
        Ok(StreamOperation { entry, state })
    }

    /// Acquires terminal ownership only for a registered write stream.
    pub(crate) async fn begin_write_retirement(
        &self,
        stream_id: StreamId,
    ) -> Result<StreamOperation, StreamAccessError> {
        let entry = self.entry(stream_id).ok_or(StreamAccessError::Missing)?;
        entry.mark_retiring();
        let state = Arc::clone(&entry.state).lock_owned().await;
        Ok(StreamOperation { entry, state })
    }

    /// Tries to acquire a drained Retiring stream without waiting on active IO.
    pub(crate) fn try_begin_retirement(
        &self,
        stream_id: StreamId,
    ) -> Result<Option<StreamOperation>, StreamAccessError> {
        let entry = self.entry(stream_id).ok_or(StreamAccessError::Missing)?;
        entry.mark_retiring();
        let Ok(state) = Arc::clone(&entry.state).try_lock_owned() else {
            return Ok(None);
        };
        Ok(Some(StreamOperation { entry, state }))
    }

    /// Marks one write stream Retiring without waiting or performing IO.
    pub(crate) fn request_write_retirement(&self, stream_id: StreamId) -> bool {
        let Some(entry) = self.entry(stream_id) else {
            return false;
        };
        entry.mark_retiring();
        true
    }

    /// Removes exactly the entry owned by a completed terminal operation.
    pub(crate) fn complete_retirement(&self, stream_id: StreamId, operation: &StreamOperation) -> bool {
        let mut inner = self.inner.lock();
        let matches = inner
            .streams
            .get(&stream_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, &operation.entry));
        if matches {
            inner.streams.remove(&stream_id);
            if let Some(position) = inner.cleanup_order.iter().position(|queued| *queued == stream_id) {
                inner.cleanup_order.remove(position);
            }
        }
        matches
    }

    /// Returns at most `limit` streams for one cleanup pass.
    ///
    /// In normal mode, Open streams are selected only after their idle timeout.
    /// Drain mode retires every inspected Open stream. Busy streams are rotated
    /// to a later pass so one slow operation cannot block the maintenance task.
    pub(crate) fn take_cleanup_batch(&self, limit: usize, drain: bool) -> Vec<StreamId> {
        let now = Instant::now();
        let mut selected = Vec::with_capacity(limit);
        let mut inner = self.inner.lock();
        let examined = limit.min(inner.cleanup_order.len());
        for _ in 0..examined {
            let Some(stream_id) = inner.cleanup_order.pop_front() else {
                break;
            };
            let Some(entry) = inner.streams.get(&stream_id).cloned() else {
                continue;
            };
            let Ok(state) = entry.state.try_lock() else {
                inner.cleanup_order.push_back(stream_id);
                continue;
            };
            if !entry.is_open() {
                drop(state);
                selected.push(stream_id);
                inner.cleanup_order.push_back(stream_id);
                continue;
            }
            if drain || state.is_idle(self.idle_timeout, now) {
                entry.mark_retiring();
                drop(state);
                selected.push(stream_id);
                inner.cleanup_order.push_back(stream_id);
            } else {
                drop(state);
                inner.cleanup_order.push_back(stream_id);
            }
        }
        selected
    }

    pub async fn active_count(&self) -> usize {
        self.inner.lock().streams.len()
    }

    fn entry(&self, stream_id: StreamId) -> Option<Arc<StreamEntry>> {
        self.inner.lock().streams.get(&stream_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use beryl_types::ids::{BlockId, BlockIndex, ClientId, InodeId, StreamId};
    use beryl_types::layout::BlockFormatId;
    use beryl_types::lease::FencingToken;
    use beryl_types::{GroupName, WorkerRunId};

    use super::{StreamAccessError, StreamManager, StreamState};
    use crate::data::core::WriteStreamContext;

    fn write_state(stream_id: StreamId) -> StreamState {
        let block_id = BlockId::new(InodeId::new(7), BlockIndex::new(3));
        let mut state = StreamState::new(WriteStreamContext {
            stream_id,
            group_name: GroupName::parse("root").expect("group name"),
            block_id,
            worker_run_id: WorkerRunId::new(),
            end_offset: 4096,
            block_stamp: 5,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            block_size: 4096,
            chunk_size: 1024,
            fencing_token: FencingToken::new(block_id, ClientId::new(11), 2),
        });
        state.last_activity = Instant::now() - Duration::from_secs(1);
        state
    }

    #[tokio::test]
    async fn operation_guard_blocks_cleanup_and_retiring_rejects_new_operations() {
        let manager = StreamManager::new(Duration::from_millis(1));
        let stream_id = StreamId::new(1);
        assert!(manager.register_write(write_state(stream_id)));

        let operation = manager.begin_operation(stream_id).await.expect("begin operation");
        assert!(manager.take_cleanup_batch(1, false).is_empty());
        drop(operation);

        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(manager.take_cleanup_batch(1, false), vec![stream_id]);
        assert!(matches!(
            manager.begin_operation(stream_id).await,
            Err(StreamAccessError::Retiring)
        ));
    }
}
