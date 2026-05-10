// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Stream state management with TTL/idle timeout cleanup.
//!
//! This module implements stream state tracking for OpenReadStream/OpenWriteStream operations.
//! Streams are automatically cleaned up after idle timeout or when gRPC client cancels.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info};
use types::chunk::ByteRange;
use types::ids::{BlockId, StreamId};

pub use crate::core::StreamMode;

/// Stream state for tracking active streams.
#[derive(Clone, Debug)]
pub struct StreamState {
    /// Stream ID.
    pub stream_id: StreamId,
    /// Block ID this stream is bound to.
    pub block_id: BlockId,
    /// Stream mode (read/write).
    pub mode: StreamMode,
    /// Block-local byte range for read streams.
    pub byte_range: Option<ByteRange>,
    /// Current cursor/offset in the stream.
    pub cursor: u64,
    /// Negotiated chunk size.
    pub chunk_size: u32,
    /// Flow control window size.
    pub flow_control_window: u32,
    /// Block stamp at stream open time (for validation).
    pub block_stamp: u64,
    /// Committed length at stream open time.
    pub committed_length: u64,
    /// Last received offset (bytes).
    pub last_received: u64,
    /// Last persisted offset (bytes).
    pub last_persisted: u64,
    /// Lease epoch for fencing (if provided).
    pub lease_epoch: Option<u64>,
    /// Fencing owner (client_id) for validation.
    pub fencing_owner: Option<types::ids::ClientId>,
    /// Last activity timestamp.
    pub last_activity: Instant,
}

impl StreamState {
    /// Create a new stream state.
    pub fn new(
        stream_id: StreamId,
        block_id: BlockId,
        mode: StreamMode,
        byte_range: Option<ByteRange>,
        chunk_size: u32,
        flow_control_window: u32,
        block_stamp: u64,
        committed_length: u64,
    ) -> Self {
        Self {
            stream_id,
            block_id,
            mode,
            byte_range,
            cursor: 0,
            chunk_size,
            flow_control_window,
            block_stamp,
            committed_length,
            last_received: committed_length,
            last_persisted: committed_length,
            lease_epoch: None,
            fencing_owner: None,
            last_activity: Instant::now(),
        }
    }

    /// Update last activity timestamp.
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Check if stream is idle (exceeded timeout).
    pub fn is_idle(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }
}

/// Stream manager for tracking and cleaning up stream states.
pub struct StreamManager {
    /// Active streams: stream_id -> StreamState.
    streams: Arc<RwLock<HashMap<StreamId, StreamState>>>,
    /// Index from block_id to stream_id (write streams).
    by_block: Arc<RwLock<HashMap<BlockId, StreamId>>>,
    /// Idle timeout for stream cleanup.
    idle_timeout: Duration,
    /// Background cleanup task handle.
    _cleanup_handle: tokio::task::JoinHandle<()>,
}

impl StreamManager {
    /// Create a new stream manager with idle timeout.
    pub fn new(idle_timeout: Duration) -> Self {
        let streams = Arc::new(RwLock::new(HashMap::new()));
        let by_block = Arc::new(RwLock::new(HashMap::new()));
        let streams_clone = Arc::clone(&streams);
        let by_block_clone = Arc::clone(&by_block);
        let timeout = idle_timeout;

        // Start background cleanup task
        let cleanup_handle = tokio::spawn(async move {
            // TODO: The check interval in seconds needs to be obtained from the configuration file.
            let mut interval = interval(Duration::from_secs(10)); // Check every 10 seconds
            loop {
                interval.tick().await;
                Self::cleanup_idle_streams(&streams_clone, &by_block_clone, timeout).await;
            }
        });

        Self {
            streams,
            by_block,
            idle_timeout: timeout,
            _cleanup_handle: cleanup_handle,
        }
    }

    /// Create with default idle timeout (60 seconds).
    pub fn with_default_timeout() -> Self {
        Self::new(Duration::from_secs(60))
    }

    /// Register a new stream.
    pub async fn register(&self, state: StreamState) {
        let stream_id = state.stream_id;
        let block_id = state.block_id;
        let mut streams = self.streams.write().await;
        streams.insert(stream_id, state);
        self.by_block.write().await.insert(block_id, stream_id);
        debug!(stream_id = %stream_id, "Stream registered");
    }

    /// Get stream state by ID.
    pub async fn get(&self, stream_id: StreamId) -> Option<StreamState> {
        let mut streams = self.streams.write().await;
        streams.get_mut(&stream_id).map(|state| {
            state.touch();
            state.clone()
        })
    }

    /// Update stream cursor and touch activity.
    pub async fn update_cursor(&self, stream_id: StreamId, cursor: u64) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(state) = streams.get_mut(&stream_id) {
            state.cursor = cursor;
            state.last_received = cursor;
            state.touch();
            true
        } else {
            false
        }
    }

    /// Update persisted offset (after fsync/commit).
    pub async fn update_persisted(&self, stream_id: StreamId, persisted: u64) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(state) = streams.get_mut(&stream_id) {
            if persisted > state.last_persisted {
                state.last_persisted = persisted;
            }
            state.touch();
            true
        } else {
            false
        }
    }

    /// Wait until last_persisted >= target or timeout_ms expires.
    pub async fn wait_persisted(&self, stream_id: StreamId, target: u64, timeout_ms: u64) -> bool {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let persisted = {
                let streams = self.streams.read().await;
                streams.get(&stream_id).map(|s| s.last_persisted)
            };
            if let Some(p) = persisted {
                if p >= target {
                    return true;
                }
            } else {
                return false;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Remove a stream (called on client cancel or explicit close).
    pub async fn remove(&self, stream_id: StreamId) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(state) = streams.remove(&stream_id) {
            self.by_block.write().await.remove(&state.block_id);
            true
        } else {
            false
        }
    }

    /// Find a write stream by block_id.
    pub async fn find_by_block(&self, block_id: BlockId) -> Option<StreamState> {
        let maybe_stream_id = { self.by_block.read().await.get(&block_id).copied() };
        if let Some(sid) = maybe_stream_id {
            self.get(sid).await
        } else {
            None
        }
    }

    /// Get active stream count.
    pub async fn active_count(&self) -> usize {
        let streams = self.streams.read().await;
        streams.len()
    }

    /// Cleanup idle streams (called by background task).
    async fn cleanup_idle_streams(
        streams: &Arc<RwLock<HashMap<StreamId, StreamState>>>,
        by_block: &Arc<RwLock<HashMap<BlockId, StreamId>>>,
        timeout: Duration,
    ) {
        let mut streams_guard = streams.write().await;
        let mut to_remove = Vec::new();

        for (stream_id, state) in streams_guard.iter() {
            if state.is_idle(timeout) {
                to_remove.push(*stream_id);
            }
        }

        let count = to_remove.len();
        for stream_id in to_remove {
            if let Some(state) = streams_guard.remove(&stream_id) {
                by_block.write().await.remove(&state.block_id);
            }
            debug!(stream_id = %stream_id, "Stream evicted due to idle timeout");
        }

        if count > 0 {
            info!(evicted = count, "Cleaned up idle streams");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::ids::{BlockIndex, DataHandleId};

    #[tokio::test]
    async fn test_stream_manager_register_and_get() {
        let manager = StreamManager::with_default_timeout();
        let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));
        let stream_id = StreamId::new((123u128 << 64) | 456u128);

        let state = StreamState::new(
            stream_id,
            block_id,
            StreamMode::Read,
            Some(ByteRange { offset: 0, len: 1024 }),
            4096,
            65536,
            100,
            1024,
        );

        manager.register(state.clone()).await;
        let retrieved = manager.get(stream_id).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().stream_id, stream_id);
    }

    #[tokio::test]
    async fn test_stream_manager_remove() {
        let manager = StreamManager::with_default_timeout();
        let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));
        let stream_id = StreamId::new((123u128 << 64) | 456u128);

        let state = StreamState::new(stream_id, block_id, StreamMode::Read, None, 4096, 65536, 100, 1024);

        manager.register(state).await;
        assert_eq!(manager.active_count().await, 1);

        assert!(manager.remove(stream_id).await);
        assert_eq!(manager.active_count().await, 0);
    }
}
