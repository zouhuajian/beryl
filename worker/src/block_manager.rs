// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! BlockManager: Unified block management (replication, lifecycle, coordination).
//!
//! This module handles:
//! - Block replication to remote workers (from metadata requests)
//! - Block lifecycle management (state transitions, coordination)
//! - Block-level operations that coordinate between BlockStore and other components
//!
//! Design principle: BlockStore focuses on storage metadata, BlockManager handles management logic.

use anyhow::{Context, Result};
use bytes::Bytes;
use std::sync::Arc;
use tracing::{error, info};

use crate::block_store::BlockStore;
use types::block::{LocalBlockMeta, LocalBlockState};
use types::ids::{BlockId, ChunkIndex, ShardGroupId, WorkerId};
use types::layout::FileLayout;

/// BlockManager: Unified block management.
pub struct BlockManager {
    /// Block store (for storage metadata).
    block_store: Arc<BlockStore>,
    /// File layout (for chunk calculations).
    layout: FileLayout,
}

impl BlockManager {
    /// Create a new BlockManager.
    pub fn new(block_store: Arc<BlockStore>, layout: FileLayout) -> Self {
        Self { block_store, layout }
    }

    /// Get block metadata.
    pub fn block_meta(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<Option<LocalBlockMeta>> {
        self.block_store.block_meta(group_id, block_id)
    }

    /// List all blocks for a group.
    pub fn list_blocks(&self, group_id: ShardGroupId) -> Vec<LocalBlockMeta> {
        self.block_store.list_blocks(group_id)
    }

    /// Mark block state.
    pub fn mark_block_state(&self, group_id: ShardGroupId, block_id: BlockId, state: LocalBlockState) -> Result<()> {
        self.block_store.mark_block_state(group_id, block_id, state)
    }

    /// Delete a block (removes all chunks and metadata).
    pub async fn delete_block(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<()> {
        self.block_store.delete_block(group_id, block_id).await
    }

    /// Replicate a block to remote workers.
    ///
    /// This reads all chunks of the block locally and sends them to target workers.
    /// The replication is done chunk-by-chunk for streaming and backpressure control.
    ///
    /// # Arguments
    /// * `group_id` - Shard group ID
    /// * `block_id` - Block to replicate
    /// * `target_workers` - List of target worker IDs to replicate to
    /// * `replication_client` - Client for sending chunks to remote workers
    ///
    /// # Returns
    /// Number of successful replications
    pub async fn replicate_block(
        &self,
        group_id: ShardGroupId,
        block_id: BlockId,
        target_workers: Vec<WorkerId>,
        replication_client: Arc<dyn ReplicationClient + Send + Sync>,
    ) -> Result<usize> {
        info!(
            group_id = group_id.as_raw(),
            block_id = %block_id,
            targets = target_workers.len(),
            "Starting block replication"
        );

        // Get block metadata
        let block_meta = match self.block_store.block_meta(group_id, block_id)? {
            Some(meta) => meta,
            None => {
                return Err(anyhow::anyhow!("Block not found: {}", block_id));
            }
        };

        // Check if block is complete
        let expected_chunks = self.layout.chunks_per_block();
        if !block_meta.is_complete(expected_chunks) {
            return Err(anyhow::anyhow!(
                "Block {} is not complete (has {}/{} chunks)",
                block_id,
                block_meta.chunk_bitmap.bits.iter().map(|b| b.count_ones()).sum::<u32>(),
                expected_chunks
            ));
        }

        // Read all chunks
        let mut chunks = Vec::new();
        for chunk_idx in 0..expected_chunks {
            let chunk_index = ChunkIndex::new(chunk_idx);
            match self.block_store.read_chunk(group_id, block_id, chunk_index).await? {
                Some(data) => chunks.push((chunk_index, data)),
                None => {
                    return Err(anyhow::anyhow!("Chunk {} not found in block {}", chunk_idx, block_id));
                }
            }
        }

        // Replicate to each target worker
        let mut success_count = 0;
        for target_worker in target_workers {
            match self
                .replicate_block_to_worker(group_id, block_id, &chunks, target_worker, replication_client.clone())
                .await
            {
                Ok(()) => {
                    success_count += 1;
                    info!(
                        group_id = group_id.as_raw(),
                        block_id = %block_id,
                        target_worker = target_worker.as_raw(),
                        "Block replicated successfully"
                    );
                }
                Err(e) => {
                    error!(
                        group_id = group_id.as_raw(),
                        block_id = %block_id,
                        target_worker = target_worker.as_raw(),
                        error = %e,
                        "Failed to replicate block"
                    );
                }
            }
        }

        Ok(success_count)
    }

    /// Replicate a block to a specific worker.
    ///
    /// This sends chunks concurrently with backpressure control.
    /// The concurrency is controlled by the replication client configuration.
    async fn replicate_block_to_worker(
        &self,
        group_id: ShardGroupId,
        block_id: BlockId,
        chunks: &[(ChunkIndex, Bytes)],
        target_worker: WorkerId,
        replication_client: Arc<dyn ReplicationClient + Send + Sync>,
    ) -> Result<()> {
        use futures::stream::{self, StreamExt};

        // Concurrency limit for chunk replication
        // Note: This should ideally come from ReplicationConfig, but for now we use a reasonable default
        // The actual backpressure is handled by the transport layer's request_limiter
        let concurrency_limit = 4; // Default, can be made configurable later

        // Create a stream of chunk send futures
        // Clone Arc before moving into closure to avoid lifetime issues
        let replication_client_for_map = Arc::clone(&replication_client);
        let target_worker_val = target_worker;
        let group_id_val = group_id;
        let block_id_val = block_id;

        let chunk_stream = stream::iter(chunks.iter().cloned()).map(move |(chunk_idx, chunk_data)| {
            let client = Arc::clone(&replication_client_for_map);
            let worker = target_worker_val;
            let gid = group_id_val;
            let bid = block_id_val;
            let cidx = chunk_idx;
            let data = chunk_data;

            async move { client.send_chunk(worker, gid, bid, cidx, data).await }
        });

        // Collect results with concurrency control
        // buffer_unordered limits the number of concurrent futures
        let results: Vec<Result<()>> = chunk_stream.buffer_unordered(concurrency_limit).collect().await;

        // Check for failures
        for (idx, result) in results.into_iter().enumerate() {
            result.context(format!(
                "Failed to replicate chunk {} of block {} to worker {}",
                idx,
                block_id,
                target_worker.as_raw()
            ))?;
        }

        Ok(())
    }

    /// Check if a block is complete (all expected chunks are present).
    pub fn is_block_complete(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<bool> {
        let block_meta = match self.block_store.block_meta(group_id, block_id)? {
            Some(meta) => meta,
            None => return Ok(false),
        };

        let expected_chunks = self.layout.chunks_per_block();
        Ok(block_meta.is_complete(expected_chunks))
    }

    /// Get block statistics (for monitoring/reporting).
    pub fn block_stats(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<Option<BlockStats>> {
        let block_meta = match self.block_store.block_meta(group_id, block_id)? {
            Some(meta) => meta,
            None => return Ok(None),
        };

        let chunk_count = block_meta.chunk_bitmap.bits.iter().map(|b| b.count_ones()).sum::<u32>();

        Ok(Some(BlockStats {
            block_id,
            state: block_meta.state,
            chunk_count,
            committed_length: block_meta.committed_length,
            total_size: block_meta.total_size,
            last_access: block_meta.last_access,
        }))
    }
}

/// Statistics for a block.
#[derive(Clone, Debug)]
pub struct BlockStats {
    pub block_id: BlockId,
    pub state: LocalBlockState,
    pub chunk_count: u32,
    pub committed_length: u64,
    pub total_size: u64,
    pub last_access: Option<u64>,
}

/// Trait for replication client (abstracts the actual transport).
///
/// This allows BlockManager to replicate blocks without depending on specific transport types.
pub trait ReplicationClient {
    /// Send a chunk to a remote worker.
    ///
    /// # Arguments
    /// * `target_worker` - Target worker ID
    /// * `group_id` - Shard group ID
    /// * `block_id` - Block ID
    /// * `chunk_idx` - Chunk index
    /// * `data` - Chunk data
    fn send_chunk(
        &self,
        target_worker: WorkerId,
        group_id: ShardGroupId,
        block_id: BlockId,
        chunk_idx: ChunkIndex,
        data: Bytes,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>>;
}

// Default implementation for testing (no-op)
#[cfg(test)]
impl ReplicationClient for () {
    fn send_chunk(
        &self,
        _target_worker: WorkerId,
        _group_id: ShardGroupId,
        _block_id: BlockId,
        _chunk_idx: ChunkIndex,
        _data: Bytes,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}
