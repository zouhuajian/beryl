// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! BlockStore: Local block/chunk storage with group_id isolation.
//!
//! Design principles:
//! - Block is the management unit: all operations (report, eviction, orphan, replicate) are block-scoped.
//! - Chunk is the IO unit: physical storage and UFS operations are chunk-scoped.
//! - Stream is NOT exposed in public API: only chunk/range readers that don't bind to transport types.

use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::marker::Unpin;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tracing::{debug, warn};

use crate::volume_manager::VolumeManager;
use types::block::{LocalBlockMeta, LocalBlockState};
use types::chunk::{ChunkBitmap, ChunkRef, ChunkSlice};
use types::ids::{BlockId, BlockIndex, ChunkId, ChunkIndex, DataHandleId, ShardGroupId};
use types::layout::FileLayout;

/// Chunk metadata (for fast chunk file lookup).
#[derive(Clone, Debug)]
struct ChunkMeta {
    /// Chunk file path.
    path: PathBuf,
    /// Chunk size in bytes.
    size: u64,
}

/// BlockStore: manages block storage with group_id isolation.
///
/// Key design:
/// - Block index: (group_id, block_id) -> LocalBlockMeta (authoritative)
/// - Chunk index: (group_id, chunk_id) -> ChunkMeta (for fast file lookup)
/// - All management operations (report, eviction, orphan, replicate) are block-scoped.
pub struct BlockStore {
    /// Volume manager.
    volume_manager: Arc<VolumeManager>,
    /// Block index: (group_id, block_id) -> LocalBlockMeta
    /// This is the authoritative index for block management.
    block_index: DashMap<(ShardGroupId, BlockId), LocalBlockMeta>,
    /// Chunk index: (group_id, chunk_id) -> ChunkMeta
    /// This is a fast lookup for chunk file paths (derived from block_index).
    chunk_index: DashMap<(ShardGroupId, ChunkId), ChunkMeta>,
    /// Manifest file path (for persistence and recovery).
    manifest_path: PathBuf,
    /// Chunk size (for validation and layout).
    chunk_size: u32,
    /// Block size (for validation and layout).
    block_size: u32,
    /// File layout (for range calculations).
    layout: FileLayout,
}

impl BlockStore {
    /// Create a new BlockStore.
    pub fn new(volume_manager: Arc<VolumeManager>, manifest_path: PathBuf, block_size: u32, chunk_size: u32) -> Self {
        let layout = FileLayout::new(block_size, chunk_size, 3); // replication=3 default
        Self {
            volume_manager,
            block_index: DashMap::new(),
            chunk_index: DashMap::new(),
            manifest_path,
            chunk_size,
            block_size,
            layout,
        }
    }

    /// Initialize BlockStore (load manifest, rebuild index).
    pub async fn init(&self) -> Result<()> {
        // Load manifest if exists
        if self.manifest_path.exists() {
            self.load_manifest().await?;
        } else {
            // Create manifest directory
            if let Some(parent) = self.manifest_path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .context("Failed to create manifest directory")?;
            }
        }

        // Rebuild index from disk (scan group_id directories)
        self.rebuild_index().await?;

        Ok(())
    }

    /// Load manifest from disk.
    async fn load_manifest(&self) -> Result<()> {
        // TODO(block_store): load manifest from JSON/protobuf file instead of logging only
        debug!(path = %self.manifest_path.display(), "Loading manifest");
        Ok(())
    }

    /// Rebuild index by scanning volume directories.
    async fn rebuild_index(&self) -> Result<()> {
        debug!("Rebuilding block and chunk index from disk");
        let volumes = self.volume_manager.volumes();

        for volume in volumes {
            if volume.state != crate::volume_manager::VolumeState::Healthy {
                continue;
            }

            // Scan <volume>/<group_id>/ directories
            let volume_path = &volume.path;
            if let Ok(mut entries) = fs::read_dir(volume_path).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        // Try to parse as group_id
                        if let Some(group_id_str) = entry_path.file_name().and_then(|n| n.to_str()) {
                            if let Ok(group_id_val) = group_id_str.parse::<u64>() {
                                let group_id = ShardGroupId::new(group_id_val);
                                self.scan_group_directory(&entry_path, group_id).await?;
                            }
                        }
                    }
                }
            }
        }

        debug!(
            blocks = self.block_index.len(),
            chunks = self.chunk_index.len(),
            "Index rebuilt"
        );
        Ok(())
    }

    /// Scan a group_id directory for chunks and build block index.
    async fn scan_group_directory(&self, group_dir: &Path, group_id: ShardGroupId) -> Result<()> {
        // First pass: collect all chunks
        let mut chunks_by_block: std::collections::HashMap<BlockId, Vec<(ChunkIndex, PathBuf, u64)>> =
            std::collections::HashMap::new();

        if let Ok(mut entries) = fs::read_dir(group_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let entry_path = entry.path();
                if entry_path.is_file() {
                    // Try to parse chunk file name: <data_handle_id>_<block_index>_<chunk_idx>.chunk
                    if let Some(file_name) = entry_path.file_name().and_then(|n| n.to_str()) {
                        if file_name.ends_with(".chunk") {
                            // Parse chunk ID from filename
                            // Format: <data_handle_id>_<block_index>_<chunk_idx>.chunk
                            let base = file_name.strip_suffix(".chunk").unwrap_or(file_name);
                            let parts: Vec<&str> = base.split('_').collect();
                            if parts.len() == 3 {
                                if let (Ok(data_handle_id), Ok(block_index), Ok(chunk_idx)) = (
                                    parts[0].parse::<u64>(),
                                    parts[1].parse::<u32>(),
                                    parts[2].parse::<u32>(),
                                ) {
                                    let block_id =
                                        BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(block_index));
                                    let chunk_id = ChunkId::new(block_id, ChunkIndex::new(chunk_idx));

                                    let metadata = fs::metadata(&entry_path).await?;
                                    let size = metadata.len();

                                    // Add to chunk index
                                    self.chunk_index.insert(
                                        (group_id, chunk_id),
                                        ChunkMeta {
                                            path: entry_path.clone(),
                                            size,
                                        },
                                    );

                                    // Group by block for block index
                                    chunks_by_block.entry(block_id).or_insert_with(Vec::new).push((
                                        ChunkIndex::new(chunk_idx),
                                        entry_path,
                                        size,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Second pass: build block index from chunks
        for (block_id, chunks) in chunks_by_block {
            let chunk_bitmap = ChunkBitmap::with_capacity_for(&self.layout);
            let mut block_meta = LocalBlockMeta::new(block_id, group_id, chunk_bitmap);

            let mut total_size = 0u64;
            for (chunk_idx, _path, size) in &chunks {
                let chunk_size_u32 = (*size).min(self.chunk_size as u64) as u32;
                block_meta.mark_chunk_committed(chunk_idx.as_raw(), chunk_size_u32);
                total_size += size;
            }

            block_meta.total_size = total_size;
            block_meta.state = LocalBlockState::Committed;

            self.block_index.insert((group_id, block_id), block_meta);
        }

        Ok(())
    }

    // ========== Block-level API (public, management unit) ==========

    /// Get block metadata.
    pub fn block_meta(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<Option<LocalBlockMeta>> {
        Ok(self.block_index.get(&(group_id, block_id)).map(|e| e.value().clone()))
    }

    /// Update layout_version for a block.
    pub fn update_layout_version(&self, group_id: ShardGroupId, block_id: BlockId, layout_version: u64) -> Result<()> {
        if let Some(mut meta) = self.block_index.get_mut(&(group_id, block_id)) {
            meta.layout_version = Some(layout_version);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Block not found: {}", block_id))
        }
    }

    /// Update block_stamp for a block.
    /// This is called when block content/route/commit changes.
    pub fn update_block_stamp(&self, group_id: ShardGroupId, block_id: BlockId, block_stamp: u64) -> Result<()> {
        if let Some(mut meta) = self.block_index.get_mut(&(group_id, block_id)) {
            meta.block_stamp = block_stamp;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Block not found: {}", block_id))
        }
    }

    /// Increment block_stamp for a block (atomic operation).
    /// Returns the new block_stamp value.
    pub fn increment_block_stamp(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<u64> {
        if let Some(mut meta) = self.block_index.get_mut(&(group_id, block_id)) {
            meta.block_stamp = meta.block_stamp.wrapping_add(1);
            if meta.block_stamp == 0 {
                meta.block_stamp = 1; // Avoid 0 (which means uninitialized)
            }
            Ok(meta.block_stamp)
        } else {
            Err(anyhow::anyhow!("Block not found: {}", block_id))
        }
    }

    /// List all blocks for a group_id (with full metadata).
    pub fn list_blocks(&self, group_id: ShardGroupId) -> Vec<LocalBlockMeta> {
        self.block_index
            .iter()
            .filter_map(|entry| {
                let (gid, _) = entry.key();
                if *gid == group_id {
                    Some(entry.value().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Mark block state.
    pub fn mark_block_state(&self, group_id: ShardGroupId, block_id: BlockId, state: LocalBlockState) -> Result<()> {
        if let Some(mut meta) = self.block_index.get_mut(&(group_id, block_id)) {
            meta.state = state;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Block not found: {}", block_id))
        }
    }

    /// Mark block as writing.
    pub fn mark_block_writing(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<()> {
        self.mark_block_state(group_id, block_id, LocalBlockState::Writing)
    }

    /// Mark block as committed.
    pub fn mark_block_committed(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<()> {
        self.mark_block_state(group_id, block_id, LocalBlockState::Committed)
    }

    /// Mark block as evictable.
    pub fn mark_block_evictable(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<()> {
        self.mark_block_state(group_id, block_id, LocalBlockState::Evictable)
    }

    /// Delete a block (removes all chunks and block metadata).
    pub async fn delete_block(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<()> {
        // Get all chunks for this block
        let chunks: Vec<ChunkId> = self
            .chunk_index
            .iter()
            .filter_map(|entry| {
                let (gid, chunk_id) = entry.key();
                if *gid == group_id && chunk_id.block == block_id {
                    Some(*chunk_id)
                } else {
                    None
                }
            })
            .collect();

        // Delete all chunk files
        for chunk_id in &chunks {
            if let Err(e) = self.delete_chunk_internal(group_id, *chunk_id).await {
                warn!(error = %e, chunk_id = %chunk_id, "Failed to delete chunk during block deletion");
            }
        }

        // Remove from block index
        self.block_index.remove(&(group_id, block_id));

        debug!(
            group_id = group_id.as_raw(),
            block_id = %block_id,
            chunks_deleted = chunks.len(),
            "Block deleted"
        );

        Ok(())
    }

    /// Commit a chunk to a block (internal chunk IO operation).
    /// This updates the block's chunk bitmap and committed_length.
    pub async fn commit_chunk(
        &self,
        group_id: ShardGroupId,
        block_id: BlockId,
        chunk_idx: ChunkIndex,
        mut data_source: impl AsyncRead + Unpin + Send,
    ) -> Result<()> {
        let chunk_id = ChunkId::new(block_id, chunk_idx);
        let final_path = self.chunk_path(group_id, chunk_id)?;

        // Ensure block directory exists (new layout)
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create block directory")?;
        }

        // Write to temporary file first
        let temp_path = final_path.with_extension("tmp");
        let mut temp_file = fs::File::create(&temp_path)
            .await
            .context("Failed to create temporary chunk file")?;

        // Stream data from reader to file
        let mut buffer = vec![0u8; 64 * 1024]; // 64KB buffer
        let mut total_bytes = 0u64;

        loop {
            let n = data_source
                .read(&mut buffer)
                .await
                .context("Failed to read from data source")?;
            if n == 0 {
                break;
            }
            temp_file
                .write_all(&buffer[..n])
                .await
                .context("Failed to write to temporary file")?;
            total_bytes += n as u64;
        }

        // Sync to disk
        temp_file.sync_all().await.context("Failed to sync temporary file")?;
        drop(temp_file);

        // Atomic rename
        fs::rename(&temp_path, &final_path)
            .await
            .context("Failed to rename temporary file to final path")?;

        // Update chunk index
        self.chunk_index.insert(
            (group_id, chunk_id),
            ChunkMeta {
                path: final_path.clone(),
                size: total_bytes,
            },
        );

        // Update block metadata (chunk bitmap, committed_length, and block_stamp)
        let chunk_size = total_bytes.min(self.chunk_size as u64) as u32;
        if let Some(mut block_meta) = self.block_index.get_mut(&(group_id, block_id)) {
            block_meta.mark_chunk_committed(chunk_idx.as_raw(), chunk_size);
            block_meta.touch(); // Update last access time
                                // Increment block_stamp on content change
            if block_meta.block_stamp == 0 {
                block_meta.block_stamp = 1; // Initialize if unset
            } else {
                block_meta.block_stamp = block_meta.block_stamp.wrapping_add(1);
                if block_meta.block_stamp == 0 {
                    block_meta.block_stamp = 1; // Avoid 0
                }
            }
        } else {
            // Block doesn't exist yet, create it
            let chunk_bitmap = ChunkBitmap::with_capacity_for(&self.layout);
            let mut block_meta = LocalBlockMeta::new(block_id, group_id, chunk_bitmap);
            block_meta.mark_chunk_committed(chunk_idx.as_raw(), chunk_size);
            block_meta.touch();
            block_meta.block_stamp = 1; // Initialize block_stamp
            self.block_index.insert((group_id, block_id), block_meta);
        }

        debug!(
            group_id = group_id.as_raw(),
            block_id = %block_id,
            chunk_idx = chunk_idx.as_raw(),
            bytes = total_bytes,
            "Chunk committed"
        );

        Ok(())
    }

    /// Read a chunk (internal chunk IO operation).
    pub async fn read_chunk(
        &self,
        group_id: ShardGroupId,
        block_id: BlockId,
        chunk_idx: ChunkIndex,
    ) -> Result<Option<Bytes>> {
        let chunk_id = ChunkId::new(block_id, chunk_idx);
        let key = (group_id, chunk_id);

        let meta = match self.chunk_index.get(&key) {
            Some(meta) => meta.clone(),
            None => return Ok(None),
        };

        // Open file and read
        let mut file = fs::File::open(&meta.path).await.context("Failed to open chunk file")?;

        let mut buffer = Vec::with_capacity(meta.size as usize);
        file.read_to_end(&mut buffer)
            .await
            .context("Failed to read chunk file")?;

        // Update block last access time
        if let Some(mut block_meta) = self.block_index.get_mut(&(group_id, block_id)) {
            block_meta.touch();
        }

        Ok(Some(Bytes::from(buffer)))
    }

    /// Read a range within a block.
    /// Returns a reader that can be used to stream the data.
    pub async fn read_range(
        &self,
        group_id: ShardGroupId,
        block_id: BlockId,
        offset: u32, // offset within block
        len: u32,
    ) -> Result<Option<impl AsyncRead + Unpin + Send>> {
        // Use layout to convert range to chunks
        let chunk_ranges = self.layout.range_to_chunks(block_id, offset, len);

        // For now, read all chunks and return a cursor
        // TODO: Implement a proper streaming reader that reads chunks on-demand
        let mut data = Vec::new();
        for (chunk_idx, offset_in_chunk, chunk_len) in chunk_ranges {
            if let Some(chunk_data) = self.read_chunk(group_id, block_id, chunk_idx).await? {
                let start = offset_in_chunk as usize;
                let end = (start + chunk_len as usize).min(chunk_data.len());
                data.extend_from_slice(&chunk_data[start..end]);
            } else {
                return Ok(None);
            }
        }

        Ok(Some(std::io::Cursor::new(data)))
    }

    // ========== Legacy chunk-level API (kept for backward compatibility, internal use) ==========

    /// Check if a chunk exists.
    pub fn has_chunk(&self, group_id: ShardGroupId, block_id: BlockId, chunk_idx: ChunkIndex) -> bool {
        let chunk_id = ChunkId::new(block_id, chunk_idx);
        self.chunk_index.contains_key(&(group_id, chunk_id))
    }

    /// Get block directory path.
    /// Format: <volume>/<group_id>/<data_handle_id>_<block_index>/
    fn block_dir(&self, group_id: ShardGroupId, block_id: BlockId) -> Result<PathBuf> {
        let volume = self
            .volume_manager
            .select_volume(group_id)
            .ok_or_else(|| anyhow::anyhow!("No volume available for group_id"))?;

        let block_dir_name = format!("{}_{}", block_id.data_handle_id.as_raw(), block_id.index.as_raw());
        Ok(volume.path.join(group_id.as_raw().to_string()).join(block_dir_name))
    }

    /// Get chunk file path for a group_id and chunk.
    /// Format: <volume>/<group_id>/<data_handle_id>_<block_index>/<chunk_idx>.chunk
    /// (new layout) or <volume>/<group_id>/<data_handle_id>_<block_index>_<chunk_idx>.chunk (legacy)
    fn chunk_path(&self, group_id: ShardGroupId, chunk_id: ChunkId) -> Result<PathBuf> {
        let block_dir = self.block_dir(group_id, chunk_id.block)?;

        // Try new layout first: <block_dir>/<chunk_idx>.chunk
        let new_path = block_dir.join(format!("{}.chunk", chunk_id.index.as_raw()));
        if new_path.exists() {
            return Ok(new_path);
        }

        // Fallback to legacy layout: <group_dir>/<data_handle_id>_<block_index>_<chunk_idx>.chunk
        let volume = self
            .volume_manager
            .select_volume(group_id)
            .ok_or_else(|| anyhow::anyhow!("No volume available for group_id"))?;
        let file_name = format!(
            "{}_{}_{}.chunk",
            chunk_id.block.data_handle_id.as_raw(),
            chunk_id.block.index.as_raw(),
            chunk_id.index.as_raw()
        );
        Ok(volume.path.join(group_id.as_raw().to_string()).join(file_name))
    }

    /// Write a chunk stream (legacy API, uses commit_chunk internally).
    /// This is kept for backward compatibility but should migrate to block-level API.
    pub async fn write_chunk_stream(
        &self,
        group_id: ShardGroupId,
        chunk_ref: ChunkRef,
        stream: impl tokio::io::AsyncRead + Unpin + Send,
    ) -> Result<()> {
        self.commit_chunk(
            group_id,
            chunk_ref.block_id,
            ChunkIndex::new(chunk_ref.chunk_idx),
            stream,
        )
        .await
    }

    /// Read a chunk slice (legacy API, uses read_chunk internally).
    /// This is kept for backward compatibility but should migrate to block-level API.
    pub async fn read_chunk_stream(&self, group_id: ShardGroupId, chunk_slice: ChunkSlice) -> Result<Option<Bytes>> {
        let chunk_data = self
            .read_chunk(
                group_id,
                chunk_slice.chunk.block_id,
                ChunkIndex::new(chunk_slice.chunk.chunk_idx),
            )
            .await?;

        match chunk_data {
            Some(data) => {
                let start = chunk_slice.offset_in_chunk as usize;
                let end = (start + chunk_slice.len as usize).min(data.len());
                Ok(Some(data.slice(start..end)))
            }
            None => Ok(None),
        }
    }

    /// List all chunks for a group_id (legacy API).
    pub fn list_chunks(&self, group_id: ShardGroupId) -> Vec<ChunkId> {
        self.chunk_index
            .iter()
            .filter_map(|entry| {
                let (gid, chunk_id) = entry.key();
                if *gid == group_id {
                    Some(*chunk_id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Internal method to delete a chunk (used by delete_block).
    async fn delete_chunk_internal(&self, group_id: ShardGroupId, chunk_id: ChunkId) -> Result<()> {
        let key = (group_id, chunk_id);

        // Get chunk path from index
        let chunk_path = match self.chunk_index.get(&key) {
            Some(meta) => meta.path.clone(),
            None => {
                // Chunk not in index, try to construct path
                self.chunk_path(group_id, chunk_id)?
            }
        };

        // Delete file (ignore error if file doesn't exist)
        if chunk_path.exists() {
            fs::remove_file(&chunk_path)
                .await
                .context("Failed to delete chunk file")?;
        }

        // Remove from chunk index
        self.chunk_index.remove(&key);

        Ok(())
    }

    /// Delete a chunk (legacy API, removes from index and deletes file).
    /// Note: This does NOT update block metadata. Use delete_block for block-level operations.
    pub async fn delete_chunk(&self, group_id: ShardGroupId, chunk_id: ChunkId) -> Result<()> {
        self.delete_chunk_internal(group_id, chunk_id).await?;

        debug!(
            group_id = group_id.as_raw(),
            chunk_id = %chunk_id,
            "Chunk deleted"
        );

        Ok(())
    }

    /// Remove chunk from index only (without deleting file).
    /// Used for fixing orphan index entries.
    pub fn remove_chunk_from_index(&self, group_id: ShardGroupId, chunk_id: ChunkId) {
        self.chunk_index.remove(&(group_id, chunk_id));
        debug!(
            group_id = group_id.as_raw(),
            chunk_id = %chunk_id,
            "Chunk removed from index"
        );
    }
}
