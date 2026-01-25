// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Orphan detection and cleanup, plus reconcile (consistency check).

use crate::block_manager::BlockManager;
use crate::block_store::BlockStore;
use crate::volume_manager::VolumeManager;
use anyhow::{Context, Result};
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::time::interval;
use tracing::{debug, error, info};
use types::ids::{BlockId, BlockIndex, DataHandleId, ShardGroupId};

/// Orphan detection configuration.
#[derive(Clone, Debug)]
pub struct OrphanConfig {
    /// Grace period before deleting orphan files (seconds).
    pub grace_period_secs: u64,
    /// Scan interval (seconds).
    pub scan_interval_secs: u64,
}

impl Default for OrphanConfig {
    fn default() -> Self {
        Self {
            grace_period_secs: 3600, // 1 hour
            scan_interval_secs: 300, // 5 minutes
        }
    }
}

/// Orphan detection result (block-level).
#[derive(Clone, Debug)]
pub struct OrphanResult {
    /// Orphan blocks (index has no files).
    pub index_no_file: Vec<(ShardGroupId, BlockId)>,
    /// Orphan blocks (files have no index).
    pub file_no_index: Vec<(ShardGroupId, BlockId, PathBuf)>, // (group_id, block_id, block_dir)
    /// Total orphan block count.
    pub total: usize,
}

/// Orphan manager (block-level).
pub struct OrphanManager {
    /// Block manager (for block-level operations).
    block_manager: Arc<BlockManager>,
    /// Block store (for index access).
    block_store: Arc<BlockStore>,
    /// Volume manager.
    volume_manager: Arc<VolumeManager>,
    /// Configuration.
    config: OrphanConfig,
    /// Metrics.
    metrics: OrphanMetrics,
    /// Last scan time per group.
    last_scan: Arc<RwLock<std::collections::HashMap<ShardGroupId, SystemTime>>>,
}

/// Orphan metrics.
#[derive(Clone, Debug)]
pub struct OrphanMetrics {
    /// Total orphans found.
    orphan_found_total: Arc<RwLock<u64>>,
    /// Total orphans deleted.
    orphan_deleted_total: Arc<RwLock<u64>>,
    /// Reconcile runs total.
    reconcile_runs_total: Arc<RwLock<u64>>,
    /// Reconcile differences total.
    reconcile_diff_total: Arc<RwLock<u64>>,
}

impl OrphanMetrics {
    fn new() -> Self {
        Self {
            orphan_found_total: Arc::new(RwLock::new(0)),
            orphan_deleted_total: Arc::new(RwLock::new(0)),
            reconcile_runs_total: Arc::new(RwLock::new(0)),
            reconcile_diff_total: Arc::new(RwLock::new(0)),
        }
    }

    pub fn inc_orphan_found(&self, count: usize) {
        *self.orphan_found_total.write() += count as u64;
    }

    pub fn inc_orphan_deleted(&self, count: usize) {
        *self.orphan_deleted_total.write() += count as u64;
    }

    pub fn inc_reconcile_run(&self) {
        *self.reconcile_runs_total.write() += 1;
    }

    pub fn inc_reconcile_diff(&self, count: usize) {
        *self.reconcile_diff_total.write() += count as u64;
    }

    pub fn get_orphan_found_total(&self) -> u64 {
        *self.orphan_found_total.read()
    }

    pub fn get_orphan_deleted_total(&self) -> u64 {
        *self.orphan_deleted_total.read()
    }

    pub fn get_reconcile_runs_total(&self) -> u64 {
        *self.reconcile_runs_total.read()
    }

    pub fn get_reconcile_diff_total(&self) -> u64 {
        *self.reconcile_diff_total.read()
    }
}

impl OrphanManager {
    /// Create a new orphan manager.
    pub fn new(
        block_manager: Arc<BlockManager>,
        block_store: Arc<BlockStore>,
        volume_manager: Arc<VolumeManager>,
        config: OrphanConfig,
    ) -> Self {
        Self {
            block_manager,
            block_store,
            volume_manager,
            config,
            metrics: OrphanMetrics::new(),
            last_scan: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Detect orphan blocks for a group.
    pub async fn detect_orphans(&self, group_id: ShardGroupId) -> Result<OrphanResult> {
        debug!(group_id = group_id.as_raw(), "Detecting orphan blocks");

        // Get all blocks in index for this group
        let index_blocks: std::collections::HashSet<BlockId> = self
            .block_store
            .list_blocks(group_id)
            .into_iter()
            .map(|meta| meta.block_id)
            .collect();

        // Get all block directories on disk for this group
        let disk_blocks = self.scan_disk_blocks(group_id).await?;

        // Find orphans: index has no files (block in index but no directory/files)
        let mut index_no_file = Vec::new();
        for block_id in &index_blocks {
            if !disk_blocks.iter().any(|(bid, _)| bid == block_id) {
                index_no_file.push((group_id, *block_id));
            }
        }

        // Find orphans: files have no index (block directory exists but not in index)
        let mut file_no_index = Vec::new();
        for (block_id, block_dir) in disk_blocks {
            if !index_blocks.contains(&block_id) {
                file_no_index.push((group_id, block_id, block_dir));
            }
        }

        let total = index_no_file.len() + file_no_index.len();
        self.metrics.inc_orphan_found(total);

        info!(
            group_id = group_id.as_raw(),
            index_no_file = index_no_file.len(),
            file_no_index = file_no_index.len(),
            total = total,
            "Orphan block detection completed"
        );

        Ok(OrphanResult {
            index_no_file,
            file_no_index,
            total,
        })
    }

    /// Scan disk for block directories (new layout) and legacy chunk files (old layout).
    async fn scan_disk_blocks(&self, group_id: ShardGroupId) -> Result<Vec<(BlockId, PathBuf)>> {
        let volumes = self.volume_manager.volumes();
        let mut blocks = std::collections::HashMap::new();

        for volume in volumes {
            if volume.state != crate::volume_manager::VolumeState::Healthy {
                continue;
            }

            let group_dir = volume.path.join(group_id.as_raw().to_string());
            if !group_dir.exists() {
                continue;
            }

            let mut entries = fs::read_dir(&group_dir)
                .await
                .context("Failed to read group directory")?;

            while let Some(entry) = entries.next_entry().await.context("Failed to read directory entry")? {
                let path = entry.path();

                if path.is_dir() {
                    // New layout: <data_handle_id>_<block_index>/ directory
                    if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                        let parts: Vec<&str> = dir_name.split('_').collect();
                        if parts.len() == 2 {
                            if let (Ok(data_handle_id), Ok(block_index)) =
                                (parts[0].parse::<u64>(), parts[1].parse::<u32>())
                            {
                                let block_id =
                                    BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(block_index));
                                blocks.insert(block_id, path);
                            }
                        }
                    }
                } else if path.is_file() && path.extension().map(|e| e == "chunk").unwrap_or(false) {
                    // Legacy layout: <data_handle_id>_<block_index>_<chunk_idx>.chunk
                    if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                        let base = file_name.strip_suffix(".chunk").unwrap_or(file_name);
                        let parts: Vec<&str> = base.split('_').collect();
                        if parts.len() == 3 {
                            if let (Ok(data_handle_id), Ok(block_index), Ok(_chunk_idx)) = (
                                parts[0].parse::<u64>(),
                                parts[1].parse::<u32>(),
                                parts[2].parse::<u32>(),
                            ) {
                                let block_id =
                                    BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(block_index));
                                // Use parent directory as block directory for legacy layout
                                let block_dir = path.parent().unwrap().to_path_buf();
                                blocks.insert(block_id, block_dir);
                            }
                        }
                    }
                }
            }
        }

        Ok(blocks.into_iter().collect())
    }

    /// Clean up orphan blocks (with grace period).
    pub async fn cleanup_orphans(&self, group_id: ShardGroupId) -> Result<usize> {
        let result = self.detect_orphans(group_id).await?;
        let mut deleted_count = 0;

        // Delete blocks with no index (after grace period check)
        for (gid, block_id, block_dir) in result.file_no_index {
            if gid != group_id {
                continue;
            }

            // Check block directory modification time (grace period)
            let metadata = fs::metadata(&block_dir).await?;
            let modified = metadata.modified()?;
            let age = SystemTime::now()
                .duration_since(modified)
                .unwrap_or(Duration::from_secs(0));

            if age.as_secs() >= self.config.grace_period_secs {
                // Delete entire orphan block directory
                if let Err(e) = fs::remove_dir_all(&block_dir).await {
                    error!(error = %e, block_dir = %block_dir.display(), "Failed to delete orphan block directory");
                } else {
                    deleted_count += 1;
                    debug!(block_id = %block_id, block_dir = %block_dir.display(), "Deleted orphan block");
                }
            } else {
                debug!(
                    block_id = %block_id,
                    block_dir = %block_dir.display(),
                    age_secs = age.as_secs(),
                    grace_period_secs = self.config.grace_period_secs,
                    "Orphan block too new, skipping"
                );
            }
        }

        // Fix index entries with no files (remove from index)
        for (gid, block_id) in result.index_no_file {
            if gid == group_id {
                // Remove block from index (this will also remove all chunks)
                if let Err(e) = self.block_manager.delete_block(gid, block_id).await {
                    error!(error = %e, block_id = %block_id, "Failed to remove orphan block from index");
                } else {
                    debug!(block_id = %block_id, "Fixed: removed orphan block from index");
                }
            }
        }

        self.metrics.inc_orphan_deleted(deleted_count);

        info!(
            group_id = group_id.as_raw(),
            deleted = deleted_count,
            "Orphan block cleanup completed"
        );

        Ok(deleted_count)
    }

    /// Reconcile: consistency check and minimal repair (block-level).
    pub async fn reconcile(&self, group_id: ShardGroupId) -> Result<ReconcileResult> {
        self.metrics.inc_reconcile_run();

        debug!(group_id = group_id.as_raw(), "Starting block reconcile");

        let result = self.detect_orphans(group_id).await?;
        let mut fixed_count = 0;

        // Fix: remove index entries with no files
        for (_gid, block_id) in &result.index_no_file {
            // Remove block from index
            if let Err(e) = self.block_manager.delete_block(group_id, *block_id).await {
                error!(error = %e, block_id = %block_id, "Failed to remove orphan block from index in reconcile");
            } else {
                fixed_count += 1;
                debug!(block_id = %block_id, "Fixed: removed orphan block from index");
            }
        }

        // Fix: delete blocks with no index (immediate, no grace period for reconcile)
        for (gid, block_id, block_dir) in &result.file_no_index {
            if *gid != group_id {
                continue;
            }

            if let Err(e) = fs::remove_dir_all(block_dir).await {
                error!(error = %e, block_dir = %block_dir.display(), "Failed to delete orphan block directory in reconcile");
            } else {
                fixed_count += 1;
                debug!(block_id = %block_id, block_dir = %block_dir.display(), "Fixed: deleted orphan block");
            }
        }

        self.metrics.inc_reconcile_diff(result.total);

        info!(
            group_id = group_id.as_raw(),
            differences = result.total,
            fixed = fixed_count,
            "Block reconcile completed"
        );

        Ok(ReconcileResult {
            differences: result.total,
            fixed: fixed_count,
        })
    }

    /// Background orphan cleanup task.
    pub async fn run_background_task(&self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(self.config.scan_interval_secs));

        loop {
            interval.tick().await;

            // Get all known groups from volumes
            let groups = self.get_known_groups().await;

            for group_id in groups {
                if let Err(e) = self.cleanup_orphans(group_id).await {
                    error!(error = %e, group_id = group_id.as_raw(), "Orphan cleanup failed");
                }
            }
        }
    }

    /// Get all known groups from volumes.
    async fn get_known_groups(&self) -> Vec<ShardGroupId> {
        let volumes = self.volume_manager.volumes();
        let mut groups = std::collections::HashSet::new();

        for volume in volumes {
            if volume.state != crate::volume_manager::VolumeState::Healthy {
                continue;
            }

            if let Ok(mut entries) = fs::read_dir(&volume.path).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        if let Some(group_id_str) = entry_path.file_name().and_then(|n| n.to_str()) {
                            if let Ok(group_id_val) = group_id_str.parse::<u64>() {
                                groups.insert(ShardGroupId::new(group_id_val));
                            }
                        }
                    }
                }
            }
        }

        groups.into_iter().collect()
    }

    /// Get metrics.
    pub fn metrics(&self) -> &OrphanMetrics {
        &self.metrics
    }
}

/// Reconcile result.
#[derive(Clone, Debug)]
pub struct ReconcileResult {
    /// Number of differences found.
    pub differences: usize,
    /// Number of differences fixed.
    pub fixed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orphan_config_default() {
        let config = OrphanConfig::default();
        assert_eq!(config.grace_period_secs, 3600);
        assert_eq!(config.scan_interval_secs, 300);
    }
}
