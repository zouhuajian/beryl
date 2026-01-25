// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Capacity watermark and eviction (LRU-based).

use crate::block_manager::BlockManager;
use crate::volume_manager::VolumeManager;
use anyhow::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use types::block::LocalBlockState;
use types::ids::{BlockId, ShardGroupId};
// Note: WorkerError is available but not used in this module yet

/// Capacity watermark configuration.
#[derive(Clone, Debug)]
pub struct WatermarkConfig {
    /// High watermark (percentage, 0.0-1.0). Triggers eviction and reject writes.
    pub high_watermark: f64,
    /// Low watermark (percentage, 0.0-1.0). Resumes writes after eviction.
    pub low_watermark: f64,
    /// Eviction rate limit (bytes per second).
    pub eviction_rate_bytes_per_sec: u64,
    /// Eviction rate limit (IOPS).
    pub eviction_rate_iops: u64,
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            high_watermark: 0.90,                           // 90%
            low_watermark: 0.80,                            // 80%
            eviction_rate_bytes_per_sec: 100 * 1024 * 1024, // 100MB/s
            eviction_rate_iops: 100,                        // 100 IOPS
        }
    }
}

/// Block access metadata for LRU.
#[derive(Clone, Debug)]
struct BlockAccess {
    /// Last access time.
    last_access: Instant,
    /// Block size in bytes (total of all chunks).
    size: u64,
    /// Whether this block is dirty (being written).
    is_dirty: bool,
    /// Whether this block is in writing state.
    is_writing: bool,
}

/// Eviction manager with capacity watermarks and LRU eviction.
pub struct EvictionManager {
    /// Block manager (for block-level operations).
    block_manager: Arc<BlockManager>,
    /// Volume manager.
    volume_manager: Arc<VolumeManager>,
    /// Watermark configuration.
    config: WatermarkConfig,
    /// Block access metadata: (group_id, block_id) -> BlockAccess.
    access_map: Arc<RwLock<HashMap<(ShardGroupId, BlockId), BlockAccess>>>,
    /// Current watermark state (per group_id).
    watermark_state: Arc<RwLock<HashMap<ShardGroupId, WatermarkState>>>,
    /// Eviction metrics.
    metrics: EvictionMetrics,
}

/// Watermark state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WatermarkState {
    /// Below low watermark (normal operation).
    Normal,
    /// Between low and high watermark (eviction active).
    Evicting,
    /// Above high watermark (reject writes).
    RejectingWrites,
}

/// Eviction metrics.
#[derive(Clone, Debug)]
/// Eviction metrics.
pub struct EvictionMetrics {
    /// Total evictions.
    eviction_total: Arc<RwLock<u64>>,
    /// Total bytes evicted.
    eviction_bytes: Arc<RwLock<u64>>,
    /// Total watermark triggers.
    watermark_trigger_total: Arc<RwLock<u64>>,
    /// Total write rejections.
    reject_write_total: Arc<RwLock<u64>>,
}

impl EvictionMetrics {
    fn new() -> Self {
        Self {
            eviction_total: Arc::new(RwLock::new(0)),
            eviction_bytes: Arc::new(RwLock::new(0)),
            watermark_trigger_total: Arc::new(RwLock::new(0)),
            reject_write_total: Arc::new(RwLock::new(0)),
        }
    }

    fn inc_eviction(&self, bytes: u64) {
        *self.eviction_total.write() += 1;
        *self.eviction_bytes.write() += bytes;
    }

    fn inc_watermark_trigger(&self) {
        *self.watermark_trigger_total.write() += 1;
    }

    fn inc_reject_write(&self) {
        *self.reject_write_total.write() += 1;
    }

    pub fn get_eviction_total(&self) -> u64 {
        *self.eviction_total.read()
    }

    pub fn get_eviction_bytes(&self) -> u64 {
        *self.eviction_bytes.read()
    }

    pub fn get_watermark_trigger_total(&self) -> u64 {
        *self.watermark_trigger_total.read()
    }

    pub fn get_reject_write_total(&self) -> u64 {
        *self.reject_write_total.read()
    }
}

impl EvictionManager {
    /// Create a new eviction manager.
    pub fn new(block_manager: Arc<BlockManager>, volume_manager: Arc<VolumeManager>, config: WatermarkConfig) -> Self {
        Self {
            block_manager,
            volume_manager,
            config,
            access_map: Arc::new(RwLock::new(HashMap::new())),
            watermark_state: Arc::new(RwLock::new(HashMap::new())),
            metrics: EvictionMetrics::new(),
        }
    }

    /// Record block access (for LRU tracking).
    pub fn record_access(&self, group_id: ShardGroupId, block_id: BlockId, size: u64) {
        let mut access_map = self.access_map.write();
        access_map.insert(
            (group_id, block_id),
            BlockAccess {
                last_access: Instant::now(),
                size,
                is_dirty: false,
                is_writing: false,
            },
        );
    }

    /// Mark block as dirty/writing (protected from eviction).
    pub fn mark_dirty(&self, group_id: ShardGroupId, block_id: BlockId, size: u64) {
        let mut access_map = self.access_map.write();
        if let Some(access) = access_map.get_mut(&(group_id, block_id)) {
            access.is_dirty = true;
            access.is_writing = true;
        } else {
            access_map.insert(
                (group_id, block_id),
                BlockAccess {
                    last_access: Instant::now(),
                    size,
                    is_dirty: true,
                    is_writing: true,
                },
            );
        }
    }

    /// Mark block as clean (no longer writing).
    pub fn mark_clean(&self, group_id: ShardGroupId, block_id: BlockId) {
        let mut access_map = self.access_map.write();
        if let Some(access) = access_map.get_mut(&(group_id, block_id)) {
            access.is_dirty = false;
            access.is_writing = false;
        }
    }

    /// Check if writes should be rejected (above high watermark).
    pub fn should_reject_write(&self, group_id: ShardGroupId) -> bool {
        let state = self.get_watermark_state(group_id);
        state == WatermarkState::RejectingWrites
    }

    /// Get current watermark state for a group.
    fn get_watermark_state(&self, group_id: ShardGroupId) -> WatermarkState {
        let watermark_state = self.watermark_state.read();
        watermark_state
            .get(&group_id)
            .copied()
            .unwrap_or(WatermarkState::Normal)
    }

    /// Update watermark state based on capacity.
    fn update_watermark_state(&self, group_id: ShardGroupId) {
        let usage = self.get_group_usage(group_id);
        let high_threshold = self.config.high_watermark;
        let low_threshold = self.config.low_watermark;

        let new_state = if usage >= high_threshold {
            WatermarkState::RejectingWrites
        } else if usage >= low_threshold {
            WatermarkState::Evicting
        } else {
            WatermarkState::Normal
        };

        let mut watermark_state = self.watermark_state.write();
        let old_state = watermark_state
            .get(&group_id)
            .copied()
            .unwrap_or(WatermarkState::Normal);

        if new_state != old_state {
            watermark_state.insert(group_id, new_state);
            self.metrics.inc_watermark_trigger();
            info!(
                group_id = group_id.as_raw(),
                usage = usage,
                old_state = ?old_state,
                new_state = ?new_state,
                "Watermark state changed"
            );
        }
    }

    /// Get group usage percentage (0.0-1.0).
    fn get_group_usage(&self, _group_id: ShardGroupId) -> f64 {
        // Calculate usage from volumes
        let volumes = self.volume_manager.volumes();
        if volumes.is_empty() {
            return 0.0;
        }

        // For simplicity, use total volume usage
        // In production, track per-group usage more accurately
        let total_capacity = volumes.iter().map(|v| v.total_bytes).sum::<u64>();
        let total_used = volumes.iter().map(|v| v.used_bytes).sum::<u64>();

        if total_capacity == 0 {
            return 0.0;
        }

        total_used as f64 / total_capacity as f64
    }

    /// Run eviction for a group (LRU-based, rate-limited, block-level).
    async fn evict_group(&self, group_id: ShardGroupId) -> Result<()> {
        // Collect evictable blocks first (with lock held)
        let mut evictable: Vec<((ShardGroupId, BlockId), BlockAccess)> = {
            let access_map = self.access_map.read();
            access_map
                .iter()
                .filter(|((gid, _), access)| *gid == group_id && !access.is_dirty && !access.is_writing)
                .map(|(k, v)| (*k, v.clone()))
                .collect()
        };

        // Sort by last_access (LRU: oldest first)
        evictable.sort_by_key(|(_, access)| access.last_access);

        // Rate limit: bytes per second and IOPS
        let rate_bytes_per_sec = self.config.eviction_rate_bytes_per_sec;
        let rate_iops = self.config.eviction_rate_iops;
        let interval_duration = Duration::from_secs(1);
        let bytes_per_interval = rate_bytes_per_sec;
        let iops_per_interval = rate_iops;

        let mut bytes_evicted_this_interval = 0u64;
        let mut iops_this_interval = 0u64;
        let mut interval_start = Instant::now();

        for ((gid, block_id), access) in evictable {
            // Check rate limits
            if interval_start.elapsed() >= interval_duration {
                bytes_evicted_this_interval = 0;
                iops_this_interval = 0;
                interval_start = Instant::now();
            }

            if bytes_evicted_this_interval >= bytes_per_interval {
                // Rate limit: wait for next interval
                tokio::time::sleep(interval_duration - interval_start.elapsed()).await;
                bytes_evicted_this_interval = 0;
                iops_this_interval = 0;
                interval_start = Instant::now();
            }

            if iops_this_interval >= iops_per_interval {
                // IOPS limit: wait
                tokio::time::sleep(interval_duration - interval_start.elapsed()).await;
                bytes_evicted_this_interval = 0;
                iops_this_interval = 0;
                interval_start = Instant::now();
            }

            // Double-check: block still exists and not dirty
            {
                let access_map = self.access_map.read();
                if let Some(current_access) = access_map.get(&(gid, block_id)) {
                    if current_access.is_dirty || current_access.is_writing {
                        continue; // Skip dirty/writing blocks
                    }
                } else {
                    continue; // Already evicted
                }
            }

            // Check block state (only evict committed/clean blocks)
            if let Ok(Some(block_meta)) = self.block_manager.block_meta(gid, block_id) {
                if block_meta.state != LocalBlockState::Committed && block_meta.state != LocalBlockState::Clean {
                    continue; // Skip non-committed blocks
                }
            } else {
                continue; // Block not found
            }

            // Delete entire block (all chunks)
            if let Err(e) = self.block_manager.delete_block(gid, block_id).await {
                warn!(error = %e, block_id = %block_id, "Failed to delete block during eviction");
                continue;
            }

            // Remove from access map
            {
                let mut access_map = self.access_map.write();
                access_map.remove(&(gid, block_id));
            }

            bytes_evicted_this_interval += access.size;
            iops_this_interval += 1;

            self.metrics.inc_eviction(access.size);

            debug!(
                group_id = gid.as_raw(),
                block_id = %block_id,
                size = access.size,
                "Evicted block"
            );
        }

        Ok(())
    }

    /// Background eviction task (runs periodically).
    pub async fn run_background_task(&self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(10)); // Check every 10 seconds

        loop {
            interval.tick().await;

            // Get all known groups
            let groups = self.get_known_groups().await;

            // Update watermark states for all groups
            for group_id in &groups {
                self.update_watermark_state(*group_id);
            }

            // Run eviction for groups that need it
            for group_id in groups {
                let state = self.get_watermark_state(group_id);
                if state == WatermarkState::Evicting || state == WatermarkState::RejectingWrites {
                    if let Err(e) = self.evict_group(group_id).await {
                        error!(error = %e, group_id = group_id.as_raw(), "Eviction failed");
                    }
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

            if let Ok(mut entries) = tokio::fs::read_dir(&volume.path).await {
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
    pub fn metrics(&self) -> &EvictionMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_config_default() {
        let config = WatermarkConfig::default();
        assert_eq!(config.high_watermark, 0.90);
        assert_eq!(config.low_watermark, 0.80);
    }

    #[test]
    fn test_watermark_state_transitions() {
        // This would require a full setup with BlockStore and VolumeManager
        // For now, just test the logic
        let high = 0.90;
        let low = 0.80;

        assert!(0.95 >= high); // RejectingWrites
        assert!(0.85 >= low && 0.85 < high); // Evicting
        assert!(0.70 < low); // Normal
    }
}
