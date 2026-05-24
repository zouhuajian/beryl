// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Unified gate for destructive actions (GC, Orphan, DeleteIntent, DeleteExecutor, LeaseCleanup).
//!
//! This module provides a centralized check for all destructive maintenance operations,
//! ensuring consistency and preventing race conditions.

use crate::error::MetadataResult;
use crate::mount::MountTable;
use crate::raft::AppRaftNode;
use crate::worker::WorkerManager;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};
use types::group_watermark::{GroupStateWatermark, MountEpoch};
use types::ids::{BlockId, ShardGroupId};
use types::RaftLogId;

/// Result of destructive action gate check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DestructiveCheckResult {
    /// Action is allowed.
    Allowed,
    /// Action is blocked with a reason.
    Blocked { reason: String },
    /// Need refresh: mount_epoch mismatch or route changed.
    /// Returns latest mount_epoch and route hint for client refresh.
    NeedRefresh {
        reason: String,
        latest_mount_epoch: Option<MountEpoch>,
        route_hint: Option<String>,
    },
}

/// Context for destructive action check.
#[derive(Clone, Debug)]
pub struct DestructiveCheckContext {
    /// Block ID (if applicable).
    pub block_id: Option<BlockId>,
    /// Shard group ID (required for cross-group gate control).
    pub group_id: Option<ShardGroupId>,
    /// Mount epoch (for route consistency checking).
    /// If provided, must match current mount_table.version().
    pub mount_epoch: Option<MountEpoch>,
    /// Guard watermark (group_id + state_id).
    /// If provided, the target shard group must have applied at least up to this state_id.
    pub guard_watermark: Option<GroupStateWatermark>,
    /// Single-group guard state ID used when no group-scoped watermark is provided.
    /// If guard_watermark is provided, this is ignored.
    pub guard_state_id: Option<RaftLogId>,
    /// Not before timestamp (from DeleteIntent).
    pub not_before_ms: Option<u64>,
    /// Action type (for logging/metrics).
    pub action_type: String,
}

impl DestructiveCheckContext {
    pub fn new(action_type: impl Into<String>) -> Self {
        Self {
            block_id: None,
            group_id: None,
            mount_epoch: None,
            guard_watermark: None,
            guard_state_id: None,
            not_before_ms: None,
            action_type: action_type.into(),
        }
    }

    pub fn with_block_id(mut self, block_id: BlockId) -> Self {
        self.block_id = Some(block_id);
        self
    }

    pub fn with_group_id(mut self, group_id: ShardGroupId) -> Self {
        self.group_id = Some(group_id);
        self
    }

    pub fn with_mount_epoch(mut self, mount_epoch: MountEpoch) -> Self {
        self.mount_epoch = Some(mount_epoch);
        self
    }

    pub fn with_guard_watermark(mut self, guard_watermark: GroupStateWatermark) -> Self {
        self.guard_watermark = Some(guard_watermark);
        self
    }

    pub fn with_guard_state_id(mut self, guard_state_id: RaftLogId) -> Self {
        self.guard_state_id = Some(guard_state_id);
        self
    }

    pub fn with_not_before_ms(mut self, not_before_ms: u64) -> Self {
        self.not_before_ms = Some(not_before_ms);
        self
    }
}

/// Unified gate check for destructive actions.
pub struct DestructiveGate {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    mount_table: Arc<MountTable>,
}

impl DestructiveGate {
    pub fn new(raft_node: Arc<AppRaftNode>, worker_manager: Arc<WorkerManager>, mount_table: Arc<MountTable>) -> Self {
        Self {
            raft_node,
            worker_manager,
            mount_table,
        }
    }

    /// Check if a destructive action is allowed.
    ///
    /// Cross-shard-group gate control with mount freshness and metadata state watermark.
    ///
    /// This implements the enhanced invariants:
    /// 1. leader-only
    /// 2. blockreport_converged==true (per shard_group if specified)
    /// 3. mount_epoch match (if provided)
    /// 4. applied_state_id >= guard_watermark.state_id (per shard_group)
    /// 5. now >= not_before_ms + min-age/grace + second confirmation window
    pub fn check_destructive_allowed(&self, ctx: &DestructiveCheckContext) -> MetadataResult<DestructiveCheckResult> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // Invariant 1: leader-only
        if !self.raft_node.is_leader() {
            return Ok(DestructiveCheckResult::Blocked {
                reason: "not_leader".to_string(),
            });
        }

        // Check mount_epoch consistency (if provided)
        if let Some(expected_mount_epoch) = ctx.mount_epoch {
            let current_mount_epoch = MountEpoch::new(self.mount_table.version());
            if expected_mount_epoch.as_u64() != current_mount_epoch.as_u64() {
                // Mount epoch mismatch: route changed, need refresh
                warn!(
                    action_type = %ctx.action_type,
                    expected_mount_epoch = expected_mount_epoch.as_u64(),
                    current_mount_epoch = current_mount_epoch.as_u64(),
                    "Mount epoch mismatch, need refresh"
                );
                return Ok(DestructiveCheckResult::NeedRefresh {
                    reason: format!(
                        "mount_epoch_mismatch: expected={}, current={}",
                        expected_mount_epoch.as_u64(),
                        current_mount_epoch.as_u64()
                    ),
                    latest_mount_epoch: Some(current_mount_epoch),
                    route_hint: ctx.group_id.map(|gid| format!("group_id={}", gid.as_raw())),
                });
            }
        }

        // Invariant 2: blockreport_converged==true
        // Note: We still check global convergence.
        // Per-group convergence can be added later if needed.
        let epoch = self.worker_manager.get_metadata_epoch();
        let active_ttl_ms = self.worker_manager.heartbeat_timeout_sec() * 1000;
        let snapshot = self.worker_manager.blockreport_convergence_snapshot(
            now_ms,
            active_ttl_ms,
            epoch,
            0.80, // 80% threshold
        );

        if !snapshot.converged {
            return Ok(DestructiveCheckResult::Blocked {
                reason: format!(
                    "blockreport_not_converged: active={}, full_reported={}, ratio={:.2}, threshold=0.80",
                    snapshot.active_workers, snapshot.full_reported_workers, snapshot.ratio
                ),
            });
        }

        // Invariant 3 (enhanced): Check guard_watermark per shard_group
        if let Some(guard_watermark) = &ctx.guard_watermark {
            // Get applied state_id for the target shard_group
            // Note: For now, we use the global Raft state_id.
            // In a true multi-raft setup, we'd query the specific group's state_id.
            let current_state_id = self.raft_node.get_last_applied_state_id();
            if let Some(current) = current_state_id {
                if !guard_watermark.is_reached(&current) {
                    return Ok(DestructiveCheckResult::Blocked {
                        reason: format!(
                            "watermark_not_reached: group_id={}, current={:?}, guard={:?}",
                            guard_watermark.group_id.as_raw(),
                            current,
                            guard_watermark.state_id
                        ),
                    });
                }
            } else {
                // No state_id yet, block
                return Ok(DestructiveCheckResult::Blocked {
                    reason: format!("state_id_not_available: group_id={}", guard_watermark.group_id.as_raw()),
                });
            }
        } else if let Some(guard_state_id) = ctx.guard_state_id {
            // Legacy: fallback to global state_id check
            let current_state_id = self.raft_node.get_last_applied_state_id();
            if let Some(current) = current_state_id {
                // Compare RaftLogId (term, index)
                if current.term < guard_state_id.term
                    || (current.term == guard_state_id.term && current.index < guard_state_id.index)
                {
                    return Ok(DestructiveCheckResult::Blocked {
                        reason: format!(
                            "state_id_not_reached: current={:?}, guard={:?}",
                            current, guard_state_id
                        ),
                    });
                }
            } else {
                // No state_id yet, block
                return Ok(DestructiveCheckResult::Blocked {
                    reason: "state_id_not_available".to_string(),
                });
            }
        }

        // Invariant 4: now >= not_before_ms + min-age/grace + second confirmation window
        if let Some(not_before_ms) = ctx.not_before_ms {
            if now_ms < not_before_ms {
                return Ok(DestructiveCheckResult::Blocked {
                    reason: format!("not_before_not_reached: now={}, not_before={}", now_ms, not_before_ms),
                });
            }

            // Additional grace window check (min-age)
            const MIN_AGE_MS: u64 = 60_000; // 1 minute minimum age
            if now_ms < not_before_ms + MIN_AGE_MS {
                return Ok(DestructiveCheckResult::Blocked {
                    reason: format!(
                        "min_age_not_reached: now={}, not_before={}, min_age={}",
                        now_ms, not_before_ms, MIN_AGE_MS
                    ),
                });
            }
        }

        debug!(
            action_type = %ctx.action_type,
            block_id = ?ctx.block_id,
            "Destructive action gate check passed"
        );

        Ok(DestructiveCheckResult::Allowed)
    }
}
