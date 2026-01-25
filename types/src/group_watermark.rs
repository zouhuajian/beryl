// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Group watermark for cross-shard-group destructive gate control.
//!
//! This module defines types for tracking state machine progress per shard group,
//! enabling safe destructive operations across multiple Raft groups.

use crate::RaftLogId;
use crate::ids::ShardGroupId;
use serde::{Deserialize, Serialize};

/// Group watermark: tracks the applied state ID for a specific shard group.
///
/// Used to ensure destructive operations only proceed when the target shard group
/// has applied at least up to the guard state ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupWatermark {
    /// Shard group ID this watermark applies to.
    pub shard_group_id: ShardGroupId,
    /// Guard state ID (term, index) that must be reached before allowing destructive operations.
    pub state_id: RaftLogId,
}

impl GroupWatermark {
    /// Create a new GroupWatermark.
    pub fn new(shard_group_id: ShardGroupId, state_id: RaftLogId) -> Self {
        Self {
            shard_group_id,
            state_id,
        }
    }

    /// Check if this watermark has been reached by the given state ID.
    /// Returns true if `applied_state_id >= self.state_id`.
    pub fn is_reached(&self, applied_state_id: &RaftLogId) -> bool {
        // Compare term first, then index
        if applied_state_id.term > self.state_id.term {
            return true;
        }
        if applied_state_id.term < self.state_id.term {
            return false;
        }
        // Same term: compare index
        applied_state_id.index >= self.state_id.index
    }

    /// Compare two watermarks from the same shard group.
    /// Returns Some(Ordering) if they are from the same group, None otherwise.
    pub fn cmp_same_group(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.shard_group_id != other.shard_group_id {
            return None;
        }
        Some(self.state_id.cmp(&other.state_id))
    }
}

/// Mount epoch (or config version) for route consistency checking.
///
/// This ensures destructive operations are only allowed when the mount/routing
/// configuration matches the expected epoch. Mismatch indicates route change
/// and requires refresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MountEpoch(pub u64);

impl MountEpoch {
    /// Create a new MountEpoch.
    pub fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    /// Get the epoch value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Default for MountEpoch {
    fn default() -> Self {
        Self(0)
    }
}
