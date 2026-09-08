// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Group state watermark for metadata freshness.
//!
//! This module defines types for tracking state-machine applied progress per
//! metadata Raft owner group.

use crate::GroupName;
use serde::{Deserialize, Serialize};

/// Raft log position used as a monotonic "state watermark".
///
/// Represents the state machine's applied position (last_applied_log_id).
/// Aligns with beryl_proto::common::RaftLogIdProto where:
/// - term: Leader term that created the entry
/// - leader_node_id: Node ID of the leader that created the entry (NOT necessarily the current leader)
/// - index: Log index
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct RaftLogId {
    /// Leader term that created the entry (align with RaftLogIdProto.term)
    pub term: u64,
    /// Node ID of the leader that created the entry (align with RaftLogIdProto.leader_node_id)
    /// This is NOT necessarily the current leader.
    pub leader_node_id: u64,
    /// Log index (align with RaftLogIdProto.index)
    pub index: u64,
}

impl RaftLogId {
    /// Create a new RaftLogId.
    pub fn new(term: u64, leader_node_id: u64, index: u64) -> Self {
        Self {
            term,
            leader_node_id,
            index,
        }
    }

    /// Return true when this applied log id satisfies a required watermark.
    pub fn has_reached(&self, required: &Self) -> bool {
        self >= required
    }
}

impl Ord for RaftLogId {
    /// Compare by (index, term, leader_node_id) for stable ordering.
    /// This ensures "watermark comparison" is stable.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.index
            .cmp(&other.index)
            .then_with(|| self.term.cmp(&other.term))
            .then_with(|| self.leader_node_id.cmp(&other.leader_node_id))
    }
}

impl PartialOrd for RaftLogId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Group state watermark for a specific metadata Raft owner group.
///
/// `state_id` is the state-machine applied RaftLogId for `group_name`. It is not
/// an append index, committed index, private apply counter, route epoch,
/// mount epoch, worker process-run identity, or writer lease epoch.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupStateWatermark {
    /// Metadata Raft owner group this watermark applies to.
    pub group_name: GroupName,
    /// Applied state-machine RaftLogId that must be reached.
    pub state_id: RaftLogId,
}

impl GroupStateWatermark {
    /// Create a new GroupStateWatermark.
    pub fn new(group_name: GroupName, state_id: RaftLogId) -> Self {
        Self { group_name, state_id }
    }
}
