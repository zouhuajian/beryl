// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft log position used as a monotonic "state watermark".
//!
//! This type represents the state machine's applied position (last_applied_log_id).
//! It aligns with proto::common::RaftLogIdProto.

use serde::{Deserialize, Serialize};

/// Raft log position used as a monotonic "state watermark".
///
/// Represents the state machine's applied position (last_applied_log_id).
/// Aligns with proto::common::RaftLogIdProto where:
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
