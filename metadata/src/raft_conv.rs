// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Conversion utilities between openraft::LogId and types::RaftLogId.
//!
//! This module provides bidirectional conversion between openraft's LogId type
//! and our domain type RaftLogId. This is the ONLY place where openraft types
//! should be converted to/from domain types.

use openraft::LogId;
use types::RaftLogId;

/// Convert from openraft::LogId<u64> to types::RaftLogId.
pub fn from_openraft_log_id(log_id: LogId<u64>) -> RaftLogId {
    RaftLogId::new(log_id.leader_id.term, log_id.leader_id.node_id, log_id.index)
}

/// Convert from &openraft::LogId<u64> to types::RaftLogId.
pub fn from_openraft_log_id_ref(log_id: &LogId<u64>) -> RaftLogId {
    RaftLogId::new(log_id.leader_id.term, log_id.leader_id.node_id, log_id.index)
}

/// Convert from Option<openraft::LogId<u64>> to Option<types::RaftLogId>.
pub fn from_option_log_id(log_id: Option<LogId<u64>>) -> Option<RaftLogId> {
    log_id.map(from_openraft_log_id)
}

/// Convert from Option<&openraft::LogId<u64>> to Option<types::RaftLogId>.
pub fn from_option_ref_log_id(log_id: Option<&LogId<u64>>) -> Option<RaftLogId> {
    log_id.map(from_openraft_log_id_ref)
}

/// Convert from types::RaftLogId to openraft::LogId<u64>.
///
/// This function constructs an openraft::LogId from a types::RaftLogId.
/// Note: openraft uses CommittedLeaderId which requires both term and node_id.
pub fn to_openraft_log_id(log_id: RaftLogId) -> LogId<u64> {
    use openraft::CommittedLeaderId;
    LogId::new(
        CommittedLeaderId {
            term: log_id.term,
            node_id: log_id.leader_node_id,
        },
        log_id.index,
    )
}

/// Convert from Option<types::RaftLogId> to Option<openraft::LogId<u64>>.
pub fn to_openraft_log_id_opt(log_id: Option<RaftLogId>) -> Option<LogId<u64>> {
    log_id.map(to_openraft_log_id)
}
