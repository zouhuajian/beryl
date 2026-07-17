// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Raft type definitions for metadata service.

use crate::raft::storage::SnapshotFile;
use beryl_types::RaftLogId;
use openraft::{LogId, RaftTypeConfig};
use serde::{Deserialize, Serialize};

pub(super) fn from_openraft_log_id(log_id: LogId<u64>) -> RaftLogId {
    RaftLogId::new(log_id.leader_id.term, log_id.leader_id.node_id, log_id.index)
}

/// Raft type configuration for metadata service.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub(crate) struct MetadataRaftTypeConfig;

impl RaftTypeConfig for MetadataRaftTypeConfig {
    type D = crate::raft::command::Command;
    type R = crate::raft::response::CommandResult;
    type NodeId = u64;
    type Node = MetadataNode;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = SnapshotFile;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

/// Raft state for metadata service.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AppMetadataRaftState {
    pub last_applied_log_id: Option<openraft::LogId<u64>>,
    pub last_purged_log_id: Option<openraft::LogId<u64>>,
    // Note: HardState and StoredMembership structures may be different in openraft 0.9.21
    // We'll use a simplified structure for now
    pub vote: Option<openraft::Vote<u64>>,
    pub committed: Option<openraft::LogId<u64>>,
    pub membership: openraft::StoredMembership<u64, MetadataNode>,
}

/// Node information for metadata service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct MetadataNode {
    pub node_id: u64,
    pub address: String,
}
