// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft type definitions for metadata service.

use crate::raft::snapshot::SnapshotFile;
use openraft::RaftTypeConfig;
use serde::{Deserialize, Serialize};

/// Raft type configuration for metadata service.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct MetadataRaftTypeConfig;

impl RaftTypeConfig for MetadataRaftTypeConfig {
    type D = crate::raft::command::Command;
    type R = Vec<u8>; // Serialized response from state machine
    type NodeId = u64;
    type Node = MetadataNode;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = SnapshotFile;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

/// Raft state for metadata service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppMetadataRaftState {
    pub last_applied_log_id: Option<openraft::LogId<u64>>,
    pub last_purged_log_id: Option<openraft::LogId<u64>>,
    // Note: HardState and StoredMembership structures may be different in openraft 0.9.21
    // We'll use a simplified structure for now
    pub vote: Option<openraft::Vote<u64>>,
    pub committed: Option<openraft::LogId<u64>>,
    pub membership: openraft::Membership<u64, MetadataNode>,
}

impl Default for AppMetadataRaftState {
    fn default() -> Self {
        Self {
            last_applied_log_id: None,
            last_purged_log_id: None,
            vote: None,
            committed: None,
            membership: openraft::Membership::new(vec![], None),
        }
    }
}

/// Node information for metadata service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MetadataNode {
    pub node_id: u64,
    pub address: String,
}

impl MetadataNode {
    pub fn new(node_id: u64, address: String) -> Self {
        Self { node_id, address }
    }
}
