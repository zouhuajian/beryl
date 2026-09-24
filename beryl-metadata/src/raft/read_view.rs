// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Process-local freshness and routing state published after Raft apply.

use crate::mount::{MountEntry, MountTable};
use crate::raft::types::{from_openraft_log_id, AppMetadataRaftState};
use beryl_types::RaftLogId;
use parking_lot::RwLock;
use std::sync::Arc;

/// Small process-local view used by freshness checks and path routing.
pub(crate) struct MetadataReadView {
    routing: Arc<MountTable>,
    raft_state: Arc<RwLock<AppMetadataRaftState>>,
}

impl MetadataReadView {
    pub(crate) fn new(routing: Arc<MountTable>, raft_state: Arc<RwLock<AppMetadataRaftState>>) -> Self {
        Self { routing, raft_state }
    }

    pub(crate) fn publish_root(&self, entry: MountEntry) {
        self.routing.upsert(entry);
    }

    pub(crate) fn last_applied(&self) -> Option<RaftLogId> {
        self.raft_state.read().last_applied_log_id.map(from_openraft_log_id)
    }

    pub(crate) fn membership(&self) -> openraft::Membership<u64, crate::raft::types::MetadataNode> {
        self.raft_state.read().membership.membership().clone()
    }

    pub(crate) fn committed_index(&self) -> Option<u64> {
        self.raft_state.read().committed.map(|log_id| log_id.index)
    }
}
