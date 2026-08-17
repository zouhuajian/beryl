// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Process-local freshness and routing state published after Raft apply.

use crate::error::MetadataResult;
use crate::mount::{MountEntry, MountTable, MountTableState};
use crate::raft::storage::RocksDBStorage;
use crate::raft::types::{from_openraft_log_id, AppMetadataRaftState};
use crate::state::RouteEpoch;
use beryl_types::RaftLogId;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) enum RoutingDelta {
    None,
    Upsert(MountEntry),
}

/// Small process-local view used by freshness checks and path routing.
pub(crate) struct MetadataReadView {
    routing: Arc<MountTable>,
    route_epoch: AtomicU64,
    raft_state: Arc<RwLock<AppMetadataRaftState>>,
}

impl MetadataReadView {
    pub(crate) fn new(
        routing: Arc<MountTable>,
        raft_state: Arc<RwLock<AppMetadataRaftState>>,
        storage: Arc<RocksDBStorage>,
    ) -> MetadataResult<Self> {
        let route_epoch = storage.get_route_epoch()?;
        Ok(Self {
            routing,
            route_epoch: AtomicU64::new(route_epoch.as_u64()),
            raft_state,
        })
    }

    pub(crate) fn publish_routing(&self, delta: RoutingDelta) -> MetadataResult<()> {
        match delta {
            RoutingDelta::None => Ok(()),
            RoutingDelta::Upsert(entry) => self.routing.upsert(entry),
        }
    }

    pub(crate) fn last_applied(&self) -> Option<RaftLogId> {
        self.raft_state.read().last_applied_log_id.map(from_openraft_log_id)
    }

    pub(crate) fn raft_state(&self) -> AppMetadataRaftState {
        self.raft_state.read().clone()
    }

    pub(crate) fn route_epoch(&self) -> RouteEpoch {
        RouteEpoch::new(self.route_epoch.load(Ordering::Acquire))
    }

    pub(crate) fn install_generation(
        &self,
        routing: MountTableState,
        route_epoch: RouteEpoch,
        raft_state: AppMetadataRaftState,
    ) {
        self.routing.replace(routing);
        self.route_epoch.store(route_epoch.as_u64(), Ordering::Release);
        *self.raft_state.write() = raft_state;
    }

    pub(crate) fn committed_index(&self) -> Option<u64> {
        self.raft_state.read().committed.map(|log_id| log_id.index)
    }
}
