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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mount::{DataIoPolicy, MountKind};
    use beryl_types::fs::InodeId;
    use beryl_types::ids::MountId;
    use beryl_types::GroupName;
    use openraft::{LeaderId, LogId};
    use tempfile::TempDir;

    fn mount(mount_id: u64, prefix: &str) -> MountEntry {
        MountEntry {
            mount_id: MountId::new(mount_id),
            mount_prefix: prefix.to_string(),
            mount_kind: MountKind::Internal,
            ufs_uri: None,
            data_io_policy: DataIoPolicy::Allow,
            mount_epoch: 1,
            namespace_owner_group_name: GroupName::parse("root").unwrap(),
            root_inode_id: InodeId::new(mount_id),
        }
    }

    #[test]
    fn routing_delta_publishes_upsert() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let routing = Arc::new(MountTable::new());
        let state = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let view = MetadataReadView::new(Arc::clone(&routing), state, storage).unwrap();

        view.publish_routing(RoutingDelta::Upsert(mount(7, "/seven"))).unwrap();
        assert!(routing.get_mount(MountId::new(7)).unwrap().is_some());
    }

    #[test]
    fn applied_state_reads_shared_published_state() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let state = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let view = MetadataReadView::new(Arc::new(MountTable::new()), Arc::clone(&state), storage).unwrap();
        state.write().last_applied_log_id = Some(LogId::new(LeaderId::new(3, 9), 17));

        let observed = view.last_applied().unwrap();
        assert_eq!((observed.term, observed.leader_node_id, observed.index), (3, 9, 17));
    }
}
