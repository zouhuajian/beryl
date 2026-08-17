// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker descriptor mutations.

use super::*;

impl AppRaftStateMachine {
    // Keep the state transition inputs explicit at the apply boundary.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_register_worker(
        &self,
        group_name: GroupName,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        fault_domain: Option<String>,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<WorkerId> {
        let worker_info = self.storage.prepare_worker_registration(
            group_name,
            worker_id,
            address,
            worker_net_protocol,
            fault_domain,
        )?;
        self.storage.register_worker_atomic(&worker_info, raft_state)?;
        Ok(worker_info.worker_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::tests::*;

    #[test]
    fn register_worker_apply_replaces_durable_descriptor_without_publishing_live_run() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let worker_manager = Arc::new(crate::worker::WorkerManager::new(60_000));
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let worker_id = WorkerId::new(760);

        let first = Command::RegisterWorkerDescriptor {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            group_name: group_name("root"),
            worker_id,
            address: "127.0.0.1:17060".to_string(),
            worker_net_protocol: 1,
            fault_domain: None,
        };
        let second = Command::RegisterWorkerDescriptor {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            group_name: group_name("root"),
            worker_id,
            address: "127.0.0.1:17061".to_string(),
            worker_net_protocol: 2,
            fault_domain: Some("rack-b".to_string()),
        };

        assert_eq!(expect_worker_upserted(sm.apply(first.clone()).unwrap()), worker_id);
        assert_eq!(expect_worker_upserted(sm.apply(first).unwrap()), worker_id);
        assert_eq!(expect_worker_upserted(sm.apply(second).unwrap()), worker_id);
        let stored = storage
            .get_worker_in_group(&group_name("root"), worker_id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.address, "127.0.0.1:17061");
        assert!(worker_manager
            .get_registration(&group_name("root"), worker_id)
            .is_none());
    }
}
