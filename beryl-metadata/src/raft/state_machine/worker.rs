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
    fn worker_descriptor_reapply_returns_original_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let worker_id = WorkerId::new(76);
        let cmd = Command::RegisterWorkerDescriptor {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            group_name: group_name("root"),
            worker_id,
            address: "127.0.0.1:17076".to_string(),
            worker_net_protocol: 1,
            fault_domain: Some("rack-a".to_string()),
        };

        assert_eq!(expect_worker_upserted(sm.apply(cmd.clone()).unwrap()), worker_id);
        assert_eq!(expect_worker_upserted(sm.apply(cmd).unwrap()), worker_id);
        let stored = storage
            .get_worker_in_group(&group_name("root"), worker_id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.address, "127.0.0.1:17076");
        assert_eq!(stored.worker_id, worker_id);
        assert_eq!(stored.group_name, group_name("root"));
    }

    #[test]
    fn register_worker_apply_replaces_durable_descriptor_without_publishing_live_run() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let worker_manager = Arc::new(crate::worker::WorkerManager::new(60));
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

    #[test]
    fn register_worker_apply_is_independent_of_same_run_live_descriptor() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let worker_manager = Arc::new(crate::worker::WorkerManager::new(60));
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let endpoint_worker_id = WorkerId::new(762);
        let protocol_worker_id = WorkerId::new(763);
        let run_id: beryl_types::WorkerRunId = "550e8400-e29b-41d4-a716-446655440002".parse().unwrap();

        assert_eq!(
            expect_worker_upserted(
                sm.apply(Command::RegisterWorkerDescriptor {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    group_name: group_name("root"),
                    worker_id: endpoint_worker_id,
                    address: "127.0.0.1:17062".to_string(),
                    worker_net_protocol: 1,
                    fault_domain: None
                })
                .unwrap()
            ),
            endpoint_worker_id
        );
        worker_manager
            .register_worker_run(
                &group_name("root"),
                endpoint_worker_id,
                "127.0.0.1:17062".to_string(),
                1,
                run_id,
                None,
            )
            .unwrap();
        assert_eq!(
            expect_worker_upserted(
                sm.apply(Command::RegisterWorkerDescriptor {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    group_name: group_name("root"),
                    worker_id: endpoint_worker_id,
                    address: "127.0.0.1:17063".to_string(),
                    worker_net_protocol: 1,
                    fault_domain: None
                })
                .unwrap()
            ),
            endpoint_worker_id
        );
        assert_eq!(
            storage
                .get_worker_in_group(&group_name("root"), endpoint_worker_id)
                .unwrap()
                .expect("stored worker")
                .address,
            "127.0.0.1:17063"
        );

        assert_eq!(
            expect_worker_upserted(
                sm.apply(Command::RegisterWorkerDescriptor {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    group_name: group_name("root"),
                    worker_id: protocol_worker_id,
                    address: "127.0.0.1:17064".to_string(),
                    worker_net_protocol: 1,
                    fault_domain: None
                })
                .unwrap()
            ),
            protocol_worker_id
        );
        worker_manager
            .register_worker_run(
                &group_name("root"),
                protocol_worker_id,
                "127.0.0.1:17064".to_string(),
                1,
                run_id,
                None,
            )
            .unwrap();
        assert_eq!(
            expect_worker_upserted(
                sm.apply(Command::RegisterWorkerDescriptor {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    group_name: group_name("root"),
                    worker_id: protocol_worker_id,
                    address: "127.0.0.1:17064".to_string(),
                    worker_net_protocol: 2,
                    fault_domain: None
                })
                .unwrap()
            ),
            protocol_worker_id
        );
        assert_eq!(
            storage
                .get_worker_in_group(&group_name("root"), protocol_worker_id)
                .unwrap()
                .expect("stored worker")
                .worker_net_protocol,
            2
        );
    }

    #[test]
    fn register_worker_apply_accepts_live_endpoint_replacement() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let worker_manager = Arc::new(crate::worker::WorkerManager::new(60));
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let worker_id = WorkerId::new(766);
        let first_run_id: beryl_types::WorkerRunId = "550e8400-e29b-41d4-a716-446655440040".parse().unwrap();

        assert_eq!(
            expect_worker_upserted(
                sm.apply(Command::RegisterWorkerDescriptor {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    group_name: group_name("root"),
                    worker_id,
                    address: "127.0.0.1:17066".to_string(),
                    worker_net_protocol: 1,
                    fault_domain: None
                })
                .unwrap()
            ),
            worker_id
        );
        worker_manager
            .register_worker_run(
                &group_name("root"),
                worker_id,
                "127.0.0.1:17066".to_string(),
                1,
                first_run_id,
                None,
            )
            .unwrap();
        worker_manager
            .record_heartbeat(
                &group_name("root"),
                worker_id,
                first_run_id,
                1,
                "127.0.0.1:17066",
                1,
                1_000,
                100,
                900,
                0,
                0,
                crate::worker::HealthStatus::Healthy,
            )
            .unwrap();

        assert_eq!(
            expect_worker_upserted(
                sm.apply(Command::RegisterWorkerDescriptor {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    group_name: group_name("root"),
                    worker_id,
                    address: "127.0.0.1:17067".to_string(),
                    worker_net_protocol: 2,
                    fault_domain: Some("rack-b".to_string())
                })
                .unwrap()
            ),
            worker_id
        );

        let stored = storage
            .get_worker_in_group(&group_name("root"), worker_id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.address, "127.0.0.1:17067");
        assert_eq!(
            worker_manager
                .get_registration(&group_name("root"), worker_id)
                .expect("live registration")
                .worker_run_id,
            first_run_id
        );
    }

    #[test]
    fn register_worker_descriptor_can_be_updated_after_reload() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let first_sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let worker_id = WorkerId::new(7601);
        let group_name = group_name("root");

        assert_eq!(
            expect_worker_upserted(
                first_sm
                    .apply(Command::RegisterWorkerDescriptor {
                        proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                        group_name: group_name.clone(),
                        worker_id,
                        address: "127.0.0.1:17601".to_string(),
                        worker_net_protocol: 1,
                        fault_domain: None
                    })
                    .unwrap()
            ),
            worker_id
        );

        let reloaded_manager = Arc::new(crate::worker::WorkerManager::new(60));
        let reloaded_sm = AppRaftStateMachine::new(Arc::clone(&storage));
        reloaded_manager
            .load_registered_workers(storage.list_workers().unwrap())
            .unwrap();
        assert!(reloaded_manager.get_registration(&group_name, worker_id).is_none());
        assert!(reloaded_manager.get_descriptor(&group_name, worker_id).is_some());

        assert_eq!(
            expect_worker_upserted(
                reloaded_sm
                    .apply(Command::RegisterWorkerDescriptor {
                        proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                        group_name: group_name.clone(),
                        worker_id,
                        address: "127.0.0.1:17602".to_string(),
                        worker_net_protocol: 1,
                        fault_domain: None
                    })
                    .unwrap()
            ),
            worker_id
        );
        assert!(reloaded_manager.get_registration(&group_name, worker_id).is_none());
        assert_eq!(
            storage
                .get_worker_in_group(&group_name, worker_id)
                .unwrap()
                .unwrap()
                .address,
            "127.0.0.1:17602"
        );
    }

    #[test]
    fn register_worker_is_scoped_by_metadata_group() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let worker_id = WorkerId::new(761);
        let first_group = group_name("root");
        let second_group = group_name("g2");

        let first = Command::RegisterWorkerDescriptor {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            group_name: first_group.clone(),
            worker_id,
            address: "127.0.0.1:17062".to_string(),
            worker_net_protocol: 1,
            fault_domain: None,
        };
        let second = Command::RegisterWorkerDescriptor {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            group_name: second_group.clone(),
            worker_id,
            address: "127.0.0.1:17063".to_string(),
            worker_net_protocol: 1,
            fault_domain: None,
        };

        assert_eq!(expect_worker_upserted(sm.apply(first).unwrap()), worker_id);
        assert_eq!(expect_worker_upserted(sm.apply(second).unwrap()), worker_id);

        let first_stored = storage.get_worker_in_group(&first_group, worker_id).unwrap().unwrap();
        let second_stored = storage.get_worker_in_group(&second_group, worker_id).unwrap().unwrap();
        assert_eq!(first_stored.group_name, first_group);
        assert_eq!(first_stored.address, "127.0.0.1:17062");
        assert_eq!(second_stored.group_name, second_group);
        assert_eq!(second_stored.address, "127.0.0.1:17063");
    }

    #[test]
    fn register_worker_apply_does_not_publish_live_worker_state() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let worker_manager = Arc::new(crate::worker::WorkerManager::new(60));
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let worker_id = WorkerId::new(762);
        let first_group = group_name("root");
        let second_group = group_name("g2");

        assert_eq!(
            expect_worker_upserted(
                sm.apply(Command::RegisterWorkerDescriptor {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    group_name: first_group.clone(),
                    worker_id,
                    address: "127.0.0.1:17064".to_string(),
                    worker_net_protocol: 1,
                    fault_domain: None
                })
                .unwrap()
            ),
            worker_id
        );
        assert_eq!(
            expect_worker_upserted(
                sm.apply(Command::RegisterWorkerDescriptor {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    group_name: second_group.clone(),
                    worker_id,
                    address: "127.0.0.1:17065".to_string(),
                    worker_net_protocol: 1,
                    fault_domain: None
                })
                .unwrap()
            ),
            worker_id
        );

        assert!(worker_manager.get_descriptor(&first_group, worker_id).is_none());
        assert!(worker_manager.get_descriptor(&second_group, worker_id).is_none());
        assert!(worker_manager.get_registration(&first_group, worker_id).is_none());
        assert!(worker_manager.get_registration(&second_group, worker_id).is_none());
        assert_eq!(
            storage
                .get_worker_in_group(&first_group, worker_id)
                .unwrap()
                .unwrap()
                .address,
            "127.0.0.1:17064"
        );
        assert_eq!(
            storage
                .get_worker_in_group(&second_group, worker_id)
                .unwrap()
                .unwrap()
                .address,
            "127.0.0.1:17065"
        );
    }
}
