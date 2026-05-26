// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for DeleteExecutor (storage-level persistence tests).

#[cfg(test)]
mod tests {
    use super::super::executor::DeleteExecutor;
    use crate::config::RaftConfig;
    use crate::error::MetadataResult;
    use crate::inflight_registry::InflightRegistry;
    use crate::metrics::MetadataMetrics;
    use crate::mount::MountTable;
    use crate::raft::{AppRaftNode, AppRaftStateMachine, Command, DedupKey, RocksDBStorage};
    use crate::state::{BlockMetaState, DeleteIntent, DeleteIntentReason, DeleteIntentStatus};
    use crate::worker::{HealthStatus, WorkerManager};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;
    use types::block::{BlockPlacement, BlockState};
    use types::fs::InodeId;
    use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, ShardGroupId, WorkerId};
    use types::{CallId, RaftLogId, WorkerRunId};

    struct DeleteExecutorTestEnv {
        _temp_dir: TempDir,
        storage: Arc<RocksDBStorage>,
        worker_manager: Arc<WorkerManager>,
        metrics: Arc<MetadataMetrics>,
        executor: DeleteExecutor,
    }

    async fn new_delete_executor_test_env() -> MetadataResult<DeleteExecutorTestEnv> {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_executor");
        let storage = Arc::new(RocksDBStorage::open(&db_path)?);
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
        let raft_config = RaftConfig {
            node_id: 1,
            peers: vec!["127.0.0.1:0".to_string()],
        };
        let raft_node = Arc::new(
            AppRaftNode::new(
                raft_config.node_id,
                Arc::clone(&storage),
                Arc::clone(&state_machine),
                &raft_config,
            )
            .await
            .unwrap(),
        );
        wait_for_test_leader(&raft_node).await;

        let worker_manager = Arc::new(WorkerManager::new(60));
        worker_manager.increment_metadata_epoch();
        let inflight_registry = Arc::new(InflightRegistry::new(5 * 60 * 1000));
        let metrics = Arc::new(MetadataMetrics::new());
        let executor = DeleteExecutor::new(
            Arc::clone(&raft_node),
            Arc::clone(&storage),
            Arc::clone(&worker_manager),
            Arc::clone(&metrics),
            mount_table,
            Arc::clone(&inflight_registry),
        );

        Ok(DeleteExecutorTestEnv {
            _temp_dir: temp_dir,
            storage,
            worker_manager,
            metrics,
            executor,
        })
    }

    async fn wait_for_test_leader(raft_node: &AppRaftNode) {
        for _ in 0..100 {
            if raft_node.is_leader() && raft_node.get_last_applied_state_id().is_some() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
        assert!(raft_node.is_leader());
        assert!(raft_node.get_last_applied_state_id().is_some());
    }

    fn worker_run_id(worker_id: WorkerId) -> WorkerRunId {
        format!("550e8400-e29b-41d4-a716-{:012x}", worker_id.as_raw())
            .parse()
            .expect("valid test WorkerRunId")
    }

    fn add_live_worker_with_blocks(
        worker_manager: &WorkerManager,
        worker_id: WorkerId,
        blocks: Vec<BlockId>,
    ) -> MetadataResult<()> {
        let group_id = ShardGroupId::new(1);
        let address = "127.0.0.1:8080".to_string();
        let run_id = worker_run_id(worker_id);
        worker_manager.register_worker(group_id, worker_id, address.clone(), 1, 100, None)?;
        worker_manager.register_worker_run(group_id, worker_id, address.clone(), 1, run_id, None)?;
        worker_manager.record_heartbeat(
            group_id,
            worker_id,
            run_id,
            1,
            &address,
            1,
            1000,
            500,
            500,
            0,
            0,
            HealthStatus::Healthy,
        )?;
        let run_id = worker_manager
            .get_registration(ShardGroupId::new(1), worker_id)
            .expect("test worker registration")
            .worker_run_id;
        let report_blocks = blocks
            .into_iter()
            .map(|block_id| crate::worker::BlockReportBlock {
                block_id,
                data_handle_id: block_id.data_handle_id.as_raw(),
                block_index: block_id.index.as_raw(),
                block_stamp: 1,
                effective_len: 4096,
                committed_length: 4096,
                block_state: crate::worker::BlockReportBlockState::Ready,
            })
            .collect();
        worker_manager.receive_full_block_report(ShardGroupId::new(1), worker_id, run_id, 1, 0, true, report_blocks)?;
        Ok(())
    }

    fn put_sealed_block(storage: &RocksDBStorage, block_id: BlockId, worker_id: WorkerId) -> MetadataResult<()> {
        storage.put_block(&BlockMetaState {
            block_id,
            inode_id: InodeId::new(99 + block_id.index.as_raw() as u64),
            data_handle_id: block_id.data_handle_id,
            state: BlockState::Sealed,
            placement: BlockPlacement {
                primary: worker_id,
                replicas: Vec::new(),
            },
            committed_length: 4096,
        })
    }

    fn put_pending_delete_intent(storage: &RocksDBStorage, intent_id: u64, block_id: BlockId) -> MetadataResult<()> {
        storage.put_delete_intent(&DeleteIntent {
            intent_id,
            block_id,
            reason: DeleteIntentReason::Gc,
            created_at_ms: 0,
            not_before_ms: 0,
            shard_group_id: Some(ShardGroupId::new(1)),
            guard_watermark: None,
            mount_epoch: None,
            guard_state_id: RaftLogId::default(),
            target_workers: Vec::new(),
            status: DeleteIntentStatus::Pending,
            finished_at_ms: None,
            last_error_msg: None,
        })
    }

    #[tokio::test]
    async fn test_delete_intent_status_persistence() -> MetadataResult<()> {
        // Setup: Create temporary RocksDB
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let storage = Arc::new(RocksDBStorage::open(&db_path)?);
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage), Arc::new(MountTable::new()));

        // Create a test intent
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let intent_id = 12345u64;
        let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));
        let guard_state_id = RaftLogId::new(1, 1, 100);

        let intent = DeleteIntent {
            intent_id,
            block_id,
            reason: DeleteIntentReason::Gc,
            created_at_ms: now_ms,
            not_before_ms: now_ms - 1000, // Already ready
            // Intent without cross-group metadata uses single-group guard_state_id only.
            shard_group_id: None,
            guard_watermark: None,
            mount_epoch: None,
            guard_state_id,
            target_workers: Vec::new(),
            status: DeleteIntentStatus::Pending,
            finished_at_ms: None,
            last_error_msg: None,
        };

        // 1. Persist intent as Pending
        storage.put_delete_intent(&intent)?;

        // 2. Verify it appears in list_pending
        let pending = storage.list_pending_delete_intents(10, now_ms)?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].intent_id, intent_id);
        assert!(matches!(pending[0].status, DeleteIntentStatus::Pending));

        // 3. Mark as Completed
        state_machine.apply(Command::UpdateDeleteIntentStatus {
            dedup: DedupKey::new(ClientId::new(710), CallId::new()),
            intent_id,
            status: DeleteIntentStatus::Completed,
            finished_at_ms: Some(now_ms + 1000),
            error_msg: None,
        })?;

        // 4. Verify it no longer appears in list_pending
        let pending_after = storage.list_pending_delete_intents(10, now_ms)?;
        assert_eq!(
            pending_after.len(),
            0,
            "Completed intent should not appear in pending list"
        );

        // 5. Verify we can still get it by intent_id
        let retrieved = storage.get_delete_intent(intent_id)?;
        assert!(retrieved.is_some());
        let retrieved_intent = retrieved.unwrap();
        assert!(matches!(retrieved_intent.status, DeleteIntentStatus::Completed));
        assert_eq!(retrieved_intent.finished_at_ms, Some(now_ms + 1000));

        // 6. Test idempotency: replay of the same status command should not drift timestamp.
        state_machine.apply(Command::UpdateDeleteIntentStatus {
            dedup: DedupKey::new(ClientId::new(711), CallId::new()),
            intent_id,
            status: DeleteIntentStatus::Completed,
            finished_at_ms: Some(now_ms + 1000),
            error_msg: None,
        })?;

        // 7. Completed -> Failed is an invalid authoritative transition.
        assert!(state_machine
            .apply(Command::UpdateDeleteIntentStatus {
                dedup: DedupKey::new(ClientId::new(712), CallId::new()),
                intent_id,
                status: DeleteIntentStatus::Failed,
                finished_at_ms: Some(now_ms + 3000),
                error_msg: Some("test error".to_string()),
            },)
            .is_err());

        let failed_intent = storage.get_delete_intent(intent_id)?.unwrap();
        assert!(matches!(failed_intent.status, DeleteIntentStatus::Completed));
        assert_eq!(failed_intent.last_error_msg, None);

        // 8. Verify Failed intent also doesn't appear in pending
        let pending_after_failed = storage.list_pending_delete_intents(10, now_ms)?;
        assert_eq!(pending_after_failed.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_intent_status_after_restart() -> MetadataResult<()> {
        // Test that completed intents persist across storage restarts
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_restart");

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let intent_id = 99999u64;
        let block_id = BlockId::new(DataHandleId::new(2), BlockIndex::new(1));
        let guard_state_id = RaftLogId::new(1, 1, 200);

        // Create and complete an intent
        {
            let storage = Arc::new(RocksDBStorage::open(&db_path)?);
            let state_machine = AppRaftStateMachine::new(Arc::clone(&storage), Arc::new(MountTable::new()));
            let intent = DeleteIntent {
                intent_id,
                block_id,
                reason: DeleteIntentReason::Gc,
                created_at_ms: now_ms,
                not_before_ms: now_ms - 1000,
                // Intent without cross-group metadata uses single-group guard_state_id only.
                shard_group_id: None,
                guard_watermark: None,
                mount_epoch: None,
                guard_state_id,
                target_workers: Vec::new(),
                status: DeleteIntentStatus::Pending,
                finished_at_ms: None,
                last_error_msg: None,
            };
            storage.put_delete_intent(&intent)?;
            state_machine.apply(Command::UpdateDeleteIntentStatus {
                dedup: DedupKey::new(ClientId::new(713), CallId::new()),
                intent_id,
                status: DeleteIntentStatus::Completed,
                finished_at_ms: Some(now_ms + 5000),
                error_msg: None,
            })?;
        }

        // "Restart": open storage again
        {
            let storage = RocksDBStorage::open(&db_path)?;

            // Verify completed intent is not in pending list
            let pending = storage.list_pending_delete_intents(10, now_ms + 10000)?;
            assert_eq!(pending.len(), 0, "Completed intent should not appear after restart");

            // Verify we can still retrieve it
            let retrieved = storage.get_delete_intent(intent_id)?;
            assert!(retrieved.is_some());
            let retrieved_intent = retrieved.unwrap();
            assert!(matches!(retrieved_intent.status, DeleteIntentStatus::Completed));
            assert_eq!(retrieved_intent.finished_at_ms, Some(now_ms + 5000));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_executor_polls_pending_intent_and_generates_delete_blocks_command() -> MetadataResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_executor_commands");
        let storage = Arc::new(RocksDBStorage::open(&db_path)?);
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
        let raft_config = RaftConfig {
            node_id: 1,
            peers: vec!["127.0.0.1:0".to_string()],
        };
        let raft_node = Arc::new(
            AppRaftNode::new(
                raft_config.node_id,
                Arc::clone(&storage),
                Arc::clone(&state_machine),
                &raft_config,
            )
            .await
            .unwrap(),
        );
        for _ in 0..100 {
            if raft_node.is_leader() && raft_node.get_last_applied_state_id().is_some() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
        assert!(raft_node.is_leader());

        let worker_manager = Arc::new(WorkerManager::new(60));
        worker_manager.increment_metadata_epoch();
        let worker_id = WorkerId::new(1);
        let address = "127.0.0.1:8080".to_string();
        let run_id = worker_run_id(worker_id);
        worker_manager
            .register_worker(ShardGroupId::new(1), worker_id, address.clone(), 1, 100, None)
            .unwrap();
        worker_manager
            .register_worker_run(ShardGroupId::new(1), worker_id, address.clone(), 1, run_id, None)
            .unwrap();
        worker_manager
            .record_heartbeat(
                ShardGroupId::new(1),
                worker_id,
                run_id,
                1,
                &address,
                1,
                1000,
                500,
                500,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();

        let block_id = BlockId::new(DataHandleId::new(7), BlockIndex::new(0));
        storage.put_block(&BlockMetaState {
            block_id,
            inode_id: InodeId::new(99),
            data_handle_id: block_id.data_handle_id,
            state: BlockState::Sealed,
            placement: BlockPlacement {
                primary: worker_id,
                replicas: Vec::new(),
            },
            committed_length: 4096,
        })?;
        worker_manager
            .receive_full_block_report(
                ShardGroupId::new(1),
                worker_id,
                run_id,
                1,
                0,
                true,
                vec![crate::worker::BlockReportBlock {
                    block_id,
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                    block_stamp: 1,
                    effective_len: 4096,
                    committed_length: 4096,
                    block_state: crate::worker::BlockReportBlockState::Ready,
                }],
            )
            .unwrap();
        assert_eq!(
            worker_manager.get_block_locations(ShardGroupId::new(1), block_id),
            vec![worker_id]
        );

        storage.put_delete_intent(&DeleteIntent {
            intent_id: 42,
            block_id,
            reason: DeleteIntentReason::Gc,
            created_at_ms: 0,
            not_before_ms: 0,
            shard_group_id: Some(ShardGroupId::new(1)),
            guard_watermark: None,
            mount_epoch: None,
            guard_state_id: RaftLogId::default(),
            target_workers: Vec::new(),
            status: DeleteIntentStatus::Pending,
            finished_at_ms: None,
            last_error_msg: None,
        })?;
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        assert_eq!(storage.list_pending_delete_intents(10, now_ms)?.len(), 1);
        let snapshot = worker_manager.blockreport_convergence_snapshot(
            now_ms,
            worker_manager.heartbeat_timeout_sec() * 1000,
            worker_manager.get_metadata_epoch(),
            0.80,
        );
        assert!(snapshot.converged);
        let block_state_allowed = raft_node
            .read(false, |sm| {
                let block_meta = sm.storage().get_block(block_id)?;
                Ok(block_meta
                    .as_ref()
                    .map(|b| matches!(b.state, BlockState::Sealed | BlockState::Aborted))
                    .unwrap_or(false))
            })
            .await?;
        assert!(block_state_allowed);
        let has_active_lease = raft_node
            .read(false, |sm| Ok(sm.storage().get_lease(block_id)?.is_some()))
            .await?;
        assert!(!has_active_lease);

        let metrics = Arc::new(MetadataMetrics::new());
        let executor = DeleteExecutor::new(
            Arc::clone(&raft_node),
            Arc::clone(&storage),
            Arc::clone(&worker_manager),
            Arc::clone(&metrics),
            mount_table,
            Arc::new(InflightRegistry::new(5 * 60 * 1000)),
        );

        assert!(raft_node.is_leader());
        executor.run_once().await?;
        assert_eq!(
            worker_manager.get_block_locations(ShardGroupId::new(1), block_id),
            vec![worker_id]
        );
        assert_eq!(metrics.delete_executor_requests_total.load(Ordering::Relaxed), 1);

        Ok(())
    }

    #[tokio::test]
    async fn delete_intent_without_authoritative_group_does_not_route_from_reported_locations() -> MetadataResult<()> {
        let env = new_delete_executor_test_env().await?;
        let worker_id = WorkerId::new(1);
        let block_id = BlockId::new(DataHandleId::new(80), BlockIndex::new(0));
        put_sealed_block(&env.storage, block_id, worker_id)?;
        add_live_worker_with_blocks(&env.worker_manager, worker_id, vec![block_id])?;
        env.storage.put_delete_intent(&DeleteIntent {
            intent_id: 150,
            block_id,
            reason: DeleteIntentReason::Gc,
            created_at_ms: 0,
            not_before_ms: 0,
            shard_group_id: None,
            guard_watermark: None,
            mount_epoch: None,
            guard_state_id: RaftLogId::default(),
            target_workers: Vec::new(),
            status: DeleteIntentStatus::Pending,
            finished_at_ms: None,
            last_error_msg: None,
        })?;

        env.executor.run_once().await?;

        assert_eq!(env.metrics.delete_executor_requests_total.load(Ordering::Relaxed), 0);
        assert!(matches!(
            env.storage.get_delete_intent(150)?.unwrap().status,
            DeleteIntentStatus::Pending
        ));

        Ok(())
    }

    #[tokio::test]
    async fn no_location_completion_persists_completed_status_through_raft() -> MetadataResult<()> {
        let env = new_delete_executor_test_env().await?;
        let worker_id = WorkerId::new(1);
        let block_id = BlockId::new(DataHandleId::new(83), BlockIndex::new(0));
        put_sealed_block(&env.storage, block_id, worker_id)?;
        put_pending_delete_intent(&env.storage, 401, block_id)?;

        env.executor.run_once().await?;

        let stored = env.storage.get_delete_intent(401)?.unwrap();
        assert!(matches!(stored.status, DeleteIntentStatus::Completed));

        Ok(())
    }
}
