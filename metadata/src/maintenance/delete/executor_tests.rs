// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for DeleteExecutor (storage-level persistence tests).

#[cfg(test)]
mod tests {
    use super::super::executor::DeleteExecutor;
    use crate::config::RaftConfig;
    use crate::error::MetadataResult;
    use crate::inflight_registry::InflightRegistry;
    use crate::mount::MountTable;
    use crate::raft::{AppRaftNode, AppRaftStateMachine, Command, DedupKey, RocksDBStorage};
    use crate::state::{BlockMetaState, DeleteIntent, DeleteIntentReason, DeleteIntentStatus};
    use crate::worker::{HealthStatus, WorkerManager};
    use proto::metadata::{worker_command_proto, DeleteBlocksCommandProto};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;
    use types::block::{BlockPlacement, BlockState};
    use types::fs::InodeId;
    use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, WorkerId};
    use types::CallId;
    use types::RaftLogId;

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
            // Intent without cross-group metadata uses legacy guard_state_id only.
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
                // Intent without cross-group metadata uses legacy guard_state_id only.
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
        worker_manager
            .register_worker(worker_id, "127.0.0.1:8080".to_string(), 1, 100, None)
            .unwrap();
        worker_manager
            .update_runtime(worker_id, 1, 100, 1000, 500, 500, 0, 0, HealthStatus::Healthy)
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
        worker_manager.apply_full_report(worker_id, vec![block_id]).unwrap();
        assert_eq!(worker_manager.get_block_locations(block_id), vec![worker_id]);

        storage.put_delete_intent(&DeleteIntent {
            intent_id: 42,
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

        let executor = DeleteExecutor::new(
            Arc::clone(&raft_node),
            Arc::clone(&storage),
            Arc::clone(&worker_manager),
            Arc::new(crate::metrics::MetadataMetrics::new()),
            mount_table,
            Arc::new(InflightRegistry::new(5 * 60 * 1000)),
        );

        assert!(raft_node.is_leader());
        executor.run_once().await?;
        assert_eq!(worker_manager.get_block_locations(block_id), vec![worker_id]);
        assert_eq!(executor.get_inflight_count(), 1);
        let commands = executor.get_pending_commands(worker_id, 10);
        assert_eq!(commands.len(), 1);
        match commands[0].command.as_ref() {
            Some(worker_command_proto::Command::DeleteBlocks(DeleteBlocksCommandProto {
                intent_id, blocks, ..
            })) => {
                assert_eq!(*intent_id, 42);
                assert_eq!(blocks.len(), 1);
                assert_eq!(
                    blocks[0].block_id.as_ref().unwrap().data_handle_id,
                    block_id.data_handle_id.as_raw()
                );
            }
            other => panic!("expected DeleteBlocks command, got {other:?}"),
        }

        Ok(())
    }
}
