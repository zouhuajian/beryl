// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for DeleteExecutor (storage-level persistence tests).

#[cfg(test)]
mod tests {
    use crate::error::MetadataResult;
    use crate::mount::MountTable;
    use crate::raft::{AppRaftStateMachine, Command, DedupKey, RocksDBStorage};
    use crate::state::{DeleteIntent, DeleteIntentReason, DeleteIntentStatus};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;
    use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId};
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
        state_machine.apply(
            Command::UpdateDeleteIntentStatus {
                dedup: DedupKey::new(ClientId::new(710), CallId::new()),
                intent_id,
                status: DeleteIntentStatus::Completed,
                finished_at_ms: Some(now_ms + 1000),
                error_msg: None,
            },
            1,
        )?;

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
        state_machine.apply(
            Command::UpdateDeleteIntentStatus {
                dedup: DedupKey::new(ClientId::new(711), CallId::new()),
                intent_id,
                status: DeleteIntentStatus::Completed,
                finished_at_ms: Some(now_ms + 1000),
                error_msg: None,
            },
            2,
        )?;

        // 7. Completed -> Failed is an invalid authoritative transition.
        assert!(state_machine
            .apply(
                Command::UpdateDeleteIntentStatus {
                    dedup: DedupKey::new(ClientId::new(712), CallId::new()),
                    intent_id,
                    status: DeleteIntentStatus::Failed,
                    finished_at_ms: Some(now_ms + 3000),
                    error_msg: Some("test error".to_string()),
                },
                3,
            )
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
            state_machine.apply(
                Command::UpdateDeleteIntentStatus {
                    dedup: DedupKey::new(ClientId::new(713), CallId::new()),
                    intent_id,
                    status: DeleteIntentStatus::Completed,
                    finished_at_ms: Some(now_ms + 5000),
                    error_msg: None,
                },
                1,
            )?;
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
}
