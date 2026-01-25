// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for DeleteExecutor (storage-level persistence tests).

#[cfg(test)]
mod tests {
    use crate::error::MetadataResult;
    use crate::raft::RocksDBStorage;
    use crate::state::{DeleteIntent, DeleteIntentReason, DeleteIntentStatus};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;
    use types::ids::{BlockId, BlockIndex, DataHandleId};
    use types::RaftLogId;

    #[tokio::test]
    async fn test_delete_intent_status_persistence() -> MetadataResult<()> {
        // Setup: Create temporary RocksDB
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let storage = RocksDBStorage::open(&db_path)?;

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
        storage.update_delete_intent_status(intent_id, DeleteIntentStatus::Completed, Some(now_ms + 1000), None)?;

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

        // 6. Test idempotency: write Completed again should not error
        storage.update_delete_intent_status(intent_id, DeleteIntentStatus::Completed, Some(now_ms + 2000), None)?;

        // 7. Test Failed status
        storage.update_delete_intent_status(
            intent_id,
            DeleteIntentStatus::Failed,
            Some(now_ms + 3000),
            Some("test error".to_string()),
        )?;

        let failed_intent = storage.get_delete_intent(intent_id)?.unwrap();
        assert!(matches!(failed_intent.status, DeleteIntentStatus::Failed));
        assert_eq!(failed_intent.last_error_msg, Some("test error".to_string()));

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
            let storage = RocksDBStorage::open(&db_path)?;
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
            storage.update_delete_intent_status(intent_id, DeleteIntentStatus::Completed, Some(now_ms + 5000), None)?;
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
