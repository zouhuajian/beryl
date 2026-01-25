// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for DeleteOpLog: idempotency and crash recovery.

#[cfg(test)]
mod tests {
    use super::super::delete_op_log::{DeleteOpLog, DeleteOpResultStatus, DeleteOpState};
    use std::sync::Arc;
    use types::ids::{BlockId, BlockIndex, DataHandleId, ShardGroupId};

    #[tokio::test]
    async fn test_delete_op_log_idempotency() {
        // T1: Ensures idempotency when the same intent_id/block_id is retried.
        let log = Arc::new(DeleteOpLog::new());
        let intent_id = 1001;
        let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));
        let group_id = ShardGroupId::new(1);
        let now_ms = 1000;

        // First acquire: should succeed
        let acquired1 = log.try_acquire(intent_id, block_id, group_id, now_ms).await.unwrap();
        assert!(acquired1, "First acquire should succeed");

        // Second acquire: should fail (already in-flight)
        let acquired2 = log
            .try_acquire(intent_id, block_id, group_id, now_ms + 100)
            .await
            .unwrap();
        assert!(!acquired2, "Second acquire should fail (already in-flight)");

        // Mark as done
        log.mark_done(intent_id, block_id, DeleteOpResultStatus::Deleted, None, now_ms + 200)
            .await
            .unwrap();

        // Third acquire after done: should fail (already done)
        let acquired3 = log
            .try_acquire(intent_id, block_id, group_id, now_ms + 300)
            .await
            .unwrap();
        assert!(!acquired3, "Third acquire should fail (already done)");

        // Verify entry exists and is Done
        let entry = log.get_entry(intent_id, block_id).await.unwrap();
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.state, DeleteOpState::Done);
        assert_eq!(entry.result_status, Some(DeleteOpResultStatus::Deleted));
    }

    #[tokio::test]
    async fn test_delete_op_log_crash_recovery() {
        // T2: Simulates worker crash recovery by replaying in-flight DeleteOpLog entries.
        let log = Arc::new(DeleteOpLog::new());
        let intent_id = 1002;
        let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(1));
        let group_id = ShardGroupId::new(1);
        let now_ms = 2000;

        // Acquire and mark as in-flight
        log.try_acquire(intent_id, block_id, group_id, now_ms).await.unwrap();
        log.mark_inflight(intent_id, block_id, now_ms + 100).await.unwrap();

        // Simulate crash: get in-flight operations
        let inflight = log.get_inflight_operations().await.unwrap();
        assert_eq!(inflight.len(), 1);
        assert_eq!(inflight[0].intent_id, intent_id);
        assert_eq!(inflight[0].block_id, block_id);
        assert_eq!(inflight[0].state, DeleteOpState::InFlight);

        // Resume: mark as done
        log.mark_done(intent_id, block_id, DeleteOpResultStatus::Deleted, None, now_ms + 200)
            .await
            .unwrap();

        // Verify no longer in-flight
        let inflight_after = log.get_inflight_operations().await.unwrap();
        assert_eq!(inflight_after.len(), 0);
    }

    #[tokio::test]
    async fn test_delete_op_log_not_found_idempotent() {
        // Test NOT_FOUND handling: idempotent success
        let log = Arc::new(DeleteOpLog::new());
        let intent_id = 1003;
        let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(2));
        let group_id = ShardGroupId::new(1);
        let now_ms = 3000;

        // Mark as NotFound (simulating block not found)
        log.try_acquire(intent_id, block_id, group_id, now_ms).await.unwrap();
        log.mark_done(intent_id, block_id, DeleteOpResultStatus::NotFound, None, now_ms + 100)
            .await
            .unwrap();

        // Retry: should return existing result (idempotent)
        let entry = log.get_entry(intent_id, block_id).await.unwrap();
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.state, DeleteOpState::Done);
        assert_eq!(entry.result_status, Some(DeleteOpResultStatus::NotFound));

        // Try acquire again: should fail (already done)
        let acquired = log
            .try_acquire(intent_id, block_id, group_id, now_ms + 200)
            .await
            .unwrap();
        assert!(!acquired, "Acquire should fail (already done with NotFound)");
    }

    #[tokio::test]
    async fn test_delete_op_log_state_transitions() {
        // Test state transitions: Accepted -> InFlight -> Done
        let log = Arc::new(DeleteOpLog::new());
        let intent_id = 1004;
        let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(3));
        let group_id = ShardGroupId::new(1);
        let now_ms = 4000;

        // Acquire: should be Accepted
        log.try_acquire(intent_id, block_id, group_id, now_ms).await.unwrap();
        let entry = log.get_entry(intent_id, block_id).await.unwrap().unwrap();
        assert_eq!(entry.state, DeleteOpState::Accepted);

        // Mark in-flight: should transition to InFlight
        log.mark_inflight(intent_id, block_id, now_ms + 100).await.unwrap();
        let entry = log.get_entry(intent_id, block_id).await.unwrap().unwrap();
        assert_eq!(entry.state, DeleteOpState::InFlight);

        // Mark done: should transition to Done
        log.mark_done(intent_id, block_id, DeleteOpResultStatus::Deleted, None, now_ms + 200)
            .await
            .unwrap();
        let entry = log.get_entry(intent_id, block_id).await.unwrap().unwrap();
        assert_eq!(entry.state, DeleteOpState::Done);
        assert_eq!(entry.result_status, Some(DeleteOpResultStatus::Deleted));
    }

    #[tokio::test]
    async fn test_delete_op_log_cleanup_old_entries() {
        // Test cleanup of old Done entries
        let log = Arc::new(DeleteOpLog::new());
        let intent_id = 1005;
        let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(4));
        let group_id = ShardGroupId::new(1);
        let now_ms = 5000;

        // Create and mark as done
        log.try_acquire(intent_id, block_id, group_id, now_ms).await.unwrap();
        log.mark_done(intent_id, block_id, DeleteOpResultStatus::Deleted, None, now_ms)
            .await
            .unwrap();

        // Cleanup entries older than 1 second
        let removed = log.cleanup_old_entries(1000, now_ms + 2000).await.unwrap();
        assert_eq!(removed, 1);

        // Verify entry is removed
        let entry = log.get_entry(intent_id, block_id).await.unwrap();
        assert!(entry.is_none());
    }
}
