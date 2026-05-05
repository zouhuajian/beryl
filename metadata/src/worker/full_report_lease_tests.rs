// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for FullReportLeaseManager.

#[cfg(test)]
mod tests {
    use crate::worker::full_report_lease::FullReportLeaseManager;
    use tokio::runtime::Runtime;
    use types::group_watermark::MountEpoch;
    use types::ids::{ShardGroupId, WorkerId};

    fn create_test_manager() -> FullReportLeaseManager {
        FullReportLeaseManager::new(10, 60_000) // max_concurrent=10, ttl=60s
    }

    #[test]
    fn test_concurrent_limit() {
        let rt = Runtime::new().unwrap();
        let manager = create_test_manager();
        let metadata_epoch = 1;
        let mount_epoch = Some(MountEpoch::new(100));

        // Try to allocate 15 leases (should only get 10)
        let mut tokens = Vec::new();
        for i in 1..=15 {
            let worker_id = WorkerId::new(i);
            let token = rt.block_on(manager.try_allocate(
                worker_id,
                None, // global
                metadata_epoch,
                mount_epoch,
            ));
            if let Some(t) = token {
                tokens.push(t);
            }
        }

        // Should have allocated exactly 10 leases
        assert_eq!(tokens.len(), 10);

        // Release 5 leases
        for (i, token) in tokens.iter().enumerate().take(5) {
            let worker_id = WorkerId::new((i + 1) as u64);
            rt.block_on(manager.verify_and_release(*token, worker_id, metadata_epoch, mount_epoch));
        }

        // Now should be able to allocate 5 more
        let mut additional_tokens = Vec::new();
        for i in 11..=15 {
            let worker_id = WorkerId::new(i);
            let token = rt.block_on(manager.try_allocate(
                worker_id,
                None, // global
                metadata_epoch,
                mount_epoch,
            ));
            if let Some(t) = token {
                additional_tokens.push(t);
            }
        }

        // Should have allocated 5 more
        assert_eq!(additional_tokens.len(), 5);
    }

    #[test]
    fn test_ttl_expiration() {
        let rt = Runtime::new().unwrap();
        let manager = FullReportLeaseManager::new(10, 100); // Very short TTL: 100ms
        let metadata_epoch = 1;
        let mount_epoch = Some(MountEpoch::new(100));

        // Allocate a lease
        let worker_id = WorkerId::new(1);
        let token = rt.block_on(manager.try_allocate(worker_id, None, metadata_epoch, mount_epoch));
        assert!(token.is_some());
        let _token = token.unwrap();

        // Wait for expiration
        std::thread::sleep(std::time::Duration::from_millis(150));

        // Try to allocate another lease (should succeed because first one expired)
        let worker_id2 = WorkerId::new(2);
        let token2 = rt.block_on(manager.try_allocate(worker_id2, None, metadata_epoch, mount_epoch));
        assert!(token2.is_some());

        // Verify first lease is expired (check via active count)
        // After expiration cleanup, active count should be 1 (only the second lease)
        let count = rt.block_on(manager.active_lease_count());
        assert_eq!(count, 1);
    }

    #[test]
    fn test_idempotent_allocation() {
        let rt = Runtime::new().unwrap();
        let manager = create_test_manager();
        let metadata_epoch = 1;
        let mount_epoch = Some(MountEpoch::new(100));

        // Allocate lease for same worker twice
        let worker_id = WorkerId::new(1);
        let token1 = rt.block_on(manager.try_allocate(worker_id, None, metadata_epoch, mount_epoch));
        assert!(token1.is_some());
        let token1 = token1.unwrap();

        // Allocate again (should return same token)
        let token2 = rt.block_on(manager.try_allocate(worker_id, None, metadata_epoch, mount_epoch));
        assert_eq!(token2, Some(token1));
    }

    #[test]
    fn test_epoch_validation() {
        let rt = Runtime::new().unwrap();
        let manager = create_test_manager();
        let metadata_epoch1 = 1;
        let metadata_epoch2 = 2;
        let mount_epoch = Some(MountEpoch::new(100));

        // Allocate lease with epoch 1
        let worker_id = WorkerId::new(1);
        let token = rt.block_on(manager.try_allocate(worker_id, None, metadata_epoch1, mount_epoch));
        assert!(token.is_some());
        let token = token.unwrap();

        // Try to verify with wrong epoch (should fail and remove lease)
        let result = rt.block_on(manager.verify_and_release(
            token,
            worker_id,
            metadata_epoch2, // Wrong epoch
            mount_epoch,
        ));
        assert!(!result);

        // Lease was removed, so verify it's gone
        let result2 = rt.block_on(manager.verify_and_release(token, worker_id, metadata_epoch1, mount_epoch));
        assert!(!result2); // Should fail because lease was removed

        // Allocate new lease with correct epoch and verify it works
        let token2 = rt.block_on(manager.try_allocate(worker_id, None, metadata_epoch1, mount_epoch));
        assert!(token2.is_some());
        let token2 = token2.unwrap();
        let result3 = rt.block_on(manager.verify_and_release(token2, worker_id, metadata_epoch1, mount_epoch));
        assert!(result3);
    }

    #[test]
    fn test_mount_epoch_validation() {
        let rt = Runtime::new().unwrap();
        let manager = create_test_manager();
        let metadata_epoch = 1;
        let mount_epoch1 = Some(MountEpoch::new(100));
        let mount_epoch2 = Some(MountEpoch::new(200));

        // Allocate lease with mount_epoch 100
        let worker_id = WorkerId::new(1);
        let token = rt.block_on(manager.try_allocate(worker_id, None, metadata_epoch, mount_epoch1));
        assert!(token.is_some());
        let token = token.unwrap();

        // Try to verify with wrong mount_epoch (should fail and remove lease)
        let result = rt.block_on(manager.verify_and_release(
            token,
            worker_id,
            metadata_epoch,
            mount_epoch2, // Wrong mount_epoch
        ));
        assert!(!result);

        // Lease was removed, so verify it's gone
        let result2 = rt.block_on(manager.verify_and_release(token, worker_id, metadata_epoch, mount_epoch1));
        assert!(!result2); // Should fail because lease was removed

        // Allocate new lease with correct mount_epoch and verify it works
        let token2 = rt.block_on(manager.try_allocate(worker_id, None, metadata_epoch, mount_epoch1));
        assert!(token2.is_some());
        let token2 = token2.unwrap();
        let result3 = rt.block_on(manager.verify_and_release(token2, worker_id, metadata_epoch, mount_epoch1));
        assert!(result3);
    }

    #[test]
    fn test_invalidate_all() {
        let rt = Runtime::new().unwrap();
        let manager = create_test_manager();
        let metadata_epoch = 1;
        let mount_epoch = Some(MountEpoch::new(100));

        // Allocate multiple leases
        let mut tokens = Vec::new();
        for i in 1..=5 {
            let worker_id = WorkerId::new(i);
            let token = rt.block_on(manager.try_allocate(worker_id, None, metadata_epoch, mount_epoch));
            if let Some(t) = token {
                tokens.push(t);
            }
        }
        assert_eq!(tokens.len(), 5);

        // Invalidate all (simulating leader change)
        rt.block_on(manager.invalidate_all());

        // Verify all leases are invalid
        for (i, &token) in tokens.iter().enumerate() {
            let worker_id = WorkerId::new(i as u64 + 1);
            let result = rt.block_on(manager.verify_and_release(token, worker_id, metadata_epoch, mount_epoch));
            assert!(!result); // Should fail because lease was invalidated
        }

        // Should be able to allocate new leases now
        let worker_id = WorkerId::new(6);
        let token = rt.block_on(manager.try_allocate(worker_id, None, metadata_epoch, mount_epoch));
        assert!(token.is_some());
    }

    #[test]
    fn test_shard_group_isolation() {
        let rt = Runtime::new().unwrap();
        let manager = create_test_manager();
        let metadata_epoch = 1;
        let mount_epoch = Some(MountEpoch::new(100));
        let group1 = Some(ShardGroupId::new(1));
        let group2 = Some(ShardGroupId::new(2));

        // Allocate 10 leases for group1 (should all succeed)
        let mut tokens1 = Vec::new();
        for i in 1..=10 {
            let worker_id = WorkerId::new(i);
            let token = rt.block_on(manager.try_allocate(worker_id, group1, metadata_epoch, mount_epoch));
            if let Some(t) = token {
                tokens1.push(t);
            }
        }
        assert_eq!(tokens1.len(), 10);

        // Try to allocate for group2 (should succeed because limits are per-group)
        let worker_id = WorkerId::new(11);
        let token2 = rt.block_on(manager.try_allocate(worker_id, group2, metadata_epoch, mount_epoch));
        assert!(token2.is_some());

        // Try to allocate one more for group1 (should fail - limit reached)
        let worker_id = WorkerId::new(12);
        let token3 = rt.block_on(manager.try_allocate(worker_id, group1, metadata_epoch, mount_epoch));
        assert!(token3.is_none());
    }

    #[test]
    fn test_active_lease_count() {
        let rt = Runtime::new().unwrap();
        let manager = create_test_manager();
        let metadata_epoch = 1;
        let mount_epoch = Some(MountEpoch::new(100));

        // Initially should be 0
        let count = rt.block_on(manager.active_lease_count());
        assert_eq!(count, 0);

        // Allocate 5 leases and track tokens
        let mut tokens = Vec::new();
        for i in 1..=5 {
            let worker_id = WorkerId::new(i);
            let token = rt.block_on(manager.try_allocate(worker_id, None, metadata_epoch, mount_epoch));
            if let Some(t) = token {
                tokens.push((WorkerId::new(i), t));
            }
        }

        // Should be 5
        let count = rt.block_on(manager.active_lease_count());
        assert_eq!(count, 5);

        // Release 2 leases
        for (worker_id, token) in tokens.iter().take(2) {
            rt.block_on(manager.verify_and_release(*token, *worker_id, metadata_epoch, mount_epoch));
        }

        // Should be 3
        let count = rt.block_on(manager.active_lease_count());
        assert_eq!(count, 3);
    }
}
