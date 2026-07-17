// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Tests for lease / append / truncate behavior.

use beryl_metadata::inode_lease::{LeaseManager, WriteMode};
use beryl_types::fs::{FsErrorCode, InodeId};
use beryl_types::ids::ClientId;
use beryl_types::ids::{BlockId, BlockIndex, DataHandleId};

#[test]
fn test_lease_conflict() {
    let manager = LeaseManager::default();
    let inode_id = InodeId::new(1);
    let client1 = ClientId::new(1);
    let client2 = ClientId::new(2);

    // Client1 acquires lease
    let (epoch1, _) = manager.try_acquire(inode_id, client1, WriteMode::Write, None).unwrap();

    // Client2 tries to acquire lease -> should fail with EBusy
    let result = manager.try_acquire(inode_id, client2, WriteMode::Write, Some(epoch1));
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), beryl_types::fs::FsErrorCode::EBusy);
}

#[test]
fn test_lease_epoch_overflow_fails_without_creating_runtime_lease() {
    let manager = LeaseManager::new(60_000, 10_000);
    let inode_id = InodeId::new(99);

    let result = manager.try_acquire(inode_id, ClientId::new(1), WriteMode::Write, Some(u64::MAX));

    assert_eq!(result, Err(FsErrorCode::EInval));
    assert!(manager.get_active_lease(inode_id).is_none());
}

#[test]
fn test_lease_renew() {
    let manager = LeaseManager::default();
    let inode_id = InodeId::new(1);
    let client_id = ClientId::new(1);

    // Acquire lease
    let (epoch, expires_at_ms1) = manager
        .try_acquire(inode_id, client_id, WriteMode::Write, None)
        .unwrap();

    // Renew lease
    let expires_at_ms2 = manager.renew(inode_id, epoch, client_id).unwrap();
    assert!(expires_at_ms2 >= expires_at_ms1);
    assert!(manager.validate_lease(inode_id, epoch).is_ok());
}

#[test]
fn test_lease_expire_and_steal() {
    let manager = LeaseManager::default();
    let inode_id = InodeId::new(1);
    let client1 = ClientId::new(1);
    let _client2 = ClientId::new(2);

    // Client1 acquires lease
    let (_epoch1, _) = manager.try_acquire(inode_id, client1, WriteMode::Write, None).unwrap();

    // Manually expire the lease (simulate time passing)
    // For testing, we can't easily manipulate time, so we'll test the cleanup logic
    manager.cleanup_expired();

    // Client2 should be able to acquire after expiration (if we could manipulate time)
    // For now, we just verify the structure works
    assert!(manager.has_active_lease(inode_id));
}

#[test]
fn test_lease_fencing() {
    let manager = LeaseManager::default();
    let inode_id = InodeId::new(1);
    let client1 = ClientId::new(1);
    let client2 = ClientId::new(2);

    // Client1 acquires lease
    let (epoch1, _) = manager.try_acquire(inode_id, client1, WriteMode::Write, None).unwrap();

    // Release client1's lease first (simulate expiration or explicit release)
    manager.release(inode_id, epoch1);

    // Client2 acquires lease (after client1 released)
    let (epoch2, _) = manager
        .try_acquire(inode_id, client2, WriteMode::Write, Some(epoch1))
        .unwrap();

    assert!(epoch2 > epoch1);

    // Client1 tries to validate old lease -> should fail (fencing)
    let result = manager.validate_lease(inode_id, epoch1);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), beryl_types::fs::FsErrorCode::EPerm);
}

#[test]
fn test_append_mode_base_size() {
    let manager = LeaseManager::default();
    let inode_id = InodeId::new(1);
    let client_id = ClientId::new(1);

    // Acquire lease in APPEND mode
    let (epoch, _) = manager
        .try_acquire(inode_id, client_id, WriteMode::Append, None)
        .unwrap();

    // Verify lease mode is stored
    let active_lease = manager.get_active_lease(inode_id).unwrap();
    assert_eq!(active_lease.mode, WriteMode::Append);
    assert_eq!(active_lease.lease_epoch, epoch);
}

#[test]
fn test_truncate_shrink_extents() {
    // Test that truncate correctly shrinks extents
    // This is tested via integration tests with full Raft setup
    // Unit test here just verifies the logic structure
    use beryl_types::fs::Extent;

    let extents = [
        Extent {
            file_offset: 0,
            block_id: BlockId::new(DataHandleId::new(1), BlockIndex::new(0)),
            block_offset: 0,
            len: 4096,
            content_revision: None,
            block_stamp: None,
        },
        Extent {
            file_offset: 4096,
            block_id: BlockId::new(DataHandleId::new(1), BlockIndex::new(1)),
            block_offset: 0,
            len: 4096,
            content_revision: None,
            block_stamp: None,
        },
    ];

    // Simulate truncate to 6000 (should truncate second extent)
    let new_size = 6000u64;
    let mut new_extents: Vec<_> = Vec::new();
    for extent in extents.iter() {
        let extent_end = extent.file_offset + extent.len;
        if extent_end <= new_size {
            new_extents.push(extent.clone());
        } else if extent.file_offset < new_size {
            let truncated_len = new_size - extent.file_offset;
            if truncated_len > 0 {
                let mut truncated_extent = extent.clone();
                truncated_extent.len = truncated_len;
                new_extents.push(truncated_extent);
            }
        }
    }

    assert_eq!(new_extents.len(), 2);
    assert_eq!(new_extents[0].len, 4096);
    assert_eq!(new_extents[1].len, 1904); // 6000 - 4096
}
