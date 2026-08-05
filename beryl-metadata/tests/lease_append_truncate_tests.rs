// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Tests for lease / append / truncate behavior.

use beryl_metadata::inode_lease::{LeaseError, LeaseManager, WriteMode};
use beryl_types::fs::InodeId;
use beryl_types::ids::ClientId;

#[test]
fn test_lease_conflict() {
    let manager = LeaseManager::default();
    let inode_id = InodeId::new(1);
    let client1 = ClientId::new(1);
    let client2 = ClientId::new(2);

    // Client1 acquires lease
    let (epoch1, _) = manager.try_acquire(inode_id, client1, WriteMode::Write, None).unwrap();

    // Client2 tries to acquire the already-owned lease.
    let result = manager.try_acquire(inode_id, client2, WriteMode::Write, Some(epoch1));
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), LeaseError::Active);
}

#[test]
fn test_lease_epoch_overflow_fails_without_creating_runtime_lease() {
    let manager = LeaseManager::new(60_000, 10_000);
    let inode_id = InodeId::new(99);

    let result = manager.try_acquire(inode_id, ClientId::new(1), WriteMode::Write, Some(u64::MAX));

    assert_eq!(result, Err(LeaseError::EpochExhausted));
    assert!(manager.get_active_lease(inode_id).is_none());
}

#[test]
fn renew_reports_absent_unowned_and_expired_leases() {
    let inode_id = InodeId::new(1);
    let client1 = ClientId::new(1);
    let client2 = ClientId::new(2);

    let manager = LeaseManager::default();
    assert_eq!(manager.renew(inode_id, 1, client1), Err(LeaseError::NotFound));
    let (epoch, initial_expiry_ms) = manager.try_acquire(inode_id, client1, WriteMode::Write, None).unwrap();
    assert_eq!(manager.renew(inode_id, epoch, client2), Err(LeaseError::OwnerMismatch));
    assert_eq!(
        manager.renew(inode_id, epoch + 1, client1),
        Err(LeaseError::EpochMismatch {
            expected: epoch,
            got: epoch + 1,
        })
    );
    let renewed_expiry_ms = manager.renew(inode_id, epoch, client1).unwrap();
    assert!(renewed_expiry_ms >= initial_expiry_ms);
    assert!(manager.validate_lease(inode_id, epoch).is_ok());

    let expired = LeaseManager::new(0, 10_000);
    let (expired_epoch, _) = expired.try_acquire(inode_id, client1, WriteMode::Write, None).unwrap();
    assert_eq!(
        expired.renew(inode_id, expired_epoch, client1),
        Err(LeaseError::Expired)
    );
    assert!(expired.get_active_lease(inode_id).is_none());
}

#[test]
fn validate_reports_absent_stale_and_expired_leases() {
    let inode_id = InodeId::new(1);
    let client = ClientId::new(1);

    let manager = LeaseManager::default();
    assert_eq!(manager.validate_lease(inode_id, 1), Err(LeaseError::NotFound));
    let (epoch, _) = manager.try_acquire(inode_id, client, WriteMode::Write, None).unwrap();
    assert_eq!(
        manager.validate_lease(inode_id, epoch + 1),
        Err(LeaseError::EpochMismatch {
            expected: epoch,
            got: epoch + 1,
        })
    );

    let expired = LeaseManager::new(0, 10_000);
    let (expired_epoch, _) = expired.try_acquire(inode_id, client, WriteMode::Write, None).unwrap();
    assert_eq!(
        expired.validate_lease(inode_id, expired_epoch),
        Err(LeaseError::Expired)
    );
}

#[test]
fn expired_lease_can_be_reacquired_with_a_higher_durable_epoch() {
    let manager = LeaseManager::new(0, 10_000);
    let inode_id = InodeId::new(1);
    let client1 = ClientId::new(1);
    let client2 = ClientId::new(2);

    let (epoch1, _) = manager.try_acquire(inode_id, client1, WriteMode::Write, None).unwrap();
    let (epoch2, _) = manager
        .try_acquire(inode_id, client2, WriteMode::Write, Some(epoch1))
        .unwrap();

    assert_eq!(epoch2, epoch1 + 1);
    let active = manager.get_active_lease(inode_id).expect("new owner lease");
    assert_eq!(active.lease_epoch, epoch2);
    assert_eq!(active.owner_client_id, client2);
    assert_eq!(
        manager.validate_lease(inode_id, epoch1),
        Err(LeaseError::EpochMismatch {
            expected: epoch2,
            got: epoch1,
        })
    );
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
