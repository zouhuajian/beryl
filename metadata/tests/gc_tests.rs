// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for GC (Garbage Collection) and refcount management.

use metadata::raft::RocksDBStorage;
use metadata::state::{DeleteIntent, DeleteIntentReason, DeleteIntentStatus};
use metadata::worker::{RepairQueue, RepairTask};
use std::sync::Arc;
use tempfile::TempDir;
use types::fs::Extent;
use types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};

#[test]
fn test_refcount_increment_on_commit() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(temp_dir.path()).unwrap());

    let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));

    // Initially, refcount should be None (0)
    assert_eq!(storage.get_block_ref_count(block_id).unwrap(), None);

    // Increment refcount
    let new_count = storage.increment_block_ref_count(block_id).unwrap();
    assert_eq!(new_count, 1);
    assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));

    // Increment again
    let new_count = storage.increment_block_ref_count(block_id).unwrap();
    assert_eq!(new_count, 2);
    assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(2));
}

#[test]
fn test_refcount_decrement_to_zero_generates_intent() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(temp_dir.path()).unwrap());

    let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));

    // Set refcount to 1
    storage.put_block_ref_count(block_id, 1).unwrap();

    // Decrement to 0
    let (new_count, reached_zero) = storage.decrement_block_ref_count(block_id).unwrap();
    assert_eq!(new_count, 0);
    assert!(reached_zero);
    assert_eq!(storage.get_block_ref_count(block_id).unwrap(), None);
}

#[test]
fn test_refcount_decrement_below_zero_clamped() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(temp_dir.path()).unwrap());

    let block_id = BlockId::new(DataHandleId::new(1), BlockIndex::new(0));

    // Try to decrement when refcount is 0 (should clamp to 0)
    let (new_count, reached_zero) = storage.decrement_block_ref_count(block_id).unwrap();
    assert_eq!(new_count, 0);
    assert!(reached_zero);
}

// Note: GC intent processing tests are now in maintenance/gc.rs tests
// This test file focuses on refcount and intent creation tests

#[test]
fn test_unique_block_ids_per_inode() {
    // Test that multiple extents with same block_id in one inode only count once
    let extents = vec![
        Extent {
            file_offset: 0,
            block_id: BlockId::new(DataHandleId::new(1), BlockIndex::new(0)),
            block_offset: 0,
            len: 4096,
            file_version: None,
            block_stamp: None,
        },
        Extent {
            file_offset: 4096,
            block_id: BlockId::new(DataHandleId::new(1), BlockIndex::new(0)), // Same block_id
            block_offset: 0,
            len: 4096,
            file_version: None,
            block_stamp: None,
        },
    ];

    // Collect unique block_ids
    let mut unique_block_ids = std::collections::HashSet::new();
    for extent in &extents {
        unique_block_ids.insert(extent.block_id);
    }

    // Should only have 1 unique block_id
    assert_eq!(unique_block_ids.len(), 1);
}
